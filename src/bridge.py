import asyncio
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from enum import Enum
from typing import AsyncGenerator

from . import config, metrics

logger = logging.getLogger(__name__)


class StreamEventType(Enum):
    """Types of events that can be streamed from Claude."""
    TOOL_USE = "tool_use"
    RESULT = "result"
    ERROR = "error"


@dataclass
class StreamEvent:
    """A single event from the Claude stream."""
    event_type: StreamEventType
    tool_name: str | None = None
    tool_input: str | None = None
    response: "ClaudeResponse | None" = None


@dataclass
class ClaudeResponse:
    text: str
    session_id: str | None
    is_error: bool
    cost_usd: float
    duration_ms: float = 0
    num_turns: int = 0
    cancelled: bool = False
    idle_timeout: bool = False


def _extract_tool_input(tool_name: str, input_data: dict) -> str | None:
    """Extract the primary argument from a tool's complete input dict."""
    tool = tool_name.lower()
    match tool:
        case "bash":
            cmd = input_data.get("command", "")
            return (cmd[:80] + "...") if len(cmd) > 80 else cmd
        case "read" | "edit" | "write":
            return input_data.get("file_path")
        case "grep" | "glob":
            return input_data.get("pattern")
        case "task" | "askuserquestion":
            desc = input_data.get("description", "")
            return (desc[:60] + "...") if len(desc) > 60 else desc
        case _:
            if input_data:
                s = json.dumps(input_data, separators=(",", ":"))
                return (s[:80] + "...") if len(s) > 80 else s
            return None


def _extract_tool_input_partial(tool_name: str, partial_json: str) -> str | None:
    """Try to extract a meaningful tool input from partial/accumulating JSON.

    Called during streaming as input_json_delta chunks arrive. Attempts JSON
    parse first, then falls back to regex extraction from the partial string.
    """
    tool = tool_name.lower()

    # Try full JSON parse (works once enough has accumulated)
    try:
        data = json.loads(partial_json)
        return _extract_tool_input(tool_name, data)
    except json.JSONDecodeError:
        pass

    # Regex fallback for partial JSON
    _FIELD_MAP = {
        "bash": ("command", 80),
        "read": ("file_path", 200),
        "edit": ("file_path", 200),
        "write": ("file_path", 200),
        "grep": ("pattern", 60),
        "glob": ("pattern", 60),
        "task": ("description", 60),
        "askuserquestion": ("description", 60),
    }
    entry = _FIELD_MAP.get(tool)
    if entry:
        field, max_len = entry
        m = re.search(rf'"{field}"\s*:\s*"([^"]+)', partial_json)
        if m:
            val = m.group(1)
            return (val[:max_len] + "...") if len(val) > max_len else val

    return None


def _subprocess_env() -> dict[str, str]:
    """Build a clean env for the claude subprocess.

    Strips CLAUDECODE to prevent the nested-session guard from blocking
    the child process when the bot itself is launched from inside Claude Code.
    """
    env = os.environ.copy()
    env.pop("CLAUDECODE", None)
    return env


async def stream_message(
    prompt: str,
    session_id: str | None = None,
    model: str = "sonnet",
    working_dir: str | None = None,
    process_handle: dict | None = None,
) -> AsyncGenerator[StreamEvent, None]:
    """Stream Claude's response as events with idle timeout.

    Uses ``--output-format stream-json --verbose --include-partial-messages``
    to get real-time tool activity.  Each stdout line is a JSON object whose
    ``type`` field determines the payload:

      - ``stream_event`` — wraps a raw Anthropic API streaming event in
        ``.event`` (content_block_start, content_block_delta, …).  These
        arrive in real-time while Claude is generating.
      - ``assistant`` — a complete assistant message with all content blocks
        (text and/or tool_use).  Arrives after the API call for a turn ends.
      - ``user`` — tool results fed back to Claude (skipped).
      - ``system`` — session init (skipped).
      - ``result`` — final result with cost / session_id / duration.

    We emit ``TOOL_USE`` events from **both** ``stream_event`` (real-time)
    and ``assistant`` (fallback).  The :class:`ProgressReporter` deduplicates.
    """
    cmd = [
        "claude",
        "-p",
        prompt,
        "--output-format", "stream-json",
        "--verbose",
        "--include-partial-messages",
        "--model", model,
        "--dangerously-skip-permissions",
    ]
    if session_id:
        cmd.extend(["--resume", session_id])

    logger.info("Running: %s", " ".join(cmd[:6]) + " ...")

    start = time.monotonic()
    # Track the tool currently being streamed via stream_event deltas
    current_tool: str | None = None
    accumulated_input: str = ""
    # Set of tools already reported via stream_event so assistant fallback
    # can skip them to avoid double-reporting with stale input text.
    reported_tools: set[str] = set()

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=working_dir,
        env=_subprocess_env(),
    )

    if process_handle is not None:
        process_handle["proc"] = proc

    try:
        while True:
            try:
                line = await asyncio.wait_for(
                    proc.stdout.readline(),
                    timeout=config.IDLE_TIMEOUT
                )
            except asyncio.TimeoutError:
                logger.warning("Claude process idle timeout (%d s)", config.IDLE_TIMEOUT)
                proc.kill()
                await proc.wait()
                elapsed = time.monotonic() - start
                metrics.CLAUDE_REQUESTS_TOTAL.labels(model=model, status="timeout").inc()
                metrics.CLAUDE_RESPONSE_DURATION.labels(model=model).observe(elapsed)
                yield StreamEvent(
                    event_type=StreamEventType.RESULT,
                    response=ClaudeResponse(
                        text="Request idle timed out. Claude stopped producing output.",
                        session_id=session_id,
                        is_error=True,
                        cost_usd=0,
                        duration_ms=elapsed * 1000,
                        num_turns=0,
                        idle_timeout=True,
                    )
                )
                return

            if not line:
                break

            line_str = line.decode().strip()
            if not line_str:
                continue

            try:
                data = json.loads(line_str)
                event_type = data.get("type")

                # ── Real-time API streaming events ──────────────
                if event_type == "stream_event":
                    inner = data.get("event", {})
                    inner_type = inner.get("type")

                    if inner_type == "content_block_start":
                        block = inner.get("content_block", {})
                        if block.get("type") == "tool_use":
                            current_tool = block.get("name", "")
                            accumulated_input = ""
                            yield StreamEvent(
                                event_type=StreamEventType.TOOL_USE,
                                tool_name=current_tool,
                            )

                    elif inner_type == "content_block_delta":
                        delta = inner.get("delta", {})
                        if current_tool and delta.get("type") == "input_json_delta":
                            accumulated_input += delta.get("partial_json", "")
                            tool_input = _extract_tool_input_partial(
                                current_tool, accumulated_input
                            )
                            if tool_input:
                                reported_tools.add(current_tool)
                                yield StreamEvent(
                                    event_type=StreamEventType.TOOL_USE,
                                    tool_name=current_tool,
                                    tool_input=tool_input,
                                )

                    elif inner_type == "content_block_stop":
                        current_tool = None
                        accumulated_input = ""

                # ── Complete assistant message (fallback) ───────
                elif event_type == "assistant":
                    message = data.get("message", {})
                    for block in message.get("content", []):
                        if block.get("type") == "tool_use":
                            tool_name = block.get("name", "")
                            # Skip if we already reported this tool via
                            # stream_event with a parsed input.
                            if tool_name in reported_tools:
                                continue
                            tool_input = _extract_tool_input(
                                tool_name, block.get("input", {})
                            )
                            yield StreamEvent(
                                event_type=StreamEventType.TOOL_USE,
                                tool_name=tool_name,
                                tool_input=tool_input,
                            )
                    # Reset for next turn
                    reported_tools.clear()

                # ── Final result ────────────────────────────────
                elif event_type == "result":
                    result_text = data.get("result", "")
                    is_error = bool(data.get("is_error"))
                    cost_usd = float(data.get("total_cost_usd", 0))
                    num_turns = int(data.get("num_turns", 0))

                    if is_error:
                        result_text = result_text or "Claude returned an error."

                    status = "error" if is_error else "success"
                    metrics.CLAUDE_REQUESTS_TOTAL.labels(model=model, status=status).inc()
                    elapsed = time.monotonic() - start
                    metrics.CLAUDE_RESPONSE_DURATION.labels(model=model).observe(elapsed)
                    if cost_usd > 0:
                        metrics.CLAUDE_COST_USD.labels(model=model).inc(cost_usd)
                    if num_turns > 0:
                        metrics.CLAUDE_TURNS_TOTAL.labels(model=model).inc(num_turns)

                    yield StreamEvent(
                        event_type=StreamEventType.RESULT,
                        response=ClaudeResponse(
                            text=result_text,
                            session_id=data.get("session_id", session_id),
                            is_error=is_error,
                            cost_usd=cost_usd,
                            duration_ms=float(data.get("duration_ms", 0)),
                            num_turns=num_turns,
                        )
                    )
                    return

                # Skip "system", "user", etc.

            except json.JSONDecodeError:
                logger.warning("Failed to parse stream line: %s", line_str[:100])

        # Process exited without result event
        elapsed = time.monotonic() - start
        stderr = (await proc.stderr.read()).decode()
        if stderr:
            logger.warning("Claude stderr: %s", stderr.strip())

        if proc.returncode != 0:
            metrics.CLAUDE_REQUESTS_TOTAL.labels(model=model, status="error").inc()
            metrics.CLAUDE_RESPONSE_DURATION.labels(model=model).observe(elapsed)
            yield StreamEvent(
                event_type=StreamEventType.RESULT,
                response=ClaudeResponse(
                    text=f"Error: {stderr.strip() or f'Claude exited with code {proc.returncode}'}",
                    session_id=session_id,
                    is_error=True,
                    cost_usd=0,
                    duration_ms=0,
                    num_turns=0,
                )
            )
        else:
            # Treat as error since we didn't get a proper result
            metrics.CLAUDE_REQUESTS_TOTAL.labels(model=model, status="error").inc()
            metrics.CLAUDE_RESPONSE_DURATION.labels(model=model).observe(elapsed)
            yield StreamEvent(
                event_type=StreamEventType.RESULT,
                response=ClaudeResponse(
                    text="Claude process exited without producing a result.",
                    session_id=session_id,
                    is_error=True,
                    cost_usd=0,
                    duration_ms=0,
                    num_turns=0,
                )
            )

    except Exception:
        logger.exception("Unexpected error in stream_message")
        yield StreamEvent(
            event_type=StreamEventType.RESULT,
            response=ClaudeResponse(
                text="An unexpected error occurred while processing your request.",
                session_id=session_id,
                is_error=True,
                cost_usd=0,
                duration_ms=0,
                num_turns=0,
            )
        )