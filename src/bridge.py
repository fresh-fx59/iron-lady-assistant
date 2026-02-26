import asyncio
import json
import logging
import os
import re
import time
from asyncio import LimitOverrunError
from dataclasses import dataclass
from enum import Enum
from typing import AsyncGenerator

from . import config, metrics

logger = logging.getLogger(__name__)


async def _drain_oversized_line(
    stream: asyncio.StreamReader,
    consumed_hint: int | None = None,
) -> None:
    """Drain bytes until newline so stream processing can continue."""
    if consumed_hint and consumed_hint > 0:
        await stream.read(consumed_hint)

    while True:
        chunk = await stream.read(64 * 1024)
        if not chunk or b"\n" in chunk:
            return


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
            if not cmd:
                return None
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


def _default_subprocess_env() -> dict[str, str]:
    """Build a clean env for the claude subprocess.

    Strips CLAUDECODE to prevent the nested-session guard from blocking
    the child process when the bot itself is launched from inside Claude Code.
    """
    env = os.environ.copy()
    env.pop("CLAUDECODE", None)
    return env


# Backward-compatible alias for tests and older code.
def _subprocess_env() -> dict[str, str]:
    return _default_subprocess_env()


def _extract_codex_tool_input(item: dict) -> str | None:
    """Extract a concise tool input from a Codex CLI item."""
    if not item:
        return None

    for key in ("command", "path", "file_path", "query", "url", "tool", "name"):
        val = item.get(key)
        if isinstance(val, str) and val:
            return (val[:120] + "...") if len(val) > 120 else val

    # Fallback to any structured input
    if "input" in item:
        try:
            s = json.dumps(item["input"], separators=(",", ":"))
            return (s[:120] + "...") if len(s) > 120 else s
        except TypeError:
            pass

    return None


def _extract_codex_session_id(payload: dict) -> str | None:
    """Best-effort extraction of a Codex session/thread identifier."""
    for key in ("thread_id", "session_id", "conversation_id"):
        val = payload.get(key)
        if isinstance(val, str) and val:
            return val
    return None


async def stream_message(
    prompt: str,
    session_id: str | None = None,
    model: str = "sonnet",
    working_dir: str | None = None,
    process_handle: dict | None = None,
    subprocess_env: dict[str, str] | None = None,
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

    base_url = subprocess_env.get("ANTHROPIC_BASE_URL") if subprocess_env else None
    logger.info("Running: %s [base_url=%s]", " ".join(cmd[:6]) + " ...", base_url or "default")

    start = time.monotonic()
    # Track the tool currently being streamed via stream_event deltas
    current_tool: str | None = None
    accumulated_input: str = ""
    # Set of tools already reported via stream_event so assistant fallback
    # can skip them to avoid double-reporting with stale input text.
    reported_tools: set[str] = set()

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=working_dir,
            env=subprocess_env or _default_subprocess_env(),
            limit=1024 * 1024,
        )
    except FileNotFoundError:
        yield StreamEvent(
            event_type=StreamEventType.RESULT,
            response=ClaudeResponse(
                text="Claude CLI is not installed or not in PATH on the server.",
                session_id=session_id,
                is_error=True,
                cost_usd=0,
                duration_ms=0,
                num_turns=0,
            ),
        )
        return

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
                # Check if process is still alive - if so, it's just doing work without output
                if proc.returncode is None:
                    logger.debug("Readline timeout but process still running, continuing to wait...")
                    continue
                # Process has actually died - report the timeout error
                logger.warning("Claude process idle timeout (%d s) - process terminated", config.IDLE_TIMEOUT)
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
            except LimitOverrunError as e:
                logger.warning(
                    "Claude stream line exceeded buffer; draining oversized line (consumed=%d)",
                    e.consumed,
                )
                await _drain_oversized_line(proc.stdout, consumed_hint=e.consumed)
                continue
            except ValueError:
                logger.warning("Claude stream line raised ValueError; draining oversized line")
                await _drain_oversized_line(proc.stdout)
                continue

            if not line:
                break

            if isinstance(line, bytes):
                line_str = line.decode().strip()
            else:
                line_str = str(line).strip()
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

                    # Log empty responses to diagnose root cause
                    if not result_text or result_text.strip() == "":
                        logger.warning(
                            "Claude returned empty result text - data keys: %s, is_error=%s, cost=%.6f, turns=%d",
                            list(data.keys()),
                            is_error,
                            cost_usd,
                            num_turns,
                        )

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
            logger.warning("Claude stderr: %s", stderr.strip()[:500])  # Limit log size
        else:
            logger.warning("Claude process exited without result and no stderr (returncode=%d)", proc.returncode)

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


async def stream_codex_message(
    prompt: str,
    session_id: str | None = None,
    model: str | None = None,
    resume_arg: str | None = None,
    working_dir: str | None = None,
    process_handle: dict | None = None,
    subprocess_env: dict[str, str] | None = None,
) -> AsyncGenerator[StreamEvent, None]:
    """Stream Codex CLI responses as events with idle timeout."""
    cmd = ["codex", "exec", "--json", "--full-auto", "--skip-git-repo-check", prompt]
    if model:
        cmd.extend(["--model", model])
    if session_id and resume_arg:
        cmd.extend([resume_arg, session_id])

    logger.info("Running: %s", " ".join(cmd[:4]) + " ...")

    start = time.monotonic()
    last_message: str | None = None
    error_text: str | None = None
    codex_session_id: str | None = session_id

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=working_dir,
            env=subprocess_env or _default_subprocess_env(),
            limit=1024 * 1024,
        )
    except FileNotFoundError:
        yield StreamEvent(
            event_type=StreamEventType.RESULT,
            response=ClaudeResponse(
                text="Codex CLI is not installed or not in PATH on the server.",
                session_id=codex_session_id,
                is_error=True,
                cost_usd=0,
                duration_ms=0,
                num_turns=0,
            ),
        )
        return

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
                if proc.returncode is None:
                    logger.debug("Codex readline timeout but process still running, continuing to wait...")
                    continue
                logger.warning("Codex process idle timeout (%d s) - process terminated", config.IDLE_TIMEOUT)
                proc.kill()
                await proc.wait()
                elapsed = time.monotonic() - start
                metrics.CLAUDE_REQUESTS_TOTAL.labels(model=model or "codex", status="timeout").inc()
                metrics.CLAUDE_RESPONSE_DURATION.labels(model=model or "codex").observe(elapsed)
                yield StreamEvent(
                    event_type=StreamEventType.RESULT,
                    response=ClaudeResponse(
                        text="Request idle timed out. Codex stopped producing output.",
                        session_id=codex_session_id,
                        is_error=True,
                        cost_usd=0,
                        duration_ms=elapsed * 1000,
                        num_turns=0,
                        idle_timeout=True,
                    )
                )
                return
            except LimitOverrunError as e:
                logger.warning(
                    "Codex stream line exceeded buffer; draining oversized line (consumed=%d)",
                    e.consumed,
                )
                await _drain_oversized_line(proc.stdout, consumed_hint=e.consumed)
                continue
            except ValueError:
                logger.warning("Codex stream line raised ValueError; draining oversized line")
                await _drain_oversized_line(proc.stdout)
                continue

            if not line:
                break

            if isinstance(line, bytes):
                line_str = line.decode().strip()
            else:
                line_str = str(line).strip()
            if not line_str:
                continue

            try:
                data = json.loads(line_str)
            except json.JSONDecodeError:
                logger.warning("Failed to parse Codex stream line: %s", line_str[:100])
                continue

            event_type = data.get("type")
            item = data.get("item", {})
            codex_session_id = (
                codex_session_id
                or _extract_codex_session_id(data)
                or _extract_codex_session_id(item)
            )

            if event_type in ("item.started", "item.completed"):
                item_type = item.get("type")
                if item_type in ("agent_message", "assistant_message"):
                    text = item.get("text") or item.get("message")
                    if text:
                        last_message = text
                    continue

                tool_name = item_type or "codex_item"
                tool_input = _extract_codex_tool_input(item)
                yield StreamEvent(
                    event_type=StreamEventType.TOOL_USE,
                    tool_name=tool_name,
                    tool_input=tool_input,
                )
            elif event_type == "error":
                error_text = data.get("message") or data.get("error", {}).get("message")

        # Process exited - gather stderr for diagnostics
        elapsed = time.monotonic() - start
        stderr = (await proc.stderr.read()).decode()
        if stderr:
            logger.warning("Codex stderr: %s", stderr.strip()[:500])

        if error_text:
            metrics.CLAUDE_REQUESTS_TOTAL.labels(model=model or "codex", status="error").inc()
            metrics.CLAUDE_RESPONSE_DURATION.labels(model=model or "codex").observe(elapsed)
            yield StreamEvent(
                event_type=StreamEventType.RESULT,
                response=ClaudeResponse(
                    text=error_text,
                    session_id=codex_session_id,
                    is_error=True,
                    cost_usd=0,
                    duration_ms=elapsed * 1000,
                    num_turns=0,
                )
            )
            return

        if last_message:
            metrics.CLAUDE_REQUESTS_TOTAL.labels(model=model or "codex", status="success").inc()
            metrics.CLAUDE_RESPONSE_DURATION.labels(model=model or "codex").observe(elapsed)
            yield StreamEvent(
                event_type=StreamEventType.RESULT,
                response=ClaudeResponse(
                    text=last_message,
                    session_id=codex_session_id,
                    is_error=False,
                    cost_usd=0,
                    duration_ms=elapsed * 1000,
                    num_turns=0,
                )
            )
            return

        # If we got here, no usable result
        if proc.returncode != 0 and stderr.strip():
            metrics.CLAUDE_REQUESTS_TOTAL.labels(model=model or "codex", status="error").inc()
            metrics.CLAUDE_RESPONSE_DURATION.labels(model=model or "codex").observe(elapsed)
            yield StreamEvent(
                event_type=StreamEventType.RESULT,
                response=ClaudeResponse(
                    text=stderr.strip(),
                    session_id=codex_session_id,
                    is_error=True,
                    cost_usd=0,
                    duration_ms=elapsed * 1000,
                    num_turns=0,
                )
            )
            return

        metrics.CLAUDE_REQUESTS_TOTAL.labels(model=model or "codex", status="error").inc()
        metrics.CLAUDE_RESPONSE_DURATION.labels(model=model or "codex").observe(elapsed)
        yield StreamEvent(
            event_type=StreamEventType.RESULT,
            response=ClaudeResponse(
                text="Codex process exited without producing a result.",
                session_id=codex_session_id,
                is_error=True,
                cost_usd=0,
                duration_ms=elapsed * 1000,
                num_turns=0,
            )
        )
    except Exception:
        logger.exception("Unexpected error in stream_codex_message")
        yield StreamEvent(
            event_type=StreamEventType.RESULT,
            response=ClaudeResponse(
                text="An unexpected error occurred while processing your request.",
                session_id=codex_session_id,
                is_error=True,
                cost_usd=0,
                duration_ms=0,
                num_turns=0,
            )
        )
