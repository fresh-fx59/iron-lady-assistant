import asyncio
import json
import logging
import time
from dataclasses import dataclass

from . import metrics

logger = logging.getLogger(__name__)


@dataclass
class ClaudeResponse:
    text: str
    session_id: str | None
    is_error: bool
    cost_usd: float
    duration_ms: float
    num_turns: int


async def send_message(
    prompt: str,
    session_id: str | None = None,
    model: str = "sonnet",
    working_dir: str | None = None,
    timeout: int = 300,
) -> ClaudeResponse:
    cmd = [
        "claude",
        "-p",
        prompt,
        "--output-format", "json",
        "--model", model,
        "--dangerously-skip-permissions",
    ]
    if session_id:
        cmd.extend(["--resume", session_id])

    logger.info("Running: %s", " ".join(cmd[:6]) + " ...")

    start = time.monotonic()

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=working_dir,
    )

    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(), timeout=timeout
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        elapsed = time.monotonic() - start
        metrics.CLAUDE_REQUESTS_TOTAL.labels(model=model, status="timeout").inc()
        metrics.CLAUDE_RESPONSE_DURATION.labels(model=model).observe(elapsed)
        return ClaudeResponse(
            text="Request timed out. Try a simpler question or start a /new conversation.",
            session_id=session_id,
            is_error=True,
            cost_usd=0,
            duration_ms=timeout * 1000,
            num_turns=0,
        )

    elapsed = time.monotonic() - start
    stdout_text = stdout.decode()
    stderr_text = stderr.decode()

    if stderr_text:
        logger.warning("Claude stderr: %s", stderr_text.strip())

    if proc.returncode != 0 and not stdout_text.strip():
        error_msg = stderr_text.strip() or f"Claude exited with code {proc.returncode}"
        metrics.CLAUDE_REQUESTS_TOTAL.labels(model=model, status="error").inc()
        metrics.CLAUDE_RESPONSE_DURATION.labels(model=model).observe(elapsed)
        return ClaudeResponse(
            text=f"Error: {error_msg}",
            session_id=session_id,
            is_error=True,
            cost_usd=0,
            duration_ms=0,
            num_turns=0,
        )

    try:
        data = json.loads(stdout_text)
    except json.JSONDecodeError:
        status = "success" if stdout_text.strip() else "error"
        metrics.CLAUDE_REQUESTS_TOTAL.labels(model=model, status=status).inc()
        metrics.CLAUDE_RESPONSE_DURATION.labels(model=model).observe(elapsed)
        if stdout_text.strip():
            return ClaudeResponse(
                text=stdout_text.strip(),
                session_id=session_id,
                is_error=False,
                cost_usd=0,
                duration_ms=0,
                num_turns=0,
            )
        return ClaudeResponse(
            text="Failed to parse Claude response.",
            session_id=session_id,
            is_error=True,
            cost_usd=0,
            duration_ms=0,
            num_turns=0,
        )

    result_text = data.get("result", "")
    is_error = bool(data.get("is_error"))
    cost_usd = float(data.get("total_cost_usd", 0))
    num_turns = int(data.get("num_turns", 0))

    if is_error:
        result_text = result_text or "Claude returned an error."

    # Record metrics
    status = "error" if is_error else "success"
    metrics.CLAUDE_REQUESTS_TOTAL.labels(model=model, status=status).inc()
    metrics.CLAUDE_RESPONSE_DURATION.labels(model=model).observe(elapsed)
    if cost_usd > 0:
        metrics.CLAUDE_COST_USD.labels(model=model).inc(cost_usd)
    if num_turns > 0:
        metrics.CLAUDE_TURNS_TOTAL.labels(model=model).inc(num_turns)

    return ClaudeResponse(
        text=result_text,
        session_id=data.get("session_id", session_id),
        is_error=is_error,
        cost_usd=cost_usd,
        duration_ms=float(data.get("duration_ms", 0)),
        num_turns=num_turns,
    )
