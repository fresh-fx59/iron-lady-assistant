import asyncio
import json
import logging
from dataclasses import dataclass

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
        "--permission-mode", "dontAsk",
    ]
    if session_id:
        cmd.extend(["--resume", session_id])

    logger.info("Running: %s", " ".join(cmd[:6]) + " ...")

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
        return ClaudeResponse(
            text="Request timed out. Try a simpler question or start a /new conversation.",
            session_id=session_id,
            is_error=True,
            cost_usd=0,
            duration_ms=timeout * 1000,
            num_turns=0,
        )

    stdout_text = stdout.decode()
    stderr_text = stderr.decode()

    if stderr_text:
        logger.warning("Claude stderr: %s", stderr_text.strip())

    if proc.returncode != 0 and not stdout_text.strip():
        error_msg = stderr_text.strip() or f"Claude exited with code {proc.returncode}"
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
        # If JSON parsing fails but we have output, return it as plain text
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
    if data.get("is_error"):
        result_text = result_text or "Claude returned an error."

    return ClaudeResponse(
        text=result_text,
        session_id=data.get("session_id", session_id),
        is_error=bool(data.get("is_error")),
        cost_usd=float(data.get("total_cost_usd", 0)),
        duration_ms=float(data.get("duration_ms", 0)),
        num_turns=int(data.get("num_turns", 0)),
    )
