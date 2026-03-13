from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable

from .. import bridge, config


async def run_claude(
    message: Any,
    state: Any,
    session: object,
    progress: Any,
    *,
    build_augmented_prompt: Callable[[str], str],
    subprocess_env: dict[str, str] | None = None,
    override_text: str | None = None,
    observed_tools: list[str] | None = None,
) -> bridge.ClaudeResponse | None:
    state.process_handle = {}

    raw_prompt = _as_text(override_text) or _as_text(getattr(message, "text", None))
    prompt = build_augmented_prompt(raw_prompt)

    stream = bridge.stream_message(
        prompt=prompt,
        session_id=getattr(session, "claude_session_id", None),
        model=getattr(session, "model", None),
        working_dir=config.CLAUDE_WORKING_DIR,
        process_handle=state.process_handle,
        subprocess_env=subprocess_env,
    )
    iterator = stream if hasattr(stream, "__aiter__") else _iter_sync(stream)

    async for event in iterator:
        if state.cancel_requested:
            await progress.show_cancelled()
            return bridge.ClaudeResponse(
                text="Request cancelled.",
                session_id=getattr(session, "claude_session_id", None),
                is_error=True,
                cost_usd=0,
                duration_ms=0,
                num_turns=0,
            )

        response = await _handle_stream_event(event, progress, observed_tools)
        if response is not None:
            return response

    return None


async def run_codex(
    message: Any,
    state: Any,
    session: object,
    progress: Any,
    *,
    build_augmented_prompt: Callable[[str], str],
    codex_working_dir: Callable[[], str] | str,
    model: str | None = None,
    session_id: str | None = None,
    resume_arg: str | None = None,
    subprocess_env: dict[str, str] | None = None,
    cli_name: str = "codex",
    override_text: str | None = None,
    observed_tools: list[str] | None = None,
) -> bridge.ClaudeResponse | None:
    state.process_handle = {}

    raw_prompt = _as_text(override_text) or _as_text(getattr(message, "text", None))
    prompt = build_augmented_prompt(raw_prompt)
    working_dir = codex_working_dir() if callable(codex_working_dir) else codex_working_dir

    stream = bridge.stream_codex_message(
        prompt=prompt,
        session_id=session_id,
        model=model,
        resume_arg=resume_arg,
        cli_name=cli_name,
        working_dir=working_dir,
        process_handle=state.process_handle,
        subprocess_env=subprocess_env,
    )
    iterator = stream if hasattr(stream, "__aiter__") else _iter_sync(stream)

    async for event in iterator:
        if state.cancel_requested:
            await progress.show_cancelled()
            return bridge.ClaudeResponse(
                text="Request cancelled.",
                session_id=session_id,
                is_error=True,
                cost_usd=0,
                duration_ms=0,
                num_turns=0,
            )

        response = await _handle_stream_event(event, progress, observed_tools)
        if response is not None:
            return response

    return None


async def run_codex_with_retries(
    message: Any,
    state: Any,
    session: object,
    progress: Any,
    *,
    run_codex_fn: Callable[..., Awaitable[bridge.ClaudeResponse | None]],
    is_transient_error_fn: Callable[[str | None], bool],
    sanitize_transient_error_fn: Callable[..., bridge.ClaudeResponse],
    logger: Any,
    model: str | None = None,
    session_id: str | None = None,
    resume_arg: str | None = None,
    subprocess_env: dict[str, str] | None = None,
    cli_name: str = "codex",
    override_text: str | None = None,
    observed_tools: list[str] | None = None,
) -> bridge.ClaudeResponse | None:
    retries_left = max(0, config.CODEX_TRANSIENT_MAX_RETRIES)
    attempt = 0
    next_session_id = session_id

    while True:
        attempt += 1
        response = await run_codex_fn(
            message,
            state,
            session,
            progress,
            model,
            next_session_id,
            resume_arg,
            subprocess_env,
            cli_name,
            override_text=override_text,
            observed_tools=observed_tools,
        )
        if not response:
            return None
        if state.cancel_requested or not response.is_error or not is_transient_error_fn(response.text):
            return response
        if retries_left <= 0:
            return sanitize_transient_error_fn(response, attempts=attempt)

        retries_left -= 1
        logger.warning(
            "Chat %d: transient Codex error on attempt %d, retrying (%d retries left): %s",
            message.chat.id,
            attempt,
            retries_left,
            response.text[:200],
        )
        if next_session_id:
            next_session_id = None
        await asyncio.sleep(max(0.0, config.CODEX_TRANSIENT_RETRY_BACKOFF_SECONDS))


async def _iter_sync(stream: Any):
    for item in stream:
        yield item


async def _handle_stream_event(event: Any, progress: Any, observed_tools: list[str] | None):
    match event.event_type:
        case bridge.StreamEventType.TOOL_USE:
            if event.tool_name:
                if observed_tools is not None:
                    observed_tools.append(event.tool_name)
                await progress.report_tool(event.tool_name, event.tool_input)
            return None
        case bridge.StreamEventType.RESULT:
            return event.response
        case "TOOL_USE":
            if getattr(event, "tool_name", None):
                if observed_tools is not None:
                    observed_tools.append(event.tool_name)
                await progress.report_tool(event.tool_name, getattr(event, "tool_input", None))
            return None
        case "RESULT":
            return event.response
    return None


def _as_text(value: object) -> str:
    return value if isinstance(value, str) else ""
