import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.features.turn_provider_execution import run_provider_execution_loop
from src.features.turn_response_dispatch import dispatch_turn_response


@pytest.mark.asyncio
async def test_dispatch_turn_response_success_sends_chunk_and_clears_errors(mock_message):
    state = SimpleNamespace(cancel_requested=False)
    progress = SimpleNamespace(finish=AsyncMock())
    final_response = SimpleNamespace(
        is_error=False,
        text="hello",
        session_id="sess-1",
        cost_usd=0.1,
    )
    provider = SimpleNamespace(name="claude")
    resume_state_store = SimpleNamespace(
        record_success=MagicMock(),
        record_failure=MagicMock(),
    )
    answer_text = AsyncMock()
    clear_errors = MagicMock()
    remember_outbound = MagicMock()

    has_content, output_size = await dispatch_turn_response(
        message=mock_message,
        state=state,
        final_response=final_response,
        progress=progress,
        scope_key="123:main",
        provider=provider,
        resume_state_store=resume_state_store,
        record_error_fn=MagicMock(),
        build_rollback_suggestion_markup_fn=lambda *_: None,
        answer_text_with_retry_fn=answer_text,
        extract_media_directives_fn=lambda text: (text, [], False),
        strip_tool_directive_lines_fn=lambda text: text,
        send_media_reply_fn=AsyncMock(),
        markdown_to_html_fn=lambda text: text,
        split_message_fn=lambda text: [text],
        strip_html_fn=lambda text: text,
        has_recent_outbound_fn=lambda *_: False,
        remember_outbound_fn=remember_outbound,
        clear_errors_fn=clear_errors,
        empty_response_fallback_text="fallback",
        logger=MagicMock(),
    )

    assert has_content is True
    assert output_size == 5
    answer_text.assert_awaited_once()
    clear_errors.assert_called_once_with("123:main")
    resume_state_store.record_success.assert_called_once()
    progress.finish.assert_awaited_once()


@pytest.mark.asyncio
async def test_dispatch_turn_response_error_reports_and_stops(mock_message):
    state = SimpleNamespace(cancel_requested=False)
    progress = SimpleNamespace(finish=AsyncMock())
    final_response = SimpleNamespace(is_error=True, text="boom")
    provider = SimpleNamespace(name="claude")
    resume_state_store = SimpleNamespace(
        record_success=MagicMock(),
        record_failure=MagicMock(),
    )
    answer_text = AsyncMock()
    record_error = MagicMock()

    await dispatch_turn_response(
        message=mock_message,
        state=state,
        final_response=final_response,
        progress=progress,
        scope_key="123:main",
        provider=provider,
        resume_state_store=resume_state_store,
        record_error_fn=record_error,
        build_rollback_suggestion_markup_fn=lambda *_: None,
        answer_text_with_retry_fn=answer_text,
        extract_media_directives_fn=lambda text: (text, [], False),
        strip_tool_directive_lines_fn=lambda text: text,
        send_media_reply_fn=AsyncMock(),
        markdown_to_html_fn=lambda text: text,
        split_message_fn=lambda text: [text],
        strip_html_fn=lambda text: text,
        has_recent_outbound_fn=lambda *_: False,
        remember_outbound_fn=MagicMock(),
        clear_errors_fn=MagicMock(),
        empty_response_fallback_text="fallback",
        logger=MagicMock(),
    )

    resume_state_store.record_failure.assert_called_once()
    record_error.assert_called_once_with("123:main")
    answer_text.assert_awaited_once()
    progress.finish.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_provider_execution_loop_success_path():
    message = SimpleNamespace(
        chat=SimpleNamespace(id=123),
        message_id=77,
        answer=AsyncMock(),
    )
    state = SimpleNamespace(cancel_requested=False, process_handle={"proc": object()}, reset_requested=True)
    session = SimpleNamespace(claude_session_id=None, codex_session_id=None)
    progress = SimpleNamespace(report_tool=AsyncMock())
    typing_task = asyncio.create_task(asyncio.sleep(3600))

    provider = SimpleNamespace(name="claude", cli="claude", resume_arg=None)
    provider_manager = SimpleNamespace(
        get_provider=lambda _scope: provider,
        reset=lambda _scope: provider,
        advance=lambda _scope: None,
        subprocess_env=lambda _provider: {},
        is_rate_limit_error=lambda _text: False,
    )
    session_manager = SimpleNamespace(
        set_provider=MagicMock(),
        update_session_id=MagicMock(),
        update_codex_session_id=MagicMock(),
    )
    resume_state_store = SimpleNamespace(record_start=MagicMock())
    steering_ledger_store = SimpleNamespace(
        mark_applied=MagicMock(),
        get_unapplied=lambda **_: [],
    )
    final_response = SimpleNamespace(
        is_error=False,
        text="ok",
        session_id=None,
    )

    async def run_claude(*_args, **_kwargs):
        return final_response

    result = await run_provider_execution_loop(
        message=message,
        state=state,
        session=session,
        progress=progress,
        typing_task=typing_task,
        scope_key="123:main",
        chat_id=123,
        thread_id=None,
        raw_prompt="hello",
        override_text=None,
        provider_manager=provider_manager,
        session_manager=session_manager,
        resume_state_store=resume_state_store,
        steering_ledger_store=steering_ledger_store,
        logger=MagicMock(),
        current_model_label_fn=lambda *_: "sonnet",
        is_codex_family_cli_fn=lambda cli: bool(cli and cli.startswith("codex")),
        find_provider_cli_fn=lambda _cli: "/usr/bin/claude",
        as_text_fn=lambda text: text or "",
        worklog_subprocess_env_fn=lambda env, **_: env,
        codex_model_arg_fn=lambda *_: None,
        run_codex_with_retries_fn=AsyncMock(),
        run_claude_fn=run_claude,
        extract_requested_tools_fn=lambda _text: [],
        inject_tool_request_fn=lambda prompt, _tool: prompt,
        build_steering_patch_fn=lambda prompt, _events: prompt,
        has_high_risk_conflict_fn=lambda _events: False,
    )

    assert result.final_response is final_response
    assert result.provider_attempts == 1
    assert result.final_provider_name == "claude"
    assert typing_task.cancelled() is True
    assert state.process_handle is None
    assert state.reset_requested is False


@pytest.mark.asyncio
async def test_run_provider_execution_loop_falls_back_when_cli_missing():
    message = SimpleNamespace(
        chat=SimpleNamespace(id=123),
        message_id=78,
        answer=AsyncMock(),
    )
    state = SimpleNamespace(cancel_requested=False, process_handle=None, reset_requested=False)
    session = SimpleNamespace(claude_session_id=None, codex_session_id=None)
    progress = SimpleNamespace(report_tool=AsyncMock())
    typing_task = asyncio.create_task(asyncio.sleep(3600))

    primary = SimpleNamespace(name="codex", cli="codex", resume_arg=None)
    fallback = SimpleNamespace(name="claude", cli="claude", resume_arg=None)
    provider_manager = SimpleNamespace(
        get_provider=lambda _scope: primary,
        reset=lambda _scope: fallback,
        advance=lambda _scope: None,
        subprocess_env=lambda _provider: {},
        is_rate_limit_error=lambda _text: False,
    )
    session_manager = SimpleNamespace(
        set_provider=MagicMock(),
        update_session_id=MagicMock(),
        update_codex_session_id=MagicMock(),
    )
    resume_state_store = SimpleNamespace(record_start=MagicMock())
    steering_ledger_store = SimpleNamespace(
        mark_applied=MagicMock(),
        get_unapplied=lambda **_: [],
    )

    async def run_claude(*_args, **_kwargs):
        return SimpleNamespace(is_error=False, text="ok", session_id=None)

    result = await run_provider_execution_loop(
        message=message,
        state=state,
        session=session,
        progress=progress,
        typing_task=typing_task,
        scope_key="123:main",
        chat_id=123,
        thread_id=None,
        raw_prompt="hello",
        override_text=None,
        provider_manager=provider_manager,
        session_manager=session_manager,
        resume_state_store=resume_state_store,
        steering_ledger_store=steering_ledger_store,
        logger=MagicMock(),
        current_model_label_fn=lambda *_: "sonnet",
        is_codex_family_cli_fn=lambda cli: bool(cli and cli.startswith("codex")),
        find_provider_cli_fn=lambda _cli: None,
        as_text_fn=lambda text: text or "",
        worklog_subprocess_env_fn=lambda env, **_: env,
        codex_model_arg_fn=lambda *_: None,
        run_codex_with_retries_fn=AsyncMock(),
        run_claude_fn=run_claude,
        extract_requested_tools_fn=lambda _text: [],
        inject_tool_request_fn=lambda prompt, _tool: prompt,
        build_steering_patch_fn=lambda prompt, _events: prompt,
        has_high_risk_conflict_fn=lambda _events: False,
    )

    session_manager.set_provider.assert_called_once_with(123, "claude", None)
    message.answer.assert_awaited()
    assert result.final_provider_name == "claude"


@pytest.mark.asyncio
async def test_run_provider_execution_loop_retries_once_on_empty_success():
    message = SimpleNamespace(
        chat=SimpleNamespace(id=123),
        message_id=79,
        answer=AsyncMock(),
    )
    state = SimpleNamespace(cancel_requested=False, process_handle=None, reset_requested=False)
    session = SimpleNamespace(claude_session_id=None, codex_session_id=None)
    progress = SimpleNamespace(report_tool=AsyncMock())
    typing_task = asyncio.create_task(asyncio.sleep(3600))

    provider = SimpleNamespace(name="claude", cli="claude", resume_arg=None)
    provider_manager = SimpleNamespace(
        get_provider=lambda _scope: provider,
        reset=lambda _scope: provider,
        advance=lambda _scope: None,
        subprocess_env=lambda _provider: {},
        is_rate_limit_error=lambda _text: False,
    )
    session_manager = SimpleNamespace(
        set_provider=MagicMock(),
        update_session_id=MagicMock(),
        update_codex_session_id=MagicMock(),
    )
    resume_state_store = SimpleNamespace(record_start=MagicMock())
    steering_ledger_store = SimpleNamespace(
        mark_applied=MagicMock(),
        get_unapplied=lambda **_: [],
    )
    responses = [
        SimpleNamespace(is_error=False, text="   ", session_id=None),
        SimpleNamespace(is_error=False, text="ok after retry", session_id=None),
    ]

    async def run_claude(*_args, **_kwargs):
        return responses.pop(0)

    result = await run_provider_execution_loop(
        message=message,
        state=state,
        session=session,
        progress=progress,
        typing_task=typing_task,
        scope_key="123:main",
        chat_id=123,
        thread_id=None,
        raw_prompt="hello",
        override_text=None,
        provider_manager=provider_manager,
        session_manager=session_manager,
        resume_state_store=resume_state_store,
        steering_ledger_store=steering_ledger_store,
        logger=MagicMock(),
        current_model_label_fn=lambda *_: "sonnet",
        is_codex_family_cli_fn=lambda cli: bool(cli and cli.startswith("codex")),
        find_provider_cli_fn=lambda _cli: "/usr/bin/claude",
        as_text_fn=lambda text: text or "",
        worklog_subprocess_env_fn=lambda env, **_: env,
        codex_model_arg_fn=lambda *_: None,
        run_codex_with_retries_fn=AsyncMock(),
        run_claude_fn=run_claude,
        extract_requested_tools_fn=lambda _text: [],
        inject_tool_request_fn=lambda prompt, _tool: prompt,
        build_steering_patch_fn=lambda prompt, _events: prompt,
        has_high_risk_conflict_fn=lambda _events: False,
    )

    assert result.final_response.text == "ok after retry"
    assert result.provider_attempts == 2
    message.answer.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_provider_execution_loop_falls_back_after_empty_retry():
    message = SimpleNamespace(
        chat=SimpleNamespace(id=123),
        message_id=80,
        answer=AsyncMock(),
    )
    state = SimpleNamespace(cancel_requested=False, process_handle=None, reset_requested=False)
    session = SimpleNamespace(claude_session_id=None, codex_session_id=None)
    progress = SimpleNamespace(report_tool=AsyncMock())
    typing_task = asyncio.create_task(asyncio.sleep(3600))

    primary = SimpleNamespace(name="primary", cli="claude", resume_arg=None)
    fallback = SimpleNamespace(name="fallback", cli="claude", resume_arg=None)

    advanced = {"used": False}

    def advance(_scope):
        if advanced["used"]:
            return None
        advanced["used"] = True
        return fallback

    provider_manager = SimpleNamespace(
        get_provider=lambda _scope: primary,
        reset=lambda _scope: primary,
        advance=advance,
        subprocess_env=lambda _provider: {},
        is_rate_limit_error=lambda _text: False,
    )
    session_manager = SimpleNamespace(
        set_provider=MagicMock(),
        update_session_id=MagicMock(),
        update_codex_session_id=MagicMock(),
    )
    resume_state_store = SimpleNamespace(record_start=MagicMock())
    steering_ledger_store = SimpleNamespace(
        mark_applied=MagicMock(),
        get_unapplied=lambda **_: [],
    )
    responses = [
        SimpleNamespace(is_error=False, text="", session_id=None),
        SimpleNamespace(is_error=False, text=" ", session_id=None),
        SimpleNamespace(is_error=False, text="fallback answer", session_id=None),
    ]

    async def run_claude(*_args, **_kwargs):
        return responses.pop(0)

    result = await run_provider_execution_loop(
        message=message,
        state=state,
        session=session,
        progress=progress,
        typing_task=typing_task,
        scope_key="123:main",
        chat_id=123,
        thread_id=None,
        raw_prompt="hello",
        override_text=None,
        provider_manager=provider_manager,
        session_manager=session_manager,
        resume_state_store=resume_state_store,
        steering_ledger_store=steering_ledger_store,
        logger=MagicMock(),
        current_model_label_fn=lambda *_: "sonnet",
        is_codex_family_cli_fn=lambda cli: bool(cli and cli.startswith("codex")),
        find_provider_cli_fn=lambda _cli: "/usr/bin/claude",
        as_text_fn=lambda text: text or "",
        worklog_subprocess_env_fn=lambda env, **_: env,
        codex_model_arg_fn=lambda *_: None,
        run_codex_with_retries_fn=AsyncMock(),
        run_claude_fn=run_claude,
        extract_requested_tools_fn=lambda _text: [],
        inject_tool_request_fn=lambda prompt, _tool: prompt,
        build_steering_patch_fn=lambda prompt, _events: prompt,
        has_high_risk_conflict_fn=lambda _events: False,
    )

    assert result.final_response.text == "fallback answer"
    assert result.final_provider_name == "fallback"
    assert result.provider_attempts == 3
    session_manager.set_provider.assert_called_once_with(123, "fallback", None)
    message.answer.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_provider_execution_loop_injects_sync_payload_and_marks_synced():
    message = SimpleNamespace(
        chat=SimpleNamespace(id=123),
        message_id=81,
        answer=AsyncMock(),
    )
    state = SimpleNamespace(cancel_requested=False, process_handle=None, reset_requested=False)
    session = SimpleNamespace(claude_session_id=None, codex_session_id="codex-sess")
    progress = SimpleNamespace(report_tool=AsyncMock())
    typing_task = asyncio.create_task(asyncio.sleep(3600))

    provider = SimpleNamespace(name="codex2", cli="codex2", resume_arg=None)
    provider_manager = SimpleNamespace(
        get_provider=lambda _scope: provider,
        reset=lambda _scope: provider,
        advance=lambda _scope: None,
        subprocess_env=lambda _provider: {},
        is_rate_limit_error=lambda _text: False,
    )
    session_manager = SimpleNamespace(
        set_provider=MagicMock(),
        update_session_id=MagicMock(),
        update_codex_session_id=MagicMock(),
    )
    resume_state_store = SimpleNamespace(record_start=MagicMock())
    steering_ledger_store = SimpleNamespace(
        mark_applied=MagicMock(),
        get_unapplied=lambda **_: [],
    )
    sync_cursor = SimpleNamespace(last_synced_worklog_id=1, last_synced_topic_version=1, last_injected_hash="")
    provider_sync_store = SimpleNamespace(
        get=MagicMock(return_value=sync_cursor),
        mark_synced=MagicMock(),
    )
    captured = {}

    async def run_codex(*_args, **kwargs):
        captured["override_text"] = kwargs.get("override_text")
        return SimpleNamespace(is_error=False, text="ok", session_id="new-codex-sess")

    result = await run_provider_execution_loop(
        message=message,
        state=state,
        session=session,
        progress=progress,
        typing_task=typing_task,
        scope_key="123:main",
        chat_id=123,
        thread_id=None,
        raw_prompt="hello",
        override_text=None,
        provider_manager=provider_manager,
        session_manager=session_manager,
        resume_state_store=resume_state_store,
        steering_ledger_store=steering_ledger_store,
        logger=MagicMock(),
        current_model_label_fn=lambda *_: "gpt-5-codex",
        is_codex_family_cli_fn=lambda cli: bool(cli and cli.startswith("codex")),
        find_provider_cli_fn=lambda _cli: "/usr/bin/codex2",
        as_text_fn=lambda text: text or "",
        worklog_subprocess_env_fn=lambda env, **_: env,
        codex_model_arg_fn=lambda *_: "gpt-5-codex",
        run_codex_with_retries_fn=run_codex,
        run_claude_fn=AsyncMock(),
        extract_requested_tools_fn=lambda _text: [],
        inject_tool_request_fn=lambda prompt, _tool: prompt,
        build_steering_patch_fn=lambda prompt, _events: prompt,
        has_high_risk_conflict_fn=lambda _events: False,
        provider_switch_context_sync_enabled=True,
        provider_sync_store=provider_sync_store,
        build_provider_sync_payload_fn=lambda *_: {
            "latest_topic_version": 4,
            "payload_text": "delta line",
            "payload_hash": "hash-1",
        },
        topic_state_store=SimpleNamespace(
            record_event=MagicMock(
                return_value=SimpleNamespace(topic_version=5),
            )
        ),
    )

    assert result.final_response.text == "ok"
    assert "provider_sync_delta" in captured["override_text"]
    provider_sync_store.mark_synced.assert_called_once_with(
        scope_key="123:main",
        provider_name="codex2",
        latest_topic_version=5,
        injected_hash="hash-1",
    )
