"""Integration tests for bot command handling contract.

These tests define the expected behavior of bot commands (/start, /new, etc.)
and message handling. These are observable user-facing behaviors.
"""

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from aiogram.types import FSInputFile
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter

from src.lifecycle_queue import LifecycleQueueStore
from src.bot import (
    cb_model_switch,
    cb_provider_switch,
    cmd_start,
    cmd_new,
    cmd_model,
    cmd_status,
    cmd_gmail_connect,
    cmd_cancel,
    cmd_selfmod_stage,
    cmd_selfmod_apply,
    cmd_schedule_every,
    cmd_schedule_daily,
    cmd_schedule_weekly,
    cmd_schedule_list,
    cmd_schedule_history,
    cmd_schedule_cancel,
    handle_message,
    handle_voice,
    _ChatState,
    _get_state,
    _command_args,
    _is_authorized,
    _is_transient_codex_error,
    _reflect,
    _run_codex_with_retries,
    _send_media_reply,
    _worklog_subprocess_env,
    VALID_MODELS,
    _answer_text_with_retry,
)


# ── Contract 1: Authorization checking ──────────────────────────
class TestAuthorizationChecking:
    """Only authorized users should receive responses."""

    def test_authorized_user_id_allowed(self):
        """User ID in ALLOWED_USER_IDS should be authorized."""
        from src import config as bot_config
        bot_config.ALLOWED_USER_IDS = {12345}
        bot_config.ALLOWED_CHAT_IDS = set()

        assert _is_authorized(12345) is True

    def test_unauthorized_user_id_denied(self):
        """User ID not in ALLOWED_USER_IDS should be denied."""
        from src import config as bot_config
        bot_config.ALLOWED_USER_IDS = {12345}
        bot_config.ALLOWED_CHAT_IDS = set()

        assert _is_authorized(99999) is False

    def test_none_user_id_denied(self):
        """None user_id should be unauthorized."""
        from src import config as bot_config
        bot_config.ALLOWED_USER_IDS = set()
        bot_config.ALLOWED_CHAT_IDS = set()
        assert _is_authorized(None) is False

    def test_empty_allowed_set_denies_all(self):
        """Empty ALLOWED_USER_IDS should deny all users."""
        from src import config as bot_config
        bot_config.ALLOWED_USER_IDS = set()
        bot_config.ALLOWED_CHAT_IDS = set()

        assert _is_authorized(12345) is False

    def test_authorized_chat_id_allowed(self):
        """Allowed chat ID should pass even without user ID."""
        from src import config as bot_config
        bot_config.ALLOWED_USER_IDS = set()
        bot_config.ALLOWED_CHAT_IDS = {-1001234567890}

        assert _is_authorized(None, -1001234567890) is True


@pytest.mark.asyncio
async def test_handle_message_queues_request_while_lifecycle_is_draining(mock_message, monkeypatch) -> None:
    queued: list[dict[str, object]] = []

    async def fake_compose(_message, _override_text=None):
        return "queued raw prompt"

    store = type(
        "LifecycleStoreStub",
        (),
        {
            "is_draining": staticmethod(lambda: True),
            "enqueue_turn": staticmethod(lambda **kwargs: queued.append(kwargs) or 1),
        },
    )()

    monkeypatch.setattr("src.bot.lifecycle_store", store)
    monkeypatch.setattr("src.bot._compose_incoming_prompt", fake_compose)
    monkeypatch.setattr("src.bot._build_augmented_prompt", lambda prompt: f"aug::{prompt}")

    await handle_message(mock_message)

    mock_message.answer.assert_awaited_once()
    assert "queued" in mock_message.answer.await_args.args[0].lower()
    assert queued[0]["scope_key"] == "123456789:main"
    assert queued[0]["prompt"] == "queued raw prompt"
    assert queued[0]["prompt_format"] == "raw"


@pytest.mark.asyncio
async def test_handle_message_ignores_passive_chat_without_explicit_target(mock_message, monkeypatch) -> None:
    from src import config as bot_config

    monkeypatch.setattr(bot_config, "PASSIVE_CHAT_IDS", {-1003019299921, -1003305897502})
    mock_message.chat.id = -1003019299921
    mock_message.text = "проверка"

    delegated = False

    async def fake_handle_text_message(*args, **kwargs):
        nonlocal delegated
        delegated = True

    monkeypatch.setattr("src.bot._message_media_handlers.handle_text_message", fake_handle_text_message)

    await handle_message(mock_message)

    assert delegated is False
    mock_message.answer.assert_not_called()


@pytest.mark.asyncio
async def test_handle_message_allows_explicit_bot_mention_in_passive_chat(mock_message, monkeypatch) -> None:
    from src import config as bot_config

    monkeypatch.setattr(bot_config, "PASSIVE_CHAT_IDS", {-1003305897502})
    mock_message.chat.id = -1003305897502
    mock_message.text = "@iron_lady_assistant_bot ответь"
    mock_message.bot.username = "iron_lady_assistant_bot"

    delegated = False

    async def fake_handle_text_message(*args, **kwargs):
        nonlocal delegated
        delegated = True

    monkeypatch.setattr("src.bot._message_media_handlers.handle_text_message", fake_handle_text_message)

    await handle_message(mock_message)

    assert delegated is True


# ── Contract 2: /start command ────────────────────────────────────
@pytest.mark.asyncio
class TestStartCommand:
    """/start should show welcome message with version."""

    async def test_start_shows_welcome(self, mock_message):
        """Should send welcome message with current version."""
        mock_message.text = "/start"

        await cmd_start(mock_message)

        mock_message.answer.assert_called_once()
        call_args = mock_message.answer.call_args
        # Should include version
        assert "v" in call_args[0][0]
        # Should include commands list
        assert "/new" in call_args[0][0]
        assert "/model" in call_args[0][0]
        assert "/status" in call_args[0][0]
        assert "/gmail_connect" in call_args[0][0]
        assert "/gmail_status" in call_args[0][0]
        assert "/cancel" in call_args[0][0]

    async def test_start_unauthorized_no_response(self, mock_message):
        """Unauthorized user should get no response."""
        mock_message.text = "/start"
        mock_message.from_user.id = 99999  # Unauthorized

        await cmd_start(mock_message)

        mock_message.answer.assert_not_called()


# ── Contract 3: /new command ─────────────────────────────────────
@pytest.mark.asyncio
class TestNewCommand:
    """/new should clear the conversation session."""

    async def test_new_clears_session_id(self, mock_message):
        """Should clear the session_id for the chat."""
        mock_message.text = "/new"
        from src.bot import session_manager
        session_manager.update_session_id(123456789, "old-session")

        await cmd_new(mock_message)

        mock_message.answer.assert_called_once()
        session = session_manager.get(123456789)
        assert session.claude_session_id is None

    async def test_new_confirms_action(self, mock_message):
        """Should send confirmation message."""
        mock_message.text = "/new"

        await cmd_new(mock_message)

        mock_message.answer.assert_called_once()
        assert "cleared" in mock_message.answer.call_args[0][0].lower()

    async def test_new_reflects_codex_session(self, mock_message, monkeypatch):
        """Codex-backed conversations should still be summarized on /new."""
        mock_message.text = "/new"
        from src.bot import cmd_new, provider_manager, session_manager

        monkeypatch.setenv("DISABLE_REFLECTION", "0")
        session_manager.update_codex_session_id(123456789, "codex-session")
        provider_manager.set_provider("123456789:main", "codex")

        reflect_calls: list[tuple[int, str, str]] = []

        async def fake_reflect(chat_id, session, provider):
            reflect_calls.append((chat_id, session.codex_session_id, provider.name))

        monkeypatch.setattr("src.bot._reflect", fake_reflect)

        scheduled = []

        def fake_create_task(coro):
            scheduled.append(coro)
            return coro

        monkeypatch.setattr("src.bot.asyncio.create_task", fake_create_task)

        await cmd_new(mock_message)
        await scheduled[0]

        assert reflect_calls == [(123456789, "codex-session", "codex")]
        session = session_manager.get(123456789)
        assert session.codex_session_id is None

    async def test_new_with_bot_mention_confirms_action(self, mock_message):
        """Bot mention variant should behave like /new."""
        mock_message.text = "/new@iron_lady_assistant_bot"

        await cmd_new(mock_message)

        mock_message.answer.assert_called_once()
        assert "cleared" in mock_message.answer.call_args[0][0].lower()

    async def test_new_cancels_active_run_before_reset(self, mock_message):
        """An active run should be cancelled instead of surviving across /new."""
        mock_message.text = "/new"
        state = _get_state("123456789:main")
        await state.lock.acquire()
        proc = AsyncMock()
        state.process_handle = {"proc": proc}

        try:
            await cmd_new(mock_message)
        finally:
            if state.lock.locked():
                state.lock.release()

        proc.kill.assert_awaited_once()
        assert state.cancel_requested is True
        assert state.reset_requested is True
        assert "reset requested" in mock_message.answer.call_args[0][0].lower()

    async def test_new_restores_persisted_provider_for_scope(self, mock_message):
        """If runtime provider drifted, /new should restore persisted provider selection."""
        from src.bot import provider_manager, session_manager

        mock_message.text = "/new"
        scope_key = "123456789:main"
        session_manager.set_provider(123456789, "claude")
        provider_manager.set_provider(scope_key, "codex")

        await cmd_new(mock_message)

        assert provider_manager.get_provider(scope_key).name == "claude"

    async def test_new_persists_provider_when_missing_in_session(self, mock_message):
        """When provider wasn't saved yet, /new should persist active scope provider."""
        from src.bot import provider_manager, session_manager

        mock_message.text = "/new"
        scope_key = "123456789:main"
        provider_manager.set_provider(scope_key, "claude")

        await cmd_new(mock_message)

        assert session_manager.get(123456789).provider == "claude"


@pytest.mark.asyncio
class TestReflectionProviderSelection:
    async def test_reflect_uses_codex_stream_for_codex_provider(self, monkeypatch):
        from src import bridge
        from src.bot import provider_manager, session_manager

        provider = provider_manager.set_provider("123456789:main", "codex")
        session = session_manager.get(123456789)
        session.codex_session_id = "codex-session"

        captured = {}

        async def fake_stream_codex_message(**kwargs):
            captured.update(kwargs)
            yield bridge.StreamEvent(
                event_type=bridge.StreamEventType.RESULT,
                response=bridge.ClaudeResponse(
                    text='{"summary":"done","topics":["codex"],"decisions":[],"entities":[]}',
                    session_id="codex-session",
                    is_error=False,
                    cost_usd=0,
                ),
            )

        monkeypatch.setattr("src.bot.bridge.stream_codex_message", fake_stream_codex_message)
        monkeypatch.setattr("src.bot.bridge.stream_message", AsyncMock())

        added = []

        def fake_add_episode(**kwargs):
            added.append(kwargs)

        monkeypatch.setattr("src.bot.memory_manager.add_episode", fake_add_episode)

        await _reflect(123456789, session, provider)

        assert captured["session_id"] == "codex-session"
        assert captured["cli_name"] == "codex"
        assert added[0]["summary"] == "done"
        assert added[0]["scope_key"] == "123456789:main"
        assert added[0]["provider"] == "codex"
        assert added[0]["session_type"] == "codex"
        assert added[0]["session_id"] == "codex-session"

    async def test_reflect_uses_provider_env_for_claude_compatible_provider(self, monkeypatch):
        from src import bridge
        from src.bot import provider_manager, session_manager

        provider = provider_manager.set_provider("123456789:main", "glm4.7")
        session = session_manager.get(123456789)
        session.claude_session_id = "glm-session"

        captured = {}

        async def fake_stream_message(**kwargs):
            captured.update(kwargs)
            yield bridge.StreamEvent(
                event_type=bridge.StreamEventType.RESULT,
                response=bridge.ClaudeResponse(
                    text='{"summary":"done","topics":["glm"],"decisions":[],"entities":[]}',
                    session_id="glm-session",
                    is_error=False,
                    cost_usd=0,
                ),
            )

        monkeypatch.setattr("src.bot.bridge.stream_message", fake_stream_message)
        monkeypatch.setattr("src.bot.bridge.stream_codex_message", AsyncMock())

        added = []

        def fake_add_episode(**kwargs):
            added.append(kwargs)

        monkeypatch.setattr("src.bot.memory_manager.add_episode", fake_add_episode)

        await _reflect(123456789, session, provider)

        assert captured["session_id"] == "glm-session"
        assert captured["model"] == "haiku"
        assert captured["subprocess_env"]["ANTHROPIC_BASE_URL"] == "http://0.0.0.0:4000"
        assert added[0]["topics"] == ["glm"]
        assert added[0]["provider"] == "glm4.7"
        assert added[0]["session_type"] == "claude"
        assert added[0]["session_id"] == "glm-session"

    async def test_new_unauthorized_no_response(self, mock_message):
        """Unauthorized user should get no response."""
        mock_message.text = "/new"
        mock_message.from_user.id = 99999

        await cmd_new(mock_message)

        mock_message.answer.assert_not_called()


@pytest.mark.asyncio
async def test_busy_message_after_new_is_not_treated_as_follow_up(mock_message, monkeypatch):
    """Fresh messages after /new should not be appended to the old run."""
    state = _get_state("123456789:main")
    await state.lock.acquire()
    state.reset_requested = True
    monkeypatch.setattr("src.bot._touch_thread_context", lambda *args, **kwargs: None)
    monkeypatch.setattr("src.bot.f08_advisory.submit_chat_turn", lambda **kwargs: None)
    mock_message.answer.reset_mock()

    try:
        await handle_message(mock_message)
    finally:
        if state.lock.locked():
            state.lock.release()

    mock_message.answer.assert_awaited_once()
    reply_text = mock_message.answer.await_args.args[0]
    assert "still stopping after /new" in reply_text.lower()


def test_worklog_subprocess_env_includes_thread_scope_for_parallel_topics() -> None:
    from src.sessions import ChatSession

    provider = type("ProviderStub", (), {"cli": "codex", "name": "codex"})()
    session = ChatSession(
        codex_session_id="codex-sess-7",
        chat_id=123456789,
        message_thread_id=77,
        topic_label="Parallel work",
        topic_started_at="2026-03-07T12:00:00+00:00",
        last_activity_at="2026-03-07T12:30:00+00:00",
    )

    env = _worklog_subprocess_env(
        {"BASE": "1"},
        chat_id=123456789,
        message_thread_id=77,
        provider=provider,
        session=session,
    )

    assert env["ILA_WORKLOG_SCOPE_KEY"] == "123456789:77"
    assert env["ILA_WORKLOG_MESSAGE_THREAD_ID"] == "77"
    assert env["ILA_WORKLOG_SESSION_ID"] == "codex-sess-7"
    assert env["ILA_WORKLOG_PROVIDER"] == "codex"


def test_touch_thread_context_does_not_overwrite_existing_topic_label_with_regular_message(mock_message) -> None:
    from src.bot import _touch_thread_context, session_manager

    mock_message.message_thread_id = 77
    mock_message.text = "Regular follow-up inside the topic"
    mock_message.forum_topic_created = None
    mock_message.forum_topic_edited = None
    session_manager.touch_thread(123456789, 77, topic_label="Real Topic", replace_topic_label=True)

    _touch_thread_context(mock_message)

    assert session_manager.get(123456789, 77).topic_label == "Real Topic"


def test_touch_thread_context_updates_topic_label_from_explicit_topic_edit(mock_message) -> None:
    from src.bot import _touch_thread_context, session_manager

    mock_message.message_thread_id = 77
    mock_message.text = None
    mock_message.forum_topic_created = None
    mock_message.forum_topic_edited = type("TopicEdit", (), {"name": "Renamed Topic"})()
    session_manager.touch_thread(123456789, 77, topic_label="Old Topic", replace_topic_label=True)

    _touch_thread_context(mock_message)

    assert session_manager.get(123456789, 77).topic_label == "Renamed Topic"


# ── Contract 4: /model command ───────────────────────────────────
@pytest.mark.asyncio
class TestModelCommand:
    """/model should switch or show current model."""

    async def test_model_with_arg_sets_model(self, mock_message):
        """Should set model when argument provided."""
        from src.bot import provider_manager
        provider_manager.set_provider("123456789:main", "claude")
        mock_message.text = "/model opus"

        await cmd_model(mock_message)

        mock_message.answer.assert_called_once()
        assert "opus" in mock_message.answer.call_args[0][0].lower()
        # Verify model was set
        from src.bot import session_manager
        assert session_manager.get(123456789).model == "opus"

    async def test_model_with_bot_mention_sets_model(self, mock_message):
        """Mentioned command should still parse the model argument."""
        from src.bot import provider_manager
        from src.bot import session_manager
        provider_manager.set_provider("123456789:main", "claude")
        mock_message.text = "/model@iron_lady_assistant_bot opus"

        await cmd_model(mock_message)

        mock_message.answer.assert_called_once()
        assert session_manager.get(123456789).model == "opus"

    async def test_model_all_valid_models(self, mock_message):
        """All valid models should be accepted."""
        from src.bot import provider_manager
        from src.bot import session_manager
        provider_manager.set_provider("123456789:main", "claude")

        for model in VALID_MODELS:
            expected_calls = mock_message.answer.call_count
            mock_message.text = f"/model {model}"
            await cmd_model(mock_message)
            assert mock_message.answer.call_count == expected_calls + 1
            assert session_manager.get(123456789).model == model

    async def test_model_with_arg_sets_codex_model(self, mock_message):
        """Should set codex model when Codex-family provider is active."""
        from src.bot import provider_manager
        from src.bot import session_manager
        provider_manager.set_provider("123456789:main", "codex")
        mock_message.text = "/model gpt-5.4"

        await cmd_model(mock_message)

        mock_message.answer.assert_called_once()
        assert "gpt-5.4" in mock_message.answer.call_args[0][0].lower()
        assert session_manager.get(123456789).codex_model == "gpt-5.4"

    async def test_model_without_arg_shows_current(self, mock_message):
        """Should show current model when no argument."""
        from src.bot import provider_manager
        provider_manager.set_provider("123456789:main", "claude")
        mock_message.text = "/model"
        from src.bot import session_manager

        await cmd_model(mock_message)

        mock_message.answer.assert_called_once()
        msg = mock_message.answer.call_args[0][0]
        assert "current" in msg.lower()
        assert "sonnet" in msg.lower()  # Default

    async def test_model_invalid_rejected(self, mock_message):
        """Should reject invalid model names."""
        mock_message.text = "/model invalid"

        await cmd_model(mock_message)

        mock_message.answer.assert_called_once()
        assert "invalid" in mock_message.answer.call_args[0][0].lower()

    async def test_model_unauthorized_no_response(self, mock_message):
        """Unauthorized user should get no response."""
        mock_message.text = "/model opus"
        mock_message.from_user.id = 99999

        await cmd_model(mock_message)

        mock_message.answer.assert_not_called()


@pytest.mark.asyncio
class TestModelAndProviderCallbacks:
    async def test_model_callback_ignores_unchanged_edit_error(self, mock_message):
        from src.bot import provider_manager
        from src.bot import session_manager

        provider_manager.set_provider("123456789:main", "codex")
        session_manager.set_codex_model(123456789, "gpt-5.4")

        callback = AsyncMock()
        callback.data = "model:gpt-5.4"
        callback.from_user.id = 123456789
        callback.message = mock_message
        callback.answer = AsyncMock()
        callback.message.edit_text = AsyncMock(
            side_effect=Exception("Telegram server says - Bad Request: message is not modified")
        )

        await cb_model_switch(callback)

        callback.message.edit_text.assert_called_once()
        callback.answer.assert_called_once()
        assert session_manager.get(123456789).codex_model == "gpt-5.4"

    async def test_provider_callback_ignores_unchanged_edit_error(self, mock_message):
        from src.bot import provider_manager
        from src.bot import session_manager

        provider_manager.set_provider("123456789:main", "codex")
        session_manager.set_provider(123456789, "codex")

        callback = AsyncMock()
        callback.data = "provider:codex"
        callback.from_user.id = 123456789
        callback.message = mock_message
        callback.answer = AsyncMock()
        callback.message.edit_text = AsyncMock(
            side_effect=Exception("Telegram server says - Bad Request: message is not modified")
        )

        await cb_provider_switch(callback)

        callback.message.edit_text.assert_called_once()
        callback.answer.assert_called_once_with("Switched to codex")
        assert session_manager.get(123456789).provider == "codex"

    async def test_model_callback_still_raises_other_edit_errors(self, mock_message):
        from src.bot import provider_manager

        provider_manager.set_provider("123456789:main", "codex")

        callback = AsyncMock()
        callback.data = "model:gpt-5.4"
        callback.from_user.id = 123456789
        callback.message = mock_message
        callback.answer = AsyncMock()
        callback.message.edit_text = AsyncMock(side_effect=RuntimeError("network broke"))

        with pytest.raises(RuntimeError, match="network broke"):
            await cb_model_switch(callback)


class TestCommandArgs:
    """Command argument parsing should ignore bot mentions."""

    def test_command_args_without_args_returns_empty(self, mock_message):
        mock_message.text = "/new@iron_lady_assistant_bot"

        assert _command_args(mock_message) == ""

    def test_command_args_with_bot_mention_returns_only_args(self, mock_message):
        mock_message.text = "/schedule_every@iron_lady_assistant_bot 15 check backlog"

        assert _command_args(mock_message) == "15 check backlog"


# ── Contract 5: /status command ─────────────────────────────────
@pytest.mark.asyncio
class TestStatusCommand:
    """/status should show current session information."""

    async def test_status_shows_session_id(self, mock_message):
        """Should show session ID or 'none' for new conversation."""
        from src.bot import provider_manager
        provider_manager.set_provider("123456789:main", "claude")
        mock_message.text = "/status"
        from src.bot import session_manager
        session_manager.update_session_id(123456789, "sess-123")

        await cmd_status(mock_message)

        mock_message.answer.assert_called_once()
        msg = mock_message.answer.call_args[0][0]
        assert "sess-123" in msg

    async def test_status_shows_none_for_new_conversation(self, mock_message):
        """Should show 'none' when no session set."""
        mock_message.text = "/status"
        from src.bot import session_manager
        # Ensure no session
        session_manager.new_conversation(123456789)

        await cmd_status(mock_message)

        mock_message.answer.assert_called_once()
        msg = mock_message.answer.call_args[0][0]
        assert "none" in msg.lower()

    async def test_status_shows_model(self, mock_message):
        """Should show current model."""
        from src.bot import provider_manager
        provider_manager.set_provider("123456789:main", "claude")
        mock_message.text = "/status"
        from src.bot import session_manager
        session_manager.set_model(123456789, "opus")

        await cmd_status(mock_message)

        mock_message.answer.assert_called_once()
        msg = mock_message.answer.call_args[0][0]
        assert "opus" in msg.lower()

    async def test_status_shows_version(self, mock_message):
        """Should show version."""
        mock_message.text = "/status"

        await cmd_status(mock_message)

        mock_message.answer.assert_called_once()
        msg = mock_message.answer.call_args[0][0]
        assert "version" in msg.lower()

    async def test_status_unauthorized_no_response(self, mock_message):
        """Unauthorized user should get no response."""
        mock_message.text = "/status"
        mock_message.from_user.id = 99999

        await cmd_status(mock_message)

        mock_message.answer.assert_not_called()


# ── Contract 6: /cancel command ───────────────────────────────────
@pytest.mark.asyncio
class TestCancelCommand:
    """/cancel should kill running process if any."""

    async def test_cancel_kills_running_process(self, mock_message):
        """Should kill process and set cancel_requested."""
        mock_message.text = "/cancel"
        from src.bot import session_manager

        # Set up state with running process
        state = _get_state("123456789:main")
        state.lock = asyncio.Lock()
        await state.lock.acquire()
        mock_proc = AsyncMock()
        state.process_handle = {"proc": mock_proc}
        state.cancel_requested = False

        await cmd_cancel(mock_message)

        mock_proc.kill.assert_called_once()
        assert state.cancel_requested is True
        # Release lock
        state.lock.release()

    async def test_cancel_with_no_process_shows_message(self, mock_message):
        """Should show message when nothing running."""
        mock_message.text = "/cancel"

        await cmd_cancel(mock_message)

        mock_message.answer.assert_called_once()
        assert "nothing" in mock_message.answer.call_args[0][0].lower()

    async def test_cancel_when_locked_but_no_handle(self, mock_message):
        """Should handle case where locked but no proc handle."""
        mock_message.text = "/cancel"
        state = _get_state("123456789:main")
        state.lock = asyncio.Lock()
        await state.lock.acquire()
        state.process_handle = None

        await cmd_cancel(mock_message)

        mock_message.answer.assert_called_once()
        state.lock.release()

    async def test_cancel_unauthorized_no_response(self, mock_message):
        """Unauthorized user should get no response."""
        mock_message.text = "/cancel"
        mock_message.from_user.id = 99999

        await cmd_cancel(mock_message)

        mock_message.answer.assert_not_called()


@pytest.mark.asyncio
class TestSelfModApplyCommand:
    """/selfmod_apply should run admin-only sandbox apply workflow."""

    async def test_selfmod_apply_requires_admin(self, mock_message):
        mock_message.text = "/selfmod_apply tools_plugin.py"
        mock_message.from_user.id = 99999

        await cmd_selfmod_apply(mock_message)

        mock_message.answer.assert_called_once()
        assert "admin-only" in mock_message.answer.call_args[0][0]

    async def test_selfmod_apply_runs_workflow(self, mock_message):
        mock_message.text = "/selfmod_apply tools_plugin.py tests/test_context_plugins.py"
        with (
            patch("src.bot.self_mod_manager.apply_candidate") as apply_mock,
            patch("src.bot.ToolRegistry") as registry_mock,
            patch("src.bot.ContextPluginRegistry") as context_registry_mock,
        ):
            apply_mock.return_value.ok = True
            apply_mock.return_value.message = "Applied and hot-reloaded src.plugins.tools_plugin"
            apply_mock.return_value.validation_output = "ok"

            await cmd_selfmod_apply(mock_message)

        assert mock_message.answer.call_count == 2
        assert "Applying sandbox candidate" in mock_message.answer.call_args_list[0][0][0]
        assert "succeeded" in mock_message.answer.call_args_list[1][0][0]
        apply_mock.assert_called_once()
        registry_mock.assert_called_once()
        context_registry_mock.assert_called_once()


@pytest.mark.asyncio
class TestSelfModStageCommand:
    """/selfmod_stage should stage code into sandbox."""

    async def test_selfmod_stage_requires_admin(self, mock_message):
        mock_message.text = "/selfmod_stage tools_plugin.py\nprint('x')"
        mock_message.from_user.id = 99999

        await cmd_selfmod_stage(mock_message)

        mock_message.answer.assert_called_once()
        assert "admin-only" in mock_message.answer.call_args[0][0]

    async def test_selfmod_stage_requires_body(self, mock_message):
        mock_message.text = "/selfmod_stage tools_plugin.py"
        await cmd_selfmod_stage(mock_message)
        assert "provide plugin code" in mock_message.answer.call_args[0][0].lower()

    async def test_selfmod_stage_success(self, mock_message):
        mock_message.text = "/selfmod_stage tools_plugin.py\n```python\nX = 1\n```"
        with patch("src.bot.self_mod_manager.stage_plugin") as stage_mock:
            stage_mock.return_value = "/tmp/sandbox/plugins/tools_plugin.py"
            await cmd_selfmod_stage(mock_message)

        mock_message.answer.assert_called_once()
        assert "staged plugin candidate" in mock_message.answer.call_args[0][0].lower()
        stage_mock.assert_called_once()


@pytest.mark.asyncio
class TestScheduleCommands:
    async def test_schedule_every_creates_schedule(self, mock_message):
        mock_message.text = "/schedule_every 15 check backlog"
        with patch("src.bot.schedule_manager") as sched_mock:
            sched_mock.create_every = AsyncMock(return_value="abcd1234-1234")
            await cmd_schedule_every(mock_message)

        assert mock_message.answer.call_count == 1
        assert "created" in mock_message.answer.call_args[0][0].lower()
        sched_mock.create_every.assert_called_once()

    async def test_schedule_every_uses_active_provider_backend(self, mock_message):
        mock_message.text = "/schedule_every 15 check backlog"
        provider = type("ProviderLike", (), {"cli": "codex2", "resume_arg": "resume", "model": "gpt-5-codex", "models": ["gpt-5-codex"]})()
        with patch("src.bot.schedule_manager") as sched_mock, patch("src.bot._current_provider", return_value=provider):
            sched_mock.create_every = AsyncMock(return_value="abcd1234-1234")
            await cmd_schedule_every(mock_message)

        kwargs = sched_mock.create_every.await_args.kwargs
        assert kwargs["provider_cli"] == "codex2"
        assert kwargs["resume_arg"] == "resume"
        assert kwargs["model"] == "gpt-5-codex"

    async def test_schedule_list_shows_empty(self, mock_message):
        mock_message.text = "/schedule_list"
        with patch("src.bot.schedule_manager") as sched_mock:
            sched_mock.list_for_chat = AsyncMock(return_value=[])
            await cmd_schedule_list(mock_message)
        assert "no recurring schedules" in mock_message.answer.call_args[0][0].lower()

    async def test_schedule_list_shows_active_run(self, mock_message):
        mock_message.text = "/schedule_list"
        schedule = type(
            "ScheduleLike",
            (),
            {
                "id": "abcd1234-1234",
                "schedule_type": "interval",
                "daily_time": None,
                "timezone_name": None,
                "weekly_day": None,
                "interval_minutes": 15,
                "next_run_at": datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc),
                "current_status": "running",
                "current_planned_for": datetime(2026, 3, 10, 11, 45, tzinfo=timezone.utc),
                "current_submitted_at": datetime(2026, 3, 10, 11, 45, tzinfo=timezone.utc),
                "current_started_at": datetime(2026, 3, 10, 11, 46, tzinfo=timezone.utc),
                "prompt": "check backlog and summarize status",
            },
        )()
        latest_run = type(
            "RunLike",
            (),
            {
                "status": "completed",
                "planned_for": datetime(2026, 3, 10, 11, 30, tzinfo=timezone.utc),
                "started_at": datetime(2026, 3, 10, 11, 31, tzinfo=timezone.utc),
            },
        )()
        with patch("src.bot.schedule_manager") as sched_mock:
            sched_mock.list_for_chat = AsyncMock(return_value=[schedule])
            sched_mock.latest_runs_by_schedule = AsyncMock(return_value={"abcd1234-1234": latest_run})
            await cmd_schedule_list(mock_message)

        answer_text = mock_message.answer.call_args[0][0].lower()
        assert "active: running" in answer_text
        assert "last: completed" in answer_text

    async def test_schedule_daily_invalid_time(self, mock_message):
        mock_message.text = "/schedule_daily 9:00 check backlog"
        with patch("src.bot.schedule_manager") as sched_mock:
            await cmd_schedule_daily(mock_message)
            sched_mock.create_daily.assert_not_called()
        assert "hh:mm" in mock_message.answer.call_args[0][0].lower()

    async def test_schedule_daily_creates_schedule(self, mock_message):
        mock_message.text = "/schedule_daily 09:00 check backlog"
        with patch("src.bot.schedule_manager") as sched_mock:
            sched_mock.create_daily = AsyncMock(return_value="abcd1234-1234")
            await cmd_schedule_daily(mock_message)

        assert mock_message.answer.call_count == 1
        assert "daily schedule created" in mock_message.answer.call_args[0][0].lower()
        sched_mock.create_daily.assert_called_once()

    async def test_schedule_weekly_invalid_day(self, mock_message):
        mock_message.text = "/schedule_weekly foo 09:00 check backlog"
        with patch("src.bot.schedule_manager") as sched_mock:
            await cmd_schedule_weekly(mock_message)
            sched_mock.create_weekly.assert_not_called()
        assert "day must be one of" in mock_message.answer.call_args[0][0].lower()

    async def test_schedule_weekly_creates_schedule(self, mock_message):
        mock_message.text = "/schedule_weekly mon 09:00 check backlog"
        with patch("src.bot.schedule_manager") as sched_mock:
            sched_mock.create_weekly = AsyncMock(return_value="abcd1234-1234")
            await cmd_schedule_weekly(mock_message)

        assert mock_message.answer.call_count == 1
        assert "weekly schedule created" in mock_message.answer.call_args[0][0].lower()
        sched_mock.create_weekly.assert_called_once()

    async def test_schedule_cancel_not_found(self, mock_message):
        mock_message.text = "/schedule_cancel deadbeef"
        with patch("src.bot.schedule_manager") as sched_mock:
            sched_mock.list_for_chat = AsyncMock(return_value=[])
            await cmd_schedule_cancel(mock_message)
        assert "not found" in mock_message.answer.call_args[0][0].lower()

    async def test_schedule_history_shows_empty(self, mock_message):
        mock_message.text = "/schedule_history"
        with patch("src.bot.schedule_manager") as sched_mock:
            sched_mock.list_runs_for_chat = AsyncMock(return_value=[])
            await cmd_schedule_history(mock_message)
        assert "no scheduled job history" in mock_message.answer.call_args[0][0].lower()

    async def test_schedule_history_resolves_inactive_schedule_by_short_id(self, mock_message):
        mock_message.text = "/schedule_history deadbeef"
        run = type(
            "RunLike",
            (),
            {
                "schedule_id": "deadbeef-1234",
                "status": "failed_recovered",
                "planned_for": datetime(2026, 3, 10, 11, 30, tzinfo=timezone.utc),
                "started_at": None,
                "completed_at": datetime(2026, 3, 10, 11, 31, tzinfo=timezone.utc),
                "background_task_id": None,
                "error_text": "Scheduler restarted before task completion",
                "response_preview": None,
            },
        )()
        with patch("src.bot.schedule_manager") as sched_mock:
            sched_mock.find_schedule_id_for_chat = AsyncMock(return_value="deadbeef-1234")
            sched_mock.list_runs_for_chat = AsyncMock(return_value=[run])
            await cmd_schedule_history(mock_message)

        answer_text = mock_message.answer.call_args[0][0].lower()
        assert "failed after restart" in answer_text
        sched_mock.find_schedule_id_for_chat.assert_called_once()

    async def test_schedule_history_shows_full_error_text(self, mock_message):
        mock_message.text = "/schedule_history deadbeef"
        long_error = "error-start " + ("x" * 5000) + " error-end"
        run = type(
            "RunLike",
            (),
            {
                "schedule_id": "deadbeef-1234",
                "status": "failed",
                "planned_for": datetime(2026, 3, 10, 11, 30, tzinfo=timezone.utc),
                "started_at": datetime(2026, 3, 10, 11, 30, 5, tzinfo=timezone.utc),
                "completed_at": datetime(2026, 3, 10, 11, 31, tzinfo=timezone.utc),
                "background_task_id": "01234567-89ab-cdef-0123-456789abcdef",
                "error_text": long_error,
                "response_preview": None,
            },
        )()
        with patch("src.bot.schedule_manager") as sched_mock:
            sched_mock.find_schedule_id_for_chat = AsyncMock(return_value="deadbeef-1234")
            sched_mock.list_runs_for_chat = AsyncMock(return_value=[run])
            await cmd_schedule_history(mock_message)

        sent_text = "\n".join(call.args[0] for call in mock_message.answer.call_args_list)
        assert "error-start" in sent_text
        assert "error-end" in sent_text
        assert sent_text.count("error-start") == 1


# ── Contract 7: Message handling ─────────────────────────────────
@pytest.mark.asyncio
class TestMessageHandling:
    """Regular messages should trigger Claude interaction."""

    async def test_message_unauthorized_no_response(self, mock_message):
        """Unauthorized messages should be ignored."""
        mock_message.text = "hello"
        mock_message.from_user.id = 99999

        await handle_message(mock_message)

        mock_message.answer.assert_not_called()

    async def test_when_busy_shows_wait_message(self, mock_message):
        """Should enqueue follow-up steering if already processing."""
        mock_message.text = "hello"

        # Lock the chat
        state = _get_state("123456789:main")
        await state.lock.acquire()

        try:
            await handle_message(mock_message)

            mock_message.answer.assert_called_once()
            assert "follow-up" in mock_message.answer.call_args[0][0].lower()
        finally:
            state.lock.release()

    async def test_midflight_steering_triggers_continuation(self, mock_message):
        """Unapplied steering should cause another continuation turn before final reply."""
        from src import bridge
        from src.bot import provider_manager, steering_ledger_store
        from src.features.state_store import SteeringEvent

        response1 = bridge.ClaudeResponse(
            text="Initial answer",
            session_id="sess-1",
            is_error=False,
            cost_usd=0.0,
            duration_ms=0,
            num_turns=0,
        )
        response2 = bridge.ClaudeResponse(
            text="Steered answer",
            session_id="sess-1",
            is_error=False,
            cost_usd=0.0,
            duration_ms=0,
            num_turns=0,
        )

        provider_manager.set_provider("123456789:main", "claude")
        calls = {"count": 0}

        async def fake_run_claude(
            message,
            state,
            session,
            progress,
            env,
            override_text=None,
            observed_tools=None,
        ):
            calls["count"] += 1
            if calls["count"] == 1:
                steering_ledger_store.append(
                    scope_key="123456789:main",
                    event=SteeringEvent(
                        event_id="evt-1",
                        created_at="2026-03-07T00:00:00+00:00",
                        source_message_id="2",
                        event_type="clarify",
                        text="Use tests only",
                        intent_patch="clarify: Use tests only",
                        conflict_flags=[],
                    ),
                )
                return response1
            return response2

        with patch("src.bot._run_claude", new=AsyncMock(side_effect=fake_run_claude)) as run_mock:
            await handle_message(mock_message)

        assert run_mock.await_count == 2
        all_answers = [call.args[0] for call in mock_message.answer.await_args_list if call.args]
        assert any("Steered answer" in text for text in all_answers)

    async def test_html_send_failure_still_delivers_plain_fallback(self, mock_message):
        """Plain fallback must not be suppressed if the HTML send never succeeded."""
        from src import bridge
        from src.bot import provider_manager

        response = bridge.ClaudeResponse(
            text="Hello <world>",
            session_id="sess-1",
            is_error=False,
            cost_usd=0.0,
            duration_ms=0,
            num_turns=0,
        )

        provider_manager.set_provider("123456789:main", "claude")
        mock_message.answer = AsyncMock(
            side_effect=[
                Exception("html parse failure"),
                None,
            ]
        )

        with patch("src.bot._run_claude", new=AsyncMock(return_value=response)):
            await handle_message(mock_message)

        assert mock_message.answer.await_count == 2
        assert mock_message.answer.await_args_list[0].kwargs.get("parse_mode") == "HTML"
        assert mock_message.answer.await_args_list[1].args[0] == "Hello &lt;world&gt;"


@pytest.mark.asyncio
class TestVoiceHandling:
    async def test_handle_message_logs_incoming_metadata(self, mock_message, monkeypatch, caplog):
        handle_inner = AsyncMock()
        monkeypatch.setattr("src.bot._handle_message_inner", handle_inner)
        mock_message.message_id = 321
        mock_message.message_thread_id = 77
        mock_message.text = "hello world"

        with caplog.at_level(logging.INFO, logger="src.bot"):
            await handle_message(mock_message)

        assert "Incoming text message: chat=123456789 thread=77 message=321" in caplog.text
        assert "Entering handle_message: chat=123456789 thread=77 message=321" in caplog.text
        handle_inner.assert_awaited_once_with(mock_message)

    async def test_handle_voice_shows_transcription_progress_before_message_processing(
        self,
        mock_message,
        monkeypatch,
    ):
        mock_message.voice = AsyncMock()
        mock_message.voice.file_id = "voice-file"
        mock_message.voice.duration = 7
        mock_message.bot.get_file = AsyncMock(return_value=type("File", (), {"file_path": "voice/path.oga"})())
        mock_message.bot.download_file = AsyncMock()

        async def slow_transcribe(_path):
            await asyncio.sleep(0.03)
            return "hello world"

        monkeypatch.setattr("src.bot.transcribe.is_available", lambda: True)
        monkeypatch.setattr("src.bot.transcribe.transcribe", slow_transcribe)
        monkeypatch.setattr("src.bot._VOICE_TRANSCRIPTION_PROGRESS_INTERVAL", 0.01)
        handle_inner = AsyncMock()
        monkeypatch.setattr("src.bot._handle_message_inner", handle_inner)

        await handle_voice(mock_message)

        assert mock_message.bot.send_message.await_count >= 1
        send_texts = [call.kwargs["text"] for call in mock_message.bot.send_message.await_args_list]
        assert any("Transcribing voice message" in text for text in send_texts)
        assert mock_message.bot.send_chat_action.await_count >= 1
        mock_message.bot.delete_message.assert_awaited_once_with(
            chat_id=123456789,
            message_id=123,
        )
        transcription_summary = next(
            (
                call
                for call in mock_message.answer.await_args_list
                if call.kwargs.get("parse_mode") == "HTML"
                and "Voice message transcribed" in call.args[0]
            ),
            None,
        )
        assert transcription_summary is not None
        assert "Transcription time:" in transcription_summary.args[0]
        handle_inner.assert_awaited_once()

    async def test_handle_voice_logs_incoming_metadata(self, mock_message, monkeypatch, caplog):
        mock_message.voice = AsyncMock()
        mock_message.voice.file_id = "voice-file"
        mock_message.voice.duration = 7
        mock_message.message_id = 654
        mock_message.message_thread_id = 88
        mock_message.bot.get_file = AsyncMock(return_value=type("File", (), {"file_path": "voice/path.oga"})())
        mock_message.bot.download_file = AsyncMock()

        monkeypatch.setattr("src.bot.transcribe.is_available", lambda: True)
        monkeypatch.setattr("src.bot.transcribe.transcribe", AsyncMock(return_value="hello world"))
        monkeypatch.setattr("src.bot._handle_message_inner", AsyncMock())

        with caplog.at_level(logging.INFO, logger="src.bot"):
            await handle_voice(mock_message)

        assert "Incoming voice message: chat=123456789 thread=88 message=654" in caplog.text
        assert "voice_duration=7" in caplog.text
        assert "Entering handle_voice: chat=123456789 thread=88 message=654" in caplog.text
        assert "Voice pipeline timings: chat=123456789 thread=88 message=654 voice_duration_s=7" in caplog.text

    async def test_handle_voice_retries_transcription_progress_after_retry_after(
        self,
        mock_message,
        monkeypatch,
    ):
        mock_message.voice = AsyncMock()
        mock_message.voice.file_id = "voice-file"
        mock_message.voice.duration = 7
        mock_message.bot.get_file = AsyncMock(return_value=type("File", (), {"file_path": "voice/path.oga"})())
        mock_message.bot.download_file = AsyncMock()
        mock_message.bot.send_message.side_effect = [
            TelegramRetryAfter(AsyncMock(), "retry later", 0),
            type("SentMessage", (), {"message_id": 321})(),
        ]

        async def slow_transcribe(_path):
            await asyncio.sleep(0.02)
            return "hello world"

        monkeypatch.setattr("src.bot.transcribe.is_available", lambda: True)
        monkeypatch.setattr("src.bot.transcribe.transcribe", slow_transcribe)
        handle_inner = AsyncMock()
        monkeypatch.setattr("src.bot._handle_message_inner", handle_inner)

        await handle_voice(mock_message)

        assert mock_message.bot.send_message.await_count >= 2
        transcription_summary = next(
            (
                call
                for call in mock_message.answer.await_args_list
                if call.kwargs.get("parse_mode") == "HTML"
                and "Voice message transcribed" in call.args[0]
            ),
            None,
        )
        assert transcription_summary is not None
        handle_inner.assert_awaited_once()

    async def test_handle_voice_marks_transcription_as_active_lifecycle_work(
        self,
        mock_message,
        monkeypatch,
        tmp_path,
    ):
        mock_message.voice = AsyncMock()
        mock_message.voice.file_id = "voice-file"
        mock_message.voice.duration = 7
        mock_message.message_id = 654
        mock_message.message_thread_id = 88
        mock_message.bot.get_file = AsyncMock(return_value=type("File", (), {"file_path": "voice/path.oga"})())
        mock_message.bot.download_file = AsyncMock()

        store = LifecycleQueueStore(tmp_path / "lifecycle.db")
        transcription_started = asyncio.Event()
        allow_transcription_finish = asyncio.Event()

        async def blocking_transcribe(_path):
            transcription_started.set()
            await allow_transcription_finish.wait()
            return "hello world"

        monkeypatch.setattr("src.bot.lifecycle_store", store)
        monkeypatch.setattr("src.bot.transcribe.is_available", lambda: True)
        monkeypatch.setattr("src.bot.transcribe.transcribe", blocking_transcribe)
        monkeypatch.setattr("src.bot._handle_message_inner", AsyncMock())

        voice_task = asyncio.create_task(handle_voice(mock_message))
        await asyncio.wait_for(transcription_started.wait(), timeout=1)
        await asyncio.sleep(0)

        assert store.active_scope_count() == 1

        allow_transcription_finish.set()
        await voice_task

        assert store.active_scope_count() == 0

    async def test_handle_voice_does_not_send_duplicate_generic_error_on_delivery_failure(
        self,
        mock_message,
        monkeypatch,
    ):
        mock_message.voice = AsyncMock()
        mock_message.voice.file_id = "voice-file"
        mock_message.voice.duration = 7
        mock_message.bot.get_file = AsyncMock(return_value=type("File", (), {"file_path": "voice/path.oga"})())
        mock_message.bot.download_file = AsyncMock()

        monkeypatch.setattr("src.bot.transcribe.is_available", lambda: True)
        monkeypatch.setattr("src.bot.transcribe.transcribe", AsyncMock(return_value="hello world"))
        monkeypatch.setattr(
            "src.bot._handle_message_inner",
            AsyncMock(side_effect=TelegramAPIError(AsyncMock(), "Server disconnected")),
        )

        await handle_voice(mock_message)

        assert not any(
            call.args and "An internal error occurred while processing your voice message." in call.args[0]
            for call in mock_message.answer.await_args_list
        )
        assert not any(
            call.kwargs.get("parse_mode") == "HTML" and "Voice transcription failed" in call.args[0]
            for call in mock_message.answer.await_args_list
        )


@pytest.mark.asyncio
class TestAudioProgress:
    async def test_send_media_reply_shows_conversion_progress_for_voice(self, mock_message, monkeypatch):
        started_actions: list[str] = []

        async def fake_keep_chat_action(message, action):
            started_actions.append(action.value)
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                return

        monkeypatch.setattr("src.bot._keep_chat_action", fake_keep_chat_action)

        await _send_media_reply(mock_message, "/tmp/reply.ogg", audio_as_voice=True)

        mock_message.bot.send_message.assert_awaited_once()
        initial_text = mock_message.bot.send_message.await_args.kwargs["text"]
        assert "Converting audio reply" in initial_text
        assert "Elapsed:" in initial_text
        mock_message.answer_voice.assert_awaited_once()
        mock_message.answer_audio.assert_not_called()
        mock_message.bot.edit_message_text.assert_awaited_once()
        assert "Audio reply sent" in mock_message.bot.edit_message_text.await_args.kwargs["text"]
        assert "Conversion time:" in mock_message.bot.edit_message_text.await_args.kwargs["text"]
        mock_message.bot.delete_message.assert_not_called()
        mock_message.bot.edit_message_text.assert_awaited_once_with(
            chat_id=123456789,
            message_id=123,
            text=mock_message.bot.edit_message_text.await_args.kwargs["text"],
            parse_mode="HTML",
        )
        assert started_actions == ["typing"]

    async def test_send_media_reply_shows_conversion_progress_for_audio(self, mock_message, monkeypatch):
        started_actions: list[str] = []

        async def fake_keep_chat_action(message, action):
            started_actions.append(action.value)
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                return

        monkeypatch.setattr("src.bot._keep_chat_action", fake_keep_chat_action)
        monkeypatch.setattr("src.bot._AUDIO_PROGRESS_UPDATE_INTERVAL", 0.01)

        async def slow_answer_audio(*args, **kwargs):
            await asyncio.sleep(0.03)

        mock_message.answer_audio.side_effect = slow_answer_audio

        await _send_media_reply(mock_message, "/tmp/reply.wav", audio_as_voice=False)

        mock_message.bot.send_message.assert_awaited_once()
        mock_message.answer_audio.assert_awaited_once()
        mock_message.answer_voice.assert_not_called()
        assert mock_message.bot.edit_message_text.await_count >= 2
        edit_texts = [call.kwargs["text"] for call in mock_message.bot.edit_message_text.await_args_list]
        assert any("Elapsed:" in text for text in edit_texts)
        assert "Audio reply sent" in edit_texts[-1]
        assert "Conversion time:" in edit_texts[-1]
        mock_message.bot.delete_message.assert_not_called()
        last_edit = mock_message.bot.edit_message_text.await_args
        mock_message.bot.edit_message_text.assert_any_await(
            chat_id=123456789,
            message_id=123,
            text=last_edit.kwargs["text"],
            parse_mode="HTML",
        )
        assert started_actions == ["typing"]

    async def test_send_media_reply_retries_progress_update_after_retry_after(
        self,
        mock_message,
        monkeypatch,
    ):
        async def fake_keep_chat_action(message, action):
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                return

        monkeypatch.setattr("src.bot._keep_chat_action", fake_keep_chat_action)
        monkeypatch.setattr("src.bot._AUDIO_PROGRESS_UPDATE_INTERVAL", 0.01)

        async def slow_answer_audio(*args, **kwargs):
            await asyncio.sleep(0.03)

        mock_message.answer_audio.side_effect = slow_answer_audio
        mock_message.bot.edit_message_text.side_effect = [
            TelegramRetryAfter(AsyncMock(), "retry later", 0),
            None,
            None,
        ]

        await _send_media_reply(mock_message, "/tmp/reply.wav", audio_as_voice=False)

        assert mock_message.bot.edit_message_text.await_count >= 2
        edit_texts = [call.kwargs["text"] for call in mock_message.bot.edit_message_text.await_args_list]
        assert any("Elapsed:" in text for text in edit_texts)
        assert "Audio reply sent" in edit_texts[-1]

    async def test_send_media_reply_retries_finalization_after_retry_after(
        self,
        mock_message,
        monkeypatch,
    ):
        async def fake_keep_chat_action(message, action):
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                return

        monkeypatch.setattr("src.bot._keep_chat_action", fake_keep_chat_action)
        monkeypatch.setattr("src.bot._AUDIO_PROGRESS_UPDATE_INTERVAL", 1.0)
        mock_message.bot.edit_message_text.side_effect = [
            TelegramRetryAfter(AsyncMock(), "retry later", 0),
            None,
        ]

        await _send_media_reply(mock_message, "/tmp/reply.wav", audio_as_voice=False)

        assert mock_message.bot.edit_message_text.await_count == 2
        final_text = mock_message.bot.edit_message_text.await_args_list[-1].kwargs["text"]
        assert "Audio reply sent" in final_text
        assert "Conversion time:" in final_text

    async def test_send_media_reply_snapshots_local_audio_before_voice_send(
        self,
        mock_message,
        monkeypatch,
        tmp_path,
    ):
        audio_path = tmp_path / "reply.ogg"
        audio_path.write_bytes(b"voice-bytes")
        sent_paths: list[Path] = []

        async def fake_keep_chat_action(message, action):
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                return

        async def fake_answer_voice(media_input):
            assert isinstance(media_input, FSInputFile)
            sent_path = Path(media_input.path)
            sent_paths.append(sent_path)
            assert sent_path != audio_path
            assert sent_path.exists()

        monkeypatch.setattr("src.bot._keep_chat_action", fake_keep_chat_action)
        mock_message.answer_voice.side_effect = fake_answer_voice

        await _send_media_reply(mock_message, str(audio_path), audio_as_voice=True)

        assert sent_paths
        assert not sent_paths[0].exists()

    async def test_send_media_reply_retries_voice_send_after_retry_after(
        self,
        mock_message,
        monkeypatch,
    ):
        async def fake_keep_chat_action(message, action):
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                return

        monkeypatch.setattr("src.bot._keep_chat_action", fake_keep_chat_action)
        mock_message.answer_voice.side_effect = [
            TelegramRetryAfter(AsyncMock(), "retry later", 0),
            None,
        ]

        await _send_media_reply(mock_message, "/tmp/reply.ogg", audio_as_voice=True)

        assert mock_message.answer_voice.await_count == 2
        final_text = mock_message.bot.edit_message_text.await_args_list[-1].kwargs["text"]
        assert "Audio reply sent" in final_text
        assert "Conversion time:" in final_text


@pytest.mark.asyncio
class TestTelegramRetryAfterRecovery:
    async def test_answer_text_with_retry_waits_and_recovers(self, mock_message, monkeypatch):
        sleep_calls: list[float] = []

        async def fake_sleep(seconds):
            sleep_calls.append(seconds)

        monkeypatch.setattr("src.bot.asyncio.sleep", fake_sleep)
        mock_message.answer.side_effect = [
            TelegramRetryAfter(AsyncMock(), "retry later", 0),
            None,
        ]

        await _answer_text_with_retry(mock_message, "Recovered reply", parse_mode="HTML")

        assert mock_message.answer.await_count == 2
        assert sleep_calls == [0]
        mock_message.answer.assert_awaited_with("Recovered reply", parse_mode="HTML")


# ── Contract 8: Chat state management ───────────────────────────
class TestChatStateManagement:
    """Per-chat state should be managed correctly."""

    def test_get_creates_new_state(self):
        """Getting state for new chat should create state."""
        state = _get_state("9999:main")

        assert isinstance(state, _ChatState)
        assert isinstance(state.lock, asyncio.Lock)
        assert state.process_handle is None
        assert state.cancel_requested is False

    def test_get_returns_same_state(self):
        """Getting state twice for same chat returns same object."""
        state1 = _get_state("8888:main")
        state2 = _get_state("8888:main")

        assert state1 is state2

    def test_different_chats_different_state(self):
        """Different chats have independent state."""
        state1 = _get_state("7777:main")
        state2 = _get_state("7778:main")

        assert state1 is not state2


# ── Contract 9: Model validation ─────────────────────────────────
class TestModelValidation:
    """Model validation constants should be correct."""

    def test_valid_models_set(self):
        """VALID_MODELS should contain expected models."""
        assert "sonnet" in VALID_MODELS
        assert "opus" in VALID_MODELS
        assert "haiku" in VALID_MODELS


class TestCodexTransientRetries:
    def test_detects_transient_codex_stream_timeout(self):
        assert _is_transient_codex_error(
            "Reconnecting... 1/5 (stream disconnected before completion: Transport error: timeout)"
        )

    @pytest.mark.asyncio
    async def test_retries_and_recovers_on_transient_codex_error(self, mock_message):
        response_error = type("obj", (object,), {
            "text": "Reconnecting... 1/5 (stream disconnected before completion: Transport error: timeout)",
            "session_id": None,
            "is_error": True,
            "cost_usd": 0.0,
            "duration_ms": 0,
            "num_turns": 0,
        })()
        response_ok = type("obj", (object,), {
            "text": "Recovered answer",
            "session_id": "sess-ok",
            "is_error": False,
            "cost_usd": 0.0,
            "duration_ms": 0,
            "num_turns": 0,
        })()

        state = _ChatState(
            lock=asyncio.Lock(),
            process_handle=None,
            cancel_requested=False,
            reset_requested=False,
        )
        with (
            patch("src.bot._run_codex", new=AsyncMock(side_effect=[response_error, response_ok])) as run_mock,
            patch("src.bot.config.CODEX_TRANSIENT_MAX_RETRIES", 1),
            patch("src.bot.config.CODEX_TRANSIENT_RETRY_BACKOFF_SECONDS", 0),
        ):
            result = await _run_codex_with_retries(
                message=mock_message,
                state=state,
                session=object(),
                progress=AsyncMock(),
                model=None,
                session_id="sess-in",
                resume_arg=None,
                subprocess_env=None,
            )

        assert result is response_ok
        assert run_mock.await_count == 2
        # Second attempt should reset session to avoid stale resume streams
        assert run_mock.await_args_list[1].args[5] is None

    @pytest.mark.asyncio
    async def test_exhausted_transient_codex_retry_returns_user_facing_error(self, mock_message):
        response_error = type("obj", (object,), {
            "text": "Reconnecting... 5/5 (stream disconnected before completion: IO error: Connection reset by peer (os error 104))",
            "session_id": None,
            "is_error": True,
            "cost_usd": 0.0,
            "duration_ms": 0,
            "num_turns": 0,
            "cancelled": False,
            "idle_timeout": False,
        })()

        state = _ChatState(
            lock=asyncio.Lock(),
            process_handle=None,
            cancel_requested=False,
            reset_requested=False,
        )
        with (
            patch("src.bot._run_codex", new=AsyncMock(return_value=response_error)) as run_mock,
            patch("src.bot.config.CODEX_TRANSIENT_MAX_RETRIES", 0),
            patch("src.bot.config.CODEX_TRANSIENT_RETRY_BACKOFF_SECONDS", 0),
        ):
            result = await _run_codex_with_retries(
                message=mock_message,
                state=state,
                session=object(),
                progress=AsyncMock(),
                model=None,
                session_id="sess-in",
                resume_arg=None,
                subprocess_env=None,
            )

        assert run_mock.await_count == 1
        assert result is not None
        assert result.is_error
        assert "Codex stream disconnected repeatedly" in result.text
        assert "Connection reset by peer" not in result.text

    @pytest.mark.asyncio
    async def test_stale_codex_thread_retry_resets_resume_session(self, mock_message):
        response_error = type("obj", (object,), {
            "text": "Error: thread/resume failed: no rollout found for thread id 019d1c41-e3d2-7ff3-8edf-d001b6e6a567",
            "session_id": "sess-stale",
            "is_error": True,
            "cost_usd": 0.0,
            "duration_ms": 0,
            "num_turns": 0,
        })()
        response_ok = type("obj", (object,), {
            "text": "Recovered answer",
            "session_id": "sess-fresh",
            "is_error": False,
            "cost_usd": 0.0,
            "duration_ms": 0,
            "num_turns": 0,
        })()

        state = _ChatState(
            lock=asyncio.Lock(),
            process_handle=None,
            cancel_requested=False,
            reset_requested=False,
        )
        with (
            patch("src.bot._run_codex", new=AsyncMock(side_effect=[response_error, response_ok])) as run_mock,
            patch("src.bot.config.CODEX_TRANSIENT_MAX_RETRIES", 1),
            patch("src.bot.config.CODEX_TRANSIENT_RETRY_BACKOFF_SECONDS", 0),
        ):
            result = await _run_codex_with_retries(
                message=mock_message,
                state=state,
                session=object(),
                progress=AsyncMock(),
                model=None,
                session_id="sess-stale",
                resume_arg=None,
                subprocess_env=None,
            )

        assert result is response_ok
        assert run_mock.await_count == 2
        assert run_mock.await_args_list[1].args[5] is None
