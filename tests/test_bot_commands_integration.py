"""Integration tests for bot command handling contract.

These tests define the expected behavior of bot commands (/start, /new, etc.)
and message handling. These are observable user-facing behaviors.
"""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from src.bot import (
    cmd_start,
    cmd_new,
    cmd_model,
    cmd_status,
    cmd_cancel,
    handle_message,
    _ChatState,
    _get_state,
    _is_authorized,
    VALID_MODELS,
)


# ── Contract 1: Authorization checking ──────────────────────────
class TestAuthorizationChecking:
    """Only authorized users should receive responses."""

    def test_authorized_user_id_allowed(self):
        """User ID in ALLOWED_USER_IDS should be authorized."""
        from src import config as bot_config
        bot_config.ALLOWED_USER_IDS = {12345}

        assert _is_authorized(12345) is True

    def test_unauthorized_user_id_denied(self):
        """User ID not in ALLOWED_USER_IDS should be denied."""
        from src import config as bot_config
        bot_config.ALLOWED_USER_IDS = {12345}

        assert _is_authorized(99999) is False

    def test_none_user_id_denied(self):
        """None user_id should be unauthorized."""
        assert _is_authorized(None) is False

    def test_empty_allowed_set_denies_all(self):
        """Empty ALLOWED_USER_IDS should deny all users."""
        from src import config as bot_config
        bot_config.ALLOWED_USER_IDS = set()

        assert _is_authorized(12345) is False


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

    async def test_new_unauthorized_no_response(self, mock_message):
        """Unauthorized user should get no response."""
        mock_message.text = "/new"
        mock_message.from_user.id = 99999

        await cmd_new(mock_message)

        mock_message.answer.assert_not_called()


# ── Contract 4: /model command ───────────────────────────────────
@pytest.mark.asyncio
class TestModelCommand:
    """/model should switch or show current model."""

    async def test_model_with_arg_sets_model(self, mock_message):
        """Should set model when argument provided."""
        mock_message.text = "/model opus"

        await cmd_model(mock_message)

        mock_message.answer.assert_called_once()
        assert "opus" in mock_message.answer.call_args[0][0].lower()
        # Verify model was set
        from src.bot import session_manager
        assert session_manager.get(123456789).model == "opus"

    async def test_model_all_valid_models(self, mock_message):
        """All valid models should be accepted."""
        from src.bot import session_manager

        for model in VALID_MODELS:
            expected_calls = mock_message.answer.call_count
            mock_message.text = f"/model {model}"
            await cmd_model(mock_message)
            assert mock_message.answer.call_count == expected_calls + 1
            assert session_manager.get(123456789).model == model

    async def test_model_without_arg_shows_current(self, mock_message):
        """Should show current model when no argument."""
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


# ── Contract 5: /status command ─────────────────────────────────
@pytest.mark.asyncio
class TestStatusCommand:
    """/status should show current session information."""

    async def test_status_shows_session_id(self, mock_message):
        """Should show session ID or 'none' for new conversation."""
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
        state = _get_state(123456789)
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
        state = _get_state(123456789)
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
        """Should show waiting message if already processing."""
        mock_message.text = "hello"

        # Lock the chat
        state = _get_state(123456789)
        await state.lock.acquire()

        try:
            await handle_message(mock_message)

            mock_message.answer.assert_called_once()
            assert "wait" in mock_message.answer.call_args[0][0].lower()
        finally:
            state.lock.release()


# ── Contract 8: Chat state management ───────────────────────────
class TestChatStateManagement:
    """Per-chat state should be managed correctly."""

    def test_get_creates_new_state(self):
        """Getting state for new chat should create state."""
        state = _get_state(9999)

        assert isinstance(state, _ChatState)
        assert isinstance(state.lock, asyncio.Lock)
        assert state.process_handle is None
        assert state.cancel_requested is False

    def test_get_returns_same_state(self):
        """Getting state twice for same chat returns same object."""
        state1 = _get_state(8888)
        state2 = _get_state(8888)

        assert state1 is state2

    def test_different_chats_different_state(self):
        """Different chats have independent state."""
        state1 = _get_state(7777)
        state2 = _get_state(7778)

        assert state1 is not state2


# ── Contract 9: Model validation ─────────────────────────────────
class TestModelValidation:
    """Model validation constants should be correct."""

    def test_valid_models_set(self):
        """VALID_MODELS should contain expected models."""
        assert "sonnet" in VALID_MODELS
        assert "opus" in VALID_MODELS
        assert "haiku" in VALID_MODELS