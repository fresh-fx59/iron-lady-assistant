"""Integration tests for bot command handling contract.

These tests define the expected behavior of bot commands (/start, /new, etc.)
and message handling. These are observable user-facing behaviors.
"""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from src.bot import (
    cmd_start,
    cmd_new,
    cmd_model,
    cmd_status,
    cmd_memory_forget,
    cmd_memory_consolidate,
    cmd_cancel,
    cmd_selfmod_stage,
    cmd_selfmod_apply,
    cmd_schedule_every,
    cmd_schedule_daily,
    cmd_schedule_weekly,
    cmd_schedule_list,
    cmd_schedule_cancel,
    cmd_stepplan_start,
    cmd_stepplan_status,
    cmd_stepplan_stop,
    handle_message,
    _ChatState,
    _save_step_plan_state,
    _load_step_plan_state,
    _get_state,
    _is_authorized,
    _is_transient_codex_error,
    _run_codex_with_retries,
    _reset_to_commit,
    VALID_MODELS,
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

    async def test_new_cancels_active_run_immediately(self, mock_message):
        """Should cancel active run and clear queued mid-flight inputs."""
        mock_message.text = "/new"
        state = _get_state("123456789:main")
        state.lock = asyncio.Lock()
        await state.lock.acquire()
        mock_proc = AsyncMock()
        state.process_handle = {"proc": mock_proc}
        state.pending_inputs = ["extra context 1", "extra context 2"]

        try:
            await cmd_new(mock_message)
        finally:
            state.lock.release()

        mock_proc.kill.assert_called_once()
        assert state.cancel_requested is True
        assert state.pending_inputs == []
        assert "immediately" in mock_message.answer.call_args[0][0].lower()


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

    async def test_model_all_valid_models(self, mock_message):
        """All valid models should be accepted."""
        from src.bot import provider_manager, session_manager
        provider_manager.set_provider("123456789:main", "claude")

        for model in VALID_MODELS:
            expected_calls = mock_message.answer.call_count
            mock_message.text = f"/model {model}"
            await cmd_model(mock_message)
            assert mock_message.answer.call_count == expected_calls + 1
            assert session_manager.get(123456789).model == model

    async def test_model_without_arg_shows_current(self, mock_message):
        """Should show current model when no argument."""
        from src.bot import provider_manager
        provider_manager.set_provider("123456789:main", "claude")
        mock_message.text = "/model"

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
        from src.bot import provider_manager, session_manager
        provider_manager.set_provider("123456789:main", "claude")
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
        from src.bot import provider_manager, session_manager
        provider_manager.set_provider("123456789:main", "claude")
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


@pytest.mark.asyncio
class TestMemoryCommands:
    async def test_memory_forget_usage(self, mock_message):
        mock_message.text = "/memory_forget"
        await cmd_memory_forget(mock_message)
        assert "usage" in mock_message.answer.call_args[0][0].lower()

    async def test_memory_forget_no_match(self, mock_message):
        mock_message.text = "/memory_forget role"
        with patch("src.bot.memory_manager.forget_fact", return_value=False) as forget_mock:
            await cmd_memory_forget(mock_message)
        forget_mock.assert_called_once_with("role")
        assert "no facts found" in mock_message.answer.call_args[0][0].lower()

    async def test_memory_forget_success(self, mock_message):
        mock_message.text = "/memory_forget role"
        with patch("src.bot.memory_manager.forget_fact", return_value=True) as forget_mock:
            await cmd_memory_forget(mock_message)
        forget_mock.assert_called_once_with("role")
        assert "removed facts" in mock_message.answer.call_args[0][0].lower()

    async def test_memory_consolidate_reports_stats(self, mock_message):
        mock_message.text = "/memory_consolidate"
        with patch(
            "src.bot.memory_manager.consolidate_facts",
            return_value={"before": 10, "after": 8, "removed": 2},
        ) as consolidate_mock:
            await cmd_memory_consolidate(mock_message)
        consolidate_mock.assert_called_once()
        msg = mock_message.answer.call_args[0][0].lower()
        assert "before" in msg and "after" in msg and "removed" in msg

    async def test_memory_commands_unauthorized_no_response(self, mock_message):
        mock_message.from_user.id = 99999
        mock_message.text = "/memory_forget role"
        await cmd_memory_forget(mock_message)
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

    async def test_schedule_list_shows_empty(self, mock_message):
        mock_message.text = "/schedule_list"
        with patch("src.bot.schedule_manager") as sched_mock:
            sched_mock.list_for_chat = AsyncMock(return_value=[])
            await cmd_schedule_list(mock_message)
        assert "no recurring schedules" in mock_message.answer.call_args[0][0].lower()

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


class TestRollbackResetSafety:
    def test_reset_to_commit_refuses_dirty_tree(self, tmppath: Path):
        repo = tmppath / "repo"
        repo.mkdir()

        with (
            patch("src.bot._repo_root", return_value=repo),
            patch("src.bot.subprocess.run") as run_mock,
        ):
            run_mock.side_effect = [
                type("Result", (), {"returncode": 0, "stdout": "", "stderr": ""})(),  # verify
                type("Result", (), {"returncode": 0, "stdout": " M src/bot.py\n", "stderr": ""})(),  # status
            ]

            ok, details = _reset_to_commit("abc123")

        assert ok is False
        assert "uncommitted changes" in details.lower()
        assert run_mock.call_count == 2

    def test_reset_to_commit_creates_recovery_branch_before_reset(self, tmppath: Path):
        repo = tmppath / "repo"
        repo.mkdir()

        with (
            patch("src.bot._repo_root", return_value=repo),
            patch("src.bot.subprocess.run") as run_mock,
        ):
            run_mock.side_effect = [
                type("Result", (), {"returncode": 0, "stdout": "", "stderr": ""})(),  # verify
                type("Result", (), {"returncode": 0, "stdout": "", "stderr": ""})(),  # status
                type("Result", (), {"returncode": 0, "stdout": "", "stderr": ""})(),  # branch
                type("Result", (), {"returncode": 0, "stdout": "HEAD is now at abc123 test\n", "stderr": ""})(),  # reset
            ]

            ok, details = _reset_to_commit("abc123")

        assert ok is True
        assert "HEAD is now at abc123 test" in details
        assert run_mock.call_args_list[2].args[0][:4] == ["git", "-C", str(repo), "branch"]
        assert run_mock.call_args_list[3].args[0] == ["git", "-C", str(repo), "reset", "--hard", "abc123"]


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
        """Should queue additional context if already processing."""
        mock_message.text = "hello"

        # Lock the chat
        state = _get_state("123456789:main")
        await state.lock.acquire()

        try:
            await handle_message(mock_message)

            mock_message.answer.assert_called_once()
            assert "extra context" in mock_message.answer.call_args[0][0].lower()
            assert state.pending_inputs == ["hello"]
        finally:
            state.lock.release()

    async def test_new_during_active_run_prevents_stale_session_restore(self, mock_message):
        """A stale in-flight completion must not restore session after /new."""
        from src.bot import session_manager, provider_manager

        run_started = asyncio.Event()
        release_run = asyncio.Event()
        stale_response = type("obj", (object,), {
            "text": "stale reply",
            "session_id": "sess-stale",
            "is_error": False,
            "cost_usd": 0.001,
            "duration_ms": 1,
            "num_turns": 1,
        })()

        async def delayed_run(*args, **kwargs):
            run_started.set()
            await release_run.wait()
            return stale_response

        provider_manager.set_provider("123456789:main", "claude")
        with (
            patch("src.bot._run_claude", new=AsyncMock(side_effect=delayed_run)),
            patch("src.bot._keep_typing", new=AsyncMock()),
        ):
            mock_message.text = "hello"
            task = asyncio.create_task(handle_message(mock_message))
            await run_started.wait()

            mock_message.text = "/new"
            await cmd_new(mock_message)
            release_run.set()
            await task

        session = session_manager.get(123456789)
        assert session.claude_session_id is None


@pytest.mark.asyncio
class TestStepPlanCommands:
    async def test_stepplan_start_queues_first_step(self, mock_message, tmppath, monkeypatch):
        plan_dir = tmppath / "plan"
        plan_dir.mkdir(parents=True, exist_ok=True)
        (plan_dir / "01 - First.md").write_text("first step", encoding="utf-8")
        (plan_dir / "02 - Second.md").write_text("second step", encoding="utf-8")
        mock_message.text = f"/stepplan_start {plan_dir}"

        manager = AsyncMock()
        manager.submit = AsyncMock(return_value="task-123")
        monkeypatch.setattr("src.bot.task_manager", manager)

        await cmd_stepplan_start(mock_message)

        manager.submit.assert_awaited_once()
        state = _load_step_plan_state()
        assert state["active"] is True
        assert state["current_index"] == 0
        assert state["current_task_id"] == "task-123"
        assert len(state["steps"]) == 2

    async def test_stepplan_status_shows_state(self, mock_message):
        mock_message.text = "/stepplan_status"
        _save_step_plan_state(
            {
                "active": True,
                "name": "Test Plan",
                "steps": ["/tmp/01 - X.md"],
                "current_index": 0,
                "current_task_id": "abc",
            }
        )

        await cmd_stepplan_status(mock_message)

        mock_message.answer.assert_called_once()
        text = mock_message.answer.call_args[0][0]
        assert "Step Plan Status" in text
        assert "Test Plan" in text

    async def test_stepplan_stop_deactivates_and_cancels_running_task(self, mock_message, monkeypatch):
        mock_message.text = "/stepplan_stop"
        manager = AsyncMock()
        manager.cancel = AsyncMock(return_value=True)
        monkeypatch.setattr("src.bot.task_manager", manager)
        _save_step_plan_state(
            {
                "active": True,
                "chat_id": 123456789,
                "current_task_id": "task-running",
            }
        )

        await cmd_stepplan_stop(mock_message)

        state = _load_step_plan_state()
        assert state["active"] is False
        assert not state["current_task_id"]
        manager.cancel.assert_awaited_once_with("task-running")


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
        assert state.reset_generation == 0
        assert state.pending_inputs == []

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

        state = _ChatState(lock=asyncio.Lock(), process_handle=None, cancel_requested=False)
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
