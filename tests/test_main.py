from unittest.mock import AsyncMock
from pathlib import Path

import pytest

from src import main


@pytest.mark.asyncio
async def test_send_startup_notification_sends_boot_message_only(monkeypatch) -> None:
    bot = AsyncMock()
    monkeypatch.setattr(main, "ALLOWED_USER_IDS", {12345})

    await main.send_startup_notification(bot, commit="abc12345")

    assert bot.send_message.await_count == 1
    first = bot.send_message.await_args_list[0].kwargs

    assert first["chat_id"] == 12345
    assert "Bot restarted" in first["text"]
    assert "Starting up" in first["text"]


@pytest.mark.asyncio
async def test_send_ready_notification_separate_message(monkeypatch) -> None:
    bot = AsyncMock()
    monkeypatch.setattr(main, "ALLOWED_USER_IDS", {12345})

    await main.send_ready_notification(bot)

    bot.send_message.assert_awaited_once_with(
        chat_id=12345,
        text="💬 Ready to accept messages.",
    )


@pytest.mark.asyncio
async def test_send_startup_notification_prefers_latest_scope_thread(monkeypatch) -> None:
    bot = AsyncMock()
    monkeypatch.setattr(main, "ALLOWED_USER_IDS", set())
    monkeypatch.setattr(main.bot_module, "_load_step_plan_state", lambda: {}, raising=False)
    monkeypatch.setattr(main.bot_module, "_latest_scope_target", lambda: (-100123, 77), raising=False)
    monkeypatch.setattr(main.bot_module.config, "ALLOWED_CHAT_IDS", {-100123})

    await main.send_startup_notification(bot, commit="abc12345")

    bot.send_message.assert_awaited_once()
    kwargs = bot.send_message.await_args.kwargs
    assert kwargs["chat_id"] == -100123
    assert kwargs["message_thread_id"] == 77
    assert "Bot restarted" in kwargs["text"]


def test_ensure_worklog_git_hook_configures_hooks_path(monkeypatch, tmppath) -> None:
    repo_root = tmppath / "repo"
    git_dir = repo_root / ".git"
    hooks_dir = repo_root / "git-hooks"
    hook_path = hooks_dir / "post-commit"
    git_dir.mkdir(parents=True)
    hooks_dir.mkdir(parents=True)
    hook_path.write_text("#!/usr/bin/env bash\n", encoding="utf-8")

    calls = []

    def fake_run(args, **kwargs):
        calls.append((args, kwargs))
        class Result:
            returncode = 0
            stdout = ""
        return Result()

    monkeypatch.setattr(main, "subprocess", type("SubprocessStub", (), {"run": staticmethod(fake_run)}))
    monkeypatch.setattr(main, "__file__", str(repo_root / "src" / "main.py"))

    (repo_root / "src").mkdir(parents=True, exist_ok=True)
    main.ensure_worklog_git_hook()

    assert calls
    assert calls[0][0][:6] == ["git", "-C", str(repo_root), "config", "--local", "core.hooksPath"]
    assert calls[0][0][6] == str(hooks_dir)


@pytest.mark.asyncio
async def test_initialize_runtime_skips_embedded_scheduler_when_disabled(monkeypatch, tmp_path) -> None:
    bot = AsyncMock()
    task_manager = AsyncMock()
    schedule_manager = AsyncMock()
    task_manager.start = AsyncMock()
    task_manager.add_observer = AsyncMock()
    schedule_manager.start = AsyncMock()

    monkeypatch.setattr(main, "EMBEDDED_SCHEDULER_ENABLED", False)
    monkeypatch.setattr(main, "MEMORY_DIR", tmp_path)
    monkeypatch.setattr(
        main,
        "TaskManager",
        None,
        raising=False,
    )

    class TaskManagerStub:
        def __new__(cls, *args, **kwargs):
            return task_manager

    class ScheduleManagerStub:
        def __new__(cls, *args, **kwargs):
            return schedule_manager

    import src.tasks as tasks_module
    import src.scheduler as scheduler_module

    monkeypatch.setattr(tasks_module, "TaskManager", TaskManagerStub)
    monkeypatch.setattr(scheduler_module, "ScheduleManager", ScheduleManagerStub)

    tm, sm = await main.initialize_runtime(bot)

    assert tm is task_manager
    assert sm is schedule_manager
    task_manager.start.assert_awaited_once()
    task_manager.add_observer.assert_not_called()
    schedule_manager.start.assert_not_called()


@pytest.mark.asyncio
async def test_auto_resume_step_plan_after_restart_submits_next_step(monkeypatch) -> None:
    bot = AsyncMock()
    task_mgr = AsyncMock()
    task_mgr.submit = AsyncMock(return_value="step-task-1")
    monkeypatch.setattr(main, "task_manager", task_mgr, raising=False)
    monkeypatch.setattr(main, "ALLOWED_USER_IDS", {12345})

    state = {
        "active": True,
        "restart_between_steps": True,
        "chat_id": -100123,
        "message_thread_id": 77,
        "user_id": 12345,
        "current_index": 1,
        "steps": ["/tmp/01.md", "/tmp/02.md"],
        "current_task_id": None,
        "auto_resume_blocked_until": "",
    }
    saved = {}

    monkeypatch.setattr(main.bot_module, "_load_step_plan_state", lambda: dict(state), raising=False)
    monkeypatch.setattr(main.bot_module, "_save_step_plan_state", lambda payload: saved.update(payload), raising=False)
    monkeypatch.setattr(main.bot_module, "_scope_key", lambda c, t: f"{c}:{t}", raising=False)
    monkeypatch.setattr(
        main.bot_module,
        "_scheduled_task_backend",
        lambda _session, _provider: ("sonnet", "sess-1", "claude", None),
        raising=False,
    )
    provider_stub = type("ProviderStub", (), {"name": "claude"})()
    monkeypatch.setattr(
        main.bot_module,
        "provider_manager",
        type("ProviderMgrStub", (), {"get_provider": staticmethod(lambda _scope: provider_stub)})(),
        raising=False,
    )
    monkeypatch.setattr(
        main.bot_module,
        "session_manager",
        type("SessionMgrStub", (), {"get": staticmethod(lambda _chat, _thread: object())})(),
        raising=False,
    )

    resumed = await main.auto_resume_step_plan_after_restart(bot)

    assert resumed is True
    task_mgr.submit.assert_awaited_once()
    submit_kwargs = task_mgr.submit.await_args.kwargs
    assert submit_kwargs["chat_id"] == -100123
    assert submit_kwargs["message_thread_id"] == 77
    assert "continue plan" in submit_kwargs["prompt"]
    assert "Current step file: /tmp/02.md" in submit_kwargs["prompt"]
    assert saved["current_task_id"] == "step-task-1"
    assert saved["last_error"] == ""

    # Ready notification + auto-resume status post.
    assert bot.send_message.await_count == 1
    notify_kwargs = bot.send_message.await_args.kwargs
    assert notify_kwargs["chat_id"] == -100123
    assert notify_kwargs["message_thread_id"] == 77
    assert "Auto-resumed step plan" in notify_kwargs["text"]
