import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from src.scheduler import ScheduleManager


class _StubTaskManager:
    def __init__(self) -> None:
        self.submissions: list[dict] = []

    async def submit(self, **kwargs):  # noqa: ANN003
        self.submissions.append(kwargs)
        return "task-id"


@pytest.mark.asyncio
async def test_create_and_list_schedule(tmp_path) -> None:
    manager = ScheduleManager(_StubTaskManager(), tmp_path / "schedules.db")

    sid = await manager.create_every(
        chat_id=1,
        user_id=2,
        prompt="do work",
        interval_minutes=5,
        model="sonnet",
        session_id=None,
    )

    items = await manager.list_for_chat(1)
    assert len(items) == 1
    assert items[0].id == sid
    assert items[0].schedule_type == "interval"
    assert items[0].interval_minutes == 5


@pytest.mark.asyncio
async def test_cancel_schedule(tmp_path) -> None:
    manager = ScheduleManager(_StubTaskManager(), tmp_path / "schedules.db")
    sid = await manager.create_every(
        chat_id=1,
        user_id=2,
        prompt="do work",
        interval_minutes=5,
        model="sonnet",
    )

    cancelled = await manager.cancel(sid)
    assert cancelled is True
    items = await manager.list_for_chat(1)
    assert not items


@pytest.mark.asyncio
async def test_create_daily_schedule(tmp_path) -> None:
    manager = ScheduleManager(_StubTaskManager(), tmp_path / "schedules.db")

    sid = await manager.create_daily(
        chat_id=1,
        user_id=2,
        prompt="daily report",
        daily_time="09:30",
        timezone_name="UTC",
        model="sonnet",
    )

    items = await manager.list_for_chat(1)
    assert len(items) == 1
    assert items[0].id == sid
    assert items[0].schedule_type == "daily"
    assert items[0].daily_time == "09:30"
    assert items[0].timezone_name == "UTC"


@pytest.mark.asyncio
async def test_due_schedule_submits_background_task(tmp_path) -> None:
    stub = _StubTaskManager()
    manager = ScheduleManager(stub, tmp_path / "schedules.db")
    sid = await manager.create_every(
        chat_id=10,
        user_id=20,
        prompt="scheduled prompt",
        interval_minutes=1,
        model="opus",
        session_id="sess-1",
    )

    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    await asyncio.to_thread(manager._update_next_run, sid, past)  # noqa: SLF001

    await manager._run_due_once()  # noqa: SLF001
    assert len(stub.submissions) == 1
    assert stub.submissions[0]["chat_id"] == 10
    assert stub.submissions[0]["model"] == "opus"
    assert stub.submissions[0]["provider_cli"] == "claude"
    assert stub.submissions[0]["resume_arg"] is None
    assert stub.submissions[0]["live_feedback"] is False
    assert stub.submissions[0]["notification_mode"] == "silent"
    assert "Scheduled run started" in stub.submissions[0]["feedback_title"]
    runs = await manager.list_runs_for_chat(10)
    assert len(runs) == 1
    assert runs[0].status == "submitted"
    assert runs[0].background_task_id == stub.submissions[0]["task_id"]
    schedule = (await manager.list_for_chat(10))[0]
    assert schedule.current_status == "submitted"
    assert schedule.current_background_task_id == stub.submissions[0]["task_id"]


@pytest.mark.asyncio
async def test_due_schedule_preserves_provider_runtime(tmp_path) -> None:
    stub = _StubTaskManager()
    manager = ScheduleManager(stub, tmp_path / "schedules.db")
    sid = await manager.create_every(
        chat_id=10,
        user_id=20,
        prompt="scheduled prompt",
        interval_minutes=1,
        model="gpt-5-codex",
        session_id="sess-1",
        provider_cli="codex2",
        resume_arg="resume",
    )

    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    await asyncio.to_thread(manager._update_next_run, sid, past)  # noqa: SLF001

    await manager._run_due_once()  # noqa: SLF001
    assert stub.submissions[0]["provider_cli"] == "codex2"
    assert stub.submissions[0]["resume_arg"] == "resume"
    assert stub.submissions[0]["model"] == "gpt-5-codex"


@pytest.mark.asyncio
async def test_scheduler_notifications_post_to_configured_topic(tmp_path) -> None:
    stub = _StubTaskManager()
    notifier = AsyncMock()
    manager = ScheduleManager(
        stub,
        tmp_path / "schedules.db",
        notification_bot=notifier,
        notification_chat_id=-100123,
        notification_thread_id=77,
        notify_level="all",
    )
    sid = await manager.create_every(
        chat_id=10,
        user_id=20,
        prompt="scheduled prompt",
        interval_minutes=1,
        model="opus",
    )
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    await asyncio.to_thread(manager._update_next_run, sid, past)  # noqa: SLF001

    await manager._run_due_once()  # noqa: SLF001
    started_at = datetime.now(timezone.utc)
    background_task_id = stub.submissions[0]["task_id"]
    started_task = type(
        "StartedTask",
        (),
        {
            "id": background_task_id,
            "started_at": started_at,
        },
    )()
    await manager.on_task_started(started_task)

    finished_task = type(
        "FinishedTask",
        (),
        {
            "id": background_task_id,
            "status": type("TaskStatusValue", (), {"value": "completed"})(),
            "started_at": started_at,
            "completed_at": datetime.now(timezone.utc),
            "error": None,
            "response": "report delivered",
        },
    )()
    await manager.on_task_finished(finished_task)

    assert notifier.send_message.await_count == 3
    first = notifier.send_message.await_args_list[0].kwargs
    assert first["chat_id"] == -100123
    assert first["message_thread_id"] == 77
    assert "Scheduled run submitted" in first["text"]
    assert "topic 77" not in first["text"]
    third = notifier.send_message.await_args_list[2].kwargs
    assert "Scheduled run completed" in third["text"]
    assert "report delivered" in third["text"]


@pytest.mark.asyncio
async def test_scheduler_failures_mode_suppresses_routine_success_notifications(tmp_path) -> None:
    stub = _StubTaskManager()
    notifier = AsyncMock()
    manager = ScheduleManager(
        stub,
        tmp_path / "schedules.db",
        notification_bot=notifier,
        notification_chat_id=-100123,
        notification_thread_id=77,
        notify_level="failures",
    )
    sid = await manager.create_every(
        chat_id=10,
        user_id=20,
        prompt="scheduled prompt",
        interval_minutes=1,
        model="opus",
    )
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    await asyncio.to_thread(manager._update_next_run, sid, past)  # noqa: SLF001

    await manager._run_due_once()  # noqa: SLF001
    background_task_id = stub.submissions[0]["task_id"]
    started_at = datetime.now(timezone.utc)
    started_task = type("StartedTask", (), {"id": background_task_id, "started_at": started_at})()
    await manager.on_task_started(started_task)
    finished_task = type(
        "FinishedTask",
        (),
        {
            "id": background_task_id,
            "status": type("TaskStatusValue", (), {"value": "completed"})(),
            "started_at": started_at,
            "completed_at": datetime.now(timezone.utc),
            "error": None,
            "response": "Overall status: `ok`",
        },
    )()
    await manager.on_task_finished(finished_task)

    notifier.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_scheduler_failures_mode_notifies_on_warn_and_recovery(tmp_path) -> None:
    stub = _StubTaskManager()
    notifier = AsyncMock()
    manager = ScheduleManager(
        stub,
        tmp_path / "schedules.db",
        notification_bot=notifier,
        notification_chat_id=-100123,
        notification_thread_id=77,
        notify_level="failures",
    )
    sid = await manager.create_every(
        chat_id=10,
        user_id=20,
        prompt="scheduled prompt",
        interval_minutes=1,
        model="opus",
    )
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    await asyncio.to_thread(manager._update_next_run, sid, past)  # noqa: SLF001

    await manager._run_due_once()  # noqa: SLF001
    background_task_id = stub.submissions[0]["task_id"]
    started_at = datetime.now(timezone.utc)
    started_task = type("StartedTask", (), {"id": background_task_id, "started_at": started_at})()
    await manager.on_task_started(started_task)
    warn_task = type(
        "FinishedTask",
        (),
        {
            "id": background_task_id,
            "status": type("TaskStatusValue", (), {"value": "completed"})(),
            "started_at": started_at,
            "completed_at": datetime.now(timezone.utc),
            "error": None,
            "response": "Overall status: `warn`",
        },
    )()
    await manager.on_task_finished(warn_task)

    await asyncio.to_thread(manager._update_next_run, sid, past)  # noqa: SLF001
    await manager._run_due_once()  # noqa: SLF001
    second_task_id = stub.submissions[1]["task_id"]
    second_started = datetime.now(timezone.utc)
    second_started_task = type("StartedTask", (), {"id": second_task_id, "started_at": second_started})()
    await manager.on_task_started(second_started_task)
    recovery_task = type(
        "FinishedTask",
        (),
        {
            "id": second_task_id,
            "status": type("TaskStatusValue", (), {"value": "completed"})(),
            "started_at": second_started,
            "completed_at": datetime.now(timezone.utc),
            "error": None,
            "response": "Overall status: `ok`",
        },
    )()
    await manager.on_task_finished(recovery_task)

    assert notifier.send_message.await_count == 2
    first = notifier.send_message.await_args_list[0].kwargs["text"]
    second = notifier.send_message.await_args_list[1].kwargs["text"]
    assert "Scheduled run completed" in first
    assert "warn" in first
    assert "Scheduled run completed" in second
    assert "ok" in second


@pytest.mark.asyncio
async def test_due_daily_schedule_submits_and_rolls_next_run(tmp_path) -> None:
    stub = _StubTaskManager()
    manager = ScheduleManager(stub, tmp_path / "schedules.db")
    sid = await manager.create_daily(
        chat_id=10,
        user_id=20,
        prompt="daily prompt",
        daily_time="00:00",
        timezone_name="UTC",
        model="haiku",
        session_id="sess-2",
    )
    past = (datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()
    await asyncio.to_thread(manager._update_next_run, sid, past)  # noqa: SLF001

    before = (await manager.list_for_chat(10))[0].next_run_at
    await manager._run_due_once()  # noqa: SLF001
    after = (await manager.list_for_chat(10))[0].next_run_at

    assert len(stub.submissions) == 1
    assert stub.submissions[0]["model"] == "haiku"
    assert after > before


@pytest.mark.asyncio
async def test_schedule_run_updated_when_background_task_finishes(tmp_path) -> None:
    stub = _StubTaskManager()
    manager = ScheduleManager(stub, tmp_path / "schedules.db")
    sid = await manager.create_every(
        chat_id=42,
        user_id=7,
        prompt="scheduled prompt",
        interval_minutes=1,
        model="opus",
    )
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    await asyncio.to_thread(manager._update_next_run, sid, past)  # noqa: SLF001

    await manager._run_due_once()  # noqa: SLF001
    run = (await manager.list_runs_for_chat(42))[0]
    background_task_id = stub.submissions[0]["task_id"]

    finished_task = type(
        "FinishedTask",
        (),
        {
            "id": background_task_id,
            "status": type("TaskStatusValue", (), {"value": "completed"})(),
            "started_at": datetime.now(timezone.utc),
            "completed_at": datetime.now(timezone.utc),
            "error": None,
            "response": "report delivered",
        },
    )()
    await manager.on_task_finished(finished_task)

    updated_run = (await manager.list_runs_for_chat(42, schedule_id=sid))[0]
    assert updated_run.id == run.id
    assert updated_run.status == "completed"
    assert updated_run.response_preview == "report delivered"
    schedule = (await manager.list_for_chat(42))[0]
    assert schedule.current_status is None
    assert schedule.current_run_id is None


@pytest.mark.asyncio
async def test_create_weekly_schedule(tmp_path) -> None:
    manager = ScheduleManager(_StubTaskManager(), tmp_path / "schedules.db")

    sid = await manager.create_weekly(
        chat_id=1,
        user_id=2,
        prompt="weekly report",
        weekly_day=0,
        daily_time="09:30",
        timezone_name="UTC",
        model="sonnet",
    )

    items = await manager.list_for_chat(1)
    assert len(items) == 1
    assert items[0].id == sid
    assert items[0].schedule_type == "weekly"
    assert items[0].weekly_day == 0
    assert items[0].daily_time == "09:30"


@pytest.mark.asyncio
async def test_schedule_run_marked_running_when_background_task_starts(tmp_path) -> None:
    stub = _StubTaskManager()
    manager = ScheduleManager(stub, tmp_path / "schedules.db")
    sid = await manager.create_every(
        chat_id=11,
        user_id=22,
        prompt="scheduled prompt",
        interval_minutes=1,
        model="opus",
    )
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    await asyncio.to_thread(manager._update_next_run, sid, past)  # noqa: SLF001

    await manager._run_due_once()  # noqa: SLF001
    started_at = datetime.now(timezone.utc)
    background_task_id = stub.submissions[0]["task_id"]
    started_task = type(
        "StartedTask",
        (),
        {
            "id": background_task_id,
            "started_at": started_at,
        },
    )()
    await manager.on_task_started(started_task)

    run = (await manager.list_runs_for_chat(11, schedule_id=sid))[0]
    assert run.status == "running"
    assert run.started_at == started_at
    schedule = (await manager.list_for_chat(11))[0]
    assert schedule.current_status == "running"
    assert schedule.current_started_at == started_at


@pytest.mark.asyncio
async def test_active_schedule_does_not_submit_overlap(tmp_path) -> None:
    stub = _StubTaskManager()
    manager = ScheduleManager(stub, tmp_path / "schedules.db")
    sid = await manager.create_every(
        chat_id=12,
        user_id=34,
        prompt="scheduled prompt",
        interval_minutes=1,
        model="opus",
    )
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    await asyncio.to_thread(manager._update_next_run, sid, past)  # noqa: SLF001

    await manager._run_due_once()  # noqa: SLF001
    await asyncio.to_thread(manager._update_next_run, sid, past)  # noqa: SLF001

    await manager._run_due_once()  # noqa: SLF001

    assert len(stub.submissions) == 1
    runs = await manager.list_runs_for_chat(12, schedule_id=sid)
    assert len(runs) == 1


@pytest.mark.asyncio
async def test_start_recovers_stale_active_run(tmp_path) -> None:
    stub = _StubTaskManager()
    db_path = tmp_path / "schedules.db"
    manager = ScheduleManager(stub, db_path)
    sid = await manager.create_every(
        chat_id=13,
        user_id=35,
        prompt="scheduled prompt",
        interval_minutes=1,
        model="opus",
    )
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    await asyncio.to_thread(manager._update_next_run, sid, past)  # noqa: SLF001
    await manager._run_due_once()  # noqa: SLF001

    restarted_manager = ScheduleManager(_StubTaskManager(), db_path)
    await restarted_manager.start()
    await restarted_manager.stop()

    run = (await restarted_manager.list_runs_for_chat(13, schedule_id=sid))[0]
    assert run.status == "failed_recovered"
    schedule = (await restarted_manager.list_for_chat(13))[0]
    assert schedule.current_run_id is None
    assert schedule.current_status is None
