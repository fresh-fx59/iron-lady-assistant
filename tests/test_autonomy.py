from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from src.autonomy import AutonomyEngine, LearningJournal
from src.tasks import BackgroundTask, TaskStatus


def _mk_task(*, status: TaskStatus, task_id: str, chat_id: int = 123, error: str = "") -> BackgroundTask:
    now = datetime.now(timezone.utc)
    return BackgroundTask(
        id=task_id,
        chat_id=chat_id,
        user_id=chat_id,
        prompt="investigate failures and propose minimal fix",
        model="sonnet",
        session_id=None,
        status=status,
        created_at=now,
        started_at=now,
        completed_at=now,
        response="ok" if status == TaskStatus.COMPLETED else None,
        error=error,
        cost_usd=0.01,
        duration_ms=1000,
        num_turns=1,
        task=None,
    )


def test_learning_journal_records_and_queries_failures(tmp_path: Path) -> None:
    journal = LearningJournal(tmp_path / "learning.db")
    failed = _mk_task(status=TaskStatus.FAILED, task_id="a1", error="timeout")
    completed = _mk_task(status=TaskStatus.COMPLETED, task_id="a2")

    journal.record_task_outcome(failed)
    journal.record_task_outcome(completed)

    failures = journal.recent_failures(chat_id=123, window_minutes=60)
    assert len(failures) == 1
    assert failures[0].task_id == "a1"
    assert failures[0].error == "timeout"


@pytest.mark.asyncio
async def test_autonomy_engine_alerts_on_repeated_failures(tmp_path: Path) -> None:
    bot = AsyncMock()
    memory = Mock()
    journal = LearningJournal(tmp_path / "learning.db")
    engine = AutonomyEngine(
        bot=bot,
        memory_manager=memory,
        journal=journal,
        failure_threshold=2,
        failure_window_minutes=60,
        alert_cooldown_minutes=30,
    )

    await engine.on_task_finished(_mk_task(status=TaskStatus.FAILED, task_id="f1", error="e1"))
    await engine.on_task_finished(_mk_task(status=TaskStatus.FAILED, task_id="f2", error="e2"))

    assert bot.send_message.await_count == 1
    memory.add_episode.assert_called()


@pytest.mark.asyncio
async def test_autonomy_engine_no_alert_when_disabled(tmp_path: Path) -> None:
    bot = AsyncMock()
    memory = Mock()
    journal = LearningJournal(tmp_path / "learning.db")
    engine = AutonomyEngine(
        bot=bot,
        memory_manager=memory,
        journal=journal,
        proactive_enabled=False,
        failure_threshold=1,
    )

    await engine.on_task_finished(_mk_task(status=TaskStatus.FAILED, task_id="f1", error="boom"))
    bot.send_message.assert_not_called()
    memory.add_episode.assert_called_once()
