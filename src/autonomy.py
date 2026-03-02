"""Autonomy hooks: self-learning journal + proactive failure escalation."""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from aiogram import Bot

from .memory import MemoryManager
from .tasks import BackgroundTask, TaskStatus

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OutcomeRecord:
    chat_id: int
    task_id: str
    status: str
    prompt_preview: str
    error: str
    timestamp: datetime


class LearningJournal:
    """Stores task outcomes for pattern detection and reflection."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self._db_path)
        con.row_factory = sqlite3.Row
        return con

    def _init_db(self) -> None:
        with self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS task_outcomes (
                    id INTEGER PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    chat_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    task_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    prompt_preview TEXT NOT NULL,
                    error TEXT NOT NULL,
                    duration_ms INTEGER NOT NULL,
                    cost_usd REAL NOT NULL,
                    num_turns INTEGER NOT NULL
                )
                """
            )

    def record_task_outcome(self, task: BackgroundTask) -> None:
        """Persist a compact outcome record."""
        finished_at = task.completed_at or datetime.now(timezone.utc)
        prompt_preview = (task.prompt or "").strip().replace("\n", " ")[:240]
        error = (task.error or "").strip()[:500]
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO task_outcomes
                (timestamp, chat_id, user_id, task_id, status, prompt_preview, error, duration_ms, cost_usd, num_turns)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    finished_at.astimezone(timezone.utc).isoformat(),
                    task.chat_id,
                    task.user_id,
                    task.id,
                    task.status.value,
                    prompt_preview,
                    error,
                    task.duration_ms,
                    task.cost_usd,
                    task.num_turns,
                ),
            )

    def recent_failures(self, chat_id: int, window_minutes: int) -> list[OutcomeRecord]:
        """Return failed outcomes within the last N minutes."""
        since = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
        with self._connect() as con:
            rows = con.execute(
                """
                SELECT chat_id, task_id, status, prompt_preview, error, timestamp
                FROM task_outcomes
                WHERE chat_id = ? AND status = ? AND timestamp >= ?
                ORDER BY timestamp DESC
                """,
                (chat_id, TaskStatus.FAILED.value, since.isoformat()),
            ).fetchall()
        return [
            OutcomeRecord(
                chat_id=row["chat_id"],
                task_id=row["task_id"],
                status=row["status"],
                prompt_preview=row["prompt_preview"],
                error=row["error"],
                timestamp=datetime.fromisoformat(row["timestamp"]),
            )
            for row in rows
        ]


class AutonomyEngine:
    """Learns from background outcomes and proactively warns on repeated failures."""

    def __init__(
        self,
        bot: Bot,
        memory_manager: MemoryManager,
        journal: LearningJournal,
        *,
        proactive_enabled: bool = True,
        failure_threshold: int = 3,
        failure_window_minutes: int = 60,
        alert_cooldown_minutes: int = 30,
    ) -> None:
        self._bot = bot
        self._memory = memory_manager
        self._journal = journal
        self._proactive_enabled = proactive_enabled
        self._failure_threshold = max(1, failure_threshold)
        self._failure_window_minutes = max(1, failure_window_minutes)
        self._alert_cooldown = timedelta(minutes=max(1, alert_cooldown_minutes))
        self._last_alert_at: dict[int, datetime] = {}

    async def on_task_finished(self, task: BackgroundTask) -> None:
        """TaskManager hook called after each terminal task state."""
        self._journal.record_task_outcome(task)
        self._store_episode(task)

        if not self._proactive_enabled or task.status != TaskStatus.FAILED:
            return

        recent = self._journal.recent_failures(
            task.chat_id,
            window_minutes=self._failure_window_minutes,
        )
        if len(recent) < self._failure_threshold:
            return

        now = datetime.now(timezone.utc)
        last_alert = self._last_alert_at.get(task.chat_id)
        if last_alert and now - last_alert < self._alert_cooldown:
            return
        self._last_alert_at[task.chat_id] = now

        await self._bot.send_message(
            chat_id=task.chat_id,
            text=(
                "⚠️ <b>Proactive alert</b>\n\n"
                f"I detected <b>{len(recent)} background task failures</b> in the last "
                f"{self._failure_window_minutes} minutes.\n\n"
                "Recommended next step:\n"
                "<code>/bg investigate recent deployment/runtime errors, "
                "identify root cause, and propose minimal fix + verification plan</code>"
            ),
            parse_mode="HTML",
        )

    def _store_episode(self, task: BackgroundTask) -> None:
        prompt_preview = (task.prompt or "").replace("\n", " ").strip()[:120]
        if task.status == TaskStatus.COMPLETED:
            summary = f"Background task completed: {prompt_preview}"
            decisions = ["Keep successful steps as reusable procedure"]
        elif task.status == TaskStatus.FAILED:
            summary = f"Background task failed: {prompt_preview}"
            decisions = [f"Error: {(task.error or 'unknown')[:120]}"]
        else:
            return

        try:
            self._memory.add_episode(
                chat_id=task.chat_id,
                summary=summary,
                topics=["background-task", task.status.value],
                decisions=decisions,
                entities=[],
            )
        except Exception:
            logger.exception("Failed to store autonomy episode for task %s", task.id)
