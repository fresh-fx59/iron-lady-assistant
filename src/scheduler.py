"""Persistent recurring task scheduler."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from .tasks import TaskManager

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ScheduledTask:
    id: str
    chat_id: int
    user_id: int
    prompt: str
    schedule_type: str
    interval_minutes: int
    daily_time: str | None
    timezone_name: str | None
    weekly_day: int | None
    model: str
    session_id: str | None
    next_run_at: datetime
    created_at: datetime


class ScheduleManager:
    """Recurring task scheduler with SQLite persistence."""

    _POLL_SECONDS = 5

    def __init__(self, task_manager: TaskManager, db_path: Path) -> None:
        self._task_manager = task_manager
        self._db_path = db_path
        self._worker_task: asyncio.Task | None = None
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self._db_path)
        con.row_factory = sqlite3.Row
        return con

    def _init_db(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS scheduled_tasks (
                    id TEXT PRIMARY KEY,
                    chat_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    prompt TEXT NOT NULL,
                    interval_minutes INTEGER NOT NULL,
                    schedule_type TEXT NOT NULL DEFAULT 'interval',
                    daily_time TEXT,
                    timezone_name TEXT,
                    weekly_day INTEGER,
                    model TEXT NOT NULL,
                    session_id TEXT,
                    next_run_at TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            self._ensure_column(con, "schedule_type", "TEXT NOT NULL DEFAULT 'interval'")
            self._ensure_column(con, "daily_time", "TEXT")
            self._ensure_column(con, "timezone_name", "TEXT")
            self._ensure_column(con, "weekly_day", "INTEGER")

    @staticmethod
    def _ensure_column(con: sqlite3.Connection, name: str, definition: str) -> None:
        columns = {row[1] for row in con.execute("PRAGMA table_info(scheduled_tasks)")}
        if name not in columns:
            con.execute(f"ALTER TABLE scheduled_tasks ADD COLUMN {name} {definition}")

    async def start(self) -> None:
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(self._worker_loop(), name="schedule_worker")

    async def stop(self) -> None:
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None

    async def create_every(
        self,
        chat_id: int,
        user_id: int,
        prompt: str,
        interval_minutes: int,
        model: str,
        session_id: str | None = None,
    ) -> str:
        task_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        next_run = now + timedelta(minutes=interval_minutes)
        await asyncio.to_thread(
            self._insert_schedule,
            task_id,
            chat_id,
            user_id,
            prompt,
            interval_minutes,
            "interval",
            None,
            None,
            None,
            model,
            session_id,
            next_run.isoformat(),
            now.isoformat(),
        )
        return task_id

    async def create_daily(
        self,
        chat_id: int,
        user_id: int,
        prompt: str,
        daily_time: str,
        timezone_name: str,
        model: str,
        session_id: str | None = None,
    ) -> str:
        task_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        next_run = self._next_daily_run(daily_time=daily_time, timezone_name=timezone_name, now_utc=now)
        await asyncio.to_thread(
            self._insert_schedule,
            task_id,
            chat_id,
            user_id,
            prompt,
            0,
            "daily",
            daily_time,
            timezone_name,
            None,
            model,
            session_id,
            next_run.isoformat(),
            now.isoformat(),
        )
        return task_id

    async def create_weekly(
        self,
        chat_id: int,
        user_id: int,
        prompt: str,
        weekly_day: int,
        daily_time: str,
        timezone_name: str,
        model: str,
        session_id: str | None = None,
    ) -> str:
        task_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        next_run = self._next_weekly_run(
            weekly_day=weekly_day,
            daily_time=daily_time,
            timezone_name=timezone_name,
            now_utc=now,
        )
        await asyncio.to_thread(
            self._insert_schedule,
            task_id,
            chat_id,
            user_id,
            prompt,
            0,
            "weekly",
            daily_time,
            timezone_name,
            weekly_day,
            model,
            session_id,
            next_run.isoformat(),
            now.isoformat(),
        )
        return task_id

    def _insert_schedule(
        self,
        task_id: str,
        chat_id: int,
        user_id: int,
        prompt: str,
        interval_minutes: int,
        schedule_type: str,
        daily_time: str | None,
        timezone_name: str | None,
        weekly_day: int | None,
        model: str,
        session_id: str | None,
        next_run_at: str,
        created_at: str,
    ) -> None:
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO scheduled_tasks
                (id, chat_id, user_id, prompt, interval_minutes, schedule_type, daily_time, timezone_name, weekly_day, model, session_id, next_run_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    chat_id,
                    user_id,
                    prompt,
                    interval_minutes,
                    schedule_type,
                    daily_time,
                    timezone_name,
                    weekly_day,
                    model,
                    session_id,
                    next_run_at,
                    created_at,
                ),
            )

    async def list_for_chat(self, chat_id: int) -> list[ScheduledTask]:
        rows = await asyncio.to_thread(self._list_rows, chat_id)
        return [self._row_to_scheduled_task(row) for row in rows]

    def _list_rows(self, chat_id: int) -> list[sqlite3.Row]:
        with self._connect() as con:
            cur = con.execute(
                """
                SELECT id, chat_id, user_id, prompt, interval_minutes, model, session_id, next_run_at, created_at
                       , schedule_type, daily_time, timezone_name, weekly_day
                FROM scheduled_tasks
                WHERE chat_id = ?
                ORDER BY next_run_at ASC
                """,
                (chat_id,),
            )
            return list(cur.fetchall())

    async def cancel(self, task_id: str) -> bool:
        deleted = await asyncio.to_thread(self._delete_schedule, task_id)
        return deleted > 0

    def _delete_schedule(self, task_id: str) -> int:
        with self._connect() as con:
            cur = con.execute("DELETE FROM scheduled_tasks WHERE id = ?", (task_id,))
            return cur.rowcount

    async def _worker_loop(self) -> None:
        while True:
            await self._run_due_once()
            await asyncio.sleep(self._POLL_SECONDS)

    async def _run_due_once(self) -> None:
        due_rows = await asyncio.to_thread(self._fetch_due_rows, datetime.now(timezone.utc).isoformat())
        for row in due_rows:
            schedule = self._row_to_scheduled_task(row)
            try:
                await self._task_manager.submit(
                    chat_id=schedule.chat_id,
                    user_id=schedule.user_id,
                    prompt=schedule.prompt,
                    model=schedule.model,
                    session_id=schedule.session_id,
                )
            except Exception:
                logger.exception("Failed to submit scheduled task %s", schedule.id)
            finally:
                next_run = self._next_run_for_schedule(schedule, datetime.now(timezone.utc))
                await asyncio.to_thread(self._update_next_run, schedule.id, next_run.isoformat())

    def _fetch_due_rows(self, now_iso: str) -> list[sqlite3.Row]:
        with self._connect() as con:
            cur = con.execute(
                """
                SELECT id, chat_id, user_id, prompt, interval_minutes, model, session_id, next_run_at, created_at
                       , schedule_type, daily_time, timezone_name, weekly_day
                FROM scheduled_tasks
                WHERE next_run_at <= ?
                ORDER BY next_run_at ASC
                LIMIT 20
                """,
                (now_iso,),
            )
            return list(cur.fetchall())

    def _update_next_run(self, task_id: str, next_run_at: str) -> None:
        with self._connect() as con:
            con.execute(
                "UPDATE scheduled_tasks SET next_run_at = ? WHERE id = ?",
                (next_run_at, task_id),
            )

    @staticmethod
    def _row_to_scheduled_task(row: sqlite3.Row) -> ScheduledTask:
        return ScheduledTask(
            id=row["id"],
            chat_id=row["chat_id"],
            user_id=row["user_id"],
            prompt=row["prompt"],
            schedule_type=row["schedule_type"] or "interval",
            interval_minutes=row["interval_minutes"],
            daily_time=row["daily_time"],
            timezone_name=row["timezone_name"],
            weekly_day=row["weekly_day"],
            model=row["model"],
            session_id=row["session_id"],
            next_run_at=datetime.fromisoformat(row["next_run_at"]),
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    @staticmethod
    def _next_daily_run(daily_time: str, timezone_name: str, now_utc: datetime) -> datetime:
        tz = ZoneInfo(timezone_name)
        local_now = now_utc.astimezone(tz)
        hour_str, minute_str = daily_time.split(":")
        target = local_now.replace(
            hour=int(hour_str),
            minute=int(minute_str),
            second=0,
            microsecond=0,
        )
        if target <= local_now:
            target += timedelta(days=1)
        return target.astimezone(timezone.utc)

    def _next_run_for_schedule(self, schedule: ScheduledTask, now_utc: datetime) -> datetime:
        if (
            schedule.schedule_type == "weekly"
            and schedule.weekly_day is not None
            and schedule.daily_time
            and schedule.timezone_name
        ):
            return self._next_weekly_run(
                weekly_day=schedule.weekly_day,
                daily_time=schedule.daily_time,
                timezone_name=schedule.timezone_name,
                now_utc=now_utc,
            )
        if schedule.schedule_type == "daily" and schedule.daily_time and schedule.timezone_name:
            return self._next_daily_run(
                daily_time=schedule.daily_time,
                timezone_name=schedule.timezone_name,
                now_utc=now_utc,
            )
        interval = schedule.interval_minutes if schedule.interval_minutes > 0 else 1
        return now_utc + timedelta(minutes=interval)

    @staticmethod
    def _next_weekly_run(
        weekly_day: int,
        daily_time: str,
        timezone_name: str,
        now_utc: datetime,
    ) -> datetime:
        tz = ZoneInfo(timezone_name)
        local_now = now_utc.astimezone(tz)
        hour_str, minute_str = daily_time.split(":")
        target = local_now.replace(
            hour=int(hour_str),
            minute=int(minute_str),
            second=0,
            microsecond=0,
        )
        day_delta = (weekly_day - target.weekday()) % 7
        target = target + timedelta(days=day_delta)
        if target <= local_now:
            target += timedelta(days=7)
        return target.astimezone(timezone.utc)
