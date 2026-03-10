"""Persistent recurring task scheduler."""

from __future__ import annotations

import asyncio
import html
import logging
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from aiogram import Bot

from .tasks import BackgroundTask, TaskManager

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ScheduledTask:
    id: str
    chat_id: int
    message_thread_id: int | None
    user_id: int
    prompt: str
    schedule_type: str
    interval_minutes: int
    daily_time: str | None
    timezone_name: str | None
    weekly_day: int | None
    model: str
    session_id: str | None
    provider_cli: str
    resume_arg: str | None
    state: str
    misfire_policy: str
    current_run_id: str | None
    current_background_task_id: str | None
    current_planned_for: datetime | None
    current_submitted_at: datetime | None
    current_started_at: datetime | None
    current_status: str | None
    next_run_at: datetime
    created_at: datetime


@dataclass(frozen=True)
class ScheduleRun:
    id: str
    schedule_id: str
    chat_id: int
    message_thread_id: int | None
    background_task_id: str | None
    planned_for: datetime
    submitted_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    status: str
    error_text: str | None
    response_preview: str | None


class ScheduleManager:
    """Recurring task scheduler with SQLite persistence."""

    _POLL_SECONDS = 5

    def __init__(
        self,
        task_manager: TaskManager,
        db_path: Path,
        notification_bot: Bot | None = None,
        notification_chat_id: int | None = None,
        notification_thread_id: int | None = None,
    ) -> None:
        self._task_manager = task_manager
        self._db_path = db_path
        self._worker_task: asyncio.Task | None = None
        self._notification_bot = notification_bot
        self._notification_chat_id = notification_chat_id
        self._notification_thread_id = notification_thread_id
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
                    message_thread_id INTEGER,
                    user_id INTEGER NOT NULL,
                    prompt TEXT NOT NULL,
                    interval_minutes INTEGER NOT NULL,
                    schedule_type TEXT NOT NULL DEFAULT 'interval',
                    daily_time TEXT,
                    timezone_name TEXT,
                    weekly_day INTEGER,
                    model TEXT NOT NULL,
                    session_id TEXT,
                    provider_cli TEXT NOT NULL DEFAULT 'claude',
                    resume_arg TEXT,
                    state TEXT NOT NULL DEFAULT 'active',
                    misfire_policy TEXT NOT NULL DEFAULT 'catch_up_one',
                    current_run_id TEXT,
                    current_background_task_id TEXT,
                    current_planned_for TEXT,
                    current_submitted_at TEXT,
                    current_started_at TEXT,
                    current_status TEXT,
                    next_run_at TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            self._ensure_column(con, "schedule_type", "TEXT NOT NULL DEFAULT 'interval'")
            self._ensure_column(con, "daily_time", "TEXT")
            self._ensure_column(con, "timezone_name", "TEXT")
            self._ensure_column(con, "weekly_day", "INTEGER")
            self._ensure_column(con, "message_thread_id", "INTEGER")
            self._ensure_column(con, "provider_cli", "TEXT NOT NULL DEFAULT 'claude'")
            self._ensure_column(con, "resume_arg", "TEXT")
            self._ensure_column(con, "state", "TEXT NOT NULL DEFAULT 'active'")
            self._ensure_column(con, "misfire_policy", "TEXT NOT NULL DEFAULT 'catch_up_one'")
            self._ensure_column(con, "current_run_id", "TEXT")
            self._ensure_column(con, "current_background_task_id", "TEXT")
            self._ensure_column(con, "current_planned_for", "TEXT")
            self._ensure_column(con, "current_submitted_at", "TEXT")
            self._ensure_column(con, "current_started_at", "TEXT")
            self._ensure_column(con, "current_status", "TEXT")
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS scheduled_task_runs (
                    id TEXT PRIMARY KEY,
                    schedule_id TEXT NOT NULL,
                    chat_id INTEGER NOT NULL,
                    message_thread_id INTEGER,
                    background_task_id TEXT,
                    planned_for TEXT NOT NULL,
                    submitted_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    status TEXT NOT NULL,
                    error_text TEXT,
                    response_preview TEXT
                )
                """
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_scheduled_task_runs_schedule_time "
                "ON scheduled_task_runs(schedule_id, submitted_at DESC)"
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_scheduled_task_runs_chat_time "
                "ON scheduled_task_runs(chat_id, message_thread_id, submitted_at DESC)"
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_scheduled_task_runs_task_id "
                "ON scheduled_task_runs(background_task_id)"
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_due "
                "ON scheduled_tasks(state, current_run_id, next_run_at)"
            )

    @staticmethod
    def _ensure_column(con: sqlite3.Connection, name: str, definition: str) -> None:
        columns = {row[1] for row in con.execute("PRAGMA table_info(scheduled_tasks)")}
        if name not in columns:
            con.execute(f"ALTER TABLE scheduled_tasks ADD COLUMN {name} {definition}")

    async def start(self) -> None:
        if self._worker_task is None:
            await asyncio.to_thread(self._recover_stale_runs, datetime.now(timezone.utc).isoformat())
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
        provider_cli: str = "claude",
        resume_arg: str | None = None,
        message_thread_id: int | None = None,
    ) -> str:
        task_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        next_run = now + timedelta(minutes=interval_minutes)
        await asyncio.to_thread(
            self._insert_schedule,
            task_id,
            chat_id,
            message_thread_id,
            user_id,
            prompt,
            interval_minutes,
            "interval",
            None,
            None,
            None,
            model,
            session_id,
            provider_cli,
            resume_arg,
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
        provider_cli: str = "claude",
        resume_arg: str | None = None,
        message_thread_id: int | None = None,
    ) -> str:
        task_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        next_run = self._next_daily_run(daily_time=daily_time, timezone_name=timezone_name, now_utc=now)
        await asyncio.to_thread(
            self._insert_schedule,
            task_id,
            chat_id,
            message_thread_id,
            user_id,
            prompt,
            0,
            "daily",
            daily_time,
            timezone_name,
            None,
            model,
            session_id,
            provider_cli,
            resume_arg,
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
        provider_cli: str = "claude",
        resume_arg: str | None = None,
        message_thread_id: int | None = None,
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
            message_thread_id,
            user_id,
            prompt,
            0,
            "weekly",
            daily_time,
            timezone_name,
            weekly_day,
            model,
            session_id,
            provider_cli,
            resume_arg,
            next_run.isoformat(),
            now.isoformat(),
        )
        return task_id

    def _insert_schedule(
        self,
        task_id: str,
        chat_id: int,
        message_thread_id: int | None,
        user_id: int,
        prompt: str,
        interval_minutes: int,
        schedule_type: str,
        daily_time: str | None,
        timezone_name: str | None,
        weekly_day: int | None,
        model: str,
        session_id: str | None,
        provider_cli: str,
        resume_arg: str | None,
        next_run_at: str,
        created_at: str,
    ) -> None:
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO scheduled_tasks
                (id, chat_id, message_thread_id, user_id, prompt, interval_minutes, schedule_type, daily_time, timezone_name, weekly_day, model, session_id, provider_cli, resume_arg, next_run_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    chat_id,
                    message_thread_id,
                    user_id,
                    prompt,
                    interval_minutes,
                    schedule_type,
                    daily_time,
                    timezone_name,
                    weekly_day,
                    model,
                    session_id,
                    provider_cli,
                    resume_arg,
                    next_run_at,
                    created_at,
                ),
            )

    async def list_for_chat(
        self,
        chat_id: int,
        message_thread_id: int | None = None,
    ) -> list[ScheduledTask]:
        rows = await asyncio.to_thread(self._list_rows, chat_id, message_thread_id)
        return [self._row_to_scheduled_task(row) for row in rows]

    async def find_schedule_id_for_chat(
        self,
        chat_id: int,
        short_id: str,
        message_thread_id: int | None = None,
    ) -> str | None:
        return await asyncio.to_thread(self._find_schedule_id_row, chat_id, short_id, message_thread_id)

    def _list_rows(self, chat_id: int, message_thread_id: int | None) -> list[sqlite3.Row]:
        with self._connect() as con:
            if message_thread_id is None:
                cur = con.execute(
                    """
                    SELECT id, chat_id, message_thread_id, user_id, prompt, interval_minutes, model, session_id, provider_cli, resume_arg, next_run_at, created_at
                           , schedule_type, daily_time, timezone_name, weekly_day
                           , state, misfire_policy, current_run_id, current_background_task_id
                           , current_planned_for, current_submitted_at, current_started_at, current_status
                    FROM scheduled_tasks
                    WHERE chat_id = ? AND message_thread_id IS NULL AND state = 'active'
                    ORDER BY next_run_at ASC
                    """,
                    (chat_id,),
                )
            else:
                cur = con.execute(
                    """
                    SELECT id, chat_id, message_thread_id, user_id, prompt, interval_minutes, model, session_id, provider_cli, resume_arg, next_run_at, created_at
                           , schedule_type, daily_time, timezone_name, weekly_day
                           , state, misfire_policy, current_run_id, current_background_task_id
                           , current_planned_for, current_submitted_at, current_started_at, current_status
                    FROM scheduled_tasks
                    WHERE chat_id = ? AND message_thread_id = ? AND state = 'active'
                    ORDER BY next_run_at ASC
                    """,
                    (chat_id, message_thread_id),
                )
            return list(cur.fetchall())

    def _find_schedule_id_row(self, chat_id: int, short_id: str, message_thread_id: int | None) -> str | None:
        with self._connect() as con:
            if message_thread_id is None:
                cur = con.execute(
                    """
                    SELECT id
                    FROM scheduled_tasks
                    WHERE chat_id = ? AND message_thread_id IS NULL AND id LIKE ?
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (chat_id, f"{short_id}%"),
                )
            else:
                cur = con.execute(
                    """
                    SELECT id
                    FROM scheduled_tasks
                    WHERE chat_id = ? AND message_thread_id = ? AND id LIKE ?
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (chat_id, message_thread_id, f"{short_id}%"),
                )
            row = cur.fetchone()
            return row["id"] if row else None

    async def cancel(self, task_id: str) -> bool:
        updated = await asyncio.to_thread(self._cancel_schedule, task_id)
        return updated > 0

    def _cancel_schedule(self, task_id: str) -> int:
        with self._connect() as con:
            cur = con.execute(
                """
                UPDATE scheduled_tasks
                SET state = 'cancelled',
                    current_run_id = NULL,
                    current_background_task_id = NULL,
                    current_planned_for = NULL,
                    current_submitted_at = NULL,
                    current_started_at = NULL,
                    current_status = NULL
                WHERE id = ? AND state = 'active'
                """,
                (task_id,),
            )
            return cur.rowcount

    async def _worker_loop(self) -> None:
        while True:
            await self._run_due_once()
            await asyncio.sleep(self._POLL_SECONDS)

    async def _run_due_once(self) -> None:
        claimed_runs = await asyncio.to_thread(self._claim_due_runs, datetime.now(timezone.utc))
        for schedule, run_id, planned_for, submitted_at in claimed_runs:
            background_task_id = str(uuid.uuid4())
            try:
                await asyncio.to_thread(
                    self._mark_run_submitted,
                    schedule.id,
                    run_id,
                    background_task_id,
                )
                background_task_id = await self._task_manager.submit(
                    chat_id=schedule.chat_id,
                    message_thread_id=schedule.message_thread_id,
                    user_id=schedule.user_id,
                    prompt=schedule.prompt,
                    model=schedule.model,
                    session_id=schedule.session_id,
                    provider_cli=schedule.provider_cli,
                    resume_arg=schedule.resume_arg,
                    live_feedback=True,
                    feedback_title=self._build_schedule_feedback_title(schedule, planned_for),
                    task_id=background_task_id,
                )
                await self._notify_schedule_event(
                    (
                        "🕒 <b>Scheduled run submitted</b>\n"
                        f"<b>Schedule:</b> <code>{schedule.id[:8]}</code>\n"
                        f"<b>Planned:</b> {planned_for.astimezone().strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"<b>Target:</b> {self._format_schedule_target(schedule.chat_id, schedule.message_thread_id)}\n"
                        f"<b>Prompt:</b> {html.escape(self._preview_text(schedule.prompt, 160) or '')}"
                    )
                )
            except Exception:
                logger.exception("Failed to submit scheduled task %s", schedule.id)
                await asyncio.to_thread(
                    self._mark_run_submission_failed,
                    schedule.id,
                    run_id,
                    submitted_at.isoformat(),
                    "Failed to submit background task",
                )
                await self._notify_schedule_event(
                    (
                        "❌ <b>Scheduled run submission failed</b>\n"
                        f"<b>Schedule:</b> <code>{schedule.id[:8]}</code>\n"
                        f"<b>Planned:</b> {planned_for.astimezone().strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"<b>Target:</b> {self._format_schedule_target(schedule.chat_id, schedule.message_thread_id)}"
                    )
                )

    def _recover_stale_runs(self, recovered_at: str) -> None:
        with self._connect() as con:
            rows = list(
                con.execute(
                    """
                    SELECT current_run_id
                    FROM scheduled_tasks
                    WHERE state = 'active' AND current_run_id IS NOT NULL
                    """
                )
            )
            for row in rows:
                run_id = row["current_run_id"]
                con.execute(
                    """
                    UPDATE scheduled_task_runs
                    SET status = 'failed_recovered',
                        completed_at = COALESCE(completed_at, ?),
                        error_text = COALESCE(error_text, 'Scheduler restarted before task completion')
                    WHERE id = ? AND completed_at IS NULL
                    """,
                    (recovered_at, run_id),
                )
            con.execute(
                """
                UPDATE scheduled_tasks
                SET current_run_id = NULL,
                    current_background_task_id = NULL,
                    current_planned_for = NULL,
                    current_submitted_at = NULL,
                    current_started_at = NULL,
                    current_status = NULL
                WHERE state = 'active' AND current_run_id IS NOT NULL
                """
            )

    async def on_task_started(self, task: BackgroundTask) -> None:
        await asyncio.to_thread(
            self._mark_run_started,
            task.id,
            task.started_at.isoformat() if task.started_at else None,
        )
        run = await asyncio.to_thread(self._find_run_row_by_background_task_id, task.id)
        if run:
            planned_for = datetime.fromisoformat(run["planned_for"]).astimezone().strftime("%Y-%m-%d %H:%M:%S")
            started_at = task.started_at.astimezone().strftime("%Y-%m-%d %H:%M:%S") if task.started_at else "unknown"
            await self._notify_schedule_event(
                (
                    "▶️ <b>Scheduled run started</b>\n"
                    f"<b>Schedule:</b> <code>{run['schedule_id'][:8]}</code>\n"
                    f"<b>Planned:</b> {planned_for}\n"
                    f"<b>Started:</b> {started_at}\n"
                    f"<b>Target:</b> {self._format_schedule_target(run['chat_id'], run['message_thread_id'])}"
                )
            )

    async def on_task_finished(self, task: BackgroundTask) -> None:
        run = await asyncio.to_thread(self._find_run_row_by_background_task_id, task.id)
        await asyncio.to_thread(
            self._update_run_for_background_task,
            task.id,
            task.status.value,
            task.started_at.isoformat() if task.started_at else None,
            task.completed_at.isoformat() if task.completed_at else None,
            task.error,
            self._preview_text(task.response),
        )
        if run:
            status_value = getattr(task.status, "value", str(task.status))
            finished_at = task.completed_at.astimezone().strftime("%Y-%m-%d %H:%M:%S") if task.completed_at else "unknown"
            detail = task.error or self._preview_text(task.response, 220) or "No detail"
            planned_for = datetime.fromisoformat(run["planned_for"]).astimezone().strftime("%Y-%m-%d %H:%M:%S")
            await self._notify_schedule_event(
                (
                    f"{self._status_emoji(status_value)} <b>Scheduled run {html.escape(status_value)}</b>\n"
                    f"<b>Schedule:</b> <code>{run['schedule_id'][:8]}</code>\n"
                    f"<b>Planned:</b> {planned_for}\n"
                    f"<b>Finished:</b> {finished_at}\n"
                    f"<b>Target:</b> {self._format_schedule_target(run['chat_id'], run['message_thread_id'])}\n"
                    f"<b>Result:</b> {html.escape(detail)}"
                )
            )

    async def list_runs_for_chat(
        self,
        chat_id: int,
        message_thread_id: int | None = None,
        schedule_id: str | None = None,
        limit: int = 10,
    ) -> list[ScheduleRun]:
        rows = await asyncio.to_thread(self._list_run_rows, chat_id, message_thread_id, schedule_id, limit)
        return [self._row_to_schedule_run(row) for row in rows]

    async def latest_runs_by_schedule(self, schedule_ids: list[str]) -> dict[str, ScheduleRun]:
        if not schedule_ids:
            return {}
        rows = await asyncio.to_thread(self._latest_run_rows_by_schedule, schedule_ids)
        return {row["schedule_id"]: self._row_to_schedule_run(row) for row in rows}

    def _claim_due_runs(
        self,
        now_utc: datetime,
        limit: int = 20,
    ) -> list[tuple[ScheduledTask, str, datetime, datetime]]:
        with self._connect() as con:
            cur = con.execute(
                """
                SELECT id, chat_id, message_thread_id, user_id, prompt, interval_minutes, model, session_id, provider_cli, resume_arg, next_run_at, created_at
                       , schedule_type, daily_time, timezone_name, weekly_day
                       , state, misfire_policy, current_run_id, current_background_task_id
                       , current_planned_for, current_submitted_at, current_started_at, current_status
                FROM scheduled_tasks
                WHERE state = 'active' AND current_run_id IS NULL AND next_run_at <= ?
                ORDER BY next_run_at ASC
                LIMIT ?
                """,
                (now_utc.isoformat(), limit),
            )
            rows = list(cur.fetchall())
            claimed: list[tuple[ScheduledTask, str, datetime, datetime]] = []
            for row in rows:
                schedule = self._row_to_scheduled_task(row)
                run_id = str(uuid.uuid4())
                planned_for = schedule.next_run_at
                submitted_at = now_utc
                next_run = self._next_run_for_schedule(schedule, now_utc)
                run_inserted = con.execute(
                    """
                    INSERT INTO scheduled_task_runs
                    (id, schedule_id, chat_id, message_thread_id, background_task_id, planned_for, submitted_at, status, error_text, response_preview)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        schedule.id,
                        schedule.chat_id,
                        schedule.message_thread_id,
                        None,
                        planned_for.isoformat(),
                        submitted_at.isoformat(),
                        "queued",
                        None,
                        None,
                    ),
                )
                if run_inserted.rowcount != 1:
                    continue
                updated = con.execute(
                    """
                    UPDATE scheduled_tasks
                    SET current_run_id = ?,
                        current_background_task_id = NULL,
                        current_planned_for = ?,
                        current_submitted_at = ?,
                        current_started_at = NULL,
                        current_status = 'queued',
                        next_run_at = ?
                    WHERE id = ? AND state = 'active' AND current_run_id IS NULL AND next_run_at = ?
                    """,
                    (
                        run_id,
                        planned_for.isoformat(),
                        submitted_at.isoformat(),
                        next_run.isoformat(),
                        schedule.id,
                        planned_for.isoformat(),
                    ),
                )
                if updated.rowcount != 1:
                    con.execute("DELETE FROM scheduled_task_runs WHERE id = ?", (run_id,))
                    continue
                claimed.append((schedule, run_id, planned_for, submitted_at))
            return claimed

    def _update_next_run(self, task_id: str, next_run_at: str) -> None:
        with self._connect() as con:
            con.execute(
                "UPDATE scheduled_tasks SET next_run_at = ? WHERE id = ?",
                (next_run_at, task_id),
            )

    def _insert_run(
        self,
        run_id: str,
        schedule_id: str,
        chat_id: int,
        message_thread_id: int | None,
        background_task_id: str | None,
        planned_for: str,
        submitted_at: str,
        status: str,
        error_text: str | None,
        response_preview: str | None,
    ) -> None:
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO scheduled_task_runs
                (id, schedule_id, chat_id, message_thread_id, background_task_id, planned_for, submitted_at, status, error_text, response_preview)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    schedule_id,
                    chat_id,
                    message_thread_id,
                    background_task_id,
                    planned_for,
                    submitted_at,
                    status,
                    error_text,
                    response_preview,
                ),
            )

    def _mark_run_submitted(
        self,
        schedule_id: str,
        run_id: str,
        background_task_id: str,
    ) -> None:
        with self._connect() as con:
            con.execute(
                """
                UPDATE scheduled_task_runs
                SET background_task_id = ?, status = 'submitted'
                WHERE id = ?
                """,
                (background_task_id, run_id),
            )
            con.execute(
                """
                UPDATE scheduled_tasks
                SET current_background_task_id = ?, current_status = 'submitted'
                WHERE id = ? AND current_run_id = ?
                """,
                (background_task_id, schedule_id, run_id),
            )

    def _mark_run_submission_failed(
        self,
        schedule_id: str,
        run_id: str,
        completed_at: str,
        error_text: str,
    ) -> None:
        with self._connect() as con:
            con.execute(
                """
                UPDATE scheduled_task_runs
                SET background_task_id = NULL, status = 'submission_failed', completed_at = ?, error_text = ?
                WHERE id = ?
                """,
                (completed_at, error_text, run_id),
            )
            con.execute(
                """
                UPDATE scheduled_tasks
                SET current_run_id = NULL,
                    current_background_task_id = NULL,
                    current_planned_for = NULL,
                    current_submitted_at = NULL,
                    current_started_at = NULL,
                    current_status = NULL
                WHERE id = ? AND current_run_id = ?
                """,
                (schedule_id, run_id),
            )

    def _mark_run_started(self, background_task_id: str, started_at: str | None) -> None:
        with self._connect() as con:
            cur = con.execute(
                """
                UPDATE scheduled_task_runs
                SET status = 'running', started_at = COALESCE(?, started_at)
                WHERE background_task_id = ?
                """,
                (started_at, background_task_id),
            )
            if cur.rowcount:
                con.execute(
                    """
                    UPDATE scheduled_tasks
                    SET current_started_at = COALESCE(?, current_started_at),
                        current_status = 'running'
                    WHERE current_background_task_id = ?
                    """,
                    (started_at, background_task_id),
                )

    def _update_run_for_background_task(
        self,
        background_task_id: str,
        status: str,
        started_at: str | None,
        completed_at: str | None,
        error_text: str | None,
        response_preview: str | None,
    ) -> None:
        with self._connect() as con:
            con.execute(
                """
                UPDATE scheduled_task_runs
                SET status = ?, started_at = ?, completed_at = ?, error_text = ?, response_preview = ?
                WHERE background_task_id = ?
                """,
                (status, started_at, completed_at, error_text, response_preview, background_task_id),
            )
            con.execute(
                """
                UPDATE scheduled_tasks
                SET current_run_id = NULL,
                    current_background_task_id = NULL,
                    current_planned_for = NULL,
                    current_submitted_at = NULL,
                    current_started_at = NULL,
                    current_status = NULL
                WHERE current_background_task_id = ?
                """,
                (background_task_id,),
            )

    def _list_run_rows(
        self,
        chat_id: int,
        message_thread_id: int | None,
        schedule_id: str | None,
        limit: int,
    ) -> list[sqlite3.Row]:
        query = (
            "SELECT id, schedule_id, chat_id, message_thread_id, background_task_id, planned_for, submitted_at,"
            " started_at, completed_at, status, error_text, response_preview "
            "FROM scheduled_task_runs WHERE chat_id = ?"
        )
        params: list[object] = [chat_id]
        if message_thread_id is None:
            query += " AND message_thread_id IS NULL"
        else:
            query += " AND message_thread_id = ?"
            params.append(message_thread_id)
        if schedule_id:
            query += " AND schedule_id = ?"
            params.append(schedule_id)
        query += " ORDER BY submitted_at DESC LIMIT ?"
        params.append(limit)
        with self._connect() as con:
            cur = con.execute(query, tuple(params))
            return list(cur.fetchall())

    def _latest_run_rows_by_schedule(self, schedule_ids: list[str]) -> list[sqlite3.Row]:
        placeholders = ", ".join("?" for _ in schedule_ids)
        query = (
            "SELECT id, schedule_id, chat_id, message_thread_id, background_task_id, planned_for, submitted_at,"
            " started_at, completed_at, status, error_text, response_preview "
            "FROM scheduled_task_runs "
            f"WHERE schedule_id IN ({placeholders}) "
            "ORDER BY submitted_at DESC"
        )
        seen: set[str] = set()
        latest_rows: list[sqlite3.Row] = []
        with self._connect() as con:
            for row in con.execute(query, tuple(schedule_ids)):
                schedule_id = row["schedule_id"]
                if schedule_id in seen:
                    continue
                seen.add(schedule_id)
                latest_rows.append(row)
        return latest_rows

    def _find_run_row_by_background_task_id(self, background_task_id: str) -> sqlite3.Row | None:
        with self._connect() as con:
            return con.execute(
                """
                SELECT schedule_id, chat_id, message_thread_id, planned_for
                FROM scheduled_task_runs
                WHERE background_task_id = ?
                """,
                (background_task_id,),
            ).fetchone()

    @staticmethod
    def _row_to_scheduled_task(row: sqlite3.Row) -> ScheduledTask:
        return ScheduledTask(
            id=row["id"],
            chat_id=row["chat_id"],
            message_thread_id=row["message_thread_id"],
            user_id=row["user_id"],
            prompt=row["prompt"],
            schedule_type=row["schedule_type"] or "interval",
            interval_minutes=row["interval_minutes"],
            daily_time=row["daily_time"],
            timezone_name=row["timezone_name"],
            weekly_day=row["weekly_day"],
            model=row["model"],
            session_id=row["session_id"],
            provider_cli=row["provider_cli"] or "claude",
            resume_arg=row["resume_arg"],
            state=row["state"] or "active",
            misfire_policy=row["misfire_policy"] or "catch_up_one",
            current_run_id=row["current_run_id"],
            current_background_task_id=row["current_background_task_id"],
            current_planned_for=datetime.fromisoformat(row["current_planned_for"]) if row["current_planned_for"] else None,
            current_submitted_at=datetime.fromisoformat(row["current_submitted_at"]) if row["current_submitted_at"] else None,
            current_started_at=datetime.fromisoformat(row["current_started_at"]) if row["current_started_at"] else None,
            current_status=row["current_status"],
            next_run_at=datetime.fromisoformat(row["next_run_at"]),
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    @staticmethod
    def _row_to_schedule_run(row: sqlite3.Row) -> ScheduleRun:
        return ScheduleRun(
            id=row["id"],
            schedule_id=row["schedule_id"],
            chat_id=row["chat_id"],
            message_thread_id=row["message_thread_id"],
            background_task_id=row["background_task_id"],
            planned_for=datetime.fromisoformat(row["planned_for"]),
            submitted_at=datetime.fromisoformat(row["submitted_at"]),
            started_at=datetime.fromisoformat(row["started_at"]) if row["started_at"] else None,
            completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
            status=row["status"],
            error_text=row["error_text"],
            response_preview=row["response_preview"],
        )

    @staticmethod
    def _preview_text(text: str | None, limit: int = 280) -> str | None:
        if not text:
            return None
        compact = " ".join(text.split())
        if len(compact) <= limit:
            return compact
        return compact[: limit - 3] + "..."

    @staticmethod
    def _format_schedule_target(chat_id: int, message_thread_id: int | None) -> str:
        if message_thread_id is None:
            return f"chat {chat_id}"
        return f"chat {chat_id} / topic {message_thread_id}"

    @staticmethod
    def _status_emoji(status: str) -> str:
        return {
            "completed": "✅",
            "failed": "❌",
            "cancelled": "🚫",
            "submission_failed": "❌",
            "failed_recovered": "⚠️",
        }.get(status, "ℹ️")

    def _build_schedule_feedback_title(self, schedule: ScheduledTask, planned_for: datetime) -> str:
        return (
            "🕒 <b>Scheduled run started</b>\n"
            f"<b>Schedule:</b> <code>{schedule.id[:8]}</code>\n"
            f"<b>Planned:</b> {planned_for.astimezone().strftime('%Y-%m-%d %H:%M:%S')}"
        )

    async def _notify_schedule_event(self, text: str) -> None:
        if self._notification_bot is None or self._notification_chat_id is None:
            return
        kwargs: dict[str, Any] = {
            "chat_id": self._notification_chat_id,
            "text": text,
            "parse_mode": "HTML",
        }
        if self._notification_thread_id is not None:
            kwargs["message_thread_id"] = self._notification_thread_id
        try:
            await self._notification_bot.send_message(**kwargs)
        except Exception:
            logger.exception("Failed to send scheduler notification")

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
