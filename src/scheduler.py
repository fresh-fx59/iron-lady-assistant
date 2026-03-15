"""Persistent recurring task scheduler."""

from __future__ import annotations

import asyncio
import html
import json
import logging
import re
import shlex
import sqlite3
import subprocess
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from aiogram import Bot

from .tasks import BackgroundTask, TaskManager, TaskNotificationMode

logger = logging.getLogger(__name__)
_RESPONSE_STATUS_RE = re.compile(r"overall status:\s*[`']?(ok|warn|critical|error|failed)[`']?", re.IGNORECASE)
_NATIVE_SCHEDULE_HEADER = "[[SCHEDULE_NATIVE]]"
_SCHEDULE_DELIVER_MARKER = "[[SCHEDULE_DELIVER]]"
_RATE_LIMIT_ERROR_RE = re.compile(
    r"(usage limit|rate limit|quota exceeded|too many requests|over capacity|429)",
    re.IGNORECASE,
)
_TRY_AGAIN_AT_RE = re.compile(r"try again at\s+(.+?)(?:[.]|$)", re.IGNORECASE)
_ORDINAL_DAY_RE = re.compile(r"(\d{1,2})(st|nd|rd|th)\b", re.IGNORECASE)
_NO_UPDATE = object()


@dataclass(frozen=True)
class NativeScheduleSpec:
    command: list[str]
    diagnose_command: list[str] | None = None
    remediate_command: list[str] | None = None
    auto_remediate: bool = False
    escalation_context: str | None = None


@dataclass(frozen=True)
class NativeScheduleResult:
    status: str
    change_type: str
    should_alert: bool
    summary: str
    payload: dict[str, Any]
    raw_output: str


@dataclass(frozen=True)
class IncidentActionReport:
    diagnostics: tuple[str, ...] = ()
    remediation_output: str | None = None
    remediation_error: str | None = None
    verification_result: NativeScheduleResult | None = None
    verification_error: str | None = None


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
        notify_level: str = "failures",
    ) -> None:
        self._task_manager = task_manager
        self._db_path = db_path
        self._worker_task: asyncio.Task | None = None
        self._notification_bot = notification_bot
        self._notification_chat_id = notification_chat_id
        self._notification_thread_id = notification_thread_id
        self._notify_level = notify_level if notify_level in {"all", "failures", "off"} else "failures"
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

    async def update_native_schedule_options(
        self,
        task_id: str,
        *,
        auto_remediate: bool | object = _NO_UPDATE,
        diagnose_command: list[str] | None | object = _NO_UPDATE,
        remediate_command: list[str] | None | object = _NO_UPDATE,
    ) -> bool:
        return await asyncio.to_thread(
            self._update_native_schedule_options,
            task_id,
            auto_remediate,
            diagnose_command,
            remediate_command,
        )

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

    def _update_native_schedule_options(
        self,
        task_id: str,
        auto_remediate: bool | object,
        diagnose_command: list[str] | None | object,
        remediate_command: list[str] | None | object,
    ) -> bool:
        with self._connect() as con:
            row = con.execute(
                "SELECT prompt FROM scheduled_tasks WHERE id = ? AND state = 'active'",
                (task_id,),
            ).fetchone()
            if row is None:
                return False
            spec = self._parse_native_schedule(row["prompt"])
            if spec is None:
                raise ValueError("Schedule is not a native schedule")
            updated_spec = NativeScheduleSpec(
                command=spec.command,
                diagnose_command=spec.diagnose_command if diagnose_command is _NO_UPDATE else diagnose_command,
                remediate_command=spec.remediate_command if remediate_command is _NO_UPDATE else remediate_command,
                auto_remediate=spec.auto_remediate if auto_remediate is _NO_UPDATE else bool(auto_remediate),
                escalation_context=spec.escalation_context,
            )
            con.execute(
                "UPDATE scheduled_tasks SET prompt = ? WHERE id = ?",
                (self._render_native_schedule(updated_spec), task_id),
            )
            return True

    async def _worker_loop(self) -> None:
        while True:
            await self._run_due_once()
            await asyncio.sleep(self._POLL_SECONDS)

    async def _run_due_once(self) -> None:
        claimed_runs = await asyncio.to_thread(self._claim_due_runs, datetime.now(timezone.utc))
        for schedule, run_id, planned_for, submitted_at in claimed_runs:
            native_spec = self._parse_native_schedule(schedule.prompt)
            if native_spec is not None:
                await self._run_native_schedule(schedule, native_spec, run_id, planned_for, submitted_at)
                continue
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
                    prompt=self._strip_delivery_marker(schedule.prompt),
                    model=schedule.model,
                    session_id=schedule.session_id,
                    provider_cli=schedule.provider_cli,
                    resume_arg=schedule.resume_arg,
                    notification_mode=self._notification_mode_for_prompt(schedule.prompt),
                    live_feedback=False,
                    feedback_title=self._build_schedule_feedback_title(schedule, planned_for),
                    task_id=background_task_id,
                )
                await self._notify_schedule_event(
                    "submitted",
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
                    "submission_failed",
                    (
                        "❌ <b>Scheduled run submission failed</b>\n"
                        f"<b>Schedule:</b> <code>{schedule.id[:8]}</code>\n"
                        f"<b>Planned:</b> {planned_for.astimezone().strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"<b>Target:</b> {self._format_schedule_target(schedule.chat_id, schedule.message_thread_id)}"
                    )
                )

    async def _run_native_schedule(
        self,
        schedule: ScheduledTask,
        native_spec: NativeScheduleSpec,
        run_id: str,
        planned_for: datetime,
        submitted_at: datetime,
    ) -> None:
        started_at = datetime.now(timezone.utc)
        await asyncio.to_thread(
            self._mark_run_started_by_id,
            schedule.id,
            run_id,
            started_at.isoformat(),
        )
        try:
            native_result = await asyncio.to_thread(self._execute_native_schedule, native_spec)
            if native_result.should_alert:
                incident_report = await self._prepare_incident_report(schedule, native_spec, native_result)
                await self._submit_native_escalation(
                    schedule,
                    run_id,
                    planned_for,
                    native_result,
                    incident_report,
                    submitted_at,
                )
                return

            completed_at = datetime.now(timezone.utc)
            response_preview = self._preview_text(f"NO_ALERT {native_result.summary}")
            previous_run = await asyncio.to_thread(self._find_previous_run_row, schedule.id, run_id)
            await asyncio.to_thread(
                self._complete_run_by_id,
                schedule.id,
                run_id,
                "completed",
                completed_at.isoformat(),
                None,
                response_preview,
            )
            await self._notify_schedule_event(
                "completed",
                (
                    "✅ <b>Scheduled run completed</b>\n"
                    f"<b>Schedule:</b> <code>{schedule.id[:8]}</code>\n"
                    f"<b>Planned:</b> {planned_for.astimezone().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"<b>Finished:</b> {completed_at.astimezone().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"<b>Target:</b> {self._format_schedule_target(schedule.chat_id, schedule.message_thread_id)}\n"
                    f"<b>Result:</b> {html.escape(response_preview or 'No detail')}"
                ),
                previous_run=previous_run,
                current_response=response_preview,
            )
        except Exception as exc:
            logger.exception("Failed to execute native scheduled task %s", schedule.id)
            completed_at = datetime.now(timezone.utc)
            error_text = str(exc)
            previous_run = await asyncio.to_thread(self._find_previous_run_row, schedule.id, run_id)
            await asyncio.to_thread(
                self._complete_run_by_id,
                schedule.id,
                run_id,
                "failed",
                completed_at.isoformat(),
                error_text,
                None,
            )
            await self._notify_schedule_event(
                "failed",
                (
                    "❌ <b>Scheduled run failed</b>\n"
                    f"<b>Schedule:</b> <code>{schedule.id[:8]}</code>\n"
                    f"<b>Planned:</b> {planned_for.astimezone().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"<b>Finished:</b> {completed_at.astimezone().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"<b>Target:</b> {self._format_schedule_target(schedule.chat_id, schedule.message_thread_id)}\n"
                    f"<b>Result:</b> {html.escape(error_text)}"
                ),
                previous_run=previous_run,
                current_error=error_text,
            )

    async def _submit_native_escalation(
        self,
        schedule: ScheduledTask,
        run_id: str,
        planned_for: datetime,
        native_result: NativeScheduleResult,
        incident_report: IncidentActionReport,
        submitted_at: datetime,
    ) -> None:
        background_task_id = str(uuid.uuid4())
        prompt = self._build_native_escalation_prompt(schedule, native_result, incident_report)
        try:
            await asyncio.to_thread(self._mark_run_submitted, schedule.id, run_id, background_task_id)
            background_task_id = await self._task_manager.submit(
                chat_id=schedule.chat_id,
                message_thread_id=schedule.message_thread_id,
                user_id=schedule.user_id,
                prompt=prompt,
                model=schedule.model,
                session_id=schedule.session_id,
                provider_cli=schedule.provider_cli,
                resume_arg=schedule.resume_arg,
                notification_mode=TaskNotificationMode.SILENT,
                live_feedback=False,
                feedback_title=self._build_schedule_feedback_title(schedule, planned_for),
                task_id=background_task_id,
            )
            await self._notify_schedule_event(
                "submitted",
                (
                    "🕒 <b>Scheduled run submitted</b>\n"
                    f"<b>Schedule:</b> <code>{schedule.id[:8]}</code>\n"
                    f"<b>Planned:</b> {planned_for.astimezone().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"<b>Target:</b> {self._format_schedule_target(schedule.chat_id, schedule.message_thread_id)}\n"
                    f"<b>Prompt:</b> {html.escape(native_result.summary)}"
                ),
            )
        except Exception:
            logger.exception("Failed to submit native escalation for scheduled task %s", schedule.id)
            await asyncio.to_thread(
                self._mark_run_submission_failed,
                schedule.id,
                run_id,
                submitted_at.isoformat(),
                "Failed to submit background task",
            )
            await self._notify_schedule_event(
                "submission_failed",
                (
                    "❌ <b>Scheduled run submission failed</b>\n"
                    f"<b>Schedule:</b> <code>{schedule.id[:8]}</code>\n"
                    f"<b>Planned:</b> {planned_for.astimezone().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"<b>Target:</b> {self._format_schedule_target(schedule.chat_id, schedule.message_thread_id)}"
                ),
            )

    async def _prepare_incident_report(
        self,
        schedule: ScheduledTask,
        native_spec: NativeScheduleSpec,
        native_result: NativeScheduleResult,
    ) -> IncidentActionReport:
        if native_result.change_type == "recovery" or native_result.status not in {"warn", "critical"}:
            return IncidentActionReport()

        diagnostics = await asyncio.to_thread(self._collect_incident_diagnostics, schedule, native_spec, native_result)
        remediation_output: str | None = None
        remediation_error: str | None = None
        verification_result: NativeScheduleResult | None = None
        verification_error: str | None = None

        remediation_command = native_spec.remediate_command
        if native_spec.auto_remediate and remediation_command is None:
            remediation_command = self._default_remediation_command(native_result)

        if native_spec.auto_remediate and remediation_command:
            try:
                remediation_output = await asyncio.to_thread(
                    self._run_auxiliary_command,
                    remediation_command,
                    "remediation",
                )
            except Exception as exc:
                remediation_error = str(exc)

            try:
                verification_result = await asyncio.to_thread(self._execute_native_schedule, native_spec)
            except Exception as exc:
                verification_error = str(exc)
        elif native_spec.auto_remediate:
            remediation_error = "No automatic remediation is available for this incident."

        return IncidentActionReport(
            diagnostics=tuple(diagnostics),
            remediation_output=remediation_output,
            remediation_error=remediation_error,
            verification_result=verification_result,
            verification_error=verification_error,
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
                "started",
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
        status_value = getattr(task.status, "value", str(task.status))
        rate_limit_retry_at = None
        if run and status_value == "failed":
            rate_limit_retry_at = self._scheduled_retry_at_for_rate_limit(task.error, task.completed_at)
        await asyncio.to_thread(
            self._update_run_for_background_task,
            task.id,
            "deferred_rate_limited" if rate_limit_retry_at else status_value,
            task.started_at.isoformat() if task.started_at else None,
            task.completed_at.isoformat() if task.completed_at else None,
            task.error,
            self._preview_text(task.response),
        )
        if run:
            if rate_limit_retry_at:
                await asyncio.to_thread(
                    self._defer_schedule_next_run,
                    run["schedule_id"],
                    rate_limit_retry_at.isoformat(),
                )
            finished_at = task.completed_at.astimezone().strftime("%Y-%m-%d %H:%M:%S") if task.completed_at else "unknown"
            detail = task.error or self._preview_text(task.response, 220) or "No detail"
            planned_for = datetime.fromisoformat(run["planned_for"]).astimezone().strftime("%Y-%m-%d %H:%M:%S")
            previous_run = await asyncio.to_thread(
                self._find_previous_run_row,
                run["schedule_id"],
                run["id"],
            )
            event_kind = "deferred_rate_limited" if rate_limit_retry_at else status_value
            header = (
                "⏸️ <b>Scheduled run deferred (rate limited)</b>\n"
                if rate_limit_retry_at
                else f"{self._status_emoji(status_value)} <b>Scheduled run {html.escape(status_value)}</b>\n"
            )
            retry_line = (
                f"\n<b>Next attempt:</b> {rate_limit_retry_at.astimezone().strftime('%Y-%m-%d %H:%M:%S')}"
                if rate_limit_retry_at
                else ""
            )
            await self._notify_schedule_event(
                event_kind,
                (
                    f"{header}"
                    f"<b>Schedule:</b> <code>{run['schedule_id'][:8]}</code>\n"
                    f"<b>Planned:</b> {planned_for}\n"
                    f"<b>Finished:</b> {finished_at}\n"
                    f"<b>Target:</b> {self._format_schedule_target(run['chat_id'], run['message_thread_id'])}\n"
                    f"<b>Result:</b> {html.escape(detail)}"
                    f"{retry_line}"
                ),
                previous_run=previous_run,
                current_response=task.response,
                current_error=task.error,
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

    def _mark_run_started_by_id(self, schedule_id: str, run_id: str, started_at: str | None) -> None:
        with self._connect() as con:
            con.execute(
                """
                UPDATE scheduled_task_runs
                SET status = 'running', started_at = COALESCE(?, started_at)
                WHERE id = ?
                """,
                (started_at, run_id),
            )
            con.execute(
                """
                UPDATE scheduled_tasks
                SET current_started_at = COALESCE(?, current_started_at),
                    current_status = 'running'
                WHERE id = ? AND current_run_id = ?
                """,
                (started_at, schedule_id, run_id),
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

    def _defer_schedule_next_run(self, schedule_id: str, next_run_at: str) -> None:
        with self._connect() as con:
            con.execute(
                """
                UPDATE scheduled_tasks
                SET next_run_at = ?
                WHERE id = ? AND next_run_at < ?
                """,
                (next_run_at, schedule_id, next_run_at),
            )

    def _complete_run_by_id(
        self,
        schedule_id: str,
        run_id: str,
        status: str,
        completed_at: str | None,
        error_text: str | None,
        response_preview: str | None,
    ) -> None:
        with self._connect() as con:
            con.execute(
                """
                UPDATE scheduled_task_runs
                SET status = ?, completed_at = ?, error_text = ?, response_preview = ?
                WHERE id = ?
                """,
                (status, completed_at, error_text, response_preview, run_id),
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
                SELECT id, schedule_id, chat_id, message_thread_id, planned_for
                FROM scheduled_task_runs
                WHERE background_task_id = ?
                """,
                (background_task_id,),
            ).fetchone()

    def _find_previous_run_row(self, schedule_id: str, current_run_id: str) -> sqlite3.Row | None:
        with self._connect() as con:
            return con.execute(
                """
                SELECT id, status, error_text, response_preview
                FROM scheduled_task_runs
                WHERE schedule_id = ? AND id != ?
                ORDER BY submitted_at DESC
                LIMIT 1
                """,
                (schedule_id, current_run_id),
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
            "deferred_rate_limited": "⏸️",
        }.get(status, "ℹ️")

    @classmethod
    def _scheduled_retry_at_for_rate_limit(
        cls,
        error_text: str | None,
        completed_at: datetime | None,
    ) -> datetime | None:
        if not error_text or not cls._is_rate_limit_error(error_text):
            return None
        return cls._parse_retry_at(error_text) or ((completed_at or datetime.now(timezone.utc)) + timedelta(hours=1))

    @staticmethod
    def _is_rate_limit_error(error_text: str | None) -> bool:
        return bool(error_text) and bool(_RATE_LIMIT_ERROR_RE.search(error_text))

    @classmethod
    def _parse_retry_at(cls, error_text: str) -> datetime | None:
        match = _TRY_AGAIN_AT_RE.search(error_text or "")
        if not match:
            return None
        cleaned = cls._clean_retry_at_text(match.group(1))
        if not cleaned:
            return None
        local_tz = datetime.now().astimezone().tzinfo or timezone.utc
        for fmt in ("%b %d, %Y %I:%M %p", "%B %d, %Y %I:%M %p"):
            try:
                parsed = datetime.strptime(cleaned, fmt)
                return parsed.replace(tzinfo=local_tz).astimezone(timezone.utc)
            except ValueError:
                continue
        return None

    @staticmethod
    def _clean_retry_at_text(raw_value: str) -> str:
        compact = " ".join((raw_value or "").split()).rstrip(".")
        return _ORDINAL_DAY_RE.sub(r"\1", compact)

    def _build_schedule_feedback_title(self, schedule: ScheduledTask, planned_for: datetime) -> str:
        return (
            "🕒 <b>Scheduled run started</b>\n"
            f"<b>Schedule:</b> <code>{schedule.id[:8]}</code>\n"
            f"<b>Planned:</b> {planned_for.astimezone().strftime('%Y-%m-%d %H:%M:%S')}"
        )

    @staticmethod
    def _notification_mode_for_prompt(prompt: str) -> TaskNotificationMode:
        if _SCHEDULE_DELIVER_MARKER in (prompt or ""):
            return TaskNotificationMode.DELIVER_RESPONSE
        return TaskNotificationMode.SILENT

    @staticmethod
    def _strip_delivery_marker(prompt: str) -> str:
        if not prompt:
            return prompt
        return "\n".join(
            line for line in prompt.splitlines() if line.strip() != _SCHEDULE_DELIVER_MARKER
        ).strip()

    @staticmethod
    def _parse_native_schedule(prompt: str) -> NativeScheduleSpec | None:
        lines = prompt.splitlines()
        if not lines or lines[0].strip() != _NATIVE_SCHEDULE_HEADER:
            return None
        command: list[str] | None = None
        diagnose_command: list[str] | None = None
        remediate_command: list[str] | None = None
        auto_remediate = False
        context_lines: list[str] = []
        for line in lines[1:]:
            if command is None and line.startswith("command:"):
                command_text = line.partition(":")[2].strip()
                if not command_text:
                    raise ValueError("Native schedule is missing command text")
                command = shlex.split(command_text)
                continue
            if line.startswith("diagnose_command:"):
                command_text = line.partition(":")[2].strip()
                if not command_text:
                    raise ValueError("Native schedule diagnose_command is empty")
                diagnose_command = shlex.split(command_text)
                continue
            if line.startswith("remediate_command:"):
                command_text = line.partition(":")[2].strip()
                if not command_text:
                    raise ValueError("Native schedule remediate_command is empty")
                remediate_command = shlex.split(command_text)
                continue
            if line.startswith("auto_remediate:"):
                auto_remediate = ScheduleManager._parse_bool_flag(line.partition(":")[2].strip())
                continue
            context_lines.append(line)
        if not command:
            raise ValueError("Native schedule is missing command")
        escalation_context = "\n".join(context_lines).strip() or None
        return NativeScheduleSpec(
            command=command,
            diagnose_command=diagnose_command,
            remediate_command=remediate_command,
            auto_remediate=auto_remediate,
            escalation_context=escalation_context,
        )

    @staticmethod
    def _execute_native_schedule(spec: NativeScheduleSpec) -> NativeScheduleResult:
        stdout = ScheduleManager._run_auxiliary_command(spec.command, "native schedule")
        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Native schedule did not return valid JSON") from exc
        if not isinstance(payload, dict):
            raise RuntimeError("Native schedule JSON payload must be an object")
        status = str(payload.get("status") or "ok").lower()
        should_alert = bool(payload.get("should_alert"))
        change_type = str(payload.get("change_type") or "unknown")
        summary = str(payload.get("summary") or "No summary provided.")
        raw_output = stdout
        return NativeScheduleResult(
            status=status,
            change_type=change_type,
            should_alert=should_alert,
            summary=summary,
            payload=payload,
            raw_output=raw_output,
        )

    @staticmethod
    def _run_auxiliary_command(command: list[str], label: str, require_success: bool = True) -> str:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            cwd=Path(__file__).resolve().parent.parent,
        )
        stdout = (completed.stdout or "").strip()
        stderr = (completed.stderr or "").strip()
        if require_success and completed.returncode != 0:
            detail = stderr or stdout or f"{label} failed with exit code {completed.returncode}"
            raise RuntimeError(detail)
        detail = stdout or stderr or f"{label} exited with code {completed.returncode}"
        if completed.returncode == 0:
            return detail
        return f"{label} exited with code {completed.returncode}\n{detail}"

    @classmethod
    def _collect_incident_diagnostics(
        cls,
        schedule: ScheduledTask,
        native_spec: NativeScheduleSpec,
        result: NativeScheduleResult,
    ) -> list[str]:
        diagnostics: list[str] = []
        if native_spec.diagnose_command:
            try:
                output = cls._run_auxiliary_command(native_spec.diagnose_command, "diagnose command", require_success=False)
            except Exception as exc:
                output = f"diagnose command failed: {exc}"
            diagnostics.append(f"Custom diagnostics:\n{cls._preview_text(output, 1600) or output}")

        for title, command in cls._default_diagnostic_commands(result):
            try:
                output = cls._run_auxiliary_command(command, title, require_success=False)
            except Exception as exc:
                output = f"{title} failed: {exc}"
            diagnostics.append(f"{title}:\n{cls._preview_text(output, 1600) or output}")
        return diagnostics

    @staticmethod
    def _default_diagnostic_commands(result: NativeScheduleResult) -> list[tuple[str, list[str]]]:
        payload_checks = result.payload.get("checks")
        if not isinstance(payload_checks, list):
            return []
        check_names = {
            str(item.get("name"))
            for item in payload_checks
            if isinstance(item, dict) and str(item.get("status")) in {"warn", "critical"}
        }
        if not ({"scrape_up", "series_presence", "f08_series_presence"} & check_names):
            return []
        return [
            ("telegram-bot.service status", ["systemctl", "status", "telegram-bot.service", "--no-pager"]),
            (
                "metrics endpoint presence",
                [
                    "/bin/bash",
                    "-lc",
                    "curl -fsS http://127.0.0.1:9101/metrics | rg -n 'telegrambot_messages_total|telegrambot_f08_governance_events_total' || true",
                ],
            ),
            ("telegram-bot.service journal", ["journalctl", "-u", "telegram-bot.service", "-n", "40", "--no-pager"]),
        ]

    @staticmethod
    def _default_remediation_command(result: NativeScheduleResult) -> list[str] | None:
        payload_checks = result.payload.get("checks")
        if not isinstance(payload_checks, list):
            return None
        check_names = {
            str(item.get("name"))
            for item in payload_checks
            if isinstance(item, dict) and str(item.get("status")) in {"warn", "critical"}
        }
        if {"scrape_up", "series_presence", "f08_series_presence"} & check_names:
            return ["systemctl", "restart", "telegram-bot.service"]
        return None

    @staticmethod
    def _parse_bool_flag(value: str) -> bool:
        return value.strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _render_native_schedule(spec: NativeScheduleSpec) -> str:
        lines = [_NATIVE_SCHEDULE_HEADER, f"command: {shlex.join(spec.command)}"]
        if spec.diagnose_command:
            lines.append(f"diagnose_command: {shlex.join(spec.diagnose_command)}")
        if spec.remediate_command:
            lines.append(f"remediate_command: {shlex.join(spec.remediate_command)}")
        if spec.auto_remediate:
            lines.append("auto_remediate: true")
        if spec.escalation_context:
            lines.append(spec.escalation_context.strip())
        return "\n".join(lines).strip() + "\n"

    @staticmethod
    def _build_native_escalation_prompt(
        schedule: ScheduledTask,
        result: NativeScheduleResult,
        incident_report: IncidentActionReport,
    ) -> str:
        prompt = (
            "A native scheduled validator detected an alert-worthy state change.\n"
            f"Schedule ID: {schedule.id}\n"
            f"Target: {ScheduleManager._format_schedule_target(schedule.chat_id, schedule.message_thread_id)}\n"
            f"Overall status: `{result.status}`\n"
            f"Change type: `{result.change_type}`\n"
            f"Summary: {result.summary}\n\n"
            "Write a concise operator-facing message. Keep the first line exactly as "
            f"`Overall status: `{result.status}`` and then explain what changed, why it matters, and the next action.\n\n"
            "Validator JSON:\n"
            f"```json\n{json.dumps(result.payload, ensure_ascii=True, indent=2)}\n```"
        )
        if incident_report.diagnostics:
            prompt += "\n\nDiagnostics collected automatically:\n"
            for item in incident_report.diagnostics:
                prompt += f"\n{item}\n"
        if incident_report.remediation_output or incident_report.remediation_error:
            prompt += "\n\nAutomatic remediation attempt:\n"
            if incident_report.remediation_output:
                prompt += f"\nOutput:\n{incident_report.remediation_output}\n"
            if incident_report.remediation_error:
                prompt += f"\nError:\n{incident_report.remediation_error}\n"
        if incident_report.verification_result:
            prompt += (
                "\n\nPost-remediation verification:\n"
                f"- status: `{incident_report.verification_result.status}`\n"
                f"- change_type: `{incident_report.verification_result.change_type}`\n"
                f"- summary: {incident_report.verification_result.summary}\n"
                "Verification JSON:\n"
                f"```json\n{json.dumps(incident_report.verification_result.payload, ensure_ascii=True, indent=2)}\n```"
            )
        elif incident_report.verification_error:
            prompt += f"\n\nPost-remediation verification failed:\n{incident_report.verification_error}\n"
        if schedule.prompt:
            native_spec = ScheduleManager._parse_native_schedule(schedule.prompt)
            if native_spec and native_spec.escalation_context:
                prompt += f"\n\nExtra escalation instructions:\n{native_spec.escalation_context}"
        return prompt

    @staticmethod
    def _normalize_detail(value: str | None) -> str:
        return " ".join((value or "").split())

    @classmethod
    def _response_signal(cls, response_text: str | None) -> str | None:
        if not response_text:
            return None
        text = response_text.strip()
        if text.upper().startswith("NO_ALERT"):
            return "ok"
        match = _RESPONSE_STATUS_RE.search(text)
        if match:
            status = match.group(1).lower()
            if status == "error":
                return "critical"
            if status == "failed":
                return "critical"
            return status
        return None

    def _should_notify_event(
        self,
        event_kind: str,
        previous_run: sqlite3.Row | None = None,
        current_response: str | None = None,
        current_error: str | None = None,
    ) -> bool:
        if self._notify_level == "off":
            return False
        if self._notify_level == "all":
            return True
        if event_kind in {"submitted", "started"}:
            return False
        if event_kind == "completed":
            current_signal = self._response_signal(current_response)
            previous_signal = self._response_signal(previous_run["response_preview"]) if previous_run else None
            if current_signal in {"warn", "critical"}:
                return (
                    previous_run is None
                    or previous_signal != current_signal
                    or self._normalize_detail(previous_run["response_preview"]) != self._normalize_detail(current_response)
                )
            return previous_signal in {"warn", "critical"}
        if event_kind in {"failed", "cancelled", "submission_failed", "failed_recovered", "deferred_rate_limited"}:
            if previous_run is None:
                return True
            return (
                previous_run["status"] != event_kind
                or self._normalize_detail(previous_run["error_text"]) != self._normalize_detail(current_error)
            )
        return False

    async def _notify_schedule_event(
        self,
        event_kind: str,
        text: str,
        previous_run: sqlite3.Row | None = None,
        current_response: str | None = None,
        current_error: str | None = None,
    ) -> None:
        if self._notification_bot is None or self._notification_chat_id is None:
            return
        if not self._should_notify_event(
            event_kind,
            previous_run=previous_run,
            current_response=current_response,
            current_error=current_error,
        ):
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
