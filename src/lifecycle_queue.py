from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class QueuedTurn:
    id: int
    scope_key: str
    chat_id: int
    message_thread_id: int | None
    user_id: int
    prompt: str
    source_message_id: int | None
    status: str
    operation_id: str | None
    task_id: str | None
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class QueuedBackgroundTask:
    id: int
    task_id: str
    chat_id: int
    message_thread_id: int | None
    user_id: int
    prompt: str
    model: str
    session_id: str | None
    provider_cli: str
    resume_arg: str | None
    notification_mode: str
    live_feedback: bool
    feedback_title: str | None
    status: str
    created_at: str
    updated_at: str


class LifecycleQueueStore:
    """Persist deploy-drain state, active work, and deferred turns across restarts."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(self._path)
        con.row_factory = sqlite3.Row
        self._ensure_schema(con)
        return con

    def _init_db(self) -> None:
        with self._connect() as con:
            self._upsert_state_unlocked(con, "barrier_phase", "open")

    def _ensure_schema(self, con: sqlite3.Connection) -> None:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS lifecycle_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS lifecycle_operations (
                id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                requested_commit TEXT,
                requested_by_scope TEXT,
                requested_by_chat_id INTEGER,
                requested_by_thread_id INTEGER,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                last_error TEXT
            );

            CREATE TABLE IF NOT EXISTS lifecycle_active_scopes (
                scope_key TEXT PRIMARY KEY,
                chat_id INTEGER NOT NULL,
                message_thread_id INTEGER,
                user_id INTEGER,
                kind TEXT NOT NULL,
                prompt_preview TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS lifecycle_queued_turns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope_key TEXT NOT NULL,
                chat_id INTEGER NOT NULL,
                message_thread_id INTEGER,
                user_id INTEGER NOT NULL,
                prompt TEXT NOT NULL,
                source_message_id INTEGER,
                status TEXT NOT NULL,
                operation_id TEXT,
                task_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_lifecycle_queued_turns_status_created
                ON lifecycle_queued_turns(status, created_at, id);

            CREATE TABLE IF NOT EXISTS lifecycle_queued_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL UNIQUE,
                chat_id INTEGER NOT NULL,
                message_thread_id INTEGER,
                user_id INTEGER NOT NULL,
                prompt TEXT NOT NULL,
                model TEXT NOT NULL,
                session_id TEXT,
                provider_cli TEXT NOT NULL,
                resume_arg TEXT,
                notification_mode TEXT NOT NULL,
                live_feedback INTEGER NOT NULL,
                feedback_title TEXT,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_lifecycle_queued_tasks_status_created
                ON lifecycle_queued_tasks(status, created_at, id);
            """
        )
        row = con.execute(
            "SELECT 1 FROM lifecycle_state WHERE key = ?",
            ("barrier_phase",),
        ).fetchone()
        if not row:
            self._upsert_state_unlocked(con, "barrier_phase", "open")

    def _upsert_state_unlocked(self, con: sqlite3.Connection, key: str, value: str) -> None:
        con.execute(
            """
            INSERT INTO lifecycle_state(key, value, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (key, value, _utc_now()),
        )

    def barrier_phase(self) -> str:
        with self._lock, self._connect() as con:
            row = con.execute(
                "SELECT value FROM lifecycle_state WHERE key = ?",
                ("barrier_phase",),
            ).fetchone()
            return str(row["value"]) if row else "open"

    def is_draining(self) -> bool:
        return self.barrier_phase() in {"draining", "restarting"}

    def begin_deploy(
        self,
        *,
        requested_commit: str,
        requested_by_scope: str = "deploy:main",
        requested_by_chat_id: int | None = None,
        requested_by_thread_id: int | None = None,
        payload: dict[str, object] | None = None,
    ) -> str:
        with self._lock, self._connect() as con:
            existing = con.execute(
                """
                SELECT id FROM lifecycle_operations
                WHERE kind = 'deploy_main' AND status IN ('draining', 'restarting')
                ORDER BY created_at DESC
                LIMIT 1
                """
            ).fetchone()
            if existing:
                self._upsert_state_unlocked(con, "barrier_phase", "draining")
                return str(existing["id"])

            operation_id = str(uuid.uuid4())
            now = _utc_now()
            con.execute(
                """
                INSERT INTO lifecycle_operations(
                    id, kind, requested_commit, requested_by_scope, requested_by_chat_id,
                    requested_by_thread_id, status, payload_json, created_at, updated_at, started_at
                )
                VALUES(?, 'deploy_main', ?, ?, ?, ?, 'draining', ?, ?, ?, ?)
                """,
                (
                    operation_id,
                    requested_commit,
                    requested_by_scope,
                    requested_by_chat_id,
                    requested_by_thread_id,
                    json.dumps(payload or {}, ensure_ascii=False),
                    now,
                    now,
                    now,
                ),
            )
            self._upsert_state_unlocked(con, "barrier_phase", "draining")
            self._upsert_state_unlocked(con, "active_operation_id", operation_id)
            return operation_id

    def mark_restarting(self, operation_id: str) -> None:
        with self._lock, self._connect() as con:
            now = _utc_now()
            con.execute(
                """
                UPDATE lifecycle_operations
                SET status = 'restarting', updated_at = ?
                WHERE id = ?
                """,
                (now, operation_id),
            )
            self._upsert_state_unlocked(con, "barrier_phase", "restarting")
            self._upsert_state_unlocked(con, "active_operation_id", operation_id)

    def acknowledge_process_restart(self) -> None:
        with self._lock, self._connect() as con:
            row = con.execute(
                "SELECT value FROM lifecycle_state WHERE key = ?",
                ("barrier_phase",),
            ).fetchone()
            if row and str(row["value"]) == "restarting":
                self._upsert_state_unlocked(con, "barrier_phase", "open")
            con.execute(
                """
                UPDATE lifecycle_queued_turns
                SET status = 'queued', updated_at = ?
                WHERE status = 'replaying'
                """,
                (_utc_now(),),
            )
            con.execute(
                """
                UPDATE lifecycle_queued_tasks
                SET status = 'queued', updated_at = ?
                WHERE status = 'replaying'
                """,
                (_utc_now(),),
            )

    def mark_operation_completed(self, operation_id: str) -> None:
        with self._lock, self._connect() as con:
            now = _utc_now()
            con.execute(
                """
                UPDATE lifecycle_operations
                SET status = 'completed', updated_at = ?, completed_at = ?, last_error = NULL
                WHERE id = ?
                """,
                (now, now, operation_id),
            )
            self._upsert_state_unlocked(con, "barrier_phase", "open")

    def mark_operation_failed(self, operation_id: str | None, error_text: str) -> None:
        with self._lock, self._connect() as con:
            now = _utc_now()
            if operation_id:
                con.execute(
                    """
                    UPDATE lifecycle_operations
                    SET status = 'failed', updated_at = ?, completed_at = ?, last_error = ?
                    WHERE id = ?
                    """,
                    (now, now, error_text[:4000], operation_id),
                )
            self._upsert_state_unlocked(con, "barrier_phase", "open")

    def upsert_active_scope(
        self,
        *,
        scope_key: str,
        chat_id: int,
        message_thread_id: int | None,
        user_id: int | None,
        kind: str,
        prompt_preview: str | None,
    ) -> None:
        with self._lock, self._connect() as con:
            created_at = _utc_now()
            con.execute(
                """
                INSERT INTO lifecycle_active_scopes(
                    scope_key, chat_id, message_thread_id, user_id, kind, prompt_preview, created_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(scope_key) DO UPDATE SET
                    chat_id = excluded.chat_id,
                    message_thread_id = excluded.message_thread_id,
                    user_id = excluded.user_id,
                    kind = excluded.kind,
                    prompt_preview = excluded.prompt_preview,
                    updated_at = excluded.updated_at
                """,
                (
                    scope_key,
                    chat_id,
                    message_thread_id,
                    user_id,
                    kind,
                    (prompt_preview or "")[:400],
                    created_at,
                    created_at,
                ),
            )

    def clear_active_scope(self, scope_key: str) -> None:
        with self._lock, self._connect() as con:
            con.execute("DELETE FROM lifecycle_active_scopes WHERE scope_key = ?", (scope_key,))

    def active_scope_count(self) -> int:
        with self._lock, self._connect() as con:
            row = con.execute("SELECT COUNT(*) AS c FROM lifecycle_active_scopes").fetchone()
            return int(row["c"]) if row else 0

    def enqueue_turn(
        self,
        *,
        scope_key: str,
        chat_id: int,
        message_thread_id: int | None,
        user_id: int,
        prompt: str,
        source_message_id: int | None,
        operation_id: str | None = None,
    ) -> int:
        with self._lock, self._connect() as con:
            if source_message_id is not None:
                existing = con.execute(
                    """
                    SELECT id FROM lifecycle_queued_turns
                    WHERE scope_key = ? AND source_message_id = ? AND status IN ('queued', 'replaying', 'submitted')
                    ORDER BY id DESC LIMIT 1
                    """,
                    (scope_key, source_message_id),
                ).fetchone()
                if existing:
                    return int(existing["id"])
            now = _utc_now()
            cur = con.execute(
                """
                INSERT INTO lifecycle_queued_turns(
                    scope_key, chat_id, message_thread_id, user_id, prompt, source_message_id,
                    status, operation_id, task_id, created_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, 'queued', ?, NULL, ?, ?)
                """,
                (
                    scope_key,
                    chat_id,
                    message_thread_id,
                    user_id,
                    prompt,
                    source_message_id,
                    operation_id,
                    now,
                    now,
                ),
            )
            return int(cur.lastrowid)

    def claim_queued_turns(self, *, limit: int = 10) -> list[QueuedTurn]:
        with self._lock, self._connect() as con:
            rows = con.execute(
                """
                SELECT id, scope_key, chat_id, message_thread_id, user_id, prompt, source_message_id,
                       status, operation_id, task_id, created_at, updated_at
                FROM lifecycle_queued_turns
                WHERE status = 'queued'
                ORDER BY created_at ASC, id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            if not rows:
                return []
            ids = [int(row["id"]) for row in rows]
            now = _utc_now()
            con.executemany(
                "UPDATE lifecycle_queued_turns SET status = 'replaying', updated_at = ? WHERE id = ?",
                [(now, row_id) for row_id in ids],
            )
            claimed: list[QueuedTurn] = []
            for row in rows:
                payload = dict(row)
                payload["status"] = "replaying"
                payload["updated_at"] = now
                claimed.append(QueuedTurn(**payload))
            return claimed

    def mark_turn_submitted(self, turn_id: int, task_id: str) -> None:
        with self._lock, self._connect() as con:
            con.execute(
                """
                UPDATE lifecycle_queued_turns
                SET status = 'submitted', task_id = ?, updated_at = ?
                WHERE id = ?
                """,
                (task_id, _utc_now(), turn_id),
            )

    def requeue_turn(self, turn_id: int) -> None:
        with self._lock, self._connect() as con:
            con.execute(
                """
                UPDATE lifecycle_queued_turns
                SET status = 'queued', updated_at = ?
                WHERE id = ?
                """,
                (_utc_now(), turn_id),
            )

    def enqueue_background_task(
        self,
        *,
        task_id: str,
        chat_id: int,
        message_thread_id: int | None,
        user_id: int,
        prompt: str,
        model: str,
        session_id: str | None,
        provider_cli: str,
        resume_arg: str | None,
        notification_mode: str,
        live_feedback: bool,
        feedback_title: str | None,
    ) -> int:
        with self._lock, self._connect() as con:
            existing = con.execute(
                "SELECT id FROM lifecycle_queued_tasks WHERE task_id = ?",
                (task_id,),
            ).fetchone()
            if existing:
                return int(existing["id"])
            now = _utc_now()
            cur = con.execute(
                """
                INSERT INTO lifecycle_queued_tasks(
                    task_id, chat_id, message_thread_id, user_id, prompt, model, session_id,
                    provider_cli, resume_arg, notification_mode, live_feedback, feedback_title,
                    status, created_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)
                """,
                (
                    task_id,
                    chat_id,
                    message_thread_id,
                    user_id,
                    prompt,
                    model,
                    session_id,
                    provider_cli,
                    resume_arg,
                    notification_mode,
                    1 if live_feedback else 0,
                    feedback_title,
                    now,
                    now,
                ),
            )
            return int(cur.lastrowid)

    def claim_queued_background_tasks(self, *, limit: int = 10) -> list[QueuedBackgroundTask]:
        with self._lock, self._connect() as con:
            rows = con.execute(
                """
                SELECT id, task_id, chat_id, message_thread_id, user_id, prompt, model, session_id,
                       provider_cli, resume_arg, notification_mode, live_feedback, feedback_title,
                       status, created_at, updated_at
                FROM lifecycle_queued_tasks
                WHERE status = 'queued'
                ORDER BY created_at ASC, id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            if not rows:
                return []
            now = _utc_now()
            con.executemany(
                "UPDATE lifecycle_queued_tasks SET status = 'replaying', updated_at = ? WHERE id = ?",
                [(now, int(row["id"])) for row in rows],
            )
            claimed: list[QueuedBackgroundTask] = []
            for row in rows:
                payload = dict(row)
                payload["status"] = "replaying"
                payload["updated_at"] = now
                payload["live_feedback"] = bool(payload["live_feedback"])
                claimed.append(QueuedBackgroundTask(**payload))
            return claimed

    def mark_background_task_submitted(self, task_id: str) -> None:
        with self._lock, self._connect() as con:
            con.execute(
                """
                UPDATE lifecycle_queued_tasks
                SET status = 'submitted', updated_at = ?
                WHERE task_id = ?
                """,
                (_utc_now(), task_id),
            )

    def requeue_background_task(self, task_id: str) -> None:
        with self._lock, self._connect() as con:
            con.execute(
                """
                UPDATE lifecycle_queued_tasks
                SET status = 'queued', updated_at = ?
                WHERE task_id = ?
                """,
                (_utc_now(), task_id),
            )
