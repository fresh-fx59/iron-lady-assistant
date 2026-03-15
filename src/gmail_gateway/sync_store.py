from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from .schema import ensure_gateway_schema


@dataclass(frozen=True)
class SyncCursor:
    account_id: str
    last_history_id: str | None
    watch_expiration_ts: str | None
    sync_state: str
    last_successful_sync_at: str | None
    last_error_code: str | None
    last_error_message: str | None
    stale_cursor_count: int
    updated_at: str


class SyncStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        ensure_gateway_schema(db_path)

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self._db_path)
        con.row_factory = sqlite3.Row
        return con

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(UTC).isoformat()

    def bootstrap(self, *, account_id: str) -> SyncCursor:
        now = self._now_iso()
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO gateway_sync_cursors (
                    account_id,
                    last_history_id,
                    watch_expiration_ts,
                    sync_state,
                    last_successful_sync_at,
                    last_error_code,
                    last_error_message,
                    stale_cursor_count,
                    updated_at
                ) VALUES (?, NULL, NULL, 'bootstrap_running', NULL, NULL, NULL, 0, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    sync_state = 'bootstrap_running',
                    last_error_code = NULL,
                    last_error_message = NULL,
                    updated_at = excluded.updated_at
                """,
                (account_id, now),
            )
        return self.get_cursor(account_id=account_id)

    def delta(self, *, account_id: str, history_id: str) -> SyncCursor:
        now = self._now_iso()
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO gateway_sync_cursors (
                    account_id,
                    last_history_id,
                    watch_expiration_ts,
                    sync_state,
                    last_successful_sync_at,
                    last_error_code,
                    last_error_message,
                    stale_cursor_count,
                    updated_at
                ) VALUES (?, ?, NULL, 'idle', ?, NULL, NULL, 0, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    last_history_id = excluded.last_history_id,
                    sync_state = 'idle',
                    last_successful_sync_at = excluded.last_successful_sync_at,
                    last_error_code = NULL,
                    last_error_message = NULL,
                    updated_at = excluded.updated_at
                """,
                (account_id, history_id, now, now),
            )
        return self.get_cursor(account_id=account_id)

    def renew_watch(self, *, account_id: str, watch_expiration_ts: str) -> SyncCursor:
        now = self._now_iso()
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO gateway_sync_cursors (
                    account_id,
                    last_history_id,
                    watch_expiration_ts,
                    sync_state,
                    last_successful_sync_at,
                    last_error_code,
                    last_error_message,
                    stale_cursor_count,
                    updated_at
                ) VALUES (?, NULL, ?, 'idle', NULL, NULL, NULL, 0, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    watch_expiration_ts = excluded.watch_expiration_ts,
                    sync_state = 'idle',
                    last_error_code = NULL,
                    last_error_message = NULL,
                    updated_at = excluded.updated_at
                """,
                (account_id, watch_expiration_ts, now),
            )
        return self.get_cursor(account_id=account_id)

    def get_cursor(self, *, account_id: str) -> SyncCursor:
        with self._connect() as con:
            row = con.execute(
                """
                SELECT
                    account_id,
                    last_history_id,
                    watch_expiration_ts,
                    sync_state,
                    last_successful_sync_at,
                    last_error_code,
                    last_error_message,
                    stale_cursor_count,
                    updated_at
                FROM gateway_sync_cursors
                WHERE account_id = ?
                """,
                (account_id,),
            ).fetchone()
        if row is None:
            raise RuntimeError(f"sync cursor missing for account {account_id}")
        return SyncCursor(
            account_id=str(row["account_id"]),
            last_history_id=str(row["last_history_id"]) if row["last_history_id"] else None,
            watch_expiration_ts=str(row["watch_expiration_ts"]) if row["watch_expiration_ts"] else None,
            sync_state=str(row["sync_state"]),
            last_successful_sync_at=(
                str(row["last_successful_sync_at"]) if row["last_successful_sync_at"] else None
            ),
            last_error_code=str(row["last_error_code"]) if row["last_error_code"] else None,
            last_error_message=str(row["last_error_message"]) if row["last_error_message"] else None,
            stale_cursor_count=int(row["stale_cursor_count"]),
            updated_at=str(row["updated_at"]),
        )
