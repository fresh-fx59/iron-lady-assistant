from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from .schema import ensure_gateway_schema


@dataclass(frozen=True)
class DeliveryReceiptRecord:
    receipt_id: str
    account_id: str
    status: str
    provider_message_id: str | None
    queued_at: str
    sent_at: str | None


@dataclass(frozen=True)
class StoredMessage:
    account_id: str
    message_id: str
    thread_id: str
    subject: str
    from_email: str
    snippet: str
    internal_ts: str
    body_text: str
    body_html: str | None
    labels: list[str]


class MessageStore:
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

    @staticmethod
    def request_hash(payload: dict[str, object]) -> str:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def get_idempotency_record(
        self,
        *,
        account_id: str,
        operation: str,
        idempotency_key: str,
    ) -> sqlite3.Row | None:
        with self._connect() as con:
            return con.execute(
                """
                SELECT request_hash, response_json, status_code
                FROM gateway_idempotency_records
                WHERE account_id = ? AND operation = ? AND idempotency_key = ?
                """,
                (account_id, operation, idempotency_key),
            ).fetchone()

    def record_send_receipt(
        self,
        *,
        account_id: str,
        idempotency_key: str,
        request_hash: str,
    ) -> DeliveryReceiptRecord:
        receipt_id = f"rcpt-{uuid4().hex}"
        queued_at = self._now_iso()
        response_payload = {
            "receipt_id": receipt_id,
            "account_id": account_id,
            "status": "queued",
            "provider_message_id": None,
            "queued_at": queued_at,
            "sent_at": None,
        }
        expires_at = self._now_iso()
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO gateway_delivery_receipts (
                    receipt_id,
                    account_id,
                    idempotency_key,
                    provider_message_id,
                    status,
                    error_code,
                    queued_at,
                    sent_at
                ) VALUES (?, ?, ?, NULL, 'queued', NULL, ?, NULL)
                """,
                (receipt_id, account_id, idempotency_key, queued_at),
            )
            con.execute(
                """
                INSERT INTO gateway_idempotency_records (
                    account_id,
                    operation,
                    idempotency_key,
                    request_hash,
                    response_json,
                    status_code,
                    created_at,
                    expires_at
                ) VALUES (?, 'send_message', ?, ?, ?, 202, ?, ?)
                """,
                (
                    account_id,
                    idempotency_key,
                    request_hash,
                    json.dumps(response_payload, ensure_ascii=False),
                    queued_at,
                    expires_at,
                ),
            )
        return DeliveryReceiptRecord(**response_payload)

    def upsert_message(
        self,
        *,
        account_id: str,
        message_id: str,
        thread_id: str,
        subject: str,
        from_email: str,
        snippet: str,
        body_text: str,
        body_html: str | None,
        labels: list[str],
        history_id: str | None = None,
    ) -> None:
        now = self._now_iso()
        payload_json = json.dumps({"body_text": body_text, "body_html": body_html}, ensure_ascii=False)
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO gateway_messages (
                    account_id, message_id, thread_id, history_id, subject, from_email,
                    snippet, internal_ts, labels_json, payload_json, first_seen_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, message_id) DO UPDATE SET
                    thread_id = excluded.thread_id,
                    history_id = excluded.history_id,
                    subject = excluded.subject,
                    from_email = excluded.from_email,
                    snippet = excluded.snippet,
                    internal_ts = excluded.internal_ts,
                    labels_json = excluded.labels_json,
                    payload_json = excluded.payload_json,
                    updated_at = excluded.updated_at
                """,
                (
                    account_id,
                    message_id,
                    thread_id,
                    history_id,
                    subject,
                    from_email,
                    snippet,
                    now,
                    json.dumps(labels, ensure_ascii=False),
                    payload_json,
                    now,
                    now,
                ),
            )

    def search_messages(self, *, account_id: str, query: str, page_size: int) -> list[StoredMessage]:
        like = f"%{query.strip()}%"
        with self._connect() as con:
            rows = con.execute(
                """
                SELECT *
                FROM gateway_messages
                WHERE account_id = ?
                  AND (
                    subject LIKE ?
                    OR snippet LIKE ?
                    OR from_email LIKE ?
                  )
                ORDER BY internal_ts DESC
                LIMIT ?
                """,
                (account_id, like, like, like, page_size),
            ).fetchall()
        return [self._row_to_message(row) for row in rows]

    def get_message(self, *, account_id: str, message_id: str) -> StoredMessage | None:
        with self._connect() as con:
            row = con.execute(
                """
                SELECT *
                FROM gateway_messages
                WHERE account_id = ? AND message_id = ?
                """,
                (account_id, message_id),
            ).fetchone()
        return self._row_to_message(row) if row else None

    def add_label(self, *, account_id: str, message_id: str, label: str) -> bool:
        with self._connect() as con:
            row = con.execute(
                "SELECT labels_json FROM gateway_messages WHERE account_id = ? AND message_id = ?",
                (account_id, message_id),
            ).fetchone()
            if row is None:
                return False
            labels = json.loads(row["labels_json"] or "[]")
            if label not in labels:
                labels.append(label)
            con.execute(
                """
                UPDATE gateway_messages
                SET labels_json = ?, updated_at = ?
                WHERE account_id = ? AND message_id = ?
                """,
                (json.dumps(labels, ensure_ascii=False), self._now_iso(), account_id, message_id),
            )
        return True

    @staticmethod
    def _row_to_message(row: sqlite3.Row) -> StoredMessage:
        payload = json.loads(row["payload_json"] or "{}")
        labels = json.loads(row["labels_json"] or "[]")
        return StoredMessage(
            account_id=str(row["account_id"]),
            message_id=str(row["message_id"]),
            thread_id=str(row["thread_id"]),
            subject=str(row["subject"] or ""),
            from_email=str(row["from_email"] or ""),
            snippet=str(row["snippet"] or ""),
            internal_ts=str(row["internal_ts"] or ""),
            body_text=str(payload.get("body_text", "")),
            body_html=payload.get("body_html"),
            labels=[str(item) for item in labels],
        )
