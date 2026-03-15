from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from .schema import ensure_gateway_schema


@dataclass(frozen=True)
class AccountAuthState:
    account_id: str
    gmail_email: str | None
    status: str
    auth_state: str
    invalid_grant_count: int


@dataclass(frozen=True)
class ConnectSession:
    session_id: str
    account_id: str
    redirect_url: str
    status: str
    created_at: str
    expires_at: str
    completed_at: str | None


@dataclass(frozen=True)
class TokenBundle:
    token_id: str
    access_token: str
    refresh_token: str | None
    expires_at: str | None


class AuthStore:
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
    def _token_ciphertext(raw: str) -> bytes:
        # Placeholder before KMS integration: persist opaque bytes shape.
        return raw.encode("utf-8")

    def upsert_account(self, *, account_id: str, gmail_email: str | None = None) -> None:
        now = self._now_iso()
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO gateway_accounts (
                    account_id, gmail_email, status, auth_state, oauth_subject, created_at, updated_at
                ) VALUES (?, ?, 'active', 'connected', NULL, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    gmail_email = excluded.gmail_email,
                    status = 'active',
                    auth_state = 'connected',
                    updated_at = excluded.updated_at
                """,
                (account_id, gmail_email, now, now),
            )

    def upsert_token(
        self,
        *,
        token_id: str,
        account_id: str,
        access_token_ciphertext: bytes,
        refresh_token_ciphertext: bytes | None,
        scopes: str,
        kms_key_version: str,
        expires_at: str | None,
    ) -> None:
        now = self._now_iso()
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO gateway_oauth_tokens (
                    token_id,
                    account_id,
                    access_token_ciphertext,
                    refresh_token_ciphertext,
                    token_type,
                    scopes,
                    expires_at,
                    kms_key_version,
                    rotation_state,
                    invalid_grant_count,
                    last_invalid_grant_at,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, 'Bearer', ?, ?, ?, 'active', 0, NULL, ?, ?)
                ON CONFLICT(token_id) DO UPDATE SET
                    access_token_ciphertext = excluded.access_token_ciphertext,
                    refresh_token_ciphertext = excluded.refresh_token_ciphertext,
                    scopes = excluded.scopes,
                    expires_at = excluded.expires_at,
                    kms_key_version = excluded.kms_key_version,
                    updated_at = excluded.updated_at
                """,
                (
                    token_id,
                    account_id,
                    access_token_ciphertext,
                    refresh_token_ciphertext,
                    scopes,
                    expires_at,
                    kms_key_version,
                    now,
                    now,
                ),
            )

    def mark_invalid_grant(self, *, account_id: str) -> None:
        now = self._now_iso()
        with self._connect() as con:
            con.execute(
                """
                UPDATE gateway_oauth_tokens
                SET invalid_grant_count = invalid_grant_count + 1,
                    last_invalid_grant_at = ?,
                    updated_at = ?
                WHERE account_id = ?
                """,
                (now, now, account_id),
            )
            con.execute(
                """
                UPDATE gateway_accounts
                SET status = 'reauth_required',
                    auth_state = 'expired',
                    updated_at = ?
                WHERE account_id = ?
                """,
                (now, account_id),
            )

    def get_active_access_token(self, *, account_id: str) -> str | None:
        bundle = self.get_active_token_bundle(account_id=account_id)
        return bundle.access_token if bundle else None

    def get_active_token_bundle(self, *, account_id: str) -> TokenBundle | None:
        with self._connect() as con:
            row = con.execute(
                """
                SELECT token_id, access_token_ciphertext, refresh_token_ciphertext, expires_at
                FROM gateway_oauth_tokens
                WHERE account_id = ?
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (account_id,),
            ).fetchone()
        if row is None:
            return None
        access = row["access_token_ciphertext"]
        if access is None:
            return None
        refresh = row["refresh_token_ciphertext"]
        access_token = access.decode("utf-8") if isinstance(access, bytes) else str(access)
        refresh_token = None
        if refresh is not None:
            refresh_token = refresh.decode("utf-8") if isinstance(refresh, bytes) else str(refresh)
        return TokenBundle(
            token_id=str(row["token_id"]),
            access_token=access_token,
            refresh_token=refresh_token,
            expires_at=str(row["expires_at"]) if row["expires_at"] else None,
        )

    def rotate_access_token(
        self,
        *,
        token_id: str,
        access_token: str,
        expires_at: str | None,
    ) -> None:
        now = self._now_iso()
        with self._connect() as con:
            con.execute(
                """
                UPDATE gateway_oauth_tokens
                SET access_token_ciphertext = ?,
                    expires_at = ?,
                    updated_at = ?
                WHERE token_id = ?
                """,
                (self._token_ciphertext(access_token), expires_at, now, token_id),
            )

    def get_account_auth_state(self, *, account_id: str) -> AccountAuthState | None:
        with self._connect() as con:
            row = con.execute(
                """
                SELECT
                    a.account_id,
                    a.gmail_email,
                    a.status,
                    a.auth_state,
                    COALESCE(MAX(t.invalid_grant_count), 0) AS invalid_grant_count
                FROM gateway_accounts a
                LEFT JOIN gateway_oauth_tokens t ON t.account_id = a.account_id
                WHERE a.account_id = ?
                GROUP BY a.account_id, a.status, a.auth_state
                """,
                (account_id,),
            ).fetchone()
        if row is None:
            return None
        return AccountAuthState(
            account_id=str(row["account_id"]),
            gmail_email=str(row["gmail_email"]) if row["gmail_email"] else None,
            status=str(row["status"]),
            auth_state=str(row["auth_state"]),
            invalid_grant_count=int(row["invalid_grant_count"]),
        )

    def start_connect_session(self, *, account_id: str, redirect_url: str) -> ConnectSession:
        now = self._now_iso()
        session_id = f"oauth-{uuid4().hex}"
        expires_at = self._now_iso()
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO gateway_accounts (
                    account_id, gmail_email, status, auth_state, oauth_subject, created_at, updated_at
                ) VALUES (?, NULL, 'active', 'not_connected', NULL, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    updated_at = excluded.updated_at
                """,
                (account_id, now, now),
            )
            con.execute(
                """
                INSERT INTO gateway_oauth_sessions (
                    session_id, account_id, redirect_url, status, created_at, expires_at, completed_at
                ) VALUES (?, ?, ?, 'pending', ?, ?, NULL)
                """,
                (session_id, account_id, redirect_url, now, expires_at),
            )
        return ConnectSession(
            session_id=session_id,
            account_id=account_id,
            redirect_url=redirect_url,
            status="pending",
            created_at=now,
            expires_at=expires_at,
            completed_at=None,
        )

    def get_connect_session(self, *, session_id: str) -> ConnectSession | None:
        with self._connect() as con:
            row = con.execute(
                """
                SELECT session_id, account_id, redirect_url, status, created_at, expires_at, completed_at
                FROM gateway_oauth_sessions
                WHERE session_id = ?
                """,
                (session_id,),
            ).fetchone()
        if row is None:
            return None
        return ConnectSession(
            session_id=str(row["session_id"]),
            account_id=str(row["account_id"]),
            redirect_url=str(row["redirect_url"]),
            status=str(row["status"]),
            created_at=str(row["created_at"]),
            expires_at=str(row["expires_at"]),
            completed_at=str(row["completed_at"]) if row["completed_at"] else None,
        )

    def complete_connect_session(
        self,
        *,
        session_id: str,
        gmail_email: str,
        access_token: str,
        refresh_token: str,
        scopes: str,
        expires_at: str | None,
        kms_key_version: str = "kms-dev-v1",
    ) -> AccountAuthState | None:
        now = self._now_iso()
        with self._connect() as con:
            row = con.execute(
                """
                SELECT account_id, status
                FROM gateway_oauth_sessions
                WHERE session_id = ?
                """,
                (session_id,),
            ).fetchone()
            if row is None:
                return None
            account_id = str(row["account_id"])
            con.execute(
                """
                UPDATE gateway_oauth_sessions
                SET status = 'completed',
                    completed_at = ?
                WHERE session_id = ?
                """,
                (now, session_id),
            )
            con.execute(
                """
                UPDATE gateway_accounts
                SET gmail_email = ?,
                    status = 'active',
                    auth_state = 'connected',
                    updated_at = ?
                WHERE account_id = ?
                """,
                (gmail_email, now, account_id),
            )
            token_id = f"tok-{uuid4().hex}"
            con.execute(
                """
                INSERT INTO gateway_oauth_tokens (
                    token_id,
                    account_id,
                    access_token_ciphertext,
                    refresh_token_ciphertext,
                    token_type,
                    scopes,
                    expires_at,
                    kms_key_version,
                    rotation_state,
                    invalid_grant_count,
                    last_invalid_grant_at,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, 'Bearer', ?, ?, ?, 'active', 0, NULL, ?, ?)
                """,
                (
                    token_id,
                    account_id,
                    self._token_ciphertext(access_token),
                    self._token_ciphertext(refresh_token),
                    scopes,
                    expires_at,
                    kms_key_version,
                    now,
                    now,
                ),
            )
        return self.get_account_auth_state(account_id=account_id)

    def disconnect_account(self, *, account_id: str) -> bool:
        now = self._now_iso()
        with self._connect() as con:
            row = con.execute(
                "SELECT account_id FROM gateway_accounts WHERE account_id = ?",
                (account_id,),
            ).fetchone()
            if row is None:
                return False
            con.execute(
                "DELETE FROM gateway_oauth_tokens WHERE account_id = ?",
                (account_id,),
            )
            con.execute(
                """
                UPDATE gateway_accounts
                SET status = 'disabled',
                    auth_state = 'revoked',
                    updated_at = ?
                WHERE account_id = ?
                """,
                (now, account_id),
            )
        return True
