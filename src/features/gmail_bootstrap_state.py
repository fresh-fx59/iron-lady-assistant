from __future__ import annotations

import json
import threading
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

GmailBootstrapPhase = Literal[
    "created",
    "cloud_auth_pending",
    "cloud_auth_granted",
    "project_bootstrapped",
    "oauth_manual_pending",
    "credentials_uploaded",
    "gmail_auth_pending",
    "completed",
    "failed",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class GmailBootstrapSession:
    session_id: str
    created_at: str
    updated_at: str
    phase: GmailBootstrapPhase
    project_id: str
    project_name: str
    redirect_uri: str
    callback_base_url: str
    oauth_client_name: str
    telegram_chat_id: int | None = None
    telegram_thread_id: int | None = None
    gcloud_account_email: str | None = None
    project_number: str | None = None
    manual_console_url: str | None = None
    manual_checklist_path: str | None = None
    credentials_path: str | None = None
    gmail_account_email: str | None = None
    connected_at: str | None = None
    last_telegram_notification_key: str | None = None
    failure_reason: str | None = None


class GmailBootstrapStateStore:
    """Persist browser-first Gmail bootstrap sessions in a local JSON file."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def _load_all_unlocked(self) -> dict[str, GmailBootstrapSession]:
        if not self._path.exists():
            return {}
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        if not isinstance(data, dict):
            return {}

        sessions: dict[str, GmailBootstrapSession] = {}
        for session_id, row in data.items():
            if not isinstance(row, dict):
                continue
            try:
                sessions[session_id] = GmailBootstrapSession(**row)
            except TypeError:
                continue
        return sessions

    def _save_all_unlocked(self, sessions: dict[str, GmailBootstrapSession]) -> None:
        payload = {key: asdict(value) for key, value in sessions.items()}
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self._path)

    def start_session(
        self,
        *,
        project_id: str,
        project_name: str,
        redirect_uri: str,
        callback_base_url: str,
        oauth_client_name: str,
        telegram_chat_id: int | None = None,
        telegram_thread_id: int | None = None,
    ) -> GmailBootstrapSession:
        with self._lock:
            sessions = self._load_all_unlocked()
            now = _now_iso()
            session = GmailBootstrapSession(
                session_id=str(uuid.uuid4()),
                created_at=now,
                updated_at=now,
                phase="cloud_auth_pending",
                project_id=project_id,
                project_name=project_name,
                redirect_uri=redirect_uri,
                callback_base_url=callback_base_url.rstrip("/"),
                oauth_client_name=oauth_client_name,
                telegram_chat_id=telegram_chat_id,
                telegram_thread_id=telegram_thread_id,
            )
            sessions[session.session_id] = session
            self._save_all_unlocked(sessions)
            return session

    def get(self, session_id: str) -> GmailBootstrapSession | None:
        with self._lock:
            return self._load_all_unlocked().get(session_id)

    def record_cloud_auth(self, *, session_id: str, account_email: str) -> GmailBootstrapSession | None:
        with self._lock:
            sessions = self._load_all_unlocked()
            session = sessions.get(session_id)
            if session is None:
                return None
            session.gcloud_account_email = account_email
            session.phase = "cloud_auth_granted"
            session.updated_at = _now_iso()
            sessions[session_id] = session
            self._save_all_unlocked(sessions)
            return session

    def record_project_bootstrap(
        self,
        *,
        session_id: str,
        project_number: str | None = None,
        manual_console_url: str | None = None,
        manual_checklist_path: str | None = None,
    ) -> GmailBootstrapSession | None:
        with self._lock:
            sessions = self._load_all_unlocked()
            session = sessions.get(session_id)
            if session is None:
                return None
            session.project_number = project_number
            session.manual_console_url = manual_console_url
            session.manual_checklist_path = manual_checklist_path
            session.phase = "oauth_manual_pending"
            session.updated_at = _now_iso()
            sessions[session_id] = session
            self._save_all_unlocked(sessions)
            return session

    def record_credentials_uploaded(
        self,
        *,
        session_id: str,
        credentials_path: str,
    ) -> GmailBootstrapSession | None:
        with self._lock:
            sessions = self._load_all_unlocked()
            session = sessions.get(session_id)
            if session is None:
                return None
            session.credentials_path = credentials_path
            session.phase = "credentials_uploaded"
            session.updated_at = _now_iso()
            sessions[session_id] = session
            self._save_all_unlocked(sessions)
            return session

    def record_gmail_auth_started(self, *, session_id: str) -> GmailBootstrapSession | None:
        with self._lock:
            sessions = self._load_all_unlocked()
            session = sessions.get(session_id)
            if session is None:
                return None
            if session.gmail_account_email is None:
                session.gmail_account_email = ""
            session.phase = "gmail_auth_pending"
            session.updated_at = _now_iso()
            sessions[session_id] = session
            self._save_all_unlocked(sessions)
            return session

    def record_gmail_auth_started_for_account(
        self,
        *,
        session_id: str,
        gmail_account_email: str,
    ) -> GmailBootstrapSession | None:
        with self._lock:
            sessions = self._load_all_unlocked()
            session = sessions.get(session_id)
            if session is None:
                return None
            session.gmail_account_email = gmail_account_email
            session.phase = "gmail_auth_pending"
            session.updated_at = _now_iso()
            sessions[session_id] = session
            self._save_all_unlocked(sessions)
            return session

    def record_completed(self, *, session_id: str, gmail_account_email: str) -> GmailBootstrapSession | None:
        with self._lock:
            sessions = self._load_all_unlocked()
            session = sessions.get(session_id)
            if session is None:
                return None
            session.gmail_account_email = gmail_account_email
            session.connected_at = _now_iso()
            session.phase = "completed"
            session.updated_at = _now_iso()
            sessions[session_id] = session
            self._save_all_unlocked(sessions)
            return session

    def list_completed(self) -> list[GmailBootstrapSession]:
        with self._lock:
            sessions = self._load_all_unlocked()
            completed = [session for session in sessions.values() if session.phase == "completed"]
            completed.sort(key=lambda item: item.updated_at, reverse=True)
            return completed

    def latest_for_scope(
        self,
        *,
        telegram_chat_id: int,
        telegram_thread_id: int | None,
    ) -> GmailBootstrapSession | None:
        with self._lock:
            sessions = self._load_all_unlocked().values()
            matching = [
                session
                for session in sessions
                if session.telegram_chat_id == telegram_chat_id
                and session.telegram_thread_id == telegram_thread_id
            ]
            if not matching:
                return None
            matching.sort(key=lambda item: item.updated_at, reverse=True)
            return matching[0]

    def record_failed(self, *, session_id: str, reason: str) -> GmailBootstrapSession | None:
        with self._lock:
            sessions = self._load_all_unlocked()
            session = sessions.get(session_id)
            if session is None:
                return None
            session.failure_reason = reason
            session.phase = "failed"
            session.updated_at = _now_iso()
            sessions[session_id] = session
            self._save_all_unlocked(sessions)
            return session

    def record_telegram_notification(
        self,
        *,
        session_id: str,
        notification_key: str,
    ) -> GmailBootstrapSession | None:
        with self._lock:
            sessions = self._load_all_unlocked()
            session = sessions.get(session_id)
            if session is None:
                return None
            session.last_telegram_notification_key = notification_key
            session.updated_at = _now_iso()
            sessions[session_id] = session
            self._save_all_unlocked(sessions)
            return session
