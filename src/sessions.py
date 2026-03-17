import json
import logging
from pathlib import Path
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

SESSIONS_FILE = Path("sessions.json")


@dataclass
class ChatSession:
    claude_session_id: str | None = None
    codex_session_id: str | None = None
    model: str = "sonnet"
    codex_model: str | None = None
    provider: str | None = None
    chat_id: int | None = None
    message_thread_id: int | None = None
    topic_label: str | None = None
    topic_started_at: str | None = None
    last_activity_at: str | None = None


def make_scope_key(chat_id: int, message_thread_id: int | None = None) -> str:
    thread_segment = "main" if message_thread_id is None else str(message_thread_id)
    return f"{chat_id}:{thread_segment}"


def _parse_scope_key(raw: str | int) -> tuple[int, int | None]:
    if isinstance(raw, int):
        return raw, None
    text = str(raw)
    parts = text.split(":", 1)
    if len(parts) == 2:
        chat_id = int(parts[0])
        thread_raw = parts[1]
        if thread_raw == "main":
            return chat_id, None
        return chat_id, int(thread_raw)
    # Legacy: plain chat_id string
    return int(text), None


class SessionManager:
    def __init__(self) -> None:
        self._sessions: dict[str, ChatSession] = {}
        self._load()

    def _load(self) -> None:
        if not SESSIONS_FILE.exists():
            return
        try:
            data = json.loads(SESSIONS_FILE.read_text())
            for raw_key, info in data.items():
                chat_id, message_thread_id = _parse_scope_key(raw_key)
                key = make_scope_key(chat_id, message_thread_id)
                session = ChatSession(**info)
                if session.chat_id is None:
                    session.chat_id = chat_id
                if session.message_thread_id is None:
                    session.message_thread_id = message_thread_id
                self._sessions[key] = session
            logger.info("Loaded %d sessions", len(self._sessions))
        except Exception:
            logger.exception("Failed to load sessions file")

    def _save(self) -> None:
        data = {str(k): asdict(v) for k, v in self._sessions.items()}
        SESSIONS_FILE.write_text(json.dumps(data, indent=2))

    @property
    def sessions(self) -> dict[str, "ChatSession"]:
        return self._sessions

    def get(self, chat_id: int, message_thread_id: int | None = None) -> ChatSession:
        key = make_scope_key(chat_id, message_thread_id)
        if key not in self._sessions:
            self._sessions[key] = ChatSession(chat_id=chat_id, message_thread_id=message_thread_id)
        return self._sessions[key]

    def update_session_id(
        self,
        chat_id: int,
        session_id: str,
        message_thread_id: int | None = None,
    ) -> None:
        session = self.get(chat_id, message_thread_id)
        session.claude_session_id = session_id
        self.touch_thread(chat_id, message_thread_id)
        self._save()

    def new_conversation(self, chat_id: int, message_thread_id: int | None = None) -> None:
        session = self.get(chat_id, message_thread_id)
        session.claude_session_id = None
        self.touch_thread(chat_id, message_thread_id)
        self._save()

    def set_model(self, chat_id: int, model: str, message_thread_id: int | None = None) -> None:
        session = self.get(chat_id, message_thread_id)
        session.model = model
        self.touch_thread(chat_id, message_thread_id)
        self._save()

    def set_codex_model(
        self,
        chat_id: int,
        model: str | None,
        message_thread_id: int | None = None,
    ) -> None:
        session = self.get(chat_id, message_thread_id)
        session.codex_model = model
        self.touch_thread(chat_id, message_thread_id)
        self._save()

    def set_provider(self, chat_id: int, provider: str, message_thread_id: int | None = None) -> None:
        session = self.get(chat_id, message_thread_id)
        session.provider = provider
        self.touch_thread(chat_id, message_thread_id)
        self._save()

    def update_codex_session_id(
        self,
        chat_id: int,
        session_id: str,
        message_thread_id: int | None = None,
    ) -> None:
        session = self.get(chat_id, message_thread_id)
        session.codex_session_id = session_id
        self.touch_thread(chat_id, message_thread_id)
        self._save()

    def new_codex_conversation(self, chat_id: int, message_thread_id: int | None = None) -> None:
        session = self.get(chat_id, message_thread_id)
        session.codex_session_id = None
        self.touch_thread(chat_id, message_thread_id)
        self._save()

    def touch_thread(
        self,
        chat_id: int,
        message_thread_id: int | None,
        topic_label: str | None = None,
        *,
        replace_topic_label: bool = False,
    ) -> None:
        session = self.get(chat_id, message_thread_id)
        now_iso = datetime.now(timezone.utc).isoformat()
        session.last_activity_at = now_iso
        if topic_label and topic_label.strip():
            clean_label = topic_label.strip()
            should_replace = replace_topic_label or session.topic_label is None
            if should_replace and session.topic_label != clean_label:
                session.topic_label = clean_label
                if session.topic_started_at is None:
                    session.topic_started_at = now_iso
        elif session.topic_started_at is None:
            session.topic_started_at = now_iso
        self._save()

    def list_tracked_threads(self, chat_id: int) -> list[dict[str, str | int | None]]:
        rows: list[dict[str, str | int | None]] = []
        for key, session in self._sessions.items():
            if session.chat_id != chat_id:
                continue
            rows.append(
                {
                    "scope_key": key,
                    "chat_id": session.chat_id,
                    "message_thread_id": session.message_thread_id,
                    "topic_label": session.topic_label,
                    "topic_started_at": session.topic_started_at,
                    "last_activity_at": session.last_activity_at,
                }
            )
        rows.sort(key=lambda row: str(row.get("message_thread_id") or ""))
        return rows
