import json
import logging
from pathlib import Path
from dataclasses import dataclass, field, asdict

logger = logging.getLogger(__name__)

SESSIONS_FILE = Path("sessions.json")


@dataclass
class ChatSession:
    claude_session_id: str | None = None
    model: str = "sonnet"


class SessionManager:
    def __init__(self) -> None:
        self._sessions: dict[int, ChatSession] = {}
        self._load()

    def _load(self) -> None:
        if not SESSIONS_FILE.exists():
            return
        try:
            data = json.loads(SESSIONS_FILE.read_text())
            for chat_id_str, info in data.items():
                self._sessions[int(chat_id_str)] = ChatSession(**info)
            logger.info("Loaded %d sessions", len(self._sessions))
        except Exception:
            logger.exception("Failed to load sessions file")

    def _save(self) -> None:
        data = {str(k): asdict(v) for k, v in self._sessions.items()}
        SESSIONS_FILE.write_text(json.dumps(data, indent=2))

    def get(self, chat_id: int) -> ChatSession:
        if chat_id not in self._sessions:
            self._sessions[chat_id] = ChatSession()
        return self._sessions[chat_id]

    def update_session_id(self, chat_id: int, session_id: str) -> None:
        session = self.get(chat_id)
        session.claude_session_id = session_id
        self._save()

    def new_conversation(self, chat_id: int) -> None:
        session = self.get(chat_id)
        session.claude_session_id = None
        self._save()

    def set_model(self, chat_id: int, model: str) -> None:
        session = self.get(chat_id)
        session.model = model
        self._save()
