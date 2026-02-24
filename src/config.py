import os

from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN: str = os.environ["TELEGRAM_BOT_TOKEN"]

_raw_ids = os.getenv("ALLOWED_USER_IDS", "")
ALLOWED_USER_IDS: set[int] = {
    int(uid.strip()) for uid in _raw_ids.split(",") if uid.strip()
}

DEFAULT_MODEL: str = os.getenv("DEFAULT_MODEL", "sonnet")
_raw_working_dir = os.getenv("CLAUDE_WORKING_DIR") or None
CLAUDE_WORKING_DIR: str | None = (
    os.path.expanduser(_raw_working_dir) if _raw_working_dir else None
)
if CLAUDE_WORKING_DIR:
    os.makedirs(CLAUDE_WORKING_DIR, exist_ok=True)
MAX_RESPONSE_TIMEOUT: int = int(os.getenv("MAX_RESPONSE_TIMEOUT", "300"))
METRICS_PORT: int = int(os.getenv("METRICS_PORT", "9101"))
