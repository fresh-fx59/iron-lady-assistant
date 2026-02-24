import os

from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN: str = os.environ["TELEGRAM_BOT_TOKEN"]

_raw_ids = os.getenv("ALLOWED_USER_IDS", "")
ALLOWED_USER_IDS: set[int] = {
    int(uid.strip()) for uid in _raw_ids.split(",") if uid.strip()
}

DEFAULT_MODEL: str = os.getenv("DEFAULT_MODEL", "sonnet")
CLAUDE_WORKING_DIR: str | None = os.getenv("CLAUDE_WORKING_DIR") or None
MAX_RESPONSE_TIMEOUT: int = int(os.getenv("MAX_RESPONSE_TIMEOUT", "300"))
METRICS_PORT: int = int(os.getenv("METRICS_PORT", "9101"))
