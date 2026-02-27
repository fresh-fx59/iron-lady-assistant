import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

VERSION: str = "0.16.19"

# ── Bot token (required) ────────────────────────────────────
BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
if not BOT_TOKEN or BOT_TOKEN == "your-bot-token-here":
    print(
        "ERROR: TELEGRAM_BOT_TOKEN is not set.\n"
        "\n"
        "  Quick fix:  bash setup.sh\n"
        "  Manual fix: edit the .env file and add your bot token.\n"
        "  Get a token from @BotFather on Telegram: https://t.me/BotFather\n"
    )
    sys.exit(1)

# ── Allowed users (required) ────────────────────────────────
_raw_ids = os.getenv("ALLOWED_USER_IDS", "")
ALLOWED_USER_IDS: set[int] = {
    int(uid.strip()) for uid in _raw_ids.split(",") if uid.strip()
}
if not ALLOWED_USER_IDS:
    print(
        "WARNING: ALLOWED_USER_IDS is empty — the bot will ignore ALL messages.\n"
        "  Add your Telegram user ID to .env (find it via @userinfobot on Telegram).\n"
    )

# ── Model & optional settings ───────────────────────────────
DEFAULT_MODEL: str = os.getenv("DEFAULT_MODEL", "sonnet")
_raw_working_dir = os.getenv("CLAUDE_WORKING_DIR") or None
CLAUDE_WORKING_DIR: str | None = (
    os.path.expanduser(_raw_working_dir) if _raw_working_dir else None
)
if CLAUDE_WORKING_DIR:
    os.makedirs(CLAUDE_WORKING_DIR, exist_ok=True)
IDLE_TIMEOUT: int = int(os.getenv("IDLE_TIMEOUT", "120"))
PROGRESS_DEBOUNCE_SECONDS: float = float(os.getenv("PROGRESS_DEBOUNCE_SECONDS", "3.0"))
METRICS_PORT: int = int(os.getenv("METRICS_PORT", "9101"))

# ── Memory system ─────────────────────────────────────────
_raw_memory_dir = os.getenv("MEMORY_DIR") or None
MEMORY_DIR: Path = Path(
    os.path.expanduser(_raw_memory_dir) if _raw_memory_dir else "memory"
)
os.makedirs(MEMORY_DIR, exist_ok=True)

# ── Tool system ───────────────────────────────────────────
_raw_tools_dir = os.getenv("TOOLS_DIR") or None
TOOLS_DIR: Path = Path(
    os.path.expanduser(_raw_tools_dir) if _raw_tools_dir else "tools"
)
# Note: TOOLS_DIR is optional — no auto-create, tools/ may not exist
