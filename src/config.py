import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

VERSION: str = "0.22.0"

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
_raw_chat_ids = os.getenv("ALLOWED_CHAT_IDS", "")
ALLOWED_CHAT_IDS: set[int] = {
    int(chat_id.strip()) for chat_id in _raw_chat_ids.split(",") if chat_id.strip()
}
if not ALLOWED_USER_IDS and not ALLOWED_CHAT_IDS:
    print(
        "WARNING: ALLOWED_USER_IDS and ALLOWED_CHAT_IDS are empty — the bot will ignore ALL messages.\n"
        "  Add Telegram user IDs and/or chat IDs to .env.\n"
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
TELEGRAM_REQUEST_TIMEOUT_SECONDS: float = float(
    os.getenv("TELEGRAM_REQUEST_TIMEOUT_SECONDS", "90")
)
TELEGRAM_POLLING_TIMEOUT_SECONDS: int = int(
    os.getenv("TELEGRAM_POLLING_TIMEOUT_SECONDS", "30")
)
TELEGRAM_BACKOFF_MIN_SECONDS: float = float(
    os.getenv("TELEGRAM_BACKOFF_MIN_SECONDS", "1.0")
)
TELEGRAM_BACKOFF_MAX_SECONDS: float = float(
    os.getenv("TELEGRAM_BACKOFF_MAX_SECONDS", "30.0")
)
TELEGRAM_BACKOFF_FACTOR: float = float(os.getenv("TELEGRAM_BACKOFF_FACTOR", "1.5"))
TELEGRAM_BACKOFF_JITTER: float = float(os.getenv("TELEGRAM_BACKOFF_JITTER", "0.1"))
CODEX_TRANSIENT_MAX_RETRIES: int = int(os.getenv("CODEX_TRANSIENT_MAX_RETRIES", "2"))
CODEX_TRANSIENT_RETRY_BACKOFF_SECONDS: float = float(
    os.getenv("CODEX_TRANSIENT_RETRY_BACKOFF_SECONDS", "2.0")
)
HEALTH_INVARIANTS_ENABLED: bool = (
    os.getenv("HEALTH_INVARIANTS_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
)
HEALTH_INVARIANTS_MAX_CHARS: int = int(os.getenv("HEALTH_INVARIANTS_MAX_CHARS", "1200"))
HEALTH_INVARIANTS_STALE_HOURS: int = int(os.getenv("HEALTH_INVARIANTS_STALE_HOURS", "72"))
HEALTH_INVARIANTS_PROVIDER_FAIL_WARN_RATIO: float = float(
    os.getenv("HEALTH_INVARIANTS_PROVIDER_FAIL_WARN_RATIO", "0.30")
)
HEALTH_INVARIANTS_EMPTY_WARN_RATIO: float = float(
    os.getenv("HEALTH_INVARIANTS_EMPTY_WARN_RATIO", "0.20")
)
HEALTH_INVARIANTS_MIN_SAMPLE_SIZE: int = int(os.getenv("HEALTH_INVARIANTS_MIN_SAMPLE_SIZE", "5"))
CONTEXT_COMPILER_ENABLED: bool = (
    os.getenv("CONTEXT_COMPILER_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
)
CONTEXT_COMPILER_MAX_CHARS: int = int(os.getenv("CONTEXT_COMPILER_MAX_CHARS", "1600"))
CONTEXT_COMPACTION_ENABLED: bool = (
    os.getenv("CONTEXT_COMPACTION_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
)
CONTEXT_COMPACTION_LIGHT_THRESHOLD_CHARS: int = int(
    os.getenv("CONTEXT_COMPACTION_LIGHT_THRESHOLD_CHARS", "12000")
)
CONTEXT_COMPACTION_AGGRESSIVE_THRESHOLD_CHARS: int = int(
    os.getenv("CONTEXT_COMPACTION_AGGRESSIVE_THRESHOLD_CHARS", "20000")
)
CONTEXT_COMPACTION_LIGHT_BLOCK_CHARS: int = int(
    os.getenv("CONTEXT_COMPACTION_LIGHT_BLOCK_CHARS", "1800")
)
CONTEXT_COMPACTION_AGGRESSIVE_BLOCK_CHARS: int = int(
    os.getenv("CONTEXT_COMPACTION_AGGRESSIVE_BLOCK_CHARS", "900")
)
SCOPE_SNAPSHOT_ENABLED: bool = (
    os.getenv("SCOPE_SNAPSHOT_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
)
SCOPE_SNAPSHOT_MAX_AGE_MINUTES: int = int(os.getenv("SCOPE_SNAPSHOT_MAX_AGE_MINUTES", "180"))
SCOPE_SNAPSHOT_COMPLETED_HASHES_LIMIT: int = int(
    os.getenv("SCOPE_SNAPSHOT_COMPLETED_HASHES_LIMIT", "20")
)
PROGRESS_DEBOUNCE_SECONDS: float = float(os.getenv("PROGRESS_DEBOUNCE_SECONDS", "3.0"))
METRICS_PORT: int = int(os.getenv("METRICS_PORT", "9101"))
AUTONOMY_ENABLED: bool = os.getenv("AUTONOMY_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
AUTONOMY_FAILURE_THRESHOLD: int = int(os.getenv("AUTONOMY_FAILURE_THRESHOLD", "3"))
AUTONOMY_FAILURE_WINDOW_MINUTES: int = int(os.getenv("AUTONOMY_FAILURE_WINDOW_MINUTES", "60"))
AUTONOMY_ALERT_COOLDOWN_MINUTES: int = int(os.getenv("AUTONOMY_ALERT_COOLDOWN_MINUTES", "30"))
STEP_PLAN_AUTO_TRIGGER_ENABLED: bool = os.getenv(
    "STEP_PLAN_AUTO_TRIGGER_ENABLED", "1"
).strip().lower() not in {"0", "false", "no"}
STEP_PLAN_DEFAULT_FOLDER: str = os.getenv("STEP_PLAN_DEFAULT_FOLDER", "").strip()

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
TOOL_DENYLIST: set[str] = {
    item.strip().lower()
    for item in (os.getenv("TOOL_DENYLIST", "") or "").split(",")
    if item.strip()
}
TOOL_REQUIRE_APPROVAL_FOR_RISKY: bool = (
    os.getenv("TOOL_REQUIRE_APPROVAL_FOR_RISKY", "0").strip().lower() in {"1", "true", "yes"}
)
