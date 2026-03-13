import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

VERSION: str = "0.34.0"

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
VOICE_TRANSCRIPTION_MAX_CONCURRENCY: int = max(
    1,
    int(os.getenv("VOICE_TRANSCRIPTION_MAX_CONCURRENCY", "1")),
)
_voice_transcription_default_threads = max(
    1,
    (os.cpu_count() or 1) // VOICE_TRANSCRIPTION_MAX_CONCURRENCY,
)
VOICE_TRANSCRIPTION_THREADS: int = max(
    1,
    int(os.getenv("VOICE_TRANSCRIPTION_THREADS", str(_voice_transcription_default_threads))),
)
METRICS_PORT: int = int(os.getenv("METRICS_PORT", "9101"))
EMBEDDED_SCHEDULER_ENABLED: bool = (
    os.getenv("EMBEDDED_SCHEDULER_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
)
_raw_scheduler_notify_chat_id = os.getenv("SCHEDULER_NOTIFY_CHAT_ID", "").strip()
SCHEDULER_NOTIFY_CHAT_ID: int | None = (
    int(_raw_scheduler_notify_chat_id) if _raw_scheduler_notify_chat_id else None
)
_raw_scheduler_notify_thread_id = os.getenv("SCHEDULER_NOTIFY_THREAD_ID", "").strip()
SCHEDULER_NOTIFY_THREAD_ID: int | None = (
    int(_raw_scheduler_notify_thread_id) if _raw_scheduler_notify_thread_id else None
)
SCHEDULER_NOTIFY_LEVEL: str = os.getenv("SCHEDULER_NOTIFY_LEVEL", "failures").strip().lower() or "failures"
AUTONOMY_ENABLED: bool = os.getenv("AUTONOMY_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
AUTONOMY_FAILURE_THRESHOLD: int = int(os.getenv("AUTONOMY_FAILURE_THRESHOLD", "3"))
AUTONOMY_FAILURE_WINDOW_MINUTES: int = int(os.getenv("AUTONOMY_FAILURE_WINDOW_MINUTES", "60"))
AUTONOMY_ALERT_COOLDOWN_MINUTES: int = int(os.getenv("AUTONOMY_ALERT_COOLDOWN_MINUTES", "30"))
_f08_mode = os.getenv("F08_GOVERNANCE_MODE", "shadow").strip().lower()
F08_GOVERNANCE_MODE: str = (
    _f08_mode
    if _f08_mode in {"shadow", "enforce_limited", "enforce_scoped", "enforce_full"}
    else "shadow"
)
F08_ENFORCEMENT_SCOPE: str = (
    os.getenv("F08_ENFORCEMENT_SCOPE", "self_mod_only").strip().lower() or "self_mod_only"
)
STEP_PLAN_AUTO_TRIGGER_ENABLED: bool = os.getenv(
    "STEP_PLAN_AUTO_TRIGGER_ENABLED", "1"
).strip().lower() not in {"0", "false", "no"}
STEP_PLAN_DEFAULT_FOLDER: str = os.getenv("STEP_PLAN_DEFAULT_FOLDER", "").strip()
TELEGRAM_PROXY_BASE_URL: str = os.getenv("TELEGRAM_PROXY_BASE_URL", "").strip().rstrip("/")
TELEGRAM_PROXY_API_KEY: str = os.getenv("TELEGRAM_PROXY_API_KEY", "").strip()
TELEGRAM_PROXY_REQUEST_TIMEOUT_SECONDS: float = float(
    os.getenv("TELEGRAM_PROXY_REQUEST_TIMEOUT_SECONDS", "120")
)
TELEGRAM_PROXY_BIND_HOST: str = os.getenv("TELEGRAM_PROXY_BIND_HOST", "127.0.0.1").strip() or "127.0.0.1"
TELEGRAM_PROXY_BIND_PORT: int = int(os.getenv("TELEGRAM_PROXY_BIND_PORT", "8787"))
_raw_proxy_allowed_channel_ids = os.getenv("TELEGRAM_PROXY_ALLOWED_CHANNEL_IDS", "")
TELEGRAM_PROXY_ALLOWED_CHANNEL_IDS: set[int] = {
    int(item.strip())
    for item in _raw_proxy_allowed_channel_ids.split(",")
    if item.strip()
}
_raw_proxy_allowed_chat_ids = os.getenv("TELEGRAM_PROXY_ALLOWED_CHAT_IDS", "")
TELEGRAM_PROXY_ALLOWED_CHAT_IDS: set[int] = {
    int(item.strip())
    for item in _raw_proxy_allowed_chat_ids.split(",")
    if item.strip()
}
TELEGRAM_PROXY_KEY_CREDENTIAL_NAME: str = (
    os.getenv("TELEGRAM_PROXY_KEY_CREDENTIAL_NAME", "telegram_proxy_key").strip()
    or "telegram_proxy_key"
)
_raw_proxy_key_fallback_path = os.getenv("TELEGRAM_PROXY_KEY_FALLBACK_PATH", "").strip()
TELEGRAM_PROXY_KEY_FALLBACK_PATH: Path | None = (
    Path(os.path.expanduser(_raw_proxy_key_fallback_path)) if _raw_proxy_key_fallback_path else None
)
TELEGRAM_PROXY_ENCRYPTED_CREDENTIALS: str = os.getenv(
    "TELEGRAM_PROXY_ENCRYPTED_CREDENTIALS",
    "",
).strip()
TELEGRAM_DIGEST_COLLECT_LIMIT: int = max(10, int(os.getenv("TELEGRAM_DIGEST_COLLECT_LIMIT", "200")))
TELEGRAM_DIGEST_SOURCE_LIMIT: int = max(1, int(os.getenv("TELEGRAM_DIGEST_SOURCE_LIMIT", "200")))
TELEGRAM_DIGEST_COLLECT_INTERVAL_MINUTES: int = max(
    30,
    int(os.getenv("TELEGRAM_DIGEST_COLLECT_INTERVAL_MINUTES", "180")),
)
TELEGRAM_DIGEST_WINDOW_HOURS: int = max(1, int(os.getenv("TELEGRAM_DIGEST_WINDOW_HOURS", "24")))

# ── Memory system ─────────────────────────────────────────
_raw_memory_dir = os.getenv("MEMORY_DIR") or None
MEMORY_DIR: Path = Path(
    os.path.expanduser(_raw_memory_dir) if _raw_memory_dir else "memory"
)
os.makedirs(MEMORY_DIR, exist_ok=True)
TELEGRAM_DIGEST_DB_PATH: Path = MEMORY_DIR / "telegram_digest.db"
TELEGRAM_DIGEST_BRIEF_PATH: Path = MEMORY_DIR / "telegram_digest_brief.md"
TELEGRAM_PROXY_SESSION_PATH: Path = MEMORY_DIR / "telethon_user_proxy"

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

# ── Gmail bootstrap web flow ──────────────────────────────
GMAIL_BOOTSTRAP_GOOGLE_CLIENT_ID: str = os.getenv("GMAIL_BOOTSTRAP_GOOGLE_CLIENT_ID", "").strip()
GMAIL_BOOTSTRAP_GOOGLE_CLIENT_SECRET: str = os.getenv(
    "GMAIL_BOOTSTRAP_GOOGLE_CLIENT_SECRET", ""
).strip()
GMAIL_BOOTSTRAP_GOOGLE_SCOPES: tuple[str, ...] = tuple(
    item.strip()
    for item in (
        os.getenv(
            "GMAIL_BOOTSTRAP_GOOGLE_SCOPES",
            "openid,email,https://www.googleapis.com/auth/cloud-platform",
        )
    ).split(",")
    if item.strip()
)
