import logging
from prometheus_client import Counter, Histogram, Gauge, Info, start_http_server

logger = logging.getLogger(__name__)

# Bot info
BOT_INFO = Info("telegrambot", "Telegram Claude Code bot information")

# Message counters
MESSAGES_TOTAL = Counter(
    "telegrambot_messages_total",
    "Total messages received",
    ["status"],  # success, error, unauthorized, busy
)

# Claude invocation metrics
CLAUDE_REQUESTS_TOTAL = Counter(
    "telegrambot_claude_requests_total",
    "Total Claude CLI invocations",
    ["model", "status"],  # status: success, error, timeout
)

CLAUDE_RESPONSE_DURATION = Histogram(
    "telegrambot_claude_response_duration_seconds",
    "Claude response time in seconds",
    ["model"],
    buckets=[1, 2, 5, 10, 20, 30, 60, 120, 300, 600, 1200],
)

CLAUDE_COST_USD = Counter(
    "telegrambot_claude_cost_usd_total",
    "Total Claude API cost in USD",
    ["model"],
)

CLAUDE_TURNS_TOTAL = Counter(
    "telegrambot_claude_turns_total",
    "Total Claude agentic turns",
    ["model"],
)

# Active sessions
ACTIVE_SESSIONS = Gauge(
    "telegrambot_active_sessions",
    "Number of active chat sessions",
)

# Background task metrics
BG_TASKS_ACTIVE = Gauge(
    "telegrambot_bg_tasks_active",
    "Number of active background tasks (queued + running)",
)

BG_TASKS_QUEUED = Gauge(
    "telegrambot_bg_tasks_queued",
    "Number of queued background tasks",
)

BG_TASKS_RUNNING = Gauge(
    "telegrambot_bg_tasks_running",
    "Number of running background tasks",
)

BG_TASKS_TOTAL = Counter(
    "telegrambot_bg_tasks_total",
    "Total background tasks submitted",
    ["status"],  # completed, failed, cancelled, timeout
)

# Current model gauge (set per chat, last used)
CURRENT_MODEL = Gauge(
    "telegrambot_current_model",
    "Currently selected model per chat",
    ["model"],
)

# Process uptime is automatically exported by prometheus_client


def start_metrics_server(port: int) -> None:
    """Start the Prometheus metrics HTTP server."""
    start_http_server(port)
    logger.info("Prometheus metrics server started on port %d", port)
