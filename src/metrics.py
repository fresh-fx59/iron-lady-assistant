import logging
from collections import defaultdict, deque
from threading import Lock
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

# F18: monitor-only cost intelligence observability
COST_INTEL_TURN_COST_USD = Histogram(
    "telegrambot_cost_intel_turn_cost_usd",
    "Per-turn cost distribution for monitor-only cost intelligence",
    ["provider", "model", "mode", "status"],
    buckets=[0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

COST_INTEL_TURN_DURATION_MS = Histogram(
    "telegrambot_cost_intel_turn_duration_ms",
    "Per-turn duration distribution (ms) for monitor-only cost intelligence",
    ["provider", "model", "mode", "status"],
    buckets=[100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000, 180000, 600000],
)

COST_INTEL_TOOL_COUNT = Histogram(
    "telegrambot_cost_intel_tool_count",
    "Tool call count per turn for monitor-only cost intelligence",
    ["provider", "model", "mode", "tool_mix"],
    buckets=[0, 1, 2, 3, 5, 8, 13, 21],
)

COST_INTEL_STEERING_EVENTS = Histogram(
    "telegrambot_cost_intel_steering_event_count",
    "Steering events observed per turn",
    ["provider", "model", "mode"],
    buckets=[0, 1, 2, 3, 5, 8, 13],
)

COST_INTEL_MESSAGE_SIZE_BUCKET_TOTAL = Counter(
    "telegrambot_cost_intel_message_size_bucket_total",
    "Message size buckets for monitor-only cost intelligence",
    ["provider", "model", "mode", "direction", "bucket"],
)

COST_INTEL_STEP_PLAN_ACTIVE_TOTAL = Counter(
    "telegrambot_cost_intel_step_plan_active_total",
    "Turns observed while step plan mode is active",
    ["provider", "model", "mode"],
)

COST_INTEL_TAXONOMY_TOTAL = Counter(
    "telegrambot_cost_intel_taxonomy_total",
    "Cost intelligence taxonomy counters (monitor-only)",
    ["category", "provider", "model", "mode"],
)

_CORE_TOOL_NAMES = frozenset({
    "read",
    "write",
    "edit",
    "grep",
    "glob",
    "list",
    "ls",
    "bash",
    "task",
    "askuserquestion",
    "enterplanmode",
    "exitplanmode",
    "web_search",
    "weather",
    "sports",
    "finance",
    "time",
    "open",
    "click",
    "find",
    "screenshot",
    "image_query",
})
_HIGH_COST_SUCCESS_USD = 0.02
_TOOL_COST_INFLATION_MIN_COUNT = 6
_SCOPE_HOTSPOT_MIN_SUCCESS_TURNS = 5
_SCOPE_HOTSPOT_MIN_AVG_COST_USD = 0.02
_SCOPE_HOTSPOT_WINDOW = 20

_scope_cost_window: dict[str, deque[float]] = defaultdict(
    lambda: deque(maxlen=_SCOPE_HOTSPOT_WINDOW)
)
_scope_cost_lock = Lock()

# Process uptime is automatically exported by prometheus_client


def start_metrics_server(port: int) -> None:
    """Start the Prometheus metrics HTTP server."""
    if port <= 0:
        logger.info("Prometheus metrics server disabled (METRICS_PORT=%d)", port)
        return
    try:
        start_http_server(port)
        logger.info("Prometheus metrics server started on port %d", port)
    except OSError as exc:
        logger.warning(
            "Prometheus metrics server disabled: failed to bind port %d (%s)",
            port,
            exc,
        )


def _message_size_bucket(size: int) -> str:
    if size <= 0:
        return "0"
    if size <= 64:
        return "1_64"
    if size <= 256:
        return "65_256"
    if size <= 1024:
        return "257_1024"
    if size <= 4096:
        return "1025_4096"
    return "4097_plus"


def _tool_mix(tool_names: list[str]) -> str:
    if not tool_names:
        return "none"
    normalized = [name.strip().lower() for name in tool_names if name and name.strip()]
    if not normalized:
        return "none"
    core_count = sum(1 for name in normalized if name in _CORE_TOOL_NAMES)
    if core_count == len(normalized):
        return "core_only"
    if core_count == 0:
        return "extended_only"
    return "mixed"


def _record_scope_success_cost(scope_key: str, cost_usd: float, is_success: bool) -> bool:
    if not scope_key or not is_success or cost_usd <= 0:
        return False
    with _scope_cost_lock:
        window = _scope_cost_window[scope_key]
        window.append(cost_usd)
        if len(window) < _SCOPE_HOTSPOT_MIN_SUCCESS_TURNS:
            return False
        average_cost = sum(window) / len(window)
        return average_cost >= _SCOPE_HOTSPOT_MIN_AVG_COST_USD


def observe_cost_intelligence_turn(
    *,
    scope_key: str,
    provider: str,
    model: str,
    mode: str,
    cost_usd: float,
    num_turns: int,
    duration_ms: float,
    is_error: bool,
    is_cancelled: bool,
    is_empty_response: bool,
    tool_timeout: bool,
    tool_names: list[str] | None,
    message_size_in: int,
    message_size_out: int,
    step_plan_active: bool,
    steering_event_count: int,
    attempts: int,
) -> list[str]:
    provider_label = (provider or "unknown").strip()
    model_label = (model or "unknown").strip()
    mode_label = (mode or "unknown").strip()

    status = "cancelled" if is_cancelled else ("error" if is_error else "success")
    tools = list(tool_names or [])
    tool_mix = _tool_mix(tools)

    COST_INTEL_TURN_DURATION_MS.labels(
        provider=provider_label, model=model_label, mode=mode_label, status=status
    ).observe(max(0.0, duration_ms))
    COST_INTEL_TOOL_COUNT.labels(
        provider=provider_label, model=model_label, mode=mode_label, tool_mix=tool_mix
    ).observe(len(tools))

    if step_plan_active:
        COST_INTEL_STEP_PLAN_ACTIVE_TOTAL.labels(
            provider=provider_label,
            model=model_label,
            mode=mode_label,
        ).inc()
    if steering_event_count > 0:
        COST_INTEL_STEERING_EVENTS.labels(
            provider=provider_label,
            model=model_label,
            mode=mode_label,
        ).observe(steering_event_count)

    COST_INTEL_MESSAGE_SIZE_BUCKET_TOTAL.labels(
        provider=provider_label,
        model=model_label,
        mode=mode_label,
        direction="in",
        bucket=_message_size_bucket(message_size_in),
    ).inc()
    COST_INTEL_MESSAGE_SIZE_BUCKET_TOTAL.labels(
        provider=provider_label,
        model=model_label,
        mode=mode_label,
        direction="out",
        bucket=_message_size_bucket(message_size_out),
    ).inc()

    if cost_usd > 0:
        COST_INTEL_TURN_COST_USD.labels(
            provider=provider_label, model=model_label, mode=mode_label, status=status
        ).observe(cost_usd)

    categories: list[str] = []
    if cost_usd >= _HIGH_COST_SUCCESS_USD and not is_error and not is_empty_response:
        categories.append("high_cost_success")
    if cost_usd > 0 and is_error:
        categories.append("cost_with_error")
    if cost_usd > 0 and is_empty_response:
        categories.append("cost_with_empty")
    if cost_usd > 0 and (attempts > 1 or tool_timeout):
        categories.append("retry_amplified_cost")
    if cost_usd > 0 and len(tools) >= _TOOL_COST_INFLATION_MIN_COUNT and tool_mix != "none":
        categories.append("tool_driven_cost_inflation")
    if _record_scope_success_cost(scope_key, cost_usd, is_success=(not is_error and not is_cancelled)):
        categories.append("scope_hotspot")

    for category in categories:
        COST_INTEL_TAXONOMY_TOTAL.labels(
            category=category,
            provider=provider_label,
            model=model_label,
            mode=mode_label,
        ).inc()

    logger.info(
        "cost_intel_turn scope=%s provider=%s model=%s mode=%s status=%s cost=%.6f "
        "turns=%d duration_ms=%.1f attempts=%d tool_count=%d tool_mix=%s tool_timeout=%s "
        "empty=%s step_plan_active=%s steering_events=%d taxonomy=%s",
        scope_key,
        provider_label,
        model_label,
        mode_label,
        status,
        cost_usd,
        num_turns,
        duration_ms,
        attempts,
        len(tools),
        tool_mix,
        tool_timeout,
        is_empty_response,
        step_plan_active,
        steering_event_count,
        ",".join(categories) if categories else "-",
    )
    return categories
