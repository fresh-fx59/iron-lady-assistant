import asyncio
from dataclasses import dataclass, replace
import hashlib
import html
import inspect
import json
import logging
import os
import re
import shutil
import subprocess
from time import monotonic
from uuid import uuid4
from datetime import datetime, timezone as tz
from pathlib import Path
import yaml
from aiogram import Router, F
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import Message, CallbackQuery, ErrorEvent
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ChatAction
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter

from . import bridge, config, metrics, transcribe
from .core.context_plugins import ContextPluginRegistry
from .sessions import ChatSession, SessionManager, make_scope_key
from .formatter import markdown_to_html, split_message, strip_html
from .features.state_store import ResumeStateStore, SteeringEvent, SteeringLedgerStore
from .f08_governance import F08GovernanceAdvisory
from .media import (
    extract_media_directives,
    is_audio_media,
    is_voice_compatible_media,
    prepared_media_input,
    resolve_media_input,
    strip_tool_directive_lines,
)
from .memory import MemoryManager
from .progress import ProgressReporter
from .providers import ProviderManager
from .scheduler import ScheduleManager
from .plugins.tools_plugin import ToolRegistry
from .self_modify import SelfModificationManager
from .tasks import TaskManager, TaskStatus

logger = logging.getLogger(__name__)
router = Router()

session_manager = SessionManager()
provider_manager = ProviderManager()
memory_manager = MemoryManager(config.MEMORY_DIR)
resume_state_store = ResumeStateStore(config.MEMORY_DIR / "resume_envelopes.json")
steering_ledger_store = SteeringLedgerStore(config.MEMORY_DIR / "steering_ledger.json")
tool_registry = ToolRegistry(
    config.TOOLS_DIR,
    denylist=config.TOOL_DENYLIST,
    require_approval_for_risky=config.TOOL_REQUIRE_APPROVAL_FOR_RISKY,
)
context_plugins = ContextPluginRegistry([tool_registry])
self_mod_manager = SelfModificationManager(Path(__file__).resolve().parent.parent)
f08_advisory = F08GovernanceAdvisory()
task_manager: TaskManager | None = None  # Set in main()
schedule_manager: ScheduleManager | None = None  # Set in main()

# Restore persisted provider selections from sessions
for _scope_id, _session in session_manager.sessions.items():
    if _session.provider:
        provider_manager.set_provider(_scope_id, _session.provider)

CLAUDE_MODELS = {"sonnet", "opus", "haiku"}
VALID_MODELS = CLAUDE_MODELS


@dataclass
class _ChatState:
    """State for each active conversation scope (chat + optional thread)."""
    lock: asyncio.Lock
    process_handle: dict | None  # Will contain {"proc": proc} when running
    cancel_requested: bool
    reset_requested: bool


# Per-conversation state dict
_chat_states: dict[str, _ChatState] = {}
_error_counts: dict[str, int] = {}
_recent_outbound_by_scope: dict[str, tuple[str, datetime]] = {}
_CODEX_TRANSIENT_ERROR_PATTERNS = (
    re.compile(r"stream disconnected before completion", re.IGNORECASE),
    re.compile(r"transport error:\s*timeout", re.IGNORECASE),
    re.compile(r"\breconnecting\.\.\.\s*\d+/\d+", re.IGNORECASE),
    re.compile(r"\b(etimedout|econnreset|connection reset)\b", re.IGNORECASE),
)
_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png"}
_INCOMING_MEDIA_DIR = config.MEMORY_DIR / "incoming_media"
_EMPTY_RESPONSE_FALLBACK_TEXT = (
    "I received an empty response from the provider. "
    "Please resend your last message."
)
_AUDIO_PROGRESS_UPDATE_INTERVAL = 1.0
_VOICE_TRANSCRIPTION_PROGRESS_INTERVAL = 2.0
# Backward-compatible state paths retained for tests/fixtures importing these symbols.
_STEP_PLAN_STATE_PATH = config.MEMORY_DIR / "step_plan_state.json"
_SCOPE_SNAPSHOT_PATH = config.MEMORY_DIR / "scope_snapshot.json"
_STEP_PLAN_FILE_PATTERN = re.compile(r"^(\d+)\s*-\s*.+\.md$", re.IGNORECASE)
_STEP_PLAN_AUTO_TRIGGER_RE = re.compile(r"\bcontinue\b.*\bplan\b", re.IGNORECASE)
_STEP_PLAN_PATH_HINT_RE = re.compile(r"(/[^\n]*Ouroboros Improvement Plan[^\n]*)")
_STEP_PLAN_FALLBACK_PATHS = (
    "/home/claude-developer/syncthing/data/syncthing-main/Obsidian/DefaultObsidianVault/"
    "Projects/Iron Lady Assistant/Ouroboros Improvement Plan",
)
_STEP_PLAN_AUTORESUME_FAILURE_THRESHOLD = 2
_STEP_PLAN_AUTORESUME_BLOCK_MINUTES = 30
_MIDFLIGHT_TOKEN_RE = re.compile(r"\w+", re.UNICODE)
_APPLIED_CHECK_RE = re.compile(r"^\s*Applied:\s*\[(x|X)\]\s*$", re.IGNORECASE | re.MULTILINE)
_NUMBER_WORDS = {
    "one": "1",
    "two": "2",
    "three": "3",
    "four": "4",
    "five": "5",
    "six": "6",
    "seven": "7",
    "eight": "8",
    "nine": "9",
    "ten": "10",
}
_STEERING_CONFLICT_PATTERNS = (
    (re.compile(r"\b(delete|drop|erase|wipe|destroy)\b", re.IGNORECASE), "destructive_action"),
    (re.compile(r"\b(ignore|disregard)\s+(all|everything|previous|prior)\b", re.IGNORECASE), "broad_override"),
    (re.compile(r"\b(secret|password|token|credential)\b", re.IGNORECASE), "sensitive_data"),
)


def _thread_id(message: Message) -> int | None:
    return getattr(message, "message_thread_id", None)


def _scope_key(chat_id: int, message_thread_id: int | None = None) -> str:
    return make_scope_key(chat_id, message_thread_id)


def _message_log_context(message: Message) -> dict[str, object]:
    caption = getattr(message, "caption", None)
    text = message.text or caption or ""
    voice = getattr(message, "voice", None)
    photo = getattr(message, "photo", None)
    return {
        "chat_id": message.chat.id,
        "thread_id": _thread_id(message),
        "message_id": getattr(message, "message_id", None),
        "user_id": message.from_user and message.from_user.id,
        "content_type": getattr(message, "content_type", None),
        "text_len": len(text),
        "has_caption": bool(caption),
        "voice_duration": getattr(voice, "duration", None) if voice else None,
        "photo_count": len(photo) if photo else 0,
    }


def _format_schedule_label(item) -> str:  # noqa: ANN001
    if item.schedule_type == "weekly" and item.daily_time and item.weekly_day is not None:
        tz_name = item.timezone_name or "UTC"
        weekday = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][item.weekly_day]
        return f"weekly {weekday} {item.daily_time} ({tz_name})"
    if item.schedule_type == "daily" and item.daily_time:
        tz_name = item.timezone_name or "UTC"
        return f"daily at {item.daily_time} ({tz_name})"
    return f"every {item.interval_minutes} min"


def _format_schedule_run_status(run) -> str:  # noqa: ANN001
    if run.status == "submission_failed":
        return "submission failed"
    if run.status == "failed_recovered":
        return "failed after restart"
    if run.status == "submitted":
        return "queued"
    return run.status.replace("_", " ")


def _format_schedule_run_summary(run) -> str:  # noqa: ANN001
    planned_local = run.planned_for.astimezone().strftime("%Y-%m-%d %H:%M")
    status_label = _format_schedule_run_status(run)
    if run.started_at:
        started_local = run.started_at.astimezone().strftime("%Y-%m-%d %H:%M")
        return f"{status_label}; planned {planned_local}; started {started_local}"
    return f"{status_label}; planned {planned_local}"


def _format_active_schedule_summary(item) -> str:  # noqa: ANN001
    if not item.current_status or not item.current_planned_for:
        return "idle"
    planned_local = item.current_planned_for.astimezone().strftime("%Y-%m-%d %H:%M")
    status_label = _format_schedule_run_status(type("RunLike", (), {"status": item.current_status})())
    if item.current_started_at:
        started_local = item.current_started_at.astimezone().strftime("%Y-%m-%d %H:%M")
        return f"{status_label}; planned {planned_local}; started {started_local}"
    if item.current_submitted_at:
        submitted_local = item.current_submitted_at.astimezone().strftime("%Y-%m-%d %H:%M")
        return f"{status_label}; planned {planned_local}; submitted {submitted_local}"
    return f"{status_label}; planned {planned_local}"


def _log_incoming_message(message: Message, route: str) -> None:
    ctx = _message_log_context(message)
    logger.info(
        "Incoming %s message: chat=%s thread=%s message=%s user=%s type=%s text_len=%s caption=%s voice_duration=%s photo_count=%s",
        route,
        ctx["chat_id"],
        ctx["thread_id"],
        ctx["message_id"],
        ctx["user_id"],
        ctx["content_type"],
        ctx["text_len"],
        ctx["has_caption"],
        ctx["voice_duration"],
        ctx["photo_count"],
    )


def _scope_key_from_message(message: Message) -> str:
    return _scope_key(message.chat.id, _thread_id(message))


def _worklog_subprocess_env(
    base_env: dict[str, str] | None,
    *,
    chat_id: int,
    message_thread_id: int | None,
    provider: object,
    session: ChatSession,
) -> dict[str, str]:
    env = dict(base_env or {})
    session_type = "codex" if _is_codex_family_cli(getattr(provider, "cli", None)) else "claude"
    session_id = session.codex_session_id if session_type == "codex" else session.claude_session_id
    env["ILA_WORKLOG_SCOPE_KEY"] = _scope_key(chat_id, message_thread_id)
    env["ILA_WORKLOG_CHAT_ID"] = str(chat_id)
    env["ILA_WORKLOG_SESSION_TYPE"] = session_type
    env["ILA_WORKLOG_PROVIDER"] = str(getattr(provider, "name", "") or "")
    env["ILA_WORKLOG_LAST_ACTIVITY_AT"] = str(session.last_activity_at or datetime.now(tz.utc).isoformat())
    if session_id:
        env["ILA_WORKLOG_SESSION_ID"] = session_id
    else:
        env.pop("ILA_WORKLOG_SESSION_ID", None)
    if message_thread_id is not None:
        env["ILA_WORKLOG_MESSAGE_THREAD_ID"] = str(message_thread_id)
    else:
        env.pop("ILA_WORKLOG_MESSAGE_THREAD_ID", None)
    if session.topic_label:
        env["ILA_WORKLOG_TOPIC_LABEL"] = session.topic_label
    else:
        env.pop("ILA_WORKLOG_TOPIC_LABEL", None)
    if session.topic_started_at:
        env["ILA_WORKLOG_TOPIC_STARTED_AT"] = session.topic_started_at
    else:
        env.pop("ILA_WORKLOG_TOPIC_STARTED_AT", None)
    return env


def _get_state(scope_key: str) -> _ChatState:
    """Get or create state for a conversation scope."""
    if scope_key not in _chat_states:
        _chat_states[scope_key] = _ChatState(
            lock=asyncio.Lock(),
            process_handle=None,
            cancel_requested=False,
            reset_requested=False,
        )
    return _chat_states[scope_key]


def _is_authorized(user_id: int | None, chat_id: int | None = None) -> bool:
    raw_user_ids = os.getenv("ALLOWED_USER_IDS", "")
    allowed_users = set(config.ALLOWED_USER_IDS)
    if raw_user_ids:
        parsed = {int(uid.strip()) for uid in raw_user_ids.split(",") if uid.strip()}
        if parsed:
            allowed_users |= parsed
            config.ALLOWED_USER_IDS = allowed_users

    raw_chat_ids = os.getenv("ALLOWED_CHAT_IDS", "")
    allowed_chats = set(config.ALLOWED_CHAT_IDS)
    if raw_chat_ids:
        parsed = {int(cid.strip()) for cid in raw_chat_ids.split(",") if cid.strip()}
        if parsed:
            allowed_chats |= parsed
            config.ALLOWED_CHAT_IDS = allowed_chats

    if user_id is not None and user_id in allowed_users:
        return True
    if chat_id is not None and chat_id in allowed_chats:
        return True
    return False


def _is_admin(user_id: int | None) -> bool:
    if user_id is None:
        return False
    return user_id in config.ALLOWED_USER_IDS


def _actor_id(message: Message) -> int:
    """Use user ID when available; fall back to chat ID for channels."""
    if message.from_user and message.from_user.id:
        return message.from_user.id
    return message.chat.id


def _topic_label_from_message(message: Message, override_text: str | None = None) -> str | None:
    created = getattr(message, "forum_topic_created", None)
    if created and getattr(created, "name", None):
        return str(created.name).strip()

    edited = getattr(message, "forum_topic_edited", None)
    if edited and getattr(edited, "name", None):
        return str(edited.name).strip()

    raw = (override_text or message.text or "").strip()
    if not raw or raw.startswith("/"):
        return None
    return raw.splitlines()[0][:120]


def _touch_thread_context(message: Message, override_text: str | None = None) -> None:
    chat_id = message.chat.id
    thread_id = _thread_id(message)
    if thread_id is None:
        return
    session_manager.touch_thread(
        chat_id=chat_id,
        message_thread_id=thread_id,
        topic_label=_topic_label_from_message(message, override_text=override_text),
    )


def _record_error(scope_key: str) -> int:
    count = _error_counts.get(scope_key, 0) + 1
    _error_counts[scope_key] = count
    return count


def _clear_errors(scope_key: str) -> None:
    _error_counts.pop(scope_key, None)


def _classify_steering_event(text: str) -> str:
    normalized = text.strip().lower()
    if re.search(r"\b(cancel|stop|abort)\b", normalized):
        return "cancel"
    if re.search(r"\b(priority|prioritize|focus)\b", normalized):
        return "priority_shift"
    if re.search(r"\b(remove|drop|ignore|disregard)\b", normalized):
        return "constraint_remove"
    if re.search(r"\b(correction|actually|instead|wrong|not)\b", normalized):
        return "correction"
    if re.search(r"\b(must|should|only|never|don't|do not)\b", normalized):
        return "constraint_add"
    return "clarify"


def _collect_conflict_flags(text: str) -> list[str]:
    flags: list[str] = []
    for pattern, flag in _STEERING_CONFLICT_PATTERNS:
        if pattern.search(text):
            flags.append(flag)
    return flags


def _create_steering_event(message: Message, text: str) -> SteeringEvent:
    event_type = _classify_steering_event(text)
    return SteeringEvent(
        event_id=str(uuid4()),
        created_at=datetime.now(tz.utc).isoformat(),
        source_message_id=str(message.message_id),
        event_type=event_type,
        text=text.strip(),
        intent_patch=f"{event_type}: {text.strip()}",
        conflict_flags=_collect_conflict_flags(text),
    )


def _build_steering_patch(base_prompt: str, events: list[SteeringEvent]) -> str:
    lines = [
        "Continue the in-flight task from the current progress.",
        f"Original user request: {base_prompt.strip()}",
        "Apply all follow-up steering updates in order:",
    ]
    for idx, event in enumerate(events, start=1):
        flags = f" (flags: {', '.join(event.conflict_flags)})" if event.conflict_flags else ""
        lines.append(f"{idx}. [{event.event_type}] {event.text}{flags}")
    lines.append("Keep already completed useful work unless a steering update explicitly cancels it.")
    return "\n".join(lines)


def _has_high_risk_conflict(events: list[SteeringEvent]) -> bool:
    return any("destructive_action" in event.conflict_flags for event in events)


def _should_suggest_rollback(scope_key: str) -> bool:
    return _error_counts.get(scope_key, 0) >= 3


def _outbound_digest(text: str) -> str:
    normalized = re.sub(r"\s+", " ", text).strip()
    if not normalized:
        return ""
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:16]


def _has_recent_outbound(scope_key: str, text: str, *, ttl_seconds: int = 120) -> bool:
    """Check whether the same chunk was sent recently in this scope."""
    digest = _outbound_digest(text)
    if not digest:
        return False
    previous = _recent_outbound_by_scope.get(scope_key)
    if not previous:
        return False
    prev_digest, prev_at = previous
    now = datetime.now(tz.utc)
    return prev_digest == digest and (now - prev_at).total_seconds() <= ttl_seconds


def _remember_outbound(scope_key: str, text: str) -> None:
    digest = _outbound_digest(text)
    if not digest:
        return
    _recent_outbound_by_scope[scope_key] = (digest, datetime.now(tz.utc))


def _is_duplicate_outbound(scope_key: str, text: str, *, ttl_seconds: int = 120) -> bool:
    """Suppress immediate duplicate replies in the same scope after retries/restarts."""
    normalized = re.sub(r"\s+", " ", text).strip()
    if not normalized:
        return False
    return _has_recent_outbound(scope_key, normalized, ttl_seconds=ttl_seconds)


def _latest_scope_target() -> tuple[int, int | None] | None:
    """Best-effort target for restart notices based on most recent session activity."""
    rows: list[tuple[datetime, int, int | None]] = []
    for session in session_manager.sessions.values():
        chat_id = int(getattr(session, "chat_id", 0) or 0)
        if not chat_id:
            continue
        user_hint = chat_id if chat_id > 0 else None
        if not (_is_authorized(user_hint, chat_id) or chat_id in config.ALLOWED_CHAT_IDS):
            continue
        raw_last = str(getattr(session, "last_activity_at", "") or "")
        try:
            last_at = datetime.fromisoformat(raw_last)
            if last_at.tzinfo is None:
                last_at = last_at.replace(tzinfo=tz.utc)
        except Exception:
            last_at = datetime.min.replace(tzinfo=tz.utc)
        rows.append((last_at, chat_id, getattr(session, "message_thread_id", None)))
    if not rows:
        return None
    rows.sort(key=lambda row: row[0], reverse=True)
    _, chat_id, message_thread_id = rows[0]
    return chat_id, message_thread_id


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _build_rollback_suggestion_markup(scope_key: str, user_id: int | None):
    if not _is_admin(user_id) or not _should_suggest_rollback(scope_key):
        return None
    kb = InlineKeyboardBuilder()
    kb.button(text="Show rollback options", callback_data="rollback_auto")
    return kb.as_markup()


def _truncate_label(text: str, max_len: int = 52) -> str:
    return text if len(text) <= max_len else text[: max_len - 3] + "..."


def _truncate_output(text: str, max_len: int = 2000) -> str:
    if len(text) <= max_len:
        return text
    remaining = len(text) - max_len
    return f"{text[:max_len]}\n... ({remaining} chars omitted)"


def _as_text(value: object) -> str:
    return value if isinstance(value, str) else ""


def _message_base_text(message: Message, override_text: str | None = None) -> str:
    if override_text is not None:
        return _as_text(override_text).strip()
    return (_as_text(getattr(message, "text", None)) or _as_text(getattr(message, "caption", None))).strip()


async def _download_photo_attachment(message: Message) -> str | None:
    photos = getattr(message, "photo", None) or []
    if not photos:
        return None
    try:
        largest = photos[-1]
        tg_file = await message.bot.get_file(largest.file_id)
        suffix = Path(tg_file.file_path or "").suffix.lower() or ".jpg"
        if suffix not in _IMAGE_EXTENSIONS:
            suffix = ".jpg"
        _INCOMING_MEDIA_DIR.mkdir(parents=True, exist_ok=True)
        target_path = _INCOMING_MEDIA_DIR / f"{message.chat.id}_{message.message_id}_{uuid4().hex[:8]}{suffix}"
        await message.bot.download_file(tg_file.file_path, destination=target_path)
        return str(target_path)
    except Exception:
        logger.exception("Failed to download photo attachment for message %s", message.message_id)
        return None


async def _compose_incoming_prompt(message: Message, override_text: str | None = None) -> str:
    base_text = _message_base_text(message, override_text)
    image_path = await _download_photo_attachment(message)
    if not image_path:
        return base_text
    attachment_block = (
        "User attached an image.\n"
        f"Local image path: {image_path}\n"
        "Inspect this image when answering."
    )
    if base_text:
        return f"{base_text}\n\n{attachment_block}"
    return attachment_block


def _inject_tool_request(prompt_text: str, tool_name: str) -> str:
    """Force a tool to be activated by adding an explicit directive."""
    base = prompt_text.rstrip()
    return f"{base}\n\nUSE_TOOL: {tool_name}\n"


def _command_args(message: Message, command: CommandObject | None = None) -> str:
    """Return command arguments with optional @bot mention stripped."""
    if command is not None:
        return (command.args or "").strip()

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        return ""
    return parts[1].strip()


def _build_augmented_prompt(raw_prompt: str) -> str:
    """Compose prompt with memory, tools, and memory instructions."""
    memory_context = _as_text(memory_manager.build_context(raw_prompt))
    tool_context = _as_text(context_plugins.build_context(raw_prompt))
    memory_instructions = _as_text(memory_manager.build_instructions())

    prompt_parts: list[str] = []
    if memory_context:
        prompt_parts.append(memory_context)
    if tool_context:
        prompt_parts.append(tool_context)
    prompt_parts.append(raw_prompt + memory_instructions)
    return "\n\n".join(prompt_parts)
def _is_transient_codex_error(text: str | None) -> bool:
    if not text:
        return False
    return any(pattern.search(text) for pattern in _CODEX_TRANSIENT_ERROR_PATTERNS)


def _sanitize_transient_codex_error_response(
    response: bridge.ClaudeResponse,
    *,
    attempts: int,
) -> bridge.ClaudeResponse:
    return bridge.ClaudeResponse(
        text=(
            "The Codex stream disconnected repeatedly and did not recover after "
            f"{attempts} attempt(s). Please retry."
        ),
        session_id=response.session_id,
        is_error=True,
        cost_usd=response.cost_usd,
        duration_ms=response.duration_ms,
        num_turns=response.num_turns,
        cancelled=response.cancelled,
        idle_timeout=response.idle_timeout,
    )


def _is_voice_compatible_media(media_ref: str) -> bool:
    return is_voice_compatible_media(media_ref)


def _is_audio_media(media_ref: str) -> bool:
    return is_audio_media(media_ref)


def _resolve_media_input(media_ref: str):
    return resolve_media_input(media_ref)


def _extract_media_directives(text: str) -> tuple[str, list[str], bool]:
    return extract_media_directives(text)


def _strip_tool_directive_lines(text: str) -> str:
    return strip_tool_directive_lines(text)
def _default_timezone_name() -> str:
    profile_path = config.MEMORY_DIR / "user_profile.yaml"
    try:
        data = yaml.safe_load(profile_path.read_text()) or {}
        prefs = data.get("preferences") or {}
        tz_name = prefs.get("timezone")
        if isinstance(tz_name, str) and tz_name.strip():
            return tz_name.strip()
    except Exception:
        pass
    return "UTC"


def _strip_markdown_code_fence(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```") and stripped.endswith("```"):
        lines = stripped.splitlines()
        if len(lines) >= 2:
            return "\n".join(lines[1:-1]).strip()
    return stripped


def _weekday_to_int(name: str) -> int | None:
    mapping = {
        "mon": 0,
        "monday": 0,
        "tue": 1,
        "tues": 1,
        "tuesday": 1,
        "wed": 2,
        "wednesday": 2,
        "thu": 3,
        "thur": 3,
        "thurs": 3,
        "thursday": 3,
        "fri": 4,
        "friday": 4,
        "sat": 5,
        "saturday": 5,
        "sun": 6,
        "sunday": 6,
    }
    return mapping.get(name.strip().lower())


def _get_recent_commits(limit: int = 10) -> list[tuple[str, str, str]]:
    result = subprocess.run(
        [
            "git",
            "-C",
            str(_repo_root()),
            "log",
            f"-n{limit}",
            "--pretty=format:%H%x09%h%x09%s",
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "git log failed")

    commits: list[tuple[str, str, str]] = []
    for line in result.stdout.splitlines():
        full_hash, short_hash, subject = (line.split("\t", 2) + ["", "", ""])[:3]
        if full_hash and short_hash:
            commits.append((full_hash, short_hash, subject))
    return commits


async def _show_rollback_options(chat_id: int, bot, message_thread_id: int | None = None) -> None:
    try:
        commits = await asyncio.to_thread(_get_recent_commits, 10)
    except Exception as e:
        await bot.send_message(chat_id, f"Failed to load commit history: {e}", message_thread_id=message_thread_id)
        return

    if not commits:
        await bot.send_message(chat_id, "No commits found for rollback.", message_thread_id=message_thread_id)
        return

    kb = InlineKeyboardBuilder()
    for full_hash, short_hash, subject in commits:
        kb.button(
            text=_truncate_label(f"{short_hash} {subject}".strip()),
            callback_data=f"rollback:{full_hash}",
        )
    kb.button(text="Cancel", callback_data="rollback_cancel")
    kb.adjust(1)

    await bot.send_message(
        chat_id,
        "Select a commit to rollback to:",
        message_thread_id=message_thread_id,
        reply_markup=kb.as_markup(),
    )


async def _restart_service(chat_id: int, bot, message_thread_id: int | None = None) -> None:
    await asyncio.sleep(1)
    proc = await asyncio.create_subprocess_exec(
        "sudo",
        "systemctl",
        "restart",
        "telegram-bot.service",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        err = (stderr or b"").decode().strip() or f"exit code {proc.returncode}"
        await bot.send_message(
            chat_id,
            f"Rollback completed, but restart failed: {err[:500]}",
            message_thread_id=message_thread_id,
        )


def _reset_to_commit(target_hash: str) -> tuple[bool, str]:
    verify = subprocess.run(
        ["git", "-C", str(_repo_root()), "rev-parse", "--verify", f"{target_hash}^{{commit}}"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if verify.returncode != 0:
        return False, verify.stderr.strip() or "Commit not found"

    reset = subprocess.run(
        ["git", "-C", str(_repo_root()), "reset", "--hard", target_hash],
        capture_output=True,
        text=True,
        timeout=20,
    )
    if reset.returncode != 0:
        return False, reset.stderr.strip() or "git reset --hard failed"

    deploy_dir = _repo_root() / ".deploy"
    deploy_dir.mkdir(exist_ok=True)
    (deploy_dir / "start_times").write_text("")
    return True, (reset.stdout.strip() or f"Rolled back to {target_hash}")


def _find_provider_cli(cli_name: str) -> str | None:
    """Resolve provider executable path from current process PATH."""
    return shutil.which(cli_name)


def _current_provider(scope_key: str):
    return provider_manager.get_provider(scope_key)


def _is_codex_family_cli(cli_name: str | None) -> bool:
    return bool(cli_name and cli_name.lower().startswith("codex"))


def _current_model_label(session: object, provider) -> str:
    if _is_codex_family_cli(provider.cli):
        return session.codex_model or provider.model or "default"
    return session.model


def _step_plan_active_flag() -> bool:
    try:
        payload = json.loads(_STEP_PLAN_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return False
    return bool(payload.get("active"))


def _provider_session_id(session: object, provider) -> str | None:
    if _is_codex_family_cli(getattr(provider, "cli", None)):
        return getattr(session, "codex_session_id", None)
    return getattr(session, "claude_session_id", None)


def _model_options(provider) -> list[str]:
    if _is_codex_family_cli(provider.cli):
        return provider.models or ["default"]
    return sorted(CLAUDE_MODELS)


def _codex_model_arg(session: object, provider) -> str | None:
    model = session.codex_model or provider.model
    allowed = set(provider.models or ["default"])
    if model and model not in allowed:
        return None
    if model == "default":
        return None
    return model


def _codex_task_model(session: object, provider) -> str:
    """Return explicit non-default Codex model for background execution."""
    model = _codex_model_arg(session, provider)
    if model:
        return model

    candidates: list[str] = []
    if provider.model:
        candidates.append(provider.model)
    candidates.extend(provider.models or [])
    for candidate in candidates:
        if candidate and candidate != "default":
            return candidate
    return "gpt-5-codex"


def _scheduled_task_backend(session: object, provider) -> tuple[str, str | None, str, str | None]:
    if _is_codex_family_cli(getattr(provider, "cli", None)):
        return (
            _codex_task_model(session, provider),
            _provider_session_id(session, provider),
            provider.cli,
            getattr(provider, "resume_arg", None),
        )
    return (
        session.model,
        getattr(session, "claude_session_id", None),
        getattr(provider, "cli", "claude"),
        getattr(provider, "resume_arg", None),
    )


def _codex_working_dir() -> str:
    """Run Codex from user home so it can access files under that tree."""
    return str(Path.home())


def _reflection_stream(session: object, provider: object, prompt: str):
    """Return a backend-specific reflection stream for the active provider."""
    session_id = _provider_session_id(session, provider)
    if not session_id:
        return None
    if _is_codex_family_cli(getattr(provider, "cli", None)):
        return bridge.stream_codex_message(
            prompt=prompt,
            session_id=session_id,
            model=_codex_model_arg(session, provider),
            resume_arg=getattr(provider, "resume_arg", None),
            cli_name=getattr(provider, "cli", "codex"),
            working_dir=_codex_working_dir(),
            subprocess_env=provider_manager.subprocess_env(provider),
        )
    return bridge.stream_message(
        prompt=prompt,
        session_id=session_id,
        model="haiku",
        working_dir=config.CLAUDE_WORKING_DIR,
        subprocess_env=provider_manager.subprocess_env(provider),
    )


@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return

    # Get user timezone if set
    user_tz = None
    try:
        data = yaml.safe_load((config.MEMORY_DIR / "user_profile.yaml"))
        prefs = data.get("preferences", {})
        user_tz = prefs.get("timezone")
    except Exception:
        pass

    status_lines = [
        f"Hello! I'm a Claude Code assistant. <b>v{config.VERSION}</b>",
    ]
    if user_tz:
        try:
            from datetime import datetime, timezone as tz
            tz_obj = tz.timezone(user_tz)
            now = datetime.now(tz.utc).astimezone(tz_obj)
            time_str = now.strftime("%H:%M")
            status_lines.append(f"<b>Time:</b> {time_str} ({user_tz})")
        except Exception:
            pass

    status_lines.extend([
        "",
        "Send me any message and I'll respond using Claude.",
        "",
        "<b>Commands:</b>",
        "/new — Start a fresh conversation",
        "/model — Switch model",
        "/provider — Switch LLM provider",
        "/status — Show current session info",
        "/threads — Show tracked forum topics/threads",
        "/memory — Show what I remember",
        "/tools — Show available tools",
        "/rollback — Roll back to previous version (admin)",
        "/selfmod_stage — Stage sandbox plugin (admin)",
        "/selfmod_apply — Validate+promote sandbox plugin (admin)",
        "/schedule_every <min> <task> — Schedule recurring task",
        "/schedule_daily <HH:MM> <task> — Schedule daily recurring task",
        "/schedule_weekly <day> <HH:MM> <task> — Schedule weekly task",
        "/schedule_list — List recurring schedules",
        "/schedule_cancel <id> — Cancel recurring schedule",
        "/bg <task> — Run task in background",
        "/bg_cancel <id> — Cancel background task",
        "/cancel — Cancel current request",
    ])

    await message.answer("\n".join(status_lines), parse_mode="HTML")


@router.message(Command("new"))
async def cmd_new(message: Message) -> None:
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    chat_id = message.chat.id
    thread_id = _thread_id(message)
    scope_key = _scope_key(chat_id, thread_id)
    session = session_manager.get(chat_id, thread_id)
    provider = provider_manager.get_provider(scope_key)
    # Keep provider choice sticky per thread even when session ids are reset.
    if session.provider and session.provider != provider.name:
        restored_provider = provider_manager.set_provider(scope_key, session.provider)
        if restored_provider:
            provider = restored_provider
        else:
            session_manager.set_provider(chat_id, provider.name, thread_id)
    elif not session.provider:
        session_manager.set_provider(chat_id, provider.name, thread_id)
    if (
        os.getenv("DISABLE_REFLECTION") != "1"
        and (session.claude_session_id or session.codex_session_id)
    ):
        reflection_session: ChatSession = replace(session)
        asyncio.create_task(_reflect(chat_id, reflection_session, provider))
    state = _get_state(scope_key)
    if state.lock.locked():
        state.cancel_requested = True
        state.reset_requested = True
        proc = state.process_handle.get("proc") if state.process_handle else None
        if proc:
            kill_result = proc.kill()
            if inspect.isawaitable(kill_result):
                await kill_result
    session_manager.new_conversation(chat_id, thread_id)
    session_manager.new_codex_conversation(chat_id, thread_id)
    steering_ledger_store.clear(scope_key=scope_key)
    _clear_errors(scope_key)
    if state.lock.locked():
        await message.answer(
            "Conversation reset requested. If a request was running, it is being cancelled. "
            "Send your next message in a moment."
        )
    else:
        await message.answer("Conversation cleared. Send a message to start fresh.")


def _parse_reflection_payload(text: str) -> dict[str, object]:
    """Parse a JSON reflection payload, tolerating fenced wrappers."""
    clean = text.strip()
    if clean.startswith("```"):
        clean = clean.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
    return json.loads(clean)


async def _reflect(chat_id: int, session: object, provider: object) -> None:
    """Background: summarize the active conversation and store it as an episode."""
    try:
        repo_path = str(_repo_root())
        branch: str | None = None
        try:
            branch_result = subprocess.run(
                ["git", "-C", repo_path, "rev-parse", "--abbrev-ref", "HEAD"],
                capture_output=True,
                text=True,
                check=True,
            )
            branch = branch_result.stdout.strip() or None
        except Exception:
            logger.debug("Could not resolve git branch for reflection worklog", exc_info=True)

        reflect_prompt = (
            "Summarize this conversation concisely. Output ONLY valid JSON, no markdown:\n"
            '{"summary": "one-sentence summary", "topics": ["topic1"], '
            '"decisions": ["decision1"], "entities": ["entity1"]}'
        )
        stream = _reflection_stream(session, provider, reflect_prompt)
        if stream is None:
            return

        async for event in stream:
            if event.event_type == bridge.StreamEventType.RESULT and event.response:
                if event.response.is_error:
                    logger.warning("Chat %d: reflection returned error: %s", chat_id, event.response.text[:200])
                    return
                data = _parse_reflection_payload(event.response.text)
                memory_manager.add_episode(
                    chat_id=chat_id,
                    summary=data.get("summary", ""),
                    topics=data.get("topics"),
                    decisions=data.get("decisions"),
                    entities=data.get("entities"),
                    message_thread_id=getattr(session, "message_thread_id", None),
                    scope_key=_scope_key(chat_id, getattr(session, "message_thread_id", None)),
                    provider=getattr(provider, "name", None),
                    session_type="codex" if getattr(session, "codex_session_id", None) else "claude",
                    session_id=getattr(session, "codex_session_id", None) or getattr(session, "claude_session_id", None),
                    topic_label=getattr(session, "topic_label", None),
                    topic_started_at=getattr(session, "topic_started_at", None),
                    repo_path=repo_path,
                    branch=branch,
                )
                logger.info("Chat %d: reflection stored", chat_id)
                return
    except Exception:
        logger.warning("Chat %d: reflection failed", chat_id, exc_info=True)


@router.message(Command("model"))
async def cmd_model(message: Message, command: CommandObject | None = None) -> None:
    """Show model selection keyboard."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    chat_id = message.chat.id
    thread_id = _thread_id(message)
    scope_key = _scope_key(chat_id, thread_id)
    session = session_manager.get(chat_id, thread_id)
    provider = _current_provider(scope_key)
    current = _current_model_label(session, provider)

    args = _command_args(message, command)
    if args:
        requested = args.split()[0]
        options = _model_options(provider)
        if requested not in options:
            await message.answer(f"Invalid model: {requested}. Use /model to see options.")
            return

        if _is_codex_family_cli(provider.cli):
            chosen = None if requested == "default" else requested
            session_manager.set_codex_model(chat_id, chosen, thread_id)
        else:
            session_manager.set_model(chat_id, requested, thread_id)

        current = _current_model_label(session_manager.get(chat_id, thread_id), provider)
        await message.answer(f"Switched to {current}")
        return

    lines = [f"<b>Current model:</b> {current}\n"]
    lines.append("<b>Select a model:</b>")

    # Build inline keyboard with buttons
    keyboard = InlineKeyboardBuilder()
    for model in _model_options(provider):
        button_text = f"{'✓ ' if model == current else ''}{model}"
        keyboard.button(text=button_text, callback_data=f"model:{model}")
    keyboard.adjust(2)  # 2 buttons per row

    await message.answer("\n".join(lines), reply_markup=keyboard.as_markup(), parse_mode="HTML")


@router.callback_query(F.data.startswith("model:"))
async def cb_model_switch(callback: CallbackQuery) -> None:
    """Handle model button click."""
    if not callback.message:
        return
    if not _is_authorized(callback.from_user and callback.from_user.id, callback.message.chat.id):
        return

    chat_id = callback.message.chat.id
    thread_id = _thread_id(callback.message)
    scope_key = _scope_key(chat_id, thread_id)
    model = callback.data.split(":", 1)[1]
    logger.info("Chat %s: model selection 'model:%s'", scope_key, model)

    provider = _current_provider(scope_key)
    options = _model_options(provider)
    if model not in options:
        await callback.answer("Invalid model", show_alert=True)
        return

    if _is_codex_family_cli(provider.cli):
        chosen = None if model == "default" else model
        session_manager.set_codex_model(chat_id, chosen, thread_id)
    else:
        session_manager.set_model(chat_id, model, thread_id)

    # Update keyboard state
    current = _current_model_label(session_manager.get(chat_id, thread_id), provider)
    lines = [f"<b>Current model:</b> {current}\n"]
    lines.append("<b>Select a model:</b>")

    keyboard = InlineKeyboardBuilder()
    for m in options:
        button_text = f"{'✓ ' if m == current else ''}{m}"
        keyboard.button(text=button_text, callback_data=f"model:{m}")
    keyboard.adjust(2)  # 2 buttons per row

    await callback.message.edit_text("\n".join(lines), reply_markup=keyboard.as_markup(), parse_mode="HTML")
    await callback.answer(f"Switched to {current}")


@router.message(Command("provider"))
async def cmd_provider(message: Message, command: CommandObject | None = None) -> None:
    """Show provider selection keyboard or switch provider by argument."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return

    chat_id = message.chat.id
    thread_id = _thread_id(message)
    scope_key = _scope_key_from_message(message)
    requested = _command_args(message, command)
    if requested:
        provider = provider_manager.set_provider(scope_key, requested)
        if not provider:
            available = ", ".join(p.name for p in provider_manager.providers)
            await message.answer(f"Provider not found: {requested}\nAvailable: {available}")
            return
        session_manager.set_provider(chat_id, provider.name, thread_id)
        await message.answer(f"Switched to provider: <b>{provider.name}</b>", parse_mode="HTML")
        return

    current = provider_manager.get_provider(scope_key)

    lines = [f"<b>Current provider:</b> {current.name}\n<i>{current.description}</i>\n"]
    lines.append("<b>Select a provider:</b>")

    # Build inline keyboard with buttons
    keyboard = InlineKeyboardBuilder()
    for p in provider_manager.providers:
        button_text = f"{'✓ ' if p.name == current.name else ''}{p.name}"
        keyboard.button(text=button_text, callback_data=f"provider:{p.name}")
    keyboard.adjust(2)  # 2 buttons per row

    await message.answer("\n".join(lines), reply_markup=keyboard.as_markup(), parse_mode="HTML")


@router.callback_query(F.data.startswith("provider:"))
async def cb_provider_switch(callback: CallbackQuery) -> None:
    """Handle provider button click."""
    if not callback.message:
        return
    if not _is_authorized(callback.from_user and callback.from_user.id, callback.message.chat.id):
        return

    chat_id = callback.message.chat.id
    thread_id = _thread_id(callback.message)
    scope_key = _scope_key(chat_id, thread_id)
    name = callback.data.split(":", 1)[1]
    logger.info("Chat %s: provider selection 'provider:%s'", scope_key, name)

    provider = provider_manager.set_provider(scope_key, name)
    if not provider:
        await callback.answer("Provider not found", show_alert=True)
        return

    # Persist provider to session
    session_manager.set_provider(chat_id, provider.name, thread_id)

    # Update keyboard state
    lines = [f"<b>Current provider:</b> {provider.name}\n<i>{provider.description}</i>\n"]
    lines.append("<b>Select a provider:</b>")

    keyboard = InlineKeyboardBuilder()
    for p in provider_manager.providers:
        button_text = f"{'✓ ' if p.name == provider.name else ''}{p.name}"
        keyboard.button(text=button_text, callback_data=f"provider:{p.name}")
    keyboard.adjust(2)  # 2 buttons per row

    await callback.message.edit_text("\n".join(lines), reply_markup=keyboard.as_markup(), parse_mode="HTML")
    await callback.answer(f"Switched to {provider.name}")


@router.message(Command("status"))
async def cmd_status(message: Message) -> None:
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    chat_id = message.chat.id
    thread_id = _thread_id(message)
    scope_key = _scope_key(chat_id, thread_id)
    session = session_manager.get(chat_id, thread_id)
    provider = provider_manager.get_provider(scope_key)
    if _is_codex_family_cli(provider.cli):
        sid = session.codex_session_id or "none (new conversation)"
    else:
        sid = session.claude_session_id or "none (new conversation)"
    current_model = _current_model_label(session, provider)
    await message.answer(
        f"<b>Version:</b> {config.VERSION}\n"
        f"<b>Thread:</b> <code>{thread_id if thread_id is not None else 'main'}</code>\n"
        f"<b>Session:</b> <code>{sid}</code>\n"
        f"<b>Model:</b> {current_model}\n"
        f"<b>Provider:</b> {provider.name} — {provider.description}",
        parse_mode="HTML",
    )


@router.message(Command("memory"))
async def cmd_memory(message: Message) -> None:
    """Show current memory state."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    content = memory_manager.format_for_display()
    for chunk in split_message(content):
        try:
            await message.answer(chunk, parse_mode="HTML")
        except Exception:
            await message.answer(strip_html(chunk))


@router.message(Command("threads"))
async def cmd_threads(message: Message) -> None:
    """List tracked topic/thread scopes for this chat."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    rows = session_manager.list_tracked_threads(message.chat.id)
    if not rows:
        await message.answer("No tracked threads yet for this chat.")
        return

    lines = ["<b>Tracked threads</b>", ""]
    for row in rows:
        thread = row.get("message_thread_id")
        topic = row.get("topic_label") or "(untitled)"
        last_seen = row.get("last_activity_at") or "n/a"
        lines.append(
            f"• <code>{thread if thread is not None else 'main'}</code> — {html.escape(str(topic))}"
        )
        lines.append(f"  last: {html.escape(str(last_seen))}")

    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(Command("tools"))
async def cmd_tools(message: Message) -> None:
    """List available tools."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    content = tool_registry.format_for_display()
    try:
        await message.answer(content, parse_mode="HTML")
    except Exception:
        await message.answer(strip_html(content))


@router.message(Command("cancel"))
async def cmd_cancel(message: Message) -> None:
    """Cancel the current request if one is running."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return

    chat_id = message.chat.id
    thread_id = _thread_id(message)
    scope_key = _scope_key(chat_id, thread_id)
    state = _get_state(scope_key)

    if not state.lock.locked() or not state.process_handle or not state.process_handle.get("proc"):
        await message.answer("Nothing to cancel.")
        return

    # Kill the process
    proc = state.process_handle["proc"]
    kill_result = proc.kill()
    if inspect.isawaitable(kill_result):
        await kill_result
    state.cancel_requested = True
    session = session_manager.get(chat_id, thread_id)
    provider = _current_provider(scope_key)
    metrics.CLAUDE_REQUESTS_TOTAL.labels(
        model=_current_model_label(session, provider),
        status="cancelled",
    ).inc()


@router.message(Command("rollback"))
async def cmd_rollback(message: Message) -> None:
    if not _is_admin(message.from_user and message.from_user.id):
        await message.answer("This command is admin-only.")
        return
    await _show_rollback_options(message.chat.id, message.bot, _thread_id(message))


@router.callback_query(F.data == "rollback_auto")
async def cb_rollback_auto(callback: CallbackQuery) -> None:
    if not _is_admin(callback.from_user and callback.from_user.id):
        await callback.answer("Admin only", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    await callback.answer()
    await _show_rollback_options(
        callback.message.chat.id,
        callback.bot,
        _thread_id(callback.message),
    )


@router.callback_query(F.data.startswith("rollback:"))
async def cb_rollback(callback: CallbackQuery) -> None:
    if not _is_admin(callback.from_user and callback.from_user.id):
        await callback.answer("Admin only", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    target_hash = callback.data.split(":", 1)[1]
    short_hash = target_hash[:8]

    kb = InlineKeyboardBuilder()
    kb.button(text=f"Yes, rollback to {short_hash}", callback_data=f"rollback_confirm:{target_hash}")
    kb.button(text="No, cancel", callback_data="rollback_cancel")
    kb.adjust(1)

    await callback.message.edit_text(
        f"Rollback to commit <code>{short_hash}</code>?\n\nThis will reset the repo and restart the bot service.",
        parse_mode="HTML",
        reply_markup=kb.as_markup(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("rollback_confirm:"))
async def cb_rollback_confirm(callback: CallbackQuery) -> None:
    if not _is_admin(callback.from_user and callback.from_user.id):
        await callback.answer("Admin only", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    target_hash = callback.data.split(":", 1)[1]
    short_hash = target_hash[:8]
    await callback.answer()
    await callback.message.edit_text(
        f"Rolling back to <code>{short_hash}</code>...",
        parse_mode="HTML",
    )

    ok, details = await asyncio.to_thread(_reset_to_commit, target_hash)
    if not ok:
        await callback.message.answer(f"Rollback failed: {details}")
        return

    _clear_errors(_scope_key_from_message(callback.message))
    await callback.message.answer(
        f"Rollback complete: <code>{short_hash}</code>\nRestarting <code>telegram-bot.service</code>...",
        parse_mode="HTML",
    )
    asyncio.create_task(
        _restart_service(callback.message.chat.id, callback.bot, _thread_id(callback.message))
    )


@router.callback_query(F.data == "rollback_cancel")
async def cb_rollback_cancel(callback: CallbackQuery) -> None:
    await callback.answer("Rollback cancelled")
    if callback.message:
        await callback.message.edit_text("Rollback cancelled.")


@router.message(Command("selfmod_stage"))
async def cmd_selfmod_stage(message: Message, command: CommandObject | None = None) -> None:
    """Admin-only: stage plugin candidate code into sandbox."""
    if not _is_admin(message.from_user and message.from_user.id):
        await message.answer("This command is admin-only.")
        return

    text = message.text or ""
    header, sep, body = text.partition("\n")
    if command is not None:
        relative_path = _command_args(message, command)
    else:
        header_parts = header.split(maxsplit=1)
        relative_path = header_parts[1].strip() if len(header_parts) > 1 else ""
    if not relative_path:
        await message.answer(
            "Usage:\n"
            "/selfmod_stage <relative_plugin_path.py>\n"
            "```python\n# plugin code here\n```",
            parse_mode="Markdown",
        )
        return
    if not sep or not body.strip():
        await message.answer("Provide plugin code on lines after the command.")
        return

    plugin_code = _strip_markdown_code_fence(body)
    if not plugin_code:
        await message.answer("Plugin code is empty after parsing.")
        return

    try:
        staged_path = await asyncio.to_thread(
            self_mod_manager.stage_plugin,
            relative_path,
            plugin_code + ("\n" if not plugin_code.endswith("\n") else ""),
        )
    except Exception as exc:
        await message.answer(f"Staging failed: {exc}")
        return

    await message.answer(
        "✅ Staged plugin candidate\n"
        f"<b>Path:</b> <code>{relative_path}</code>\n"
        f"<b>Sandbox file:</b> <code>{staged_path}</code>\n"
        "Next: run /selfmod_apply with this path.",
        parse_mode="HTML",
    )


@router.message(Command("selfmod_apply"))
async def cmd_selfmod_apply(message: Message, command: CommandObject | None = None) -> None:
    """Admin-only: validate sandbox candidate, promote, and hot-reload."""
    if not _is_admin(message.from_user and message.from_user.id):
        await message.answer("This command is admin-only.")
        return

    args = _command_args(message, command)
    if not args:
        await message.answer(
            "Usage: /selfmod_apply <relative_plugin_path.py> [test_target]\n"
            "Example: /selfmod_apply tools_plugin.py tests/test_context_plugins.py"
        )
        return

    parts = args.split(maxsplit=1)
    relative_path = parts[0].strip()
    test_target = parts[1].strip() if len(parts) > 1 else "tests/test_context_plugins.py"
    f08_advisory.submit_selfmod_apply(
        scope_key=_scope_key_from_message(message),
        relative_path=relative_path,
        test_target=test_target,
    )

    await message.answer(
        f"Applying sandbox candidate <code>{relative_path}</code>\n"
        f"Validation target: <code>{test_target}</code>",
        parse_mode="HTML",
    )

    result = await asyncio.to_thread(
        self_mod_manager.apply_candidate,
        relative_path,
        test_target,
    )

    validation_text = result.validation_output or "(no output)"
    status = "✅ <b>Self-mod apply succeeded</b>" if result.ok else "❌ <b>Self-mod apply failed</b>"
    lines = [
        status,
        f"<b>Result:</b> {result.message}",
        "",
        "<b>Validation output:</b>",
        f"<pre>{html.escape(_truncate_output(validation_text))}</pre>",
    ]
    await message.answer("\n".join(lines), parse_mode="HTML")

    if result.ok:
        global tool_registry, context_plugins
        tool_registry = ToolRegistry(
            config.TOOLS_DIR,
            denylist=config.TOOL_DENYLIST,
            require_approval_for_risky=config.TOOL_REQUIRE_APPROVAL_FOR_RISKY,
        )
        context_plugins = ContextPluginRegistry([tool_registry])


@router.message(Command("bg"))
async def cmd_bg(message: Message, command: CommandObject | None = None) -> None:
    """Run a task in the background."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return

    if not task_manager:
        await message.answer("Background tasks not available.")
        return

    # Extract prompt after /bg
    prompt = _command_args(message, command)
    if not prompt:
        await message.answer("Please provide a task to run in background.\n\nExample: /bg write a python script to backup my database")
        return

    chat_id = message.chat.id
    thread_id = _thread_id(message)
    session = session_manager.get(chat_id, thread_id)

    # Build memory and tool-augmented prompt
    memory_context = memory_manager.build_context(prompt)
    tool_context = context_plugins.build_context(prompt)
    memory_instructions = memory_manager.build_instructions()

    prompt_parts = []
    if memory_context:
        prompt_parts.append(memory_context)
    if tool_context:
        prompt_parts.append(tool_context)
    prompt_parts.append(prompt + memory_instructions)

    full_prompt = "\n\n".join(prompt_parts)

    task_id = await task_manager.submit(
        chat_id=chat_id,
        message_thread_id=thread_id,
        user_id=_actor_id(message),
        prompt=full_prompt,
        model=task_model,
        session_id=session_id,
        provider_cli=provider_cli,
        resume_arg=resume_arg,
    )

    lines = [
        f"✅ <b>Task queued</b>",
        f"",
        f"<b>Task ID:</b> <code>{task_id}</code>",
        f"<b>Model:</b> {task_model}",
        f"",
        f"I'll notify you when it completes. You can continue chatting.",
        f"",
        f"<b>Commands:</b>",
        f"/bg-list — List active tasks",
        f"/bg_cancel {task_id} — Cancel this task",
    ]
    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(F.text.regexp(r"^/bg-list(?:@[A-Za-z0-9_]+)?$"))
async def cmd_bg_list(message: Message) -> None:
    """List active background tasks."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return

    if not task_manager:
        await message.answer("Background tasks not available.")
        return

    tasks = task_manager.list_user_tasks(message.chat.id, _thread_id(message))

    if not tasks:
        await message.answer("No active background tasks.")
        return

    lines = ["<b>Active background tasks:</b>", ""]
    for task in tasks:
        status_emoji = {
            TaskStatus.QUEUED: "⏳",
            TaskStatus.RUNNING: "🔄",
        }.get(task.status, "❓")

        duration = ""
        if task.started_at:
            duration = f" ({(datetime.now() - task.started_at).total_seconds():.0f}s)"

        lines.append(
            f"{status_emoji} <code>{task.id[:8]}</code> — {task.status.value}{duration}"
        )
        lines.append(f"   {task.prompt[:100]}...")
        lines.append("")

    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(Command("bg_cancel"))
async def cmd_bg_cancel(message: Message, command: CommandObject | None = None) -> None:
    """Cancel a background task."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return

    if not task_manager:
        await message.answer("Background tasks not available.")
        return

    task_id = _command_args(message, command)
    if not task_id:
        await message.answer("Please provide a task ID.\n\nExample: /bg_cancel abc123")
        return

    # Find full task ID from partial match
    full_task_id = None
    for tid in task_manager.tasks:
        if tid.startswith(task_id):
            full_task_id = tid
            break

    if not full_task_id:
        await message.answer("Task not found.")
        return

    task = await task_manager.get_status(full_task_id)
    if not task or task.chat_id != message.chat.id or task.message_thread_id != _thread_id(message):
        await message.answer("Task not found.")
        return

    if task.status not in (TaskStatus.QUEUED, TaskStatus.RUNNING):
        await message.answer(f"Task is already {task.status.value}.")
        return

    cancelled = await task_manager.cancel(full_task_id)
    if cancelled:
        await message.answer(f"✅ Cancelled task <code>{full_task_id[:8]}</code>", parse_mode="HTML")
    else:
        await message.answer("Could not cancel task.")


@router.message(Command("schedule_every"))
async def cmd_schedule_every(message: Message, command: CommandObject | None = None) -> None:
    """Create recurring background task schedule."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    if not schedule_manager:
        await message.answer("Scheduler not available.")
        return

    args = _command_args(message, command)
    parts = args.split(maxsplit=1) if args else []
    if len(parts) < 2:
        await message.answer(
            "Usage: /schedule_every <minutes> <task>\n"
            "Example: /schedule_every 60 summarize open PRs"
        )
        return

    try:
        interval_minutes = int(parts[0])
    except ValueError:
        await message.answer("Minutes must be an integer.")
        return

    if interval_minutes < 1 or interval_minutes > 10080:
        await message.answer("Minutes must be between 1 and 10080.")
        return

    task_text = parts[1].strip()
    if not task_text:
        await message.answer("Task text cannot be empty.")
        return

    chat_id = message.chat.id
    thread_id = _thread_id(message)
    session = session_manager.get(chat_id, thread_id)
    provider = _current_provider(_scope_key(chat_id, thread_id))
    task_model, session_id, provider_cli, resume_arg = _scheduled_task_backend(session, provider)
    full_prompt = _build_augmented_prompt(task_text)

    schedule_id = await schedule_manager.create_every(
        chat_id=chat_id,
        message_thread_id=thread_id,
        user_id=_actor_id(message),
        prompt=full_prompt,
        interval_minutes=interval_minutes,
        model=task_model,
        session_id=session_id,
        provider_cli=provider_cli,
        resume_arg=resume_arg,
    )
    await message.answer(
        "✅ Recurring schedule created\n"
        f"<b>ID:</b> <code>{schedule_id[:8]}</code>\n"
        f"<b>Interval:</b> every {interval_minutes} min\n"
        f"Use /schedule_list to view schedules.",
        parse_mode="HTML",
    )


@router.message(Command("schedule_list"))
async def cmd_schedule_list(message: Message) -> None:
    """List recurring schedules for this chat."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    if not schedule_manager:
        await message.answer("Scheduler not available.")
        return

    schedules = await schedule_manager.list_for_chat(message.chat.id, _thread_id(message))
    if not schedules:
        await message.answer("No recurring schedules.")
        return

    latest_runs = await schedule_manager.latest_runs_by_schedule([item.id for item in schedules])
    lines = ["<b>Recurring schedules:</b>", ""]
    for item in schedules:
        next_run_local = item.next_run_at.astimezone().strftime("%Y-%m-%d %H:%M")
        schedule_label = _format_schedule_label(item)
        lines.append(f"⏱ <code>{item.id[:8]}</code> — {schedule_label}")
        lines.append(f"   next: {next_run_local}")
        if item.current_status:
            lines.append(f"   active: {_format_active_schedule_summary(item)}")
        latest_run = latest_runs.get(item.id)
        if latest_run:
            lines.append(f"   last: {_format_schedule_run_summary(latest_run)}")
        else:
            lines.append("   last: no executions yet")
        lines.append(f"   {item.prompt[:80]}...")
        lines.append("")
    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(Command("schedule_history"))
async def cmd_schedule_history(message: Message, command: CommandObject | None = None) -> None:
    """Show recent recurring schedule executions for this chat."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    if not schedule_manager:
        await message.answer("Scheduler not available.")
        return

    short_id = _command_args(message, command).strip()
    schedule_id: str | None = None
    if short_id:
        schedule_id = await schedule_manager.find_schedule_id_for_chat(
            message.chat.id,
            short_id,
            _thread_id(message),
        )
        if not schedule_id:
            await message.answer("Schedule not found.")
            return

    runs = await schedule_manager.list_runs_for_chat(
        message.chat.id,
        _thread_id(message),
        schedule_id=schedule_id,
        limit=10,
    )
    if not runs:
        await message.answer("No scheduled job history yet.")
        return

    lines = ["<b>Scheduled job history:</b>", ""]
    for run in runs:
        lines.append(
            f"🕓 <code>{run.schedule_id[:8]}</code> — {_format_schedule_run_status(run)}"
        )
        lines.append(f"   planned: {run.planned_for.astimezone().strftime('%Y-%m-%d %H:%M')}")
        if run.started_at:
            lines.append(f"   started: {run.started_at.astimezone().strftime('%Y-%m-%d %H:%M:%S')}")
        if run.completed_at:
            lines.append(f"   finished: {run.completed_at.astimezone().strftime('%Y-%m-%d %H:%M:%S')}")
        if run.background_task_id:
            lines.append(f"   task: <code>{run.background_task_id[:8]}</code>")
        detail = run.error_text or run.response_preview
        if detail:
            lines.append(f"   result: {html.escape(detail[:160])}")
        lines.append("")
    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(Command("schedule_weekly"))
async def cmd_schedule_weekly(message: Message, command: CommandObject | None = None) -> None:
    """Create weekly recurring background task schedule."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    if not schedule_manager:
        await message.answer("Scheduler not available.")
        return

    args = _command_args(message, command)
    parts = args.split(maxsplit=2) if args else []
    if len(parts) < 3:
        await message.answer(
            "Usage: /schedule_weekly <day> <HH:MM> <task>\n"
            "Example: /schedule_weekly mon 09:00 check sprint board"
        )
        return

    weekday = _weekday_to_int(parts[0])
    if weekday is None:
        await message.answer("Day must be one of: mon,tue,wed,thu,fri,sat,sun.")
        return

    daily_time = parts[1].strip()
    if not re.fullmatch(r"([01]\d|2[0-3]):([0-5]\d)", daily_time):
        await message.answer("Time must be in HH:MM 24-hour format.")
        return

    task_text = parts[2].strip()
    if not task_text:
        await message.answer("Task text cannot be empty.")
        return

    timezone_name = _default_timezone_name()
    chat_id = message.chat.id
    thread_id = _thread_id(message)
    session = session_manager.get(chat_id, thread_id)
    provider = _current_provider(_scope_key(chat_id, thread_id))
    task_model, session_id, provider_cli, resume_arg = _scheduled_task_backend(session, provider)
    full_prompt = _build_augmented_prompt(task_text)

    try:
        schedule_id = await schedule_manager.create_weekly(
            chat_id=chat_id,
            message_thread_id=thread_id,
            user_id=_actor_id(message),
            prompt=full_prompt,
            weekly_day=weekday,
            daily_time=daily_time,
            timezone_name=timezone_name,
            model=task_model,
            session_id=session_id,
            provider_cli=provider_cli,
            resume_arg=resume_arg,
        )
    except Exception as exc:
        await message.answer(f"Could not create weekly schedule: {exc}")
        return

    day_label = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][weekday]
    await message.answer(
        "✅ Weekly schedule created\n"
        f"<b>ID:</b> <code>{schedule_id[:8]}</code>\n"
        f"<b>Time:</b> {day_label} {daily_time} ({timezone_name})\n"
        f"Use /schedule_list to view schedules.",
        parse_mode="HTML",
    )


@router.message(Command("schedule_daily"))
async def cmd_schedule_daily(message: Message, command: CommandObject | None = None) -> None:
    """Create daily recurring background task schedule."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    if not schedule_manager:
        await message.answer("Scheduler not available.")
        return

    args = _command_args(message, command)
    parts = args.split(maxsplit=1) if args else []
    if len(parts) < 2:
        await message.answer(
            "Usage: /schedule_daily <HH:MM> <task>\n"
            "Example: /schedule_daily 09:00 check PR reviews"
        )
        return

    daily_time = parts[0].strip()
    if not re.fullmatch(r"([01]\d|2[0-3]):([0-5]\d)", daily_time):
        await message.answer("Time must be in HH:MM 24-hour format.")
        return

    task_text = parts[1].strip()
    if not task_text:
        await message.answer("Task text cannot be empty.")
        return

    timezone_name = _default_timezone_name()

    chat_id = message.chat.id
    thread_id = _thread_id(message)
    session = session_manager.get(chat_id, thread_id)
    provider = _current_provider(_scope_key(chat_id, thread_id))
    task_model, session_id, provider_cli, resume_arg = _scheduled_task_backend(session, provider)
    full_prompt = _build_augmented_prompt(task_text)

    try:
        schedule_id = await schedule_manager.create_daily(
            chat_id=chat_id,
            message_thread_id=thread_id,
            user_id=_actor_id(message),
            prompt=full_prompt,
            daily_time=daily_time,
            timezone_name=timezone_name,
            model=task_model,
            session_id=session_id,
            provider_cli=provider_cli,
            resume_arg=resume_arg,
        )
    except Exception as exc:
        await message.answer(f"Could not create daily schedule: {exc}")
        return

    await message.answer(
        "✅ Daily schedule created\n"
        f"<b>ID:</b> <code>{schedule_id[:8]}</code>\n"
        f"<b>Time:</b> {daily_time} ({timezone_name})\n"
        f"Use /schedule_list to view schedules.",
        parse_mode="HTML",
    )


@router.message(Command("schedule_cancel"))
async def cmd_schedule_cancel(message: Message, command: CommandObject | None = None) -> None:
    """Cancel recurring schedule by full or short ID."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    if not schedule_manager:
        await message.answer("Scheduler not available.")
        return

    short_id = _command_args(message, command)
    if not short_id:
        await message.answer("Usage: /schedule_cancel <schedule_id>")
        return

    schedules = await schedule_manager.list_for_chat(message.chat.id, _thread_id(message))
    target = next((s for s in schedules if s.id.startswith(short_id)), None)
    if not target:
        await message.answer("Schedule not found.")
        return

    cancelled = await schedule_manager.cancel(target.id)
    if cancelled:
        await message.answer(f"✅ Cancelled schedule <code>{target.id[:8]}</code>", parse_mode="HTML")
    else:
        await message.answer("Could not cancel schedule.")


async def _run_claude(
    message: Message,
    state: _ChatState,
    session: object,
    progress: ProgressReporter,
    subprocess_env: dict[str, str] | None = None,
    override_text: str | None = None,
    observed_tools: list[str] | None = None,
) -> bridge.ClaudeResponse | None:
    """Run a single Claude subprocess attempt. Returns the response or None."""
    state.process_handle = {}

    # Build memory and tool-augmented prompt
    raw_prompt = _as_text(override_text) or _as_text(getattr(message, "text", None))
    memory_context = memory_manager.build_context(raw_prompt)
    tool_context = context_plugins.build_context(raw_prompt)
    memory_instructions = memory_manager.build_instructions()

    # Assemble prompt with all context layers
    prompt_parts = []
    if isinstance(memory_context, str) and memory_context:
        prompt_parts.append(memory_context)
    if isinstance(tool_context, str) and tool_context:
        prompt_parts.append(tool_context)
    prompt_parts.append(raw_prompt + memory_instructions)

    prompt = "\n\n".join(prompt_parts)

    stream = bridge.stream_message(
        prompt=prompt,
        session_id=session.claude_session_id,
        model=session.model,
        working_dir=config.CLAUDE_WORKING_DIR,
        process_handle=state.process_handle,
        subprocess_env=subprocess_env,
    )
    if hasattr(stream, "__aiter__"):
        iterator = stream
    else:
        async def _iter_sync():
            for item in stream:
                yield item
        iterator = _iter_sync()

    async for event in iterator:
        if state.cancel_requested:
            await progress.show_cancelled()
            return bridge.ClaudeResponse(
                text="Request cancelled.",
                session_id=session.claude_session_id,
                is_error=True,
                cost_usd=0,
                duration_ms=0,
                num_turns=0,
            )

        match event.event_type:
            case bridge.StreamEventType.TOOL_USE:
                if event.tool_name:
                    if observed_tools is not None:
                        observed_tools.append(event.tool_name)
                    await progress.report_tool(event.tool_name, event.tool_input)
            case bridge.StreamEventType.RESULT:
                return event.response
            case "TOOL_USE":
                if getattr(event, "tool_name", None):
                    if observed_tools is not None:
                        observed_tools.append(event.tool_name)
                    await progress.report_tool(event.tool_name, getattr(event, "tool_input", None))
            case "RESULT":
                return event.response

    return None


async def _run_codex(
    message: Message,
    state: _ChatState,
    session: object,
    progress: ProgressReporter,
    model: str | None = None,
    session_id: str | None = None,
    resume_arg: str | None = None,
    subprocess_env: dict[str, str] | None = None,
    cli_name: str = "codex",
    override_text: str | None = None,
    observed_tools: list[str] | None = None,
) -> bridge.ClaudeResponse | None:
    """Run a single Codex CLI subprocess attempt. Returns the response or None."""
    state.process_handle = {}

    # Build memory and tool-augmented prompt
    raw_prompt = _as_text(override_text) or _as_text(getattr(message, "text", None))
    memory_context = memory_manager.build_context(raw_prompt)
    tool_context = context_plugins.build_context(raw_prompt)
    memory_instructions = memory_manager.build_instructions()

    prompt_parts = []
    if isinstance(memory_context, str) and memory_context:
        prompt_parts.append(memory_context)
    if isinstance(tool_context, str) and tool_context:
        prompt_parts.append(tool_context)
    prompt_parts.append(raw_prompt + memory_instructions)

    prompt = "\n\n".join(prompt_parts)

    stream = bridge.stream_codex_message(
        prompt=prompt,
        session_id=session_id,
        model=model,
        resume_arg=resume_arg,
        cli_name=cli_name,
        working_dir=_codex_working_dir(),
        process_handle=state.process_handle,
        subprocess_env=subprocess_env,
    )
    if hasattr(stream, "__aiter__"):
        iterator = stream
    else:
        async def _iter_sync():
            for item in stream:
                yield item
        iterator = _iter_sync()

    async for event in iterator:
        if state.cancel_requested:
            await progress.show_cancelled()
            return bridge.ClaudeResponse(
                text="Request cancelled.",
                session_id=session_id,
                is_error=True,
                cost_usd=0,
                duration_ms=0,
                num_turns=0,
            )

        match event.event_type:
            case bridge.StreamEventType.TOOL_USE:
                if event.tool_name:
                    if observed_tools is not None:
                        observed_tools.append(event.tool_name)
                    await progress.report_tool(event.tool_name, event.tool_input)
            case bridge.StreamEventType.RESULT:
                return event.response
            case "TOOL_USE":
                if getattr(event, "tool_name", None):
                    if observed_tools is not None:
                        observed_tools.append(event.tool_name)
                    await progress.report_tool(event.tool_name, getattr(event, "tool_input", None))
            case "RESULT":
                return event.response

    return None


async def _run_codex_with_retries(
    message: Message,
    state: _ChatState,
    session: object,
    progress: ProgressReporter,
    model: str | None = None,
    session_id: str | None = None,
    resume_arg: str | None = None,
    subprocess_env: dict[str, str] | None = None,
    cli_name: str = "codex",
    override_text: str | None = None,
    observed_tools: list[str] | None = None,
) -> bridge.ClaudeResponse | None:
    retries_left = max(0, config.CODEX_TRANSIENT_MAX_RETRIES)
    attempt = 0
    next_session_id = session_id

    while True:
        attempt += 1
        response = await _run_codex(
            message,
            state,
            session,
            progress,
            model,
            next_session_id,
            resume_arg,
            subprocess_env,
            cli_name,
            override_text=override_text,
            observed_tools=observed_tools,
        )
        if not response:
            return None
        if state.cancel_requested or not response.is_error or not _is_transient_codex_error(response.text):
            return response
        if retries_left <= 0:
            return _sanitize_transient_codex_error_response(response, attempts=attempt)

        retries_left -= 1
        logger.warning(
            "Chat %d: transient Codex error on attempt %d, retrying (%d retries left): %s",
            message.chat.id,
            attempt,
            retries_left,
            response.text[:200],
        )
        if next_session_id:
            # First retry starts a fresh Codex conversation to bypass stale stream state.
            next_session_id = None
        await asyncio.sleep(max(0.0, config.CODEX_TRANSIENT_RETRY_BACKOFF_SECONDS))


@router.message(F.voice)
async def handle_voice(message: Message) -> None:
    """Transcribe voice message via whisper.cpp and process as text."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return

    _log_incoming_message(message, "voice")
    logger.info(
        "Entering handle_voice: chat=%s thread=%s message=%s",
        message.chat.id,
        _thread_id(message),
        message.message_id,
    )

    if not transcribe.is_available():
        await message.answer(
            "Voice messages are not supported — whisper.cpp is not installed.\n"
            "Run <code>bash setup_whisper.sh</code> on the server to enable.",
            parse_mode="HTML",
        )
        return

    import tempfile

    file_lookup_started_at = monotonic()
    file = await message.bot.get_file(message.voice.file_id)
    file_lookup_elapsed_ms = (monotonic() - file_lookup_started_at) * 1000
    tmp = tempfile.NamedTemporaryFile(suffix=".oga", delete=False)
    transcription_started_at = monotonic()
    transcription_status_ref: dict[str, int | None] = {"message_id": None}
    transcription_status_task: asyncio.Task | None = None
    transcription_status_retry_task: asyncio.Task | None = None
    transcription_completed = False
    transcription_failed_notified = False
    await _send_chat_action_once(message, ChatAction.TYPING)
    transcription_typing_task = asyncio.create_task(_keep_chat_action(message, ChatAction.TYPING))
    try:
        await asyncio.sleep(0)
        (
            transcription_status_message_id,
            transcription_retry_after,
        ) = await _send_voice_transcription_progress_message(
            message,
            monotonic() - transcription_started_at,
        )
        transcription_status_ref["message_id"] = transcription_status_message_id
        if transcription_status_message_id is not None:
            transcription_status_task = asyncio.create_task(
                _update_voice_transcription_progress(
                    message,
                    transcription_status_message_id,
                    transcription_started_at,
                )
            )
        elif transcription_retry_after is not None:
            transcription_status_retry_task = asyncio.create_task(
                _retry_voice_transcription_progress_message(
                    message,
                    transcription_status_ref,
                    transcription_started_at,
                    transcription_retry_after,
                )
            )
        download_started_at = monotonic()
        await message.bot.download_file(file.file_path, tmp.name)
        download_elapsed_ms = (monotonic() - download_started_at) * 1000
        transcribe_started_at = monotonic()
        text = await transcribe.transcribe(tmp.name)
        transcribe_elapsed_ms = (monotonic() - transcribe_started_at) * 1000
        transcription_completed = True
        total_pre_llm_elapsed_ms = (monotonic() - transcription_started_at) * 1000
        logger.info("Chat %d: transcribed voice (%ds) → %d chars",
                     message.chat.id, message.voice.duration, len(text))
        logger.info(
            "Voice pipeline timings: chat=%s thread=%s message=%s voice_duration_s=%s "
            "file_lookup_ms=%.1f download_ms=%.1f transcribe_call_ms=%.1f total_pre_llm_ms=%.1f "
            "temp_audio=%s",
            message.chat.id,
            _thread_id(message),
            message.message_id,
            message.voice.duration,
            file_lookup_elapsed_ms,
            download_elapsed_ms,
            transcribe_elapsed_ms,
            total_pre_llm_elapsed_ms,
            os.path.basename(tmp.name),
        )
    except Exception:
        logger.exception("Voice transcription failed")
        transcription_failed_notified = True
        await message.answer("Failed to transcribe voice message.")
        return
    finally:
        transcription_typing_task.cancel()
        try:
            await transcription_typing_task
        except asyncio.CancelledError:
            pass
        if transcription_status_task is not None:
            transcription_status_task.cancel()
            try:
                await transcription_status_task
            except asyncio.CancelledError:
                pass
        if transcription_status_retry_task is not None:
            transcription_status_retry_task.cancel()
            try:
                await transcription_status_retry_task
            except asyncio.CancelledError:
                pass
        transcription_elapsed_seconds = monotonic() - transcription_started_at
        transcription_status_message_id = transcription_status_ref["message_id"]
        transcription_final_text = (
            _format_voice_transcription_complete(transcription_elapsed_seconds)
            if transcription_completed
            else _format_voice_transcription_failed(transcription_elapsed_seconds)
        )
        await _publish_voice_transcription_result(
            message,
            progress_message_id=transcription_status_message_id,
            text=transcription_final_text,
            send_summary=transcription_completed or not transcription_failed_notified,
        )
        os.unlink(tmp.name)

    override = f"[Voice message] {text}"
    try:
        await _handle_message_inner(message, override_text=override)
    except TelegramAPIError:
        logger.exception("Voice response delivery failed after transcription")
        metrics.MESSAGES_TOTAL.labels(status="error").inc()
        _record_error(_scope_key_from_message(message))
    except Exception:
        logger.exception("Unhandled exception in handle_voice")
        metrics.MESSAGES_TOTAL.labels(status="error").inc()
        _record_error(_scope_key_from_message(message))
        try:
            await message.answer("An internal error occurred while processing your voice message.")
        except TelegramAPIError:
            logger.exception("Voice fallback error delivery failed")


@router.message(F.text)
async def handle_message(message: Message) -> None:
    _log_incoming_message(message, "text")
    logger.info(
        "Entering handle_message: chat=%s thread=%s message=%s",
        message.chat.id,
        _thread_id(message),
        message.message_id,
    )
    try:
        await _handle_message_inner(message)
    except Exception:
        logger.exception("Unhandled exception in handle_message")
        metrics.MESSAGES_TOTAL.labels(status="error").inc()
        scope_key = _scope_key_from_message(message)
        _record_error(scope_key)
        reply_markup = _build_rollback_suggestion_markup(
            scope_key,
            message.from_user and message.from_user.id,
        )
        await message.answer(
            "An internal error occurred while processing your request.",
            reply_markup=reply_markup,
        )


@router.message(F.photo)
async def handle_photo_message(message: Message) -> None:
    _log_incoming_message(message, "photo")
    logger.info(
        "Entering handle_photo_message: chat=%s thread=%s message=%s",
        message.chat.id,
        _thread_id(message),
        message.message_id,
    )
    try:
        await _handle_message_inner(message)
    except Exception:
        logger.exception("Unhandled exception in handle_photo_message")
        metrics.MESSAGES_TOTAL.labels(status="error").inc()
        scope_key = _scope_key_from_message(message)
        _record_error(scope_key)
        reply_markup = _build_rollback_suggestion_markup(
            scope_key,
            message.from_user and message.from_user.id,
        )
        await message.answer(
            "An internal error occurred while processing your request.",
            reply_markup=reply_markup,
        )


@router.channel_post(F.text)
async def handle_channel_post(message: Message) -> None:
    await handle_message(message)


@router.channel_post(F.photo)
async def handle_channel_photo(message: Message) -> None:
    await handle_photo_message(message)


@router.message(F.forum_topic_created)
async def handle_forum_topic_created(message: Message) -> None:
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    _touch_thread_context(message)


@router.message(F.forum_topic_edited)
async def handle_forum_topic_edited(message: Message) -> None:
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    _touch_thread_context(message)


async def _handle_message_inner(message: Message, override_text: str | None = None) -> None:
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        metrics.MESSAGES_TOTAL.labels(status="unauthorized").inc()
        return

    _touch_thread_context(message, override_text=override_text)
    chat_id = message.chat.id
    thread_id = _thread_id(message)
    scope_key = _scope_key(chat_id, thread_id)
    raw_prompt = await _compose_incoming_prompt(message, override_text)
    f08_advisory.submit_chat_turn(scope_key=scope_key, prompt=raw_prompt)
    state = _get_state(scope_key)
    logger.info(
        "Starting message processing: scope=%s override=%s prompt_len=%d",
        scope_key,
        bool(override_text),
        len(raw_prompt),
    )

    if state.lock.locked():
        metrics.MESSAGES_TOTAL.labels(status="busy").inc()
        if state.reset_requested:
            await message.answer(
                "The previous request is still stopping after /new. "
                "Please resend your fresh request in a moment."
            )
            return
        ok_fast_resume, _ = resume_state_store.can_fast_resume(
            scope_key=scope_key,
            input_text=raw_prompt,
        )
        if ok_fast_resume:
            await message.answer("I am already processing this same request and will send the result shortly.")
        else:
            steering_event = _create_steering_event(message, raw_prompt)
            steering_ledger_store.append(scope_key=scope_key, event=steering_event)
            await message.answer(
                "Applied your follow-up to the active run. I will continue from current progress."
            )
        return

    async with state.lock:
        # A newly-started foreground run supersedes any prior reset request.
        state.cancel_requested = False
        state.reset_requested = False

        session = session_manager.get(chat_id, thread_id)
        progress = ProgressReporter(message)
        typing_task = asyncio.create_task(_keep_typing(message))
        await progress.show_working()

        final_response: bridge.ClaudeResponse | None = None
        provider = provider_manager.get_provider(scope_key)
        observed_tools: list[str] = []
        provider_attempts = 0
        steering_events_applied = 0
        final_provider_name = provider.name
        final_model_name = _current_model_label(session, provider)
        step_plan_active = _step_plan_active_flag() or bool(_STEP_PLAN_AUTO_TRIGGER_RE.search(raw_prompt))
        response_has_user_content = False
        output_size_out = 0

        try:
            if provider.cli != "claude" and _find_provider_cli(provider.cli) is None:
                fallback = provider_manager.reset(scope_key)
                session_manager.set_provider(chat_id, fallback.name, thread_id)
                await message.answer(
                    f"Provider <b>{provider.name}</b> requires missing CLI "
                    f"<code>{provider.cli}</code>. Switched to <b>{fallback.name}</b>.",
                    parse_mode="HTML",
                )
                provider = fallback
            turn_prompt = override_text
            pending_apply_ids: list[str] = []
            while True:
                effective_prompt = _as_text(turn_prompt) or raw_prompt
                env = _worklog_subprocess_env(
                    provider_manager.subprocess_env(provider),
                    chat_id=chat_id,
                    message_thread_id=thread_id,
                    provider=provider,
                    session=session,
                )
                logger.info(
                    "Chat %s: using provider '%s' (cli=%s) with env=%s",
                    scope_key,
                    provider.name,
                    provider.cli,
                    {k: v for k, v in env.items() if k.startswith("ANTHROPIC_")},
                )
                resume_state_store.record_start(
                    scope_key=scope_key,
                    task_id=f"msg:{message.message_id}",
                    step_id="interactive_turn",
                    provider_cli=provider.cli,
                    model=_current_model_label(session, provider),
                    session_id=session.codex_session_id if _is_codex_family_cli(provider.cli) else session.claude_session_id,
                    input_text=effective_prompt,
                    resume_reason="manual_continue" if turn_prompt else "restart",
                )

                if _is_codex_family_cli(provider.cli):
                    provider_attempts += 1
                    codex_model = _codex_model_arg(session, provider)
                    final_response = await _run_codex_with_retries(
                        message,
                        state,
                        session,
                        progress,
                        codex_model,
                        session.codex_session_id,
                        provider.resume_arg,
                        env,
                        provider.cli,
                        override_text=effective_prompt,
                        observed_tools=observed_tools,
                    )
                else:
                    provider_attempts += 1
                    final_response = await _run_claude(
                        message, state, session, progress, env,
                        override_text=effective_prompt,
                        observed_tools=observed_tools,
                    )
                final_provider_name = provider.name
                final_model_name = _current_model_label(session, provider)

                # ── Fallback on provider errors ───────────────────────
                error_text_l = (final_response.text or "").strip().lower() if final_response else ""
                should_fallback = bool(
                    final_response
                    and final_response.is_error
                    and not state.cancel_requested
                    and (
                        provider_manager.is_rate_limit_error(final_response.text)
                        # Claude CLI sometimes returns a generic empty-body failure.
                        or (provider.cli == "claude" and error_text_l == "claude returned an error.")
                    )
                )
                if should_fallback:
                    next_provider = provider_manager.advance(scope_key)
                    if next_provider:
                        reason = (
                            "Rate limited"
                            if provider_manager.is_rate_limit_error(final_response.text)
                            else "Provider error"
                        )
                        await message.answer(
                            f"{reason} on <b>{provider.name}</b>. "
                            f"Switching to <b>{next_provider.name}</b>...",
                            parse_mode="HTML",
                        )
                        logger.info(
                            "Chat %s: fallback from '%s' to '%s' (error=%r)",
                            scope_key, provider.name, next_provider.name, final_response.text,
                        )
                        provider = next_provider
                        session_manager.set_provider(chat_id, next_provider.name, thread_id)
                        env = _worklog_subprocess_env(
                            provider_manager.subprocess_env(next_provider),
                            chat_id=chat_id,
                            message_thread_id=thread_id,
                            provider=next_provider,
                            session=session,
                        )
                        if _is_codex_family_cli(next_provider.cli):
                            provider_attempts += 1
                            codex_model = _codex_model_arg(session, next_provider)
                            final_response = await _run_codex_with_retries(
                                message,
                                state,
                                session,
                                progress,
                                codex_model,
                                session.codex_session_id,
                                next_provider.resume_arg,
                                env,
                                next_provider.cli,
                                override_text=effective_prompt,
                                observed_tools=observed_tools,
                            )
                        else:
                            provider_attempts += 1
                            final_response = await _run_claude(
                                message, state, session, progress, env,
                                override_text=effective_prompt,
                                observed_tools=observed_tools,
                            )
                        final_provider_name = next_provider.name
                        final_model_name = _current_model_label(session, next_provider)

                requested_tools = ToolRegistry.extract_requested_tools(
                    final_response.text if final_response else ""
                )
                if (
                    requested_tools
                    and final_response
                    and not final_response.is_error
                    and not state.cancel_requested
                ):
                    selected_tool = requested_tools[0]
                    logger.info(
                        "Chat %d: second-pass tool activation requested: %s",
                        message.chat.id,
                        selected_tool,
                    )
                    await progress.report_tool("tool_selector", selected_tool)
                    forced_prompt = _inject_tool_request(effective_prompt, selected_tool)
                    if _is_codex_family_cli(provider.cli):
                        provider_attempts += 1
                        codex_model = _codex_model_arg(session, provider)
                        retry_response = await _run_codex_with_retries(
                            message,
                            state,
                            session,
                            progress,
                            codex_model,
                            session.codex_session_id,
                            provider.resume_arg,
                            env,
                            provider.cli,
                            override_text=forced_prompt,
                            observed_tools=observed_tools,
                        )
                    else:
                        provider_attempts += 1
                        retry_response = await _run_claude(
                            message,
                            state,
                            session,
                            progress,
                            env,
                            override_text=forced_prompt,
                            observed_tools=observed_tools,
                        )
                    if retry_response:
                        final_response = retry_response

                if (
                    final_response
                    and not final_response.is_error
                    and not state.cancel_requested
                    and final_response.session_id
                ):
                    if _is_codex_family_cli(provider.cli):
                        session_manager.update_codex_session_id(chat_id, final_response.session_id, thread_id)
                    else:
                        session_manager.update_session_id(chat_id, final_response.session_id, thread_id)

                if (
                    pending_apply_ids
                    and final_response
                    and not final_response.is_error
                    and not state.cancel_requested
                ):
                    steering_ledger_store.mark_applied(scope_key=scope_key, event_ids=pending_apply_ids)
                    pending_apply_ids = []

                if not final_response or final_response.is_error or state.cancel_requested:
                    break

                unapplied = steering_ledger_store.get_unapplied(scope_key=scope_key)
                if not unapplied:
                    break
                if _has_high_risk_conflict(unapplied):
                    await message.answer(
                        "I received a high-risk follow-up while work is in progress. "
                        "Please clarify the exact intended change in one message."
                    )
                    break

                pending_apply_ids = [event.event_id for event in unapplied]
                steering_events_applied += len(unapplied)
                await progress.report_tool("steering", f"{len(unapplied)} pending update(s)")
                turn_prompt = _build_steering_patch(raw_prompt, unapplied)
                logger.info(
                    "Chat %s: applying %d cumulative steering event(s) in continuation",
                    scope_key,
                    len(unapplied),
                )
        finally:
            typing_task.cancel()
            try:
                await typing_task
            except asyncio.CancelledError:
                pass
            state.process_handle = None
            state.reset_requested = False

        # ── Send response ─────────────────────────────────────
        if state.cancel_requested:
            await progress.finish()
            _clear_errors(scope_key)
        elif final_response:
            if final_response.is_error:
                resume_state_store.record_failure(scope_key=scope_key)
                error_text = final_response.text or "(No response)"
                logger.warning(
                    "Chat %d: provider '%s' returned error response: %r",
                    message.chat.id,
                    provider.name,
                    error_text[:500],
                )
                _record_error(scope_key)
                reply_markup = _build_rollback_suggestion_markup(
                    scope_key,
                    message.from_user and message.from_user.id,
                )
                await _answer_text_with_retry(
                    message,
                    error_text,
                    reply_markup=reply_markup,
                )
                await progress.finish()
            else:
                resume_state_store.record_success(
                    scope_key=scope_key,
                    output_text=final_response.text or "",
                )
                raw_response_text = final_response.text or ""
                clean_text, media_refs, audio_as_voice = _extract_media_directives(raw_response_text)
                clean_text = _strip_tool_directive_lines(clean_text)
                response_has_user_content = bool(clean_text.strip() or media_refs)
                output_size_out = len(clean_text)
                for media_ref in media_refs:
                    try:
                        await _send_media_reply(
                            message,
                            media_ref,
                            audio_as_voice=audio_as_voice,
                        )
                    except Exception:
                        logger.exception(
                            "Chat %d: failed to send media '%s'",
                            message.chat.id,
                            media_ref,
                        )

                chunks: list[str] = []
                if clean_text.strip():
                    html = markdown_to_html(clean_text)
                    chunks = split_message(html)

                if not chunks:
                    if not media_refs:
                        logger.warning(
                            "Chat %d: Got empty response object - text='%s', is_error=%s, session_id=%s, cost=%.6f",
                            message.chat.id,
                            repr(final_response.text[:200]) if final_response.text else "None",
                            final_response.is_error,
                            final_response.session_id,
                            final_response.cost_usd,
                        )
                        chunks = [_EMPTY_RESPONSE_FALLBACK_TEXT]

                for chunk in chunks:
                    if not chunk.strip():
                        continue
                    plain_preview = strip_html(chunk)
                    if _has_recent_outbound(scope_key, plain_preview):
                        logger.info("Chat %s: suppressed duplicate outgoing chunk", scope_key)
                        continue
                    try:
                        await _answer_text_with_retry(
                            message,
                            chunk,
                            parse_mode="HTML",
                        )
                        _remember_outbound(scope_key, plain_preview)
                    except Exception:
                        plain = strip_html(chunk)
                        for plain_chunk in split_message(plain):
                            if not plain_chunk.strip():
                                continue
                            if _has_recent_outbound(scope_key, plain_chunk):
                                logger.info("Chat %s: suppressed duplicate plain outgoing chunk", scope_key)
                                continue
                            await _answer_text_with_retry(message, plain_chunk)
                            _remember_outbound(scope_key, plain_chunk)

                await progress.finish()
                _clear_errors(scope_key)
        else:
            _record_error(scope_key)
            reply_markup = _build_rollback_suggestion_markup(
                scope_key,
                message.from_user and message.from_user.id,
            )
            await _answer_text_with_retry(
                message,
                "An internal error occurred while processing your request.",
                reply_markup=reply_markup,
            )
            await progress.finish()

        # Update session ID if we got one back
        if (
            final_response
            and not _is_codex_family_cli(provider.cli)
            and final_response.session_id
            and final_response.session_id != session.claude_session_id
        ):
            session_manager.update_session_id(chat_id, final_response.session_id, thread_id)
        if (
            final_response
            and _is_codex_family_cli(provider.cli)
            and final_response.session_id
            and final_response.session_id != session.codex_session_id
        ):
            session_manager.update_codex_session_id(chat_id, final_response.session_id, thread_id)

        # Track metrics
        if final_response:
            status = "error" if final_response.is_error else "success"
            if state.cancel_requested:
                status = "cancelled"
            metrics.MESSAGES_TOTAL.labels(status=status).inc()
        metrics.observe_cost_intelligence_turn(
            scope_key=scope_key,
            provider=final_provider_name,
            model=final_model_name,
            mode="foreground",
            cost_usd=float(final_response.cost_usd) if final_response else 0.0,
            num_turns=int(final_response.num_turns) if final_response else 0,
            duration_ms=float(final_response.duration_ms) if final_response else 0.0,
            is_error=bool(final_response.is_error) if final_response else True,
            is_cancelled=state.cancel_requested,
            is_empty_response=(
                not response_has_user_content
                if (final_response and not final_response.is_error and not state.cancel_requested)
                else not bool((final_response.text or "").strip()) if final_response else True
            ),
            tool_timeout=bool(final_response.idle_timeout) if final_response else False,
            tool_names=observed_tools,
            message_size_in=len(raw_prompt),
            message_size_out=output_size_out,
            step_plan_active=step_plan_active,
            steering_event_count=steering_events_applied,
            attempts=max(1, provider_attempts),
        )


@router.errors()
async def on_router_error(event: ErrorEvent) -> bool:
    logger.exception("Unhandled router error: %s", event.exception)

    update = event.update
    message = getattr(update, "message", None)
    callback = getattr(update, "callback_query", None)

    if message:
        scope_key = _scope_key_from_message(message)
        user_id = message.from_user and message.from_user.id
        _record_error(scope_key)
        reply_markup = _build_rollback_suggestion_markup(scope_key, user_id)
        await message.answer(
            "An internal error occurred while processing your request.",
            reply_markup=reply_markup,
        )
    elif callback and callback.message:
        scope_key = _scope_key_from_message(callback.message)
        user_id = callback.from_user and callback.from_user.id
        _record_error(scope_key)
        reply_markup = _build_rollback_suggestion_markup(scope_key, user_id)
        try:
            await callback.answer("An internal error occurred.", show_alert=True)
        except Exception:
            pass
        await callback.message.answer(
            "An internal error occurred while processing your request.",
            reply_markup=reply_markup,
        )

    return True


async def _keep_typing(message: Message) -> None:
    """Send typing indicator every 5 seconds."""
    await _keep_chat_action(message, ChatAction.TYPING)


async def _keep_chat_action(message: Message, action: ChatAction) -> None:
    """Send a Telegram chat action every 5 seconds until cancelled."""
    thread_id = _thread_id(message)
    try:
        while True:
            try:
                if thread_id is not None:
                    await message.bot.send_chat_action(
                        chat_id=message.chat.id,
                        message_thread_id=thread_id,
                        action=action,
                    )
                else:
                    await message.bot.send_chat_action(chat_id=message.chat.id, action=action)
            except TelegramAPIError as e:
                logger.debug("Typing indicator failed (transient): %s", e)
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        return


async def _send_chat_action_once(message: Message, action: ChatAction) -> None:
    try:
        if _thread_id(message) is not None:
            await message.bot.send_chat_action(
                chat_id=message.chat.id,
                message_thread_id=_thread_id(message),
                action=action,
            )
        else:
            await message.bot.send_chat_action(chat_id=message.chat.id, action=action)
    except TelegramAPIError as e:
        logger.debug("Single chat action failed (transient): %s", e)


async def _send_media_reply(message: Message, media_ref: str, *, audio_as_voice: bool) -> None:
    async with prepared_media_input(media_ref) as media_input:
        if audio_as_voice and _is_voice_compatible_media(media_ref):
            await _send_audio_with_progress(message, media_input, as_voice=True)
            return
        if _is_audio_media(media_ref):
            await _send_audio_with_progress(message, media_input, as_voice=False)
            return
        await _answer_document_with_retry(message, media_input)


async def _answer_with_retry(
    send_callable,
    *args,
    floodwait_prefix: str,
    **kwargs,
):
    while True:
        try:
            return await send_callable(*args, **kwargs)
        except TelegramRetryAfter as e:
            logger.warning("%s rate-limited, retry in %ss", floodwait_prefix, e.retry_after)
            await asyncio.sleep(max(0, e.retry_after))


async def _answer_text_with_retry(
    message: Message,
    text: str,
    *,
    parse_mode: str | None = None,
    reply_markup=None,
):
    kwargs = {}
    if parse_mode is not None:
        kwargs["parse_mode"] = parse_mode
    if reply_markup is not None:
        kwargs["reply_markup"] = reply_markup
    return await _answer_with_retry(
        message.answer,
        text,
        floodwait_prefix="Text reply",
        **kwargs,
    )


async def _answer_voice_with_retry(message: Message, media_input):
    return await _answer_with_retry(
        message.answer_voice,
        media_input,
        floodwait_prefix="Voice reply",
    )


async def _answer_audio_with_retry(message: Message, media_input):
    return await _answer_with_retry(
        message.answer_audio,
        media_input,
        floodwait_prefix="Audio reply",
    )


async def _answer_document_with_retry(message: Message, media_input):
    return await _answer_with_retry(
        message.answer_document,
        media_input,
        floodwait_prefix="Document reply",
    )


async def _send_audio_with_progress(message: Message, media_input, *, as_voice: bool) -> None:
    progress_message_id: int | None = None
    progress_task: asyncio.Task | None = None
    started_at = monotonic()
    completed = False
    typing_task = asyncio.create_task(_keep_chat_action(message, ChatAction.TYPING))

    try:
        await asyncio.sleep(0)
        try:
            progress_message = await message.bot.send_message(
                chat_id=message.chat.id,
                message_thread_id=_thread_id(message),
                text=_format_audio_conversion_progress(monotonic() - started_at),
                parse_mode="HTML",
            )
            progress_message_id = progress_message.message_id
            progress_task = asyncio.create_task(
                _update_audio_conversion_progress(message, progress_message_id, started_at)
            )
        except TelegramAPIError as e:
            logger.debug("Audio conversion progress message failed: %s", e)

        if as_voice:
            await _answer_voice_with_retry(message, media_input)
        else:
            await _answer_audio_with_retry(message, media_input)
        completed = True
    finally:
        elapsed_seconds = monotonic() - started_at
        typing_task.cancel()
        if progress_task is not None:
            progress_task.cancel()
        try:
            await typing_task
        except asyncio.CancelledError:
            pass
        if progress_task is not None:
            try:
                await progress_task
            except asyncio.CancelledError:
                pass
        if progress_message_id is not None:
            final_text = (
                _format_audio_conversion_complete(elapsed_seconds)
                if completed
                else _format_audio_conversion_failed(elapsed_seconds)
            )
            await _finalize_audio_conversion_progress(
                message,
                progress_message_id,
                final_text,
            )


def _format_audio_conversion_progress(elapsed_seconds: float) -> str:
    return (
        "🎙️ <b>Converting audio reply...</b>\n"
        f"Elapsed: <code>{elapsed_seconds:.1f}s</code>"
    )


def _format_voice_transcription_progress(elapsed_seconds: float) -> str:
    return (
        "🎤 <b>Transcribing voice message...</b>\n"
        f"Elapsed: <code>{elapsed_seconds:.1f}s</code>"
    )


def _format_voice_transcription_complete(elapsed_seconds: float) -> str:
    return (
        "✅ <b>Voice message transcribed</b>\n"
        f"Transcription time: <code>{elapsed_seconds:.1f}s</code>"
    )


def _format_voice_transcription_failed(elapsed_seconds: float) -> str:
    return (
        "❌ <b>Voice transcription failed</b>\n"
        f"Elapsed before failure: <code>{elapsed_seconds:.1f}s</code>"
    )


def _format_audio_conversion_complete(elapsed_seconds: float) -> str:
    return (
        "✅ <b>Audio reply sent</b>\n"
        f"Conversion time: <code>{elapsed_seconds:.1f}s</code>"
    )


def _format_audio_conversion_failed(elapsed_seconds: float) -> str:
    return (
        "❌ <b>Audio reply failed</b>\n"
        f"Elapsed before failure: <code>{elapsed_seconds:.1f}s</code>"
    )


async def _update_audio_conversion_progress(
    message: Message,
    progress_message_id: int,
    started_at: float,
) -> None:
    try:
        while True:
            await asyncio.sleep(_AUDIO_PROGRESS_UPDATE_INTERVAL)
            try:
                await message.bot.edit_message_text(
                    chat_id=message.chat.id,
                    message_id=progress_message_id,
                    text=_format_audio_conversion_progress(monotonic() - started_at),
                    parse_mode="HTML",
                )
            except TelegramRetryAfter as e:
                logger.debug("Audio conversion progress rate-limited, retry in %ss", e.retry_after)
                await asyncio.sleep(max(0, e.retry_after))
            except TelegramAPIError as e:
                if "message is not modified" not in str(e).lower():
                    logger.debug("Audio conversion progress update failed: %s", e)
                    return
    except asyncio.CancelledError:
        return


async def _finalize_audio_conversion_progress(
    message: Message,
    progress_message_id: int,
    text: str,
) -> None:
    try:
        while True:
            try:
                await message.bot.edit_message_text(
                    chat_id=message.chat.id,
                    message_id=progress_message_id,
                    text=text,
                    parse_mode="HTML",
                )
                return
            except TelegramRetryAfter as e:
                logger.debug(
                    "Audio conversion finalization rate-limited, retry in %ss",
                    e.retry_after,
                )
                await asyncio.sleep(max(0, e.retry_after))
            except TelegramAPIError as e:
                logger.debug("Could not finalize audio conversion progress message: %s", e)
                return
    except asyncio.CancelledError:
        return


async def _send_voice_transcription_progress_message(
    message: Message,
    elapsed_seconds: float,
) -> tuple[int | None, int | None]:
    try:
        progress_message = await message.bot.send_message(
            chat_id=message.chat.id,
            message_thread_id=_thread_id(message),
            text=_format_voice_transcription_progress(elapsed_seconds),
            parse_mode="HTML",
        )
        return progress_message.message_id, None
    except TelegramRetryAfter as e:
        logger.debug("Voice transcription progress rate-limited, retry in %ss", e.retry_after)
        return None, e.retry_after
    except TelegramAPIError as e:
        logger.debug("Voice transcription progress message failed: %s", e)
        return None, None


async def _update_voice_transcription_progress(
    message: Message,
    progress_message_id: int,
    started_at: float,
) -> None:
    try:
        while True:
            await asyncio.sleep(_VOICE_TRANSCRIPTION_PROGRESS_INTERVAL)
            try:
                await message.bot.edit_message_text(
                    chat_id=message.chat.id,
                    message_id=progress_message_id,
                    text=_format_voice_transcription_progress(monotonic() - started_at),
                    parse_mode="HTML",
                )
            except TelegramRetryAfter as e:
                logger.debug("Voice transcription progress rate-limited, retry in %ss", e.retry_after)
                await asyncio.sleep(max(0, e.retry_after))
            except TelegramAPIError as e:
                if "message is not modified" not in str(e).lower():
                    logger.debug("Voice transcription progress update failed: %s", e)
                    return
    except asyncio.CancelledError:
        return


async def _publish_voice_transcription_result(
    message: Message,
    *,
    progress_message_id: int | None,
    text: str,
    send_summary: bool,
) -> None:
    if progress_message_id is not None:
        try:
            await message.bot.delete_message(
                chat_id=message.chat.id,
                message_id=progress_message_id,
            )
        except TelegramAPIError as e:
            logger.debug("Could not delete voice transcription progress message: %s", e)

    if not send_summary:
        return

    try:
        await _answer_text_with_retry(message, text, parse_mode="HTML")
    except TelegramAPIError as e:
        logger.debug("Could not send voice transcription summary message: %s", e)


async def _retry_voice_transcription_progress_message(
    message: Message,
    transcription_status_ref: dict[str, int | None],
    started_at: float,
    retry_after: int,
) -> None:
    try:
        await asyncio.sleep(max(0, retry_after))
        progress_message_id, next_retry_after = await _send_voice_transcription_progress_message(
            message,
            monotonic() - started_at,
        )
        if progress_message_id is not None:
            transcription_status_ref["message_id"] = progress_message_id
            await _update_voice_transcription_progress(message, progress_message_id, started_at)
            return
        if next_retry_after is not None:
            await _retry_voice_transcription_progress_message(
                message,
                transcription_status_ref,
                started_at,
                next_retry_after,
            )
    except asyncio.CancelledError:
        return
