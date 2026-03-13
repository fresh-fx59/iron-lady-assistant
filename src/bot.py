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
from .sessions import ChatSession, SessionManager
from .formatter import markdown_to_html, split_message, strip_html
from .features.state_store import ResumeStateStore, SteeringEvent, SteeringLedgerStore
from .features.prompt_helpers import (
    as_text as _as_text_impl,
    default_timezone_name as _default_timezone_name_impl,
    inject_tool_request as _inject_tool_request_impl,
    strip_markdown_code_fence as _strip_markdown_code_fence_impl,
    truncate_label as _truncate_label_impl,
    truncate_output as _truncate_output_impl,
    weekday_to_int as _weekday_to_int_impl,
)
from .features.scope_helpers import (
    actor_id as _actor_id_impl,
    scope_key as _scope_key_impl,
    scope_key_from_message as _scope_key_from_message_impl,
    thread_id as _thread_id_impl,
)
from .features.provider_runtime_helpers import (
    is_transient_codex_error as _is_transient_codex_error_impl,
    sanitize_transient_codex_error_response as _sanitize_transient_codex_error_response_impl,
)
from .features import provider_runtime as _provider_runtime
from .features import provider_command_handlers as _provider_command_handlers
from .features import lifecycle_ops_command_handlers as _lifecycle_ops_command_handlers
from .features import background_schedule_handlers as _background_schedule_handlers
from .features import rollback_selfmod_handlers as _rollback_selfmod_handlers
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
from .ocr_local import extract_ocr_text
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


def _reload_tool_registry() -> None:
    global tool_registry, context_plugins
    tool_registry = ToolRegistry(
        config.TOOLS_DIR,
        denylist=config.TOOL_DENYLIST,
        require_approval_for_risky=config.TOOL_REQUIRE_APPROVAL_FOR_RISKY,
    )
    context_plugins = ContextPluginRegistry([tool_registry])

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
_STEERING_CONFLICT_PATTERNS = (
    (re.compile(r"\b(delete|drop|erase|wipe|destroy)\b", re.IGNORECASE), "destructive_action"),
    (re.compile(r"\b(ignore|disregard)\s+(all|everything|previous|prior)\b", re.IGNORECASE), "broad_override"),
    (re.compile(r"\b(secret|password|token|credential)\b", re.IGNORECASE), "sensitive_data"),
)


def _thread_id(message: Message) -> int | None:
    return _thread_id_impl(message)


def _scope_key(chat_id: int, message_thread_id: int | None = None) -> str:
    return _scope_key_impl(chat_id, message_thread_id)


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
    return _scope_key_from_message_impl(message)


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
    return _actor_id_impl(message)


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
    return _truncate_label_impl(text, max_len=max_len)


def _truncate_output(text: str, max_len: int = 2000) -> str:
    return _truncate_output_impl(text, max_len=max_len)


def _as_text(value: object) -> str:
    return _as_text_impl(value)


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
    ocr_text = await asyncio.to_thread(extract_ocr_text, image_path)
    attachment_block = (
        "User attached an image.\n"
        f"Local image path: {image_path}\n"
        "Inspect this image when answering."
    )
    if ocr_text:
        attachment_block += (
            "\n"
            "Local OCR text (best-effort; low-quality images may include misreads):\n"
            f"{ocr_text}"
        )
    if base_text:
        return f"{base_text}\n\n{attachment_block}"
    return attachment_block


def _inject_tool_request(prompt_text: str, tool_name: str) -> str:
    """Force a tool to be activated by adding an explicit directive."""
    return _inject_tool_request_impl(prompt_text, tool_name)


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
    return _is_transient_codex_error_impl(text, patterns=_CODEX_TRANSIENT_ERROR_PATTERNS)


def _sanitize_transient_codex_error_response(
    response: bridge.ClaudeResponse,
    *,
    attempts: int,
) -> bridge.ClaudeResponse:
    return _sanitize_transient_codex_error_response_impl(response, attempts=attempts)


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
    return _default_timezone_name_impl()


def _strip_markdown_code_fence(text: str) -> str:
    return _strip_markdown_code_fence_impl(text)


def _weekday_to_int(name: str) -> int | None:
    return _weekday_to_int_impl(name)


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


def _load_step_plan_state() -> dict[str, object]:
    try:
        payload = json.loads(_STEP_PLAN_STATE_PATH.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _save_step_plan_state(state: dict[str, object]) -> None:
    try:
        _STEP_PLAN_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _STEP_PLAN_STATE_PATH.write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        logger.debug("Could not persist step plan state", exc_info=True)


def _current_model_label(session: object, provider) -> str:
    if _is_codex_family_cli(provider.cli):
        return session.codex_model or provider.model or "default"
    return session.model


def _step_plan_active_flag() -> bool:
    payload = _load_step_plan_state()
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
    await _lifecycle_ops_command_handlers.cmd_start(
        message,
        is_authorized=_is_authorized,
        config=config,
    )


@router.message(Command("new"))
async def cmd_new(message: Message) -> None:
    await _lifecycle_ops_command_handlers.cmd_new(
        message,
        is_authorized=_is_authorized,
        thread_id_fn=_thread_id,
        scope_key_fn=_scope_key,
        provider_manager=provider_manager,
        session_manager=session_manager,
        steering_ledger_store=steering_ledger_store,
        clear_errors_fn=_clear_errors,
        get_state_fn=_get_state,
        reflect_fn=_reflect,
    )


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
    await _provider_command_handlers.cmd_model(
        message,
        command,
        is_authorized=_is_authorized,
        thread_id_fn=_thread_id,
        scope_key_fn=_scope_key,
        current_provider_fn=_current_provider,
        current_model_label_fn=_current_model_label,
        command_args_fn=_command_args,
        model_options_fn=_model_options,
        is_codex_family_cli_fn=_is_codex_family_cli,
        session_manager=session_manager,
    )


@router.callback_query(F.data.startswith("model:"))
async def cb_model_switch(callback: CallbackQuery) -> None:
    await _provider_command_handlers.cb_model_switch(
        callback,
        is_authorized=_is_authorized,
        thread_id_fn=_thread_id,
        scope_key_fn=_scope_key,
        current_provider_fn=_current_provider,
        model_options_fn=_model_options,
        is_codex_family_cli_fn=_is_codex_family_cli,
        current_model_label_fn=_current_model_label,
        session_manager=session_manager,
        logger=logger,
    )


@router.message(Command("provider"))
async def cmd_provider(message: Message, command: CommandObject | None = None) -> None:
    await _provider_command_handlers.cmd_provider(
        message,
        command,
        is_authorized=_is_authorized,
        thread_id_fn=_thread_id,
        scope_key_from_message_fn=_scope_key_from_message,
        command_args_fn=_command_args,
        provider_manager=provider_manager,
        session_manager=session_manager,
    )


@router.callback_query(F.data.startswith("provider:"))
async def cb_provider_switch(callback: CallbackQuery) -> None:
    await _provider_command_handlers.cb_provider_switch(
        callback,
        is_authorized=_is_authorized,
        thread_id_fn=_thread_id,
        scope_key_fn=_scope_key,
        provider_manager=provider_manager,
        session_manager=session_manager,
        logger=logger,
    )


@router.message(Command("status"))
async def cmd_status(message: Message) -> None:
    await _lifecycle_ops_command_handlers.cmd_status(
        message,
        is_authorized=_is_authorized,
        thread_id_fn=_thread_id,
        scope_key_fn=_scope_key,
        session_manager=session_manager,
        provider_manager=provider_manager,
        is_codex_family_cli_fn=_is_codex_family_cli,
        current_model_label_fn=_current_model_label,
        version=config.VERSION,
    )


@router.message(Command("memory"))
async def cmd_memory(message: Message) -> None:
    await _lifecycle_ops_command_handlers.cmd_memory(
        message,
        is_authorized=_is_authorized,
        memory_manager=memory_manager,
        split_message_fn=split_message,
        strip_html_fn=strip_html,
    )


@router.message(Command("threads"))
async def cmd_threads(message: Message) -> None:
    await _lifecycle_ops_command_handlers.cmd_threads(
        message,
        is_authorized=_is_authorized,
        session_manager=session_manager,
    )


@router.message(Command("tools"))
async def cmd_tools(message: Message) -> None:
    await _lifecycle_ops_command_handlers.cmd_tools(
        message,
        is_authorized=_is_authorized,
        tool_registry=tool_registry,
        strip_html_fn=strip_html,
    )


@router.message(Command("cancel"))
async def cmd_cancel(message: Message) -> None:
    await _lifecycle_ops_command_handlers.cmd_cancel(
        message,
        is_authorized=_is_authorized,
        thread_id_fn=_thread_id,
        scope_key_fn=_scope_key,
        get_state_fn=_get_state,
        session_manager=session_manager,
        current_provider_fn=_current_provider,
        current_model_label_fn=_current_model_label,
        metrics=metrics,
    )


@router.message(Command("rollback"))
async def cmd_rollback(message: Message) -> None:
    await _rollback_selfmod_handlers.cmd_rollback(
        message,
        is_admin=_is_admin,
        show_rollback_options_fn=_show_rollback_options,
        thread_id_fn=_thread_id,
    )


@router.callback_query(F.data == "rollback_auto")
async def cb_rollback_auto(callback: CallbackQuery) -> None:
    await _rollback_selfmod_handlers.cb_rollback_auto(
        callback,
        is_admin=_is_admin,
        show_rollback_options_fn=_show_rollback_options,
        thread_id_fn=_thread_id,
    )


@router.callback_query(F.data.startswith("rollback:"))
async def cb_rollback(callback: CallbackQuery) -> None:
    await _rollback_selfmod_handlers.cb_rollback(
        callback,
        is_admin=_is_admin,
    )


@router.callback_query(F.data.startswith("rollback_confirm:"))
async def cb_rollback_confirm(callback: CallbackQuery) -> None:
    await _rollback_selfmod_handlers.cb_rollback_confirm(
        callback,
        is_admin=_is_admin,
        reset_to_commit_fn=_reset_to_commit,
        clear_errors_fn=_clear_errors,
        scope_key_from_message_fn=_scope_key_from_message,
        restart_service_fn=_restart_service,
        thread_id_fn=_thread_id,
    )


@router.callback_query(F.data == "rollback_cancel")
async def cb_rollback_cancel(callback: CallbackQuery) -> None:
    await _rollback_selfmod_handlers.cb_rollback_cancel(callback)


@router.message(Command("selfmod_stage"))
async def cmd_selfmod_stage(message: Message, command: CommandObject | None = None) -> None:
    await _rollback_selfmod_handlers.cmd_selfmod_stage(
        message,
        command,
        is_admin=_is_admin,
        command_args_fn=_command_args,
        strip_markdown_code_fence_fn=_strip_markdown_code_fence,
        self_mod_manager=self_mod_manager,
    )


@router.message(Command("selfmod_apply"))
async def cmd_selfmod_apply(message: Message, command: CommandObject | None = None) -> None:
    await _rollback_selfmod_handlers.cmd_selfmod_apply(
        message,
        command,
        is_admin=_is_admin,
        command_args_fn=_command_args,
        scope_key_from_message_fn=_scope_key_from_message,
        f08_advisory=f08_advisory,
        self_mod_manager=self_mod_manager,
        truncate_output_fn=_truncate_output,
        reload_tooling_fn=_reload_tool_registry,
    )


@router.message(Command("bg"))
async def cmd_bg(message: Message, command: CommandObject | None = None) -> None:
    await _background_schedule_handlers.cmd_bg(
        message,
        command,
        is_authorized=_is_authorized,
        task_manager=task_manager,
        command_args_fn=_command_args,
        thread_id_fn=_thread_id,
        actor_id_fn=_actor_id,
        session_manager=session_manager,
        task_backend_fn=_scheduled_task_backend,
        current_provider_fn=_current_provider,
        scope_key_fn=_scope_key,
        build_augmented_prompt_fn=_build_augmented_prompt,
    )


@router.message(F.text.regexp(r"^/bg-list(?:@[A-Za-z0-9_]+)?$"))
async def cmd_bg_list(message: Message) -> None:
    await _background_schedule_handlers.cmd_bg_list(
        message,
        is_authorized=_is_authorized,
        task_manager=task_manager,
        thread_id_fn=_thread_id,
        task_status=TaskStatus,
    )


@router.message(Command("bg_cancel"))
async def cmd_bg_cancel(message: Message, command: CommandObject | None = None) -> None:
    await _background_schedule_handlers.cmd_bg_cancel(
        message,
        command,
        is_authorized=_is_authorized,
        task_manager=task_manager,
        command_args_fn=_command_args,
        thread_id_fn=_thread_id,
        task_status=TaskStatus,
    )


@router.message(Command("schedule_every"))
async def cmd_schedule_every(message: Message, command: CommandObject | None = None) -> None:
    await _background_schedule_handlers.cmd_schedule_every(
        message,
        command,
        is_authorized=_is_authorized,
        schedule_manager=schedule_manager,
        command_args_fn=_command_args,
        thread_id_fn=_thread_id,
        session_manager=session_manager,
        current_provider_fn=_current_provider,
        scope_key_fn=_scope_key,
        actor_id_fn=_actor_id,
        task_backend_fn=_scheduled_task_backend,
        build_augmented_prompt_fn=_build_augmented_prompt,
    )


@router.message(Command("schedule_list"))
async def cmd_schedule_list(message: Message) -> None:
    await _background_schedule_handlers.cmd_schedule_list(
        message,
        is_authorized=_is_authorized,
        schedule_manager=schedule_manager,
        thread_id_fn=_thread_id,
        format_schedule_label_fn=_format_schedule_label,
        format_active_schedule_summary_fn=_format_active_schedule_summary,
        format_schedule_run_summary_fn=_format_schedule_run_summary,
    )


@router.message(Command("schedule_history"))
async def cmd_schedule_history(message: Message, command: CommandObject | None = None) -> None:
    await _background_schedule_handlers.cmd_schedule_history(
        message,
        command,
        is_authorized=_is_authorized,
        schedule_manager=schedule_manager,
        command_args_fn=_command_args,
        thread_id_fn=_thread_id,
        format_schedule_run_status_fn=_format_schedule_run_status,
    )


@router.message(Command("schedule_weekly"))
async def cmd_schedule_weekly(message: Message, command: CommandObject | None = None) -> None:
    await _background_schedule_handlers.cmd_schedule_weekly(
        message,
        command,
        is_authorized=_is_authorized,
        schedule_manager=schedule_manager,
        command_args_fn=_command_args,
        weekday_to_int_fn=_weekday_to_int,
        default_timezone_name_fn=_default_timezone_name,
        thread_id_fn=_thread_id,
        session_manager=session_manager,
        current_provider_fn=_current_provider,
        scope_key_fn=_scope_key,
        task_backend_fn=_scheduled_task_backend,
        build_augmented_prompt_fn=_build_augmented_prompt,
        actor_id_fn=_actor_id,
    )


@router.message(Command("schedule_daily"))
async def cmd_schedule_daily(message: Message, command: CommandObject | None = None) -> None:
    await _background_schedule_handlers.cmd_schedule_daily(
        message,
        command,
        is_authorized=_is_authorized,
        schedule_manager=schedule_manager,
        command_args_fn=_command_args,
        default_timezone_name_fn=_default_timezone_name,
        thread_id_fn=_thread_id,
        session_manager=session_manager,
        current_provider_fn=_current_provider,
        scope_key_fn=_scope_key,
        task_backend_fn=_scheduled_task_backend,
        build_augmented_prompt_fn=_build_augmented_prompt,
        actor_id_fn=_actor_id,
    )


@router.message(Command("schedule_cancel"))
async def cmd_schedule_cancel(message: Message, command: CommandObject | None = None) -> None:
    await _background_schedule_handlers.cmd_schedule_cancel(
        message,
        command,
        is_authorized=_is_authorized,
        schedule_manager=schedule_manager,
        command_args_fn=_command_args,
        thread_id_fn=_thread_id,
    )


async def _run_claude(
    message: Message,
    state: _ChatState,
    session: object,
    progress: ProgressReporter,
    subprocess_env: dict[str, str] | None = None,
    override_text: str | None = None,
    observed_tools: list[str] | None = None,
) -> bridge.ClaudeResponse | None:
    return await _provider_runtime.run_claude(
        message,
        state,
        session,
        progress,
        build_augmented_prompt=_build_augmented_prompt,
        subprocess_env=subprocess_env,
        override_text=override_text,
        observed_tools=observed_tools,
    )


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
    return await _provider_runtime.run_codex(
        message,
        state,
        session,
        progress,
        build_augmented_prompt=_build_augmented_prompt,
        codex_working_dir=_codex_working_dir,
        model=model,
        session_id=session_id,
        resume_arg=resume_arg,
        subprocess_env=subprocess_env,
        cli_name=cli_name,
        override_text=override_text,
        observed_tools=observed_tools,
    )


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
    return await _provider_runtime.run_codex_with_retries(
        message,
        state,
        session,
        progress,
        run_codex_fn=_run_codex,
        is_transient_error_fn=_is_transient_codex_error,
        sanitize_transient_error_fn=_sanitize_transient_codex_error_response,
        logger=logger,
        model=model,
        session_id=session_id,
        resume_arg=resume_arg,
        subprocess_env=subprocess_env,
        cli_name=cli_name,
        override_text=override_text,
        observed_tools=observed_tools,
    )


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
