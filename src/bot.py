import asyncio
from dataclasses import dataclass, field
import html
import inspect
import json
import logging
import os
import re
import shlex
import shutil
import subprocess
from datetime import datetime, timezone as tz
from pathlib import Path
from typing import Awaitable, Callable
from urllib.parse import urlparse

import yaml
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, ErrorEvent, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ChatAction
from aiogram.exceptions import TelegramAPIError

from . import bridge, config, metrics, ocr, transcribe
from .core.context_plugins import ContextPluginRegistry
from .health_invariants import HealthInvariants
from .identity import IdentityManager
from .sessions import SessionManager, make_scope_key
from .formatter import markdown_to_html, split_message, strip_html
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
identity_manager = IdentityManager(config.MEMORY_DIR)
tool_registry = ToolRegistry(config.TOOLS_DIR)
context_plugins = ContextPluginRegistry([tool_registry])
self_mod_manager = SelfModificationManager(Path(__file__).resolve().parent.parent)
task_manager: TaskManager | None = None  # Set in main()
schedule_manager: ScheduleManager | None = None  # Set in main()
_step_plan_restart_callback: Callable[[str], Awaitable[None]] | None = None
health_invariants = HealthInvariants()

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
    reset_generation: int = 0
    pending_inputs: list[str] = field(default_factory=list)


# Per-conversation state dict
_chat_states: dict[str, _ChatState] = {}
_error_counts: dict[str, int] = {}
_CODEX_TRANSIENT_ERROR_PATTERNS = (
    re.compile(r"stream disconnected before completion", re.IGNORECASE),
    re.compile(r"transport error:\s*timeout", re.IGNORECASE),
    re.compile(r"\breconnecting\.\.\.\s*\d+/\d+", re.IGNORECASE),
    re.compile(r"\b(etimedout|econnreset|connection reset)\b", re.IGNORECASE),
)
_AUDIO_AS_VOICE_TAG_RE = re.compile(r"\[\[\s*audio_as_voice\s*\]\]", re.IGNORECASE)
# Accept optional visual prefixes like "📍 MEDIA:/tmp/file.mp3" while still
# requiring the directive to start the line.
_MEDIA_LINE_RE = re.compile(r"^\s*(?:[^\w\s]+\s*)?MEDIA:\s*(.+?)\s*$", re.IGNORECASE)
_VOICE_COMPATIBLE_EXTENSIONS = {".ogg", ".opus", ".mp3", ".m4a"}
_AUDIO_EXTENSIONS = _VOICE_COMPATIBLE_EXTENSIONS | {".wav", ".aac", ".flac"}
_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png"}
_STEP_PLAN_STATE_PATH = config.MEMORY_DIR / "step_plan_state.json"
_STEP_PLAN_FILE_PATTERN = re.compile(r"^(\d+)\s*-\s*.+\.md$", re.IGNORECASE)


def _thread_id(message: Message) -> int | None:
    return getattr(message, "message_thread_id", None)


def _scope_key(chat_id: int, message_thread_id: int | None = None) -> str:
    return make_scope_key(chat_id, message_thread_id)


def _scope_key_from_message(message: Message) -> str:
    return _scope_key(message.chat.id, _thread_id(message))


def _get_state(scope_key: str) -> _ChatState:
    """Get or create state for a conversation scope."""
    if scope_key not in _chat_states:
        _chat_states[scope_key] = _ChatState(
            lock=asyncio.Lock(),
            process_handle=None,
            cancel_requested=False,
        )
    return _chat_states[scope_key]


def _queue_pending_input(state: _ChatState, text: str) -> int:
    """Queue additional user context while current request is running."""
    state.pending_inputs.append(text)
    return len(state.pending_inputs)


def _drain_pending_inputs(state: _ChatState) -> list[str]:
    """Atomically consume queued mid-flight inputs."""
    if not state.pending_inputs:
        return []
    pending = list(state.pending_inputs)
    state.pending_inputs.clear()
    return pending


async def _cancel_active_scope_run(state: _ChatState, require_process: bool = False) -> bool:
    """Request cancellation and kill the active subprocess if present."""
    if not state.lock.locked():
        return False
    has_proc = bool(state.process_handle and state.process_handle.get("proc"))
    if require_process and not has_proc:
        return False
    state.cancel_requested = True
    _drain_pending_inputs(state)
    if has_proc:
        proc = state.process_handle["proc"]
        kill_result = proc.kill()
        if inspect.isawaitable(kill_result):
            await kill_result
    return True


def _build_midflight_followup_prompt(items: list[str]) -> str:
    """Build a single follow-up prompt from queued user additions."""
    if not items:
        return ""
    lines = "\n".join(f"- {item}" for item in items)
    return (
        "Additional user information arrived while the previous response was being generated.\n"
        "Use it as updated context and continue from the latest conversation state.\n\n"
        f"{lines}"
    )


def set_step_plan_restart_callback(
    callback: Callable[[str], Awaitable[None]] | None,
) -> None:
    """Inject restart callback from main runtime."""
    global _step_plan_restart_callback
    _step_plan_restart_callback = callback


def _step_plan_default_state() -> dict:
    return {
        "active": False,
        "name": "",
        "folder_path": "",
        "chat_id": 0,
        "message_thread_id": None,
        "user_id": 0,
        "steps": [],
        "current_index": 0,
        "current_task_id": None,
        "restart_between_steps": True,
        "last_error": "",
        "updated_at": datetime.now(tz.utc).isoformat(),
    }


def _load_step_plan_state() -> dict:
    if not _STEP_PLAN_STATE_PATH.exists():
        return _step_plan_default_state()
    try:
        data = json.loads(_STEP_PLAN_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Failed to read step plan state, resetting", exc_info=True)
        return _step_plan_default_state()
    if not isinstance(data, dict):
        return _step_plan_default_state()
    state = _step_plan_default_state()
    state.update(data)
    return state


def _save_step_plan_state(state: dict) -> None:
    payload = _step_plan_default_state()
    payload.update(state)
    payload["updated_at"] = datetime.now(tz.utc).isoformat()
    _STEP_PLAN_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _STEP_PLAN_STATE_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _load_plan_steps_from_folder(folder_path: str) -> list[str]:
    folder = Path(folder_path).expanduser().resolve()
    if not folder.exists() or not folder.is_dir():
        raise FileNotFoundError(f"Folder not found: {folder}")

    ordered_files: list[tuple[int, Path]] = []
    for child in folder.iterdir():
        if not child.is_file():
            continue
        match = _STEP_PLAN_FILE_PATTERN.match(child.name)
        if not match:
            continue
        ordered_files.append((int(match.group(1)), child))

    ordered_files.sort(key=lambda row: row[0])
    return [str(path) for _, path in ordered_files]


def _build_step_plan_prompt(step_file: str, step_index: int, total_steps: int) -> str:
    return (
        f"Execute implementation step {step_index}/{total_steps} from this plan file:\n"
        f"{step_file}\n\n"
        "Requirements:\n"
        "1. Follow the step plan in the file.\n"
        "2. Implement code changes fully and verify with tests.\n"
        "3. Mark the plan file as applied when done.\n"
        "4. Bump project version and include version in commit message.\n"
        "5. Commit and push to trigger deployment/restart.\n"
        "6. If blocked, report concrete blocker and stop safely."
    )


async def _submit_current_step_plan_task(state: dict) -> str:
    if not task_manager:
        raise RuntimeError("Background tasks not available.")
    steps = state.get("steps") or []
    if not steps:
        raise RuntimeError("Step plan has no steps.")
    current_index = int(state.get("current_index") or 0)
    if current_index >= len(steps):
        raise RuntimeError("All steps already completed.")

    step_file = str(steps[current_index])
    prompt = _build_step_plan_prompt(step_file, current_index + 1, len(steps))
    full_prompt = _build_augmented_prompt(prompt)

    chat_id = int(state.get("chat_id") or 0)
    message_thread_id = state.get("message_thread_id")
    user_id = int(state.get("user_id") or 0)
    session = session_manager.get(chat_id, message_thread_id)

    task_id = await task_manager.submit(
        chat_id=chat_id,
        message_thread_id=message_thread_id,
        user_id=user_id,
        prompt=full_prompt,
        model=session.model,
        session_id=session.claude_session_id,
    )
    state["current_task_id"] = task_id
    state["last_error"] = ""
    _save_step_plan_state(state)
    return task_id


def _step_plan_status_text(state: dict) -> str:
    steps = state.get("steps") or []
    current_index = int(state.get("current_index") or 0)
    total = len(steps)
    current_file = steps[current_index] if 0 <= current_index < total else "-"
    lines = [
        "<b>Step Plan Status</b>",
        "",
        f"<b>Active:</b> {'yes' if state.get('active') else 'no'}",
        f"<b>Name:</b> {html.escape(str(state.get('name') or '-'))}",
        f"<b>Progress:</b> {min(current_index, total)}/{total}",
        f"<b>Current file:</b> <code>{html.escape(str(current_file))}</code>",
        f"<b>Current task:</b> <code>{html.escape(str(state.get('current_task_id') or '-'))}</code>",
        f"<b>Restart between steps:</b> {'yes' if state.get('restart_between_steps') else 'no'}",
    ]
    last_error = str(state.get("last_error") or "").strip()
    if last_error:
        lines.extend(["", f"<b>Last error:</b> {html.escape(last_error[:500])}"])
    return "\n".join(lines)


class StepPlanObserver:
    """Observer that advances persisted step plans after background task completion."""

    async def on_task_finished(self, task) -> None:
        state = _load_step_plan_state()
        if not state.get("active"):
            return

        if str(state.get("current_task_id") or "") != task.id:
            return

        if task.status != TaskStatus.COMPLETED:
            state["active"] = False
            state["last_error"] = task.error or f"Task ended with status={task.status.value}"
            state["current_task_id"] = None
            _save_step_plan_state(state)
            try:
                await self._notify(
                    int(state.get("chat_id") or task.chat_id),
                    state.get("message_thread_id"),
                    "❌ Step plan paused because current step failed. Use /stepplan_status to inspect.",
                )
            except Exception:
                logger.exception("Failed to send step plan failure notification")
            return

        steps = state.get("steps") or []
        state["current_index"] = int(state.get("current_index") or 0) + 1
        state["current_task_id"] = None

        if int(state["current_index"]) >= len(steps):
            state["active"] = False
            _save_step_plan_state(state)
            await self._notify(
                int(state.get("chat_id") or task.chat_id),
                state.get("message_thread_id"),
                "✅ Step plan completed.",
            )
            return

        _save_step_plan_state(state)
        await self._notify(
            int(state.get("chat_id") or task.chat_id),
            state.get("message_thread_id"),
            "✅ Step completed. Restarting bot to continue with the next step...",
        )
        if state.get("restart_between_steps", True) and _step_plan_restart_callback:
            await _step_plan_restart_callback("step_plan_next_step")
            return

        try:
            next_task_id = await _submit_current_step_plan_task(state)
            await self._notify(
                int(state.get("chat_id") or task.chat_id),
                state.get("message_thread_id"),
                f"🔄 Continuing without restart. Next step task queued: <code>{next_task_id}</code>",
            )
        except Exception as exc:
            state = _load_step_plan_state()
            state["active"] = False
            state["last_error"] = f"Could not queue next step: {exc}"
            state["current_task_id"] = None
            _save_step_plan_state(state)
            await self._notify(
                int(state.get("chat_id") or task.chat_id),
                state.get("message_thread_id"),
                f"❌ Step plan paused: {html.escape(str(exc)[:300])}",
            )

    async def _notify(self, chat_id: int, message_thread_id: int | None, text: str) -> None:
        if not task_manager:
            return
        await task_manager.bot.send_message(
            chat_id=chat_id,
            message_thread_id=message_thread_id,
            text=text,
            parse_mode="HTML",
        )


_step_plan_observer = StepPlanObserver()


def get_step_plan_observer() -> StepPlanObserver:
    return _step_plan_observer


async def resume_step_plan_after_restart() -> None:
    """Resume pending step plan on startup if active."""
    state = _load_step_plan_state()
    if not state.get("active"):
        return
    if not task_manager:
        return

    steps = state.get("steps") or []
    current_index = int(state.get("current_index") or 0)
    if current_index >= len(steps):
        state["active"] = False
        state["current_task_id"] = None
        _save_step_plan_state(state)
        return

    # Task IDs from previous process are stale after restart.
    state["current_task_id"] = None
    _save_step_plan_state(state)

    try:
        task_id = await _submit_current_step_plan_task(state)
        await task_manager.bot.send_message(
            chat_id=int(state.get("chat_id") or 0),
            message_thread_id=state.get("message_thread_id"),
            text=(
                "🔁 <b>Step plan resumed after restart</b>\n"
                f"Queued step {current_index + 1}/{len(steps)} as task "
                f"<code>{task_id}</code>."
            ),
            parse_mode="HTML",
        )
    except Exception as exc:
        state = _load_step_plan_state()
        state["active"] = False
        state["last_error"] = f"Resume failed: {exc}"
        _save_step_plan_state(state)
        logger.exception("Failed to resume step plan after restart")


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


def _should_suggest_rollback(scope_key: str) -> bool:
    return _error_counts.get(scope_key, 0) >= 3


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


def _inject_tool_request(prompt_text: str, tool_name: str) -> str:
    """Force a tool to be activated by adding an explicit directive."""
    base = prompt_text.rstrip()
    return f"{base}\n\nUSE_TOOL: {tool_name}\n"


def _build_augmented_prompt(raw_prompt: str) -> str:
    """Compose prompt with memory, identity, tools, and memory instructions."""
    memory_context = _as_text(memory_manager.build_context(raw_prompt))
    identity_context = _as_text(identity_manager.build_context())
    tool_context = _as_text(context_plugins.build_context(raw_prompt))
    memory_instructions = _as_text(memory_manager.build_instructions())
    invariants_context = ""
    if config.HEALTH_INVARIANTS_ENABLED:
        invariants_context = health_invariants.build_block(
            app_version=config.VERSION,
            memory_dir=config.MEMORY_DIR,
            max_chars=config.HEALTH_INVARIANTS_MAX_CHARS,
            stale_after_hours=config.HEALTH_INVARIANTS_STALE_HOURS,
            provider_fail_warn_ratio=config.HEALTH_INVARIANTS_PROVIDER_FAIL_WARN_RATIO,
            empty_warn_ratio=config.HEALTH_INVARIANTS_EMPTY_WARN_RATIO,
            min_sample_size=config.HEALTH_INVARIANTS_MIN_SAMPLE_SIZE,
            claude_md_path=_repo_root() / "CLAUDE.md",
        )

    prompt_parts: list[str] = []
    if memory_context:
        prompt_parts.append(memory_context)
    if identity_context:
        prompt_parts.append(identity_context)
    if invariants_context:
        prompt_parts.append(invariants_context)
    if tool_context:
        prompt_parts.append(tool_context)
    prompt_parts.append(raw_prompt + memory_instructions)
    return "\n\n".join(prompt_parts)


def _is_transient_codex_error(text: str | None) -> bool:
    if not text:
        return False
    return any(pattern.search(text) for pattern in _CODEX_TRANSIENT_ERROR_PATTERNS)


def _media_extension(media_ref: str) -> str:
    raw = media_ref.strip().strip("`").strip("\"'")
    if not raw:
        return ""
    parsed = urlparse(raw)
    if parsed.scheme in {"http", "https"}:
        return Path(parsed.path).suffix.lower()
    return Path(raw).suffix.lower()


def _is_voice_compatible_media(media_ref: str) -> bool:
    return _media_extension(media_ref) in _VOICE_COMPATIBLE_EXTENSIONS


def _is_audio_media(media_ref: str) -> bool:
    return _media_extension(media_ref) in _AUDIO_EXTENSIONS


def _is_image_media(media_ref: str) -> bool:
    return _media_extension(media_ref) in _IMAGE_EXTENSIONS


def _resolve_media_input(media_ref: str):
    raw = media_ref.strip().strip("`").strip("\"'")
    parsed = urlparse(raw)
    if parsed.scheme in {"http", "https"}:
        return raw
    path = Path(raw).expanduser()
    if path.exists() and path.is_file():
        return FSInputFile(path)
    return raw


def _extract_media_directives(text: str) -> tuple[str, list[str], bool]:
    if not text:
        return "", [], False

    audio_as_voice = bool(_AUDIO_AS_VOICE_TAG_RE.search(text))
    without_tag = _AUDIO_AS_VOICE_TAG_RE.sub("", text)

    media_refs: list[str] = []
    text_lines: list[str] = []
    for line in without_tag.splitlines():
        match = _MEDIA_LINE_RE.match(line)
        if match:
            media = match.group(1).strip().strip("`").strip("\"'")
            if media:
                media_refs.append(media)
            continue
        text_lines.append(line)

    cleaned_text = "\n".join(text_lines).strip()
    return cleaned_text, media_refs, audio_as_voice


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
    repo_root = _repo_root()

    verify = subprocess.run(
        ["git", "-C", str(repo_root), "rev-parse", "--verify", f"{target_hash}^{{commit}}"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if verify.returncode != 0:
        return False, verify.stderr.strip() or "Commit not found"

    status = subprocess.run(
        ["git", "-C", str(repo_root), "status", "--porcelain"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if status.returncode != 0:
        return False, status.stderr.strip() or "git status failed"
    if status.stdout.strip():
        return False, "Working tree has uncommitted changes; refusing rollback"

    stamp = datetime.now(tz.utc).strftime("%Y%m%d%H%M%S")
    recovery_branch = f"rollback-safety/{stamp}"
    branch = subprocess.run(
        ["git", "-C", str(repo_root), "branch", recovery_branch, "HEAD"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if branch.returncode != 0:
        return False, branch.stderr.strip() or "Failed to create rollback recovery branch"

    reset = subprocess.run(
        ["git", "-C", str(repo_root), "reset", "--hard", target_hash],
        capture_output=True,
        text=True,
        timeout=20,
    )
    if reset.returncode != 0:
        return False, reset.stderr.strip() or "git reset --hard failed"

    deploy_dir = repo_root / ".deploy"
    deploy_dir.mkdir(exist_ok=True)
    (deploy_dir / "start_times").write_text("")
    return True, (reset.stdout.strip() or f"Rolled back to {target_hash} (recovery branch: {recovery_branch})")


def _find_provider_cli(cli_name: str) -> str | None:
    """Resolve provider executable path from current process PATH."""
    return shutil.which(cli_name)


def _current_provider(scope_key: str):
    return provider_manager.get_provider(scope_key)


def _current_model_label(session: object, provider) -> str:
    if provider.cli == "codex":
        return session.codex_model or provider.model or "default"
    return session.model


def _model_options(provider) -> list[str]:
    if provider.cli == "codex":
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


def _codex_working_dir() -> str:
    """Run Codex from user home so it can access files under that tree."""
    return str(Path.home())


@router.message(F.text == "/start")
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
        "/memory_forget <key> — Remove semantic fact by key",
        "/memory_consolidate — De-duplicate and clean memory facts",
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
        "/stepplan_start <folder> [--no-restart] — Start persisted step plan (admin)",
        "/stepplan_status — Show persisted step plan status",
        "/stepplan_stop — Stop persisted step plan",
        "/cancel — Cancel current request",
    ])

    await message.answer("\n".join(status_lines), parse_mode="HTML")


@router.message(F.text == "/new")
async def cmd_new(message: Message) -> None:
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    chat_id = message.chat.id
    thread_id = _thread_id(message)
    scope_key = _scope_key(chat_id, thread_id)
    state = _get_state(scope_key)
    state.reset_generation += 1
    cancelled = await _cancel_active_scope_run(state)
    session = session_manager.get(chat_id, thread_id)
    if session.claude_session_id and os.getenv("DISABLE_REFLECTION") != "1":
        asyncio.create_task(_reflect(chat_id, session))
    session_manager.new_conversation(chat_id, thread_id)
    session_manager.new_codex_conversation(chat_id, thread_id)
    _clear_errors(scope_key)
    if cancelled:
        await message.answer("Conversation cleared immediately. Active request was cancelled.")
    else:
        await message.answer("Conversation cleared. Send a message to start fresh.")


async def _reflect(chat_id: int, session: object) -> None:
    """Background: ask Claude to summarize the conversation, store as episode."""
    try:
        reflect_prompt = (
            "Summarize this conversation concisely. Output ONLY valid JSON, no markdown:\n"
            '{"summary": "one-sentence summary", "topics": ["topic1"], '
            '"decisions": ["decision1"], "entities": ["entity1"]}'
        )
        async for event in bridge.stream_message(
            prompt=reflect_prompt,
            session_id=session.claude_session_id,
            model="haiku",
            working_dir=config.CLAUDE_WORKING_DIR,
        ):
            if event.event_type == bridge.StreamEventType.RESULT and event.response:
                text = event.response.text.strip()
                # Strip markdown code fences if present
                if text.startswith("```"):
                    text = text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
                data = json.loads(text)
                memory_manager.add_episode(
                    chat_id=chat_id,
                    summary=data.get("summary", ""),
                    topics=data.get("topics"),
                    decisions=data.get("decisions"),
                    entities=data.get("entities"),
                )
                logger.info("Chat %d: reflection stored", chat_id)
                return
    except Exception:
        logger.warning("Chat %d: reflection failed", chat_id, exc_info=True)


@router.message(F.text.startswith("/model"))
async def cmd_model(message: Message) -> None:
    """Show model selection keyboard."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    chat_id = message.chat.id
    thread_id = _thread_id(message)
    scope_key = _scope_key(chat_id, thread_id)
    session = session_manager.get(chat_id, thread_id)
    provider = _current_provider(scope_key)
    current = _current_model_label(session, provider)

    raw_text = message.text or ""
    parts = raw_text.split(maxsplit=1)
    if len(parts) > 1:
        requested = parts[1].split()[0]
        options = _model_options(provider)
        if requested not in options:
            await message.answer(f"Invalid model: {requested}. Use /model to see options.")
            return

        if provider.cli == "codex":
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

    if provider.cli == "codex":
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


@router.message(F.text.startswith("/provider"))
async def cmd_provider(message: Message) -> None:
    """Show provider selection keyboard or switch provider by argument."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return

    chat_id = message.chat.id
    thread_id = _thread_id(message)
    scope_key = _scope_key_from_message(message)
    raw_text = message.text or ""
    parts = raw_text.split(maxsplit=1)
    if len(parts) > 1:
        requested = parts[1].strip()
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


@router.message(F.text == "/status")
async def cmd_status(message: Message) -> None:
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    chat_id = message.chat.id
    thread_id = _thread_id(message)
    scope_key = _scope_key(chat_id, thread_id)
    session = session_manager.get(chat_id, thread_id)
    provider = provider_manager.get_provider(scope_key)
    if provider.cli == "codex":
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


@router.message(F.text == "/memory")
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


@router.message(F.text.startswith("/memory_forget"))
async def cmd_memory_forget(message: Message) -> None:
    """Remove semantic memory facts by key."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Usage: /memory_forget <fact_key>")
        return

    key = parts[1].strip()
    removed = memory_manager.forget_fact(key)
    if not removed:
        await message.answer(f"No facts found for key: <code>{html.escape(key)}</code>", parse_mode="HTML")
        return
    await message.answer(f"Removed facts for key: <code>{html.escape(key)}</code>", parse_mode="HTML")


@router.message(F.text == "/memory_consolidate")
async def cmd_memory_consolidate(message: Message) -> None:
    """Merge duplicate facts and prune low-confidence noise."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    stats = memory_manager.consolidate_facts()
    await message.answer(
        "Memory consolidation complete.\n"
        f"Before: <b>{stats['before']}</b>\n"
        f"After: <b>{stats['after']}</b>\n"
        f"Removed: <b>{stats['removed']}</b>",
        parse_mode="HTML",
    )


@router.message(F.text == "/threads")
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


@router.message(F.text == "/tools")
async def cmd_tools(message: Message) -> None:
    """List available tools."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    content = tool_registry.format_for_display()
    try:
        await message.answer(content, parse_mode="HTML")
    except Exception:
        await message.answer(strip_html(content))


@router.message(F.text == "/cancel")
async def cmd_cancel(message: Message) -> None:
    """Cancel the current request if one is running."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return

    chat_id = message.chat.id
    thread_id = _thread_id(message)
    scope_key = _scope_key(chat_id, thread_id)
    state = _get_state(scope_key)

    if not state.lock.locked():
        await message.answer("Nothing to cancel.")
        return

    cancelled = await _cancel_active_scope_run(state, require_process=True)
    if not cancelled:
        await message.answer("Nothing to cancel.")
        return
    session = session_manager.get(chat_id, thread_id)
    provider = _current_provider(scope_key)
    metrics.CLAUDE_REQUESTS_TOTAL.labels(
        model=_current_model_label(session, provider),
        status="cancelled",
    ).inc()


@router.message(F.text == "/rollback")
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


@router.message(F.text.startswith("/selfmod_stage"))
async def cmd_selfmod_stage(message: Message) -> None:
    """Admin-only: stage plugin candidate code into sandbox."""
    if not _is_admin(message.from_user and message.from_user.id):
        await message.answer("This command is admin-only.")
        return

    text = message.text or ""
    header, sep, body = text.partition("\n")
    parts = header.split(maxsplit=1)
    if len(parts) < 2:
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

    relative_path = parts[1].strip()
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


@router.message(F.text.startswith("/selfmod_apply"))
async def cmd_selfmod_apply(message: Message) -> None:
    """Admin-only: validate sandbox candidate, promote, and hot-reload."""
    if not _is_admin(message.from_user and message.from_user.id):
        await message.answer("This command is admin-only.")
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        await message.answer(
            "Usage: /selfmod_apply <relative_plugin_path.py> [test_target]\n"
            "Example: /selfmod_apply tools_plugin.py tests/test_context_plugins.py"
        )
        return

    relative_path = parts[1].strip()
    test_target = parts[2].strip() if len(parts) > 2 else "tests/test_context_plugins.py"

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
        tool_registry = ToolRegistry(config.TOOLS_DIR)
        context_plugins = ContextPluginRegistry([tool_registry])


@router.message(F.text.startswith("/bg "))
async def cmd_bg(message: Message) -> None:
    """Run a task in the background."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return

    if not task_manager:
        await message.answer("Background tasks not available.")
        return

    # Extract prompt after /bg
    prompt = message.text[3:].strip()
    if not prompt:
        await message.answer("Please provide a task to run in background.\n\nExample: /bg write a python script to backup my database")
        return

    chat_id = message.chat.id
    thread_id = _thread_id(message)
    session = session_manager.get(chat_id, thread_id)

    full_prompt = _build_augmented_prompt(prompt)

    task_id = await task_manager.submit(
        chat_id=chat_id,
        message_thread_id=thread_id,
        user_id=_actor_id(message),
        prompt=full_prompt,
        model=session.model,
        session_id=session.claude_session_id,
    )

    lines = [
        f"✅ <b>Task queued</b>",
        f"",
        f"<b>Task ID:</b> <code>{task_id}</code>",
        f"<b>Model:</b> {session.model}",
        f"",
        f"I'll notify you when it completes. You can continue chatting.",
        f"",
        f"<b>Commands:</b>",
        f"/bg-list — List active tasks",
        f"/bg_cancel {task_id} — Cancel this task",
    ]
    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(F.text == "/bg-list")
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


@router.message(F.text.startswith("/bg_cancel "))
async def cmd_bg_cancel(message: Message) -> None:
    """Cancel a background task."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return

    if not task_manager:
        await message.answer("Background tasks not available.")
        return

    task_id = message.text[11:].strip()
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


@router.message(F.text.startswith("/stepplan_start"))
async def cmd_stepplan_start(message: Message) -> None:
    """Start persisted step-by-step implementation plan."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    if not _is_admin(message.from_user and message.from_user.id):
        await message.answer("Only admin can start a step plan.")
        return
    if not task_manager:
        await message.answer("Background tasks not available.")
        return

    raw_args = (message.text or "")[len("/stepplan_start"):].strip()
    if not raw_args:
        await message.answer(
            "Usage: /stepplan_start <folder_path> [--no-restart]\n"
            "Example: /stepplan_start \"/home/claude-developer/.../Ouroboros Improvement Plan\""
        )
        return

    try:
        argv = shlex.split(raw_args)
    except ValueError as exc:
        await message.answer(f"Invalid arguments: {exc}")
        return

    restart_between_steps = True
    folder_tokens: list[str] = []
    for token in argv:
        if token == "--no-restart":
            restart_between_steps = False
            continue
        folder_tokens.append(token)

    if not folder_tokens:
        await message.answer("Please provide a folder path.")
        return

    folder_path = " ".join(folder_tokens)
    try:
        steps = _load_plan_steps_from_folder(folder_path)
    except Exception as exc:
        await message.answer(f"Could not load plan folder: {exc}")
        return

    if not steps:
        await message.answer(
            "No step files found. Expected files like:\n"
            "<code>01 - Something.md</code>",
            parse_mode="HTML",
        )
        return

    state = _step_plan_default_state()
    state.update(
        {
            "active": True,
            "name": Path(folder_path).expanduser().resolve().name,
            "folder_path": str(Path(folder_path).expanduser().resolve()),
            "chat_id": message.chat.id,
            "message_thread_id": _thread_id(message),
            "user_id": _actor_id(message),
            "steps": steps,
            "current_index": 0,
            "current_task_id": None,
            "restart_between_steps": restart_between_steps,
            "last_error": "",
        }
    )
    _save_step_plan_state(state)

    try:
        task_id = await _submit_current_step_plan_task(state)
    except Exception as exc:
        state = _load_step_plan_state()
        state["active"] = False
        state["last_error"] = f"Failed to queue first step: {exc}"
        _save_step_plan_state(state)
        await message.answer(f"Could not queue first step: {exc}")
        return

    await message.answer(
        "✅ <b>Step plan started</b>\n"
        f"<b>Steps:</b> {len(steps)}\n"
        f"<b>Restart between steps:</b> {'yes' if restart_between_steps else 'no'}\n"
        f"<b>First task:</b> <code>{task_id}</code>",
        parse_mode="HTML",
    )


@router.message(F.text == "/stepplan_status")
async def cmd_stepplan_status(message: Message) -> None:
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    state = _load_step_plan_state()
    await message.answer(_step_plan_status_text(state), parse_mode="HTML")


@router.message(F.text == "/stepplan_stop")
async def cmd_stepplan_stop(message: Message) -> None:
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    if not _is_admin(message.from_user and message.from_user.id):
        await message.answer("Only admin can stop a step plan.")
        return

    state = _load_step_plan_state()
    running_task_id = str(state.get("current_task_id") or "")
    state["active"] = False
    state["current_task_id"] = None
    state["last_error"] = ""
    _save_step_plan_state(state)

    cancelled = False
    if running_task_id and task_manager:
        cancelled = await task_manager.cancel(running_task_id)

    suffix = " Running step task cancelled." if cancelled else ""
    await message.answer(f"🛑 Step plan stopped.{suffix}")


@router.message(F.text.startswith("/schedule_every"))
async def cmd_schedule_every(message: Message) -> None:
    """Create recurring background task schedule."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    if not schedule_manager:
        await message.answer("Scheduler not available.")
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3:
        await message.answer(
            "Usage: /schedule_every <minutes> <task>\n"
            "Example: /schedule_every 60 summarize open PRs"
        )
        return

    try:
        interval_minutes = int(parts[1])
    except ValueError:
        await message.answer("Minutes must be an integer.")
        return

    if interval_minutes < 1 or interval_minutes > 10080:
        await message.answer("Minutes must be between 1 and 10080.")
        return

    task_text = parts[2].strip()
    if not task_text:
        await message.answer("Task text cannot be empty.")
        return

    chat_id = message.chat.id
    thread_id = _thread_id(message)
    session = session_manager.get(chat_id, thread_id)
    full_prompt = _build_augmented_prompt(task_text)

    schedule_id = await schedule_manager.create_every(
        chat_id=chat_id,
        message_thread_id=thread_id,
        user_id=_actor_id(message),
        prompt=full_prompt,
        interval_minutes=interval_minutes,
        model=session.model,
        session_id=session.claude_session_id,
    )
    await message.answer(
        "✅ Recurring schedule created\n"
        f"<b>ID:</b> <code>{schedule_id[:8]}</code>\n"
        f"<b>Interval:</b> every {interval_minutes} min\n"
        f"Use /schedule_list to view schedules.",
        parse_mode="HTML",
    )


@router.message(F.text == "/schedule_list")
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

    lines = ["<b>Recurring schedules:</b>", ""]
    for item in schedules:
        next_run_local = item.next_run_at.astimezone().strftime("%Y-%m-%d %H:%M")
        if item.schedule_type == "weekly" and item.daily_time and item.weekly_day is not None:
            tz_name = item.timezone_name or "UTC"
            weekday = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][item.weekly_day]
            schedule_label = f"weekly {weekday} {item.daily_time} ({tz_name})"
        elif item.schedule_type == "daily" and item.daily_time:
            tz_name = item.timezone_name or "UTC"
            schedule_label = f"daily at {item.daily_time} ({tz_name})"
        else:
            schedule_label = f"every {item.interval_minutes} min"
        lines.append(f"⏱ <code>{item.id[:8]}</code> — {schedule_label}")
        lines.append(f"   next: {next_run_local}")
        lines.append(f"   {item.prompt[:80]}...")
        lines.append("")
    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(F.text.startswith("/schedule_weekly"))
async def cmd_schedule_weekly(message: Message) -> None:
    """Create weekly recurring background task schedule."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    if not schedule_manager:
        await message.answer("Scheduler not available.")
        return

    parts = (message.text or "").split(maxsplit=3)
    if len(parts) < 4:
        await message.answer(
            "Usage: /schedule_weekly <day> <HH:MM> <task>\n"
            "Example: /schedule_weekly mon 09:00 check sprint board"
        )
        return

    weekday = _weekday_to_int(parts[1])
    if weekday is None:
        await message.answer("Day must be one of: mon,tue,wed,thu,fri,sat,sun.")
        return

    daily_time = parts[2].strip()
    if not re.fullmatch(r"([01]\d|2[0-3]):([0-5]\d)", daily_time):
        await message.answer("Time must be in HH:MM 24-hour format.")
        return

    task_text = parts[3].strip()
    if not task_text:
        await message.answer("Task text cannot be empty.")
        return

    timezone_name = _default_timezone_name()
    chat_id = message.chat.id
    thread_id = _thread_id(message)
    session = session_manager.get(chat_id, thread_id)
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
            model=session.model,
            session_id=session.claude_session_id,
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


@router.message(F.text.startswith("/schedule_daily"))
async def cmd_schedule_daily(message: Message) -> None:
    """Create daily recurring background task schedule."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    if not schedule_manager:
        await message.answer("Scheduler not available.")
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3:
        await message.answer(
            "Usage: /schedule_daily <HH:MM> <task>\n"
            "Example: /schedule_daily 09:00 check PR reviews"
        )
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
    full_prompt = _build_augmented_prompt(task_text)

    try:
        schedule_id = await schedule_manager.create_daily(
            chat_id=chat_id,
            message_thread_id=thread_id,
            user_id=_actor_id(message),
            prompt=full_prompt,
            daily_time=daily_time,
            timezone_name=timezone_name,
            model=session.model,
            session_id=session.claude_session_id,
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


@router.message(F.text.startswith("/schedule_cancel "))
async def cmd_schedule_cancel(message: Message) -> None:
    """Cancel recurring schedule by full or short ID."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    if not schedule_manager:
        await message.answer("Scheduler not available.")
        return

    short_id = (message.text or "")[17:].strip()
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
) -> bridge.ClaudeResponse | None:
    """Run a single Claude subprocess attempt. Returns the response or None."""
    state.process_handle = {}

    raw_prompt = override_text or message.text or ""
    prompt = _build_augmented_prompt(raw_prompt)

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
                    await progress.report_tool(event.tool_name, event.tool_input)
            case bridge.StreamEventType.RESULT:
                return event.response
            case "TOOL_USE":
                if getattr(event, "tool_name", None):
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
    override_text: str | None = None,
) -> bridge.ClaudeResponse | None:
    """Run a single Codex CLI subprocess attempt. Returns the response or None."""
    state.process_handle = {}

    raw_prompt = override_text or message.text or ""
    prompt = _build_augmented_prompt(raw_prompt)

    stream = bridge.stream_codex_message(
        prompt=prompt,
        session_id=session_id,
        model=model,
        resume_arg=resume_arg,
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
                    await progress.report_tool(event.tool_name, event.tool_input)
            case bridge.StreamEventType.RESULT:
                return event.response
            case "TOOL_USE":
                if getattr(event, "tool_name", None):
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
    override_text: str | None = None,
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
            override_text=override_text,
        )
        if not response:
            return None
        if state.cancel_requested or not response.is_error or not _is_transient_codex_error(response.text):
            return response
        if retries_left <= 0:
            return response

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

    if not transcribe.is_available():
        await message.answer(
            "Voice messages are not supported — whisper.cpp is not installed.\n"
            "Run <code>bash setup_whisper.sh</code> on the server to enable.",
            parse_mode="HTML",
        )
        return

    import tempfile

    file = await message.bot.get_file(message.voice.file_id)
    tmp = tempfile.NamedTemporaryFile(suffix=".oga", delete=False)
    try:
        await message.bot.download_file(file.file_path, tmp.name)
        text = await transcribe.transcribe(tmp.name)
        logger.info("Chat %d: transcribed voice (%ds) → %d chars",
                     message.chat.id, message.voice.duration, len(text))
    except Exception:
        logger.exception("Voice transcription failed")
        await message.answer("Failed to transcribe voice message.")
        return
    finally:
        os.unlink(tmp.name)

    override = f"[Voice message] {text}"
    try:
        await _handle_message_inner(message, override_text=override)
    except Exception:
        logger.exception("Unhandled exception in handle_voice")
        metrics.MESSAGES_TOTAL.labels(status="error").inc()
        _record_error(_scope_key_from_message(message))
        await message.answer("An internal error occurred while processing your voice message.")


@router.message(F.photo)
@router.message(F.document.mime_type.startswith("image/"))
async def handle_image(message: Message) -> None:
    """Extract text from image (OCR) and process as text message."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return

    import tempfile

    caption = (message.caption or "").strip()
    file_id: str | None = None

    if message.photo:
        file_id = message.photo[-1].file_id
    elif message.document and (message.document.mime_type or "").lower().startswith("image/"):
        file_id = message.document.file_id

    if not file_id:
        await message.answer("Image message was received, but no valid image file was found.")
        return

    file = await message.bot.get_file(file_id)
    suffix = Path(file.file_path or "").suffix or ".jpg"
    tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    ocr_text = ""
    ocr_error = False
    try:
        await message.bot.download_file(file.file_path, tmp.name)
        if ocr.is_available():
            try:
                ocr_text = await ocr.extract_text(tmp.name)
            except Exception:
                logger.exception("Image OCR failed")
                ocr_error = True
        else:
            ocr_error = True
    except Exception:
        logger.exception("Image download failed")
        await message.answer("Failed to download image for OCR.")
        return
    finally:
        os.unlink(tmp.name)

    parts = ["[Image message]"]
    if caption:
        parts.append(f"Caption: {caption}")
    if ocr_text:
        parts.append(f"OCR text:\n{ocr_text[:8000]}")
    elif ocr_error:
        parts.append(
            "OCR unavailable or failed on this host. "
            "Install Tesseract (`sudo apt-get install -y tesseract-ocr`) to enable OCR."
        )

    override = "\n\n".join(parts).strip()
    try:
        await _handle_message_inner(message, override_text=override)
    except Exception:
        logger.exception("Unhandled exception in handle_image")
        metrics.MESSAGES_TOTAL.labels(status="error").inc()
        _record_error(_scope_key_from_message(message))
        await message.answer("An internal error occurred while processing your image message.")


@router.message(F.text)
async def handle_message(message: Message) -> None:
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


@router.channel_post(F.text)
async def handle_channel_post(message: Message) -> None:
    await handle_message(message)


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
    state = _get_state(scope_key)

    if state.lock.locked():
        metrics.MESSAGES_TOTAL.labels(status="busy").inc()
        queued_text = (override_text or message.text or "").strip()
        if queued_text:
            queued_count = _queue_pending_input(state, queued_text)
            await message.answer(
                f"Working on your previous request. "
                f"Added this as extra context ({queued_count} queued) and will process it next."
            )
        else:
            await message.answer("Still processing your previous message, please wait...")
        return

    async with state.lock:
        # Reset cancellation state
        state.cancel_requested = False
        run_generation = state.reset_generation

        session = session_manager.get(chat_id, thread_id)
        progress = ProgressReporter(message)
        typing_task = asyncio.create_task(_keep_typing(message))
        await progress.show_working()

        final_response: bridge.ClaudeResponse | None = None

        try:
            provider = provider_manager.get_provider(scope_key)
            if provider.cli != "claude" and _find_provider_cli(provider.cli) is None:
                fallback = provider_manager.reset(scope_key)
                session_manager.set_provider(chat_id, fallback.name, thread_id)
                await message.answer(
                    f"Provider <b>{provider.name}</b> requires missing CLI "
                    f"<code>{provider.cli}</code>. Switched to <b>{fallback.name}</b>.",
                    parse_mode="HTML",
                )
                provider = fallback
            env = provider_manager.subprocess_env(provider)
            logger.info(
                "Chat %s: using provider '%s' (cli=%s) with env=%s",
                scope_key,
                provider.name,
                provider.cli,
                {k: v for k, v in env.items() if k.startswith("ANTHROPIC_")},
            )

            if provider.cli == "codex":
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
                    override_text=override_text,
                )
            else:
                final_response = await _run_claude(
                    message, state, session, progress, env,
                    override_text=override_text,
                )

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
                health_invariants.record_provider_result(success=False)
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
                    env = provider_manager.subprocess_env(next_provider)
                    if next_provider.cli == "codex":
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
                            override_text=override_text,
                        )
                    else:
                        final_response = await _run_claude(
                            message, state, session, progress, env,
                            override_text=override_text,
                        )

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
                    "Chat %s: second-pass tool activation requested: %s",
                    scope_key,
                    selected_tool,
                )
                await progress.report_tool("tool_selector", selected_tool)
                forced_prompt = _inject_tool_request(
                    override_text or message.text or "",
                    selected_tool,
                )
                if provider.cli == "codex":
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
                        override_text=forced_prompt,
                    )
                else:
                    retry_response = await _run_claude(
                        message,
                        state,
                        session,
                        progress,
                        env,
                        override_text=forced_prompt,
                    )
                if retry_response:
                    final_response = retry_response
        finally:
            typing_task.cancel()
            try:
                await typing_task
            except asyncio.CancelledError:
                pass

        # ── Send response ─────────────────────────────────────
        if state.cancel_requested:
            await progress.finish()
            _clear_errors(scope_key)
        elif final_response:
            if final_response.is_error:
                health_invariants.record_provider_result(success=False)
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
                await message.answer(error_text, reply_markup=reply_markup)
                await progress.finish()
            else:
                health_invariants.record_provider_result(success=True)
                clean_text, media_refs, audio_as_voice = _extract_media_directives(final_response.text or "")

                for media_ref in media_refs:
                    media_input = _resolve_media_input(media_ref)
                    try:
                        if _is_voice_compatible_media(media_ref):
                            await message.answer_voice(media_input)
                        elif audio_as_voice and _is_audio_media(media_ref):
                            await message.answer_voice(media_input)
                        elif _is_audio_media(media_ref):
                            await message.answer_audio(media_input)
                        elif _is_image_media(media_ref):
                            await message.answer_photo(media_input)
                        else:
                            await message.answer_document(media_input)
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
                        health_invariants.record_empty_response(is_empty=True)
                        logger.warning(
                            "Chat %d: Got empty response object - text='%s', is_error=%s, session_id=%s, cost=%.6f",
                            message.chat.id,
                            repr(final_response.text[:200]) if final_response.text else "None",
                            final_response.is_error,
                            final_response.session_id,
                            final_response.cost_usd,
                        )
                        chunks = ["(empty response)"]
                    else:
                        health_invariants.record_empty_response(is_empty=False)
                else:
                    health_invariants.record_empty_response(is_empty=False)

                for chunk in chunks:
                    if not chunk.strip():
                        continue
                    try:
                        await message.answer(chunk, parse_mode="HTML")
                    except Exception:
                        plain = strip_html(chunk)
                        for plain_chunk in split_message(plain):
                            if not plain_chunk.strip():
                                continue
                            await message.answer(plain_chunk)

                await progress.finish()
                _clear_errors(scope_key)
        else:
            _record_error(scope_key)
            reply_markup = _build_rollback_suggestion_markup(
                scope_key,
                message.from_user and message.from_user.id,
            )
            await message.answer(
                "An internal error occurred while processing your request.",
                reply_markup=reply_markup,
            )
            await progress.finish()

        # Update session ID if we got one back
        if (
            final_response
            and provider.cli != "codex"
            and not state.cancel_requested
            and state.reset_generation == run_generation
            and final_response.session_id
            and final_response.session_id != session.claude_session_id
        ):
            session_manager.update_session_id(chat_id, final_response.session_id, thread_id)
        if (
            final_response
            and provider.cli == "codex"
            and not state.cancel_requested
            and state.reset_generation == run_generation
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

    if state.cancel_requested:
        _drain_pending_inputs(state)
        return

    queued_inputs = _drain_pending_inputs(state)
    if queued_inputs:
        followup_prompt = _build_midflight_followup_prompt(queued_inputs)
        if followup_prompt:
            await _handle_message_inner(message, override_text=followup_prompt)


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
    thread_id = _thread_id(message)
    try:
        while True:
            try:
                if thread_id is not None:
                    await message.bot.send_chat_action(
                        chat_id=message.chat.id,
                        message_thread_id=thread_id,
                        action=ChatAction.TYPING,
                    )
                else:
                    await message.bot.send_chat_action(chat_id=message.chat.id, action=ChatAction.TYPING)
            except TelegramAPIError as e:
                logger.debug("Typing indicator failed (transient): %s", e)
                health_invariants.record_progress_channel_error()
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        return
