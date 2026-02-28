import asyncio
from dataclasses import dataclass
import html
import inspect
import json
import logging
import os
import re
import shutil
import subprocess
from datetime import datetime, timezone as tz
from pathlib import Path
from urllib.parse import urlparse

import yaml
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, ErrorEvent, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ChatAction
from aiogram.exceptions import TelegramAPIError

from . import bridge, config, metrics, transcribe
from .core.context_plugins import ContextPluginRegistry
from .sessions import SessionManager
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
tool_registry = ToolRegistry(config.TOOLS_DIR)
context_plugins = ContextPluginRegistry([tool_registry])
self_mod_manager = SelfModificationManager(Path(__file__).resolve().parent.parent)
task_manager: TaskManager | None = None  # Set in main()
schedule_manager: ScheduleManager | None = None  # Set in main()

# Restore persisted provider selections from sessions
for _chat_id, _session in session_manager.sessions.items():
    if _session.provider:
        provider_manager.set_provider(_chat_id, _session.provider)

CLAUDE_MODELS = {"sonnet", "opus", "haiku"}
VALID_MODELS = CLAUDE_MODELS


@dataclass
class _ChatState:
    """State for each active chat."""
    lock: asyncio.Lock
    process_handle: dict | None  # Will contain {"proc": proc} when running
    cancel_requested: bool


# Per-chat state dict
_chat_states: dict[int, _ChatState] = {}
_error_counts: dict[int, int] = {}
_CODEX_TRANSIENT_ERROR_PATTERNS = (
    re.compile(r"stream disconnected before completion", re.IGNORECASE),
    re.compile(r"transport error:\s*timeout", re.IGNORECASE),
    re.compile(r"\breconnecting\.\.\.\s*\d+/\d+", re.IGNORECASE),
    re.compile(r"\b(etimedout|econnreset|connection reset)\b", re.IGNORECASE),
)
_AUDIO_AS_VOICE_TAG_RE = re.compile(r"\[\[\s*audio_as_voice\s*\]\]", re.IGNORECASE)
_MEDIA_LINE_RE = re.compile(r"^\s*MEDIA:\s*(.+?)\s*$", re.IGNORECASE)
_VOICE_COMPATIBLE_EXTENSIONS = {".ogg", ".opus", ".mp3", ".m4a"}
_AUDIO_EXTENSIONS = _VOICE_COMPATIBLE_EXTENSIONS | {".wav", ".aac", ".flac"}


def _get_state(chat_id: int) -> _ChatState:
    """Get or create state for a chat."""
    if chat_id not in _chat_states:
        _chat_states[chat_id] = _ChatState(lock=asyncio.Lock(), process_handle=None, cancel_requested=False)
    return _chat_states[chat_id]


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


def _record_error(chat_id: int) -> int:
    count = _error_counts.get(chat_id, 0) + 1
    _error_counts[chat_id] = count
    return count


def _clear_errors(chat_id: int) -> None:
    _error_counts.pop(chat_id, None)


def _should_suggest_rollback(chat_id: int) -> bool:
    return _error_counts.get(chat_id, 0) >= 3


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _build_rollback_suggestion_markup(chat_id: int, user_id: int | None):
    if not _is_admin(user_id) or not _should_suggest_rollback(chat_id):
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


async def _show_rollback_options(chat_id: int, bot) -> None:
    try:
        commits = await asyncio.to_thread(_get_recent_commits, 10)
    except Exception as e:
        await bot.send_message(chat_id, f"Failed to load commit history: {e}")
        return

    if not commits:
        await bot.send_message(chat_id, "No commits found for rollback.")
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
        reply_markup=kb.as_markup(),
    )


async def _restart_service(chat_id: int, bot) -> None:
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
        await bot.send_message(chat_id, f"Rollback completed, but restart failed: {err[:500]}")


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


def _current_provider(chat_id: int):
    return provider_manager.get_provider(chat_id)


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


@router.message(F.text == "/new")
async def cmd_new(message: Message) -> None:
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    session = session_manager.get(message.chat.id)
    if session.claude_session_id and os.getenv("DISABLE_REFLECTION") != "1":
        asyncio.create_task(_reflect(message.chat.id, session))
    session_manager.new_conversation(message.chat.id)
    session_manager.new_codex_conversation(message.chat.id)
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
    session = session_manager.get(message.chat.id)
    provider = _current_provider(message.chat.id)
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
            session_manager.set_codex_model(message.chat.id, chosen)
        else:
            session_manager.set_model(message.chat.id, requested)

        current = _current_model_label(session_manager.get(message.chat.id), provider)
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
    if not _is_authorized(callback.from_user and callback.from_user.id):
        return

    chat_id = callback.message.chat.id
    model = callback.data.split(":", 1)[1]
    logger.info("Chat %d: model selection 'model:%s'", chat_id, model)

    provider = _current_provider(chat_id)
    options = _model_options(provider)
    if model not in options:
        await callback.answer("Invalid model", show_alert=True)
        return

    if provider.cli == "codex":
        chosen = None if model == "default" else model
        session_manager.set_codex_model(chat_id, chosen)
    else:
        session_manager.set_model(chat_id, model)

    # Update keyboard state
    current = _current_model_label(session_manager.get(chat_id), provider)
    lines = [f"<b>Current model:</b> {current}\n"]
    lines.append("<b>Select a model:</b>")

    keyboard = InlineKeyboardBuilder()
    for m in options:
        button_text = f"{'✓ ' if m == current else ''}{m}"
        keyboard.button(text=button_text, callback_data=f"model:{m}")
    keyboard.adjust(2)  # 2 buttons per row

    await callback.message.edit_text("\n".join(lines), reply_markup=keyboard.as_markup(), parse_mode="HTML")
    await callback.answer(f"Switched to {current}")


@router.message(F.text == "/provider")
async def cmd_provider(message: Message) -> None:
    """Show provider selection keyboard."""
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return

    current = provider_manager.get_provider(message.chat.id)

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
    if not _is_authorized(callback.from_user and callback.from_user.id):
        return

    chat_id = callback.message.chat.id
    name = callback.data.split(":", 1)[1]
    logger.info("Chat %d: provider selection 'provider:%s'", chat_id, name)

    provider = provider_manager.set_provider(chat_id, name)
    if not provider:
        await callback.answer("Provider not found", show_alert=True)
        return

    # Persist provider to session
    session_manager.set_provider(chat_id, provider.name)

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
    session = session_manager.get(message.chat.id)
    provider = provider_manager.get_provider(message.chat.id)
    if provider.cli == "codex":
        sid = session.codex_session_id or "none (new conversation)"
    else:
        sid = session.claude_session_id or "none (new conversation)"
    current_model = _current_model_label(session, provider)
    await message.answer(
        f"<b>Version:</b> {config.VERSION}\n"
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

    state = _get_state(message.chat.id)

    if not state.lock.locked() or not state.process_handle or not state.process_handle.get("proc"):
        await message.answer("Nothing to cancel.")
        return

    # Kill the process
    proc = state.process_handle["proc"]
    kill_result = proc.kill()
    if inspect.isawaitable(kill_result):
        await kill_result
    state.cancel_requested = True
    session = session_manager.get(message.chat.id)
    provider = _current_provider(message.chat.id)
    metrics.CLAUDE_REQUESTS_TOTAL.labels(
        model=_current_model_label(session, provider),
        status="cancelled",
    ).inc()


@router.message(F.text == "/rollback")
async def cmd_rollback(message: Message) -> None:
    if not _is_admin(message.from_user and message.from_user.id):
        await message.answer("This command is admin-only.")
        return
    await _show_rollback_options(message.chat.id, message.bot)


@router.callback_query(F.data == "rollback_auto")
async def cb_rollback_auto(callback: CallbackQuery) -> None:
    if not _is_admin(callback.from_user and callback.from_user.id):
        await callback.answer("Admin only", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    await callback.answer()
    await _show_rollback_options(callback.message.chat.id, callback.bot)


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

    _clear_errors(callback.message.chat.id)
    await callback.message.answer(
        f"Rollback complete: <code>{short_hash}</code>\nRestarting <code>telegram-bot.service</code>...",
        parse_mode="HTML",
    )
    asyncio.create_task(_restart_service(callback.message.chat.id, callback.bot))


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

    session = session_manager.get(message.chat.id)

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
        chat_id=message.chat.id,
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

    tasks = task_manager.list_user_tasks(message.chat.id)

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
    if not task or task.chat_id != message.chat.id:
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

    session = session_manager.get(message.chat.id)
    memory_context = _as_text(memory_manager.build_context(task_text))
    tool_context = _as_text(context_plugins.build_context(task_text))
    memory_instructions = _as_text(memory_manager.build_instructions())
    prompt_parts = []
    if memory_context:
        prompt_parts.append(memory_context)
    if tool_context:
        prompt_parts.append(tool_context)
    prompt_parts.append(task_text + memory_instructions)
    full_prompt = "\n\n".join(prompt_parts)

    schedule_id = await schedule_manager.create_every(
        chat_id=message.chat.id,
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

    schedules = await schedule_manager.list_for_chat(message.chat.id)
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
    session = session_manager.get(message.chat.id)
    memory_context = _as_text(memory_manager.build_context(task_text))
    tool_context = _as_text(context_plugins.build_context(task_text))
    memory_instructions = _as_text(memory_manager.build_instructions())
    prompt_parts = []
    if memory_context:
        prompt_parts.append(memory_context)
    if tool_context:
        prompt_parts.append(tool_context)
    prompt_parts.append(task_text + memory_instructions)
    full_prompt = "\n\n".join(prompt_parts)

    try:
        schedule_id = await schedule_manager.create_weekly(
            chat_id=message.chat.id,
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

    session = session_manager.get(message.chat.id)
    memory_context = _as_text(memory_manager.build_context(task_text))
    tool_context = _as_text(context_plugins.build_context(task_text))
    memory_instructions = _as_text(memory_manager.build_instructions())
    prompt_parts = []
    if memory_context:
        prompt_parts.append(memory_context)
    if tool_context:
        prompt_parts.append(tool_context)
    prompt_parts.append(task_text + memory_instructions)
    full_prompt = "\n\n".join(prompt_parts)

    try:
        schedule_id = await schedule_manager.create_daily(
            chat_id=message.chat.id,
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

    schedules = await schedule_manager.list_for_chat(message.chat.id)
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

    # Build memory and tool-augmented prompt
    raw_prompt = override_text or message.text or ""
    memory_context = memory_manager.build_context(raw_prompt)
    tool_context = context_plugins.build_context(raw_prompt)
    memory_instructions = memory_manager.build_instructions()

    # Assemble prompt with all context layers
    prompt_parts = []
    if memory_context:
        prompt_parts.append(memory_context)
    if tool_context:
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

    # Build memory and tool-augmented prompt
    raw_prompt = override_text or message.text or ""
    memory_context = memory_manager.build_context(raw_prompt)
    tool_context = context_plugins.build_context(raw_prompt)
    memory_instructions = memory_manager.build_instructions()

    prompt_parts = []
    if memory_context:
        prompt_parts.append(memory_context)
    if tool_context:
        prompt_parts.append(tool_context)
    prompt_parts.append(raw_prompt + memory_instructions)

    prompt = "\n\n".join(prompt_parts)

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
        _record_error(message.chat.id)
        await message.answer("An internal error occurred while processing your voice message.")


@router.message(F.text)
async def handle_message(message: Message) -> None:
    try:
        await _handle_message_inner(message)
    except Exception:
        logger.exception("Unhandled exception in handle_message")
        metrics.MESSAGES_TOTAL.labels(status="error").inc()
        _record_error(message.chat.id)
        reply_markup = _build_rollback_suggestion_markup(
            message.chat.id,
            message.from_user and message.from_user.id,
        )
        await message.answer(
            "An internal error occurred while processing your request.",
            reply_markup=reply_markup,
        )


@router.channel_post(F.text)
async def handle_channel_post(message: Message) -> None:
    await handle_message(message)


async def _handle_message_inner(message: Message, override_text: str | None = None) -> None:
    if not _is_authorized(message.from_user and message.from_user.id, message.chat.id):
        metrics.MESSAGES_TOTAL.labels(status="unauthorized").inc()
        return

    state = _get_state(message.chat.id)

    if state.lock.locked():
        metrics.MESSAGES_TOTAL.labels(status="busy").inc()
        await message.answer("Still processing your previous message, please wait...")
        return

    async with state.lock:
        # Reset cancellation state
        state.cancel_requested = False

        session = session_manager.get(message.chat.id)
        progress = ProgressReporter(message)
        typing_task = asyncio.create_task(_keep_typing(message))

        final_response: bridge.ClaudeResponse | None = None

        try:
            provider = provider_manager.get_provider(message.chat.id)
            if provider.cli != "claude" and _find_provider_cli(provider.cli) is None:
                fallback = provider_manager.reset(message.chat.id)
                session_manager.set_provider(message.chat.id, fallback.name)
                await message.answer(
                    f"Provider <b>{provider.name}</b> requires missing CLI "
                    f"<code>{provider.cli}</code>. Switched to <b>{fallback.name}</b>.",
                    parse_mode="HTML",
                )
                provider = fallback
            env = provider_manager.subprocess_env(provider)
            logger.info(
                "Chat %d: using provider '%s' (cli=%s) with env=%s",
                message.chat.id,
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

            # ── Fallback on rate-limit ────────────────────────────
            if (
                final_response
                and final_response.is_error
                and not state.cancel_requested
                and provider_manager.is_rate_limit_error(final_response.text)
            ):
                next_provider = provider_manager.advance(message.chat.id)
                if next_provider:
                    await message.answer(
                        f"Rate limited on <b>{provider.name}</b>. "
                        f"Switching to <b>{next_provider.name}</b>...",
                        parse_mode="HTML",
                    )
                    logger.info(
                        "Chat %d: rate limit on '%s', retrying with '%s'",
                        message.chat.id, provider.name, next_provider.name,
                    )
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
        finally:
            typing_task.cancel()
            try:
                await typing_task
            except asyncio.CancelledError:
                pass

        # ── Send response ─────────────────────────────────────
        if state.cancel_requested:
            await progress.finish()
            _clear_errors(message.chat.id)
        elif final_response:
            if final_response.is_error:
                error_text = final_response.text or "(No response)"
                _record_error(message.chat.id)
                reply_markup = _build_rollback_suggestion_markup(
                    message.chat.id,
                    message.from_user and message.from_user.id,
                )
                await message.answer(error_text, reply_markup=reply_markup)
                await progress.finish()
            else:
                clean_text, media_refs, audio_as_voice = _extract_media_directives(final_response.text or "")

                for media_ref in media_refs:
                    media_input = _resolve_media_input(media_ref)
                    try:
                        if audio_as_voice and _is_voice_compatible_media(media_ref):
                            await message.answer_voice(media_input)
                        elif _is_audio_media(media_ref):
                            await message.answer_audio(media_input)
                        else:
                            await message.answer_document(media_input)
                    except Exception:
                        logger.exception(
                            "Chat %d: failed to send media '%s'",
                            message.chat.id,
                            media_ref,
                        )

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
                        chunks = ["(empty response)"]

                for chunk in chunks:
                    try:
                        await message.answer(chunk, parse_mode="HTML")
                    except Exception:
                        plain = strip_html(chunk)
                        for plain_chunk in split_message(plain):
                            await message.answer(plain_chunk)

                await progress.finish()
                _clear_errors(message.chat.id)
        else:
            _record_error(message.chat.id)
            reply_markup = _build_rollback_suggestion_markup(
                message.chat.id,
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
            and final_response.session_id
            and final_response.session_id != session.claude_session_id
        ):
            session_manager.update_session_id(message.chat.id, final_response.session_id)
        if (
            final_response
            and provider.cli == "codex"
            and final_response.session_id
            and final_response.session_id != session.codex_session_id
        ):
            session_manager.update_codex_session_id(message.chat.id, final_response.session_id)

        # Track metrics
        if final_response:
            status = "error" if final_response.is_error else "success"
            if state.cancel_requested:
                status = "cancelled"
            metrics.MESSAGES_TOTAL.labels(status=status).inc()


@router.errors()
async def on_router_error(event: ErrorEvent) -> bool:
    logger.exception("Unhandled router error: %s", event.exception)

    update = event.update
    message = getattr(update, "message", None)
    callback = getattr(update, "callback_query", None)

    if message:
        chat_id = message.chat.id
        user_id = message.from_user and message.from_user.id
        _record_error(chat_id)
        reply_markup = _build_rollback_suggestion_markup(chat_id, user_id)
        await message.answer(
            "An internal error occurred while processing your request.",
            reply_markup=reply_markup,
        )
    elif callback and callback.message:
        chat_id = callback.message.chat.id
        user_id = callback.from_user and callback.from_user.id
        _record_error(chat_id)
        reply_markup = _build_rollback_suggestion_markup(chat_id, user_id)
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
    try:
        while True:
            try:
                await message.bot.send_chat_action(chat_id=message.chat.id, action=ChatAction.TYPING)
            except TelegramAPIError as e:
                logger.debug("Typing indicator failed (transient): %s", e)
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        return
