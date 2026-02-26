import asyncio
from dataclasses import dataclass
import json
import logging
import os
import shutil
from datetime import datetime, timezone as tz
from pathlib import Path

import yaml
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ChatAction
from aiogram.exceptions import TelegramAPIError

from . import bridge, config, metrics
from .sessions import SessionManager
from .formatter import markdown_to_html, split_message, strip_html
from .memory import MemoryManager
from .progress import ProgressReporter
from .providers import ProviderManager
from .tools import ToolRegistry
from .tasks import TaskManager, TaskStatus

logger = logging.getLogger(__name__)
router = Router()

session_manager = SessionManager()
provider_manager = ProviderManager()
memory_manager = MemoryManager(config.MEMORY_DIR)
tool_registry = ToolRegistry(config.TOOLS_DIR)
task_manager: TaskManager | None = None  # Set in main()

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


def _get_state(chat_id: int) -> _ChatState:
    """Get or create state for a chat."""
    if chat_id not in _chat_states:
        _chat_states[chat_id] = _ChatState(lock=asyncio.Lock(), process_handle=None, cancel_requested=False)
    return _chat_states[chat_id]


def _is_authorized(user_id: int | None) -> bool:
    raw_ids = os.getenv("ALLOWED_USER_IDS", "")
    allowed = set(config.ALLOWED_USER_IDS)
    if raw_ids:
        parsed = {int(uid.strip()) for uid in raw_ids.split(",") if uid.strip()}
        if parsed:
            allowed |= parsed
            config.ALLOWED_USER_IDS = allowed
    if not allowed:
        return False
    return user_id in allowed


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


@router.message(F.text == "/start")
async def cmd_start(message: Message) -> None:
    if not _is_authorized(message.from_user and message.from_user.id):
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
        "/bg <task> — Run task in background",
        "/bg_cancel <id> — Cancel background task",
        "/cancel — Cancel current request",
    ])

    await message.answer("\n".join(status_lines), parse_mode="HTML")


@router.message(F.text == "/new")
async def cmd_new(message: Message) -> None:
    if not _is_authorized(message.from_user and message.from_user.id):
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
    if not _is_authorized(message.from_user and message.from_user.id):
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
    if not _is_authorized(message.from_user and message.from_user.id):
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
    if not _is_authorized(message.from_user and message.from_user.id):
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
    if not _is_authorized(message.from_user and message.from_user.id):
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
    if not _is_authorized(message.from_user and message.from_user.id):
        return
    content = tool_registry.format_for_display()
    try:
        await message.answer(content, parse_mode="HTML")
    except Exception:
        await message.answer(strip_html(content))


@router.message(F.text == "/cancel")
async def cmd_cancel(message: Message) -> None:
    """Cancel the current request if one is running."""
    if not _is_authorized(message.from_user and message.from_user.id):
        return

    state = _get_state(message.chat.id)

    if not state.lock.locked() or not state.process_handle or not state.process_handle.get("proc"):
        await message.answer("Nothing to cancel.")
        return

    # Kill the process
    proc = state.process_handle["proc"]
    proc.kill()
    state.cancel_requested = True
    session = session_manager.get(message.chat.id)
    provider = _current_provider(message.chat.id)
    metrics.CLAUDE_REQUESTS_TOTAL.labels(
        model=_current_model_label(session, provider),
        status="cancelled",
    ).inc()


@router.message(F.text.startswith("/bg "))
async def cmd_bg(message: Message) -> None:
    """Run a task in the background."""
    if not _is_authorized(message.from_user and message.from_user.id):
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
    tool_context = tool_registry.build_context(prompt)
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
        user_id=message.from_user.id,
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
    if not _is_authorized(message.from_user and message.from_user.id):
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
    if not _is_authorized(message.from_user and message.from_user.id):
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


async def _run_claude(
    message: Message,
    state: _ChatState,
    session: object,
    progress: ProgressReporter,
    subprocess_env: dict[str, str] | None = None,
) -> bridge.ClaudeResponse | None:
    """Run a single Claude subprocess attempt. Returns the response or None."""
    state.process_handle = {}

    # Build memory and tool-augmented prompt
    raw_prompt = message.text or ""
    memory_context = memory_manager.build_context(raw_prompt)
    tool_context = tool_registry.build_context(raw_prompt)
    memory_instructions = memory_manager.build_instructions()

    # Assemble prompt with all context layers
    prompt_parts = []
    if memory_context:
        prompt_parts.append(memory_context)
    if tool_context:
        prompt_parts.append(tool_context)
    prompt_parts.append(raw_prompt + memory_instructions)

    prompt = "\n\n".join(prompt_parts)

    async for event in bridge.stream_message(
        prompt=prompt,
        session_id=session.claude_session_id,
        model=session.model,
        working_dir=config.CLAUDE_WORKING_DIR,
        process_handle=state.process_handle,
        subprocess_env=subprocess_env,
    ):
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
) -> bridge.ClaudeResponse | None:
    """Run a single Codex CLI subprocess attempt. Returns the response or None."""
    state.process_handle = {}

    # Build memory and tool-augmented prompt
    raw_prompt = message.text or ""
    memory_context = memory_manager.build_context(raw_prompt)
    tool_context = tool_registry.build_context(raw_prompt)
    memory_instructions = memory_manager.build_instructions()

    prompt_parts = []
    if memory_context:
        prompt_parts.append(memory_context)
    if tool_context:
        prompt_parts.append(tool_context)
    prompt_parts.append(raw_prompt + memory_instructions)

    prompt = "\n\n".join(prompt_parts)

    async for event in bridge.stream_codex_message(
        prompt=prompt,
        session_id=session_id,
        model=model,
        resume_arg=resume_arg,
        working_dir=config.CLAUDE_WORKING_DIR,
        process_handle=state.process_handle,
        subprocess_env=subprocess_env,
    ):
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

    return None


@router.message(F.text)
async def handle_message(message: Message) -> None:
    if not _is_authorized(message.from_user and message.from_user.id):
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
                final_response = await _run_codex(
                    message,
                    state,
                    session,
                    progress,
                    codex_model,
                    session.codex_session_id,
                    provider.resume_arg,
                    env,
                )
            else:
                final_response = await _run_claude(message, state, session, progress, env)

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
                        final_response = await _run_codex(
                            message,
                            state,
                            session,
                            progress,
                            codex_model,
                            session.codex_session_id,
                            next_provider.resume_arg,
                            env,
                        )
                    else:
                        final_response = await _run_claude(
                            message, state, session, progress, env,
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
        elif final_response:
            if final_response.is_error:
                error_text = final_response.text or "(No response)"
                await message.answer(error_text)
                await progress.finish()
            else:
                html = markdown_to_html(final_response.text)
                chunks = split_message(html)

                if not chunks:
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
