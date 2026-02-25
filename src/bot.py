import asyncio
from dataclasses import dataclass
import json
import logging
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

logger = logging.getLogger(__name__)
router = Router()

session_manager = SessionManager()
provider_manager = ProviderManager()
memory_manager = MemoryManager(config.MEMORY_DIR)
tool_registry = ToolRegistry(config.TOOLS_DIR)

# Restore persisted provider selections from sessions
for _chat_id, _session in session_manager.sessions.items():
    if _session.provider:
        provider_manager.set_provider(_chat_id, _session.provider)

VALID_MODELS = {"sonnet", "opus", "haiku"}


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
    if not config.ALLOWED_USER_IDS:
        return False
    return user_id in config.ALLOWED_USER_IDS


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
        "/cancel — Cancel current request",
    ])

    await message.answer("\n".join(status_lines), parse_mode="HTML")


@router.message(F.text == "/new")
async def cmd_new(message: Message) -> None:
    if not _is_authorized(message.from_user and message.from_user.id):
        return
    session = session_manager.get(message.chat.id)
    if session.claude_session_id:
        asyncio.create_task(_reflect(message.chat.id, session))
    session_manager.new_conversation(message.chat.id)
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


@router.message(F.text == "/model")
async def cmd_model(message: Message) -> None:
    """Show model selection keyboard."""
    if not _is_authorized(message.from_user and message.from_user.id):
        return

    current = session_manager.get(message.chat.id).model

    lines = [f"<b>Current model:</b> {current}\n"]
    lines.append("<b>Select a model:</b>")

    # Build inline keyboard with buttons
    keyboard = InlineKeyboardBuilder()
    for model in sorted(VALID_MODELS):
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

    if model not in VALID_MODELS:
        await callback.answer("Invalid model", show_alert=True)
        return

    session_manager.set_model(chat_id, model)

    # Update keyboard state
    lines = [f"<b>Current model:</b> {model}\n"]
    lines.append("<b>Select a model:</b>")

    keyboard = InlineKeyboardBuilder()
    for m in sorted(VALID_MODELS):
        button_text = f"{'✓ ' if m == model else ''}{m}"
        keyboard.button(text=button_text, callback_data=f"model:{m}")
    keyboard.adjust(2)  # 2 buttons per row

    await callback.message.edit_text("\n".join(lines), reply_markup=keyboard.as_markup(), parse_mode="HTML")
    await callback.answer(f"Switched to {model}")


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
    sid = session.claude_session_id or "none (new conversation)"
    provider = provider_manager.get_provider(message.chat.id)
    await message.answer(
        f"<b>Version:</b> {config.VERSION}\n"
        f"<b>Session:</b> <code>{sid}</code>\n"
        f"<b>Model:</b> {session.model}\n"
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
    metrics.CLAUDE_REQUESTS_TOTAL.labels(model=session_manager.get(message.chat.id).model, status="cancelled").inc()


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
            env = provider_manager.subprocess_env(provider)
            logger.info(
                "Chat %d: using provider '%s' with env=%s",
                message.chat.id,
                provider.name,
                {k: v for k, v in env.items() if k.startswith("ANTHROPIC_")},
            )

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
        if final_response and final_response.session_id and final_response.session_id != session.claude_session_id:
            session_manager.update_session_id(message.chat.id, final_response.session_id)

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