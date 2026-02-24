import asyncio
from dataclasses import dataclass
import logging

from aiogram import Router, F
from aiogram.types import Message
from aiogram.enums import ChatAction

from . import bridge, config
from .sessions import SessionManager
from .formatter import markdown_to_html, split_message, strip_html
from .progress import ProgressReporter
from . import metrics

logger = logging.getLogger(__name__)
router = Router()

session_manager = SessionManager()

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
    await message.answer(
        f"Hello! I'm a Claude Code assistant. <b>v{config.VERSION}</b>\n\n"
        "Send me any message and I'll respond using Claude.\n\n"
        "<b>Commands:</b>\n"
        "/new — Start a fresh conversation\n"
        "/model [sonnet|opus|haiku] — Switch model\n"
        "/status — Show current session info\n"
        "/cancel — Cancel current request",
        parse_mode="HTML",
    )


@router.message(F.text == "/new")
async def cmd_new(message: Message) -> None:
    if not _is_authorized(message.from_user and message.from_user.id):
        return
    session_manager.new_conversation(message.chat.id)
    await message.answer("Conversation cleared. Send a message to start fresh.")


@router.message(F.text.startswith("/model"))
async def cmd_model(message: Message) -> None:
    if not _is_authorized(message.from_user and message.from_user.id):
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        current = session_manager.get(message.chat.id).model
        await message.answer(
            f"Current model: <b>{current}</b>\n"
            f"Usage: /model [sonnet|opus|haiku]",
            parse_mode="HTML",
        )
        return
    model = parts[1].lower()
    if model not in VALID_MODELS:
        await message.answer(f"Invalid model. Choose from: {', '.join(sorted(VALID_MODELS))}")
        return
    session_manager.set_model(message.chat.id, model)
    await message.answer(f"Model switched to <b>{model}</b>.", parse_mode="HTML")


@router.message(F.text == "/status")
async def cmd_status(message: Message) -> None:
    if not _is_authorized(message.from_user and message.from_user.id):
        return
    session = session_manager.get(message.chat.id)
    sid = session.claude_session_id or "none (new conversation)"
    await message.answer(
        f"<b>Version:</b> {config.VERSION}\n"
        f"<b>Session:</b> <code>{sid}</code>\n"
        f"<b>Model:</b> {session.model}",
        parse_mode="HTML",
    )


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
        state.process_handle = {}

        session = session_manager.get(message.chat.id)

        # Create progress reporter
        progress = ProgressReporter(message)

        # Send typing indicator periodically
        typing_task = asyncio.create_task(_keep_typing(message))

        final_response: bridge.ClaudeResponse | None = None

        try:
            async for event in bridge.stream_message(
                prompt=message.text or "",
                session_id=session.claude_session_id,
                model=session.model,
                working_dir=config.CLAUDE_WORKING_DIR,
                process_handle=state.process_handle,
            ):
                if state.cancel_requested:
                    # User cancelled the request
                    await progress.show_cancelled()
                    final_response = bridge.ClaudeResponse(
                        text="Request cancelled.",
                        session_id=session.claude_session_id,
                        is_error=True,
                        cost_usd=0,
                        duration_ms=0,
                        num_turns=0,
                    )
                    break

                match event.event_type:
                    case bridge.StreamEventType.TOOL_USE:
                        if event.tool_name:
                            await progress.report_tool(event.tool_name, event.tool_input)
                    case bridge.StreamEventType.RESULT:
                        if event.response:
                            final_response = event.response
                        break

        finally:
            typing_task.cancel()
            try:
                await typing_task
            except asyncio.CancelledError:
                pass

        # Clean up progress
        if state.cancel_requested:
            await progress.finish()
        elif final_response:
            if final_response.is_error:
                await message.answer(final_response.text)
                await progress.finish()
            else:
                # Send response, then finish progress
                html = markdown_to_html(final_response.text)
                chunks = split_message(html)

                for chunk in chunks:
                    try:
                        await message.answer(chunk, parse_mode="HTML")
                    except Exception:
                        # Fallback: strip HTML and send as plain text
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
            await message.bot.send_chat_action(chat_id=message.chat.id, action=ChatAction.TYPING)
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        return