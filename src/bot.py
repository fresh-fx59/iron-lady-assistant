import asyncio
import logging

from aiogram import Router, F
from aiogram.types import Message
from aiogram.enums import ChatAction

from . import bridge, config
from .sessions import SessionManager
from .formatter import markdown_to_html, split_message, strip_html
from . import metrics

logger = logging.getLogger(__name__)
router = Router()

session_manager = SessionManager()

# Per-chat locks to prevent overlapping Claude invocations
_chat_locks: dict[int, asyncio.Lock] = {}

VALID_MODELS = {"sonnet", "opus", "haiku"}


def _get_lock(chat_id: int) -> asyncio.Lock:
    if chat_id not in _chat_locks:
        _chat_locks[chat_id] = asyncio.Lock()
    return _chat_locks[chat_id]


def _is_authorized(user_id: int | None) -> bool:
    if not config.ALLOWED_USER_IDS:
        return False
    return user_id in config.ALLOWED_USER_IDS


@router.message(F.text == "/start")
async def cmd_start(message: Message) -> None:
    if not _is_authorized(message.from_user and message.from_user.id):
        return
    await message.answer(
        "Hello! I'm a Claude Code assistant.\n\n"
        "Send me any message and I'll respond using Claude.\n\n"
        "<b>Commands:</b>\n"
        "/new — Start a fresh conversation\n"
        "/model [sonnet|opus|haiku] — Switch model\n"
        "/status — Show current session info",
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
        f"<b>Session:</b> <code>{sid}</code>\n"
        f"<b>Model:</b> {session.model}",
        parse_mode="HTML",
    )


@router.message(F.text)
async def handle_message(message: Message) -> None:
    if not _is_authorized(message.from_user and message.from_user.id):
        metrics.MESSAGES_TOTAL.labels(status="unauthorized").inc()
        return

    lock = _get_lock(message.chat.id)
    if lock.locked():
        metrics.MESSAGES_TOTAL.labels(status="busy").inc()
        await message.answer("Still processing your previous message, please wait...")
        return

    async with lock:
        session = session_manager.get(message.chat.id)

        # Send typing indicator periodically
        typing_task = asyncio.create_task(_keep_typing(message))

        try:
            response = await bridge.send_message(
                prompt=message.text or "",
                session_id=session.claude_session_id,
                model=session.model,
                working_dir=config.CLAUDE_WORKING_DIR,
                timeout=config.MAX_RESPONSE_TIMEOUT,
            )
        finally:
            typing_task.cancel()
            try:
                await typing_task
            except asyncio.CancelledError:
                pass

        # Update session ID if we got one back
        if response.session_id and response.session_id != session.claude_session_id:
            session_manager.update_session_id(message.chat.id, response.session_id)

        # Track metrics
        status = "error" if response.is_error else "success"
        metrics.MESSAGES_TOTAL.labels(status=status).inc()

        # Format and send response
        if response.is_error:
            await message.answer(response.text)
            return

        html = markdown_to_html(response.text)
        chunks = split_message(html)

        for chunk in chunks:
            try:
                await message.answer(chunk, parse_mode="HTML")
            except Exception:
                # Fallback: strip HTML and send as plain text
                plain = strip_html(chunk)
                for plain_chunk in split_message(plain):
                    await message.answer(plain_chunk)


async def _keep_typing(message: Message) -> None:
    """Send typing indicator every 5 seconds."""
    try:
        while True:
            await message.answer_chat_action(ChatAction.TYPING)
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        return
