import asyncio
import html
import logging
from collections import deque
from typing import Optional

from aiogram.types import Message
from aiogram.exceptions import TelegramAPIError

from . import config

logger = logging.getLogger(__name__)

# Animated status messages for heartbeat
_HEARTBEAT_MESSAGES = [
    "🔄 Working...",
    "🔄 Still working...",
    "🔄 Continuing...",
    "🔄 Processing...",
    "🔄 Nearly there...",
    "🔄 Working...",
]
_HEARTBEAT_INTERVAL = 5.0  # Update every 5 seconds during heartbeat


class ProgressReporter:
    """Manages a single editable Telegram message showing Claude's current activity.

    Shows recent tool actions (Reading, Editing, Running, etc.) with debounced edits
    to avoid hitting Telegram rate limits. Also has a heartbeat animation to show
    activity when Claude is working on long-running tasks.
    """

    def __init__(self, message: Message, debounce_seconds: float | None = None):
        self._message = message
        self._chat_id = message.chat.id
        self._message_thread_id = getattr(message, "message_thread_id", None)
        self._bot = message.bot
        self._debounce_seconds = (
            config.PROGRESS_DEBOUNCE_SECONDS if debounce_seconds is None else debounce_seconds
        )

        self._progress_message_id: int | None = None
        self._history: deque[str] = deque(maxlen=5)  # Keep last ~5 actions
        self._last_update_text: str = ""
        self._dirty: bool = False
        self._task: asyncio.Task | None = None
        self._shutdown: bool = False
        self._heartbeat_task: asyncio.Task | None = None
        self._heartbeat_index: int = 0

    async def report_tool(self, tool_name: str, tool_input: str | None) -> None:
        """Report a tool action being performed.

        Args:
            tool_name: Name of the tool (e.g., "Bash", "Read", "Edit")
            tool_input: Primary argument (e.g., command, file_path, pattern)
        """
        # Stop heartbeat when we get new activity - it will restart after debounce
        self._stop_heartbeat()

        # Translate tool events to human-readable lines
        text = self._format_tool_action(tool_name, tool_input)

        # Skip if this is a duplicate of the most recent action
        if self._history and self._history[-1] == text:
            return

        self._history.append(text)
        self._dirty = True

        # Cancel any pending update task and start a new debounced one
        if self._task and not self._task.done():
            self._task.cancel()
        self._task = asyncio.create_task(self._debounced_update())

    async def show_working(self) -> None:
        """Show an initial working indicator before the first tool event arrives."""
        if self._progress_message_id is not None:
            return
        text = "🔄 <b>Working...</b>"
        try:
            msg = await self._bot.send_message(
                chat_id=self._chat_id,
                message_thread_id=self._message_thread_id,
                text=text,
                parse_mode="HTML",
            )
            self._progress_message_id = msg.message_id
            self._last_update_text = text
            self._start_heartbeat()
        except TelegramAPIError as e:
            logger.warning("Failed to send initial progress message: %s", e)

    def _format_tool_action(self, tool_name: str, tool_input: str | None) -> str:
        """Format a tool action into a human-readable line."""
        tool_name = tool_name.lower()
        match tool_name:
            case "bash":
                prefix = "Running"
            case "read":
                prefix = "Reading"
            case "edit":
                prefix = "Editing"
            case "write":
                prefix = "Writing"
            case "grep" | "glob":
                prefix = "Searching"
            case "task":
                prefix = "Delegating task"
            case "askuserquestion":
                prefix = "Waiting for input"
            case "skill":
                prefix = "Running skill"
            case "enterplanmode":
                prefix = "Planning"
            case "exitplanmode":
                prefix = "Approving plan"
            case _:
                prefix = f"Using {tool_name}"

        prefix = html.escape(prefix)
        if tool_input:
            safe_input = html.escape(tool_input, quote=False).replace("\n", " ")
            return f"{prefix}: {safe_input}"
        return f"{prefix}..."

    async def _debounced_update(self) -> None:
        """Debounced update of the progress message.

        Wait for the debounce period, then update if there are still uncommitted changes.
        Start heartbeat animation after updating.
        """
        try:
            await asyncio.sleep(self._debounce_seconds)

            if self._shutdown:
                return

            if not self._dirty:
                return

            # Build the message text
            if self._history:
                lines = list(self._history)
                text = f"🔄 <b>Working...</b>\n" + "\n".join(f"• {line}" for line in lines)
            else:
                text = "🔄 <b>Working...</b>"

            # Only update if text changed
            if text == self._last_update_text:
                self._dirty = False
                return

            self._last_update_text = text
            self._dirty = False

            if self._progress_message_id is None:
                # Send new message
                try:
                    msg = await self._bot.send_message(
                        chat_id=self._chat_id,
                        message_thread_id=self._message_thread_id,
                        text=text,
                        parse_mode="HTML",
                    )
                    self._progress_message_id = msg.message_id
                except TelegramAPIError as e:
                    logger.warning("Failed to send progress message: %s", e)
            else:
                # Edit existing message
                try:
                    await self._bot.edit_message_text(
                        chat_id=self._chat_id,
                        message_id=self._progress_message_id,
                        text=text,
                        parse_mode="HTML",
                    )
                except TelegramAPIError as e:
                    # MessageNotModified is harmless, other errors log warning
                    if "message is not modified" not in str(e).lower():
                        logger.warning("Failed to update progress message: %s", e)

            # Start heartbeat animation after the update
            self._start_heartbeat()

        except asyncio.CancelledError:
            # Task was cancelled by a newer one
            pass

    def _start_heartbeat(self) -> None:
        """Start the heartbeat animation task."""
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_animate())

    def _stop_heartbeat(self) -> None:
        """Stop the heartbeat animation task."""
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
        self._heartbeat_task = None
        self._heartbeat_index = 0

    async def _heartbeat_animate(self) -> None:
        """Animate the progress message to show ongoing activity.

        Rotates through status messages to indicate Claude is still working
        even without new tool events.
        """
        try:
            while True and not self._shutdown:
                await asyncio.sleep(_HEARTBEAT_INTERVAL)

                if self._shutdown:
                    return

                if self._progress_message_id is None:
                    return

                # Rotate to next heartbeat message
                self._heartbeat_index = (self._heartbeat_index + 1) % len(_HEARTBEAT_MESSAGES)
                status = _HEARTBEAT_MESSAGES[self._heartbeat_index]

                # Build updated text
                if self._history:
                    lines = list(self._history)
                    text = f"{status}\n" + "\n".join(f"• {line}" for line in lines)
                else:
                    text = status

                # Only update if text changed
                if text == self._last_update_text:
                    continue

                try:
                    await self._bot.edit_message_text(
                        chat_id=self._chat_id,
                        message_id=self._progress_message_id,
                        text=text,
                        parse_mode="HTML",
                    )
                    self._last_update_text = text
                except TelegramAPIError as e:
                    # Ignore errors - message might have been deleted
                    if "message is not modified" not in str(e).lower():
                        logger.debug("Heartbeat update failed (likely deleted): %s", e)
                    return

        except asyncio.CancelledError:
            pass

    async def finish(self) -> None:
        """Clean up the progress message after final response is sent.

        Deletes the progress message if it exists.
        """
        self._shutdown = True
        self._stop_heartbeat()

        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        if self._progress_message_id is not None:
            try:
                await self._bot.delete_message(
                    chat_id=self._chat_id,
                    message_id=self._progress_message_id,
                )
            except TelegramAPIError as e:
                # Message might have been deleted already or doesn't exist
                logger.debug("Could not delete progress message: %s", e)

    async def show_cancelled(self) -> None:
        """Update the progress message to show cancellation before deletion."""
        self._shutdown = True
        self._stop_heartbeat()

        if self._progress_message_id is not None:
            try:
                await self._bot.edit_message_text(
                    chat_id=self._chat_id,
                    message_id=self._progress_message_id,
                    text="❌ <b>Request cancelled</b>",
                    parse_mode="HTML",
                )
                # Small delay so the user sees the cancellation message
                await asyncio.sleep(1)
            except TelegramAPIError:
                pass

    async def show_idle_timeout(self) -> None:
        """Update the progress message to show idle timeout before deletion."""
        self._shutdown = True
        self._stop_heartbeat()

        if self._progress_message_id is not None:
            try:
                await self._bot.edit_message_text(
                    chat_id=self._chat_id,
                    message_id=self._progress_message_id,
                    text="⏱️ <b>Timed out</b> — Claude stopped producing output",
                    parse_mode="HTML",
                )
                await asyncio.sleep(1)
            except TelegramAPIError:
                pass
