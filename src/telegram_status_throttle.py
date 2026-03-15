import asyncio
from dataclasses import dataclass
from time import monotonic
from typing import Awaitable, Callable, TypeVar

from . import config

T = TypeVar("T")


@dataclass
class _ChatStatusWindow:
    lock: asyncio.Lock
    next_ephemeral_send_at: float = 0.0


_chat_status_windows: dict[int, _ChatStatusWindow] = {}
_chat_status_windows_guard = asyncio.Lock()


class EphemeralStatusSuppressedError(Exception):
    """Raised when a transient Telegram status message is skipped by cooldown."""


async def send_ephemeral_status(
    chat_id: int,
    sender: Callable[[], Awaitable[T]],
    *,
    minimum_interval_seconds: float | None = None,
) -> T:
    """Serialize transient status sends per chat and enforce a cooldown after success."""
    window = await _window_for_chat(chat_id)
    interval = (
        config.TELEGRAM_STATUS_MESSAGE_COOLDOWN_SECONDS
        if minimum_interval_seconds is None
        else max(0.0, minimum_interval_seconds)
    )

    async with window.lock:
        now = monotonic()
        if now < window.next_ephemeral_send_at:
            raise EphemeralStatusSuppressedError()

        try:
            result = await sender()
        except Exception as exc:
            retry_after = getattr(exc, "retry_after", None)
            if retry_after is not None:
                window.next_ephemeral_send_at = max(now, monotonic()) + max(0.0, float(retry_after))
            else:
                window.next_ephemeral_send_at = min(window.next_ephemeral_send_at, now)
            raise

        window.next_ephemeral_send_at = monotonic() + interval
        return result


async def allow_ephemeral_status_send(
    chat_id: int,
    *,
    minimum_interval_seconds: float | None = None,
) -> bool:
    """Return whether a new transient status message may be sent for this chat now."""
    window = await _window_for_chat(chat_id)
    interval = (
        config.TELEGRAM_STATUS_MESSAGE_COOLDOWN_SECONDS
        if minimum_interval_seconds is None
        else max(0.0, minimum_interval_seconds)
    )

    async with window.lock:
        now = monotonic()
        if now < window.next_ephemeral_send_at:
            return False
        window.next_ephemeral_send_at = now + interval
        return True


async def postpone_ephemeral_status_send(chat_id: int, retry_after_seconds: float) -> None:
    """Push the next transient status send into the future after a Telegram rate limit."""
    window = await _window_for_chat(chat_id)
    async with window.lock:
        window.next_ephemeral_send_at = max(
            window.next_ephemeral_send_at,
            monotonic() + max(0.0, retry_after_seconds),
        )


async def _window_for_chat(chat_id: int) -> _ChatStatusWindow:
    async with _chat_status_windows_guard:
        return _chat_status_windows.setdefault(chat_id, _ChatStatusWindow(lock=asyncio.Lock()))
