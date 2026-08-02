"""The passive-chat rule must be enforced for EVERY message type.

On 2026-08-01 a captioned video posted to a passive chat ran a full agent turn
and streamed its `ffprobe` tool calls into the channel. The rule was correct
and the chat was configured correctly — but `_should_ignore_passive_message`
was an opt-in check inside individual handlers, and only 4 of the 13 message
handlers opted in. Video was not one of them.

These tests drive the gate itself, so they cover the handlers that exist today
and any handler added later.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src import bot as bot_module
from src import config as bot_config

PASSIVE_CHAT = -1003019299921
ACTIVE_CHAT = -1003796914868

#: Every content type the router has a handler for.
CONTENT_TYPES = [
    "text",
    "voice",
    "photo",
    "document",
    "audio",
    "video",
    "animation",
    "video_note",
]


def _message(chat_id: int, content_type: str, *, text=None, caption=None, reply_to=None):
    return SimpleNamespace(
        chat=SimpleNamespace(id=chat_id),
        message_id=216,
        message_thread_id=None,
        content_type=content_type,
        text=text,
        caption=caption,
        reply_to_message=reply_to,
        bot=SimpleNamespace(username="iron_lady_assistant_bot", id=999, me=None),
    )


@pytest.fixture(autouse=True)
def _passive_config(monkeypatch):
    monkeypatch.setattr(bot_config, "PASSIVE_CHAT_IDS", {PASSIVE_CHAT, -1003305897502})


async def _run_gate(message) -> tuple[bool, object]:
    """Returns (handler_was_called, gate_result)."""
    handler = AsyncMock(return_value="handled")
    # The gate type-checks with isinstance(event, Message); patch that to the
    # stub's type so we can exercise it without constructing a full aiogram
    # Message (which requires a bound bot and dozens of fields).
    result = await bot_module.passive_chat_gate(handler, message, {})
    return handler.await_count > 0, result


@pytest.fixture(autouse=True)
def _accept_stub_messages(monkeypatch):
    monkeypatch.setattr(bot_module, "Message", SimpleNamespace)


@pytest.mark.asyncio
@pytest.mark.parametrize("content_type", CONTENT_TYPES)
async def test_gate_drops_every_content_type_in_passive_chat(content_type: str) -> None:
    """The regression: a captioned video must be dropped, like text already was."""
    message = _message(
        PASSIVE_CHAT,
        content_type,
        caption="сервис для создания цифровых аватаров",
    )
    called, result = await _run_gate(message)
    assert called is False, f"{content_type} reached its handler in a passive chat"
    assert result is None


@pytest.mark.asyncio
@pytest.mark.parametrize("content_type", CONTENT_TYPES)
async def test_gate_allows_every_content_type_in_an_active_chat(content_type: str) -> None:
    """Non-passive chats — the ILA supergroup — are untouched."""
    message = _message(ACTIVE_CHAT, content_type, caption="привет")
    called, result = await _run_gate(message)
    assert called is True, f"{content_type} was dropped in a non-passive chat"
    assert result == "handled"


@pytest.mark.asyncio
@pytest.mark.parametrize("content_type", CONTENT_TYPES)
async def test_gate_allows_explicit_mention_in_passive_chat(content_type: str) -> None:
    """@-mentioning her in the passive chat still wakes her, as before."""
    message = _message(
        PASSIVE_CHAT,
        content_type,
        caption="@iron_lady_assistant_bot посмотри это видео",
    )
    called, _ = await _run_gate(message)
    assert called is True, f"{content_type} ignored an explicit @-mention"


@pytest.mark.asyncio
async def test_gate_allows_reply_to_the_bot_in_passive_chat() -> None:
    """Replying to her message counts as addressing her."""
    reply = SimpleNamespace(from_user=SimpleNamespace(is_bot=True, id=999))
    message = _message(PASSIVE_CHAT, "text", text="и что дальше?", reply_to=reply)
    called, _ = await _run_gate(message)
    assert called is True


@pytest.mark.asyncio
async def test_gate_passes_through_non_message_events() -> None:
    """Callback queries and the like must not be swallowed."""
    handler = AsyncMock(return_value="handled")
    result = await bot_module.passive_chat_gate(handler, object(), {})
    assert handler.await_count == 1
    assert result == "handled"


def test_gate_is_registered_on_every_message_stream() -> None:
    """If registration is lost, the handlers are exposed again — assert it."""
    for stream in ("message", "channel_post", "edited_message"):
        observer = getattr(bot_module.router, stream)
        registered = list(observer.outer_middleware)
        assert bot_module.passive_chat_gate in registered, (
            f"passive_chat_gate is not registered on router.{stream}"
        )
