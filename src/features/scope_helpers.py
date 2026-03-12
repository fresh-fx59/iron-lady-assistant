from __future__ import annotations

from typing import Any

from ..sessions import make_scope_key


def thread_id(message: Any) -> int | None:
    return getattr(message, "message_thread_id", None)


def scope_key(chat_id: int, message_thread_id: int | None = None) -> str:
    return make_scope_key(chat_id, message_thread_id)


def scope_key_from_message(message: Any) -> str:
    return scope_key(message.chat.id, thread_id(message))


def actor_id(message: Any) -> int:
    if message.from_user and message.from_user.id:
        return message.from_user.id
    return message.chat.id
