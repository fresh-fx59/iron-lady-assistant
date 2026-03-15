from __future__ import annotations

from typing import Any

from ..sessions import make_scope_key


def thread_id(message: Any) -> int | None:
    direct = getattr(message, "message_thread_id", None)
    if isinstance(direct, int):
        return direct

    reply = getattr(message, "reply_to_message", None)
    reply_thread = getattr(reply, "message_thread_id", None)
    if isinstance(reply_thread, int):
        return reply_thread

    # Telegram occasionally omits `message_thread_id` on replies to the
    # topic starter service message. In that narrow case the starter message id
    # is the topic id, so recover it instead of collapsing to `chat:main`.
    if getattr(message, "is_topic_message", False) and getattr(reply, "forum_topic_created", None):
        reply_message_id = getattr(reply, "message_id", None)
        if isinstance(reply_message_id, int):
            return reply_message_id

    return None


def scope_key(chat_id: int, message_thread_id: int | None = None) -> str:
    return make_scope_key(chat_id, message_thread_id)


def scope_key_from_message(message: Any) -> str:
    return scope_key(message.chat.id, thread_id(message))


def actor_id(message: Any) -> int:
    if message.from_user and message.from_user.id:
        return message.from_user.id
    return message.chat.id
