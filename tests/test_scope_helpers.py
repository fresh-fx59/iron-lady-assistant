from types import SimpleNamespace

from src.features.scope_helpers import scope_key_from_message, thread_id


def test_thread_id_uses_message_thread_id_when_present() -> None:
    message = SimpleNamespace(message_thread_id=77, reply_to_message=None, is_topic_message=True)

    assert thread_id(message) == 77


def test_thread_id_falls_back_to_reply_thread_id() -> None:
    reply = SimpleNamespace(message_thread_id=4451)
    message = SimpleNamespace(message_thread_id=None, reply_to_message=reply, is_topic_message=True)

    assert thread_id(message) == 4451


def test_scope_key_from_message_uses_reply_thread_fallback() -> None:
    reply = SimpleNamespace(message_thread_id=4451)
    message = SimpleNamespace(
        chat=SimpleNamespace(id=-1003796914868),
        message_thread_id=None,
        reply_to_message=reply,
        is_topic_message=True,
    )

    assert scope_key_from_message(message) == "-1003796914868:4451"


def test_thread_id_recovers_topic_starter_reply_when_thread_missing() -> None:
    reply = SimpleNamespace(message_thread_id=None, forum_topic_created=object(), message_id=4451)
    message = SimpleNamespace(message_thread_id=None, reply_to_message=reply, is_topic_message=True)

    assert thread_id(message) == 4451
