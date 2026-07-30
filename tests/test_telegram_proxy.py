import types
from datetime import datetime, timezone

import pytest

from src.telegram_proxy import ProxyChannelRecord
from src.telegram_proxy import _json_safe
from src.telegram_proxy import TelegramProxy


def test_json_safe_converts_datetime_values() -> None:
    payload = {
        "when": datetime(2026, 3, 12, 14, 0, tzinfo=timezone.utc),
        "nested": [{"at": datetime(2026, 3, 12, 14, 1, tzinfo=timezone.utc)}],
    }

    result = _json_safe(payload)

    assert result["when"] == "2026-03-12 14:00:00+00:00"
    assert result["nested"][0]["at"] == "2026-03-12 14:01:00+00:00"


@pytest.mark.asyncio
async def test_read_messages_recent_first_omits_zero_min_id(monkeypatch) -> None:
    seen_kwargs: dict[str, object] = {}

    class FakeClient:
        async def iter_messages(self, entity, **kwargs):  # noqa: ARG002
            seen_kwargs.update(kwargs)
            yield types.SimpleNamespace(
                id=42,
                date=datetime(2026, 3, 12, 14, 0, tzinfo=timezone.utc),
                sender_id=10,
                views=123,
                forwards=4,
                replies=None,
                username=None,
                message="Recent message",
                to_dict=lambda: {"id": 42},
            )

    proxy = TelegramProxy()
    proxy._client = FakeClient()
    proxy._entity_cache[("linked_chat", 123)] = types.SimpleNamespace(username="chat_name")

    async def fake_resolve_entity(**kwargs):  # noqa: ARG001
        return proxy._entity_cache[("linked_chat", 123)]

    monkeypatch.setattr(proxy, "_resolve_entity", fake_resolve_entity)

    messages = await proxy.read_messages(
        kind="linked_chat",
        entity_id=123,
        min_id=0,
        limit=5,
        recent_first=True,
    )

    assert "min_id" not in seen_kwargs
    assert seen_kwargs["reverse"] is False
    assert seen_kwargs["limit"] == 5
    assert messages[0]["message_id"] == 42


@pytest.mark.asyncio
async def test_list_channels_applies_limit_after_filtering_broadcast_dialogs() -> None:
    class FakeChannel:
        def __init__(self, entity_id: int, title: str, *, broadcast: bool) -> None:
            self.id = entity_id
            self.title = title
            self.username = f"user_{entity_id}"
            self.broadcast = broadcast

    class FakeClient:
        def __init__(self) -> None:
            self.seen_limit = object()

        async def iter_dialogs(self, *, limit=None):
            self.seen_limit = limit
            for entity in [
                types.SimpleNamespace(id=10, title="Group", username=None, broadcast=False),
                FakeChannel(11, "Channel One", broadcast=True),
                types.SimpleNamespace(id=12, title="Direct", username=None),
                FakeChannel(13, "Channel Two", broadcast=True),
                FakeChannel(14, "Channel Three", broadcast=True),
            ]:
                yield types.SimpleNamespace(entity=entity)

        async def __call__(self, request):  # noqa: ARG002
            return types.SimpleNamespace(full_chat=types.SimpleNamespace(linked_chat_id=None))

    proxy = TelegramProxy()
    proxy._client = FakeClient()
    proxy._channel_cls = FakeChannel
    proxy._get_full_channel_request = lambda entity: entity

    records = await proxy.list_channels(limit=2)

    assert proxy._client.seen_limit is None
    assert records == [
        ProxyChannelRecord(
            entity_id=11,
            title="Channel One",
            username="user_11",
            linked_chat_id=None,
            linked_chat_title=None,
            linked_chat_username=None,
        ),
        ProxyChannelRecord(
            entity_id=13,
            title="Channel Two",
            username="user_13",
            linked_chat_id=None,
            linked_chat_title=None,
            linked_chat_username=None,
        ),
    ]


@pytest.mark.asyncio
async def test_read_messages_falls_back_to_dialog_cache_when_entity_lookup_by_id_fails(
    monkeypatch,
) -> None:
    seen_kwargs: dict[str, object] = {}

    class FakeChannel:
        def __init__(self, entity_id: int, *, broadcast: bool) -> None:
            self.id = entity_id
            self.broadcast = broadcast
            self.username = "ai_engineer_helper"
            self.title = "AI for Engineers"

    channel_entity = FakeChannel(3019299921, broadcast=True)

    class FakeClient:
        async def get_entity(self, entity_id):  # noqa: ARG002
            raise ValueError("not cached")

        async def iter_dialogs(self, *, limit=None):  # noqa: ARG002
            yield types.SimpleNamespace(entity=channel_entity)

        async def iter_messages(self, entity, **kwargs):
            assert entity is channel_entity
            seen_kwargs.update(kwargs)
            yield types.SimpleNamespace(
                id=111,
                date=datetime(2026, 3, 14, 6, 0, tzinfo=timezone.utc),
                sender_id=10,
                views=123,
                forwards=4,
                replies=None,
                username=None,
                message="Cached from dialogs",
                to_dict=lambda: {"id": 111},
            )

    proxy = TelegramProxy()
    proxy._client = FakeClient()
    proxy._channel_cls = FakeChannel

    messages = await proxy.read_messages(
        kind="channel",
        entity_id=3019299921,
        min_id=110,
        limit=5,
        recent_first=False,
    )

    assert seen_kwargs["min_id"] == 110
    assert seen_kwargs["reverse"] is True
    assert messages[0]["message_id"] == 111


# ── multi-username channels (Telegram's Channel.usernames) ────────
# @oestick ("AI и грабли", 12 925 subs) is joined and readable, yet its legacy
# `Channel.username` is EMPTY: the handle lives in `Channel.usernames` (a list of
# Username objects with .username/.active), so every configured-source lookup and
# every t.me link built from the legacy attribute silently resolved to nothing.
class _MultiUsernameChannel:
    def __init__(self, entity_id, title, usernames, *, username=None, broadcast=True):
        self.id = entity_id
        self.title = title
        self.username = username
        self.usernames = usernames
        self.broadcast = broadcast


def _uname(name, active=True):
    return types.SimpleNamespace(username=name, active=active)


@pytest.mark.asyncio
async def test_list_channels_falls_back_to_the_active_multi_username() -> None:
    entity = _MultiUsernameChannel(
        1741956568, "AI и грабли", [_uname("old_handle", active=False), _uname("oestick")]
    )

    class FakeClient:
        async def iter_dialogs(self, *, limit=None):  # noqa: ARG002
            yield types.SimpleNamespace(entity=entity)

        async def __call__(self, request):  # noqa: ARG002
            return types.SimpleNamespace(full_chat=types.SimpleNamespace(linked_chat_id=None))

    proxy = TelegramProxy()
    proxy._client = FakeClient()
    proxy._channel_cls = _MultiUsernameChannel
    proxy._get_full_channel_request = lambda entity: entity

    records = await proxy.list_channels(limit=10)

    assert [r.username for r in records] == ["oestick"]
    assert records[0].entity_id == 1741956568


@pytest.mark.asyncio
async def test_list_channels_prefers_the_legacy_username_when_present() -> None:
    entity = _MultiUsernameChannel(
        11, "Legacy", [_uname("secondary")], username="primary_handle"
    )

    class FakeClient:
        async def iter_dialogs(self, *, limit=None):  # noqa: ARG002
            yield types.SimpleNamespace(entity=entity)

        async def __call__(self, request):  # noqa: ARG002
            return types.SimpleNamespace(full_chat=types.SimpleNamespace(linked_chat_id=None))

    proxy = TelegramProxy()
    proxy._client = FakeClient()
    proxy._channel_cls = _MultiUsernameChannel
    proxy._get_full_channel_request = lambda entity: entity

    records = await proxy.list_channels(limit=10)
    assert [r.username for r in records] == ["primary_handle"]


@pytest.mark.asyncio
async def test_list_channels_lookup_matches_a_multi_username() -> None:
    entity = _MultiUsernameChannel(1741956568, "AI и грабли", [_uname("oestick")])

    class FakeClient:
        async def iter_dialogs(self, *, limit=None):  # noqa: ARG002
            yield types.SimpleNamespace(entity=entity)

        async def __call__(self, request):  # noqa: ARG002
            return types.SimpleNamespace(full_chat=types.SimpleNamespace(linked_chat_id=None))

    proxy = TelegramProxy()
    proxy._client = FakeClient()
    proxy._channel_cls = _MultiUsernameChannel
    proxy._get_full_channel_request = lambda entity: entity

    records = await proxy.list_channels(limit=10, lookup="oestick")
    assert [r.entity_id for r in records] == [1741956568]


@pytest.mark.asyncio
async def test_list_channels_reads_a_linked_chat_multi_username() -> None:
    linked = _MultiUsernameChannel(200, "Chat", [_uname("chat_handle")], broadcast=False)
    parent = _MultiUsernameChannel(100, "Channel", [], username="parent")

    class FakeClient:
        async def iter_dialogs(self, *, limit=None):  # noqa: ARG002
            yield types.SimpleNamespace(entity=linked)
            yield types.SimpleNamespace(entity=parent)

        async def __call__(self, request):  # noqa: ARG002
            return types.SimpleNamespace(full_chat=types.SimpleNamespace(linked_chat_id=200))

    proxy = TelegramProxy()
    proxy._client = FakeClient()
    proxy._channel_cls = _MultiUsernameChannel
    proxy._get_full_channel_request = lambda entity: entity

    records = await proxy.list_channels(limit=10)
    assert records[0].linked_chat_username == "chat_handle"


@pytest.mark.asyncio
async def test_list_channels_ignores_inactive_only_usernames() -> None:
    entity = _MultiUsernameChannel(12, "Retired", [_uname("gone", active=False)])

    class FakeClient:
        async def iter_dialogs(self, *, limit=None):  # noqa: ARG002
            yield types.SimpleNamespace(entity=entity)

        async def __call__(self, request):  # noqa: ARG002
            return types.SimpleNamespace(full_chat=types.SimpleNamespace(linked_chat_id=None))

    proxy = TelegramProxy()
    proxy._client = FakeClient()
    proxy._channel_cls = _MultiUsernameChannel
    proxy._get_full_channel_request = lambda entity: entity

    records = await proxy.list_channels(limit=10)
    assert records[0].username is None


@pytest.mark.asyncio
async def test_read_messages_links_via_a_multi_username(monkeypatch) -> None:
    """The t.me link is built from the entity's handle; without the fallback every
    @oestick message would carry link=None and build_draft_input drops link-less
    rows, so the channel could never reach the digest even once resolved."""
    entity = _MultiUsernameChannel(1741956568, "AI и грабли", [_uname("oestick")])

    class FakeClient:
        async def iter_messages(self, entity, **kwargs):  # noqa: ARG002
            yield types.SimpleNamespace(
                id=42,
                date=datetime(2026, 7, 30, 6, 0, tzinfo=timezone.utc),
                sender_id=None,
                views=1000,
                forwards=3,
                replies=None,
                message="Пост канала",
                to_dict=lambda: {"id": 42},
            )

    proxy = TelegramProxy()
    proxy._client = FakeClient()

    async def fake_resolve_entity(**kwargs):  # noqa: ARG001
        return entity

    monkeypatch.setattr(proxy, "_resolve_entity", fake_resolve_entity)

    messages = await proxy.read_messages(
        kind="channel", entity_id=1741956568, min_id=0, limit=5, recent_first=True
    )
    assert messages[0]["link"] == "https://t.me/oestick/42"
