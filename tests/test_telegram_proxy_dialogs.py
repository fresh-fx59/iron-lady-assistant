"""GET /v1/dialogs — read-only enumeration of EVERY dialog, not just broadcasts.

`/v1/channels` filters to broadcast=True, so a hand-joined group is invisible to
every audit. These tests pin the unfiltered view's shape against a faked
Telethon dialog list, including the multi-username channel (`entity.username`
empty, handle only in `entity.usernames`) and a group with no username at all.
"""
from __future__ import annotations

import types

import pytest

from src.telegram_proxy import TelegramProxy, _dialog_kind, _dialog_usernames


class FakeChannel:
    def __init__(
        self,
        entity_id: int,
        title: str,
        *,
        broadcast: bool = False,
        megagroup: bool = False,
        username: str | None = None,
        usernames: list | None = None,
        participants_count: int | None = None,
    ) -> None:
        self.id = entity_id
        self.title = title
        self.broadcast = broadcast
        self.megagroup = megagroup
        self.username = username
        self.usernames = usernames or []
        self.participants_count = participants_count


class FakeChat:
    """Legacy (non-channel) group: has a title, no broadcast/megagroup flags."""

    def __init__(self, entity_id: int, title: str, participants_count: int | None = None) -> None:
        self.id = entity_id
        self.title = title
        self.participants_count = participants_count


class FakeUser:
    def __init__(self, entity_id: int, *, bot: bool = False) -> None:
        self.id = entity_id
        self.bot = bot
        self.first_name = "Someone"


class FakeUsername:
    def __init__(self, username: str, active: bool = True) -> None:
        self.username = username
        self.active = active


def _dialogs(entities):
    return [types.SimpleNamespace(entity=entity, name=getattr(entity, "title", "dm")) for entity in entities]


class FakeClient:
    def __init__(self, entities, *, linked: dict[int, int] | None = None) -> None:
        self._dialogs = _dialogs(entities)
        self._linked = linked or {}
        self.seen_limit: object = object()
        self.full_calls = 0

    async def iter_dialogs(self, *, limit=None):
        self.seen_limit = limit
        for dialog in self._dialogs:
            yield dialog

    async def __call__(self, request):
        self.full_calls += 1
        entity_id = int(getattr(request, "channel_id"))
        return types.SimpleNamespace(
            full_chat=types.SimpleNamespace(linked_chat_id=self._linked.get(entity_id))
        )


def _proxy(client: FakeClient) -> TelegramProxy:
    proxy = TelegramProxy()
    proxy._client = client
    proxy._get_full_channel_request = lambda entity: types.SimpleNamespace(channel_id=entity.id)
    return proxy


def test_dialog_usernames_reads_the_multi_username_list_when_legacy_field_is_empty() -> None:
    entity = FakeChannel(1, "Oestick", broadcast=True, username="", usernames=[FakeUsername("oestick")])

    assert _dialog_usernames(entity) == ["oestick"]


def test_dialog_usernames_skips_inactive_handles_and_dedupes() -> None:
    entity = FakeChannel(
        2,
        "Multi",
        broadcast=True,
        username="primary",
        usernames=[FakeUsername("primary"), FakeUsername("old_one", active=False), FakeUsername("alias")],
    )

    assert _dialog_usernames(entity) == ["primary", "alias"]


@pytest.mark.parametrize(
    "entity,expected",
    [
        (FakeChannel(1, "News", broadcast=True), "channel"),
        (FakeChannel(2, "Chat", megagroup=True), "megagroup"),
        (FakeChat(3, "Old group"), "group"),
        (FakeUser(4), "user"),
        (FakeUser(5, bot=True), "bot"),
    ],
)
def test_dialog_kind_classifies_every_peer_shape(entity, expected) -> None:
    assert _dialog_kind(entity) == expected


@pytest.mark.asyncio
async def test_list_dialogs_returns_groups_that_list_channels_filters_out() -> None:
    entities = [
        FakeChannel(101, "AI Daily", broadcast=True, username="ai_daily_summary", participants_count=42),
        FakeChannel(102, "Oestick", broadcast=True, username="", usernames=[FakeUsername("oestick")]),
        FakeChannel(103, "Hidden group", megagroup=True, participants_count=7),
        FakeChat(104, "Legacy group"),
        FakeUser(105),
        FakeUser(106, bot=True),
    ]
    client = FakeClient(entities)

    records = await _proxy(client).list_dialogs(limit=500)

    by_id = {record.entity_id: record for record in records}
    assert client.seen_limit == 500
    assert [record.kind for record in records] == ["channel", "channel", "megagroup", "group", "user", "bot"]
    # The whole point of the route: the two group peers /v1/channels can never see.
    assert {103, 104} <= by_id.keys()
    assert by_id[102].username == "oestick"
    assert by_id[102].usernames == ["oestick"]
    assert by_id[101].participants_count == 42
    assert by_id[103].username is None
    assert by_id[103].is_megagroup is True
    assert by_id[101].is_broadcast is True
    assert by_id[104].kind == "group"
    assert by_id[105].title  # never empty, even for a DM


@pytest.mark.asyncio
async def test_list_dialogs_skips_the_linked_lookup_unless_asked() -> None:
    client = FakeClient([FakeChannel(201, "Parent", broadcast=True, username="parent")], linked={201: 202})

    records = await _proxy(client).list_dialogs(limit=10)

    assert client.full_calls == 0
    assert records[0].linked_chat_id is None


@pytest.mark.asyncio
async def test_list_dialogs_resolves_linked_chat_for_channels_when_asked() -> None:
    entities = [
        FakeChannel(201, "Parent", broadcast=True, username="parent"),
        FakeChannel(202, "Parent chat", megagroup=True),
    ]
    client = FakeClient(entities, linked={201: 202})

    records = await _proxy(client).list_dialogs(limit=10, with_linked=True)

    assert records[0].linked_chat_id == 202
    # Only broadcast channels can have a discussion group: one round trip, not two.
    assert client.full_calls == 1
    assert records[1].linked_chat_id is None


@pytest.mark.asyncio
async def test_list_dialogs_survives_a_failing_linked_lookup() -> None:
    class Boom(FakeClient):
        async def __call__(self, request):  # noqa: ARG002
            raise RuntimeError("CHANNEL_PRIVATE")

    client = Boom([FakeChannel(301, "Parent", broadcast=True, username="parent")])

    records = await _proxy(client).list_dialogs(limit=10, with_linked=True)

    assert records[0].linked_chat_id is None
