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
    # user/bot dialogs are filtered SERVER-SIDE (see the leak test below).
    assert [record.kind for record in records] == ["channel", "channel", "megagroup", "group"]
    # The whole point of the route: the two group peers /v1/channels can never see.
    assert {103, 104} <= by_id.keys()
    assert by_id[102].username == "oestick"
    assert by_id[102].usernames == ["oestick"]
    assert by_id[101].participants_count == 42
    assert by_id[103].username is None
    assert by_id[103].is_megagroup is True
    assert by_id[101].is_broadcast is True
    assert by_id[104].kind == "group"


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


# ── 2026-07-30 review: the route's four confirmed defects ─────────


@pytest.mark.asyncio
async def test_dms_and_bot_chats_never_leave_the_proxy() -> None:
    """The token holder must not receive the account's entire contact list."""
    entities = [
        FakeChannel(401, "News", broadcast=True, username="news"),
        FakeUser(402),
        FakeUser(403, bot=True),
    ]
    client = FakeClient(entities)

    records = await _proxy(client).list_dialogs(limit=500)

    assert [r.entity_id for r in records] == [401]
    assert all(r.kind not in {"user", "bot"} for r in records)


@pytest.mark.asyncio
async def test_the_linked_chat_sweep_is_paced_and_bounded(monkeypatch) -> None:
    from src import telegram_proxy

    monkeypatch.setattr(telegram_proxy, "_LINKED_LOOKUP_MAX", 3)
    slept: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr(telegram_proxy.asyncio, "sleep", fake_sleep)
    entities = [FakeChannel(500 + i, f"C{i}", broadcast=True, username=f"c{i}") for i in range(6)]
    client = FakeClient(entities, linked={500: 900})

    records = await _proxy(client).list_dialogs(limit=500, with_linked=True)

    assert client.full_calls == 3, "the daily sweep must be bounded"
    assert slept == [pytest.approx(telegram_proxy._LINKED_LOOKUP_PACING_SECONDS)] * 2
    assert [r.linked_chat_lookup for r in records][3:] == ["skipped (bounded at 3 lookups/call)"] * 3
    assert records[0].linked_chat_id == 900


@pytest.mark.asyncio
async def test_a_floodwait_stops_the_sweep_and_is_recorded_as_degraded_input() -> None:
    """A swallowed FloodWait turned linked_chat_id into a silent None."""
    from telethon.errors import FloodWaitError

    class Flooding(FakeClient):
        async def __call__(self, request):  # noqa: ARG002
            self.full_calls += 1
            exc = FloodWaitError.__new__(FloodWaitError)
            exc.seconds = 42
            raise exc

    entities = [FakeChannel(600 + i, f"C{i}", broadcast=True, username=f"c{i}") for i in range(3)]
    client = Flooding(entities)

    records = await _proxy(client).list_dialogs(limit=500, with_linked=True)

    assert client.full_calls == 1, "never retry inside a FloodWait"
    assert all("floodwait" in r.linked_chat_lookup for r in records)
    assert all(r.linked_chat_id is None for r in records)


@pytest.mark.asyncio
async def test_a_generic_lookup_failure_is_recorded_too() -> None:
    class Boom(FakeClient):
        async def __call__(self, request):  # noqa: ARG002
            raise RuntimeError("CHANNEL_PRIVATE")

    client = Boom([FakeChannel(701, "Parent", broadcast=True, username="parent")])

    records = await _proxy(client).list_dialogs(limit=10, with_linked=True)

    assert records[0].linked_chat_id is None
    assert records[0].linked_chat_lookup == "failed (RuntimeError)"


@pytest.mark.asyncio
async def test_the_route_clamps_like_its_list_channels_neighbour_and_hides_internals(monkeypatch) -> None:
    import json as _json

    from src import config, telegram_proxy

    monkeypatch.setattr(config, "TELEGRAM_PROXY_API_KEY", "tok", raising=False)
    asked: dict = {}

    class RecordingProxy:
        async def list_dialogs(self, *, limit: int, with_linked: bool = False):
            asked.update(limit=limit, with_linked=with_linked)
            return [
                telegram_proxy.ProxyDialogRecord(
                    entity_id=1,
                    title="C",
                    kind="channel",
                    username="c",
                    usernames=["c"],
                    is_broadcast=True,
                    is_megagroup=False,
                    participants_count=None,
                    linked_chat_id=None,
                    _entity="SHOULD-NOT-LEAK",
                )
            ]

    request = types.SimpleNamespace(
        query={"limit": "1000", "with_linked": "1"},
        headers={"Authorization": "Bearer tok"},
        app={"proxy": RecordingProxy()},
    )

    response = await telegram_proxy._list_dialogs(request)

    assert asked["limit"] == 500  # 1000 was silently clamped before; 500 matches /v1/channels
    payload = _json.loads(response.text)
    assert "_entity" not in payload["dialogs"][0]
    assert payload["dialogs"][0]["linked_chat_lookup"] == "ok"
