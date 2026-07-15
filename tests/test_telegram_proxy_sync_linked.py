"""Tests for POST /v1/join/sync-linked — the ENQUEUE-ONLY sync that queues the
linked discussion chats behind broadcast channels the parser account is already
in, so the existing paced loop can join them (ban-safe, ≤cap/day).

Contract under test:

  * a broadcast channel with a linked_chat_id → the linked id is ENQUEUED as a
    pending ``id:<n>`` / kind ``linked`` target (via _discover_and_enqueue_linked),
  * a broadcast channel with NO linked chat → nothing enqueued,
  * a linked chat we are ALREADY a member of → skipped, never queued,
  * the sync NEVER issues a join/import request (enqueue only),
  * it is idempotent (a 2nd call enqueues nothing new),
  * the HTTP endpoint requires Bearer auth.

The Telethon client is fully mocked and every db is a throwaway tmp sqlite —
nothing here touches real Telegram or the @giedi_0 account.
"""

from __future__ import annotations

import inspect
import types

import pytest

import src.telegram_proxy as tp
from src.telegram_proxy import JoinStore, TelegramProxy


# ── Test doubles ───────────────────────────────────────────────────
class FakeChannel:
    """Stand-in for a Telethon ``Channel`` (broadcast channel or megagroup)."""

    def __init__(self, entity_id: int, *, broadcast: bool, username: str | None = None):
        self.id = entity_id
        self.broadcast = broadcast
        self.megagroup = not broadcast
        self.username = username
        self.title = username or f"chan-{entity_id}"


def _full_req(entity):
    return ("full", entity)


def _join_req(entity):
    return ("join", entity)


def _import_req(invite_hash):
    return ("import", invite_hash)


class FakeClient:
    """Minimal Telethon stand-in. Records every request so a stray join/import
    (which must NEVER happen on the sync path) is detectable."""

    def __init__(self, *, dialogs=None, full_map=None, entities=None):
        self._dialogs = list(dialogs or [])
        self.full_map = dict(full_map or {})   # entity_id -> full result
        self.entities = dict(entities or {})   # get_entity ref -> entity
        self.full_calls: list[int] = []
        self.join_calls: list = []
        self.import_calls: list = []

    async def iter_dialogs(self, *, limit=None):
        count = 0
        for entity in self._dialogs:
            if limit is not None and count >= limit:
                break
            yield types.SimpleNamespace(entity=entity)
            count += 1

    async def get_entity(self, ref):
        if ref in self.entities:
            return self.entities[ref]
        return FakeChannel(int(ref) if isinstance(ref, int) else 0, broadcast=False)

    async def __call__(self, request):
        marker, payload = request
        if marker == "full":
            self.full_calls.append(int(payload.id))
            return self.full_map.get(
                int(payload.id),
                types.SimpleNamespace(
                    full_chat=types.SimpleNamespace(linked_chat_id=None), chats=[]
                ),
            )
        if marker == "join":
            self.join_calls.append(payload)
            return types.SimpleNamespace(chats=[payload])
        if marker == "import":
            self.import_calls.append(payload)
            return types.SimpleNamespace(chats=[payload])
        raise AssertionError(f"unexpected request marker {marker!r}")


def _make_proxy(store: JoinStore, client: FakeClient) -> TelegramProxy:
    proxy = TelegramProxy()
    proxy._client = client
    proxy._join_store = store
    proxy._channel_cls = FakeChannel
    proxy._get_full_channel_request = _full_req
    # Wire the join/import factories too, so an accidental join attempt would be
    # dispatched by the fake client (and recorded) rather than silently no-op.
    proxy._join_channel_request = _join_req
    proxy._import_chat_invite_request = _import_req
    return proxy


def _pending_row(store: JoinStore, target: str):
    with store._connect() as con:
        return con.execute(
            "SELECT kind, status FROM joins WHERE target = ?", (target,)
        ).fetchone()


# ── 1. Broadcast channel with a linked chat → enqueued ─────────────
@pytest.mark.asyncio
async def test_sync_enqueues_linked_chat_for_broadcast(tmp_path):
    store = JoinStore(db_path=tmp_path / "join.db")
    channel = FakeChannel(500, broadcast=True, username="chan")
    linked = FakeChannel(600, broadcast=False, username="chan_chat")
    client = FakeClient(
        dialogs=[channel],  # linked 600 is NOT a dialog → not yet joined
        full_map={
            500: types.SimpleNamespace(
                full_chat=types.SimpleNamespace(linked_chat_id=600),
                chats=[channel, linked],
            )
        },
        entities={600: linked},
    )
    proxy = _make_proxy(store, client)

    result = await proxy.sync_linked_discussions()

    assert result == {
        "channels_scanned": 1,
        "linked_enqueued": 1,
        "linked_already_joined": 0,
        "linked_ids": [600],
    }
    row = _pending_row(store, "id:600")
    assert row is not None
    assert row["kind"] == "linked"
    assert row["status"] == "pending"
    # ENQUEUE-ONLY: no join/import request was issued by the sync.
    assert client.join_calls == []
    assert client.import_calls == []


# ── 2. Broadcast channel with NO linked chat → nothing enqueued ────
@pytest.mark.asyncio
async def test_sync_skips_channel_without_linked_chat(tmp_path):
    store = JoinStore(db_path=tmp_path / "join.db")
    channel = FakeChannel(500, broadcast=True, username="chan")
    client = FakeClient(
        dialogs=[channel],
        full_map={
            500: types.SimpleNamespace(
                full_chat=types.SimpleNamespace(linked_chat_id=None), chats=[]
            )
        },
    )
    proxy = _make_proxy(store, client)

    result = await proxy.sync_linked_discussions()

    assert result == {
        "channels_scanned": 1,
        "linked_enqueued": 0,
        "linked_already_joined": 0,
        "linked_ids": [],
    }
    assert store.pending_count() == 0
    assert client.join_calls == []


# ── 3. Linked chat we are already a member of → skipped ────────────
@pytest.mark.asyncio
async def test_sync_skips_already_joined_linked_chat(tmp_path):
    store = JoinStore(db_path=tmp_path / "join.db")
    channel = FakeChannel(500, broadcast=True, username="chan")
    linked = FakeChannel(600, broadcast=False, username="chan_chat")
    # linked 600 IS a dialog → preload marks it already-joined.
    client = FakeClient(
        dialogs=[channel, linked],
        full_map={
            500: types.SimpleNamespace(
                full_chat=types.SimpleNamespace(linked_chat_id=600),
                chats=[channel, linked],
            )
        },
    )
    proxy = _make_proxy(store, client)

    result = await proxy.sync_linked_discussions()

    assert result == {
        "channels_scanned": 1,
        "linked_enqueued": 0,
        "linked_already_joined": 1,
        "linked_ids": [],
    }
    # An already-joined chat must NOT be queued.
    assert store.has_target("id:600") is False
    assert store.pending_count() == 0
    assert client.join_calls == []


# ── 4. The sync path never issues a join/import request ────────────
def test_sync_path_never_joins_or_imports():
    """Static guard: the sync method + its handler must not reference any join /
    import request or delegate to the paced join methods — enqueue only."""
    banned = (
        "_join_channel_request",
        "_import_chat_invite_request",
        "JoinChannelRequest",
        "ImportChatInviteRequest",
        "_attempt_join",
        "_join_public",
        "_join_private",
        "_join_linked",
    )
    sources = "\n".join(
        inspect.getsource(fn)
        for fn in (TelegramProxy.sync_linked_discussions, tp._join_sync_linked)
    )
    for name in banned:
        assert name not in sources, f"sync path must never reference {name}"


# ── 5. Idempotent: a 2nd sync enqueues nothing new ─────────────────
@pytest.mark.asyncio
async def test_sync_is_idempotent(tmp_path):
    store = JoinStore(db_path=tmp_path / "join.db")
    channel = FakeChannel(500, broadcast=True, username="chan")
    linked = FakeChannel(600, broadcast=False, username="chan_chat")
    client = FakeClient(
        dialogs=[channel],
        full_map={
            500: types.SimpleNamespace(
                full_chat=types.SimpleNamespace(linked_chat_id=600),
                chats=[channel, linked],
            )
        },
        entities={600: linked},
    )
    proxy = _make_proxy(store, client)

    first = await proxy.sync_linked_discussions()
    assert first["linked_enqueued"] == 1
    assert first["linked_ids"] == [600]

    second = await proxy.sync_linked_discussions()
    # Already queued (not yet joined) → counted in neither bucket, nothing new.
    assert second == {
        "channels_scanned": 1,
        "linked_enqueued": 0,
        "linked_already_joined": 0,
        "linked_ids": [],
    }
    # Still exactly one linked row — no duplicate.
    with store._connect() as con:
        n = con.execute(
            "SELECT COUNT(*) AS n FROM joins WHERE target = ?", ("id:600",)
        ).fetchone()["n"]
    assert n == 1
    assert client.join_calls == []


# ── 6. HTTP endpoint: happy path + auth ────────────────────────────
async def _http_app(proxy, monkeypatch):
    """Build the real aiohttp app with startup/cleanup skipped + proxy injected."""
    from aiohttp.test_utils import TestClient, TestServer

    import src.config as cfg

    monkeypatch.setattr(cfg, "TELEGRAM_PROXY_API_KEY", "secret", raising=False)
    app = tp.create_app()
    app.on_startup.clear()
    app.on_cleanup.clear()
    app["proxy"] = proxy
    server = TestServer(app)
    http = TestClient(server)
    await http.start_server()
    return http


@pytest.mark.asyncio
async def test_sync_http_envelope(tmp_path, monkeypatch):
    store = JoinStore(db_path=tmp_path / "join.db")
    channel = FakeChannel(500, broadcast=True, username="chan")
    linked = FakeChannel(600, broadcast=False, username="chan_chat")
    client = FakeClient(
        dialogs=[channel],
        full_map={
            500: types.SimpleNamespace(
                full_chat=types.SimpleNamespace(linked_chat_id=600),
                chats=[channel, linked],
            )
        },
        entities={600: linked},
    )
    proxy = _make_proxy(store, client)
    http = await _http_app(proxy, monkeypatch)
    try:
        resp = await http.post(
            "/v1/join/sync-linked", headers={"Authorization": "Bearer secret"}
        )
        assert resp.status == 200
        body = await resp.json()
        assert body["ok"] is True
        assert body["channels_scanned"] == 1
        assert body["linked_enqueued"] == 1
        assert body["linked_already_joined"] == 0
        assert body["linked_ids"] == [600]
    finally:
        await http.close()


@pytest.mark.asyncio
async def test_sync_http_requires_auth(tmp_path, monkeypatch):
    store = JoinStore(db_path=tmp_path / "join.db")
    proxy = _make_proxy(store, FakeClient(dialogs=[]))
    http = await _http_app(proxy, monkeypatch)
    try:
        resp = await http.post("/v1/join/sync-linked")
        assert resp.status == 401
    finally:
        await http.close()
