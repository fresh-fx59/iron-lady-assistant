"""Tests for the lead-candidate feed + sender-resolution proxy endpoints.

Covers the SHARED CONTRACT for the lead scorer:

  * ``GET /v1/leads/candidates`` — rowid cursor pagination (only rowid>since_id,
    ordered ASC, correct max_id), the ``role='lead'`` source filter, and the
    ``sender_id IS NULL`` exclusion; plus the derived ``t.me/c/`` deep link.
  * ``GET /v1/users/<id>`` — a cache hit avoids a second ``get_entity``, and a
    FloodWait (or any lookup error) returns HTTP 200 with an ``error`` field
    rather than a 500.

The Telethon client is fully mocked and every db is a throwaway tmp sqlite —
nothing here touches real Telegram or the real digest db.
"""

from __future__ import annotations

import types
from datetime import datetime, timezone

import pytest
from telethon.errors import FloodWaitError

import src.telegram_proxy as tp
from src.telegram_digest import (
    LEAD_SOURCE_ROLE,
    TelegramDigestStore,
    lead_message_link,
)


# ── helpers ────────────────────────────────────────────────────────
def _seed_source(store: TelegramDigestStore, *, peer_key, entity_id, title, kind, role):
    store.upsert_source(
        peer_key=peer_key,
        entity_id=entity_id,
        title=title,
        username=None,
        kind=kind,
        linked_channel_key=None,
        role=role,
    )


def _seed_message(store: TelegramDigestStore, *, peer_key, message_id, sender_id, text="hi"):
    store.insert_message(
        peer_key=peer_key,
        message_id=message_id,
        posted_at=datetime(2026, 7, 14, 12, 0, tzinfo=timezone.utc),
        sender_id=sender_id,
        views=None,
        forwards=None,
        replies=None,
        link=None,
        text=text,
        raw_json={},
    )


def _lead_store(tmp_path):
    """A store with one lead source + one non-lead (digest) source seeded."""
    store = TelegramDigestStore(db_path=tmp_path / "digest.db")
    _seed_source(
        store,
        peer_key="linked_chat:2222",
        entity_id=2222,
        title="Lead Group",
        kind="linked_chat",
        role=LEAD_SOURCE_ROLE,
    )
    _seed_source(
        store,
        peer_key="channel:9999",
        entity_id=9999,
        title="Digest Channel",
        kind="channel",
        role="digest",
    )
    return store


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


# ── link derivation ────────────────────────────────────────────────
@pytest.mark.parametrize(
    "peer_key,message_id,expected",
    [
        ("linked_chat:2222", 15, "https://t.me/c/2222/15"),
        ("channel:1234567890", 7, "https://t.me/c/1234567890/7"),
        ("linked_chat:-1002222", 15, "https://t.me/c/2222/15"),  # marked-id prefix stripped
        ("linked_chat:", 5, ""),  # no id
        ("garbage", 5, ""),  # no colon
        ("linked_chat:abc", 5, ""),  # non-numeric id
    ],
)
def test_lead_message_link_derivation(peer_key, message_id, expected):
    assert lead_message_link(peer_key, message_id) == expected


# ── candidates: pagination / filters (store level) ─────────────────
def test_candidates_pagination_by_since_id(tmp_path):
    store = _lead_store(tmp_path)
    for mid in (10, 11, 12):
        _seed_message(store, peer_key="linked_chat:2222", message_id=mid, sender_id=500 + mid)

    first = store.lead_candidates(since_id=0, limit=2)
    assert [it["id"] for it in first] == [1, 2]  # rowids 1,2 — ordered ASC
    assert first[0]["message_id"] == 10

    # Page forward using the last id seen; only rowid>since is returned.
    second = store.lead_candidates(since_id=first[-1]["id"], limit=2)
    assert [it["id"] for it in second] == [3]
    assert second[0]["message_id"] == 12

    # Cursor at the end returns nothing.
    assert store.lead_candidates(since_id=second[-1]["id"], limit=2) == []


def test_candidates_role_filter_excludes_non_lead(tmp_path):
    store = _lead_store(tmp_path)
    _seed_message(store, peer_key="linked_chat:2222", message_id=10, sender_id=501)
    _seed_message(store, peer_key="channel:9999", message_id=10, sender_id=502)  # digest role

    items = store.lead_candidates(since_id=0, limit=100)
    assert [it["peer_key"] for it in items] == ["linked_chat:2222"]
    assert [it["chat_title"] for it in items] == ["Lead Group"]


def test_candidates_excludes_null_sender(tmp_path):
    store = _lead_store(tmp_path)
    _seed_message(store, peer_key="linked_chat:2222", message_id=10, sender_id=None)  # excluded
    _seed_message(store, peer_key="linked_chat:2222", message_id=11, sender_id=777)

    items = store.lead_candidates(since_id=0, limit=100)
    assert [it["sender_id"] for it in items] == [777]
    assert items[0]["link"] == "https://t.me/c/2222/11"


# ── candidates: HTTP envelope ──────────────────────────────────────
@pytest.mark.asyncio
async def test_candidates_http_envelope_and_max_id(tmp_path, monkeypatch):
    store = _lead_store(tmp_path)
    for mid in (10, 11):
        _seed_message(store, peer_key="linked_chat:2222", message_id=mid, sender_id=600 + mid)

    proxy = tp.TelegramProxy()
    proxy._digest_store = store
    http = await _http_app(proxy, monkeypatch)
    try:
        headers = {"Authorization": "Bearer secret"}
        resp = await http.get("/v1/leads/candidates?since_id=0&limit=10", headers=headers)
        assert resp.status == 200
        body = await resp.json()
        assert body["count"] == 2
        assert body["max_id"] == 2  # max rowid returned
        assert [it["id"] for it in body["items"]] == [1, 2]
        assert body["items"][0]["link"] == "https://t.me/c/2222/10"

        # Empty page echoes since_id so the cursor never rewinds.
        resp = await http.get("/v1/leads/candidates?since_id=2", headers=headers)
        empty = await resp.json()
        assert empty == {"items": [], "max_id": 2, "count": 0}
    finally:
        await http.close()


@pytest.mark.asyncio
async def test_candidates_http_requires_auth(tmp_path, monkeypatch):
    proxy = tp.TelegramProxy()
    proxy._digest_store = _lead_store(tmp_path)
    http = await _http_app(proxy, monkeypatch)
    try:
        resp = await http.get("/v1/leads/candidates")
        assert resp.status == 401
    finally:
        await http.close()


# ── /v1/users: cache + FloodWait tolerance ─────────────────────────
class _CountingClient:
    def __init__(self, entity):
        self._entity = entity
        self.calls = 0

    async def get_entity(self, sender_id):  # noqa: ARG002
        self.calls += 1
        return self._entity


@pytest.mark.asyncio
async def test_users_cache_hit_avoids_second_get_entity(tmp_path, monkeypatch):
    store = TelegramDigestStore(db_path=tmp_path / "digest.db")
    entity = types.SimpleNamespace(
        username="jane", first_name="Jane", last_name="Doe", bot=False
    )
    client = _CountingClient(entity)
    proxy = tp.TelegramProxy()
    proxy._digest_store = store
    proxy._client = client

    http = await _http_app(proxy, monkeypatch)
    try:
        headers = {"Authorization": "Bearer secret"}
        first = await (await http.get("/v1/users/12345", headers=headers)).json()
        assert first == {
            "id": 12345,
            "sender_id": 12345,
            "kind": "user",
            "username": "jane",
            "title": "Jane Doe",
            "name": "Jane Doe",
            "is_bot": False,
            "cached": False,
            "error": None,
        }
        assert client.calls == 1

        # Second call is served from the cache — no new get_entity.
        second = await (await http.get("/v1/users/12345", headers=headers)).json()
        assert second["cached"] is True
        assert second["kind"] == "user"
        assert second["title"] == "Jane Doe"
        assert second["name"] == "Jane Doe"
        assert client.calls == 1
    finally:
        await http.close()


@pytest.mark.asyncio
async def test_users_floodwait_returns_200_with_error(tmp_path, monkeypatch):
    class _FloodClient:
        def __init__(self):
            self.calls = 0

        async def get_entity(self, sender_id):  # noqa: ARG002
            self.calls += 1
            raise FloodWaitError(request=None, capture=42)

    store = TelegramDigestStore(db_path=tmp_path / "digest.db")
    client = _FloodClient()
    proxy = tp.TelegramProxy()
    proxy._digest_store = store
    proxy._client = client

    http = await _http_app(proxy, monkeypatch)
    try:
        headers = {"Authorization": "Bearer secret"}
        resp = await http.get("/v1/users/777", headers=headers)
        assert resp.status == 200  # NOT a 500
        body = await resp.json()
        assert body == {
            "id": 777,
            "sender_id": 777,
            "kind": None,
            "username": None,
            "title": "",
            "name": "",
            "is_bot": False,
            "cached": False,
            "error": "FloodWaitError",
        }
        # The failure is not cached, so a later retry still tries the network.
        assert store.get_lead_sender(777) is None
        assert client.calls == 1
    finally:
        await http.close()


# ── /v1/users generalised to ANY entity: chat / channel / megagroup ─
@pytest.mark.asyncio
async def test_users_resolves_channel_megagroup(tmp_path, monkeypatch):
    """A megagroup (Telethon Channel with megagroup=True) resolves to kind
    'channel' with its title; a private group has username=None. The kind+title
    are cached so a re-scan is served from the db with no 2nd get_entity."""
    store = TelegramDigestStore(db_path=tmp_path / "digest.db")
    entity = types.SimpleNamespace(
        title="Private Lead Group", username=None, broadcast=False, megagroup=True
    )
    client = _CountingClient(entity)
    proxy = tp.TelegramProxy()
    proxy._digest_store = store
    proxy._client = client

    http = await _http_app(proxy, monkeypatch)
    try:
        headers = {"Authorization": "Bearer secret"}
        first = await (await http.get("/v1/users/1976968455", headers=headers)).json()
        assert first == {
            "id": 1976968455,
            "sender_id": 1976968455,
            "kind": "channel",
            "username": None,  # private megagroup has no public handle
            "title": "Private Lead Group",
            "name": "Private Lead Group",
            "is_bot": False,
            "cached": False,
            "error": None,
        }
        assert client.calls == 1

        # kind + title landed in the cache; a re-scan hits it (no new get_entity).
        cached = store.get_lead_sender(1976968455)
        assert cached["kind"] == "channel"
        assert cached["title"] == "Private Lead Group"

        second = await (await http.get("/v1/users/1976968455", headers=headers)).json()
        assert second["cached"] is True
        assert second["kind"] == "channel"
        assert second["title"] == "Private Lead Group"
        assert client.calls == 1
    finally:
        await http.close()


@pytest.mark.asyncio
async def test_users_resolves_public_broadcast_channel(tmp_path, monkeypatch):
    """A public broadcast channel keeps its @username and resolves kind='channel'."""
    store = TelegramDigestStore(db_path=tmp_path / "digest.db")
    entity = types.SimpleNamespace(
        title="Deals Channel", username="dealschan", broadcast=True, megagroup=False
    )
    proxy = tp.TelegramProxy()
    proxy._digest_store = store
    proxy._client = _CountingClient(entity)
    http = await _http_app(proxy, monkeypatch)
    try:
        body = await (
            await http.get("/v1/users/42", headers={"Authorization": "Bearer secret"})
        ).json()
        assert body["kind"] == "channel"
        assert body["username"] == "dealschan"
        assert body["title"] == "Deals Channel"
        assert body["is_bot"] is False
    finally:
        await http.close()


@pytest.mark.asyncio
async def test_users_resolves_basic_group_chat(tmp_path, monkeypatch):
    """A basic group (Telethon Chat: title, no broadcast/megagroup, no name)
    resolves to kind 'chat' with username=None."""
    store = TelegramDigestStore(db_path=tmp_path / "digest.db")
    entity = types.SimpleNamespace(title="Small Group")  # Chat has no username attr
    proxy = tp.TelegramProxy()
    proxy._digest_store = store
    proxy._client = _CountingClient(entity)
    http = await _http_app(proxy, monkeypatch)
    try:
        body = await (
            await http.get("/v1/users/555", headers={"Authorization": "Bearer secret"})
        ).json()
        assert body["kind"] == "chat"
        assert body["username"] is None
        assert body["title"] == "Small Group"
        assert body["cached"] is False
    finally:
        await http.close()


@pytest.mark.asyncio
async def test_users_marked_id_fallback_resolves_channel(tmp_path, monkeypatch):
    """A bare internal id that get_entity(id) cannot resolve is retried via its
    -100-marked PeerChannel form — the fallback path required for megagroups."""
    from telethon.tl.types import PeerChannel

    entity = types.SimpleNamespace(
        title="Marked Megagroup", username=None, broadcast=False, megagroup=True
    )

    class _MarkedClient:
        def __init__(self):
            self.calls = 0
            self.seen = []

        async def get_entity(self, ref):
            self.calls += 1
            self.seen.append(ref)
            # The bare positive id is not directly resolvable; the marked
            # PeerChannel form is.
            if isinstance(ref, PeerChannel):
                assert ref.channel_id == 1976968455
                return entity
            raise ValueError(f"Cannot find any entity corresponding to {ref!r}")

    store = TelegramDigestStore(db_path=tmp_path / "digest.db")
    client = _MarkedClient()
    proxy = tp.TelegramProxy()
    proxy._digest_store = store
    proxy._client = client
    http = await _http_app(proxy, monkeypatch)
    try:
        body = await (
            await http.get(
                "/v1/users/1976968455", headers={"Authorization": "Bearer secret"}
            )
        ).json()
        assert body["kind"] == "channel"
        assert body["title"] == "Marked Megagroup"
        assert body["error"] is None
        # Tier 1 (bare id) then tier 2 (PeerChannel) — exactly two attempts.
        assert client.calls == 2
        assert isinstance(client.seen[1], PeerChannel)
    finally:
        await http.close()


@pytest.mark.asyncio
async def test_users_bad_id_is_400(tmp_path, monkeypatch):
    proxy = tp.TelegramProxy()
    proxy._digest_store = TelegramDigestStore(db_path=tmp_path / "digest.db")
    http = await _http_app(proxy, monkeypatch)
    try:
        resp = await http.get("/v1/users/not-an-int", headers={"Authorization": "Bearer secret"})
        assert resp.status == 400
    finally:
        await http.close()
