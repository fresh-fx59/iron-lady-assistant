"""Recency-floor for the lead-candidate feed.

Regression + spec for the 2026-07-24 fix: the scorer was crawling the lead feed
oldest-first by a rowid cursor and could never reach the fresh tail once the feed
grew a large historical backlog (every joined group's back-catalogue), so every
run's page was months-old messages that the recency gate dropped wholesale
(``fetched=2000 dropped_old=2000 survivors=0`` for 8 days).

The fix: ``lead_candidates`` accepts an optional ``since_ts`` floor so the DB skips
the ancient backlog and returns only messages at/after the floor — regardless of
how far behind the rowid cursor is. The exact recency decision stays in the
scorer's Python gate; this floor only has to shed the bulk backlog.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

import src.telegram_proxy as tp
from src.telegram_digest import LEAD_SOURCE_ROLE, TelegramDigestStore


def _seed_source(store, *, peer_key, entity_id, title, kind, role):
    store.upsert_source(
        peer_key=peer_key, entity_id=entity_id, title=title, username=None,
        kind=kind, linked_channel_key=None, role=role,
    )


def _seed_message_at(store, *, peer_key, message_id, sender_id, posted_at, text="hi"):
    store.insert_message(
        peer_key=peer_key, message_id=message_id, posted_at=posted_at,
        sender_id=sender_id, views=None, forwards=None, replies=None,
        link=None, text=text, raw_json={},
    )


def _lead_store(tmp_path):
    store = TelegramDigestStore(db_path=tmp_path / "digest.db")
    _seed_source(store, peer_key="linked_chat:2222", entity_id=2222,
                 title="Lead Group", kind="linked_chat", role=LEAD_SOURCE_ROLE)
    return store


# The backlog message is inserted FIRST (low rowid) and is months old; the fresh
# message is inserted AFTER (higher rowid) and is recent. A rowid-only cursor at 0
# returns both; a recency floor between them must return ONLY the fresh one.
_OLD = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
_NEW = datetime(2026, 7, 24, 9, 0, tzinfo=timezone.utc)
_FLOOR = datetime(2026, 7, 22, 0, 0, tzinfo=timezone.utc).isoformat()  # between OLD and NEW


def test_since_ts_floor_skips_old_backlog(tmp_path):
    store = _lead_store(tmp_path)
    _seed_message_at(store, peer_key="linked_chat:2222", message_id=10,
                     sender_id=501, posted_at=_OLD)   # rowid 1, months old
    _seed_message_at(store, peer_key="linked_chat:2222", message_id=11,
                     sender_id=502, posted_at=_NEW)   # rowid 2, fresh

    # Cursor at 0 would return BOTH by rowid; the floor must drop the backlog row.
    items = store.lead_candidates(since_id=0, since_ts=_FLOOR, limit=100)
    assert [it["message_id"] for it in items] == [11]
    assert items[0]["sender_id"] == 502


def test_since_ts_none_is_unfiltered_backcompat(tmp_path):
    store = _lead_store(tmp_path)
    _seed_message_at(store, peer_key="linked_chat:2222", message_id=10,
                     sender_id=501, posted_at=_OLD)
    _seed_message_at(store, peer_key="linked_chat:2222", message_id=11,
                     sender_id=502, posted_at=_NEW)

    # No floor → unchanged behaviour: both rows, rowid ASC.
    items = store.lead_candidates(since_id=0, limit=100)
    assert [it["message_id"] for it in items] == [10, 11]


def test_since_ts_composes_with_since_id(tmp_path):
    """The floor and the rowid cursor are ANDed: only fresh rows past the cursor."""
    store = _lead_store(tmp_path)
    _seed_message_at(store, peer_key="linked_chat:2222", message_id=10,
                     sender_id=501, posted_at=_NEW)   # rowid 1, fresh
    _seed_message_at(store, peer_key="linked_chat:2222", message_id=11,
                     sender_id=502, posted_at=_NEW)   # rowid 2, fresh

    items = store.lead_candidates(since_id=1, since_ts=_FLOOR, limit=100)
    assert [it["id"] for it in items] == [2]


@pytest.mark.asyncio
async def test_http_since_ts_passthrough(tmp_path, monkeypatch):
    from aiohttp.test_utils import TestClient, TestServer
    import src.config as cfg

    store = _lead_store(tmp_path)
    _seed_message_at(store, peer_key="linked_chat:2222", message_id=10,
                     sender_id=501, posted_at=_OLD)
    _seed_message_at(store, peer_key="linked_chat:2222", message_id=11,
                     sender_id=502, posted_at=_NEW)

    monkeypatch.setattr(cfg, "TELEGRAM_PROXY_API_KEY", "secret", raising=False)
    app = tp.create_app()
    app.on_startup.clear()
    app.on_cleanup.clear()
    proxy = tp.TelegramProxy()
    proxy._digest_store = store
    app["proxy"] = proxy
    server = TestServer(app)
    http = TestClient(server)
    await http.start_server()
    try:
        headers = {"Authorization": "Bearer secret"}
        resp = await http.get(
            f"/v1/leads/candidates?since_id=0&since_ts={_FLOOR}&limit=10", headers=headers)
        assert resp.status == 200
        body = await resp.json()
        assert [it["message_id"] for it in body["items"]] == [11]
        assert body["count"] == 1
    finally:
        await http.close()
