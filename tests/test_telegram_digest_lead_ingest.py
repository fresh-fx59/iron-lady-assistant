"""Tests for M2 lead-group ingestion into the Telegram digest store.

Covers the role split (digest vs lead), the JOIN-store → sources sync, the
role-filtered incremental collect, and per-source error isolation.
"""

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from src.telegram_digest import (
    TelegramDigestStore,
    collect_digest,
    sync_joined_sources,
)


def _make_legacy_digest_db(path) -> None:
    """Create a digest db with the PRE-role schema and seed a source + message."""
    con = sqlite3.connect(path)
    con.execute(
        """
        CREATE TABLE digest_sources (
            peer_key TEXT PRIMARY KEY,
            entity_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            username TEXT,
            kind TEXT NOT NULL,
            linked_channel_key TEXT,
            last_collected_message_id INTEGER,
            last_collected_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE digest_messages (
            peer_key TEXT NOT NULL,
            message_id INTEGER NOT NULL,
            posted_at TEXT NOT NULL,
            sender_id INTEGER,
            views INTEGER,
            forwards INTEGER,
            replies INTEGER,
            link TEXT,
            text TEXT,
            raw_json TEXT,
            PRIMARY KEY(peer_key, message_id)
        )
        """
    )
    now = datetime.now(timezone.utc).isoformat()
    con.execute(
        """
        INSERT INTO digest_sources
        (peer_key, entity_id, title, username, kind, linked_channel_key,
         last_collected_message_id, last_collected_at, created_at, updated_at)
        VALUES ('channel:1', 1, 'Legacy Channel', 'legacy', 'channel', NULL,
                42, ?, ?, ?)
        """,
        (now, now, now),
    )
    con.execute(
        """
        INSERT INTO digest_messages
        (peer_key, message_id, posted_at, sender_id, views, forwards, replies, link, text, raw_json)
        VALUES ('channel:1', 42, ?, 5, 10, 1, 0, NULL, 'legacy body', '{}')
        """,
        (now,),
    )
    con.commit()
    con.close()


def _make_join_db(path, rows) -> None:
    """Create a minimal JOIN store (like the proxy join loop's) and seed rows."""
    con = sqlite3.connect(path)
    con.execute(
        """
        CREATE TABLE joins (
            target TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            status TEXT NOT NULL,
            entity_id INTEGER,
            error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            joined_at TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            retry_at TEXT
        )
        """
    )
    now = datetime.now(timezone.utc).isoformat()
    for target, status, entity_id in rows:
        con.execute(
            """
            INSERT INTO joins(target, kind, status, entity_id, created_at, updated_at)
            VALUES (?, 'group', ?, ?, ?, ?)
            """,
            (target, status, entity_id, now, now),
        )
    con.commit()
    con.close()


# ── (a) additive role migration ────────────────────────────────────────────
def test_role_migration_is_additive_on_existing_db(tmp_path) -> None:
    db_path = tmp_path / "digest.db"
    _make_legacy_digest_db(db_path)

    # Instantiating the store runs the additive migration in-place.
    store = TelegramDigestStore(db_path)

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    cols = {row["name"] for row in con.execute("PRAGMA table_info(digest_sources)").fetchall()}
    assert "role" in cols

    # Existing row survives and defaults to role='digest'; no data loss.
    row = con.execute(
        "SELECT title, entity_id, last_collected_message_id, role FROM digest_sources WHERE peer_key='channel:1'"
    ).fetchone()
    assert row["title"] == "Legacy Channel"
    assert row["entity_id"] == 1
    assert row["last_collected_message_id"] == 42
    assert row["role"] == "digest"

    msg = con.execute(
        "SELECT text FROM digest_messages WHERE peer_key='channel:1' AND message_id=42"
    ).fetchone()
    assert msg["text"] == "legacy body"
    con.close()

    # And the legacy row is still listed as a digest source by the API.
    assert [s.peer_key for s in store.list_sources(roles=("digest",))] == ["channel:1"]


# ── (b) sync_joined_sources ─────────────────────────────────────────────────
def test_sync_joined_sources_upserts_leads_and_is_idempotent(tmp_path) -> None:
    db_path = tmp_path / "digest.db"
    join_path = tmp_path / "telegram_join.db"
    store = TelegramDigestStore(db_path)
    _make_join_db(
        join_path,
        rows=[
            ("@lead_alpha", "joined", 111),
            ("@lead_beta", "joined", 222),
            ("@pending_gamma", "pending", None),   # not joined → excluded
            ("@joined_no_id", "joined", None),      # joined but no entity_id → excluded
        ],
    )

    synced = sync_joined_sources(store, join_path)
    assert synced == 2

    leads = {s.peer_key: s for s in store.list_sources(roles=("lead",))}
    assert set(leads) == {"linked_chat:111", "linked_chat:222"}
    assert leads["linked_chat:111"].kind == "linked_chat"
    assert leads["linked_chat:111"].role == "lead"
    assert leads["linked_chat:111"].title == "@lead_alpha"
    assert leads["linked_chat:111"].entity_id == 111

    # Idempotent: re-running adds nothing new.
    synced_again = sync_joined_sources(store, join_path)
    assert synced_again == 2
    assert len(store.list_sources(roles=("lead",))) == 2

    # Digest sources are untouched by the sync.
    assert store.list_sources(roles=("digest",)) == []


def test_sync_joined_sources_missing_db_is_noop(tmp_path) -> None:
    store = TelegramDigestStore(tmp_path / "digest.db")
    missing = tmp_path / "does_not_exist.db"
    assert sync_joined_sources(store, missing) == 0
    assert not missing.exists()  # must NOT create a stray file
    assert store.list_sources() == []


# ── (c) role-filtered collect reads only lead sources ───────────────────────
@pytest.mark.asyncio
async def test_collect_lead_reads_only_lead_sources(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "digest.db"
    store = TelegramDigestStore(db_path)
    store.upsert_source(
        peer_key="channel:1", entity_id=1, title="Digest Chan",
        username=None, kind="channel", linked_channel_key=None, role="digest",
    )
    store.upsert_source(
        peer_key="linked_chat:900", entity_id=900, title="Lead Group",
        username=None, kind="linked_chat", linked_channel_key=None, role="lead",
    )

    read_entities: list[int] = []
    now = datetime.now(timezone.utc)

    class FakeProxyClient:
        async def list_channels(self, *, limit):  # noqa: ARG002
            raise AssertionError("list_channels must not run for a lead collect")

        async def read_messages(self, *, kind, entity_id, min_id, limit, recent_first=False):  # noqa: ARG002
            read_entities.append(entity_id)
            return [{
                "message_id": 7001,
                "posted_at": (now - timedelta(minutes=3)).isoformat(),
                "sender_id": 55, "views": None, "forwards": None, "replies": 1,
                "link": None, "text": "lead chatter", "raw_json": {"id": 7001},
            }]

    monkeypatch.setattr("src.telegram_digest.TelegramProxyClient", FakeProxyClient)

    payload = await collect_digest(
        db_path=db_path,
        brief_path=tmp_path / "brief.md",
        roles=["lead"],
        join_db_path=tmp_path / "no_join.db",  # missing → sync no-ops
        collect_limit=10,
    )

    assert payload["status"] == "ok"
    assert payload["payload"]["roles"] == ["lead"]
    # ONLY the lead entity was read; the digest channel was never touched.
    assert read_entities == [900]

    con = sqlite3.connect(db_path)
    peers = [r[0] for r in con.execute(
        "SELECT DISTINCT peer_key FROM digest_messages"
    ).fetchall()]
    con.close()
    assert peers == ["linked_chat:900"]


# ── (d) per-source error isolation ──────────────────────────────────────────
@pytest.mark.asyncio
async def test_collect_isolates_per_source_errors(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "digest.db"
    store = TelegramDigestStore(db_path)
    store.upsert_source(
        peer_key="linked_chat:100", entity_id=100, title="Bad Lead",
        username=None, kind="linked_chat", linked_channel_key=None, role="lead",
    )
    store.upsert_source(
        peer_key="linked_chat:200", entity_id=200, title="Good Lead",
        username=None, kind="linked_chat", linked_channel_key=None, role="lead",
    )
    now = datetime.now(timezone.utc)

    class FloodyProxyClient:
        async def read_messages(self, *, kind, entity_id, min_id, limit, recent_first=False):  # noqa: ARG002
            if entity_id == 100:
                raise RuntimeError("FloodWaitError: retry in 300s")
            return [{
                "message_id": 5005,
                "posted_at": (now - timedelta(minutes=1)).isoformat(),
                "sender_id": 9, "views": None, "forwards": None, "replies": 0,
                "link": None, "text": "good message", "raw_json": {"id": 5005},
            }]

    monkeypatch.setattr("src.telegram_digest.TelegramProxyClient", FloodyProxyClient)

    payload = await collect_digest(
        db_path=db_path,
        brief_path=tmp_path / "brief.md",
        roles=["lead"],
        join_db_path=tmp_path / "no_join.db",
        collect_limit=10,
    )

    # The pass completed despite the FloodWait on one source.
    assert payload["status"] == "ok"
    assert payload["payload"]["failed_sources"] == 1
    assert payload["payload"]["collected_messages"] == 1

    # Good source: message persisted + watermark advanced.
    assert store.last_message_id("linked_chat:200") == 5005
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    good = con.execute(
        "SELECT last_collected_message_id FROM digest_sources WHERE peer_key='linked_chat:200'"
    ).fetchone()
    assert good["last_collected_message_id"] == 5005

    # Bad source: nothing persisted, watermark untouched → retried next pass.
    assert store.last_message_id("linked_chat:100") == 0
    bad = con.execute(
        "SELECT last_collected_message_id FROM digest_sources WHERE peer_key='linked_chat:100'"
    ).fetchone()
    assert bad["last_collected_message_id"] is None
    con.close()


# ── (e) render_briefing stays digest-only ───────────────────────────────────
def test_render_briefing_shows_only_digest_role(tmp_path) -> None:
    store = TelegramDigestStore(tmp_path / "digest.db")
    store.upsert_source(
        peer_key="channel:1", entity_id=1, title="Digest Source",
        username=None, kind="channel", linked_channel_key=None, role="digest",
    )
    store.upsert_source(
        peer_key="linked_chat:900", entity_id=900, title="Secret Lead Group",
        username=None, kind="linked_chat", linked_channel_key=None, role="lead",
    )
    now = datetime.now(timezone.utc)
    store.insert_message(
        peer_key="channel:1", message_id=1, posted_at=now - timedelta(hours=1),
        sender_id=1, views=1, forwards=0, replies=0, link=None,
        text="digest visible line", raw_json={"id": 1},
    )
    store.insert_message(
        peer_key="linked_chat:900", message_id=2, posted_at=now - timedelta(hours=1),
        sender_id=2, views=None, forwards=None, replies=3, link=None,
        text="lead private line", raw_json={"id": 2},
    )

    briefing = store.render_briefing(window_hours=24)

    assert "Digest Source" in briefing
    assert "digest visible line" in briefing
    assert "Secret Lead Group" not in briefing
    assert "lead private line" not in briefing
