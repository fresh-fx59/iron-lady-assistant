import sqlite3
import types
from datetime import datetime, timedelta, timezone

import pytest

from src.telegram_digest import collect_digest
from src.telegram_digest import TelegramDigestStore


def test_render_briefing_groups_recent_channel_and_linked_chat_messages(tmp_path) -> None:
    store = TelegramDigestStore(tmp_path / "telegram-digest.db")
    store.upsert_source(
        peer_key="channel:1",
        entity_id=1,
        title="Channel One",
        username="channel_one",
        kind="channel",
        linked_channel_key=None,
    )
    store.upsert_source(
        peer_key="linked_chat:2",
        entity_id=2,
        title="Channel One Chat",
        username=None,
        kind="linked_chat",
        linked_channel_key="channel:1",
    )
    now = datetime.now(timezone.utc)
    store.insert_message(
        peer_key="channel:1",
        message_id=101,
        posted_at=now - timedelta(hours=2),
        sender_id=10,
        views=123,
        forwards=4,
        replies=2,
        link="https://t.me/channel_one/101",
        text="Important channel update",
        raw_json={"id": 101},
    )
    store.insert_message(
        peer_key="linked_chat:2",
        message_id=55,
        posted_at=now - timedelta(hours=1),
        sender_id=20,
        views=None,
        forwards=None,
        replies=7,
        link=None,
        text="Discussion about the channel update",
        raw_json={"id": 55},
    )

    briefing = store.render_briefing(window_hours=24, per_source_limit=4, source_limit=10)

    assert "# Telegram digest briefing" in briefing
    assert "## Channel One [channel]" in briefing
    assert "## Channel One Chat [linked_chat]" in briefing
    assert "Important channel update" in briefing
    assert "Discussion about the channel update" in briefing


@pytest.mark.asyncio
async def test_collect_digest_via_proxy_ingests_channel_and_linked_chat_messages(
    tmp_path,
    monkeypatch,
) -> None:
    class FakeProxyClient:
        async def list_channels(self, *, limit: int):  # noqa: ARG002
            return [
                types.SimpleNamespace(
                    entity_id=100,
                    title="Main Channel",
                    username="main_channel",
                    linked_chat_id=200,
                    linked_chat_title="Main Channel Chat",
                    linked_chat_username=None,
                )
            ]

        async def read_messages(self, *, kind: str, entity_id: int, min_id: int, limit: int):  # noqa: ARG002
            if kind == "channel" and entity_id == 100:
                return [
                    {
                        "message_id": 101,
                        "posted_at": "2026-03-12T08:00:00+00:00",
                        "sender_id": 10,
                        "views": 123,
                        "forwards": 4,
                        "replies": 2,
                        "link": "https://t.me/main_channel/101",
                        "text": "Proxy channel update",
                        "raw_json": {"id": 101},
                    }
                ]
            if kind == "linked_chat" and entity_id == 200:
                return [
                    {
                        "message_id": 55,
                        "posted_at": "2026-03-12T08:05:00+00:00",
                        "sender_id": 20,
                        "views": None,
                        "forwards": None,
                        "replies": 7,
                        "link": None,
                        "text": "Proxy linked chat discussion",
                        "raw_json": {"id": 55},
                    }
                ]
            return []

    monkeypatch.setattr("src.telegram_digest.TelegramProxyClient", FakeProxyClient)

    db_path = tmp_path / "digest.db"
    brief_path = tmp_path / "brief.md"
    payload = await collect_digest(db_path=db_path, brief_path=brief_path, source_limit=10, collect_limit=10)

    assert payload["status"] == "ok"
    assert payload["payload"]["transport"] == "telegram_proxy"
    con = sqlite3.connect(db_path)
    row = con.execute(
        "SELECT linked_channel_key FROM digest_sources WHERE peer_key = 'linked_chat:200'"
    ).fetchone()
    assert row is not None
    assert row[0] == "channel:100"
    brief = brief_path.read_text()
    assert "Proxy channel update" in brief
    assert "Proxy linked chat discussion" in brief
