"""tests/test_telegram_aggregator.py"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from src.telegram_aggregator import AGG_ROLE, collect, load_file_env, parse_sources, resolve_paths
from src.telegram_digest import TelegramDigestStore


def test_resolve_paths_defaults(monkeypatch, tmp_path):
    monkeypatch.setenv("AGGREGATOR_STATE_DIR", str(tmp_path / "agg"))
    paths = resolve_paths()
    assert paths.state_dir == tmp_path / "agg"
    assert paths.db_path == tmp_path / "agg" / "aggregator.db"
    assert paths.sources_path == tmp_path / "agg" / "sources.txt"
    assert paths.drafts_dir == tmp_path / "agg" / "drafts"
    assert paths.drafts_dir.is_dir()  # created


def test_resolve_paths_sources_override(monkeypatch, tmp_path):
    monkeypatch.setenv("AGGREGATOR_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("AGGREGATOR_SOURCES_PATH", str(tmp_path / "x" / "list.txt"))
    assert resolve_paths().sources_path == tmp_path / "x" / "list.txt"


def test_load_file_env_reads_files(monkeypatch, tmp_path):
    secret = tmp_path / "k"
    secret.write_text("sekret-value\n")
    monkeypatch.delenv("TELEGRAM_PROXY_API_KEY", raising=False)
    monkeypatch.setenv("TELEGRAM_PROXY_API_KEY_FILE", str(secret))
    env: dict[str, str] = dict(os.environ)
    load_file_env(env)
    assert env["TELEGRAM_PROXY_API_KEY"] == "sekret-value"


def test_load_file_env_never_overwrites_existing(monkeypatch, tmp_path):
    secret = tmp_path / "k"
    secret.write_text("from-file")
    env = {"TELEGRAM_PROXY_API_KEY": "already", "TELEGRAM_PROXY_API_KEY_FILE": str(secret)}
    load_file_env(env)
    assert env["TELEGRAM_PROXY_API_KEY"] == "already"


def test_load_file_env_missing_file_is_noop(tmp_path):
    env = {"TELEGRAM_AGGREGATOR_BOT_TOKEN_FILE": str(tmp_path / "absent")}
    load_file_env(env)
    assert "TELEGRAM_AGGREGATOR_BOT_TOKEN" not in env


def test_parse_sources_formats_and_dedup():
    text = """
    # AI channels
    @data_secrets
    https://t.me/neuraldeep
    t.me/llm_under_hood
    data_secrets

    https://t.me/+privateHashAAA   # invite links are NOT usernames -> skipped
    """
    assert parse_sources(text) == ["data_secrets", "neuraldeep", "llm_under_hood"]


class FakeChannel:
    def __init__(self, entity_id, username, title="T"):
        self.entity_id = entity_id
        self.username = username
        self.title = title
        self.linked_chat_id = None
        self.linked_chat_title = None
        self.linked_chat_username = None


class FakeProxyClient:
    def __init__(self, channels, messages_by_entity):
        self._channels = channels
        self._messages = messages_by_entity
        self.read_calls = []

    async def list_channels(self, *, limit):
        return self._channels

    async def read_messages(self, *, kind, entity_id, min_id, limit, recent_first=False):
        self.read_calls.append((kind, entity_id, min_id))
        if isinstance(self._messages.get(entity_id), Exception):
            raise self._messages[entity_id]
        return [m for m in self._messages.get(entity_id, []) if m["message_id"] > min_id]


def _msg(mid, text="hello world", link=None, views=10):
    return {
        "message_id": mid,
        "posted_at": "2026-07-14T10:00:00+00:00",
        "sender_id": None,
        "views": views,
        "forwards": 1,
        "replies": None,
        "link": link or f"https://t.me/chan/{mid}",
        "text": text,
        "raw_json": {},
    }


async def test_collect_resolves_and_ingests(tmp_path):
    store = TelegramDigestStore(tmp_path / "agg.db")
    client = FakeProxyClient(
        channels=[FakeChannel(111, "data_secrets"), FakeChannel(222, "neuraldeep")],
        messages_by_entity={111: [_msg(1), _msg(2)], 222: [_msg(5)]},
    )
    result = await collect(client, store, ["data_secrets", "neuraldeep", "missing_chan"])
    assert result["resolved"] == 2
    assert result["unresolved"] == ["missing_chan"]
    assert result["collected_messages"] == 3
    sources = store.list_sources(roles=(AGG_ROLE,))
    assert {s.entity_id for s in sources} == {111, 222}


async def test_collect_is_incremental(tmp_path):
    store = TelegramDigestStore(tmp_path / "agg.db")
    client = FakeProxyClient(
        channels=[FakeChannel(111, "data_secrets")],
        messages_by_entity={111: [_msg(1), _msg(2)]},
    )
    await collect(client, store, ["data_secrets"])
    result2 = await collect(client, store, ["data_secrets"])
    assert result2["collected_messages"] == 0
    # second pass asked the proxy only for messages newer than the watermark
    assert client.read_calls[-1][2] == 2


async def test_collect_isolates_per_source_failures(tmp_path):
    store = TelegramDigestStore(tmp_path / "agg.db")
    client = FakeProxyClient(
        channels=[FakeChannel(111, "ok_chan"), FakeChannel(222, "broken_chan")],
        messages_by_entity={111: [_msg(1)], 222: RuntimeError("FLOOD_WAIT")},
    )
    result = await collect(client, store, ["ok_chan", "broken_chan"])
    assert result["collected_messages"] == 1
    assert result["failed_sources"] == 1
