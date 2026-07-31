"""Correction 1 — in production the scanner was BLIND to what is already enrolled.

Moving the durable enrolment write onto the proxy dropped the unit's access to
/var/lib/iron-lady (0700 iron-lady), and `resolve_scan_paths` then defaulted
join_db/digest_db to None. `load_tracked` read those two ledgers by opening the
files directly, so in production it returned EMPTY lead/join sets and silently
lost the answer to "is this already enrolled?" — the live dry-run went from 18
new of 115 to 65 new of 116, i.e. ~47 already-tracked entities re-proposed. The
write itself is idempotent, but the report claimed enrolments that were no-ops,
the 80-per-run topical reader burned its budget re-scoring tracked entities, and
chat_sources got staged for chats the pipeline already had.

The proxy is the durable authority for the WRITE, so it is now the authority for
the corresponding READ: `GET /v1/sources/tracked`. These tests pin

  * the endpoint: same auth as the write route, read-only, and exposing NOTHING
    beyond what an enrolment decision needs;
  * the wiring: load_tracked consumes that payload, and the direct db paths stay
    as the test/override path;
  * the safety invariant the previous repair established, which matters MORE now:
    a tracked-state read that FAILS blocks every write and is reported — while a
    read that succeeds and legitimately returns nothing does not. The two must
    stay distinguishable, and "no tracked-state source configured at all" is a
    failure, not an empty ledger (that is exactly the bug above).
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from aiohttp import web

from src.telegram_digest import LEAD_SOURCE_ROLE, TelegramDigestStore, _peer_key
from src.telegram_dialog_scan import ScanPaths, load_tracked, run_scan
from src.telegram_proxy import JoinStore, TelegramProxy, _tracked_sources

_TOPICAL_POST = "Новая модель OpenAI ускоряет инференс агентов в проде на 40 процентов"


class _Request:
    """GET has no body — auth + app are the whole surface."""

    def __init__(self, app, *, token="k"):
        self.app = app
        self.headers = {"Authorization": f"Bearer {token}"} if token else {}
        self.query: dict[str, str] = {}


class _Proxy(TelegramProxy):
    """The REAL proxy method with only the two stores swapped for temp dbs."""

    def __init__(self, digest_db: Path, join_db: Path):  # noqa: D107 — no Telethon start
        self._digest_store = TelegramDigestStore(digest_db)
        self._join_store = JoinStore(join_db)

    def _get_digest_store(self):
        return self._digest_store

    def _get_join_store(self):
        return self._join_store


@pytest.fixture()
def proxy(tmp_path, monkeypatch):
    from src import config

    monkeypatch.setattr(config, "TELEGRAM_PROXY_API_KEY", "k")
    p = _Proxy(tmp_path / "digest.db", tmp_path / "join.db")
    p._digest_store.add_source_role(
        peer_key=_peer_key("channel", 7), entity_id=7, title="Secret internal title",
        username="feed", kind="channel", linked_channel_key=None, role="news",
    )
    p._digest_store.add_source_role(
        peer_key=_peer_key("linked_chat", 8), entity_id=8, title="Chat",
        username="feedchat", kind="linked_chat", linked_channel_key=None, role=LEAD_SOURCE_ROLE,
    )
    p._join_store.record_existing_membership("feedchat", "public", 8)
    return p


def _get(proxy, *, token="k") -> dict:
    return json.loads(asyncio.run(_tracked_sources(_Request({"proxy": proxy}, token=token))).text)


def _paths(tmp_path: Path, **over) -> ScanPaths:
    base = dict(
        sources=tmp_path / "sources.txt",
        chat_sources=tmp_path / "chat_sources.txt",
        mirror=tmp_path / "mirror.txt",
        state=tmp_path / "state.json",
        deny=tmp_path / "deny.txt",
        topics=tmp_path / "topics.txt",
        lock=tmp_path / "scan.lock",
    )
    base.update(over)
    return ScanPaths(**base)


def _dialog(entity_id: int, username: str) -> dict:
    return {
        "kind": "channel", "entity_id": entity_id, "username": username,
        "usernames": [username], "title": username, "is_broadcast": True,
        "linked_chat_id": None, "linked_chat_lookup": "ok",
    }


def _reader(_kind, _entity_id):
    return [_TOPICAL_POST] * 20


# ── the endpoint ──────────────────────────────────────────────────


def test_tracked_endpoint_rejects_a_missing_or_wrong_bearer(proxy):
    """Same auth as the write route it mirrors — no anonymous read of the ledger."""
    with pytest.raises(web.HTTPUnauthorized):
        _get(proxy, token="nope")
    with pytest.raises(web.HTTPUnauthorized):
        _get(proxy, token=None)


def test_tracked_endpoint_returns_exactly_what_an_enrolment_decision_needs(proxy):
    payload = _get(proxy)
    assert {s["peer_key"] for s in payload["digest_sources"]} == {"channel:7", "linked_chat:8"}
    by_key = {s["peer_key"]: s for s in payload["digest_sources"]}
    assert by_key["channel:7"]["entity_id"] == 7
    assert LEAD_SOURCE_ROLE in by_key["linked_chat:8"]["role"]
    assert payload["joins"] == ["feedchat"]
    # NOTHING beyond the decision: no titles, no usernames, no message content.
    assert all(set(s) == {"peer_key", "entity_id", "role"} for s in payload["digest_sources"])
    assert "Secret internal title" not in json.dumps(payload, ensure_ascii=False)


def test_tracked_endpoint_reports_a_store_failure_as_a_gateway_error(proxy, monkeypatch):
    """Mirrors the write route: an unexpected store failure is 502, never a 200
    with an empty body — an empty answer means 'nothing tracked' downstream."""
    monkeypatch.setattr(
        proxy._digest_store, "source_roles", lambda: (_ for _ in ()).throw(RuntimeError("db is locked"))
    )
    with pytest.raises(web.HTTPBadGateway):
        _get(proxy)


# ── the wiring ────────────────────────────────────────────────────


def test_load_tracked_reads_the_pipeline_state_over_the_reader(tmp_path, proxy):
    """The wire payload itself, not a hand-made dict, drives the tracked sets."""
    payload = _get(proxy)
    tracked = load_tracked(_paths(tmp_path), tracked_reader=lambda: payload)
    assert tracked.errors == []
    assert tracked.digest_peer_keys == {"channel:7", "linked_chat:8"}
    assert tracked.lead_entity_ids == {8}  # only the LEAD-role row
    assert tracked.join_targets == {"feedchat"}


def test_an_already_enrolled_entity_is_no_longer_reproposed(tmp_path, proxy):
    """The 47 re-proposals were a LEADS re-proposal, and they stay fixed.

    PER-SURFACE (2026-07-31): entity 8 is settled for leads and STILL a news
    candidate — it had never been scored for the public digest, and the global
    short-circuit that hid it there is exactly the defect this repair removes."""
    from src.telegram_dialog_scan import outstanding_surfaces

    payload = _get(proxy)
    dialogs = [_dialog(8, "feedchat"), _dialog(9, "brandnew")]
    paths = _paths(tmp_path)
    report = run_scan(
        paths=paths, dialogs=dialogs, dry_run=True,
        post_reader=_reader, tracked_reader=lambda: payload,
    )
    tracked = load_tracked(paths, tracked_reader=lambda: payload)
    assert outstanding_surfaces(dialogs[0], tracked) == {"news"}, "leads already has it"
    assert outstanding_surfaces(dialogs[1], tracked) == {"leads", "news"}
    assert report.new_dialogs == [8, 9]

    blind = run_scan(paths=_paths(tmp_path), dialogs=dialogs, dry_run=True, post_reader=_reader,
                     tracked_reader=lambda: {"digest_sources": [], "joins": []})
    assert blind.new_dialogs == [8, 9]  # what production was doing every night


def test_direct_db_paths_still_win_as_the_test_override(tmp_path, proxy):
    def explode():
        raise AssertionError("the reader must not be consulted when a db path is configured")

    paths = _paths(tmp_path, digest_db=Path(proxy._digest_store._db_path),
                   join_db=Path(proxy._join_store._db_path))
    tracked = load_tracked(paths, tracked_reader=explode)
    assert tracked.errors == []
    assert tracked.lead_entity_ids == {8}
    assert tracked.join_targets == {"feedchat"}


# ── the safety invariant ──────────────────────────────────────────


def test_a_failed_tracked_read_blocks_every_write_and_is_reported(tmp_path):
    def boom():
        raise RuntimeError("HTTP 503 from /v1/sources/tracked")

    calls: list[dict] = []
    report = run_scan(
        paths=_paths(tmp_path), dialogs=[_dialog(9, "brandnew")], dry_run=False,
        post_reader=_reader, tracked_reader=boom,
        lead_enroller=lambda **kw: calls.append(kw) or {"ok": True},
    )
    assert report.wrote_nothing is True
    assert calls == []
    assert not (tmp_path / "sources.txt").exists()
    assert any("HTTP 503" in e for e in report.errors)
    assert any("refusing to write" in e for e in report.errors)


def test_an_empty_tracked_read_is_not_a_failed_one(tmp_path):
    """A pipeline with nothing enrolled yet must still be able to enroll."""
    calls: list[dict] = []
    report = run_scan(
        paths=_paths(tmp_path), dialogs=[_dialog(9, "brandnew")], dry_run=False,
        post_reader=_reader, tracked_reader=lambda: {"digest_sources": [], "joins": []},
        lead_enroller=lambda **kw: calls.append(kw) or {"ok": True, "created": True},
    )
    assert report.errors == []
    assert report.wrote_nothing is False
    assert [c["entity_id"] for c in calls] == [9]


def test_no_tracked_state_source_at_all_is_a_FAILURE_not_an_empty_ledger(tmp_path):
    """THE production bug: unset db paths + no reader used to read as 'nothing is
    tracked', which re-proposed every already-enrolled source."""
    tracked = load_tracked(_paths(tmp_path))
    assert tracked.errors, "a scanner with no way to read tracked state must NOT proceed"
    calls: list[dict] = []
    report = run_scan(
        paths=_paths(tmp_path), dialogs=[_dialog(9, "brandnew")], dry_run=False,
        post_reader=_reader, lead_enroller=lambda **kw: calls.append(kw) or {"ok": True},
    )
    assert report.wrote_nothing is True
    assert calls == []


def test_a_malformed_tracked_payload_is_a_failure_too(tmp_path):
    for payload in ({"digest_sources": "not-a-list", "joins": []}, ["nope"], {"joins": 5}):
        tracked = load_tracked(_paths(tmp_path), tracked_reader=lambda p=payload: p)
        assert tracked.errors, f"{payload!r} must not read as an empty ledger"
