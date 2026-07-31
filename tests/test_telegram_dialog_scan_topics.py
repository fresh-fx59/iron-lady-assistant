"""Change 1 — the TOPICAL GATE, and Change 2 — lead enrolment over the proxy API.

Change 1 pins the deterministic topical scorer that replaced default-allow:
vocabulary parsing (RU stems + EN + word:/re: escape hatches), the metric
(hits / scoreable posts over the N most recent posts), the thin-evidence and
read-failure quarantines, the news/leads scope split, pacing + per-entity read
caching, and the score reaching the operator's report.

Change 2 pins the enrolment path that moved off the filesystem onto
`POST /v1/sources/lead-enrol`: auth, field validation, the four invariants the
previous repair established (normalize_target ledger key, INSERT OR IGNORE,
role-as-a-set union, never upsert an existing row), and the scanner's
ordering guarantee across the HTTP boundary (a failed or AMBIGUOUS durable
write blocks the sources.txt line AND the `seen` entry, and is never retried
blindly inside the same run).
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path

import pytest
from aiohttp import web

from src.telegram_dialog_scan import (
    DEFAULT_TOPIC_VOCABULARY_TEXT,
    MIN_SCOREABLE_POSTS,
    TOPIC_SCORE_THRESHOLD,
    Decision,
    ScanPaths,
    ScanReport,
    TopicScore,
    broadcast_channel_is_a_news_source,
    classify,
    load_deny_rules,
    load_topic_rules,
    parse_topic_rules,
    render_report,
    run_scan,
    score_posts,
    topic_rule_errors,
)
from src.telegram_digest import LEAD_SOURCE_ROLE, TelegramDigestStore, _peer_key
from src.telegram_proxy import TelegramProxy, _enrol_lead_source, normalize_target

AI = "Новая модель OpenAI ускоряет инференс агентов в проде на 40 процентов"
OFF = "Ребята, кто идёт на концерт в субботу? Билеты ещё есть, встречаемся у входа"

# ── shim: tracked state is now READ over the proxy too ────────────
# These tests predate `GET /v1/sources/tracked`; an EMPTY but SUCCESSFUL read
# keeps each of them testing what it was written to test, while the real
# "no tracked-state source at all" failure is pinned in
# tests/test_telegram_dialog_scan_tracked.py.
_run_scan_impl = run_scan


def run_scan(*, tracked_reader=None, **kwargs):  # noqa: F811 — deliberate shim
    return _run_scan_impl(
        tracked_reader=tracked_reader or (lambda: {"digest_sources": [], "joins": []}), **kwargs
    )


# ── vocabulary parsing ────────────────────────────────────────────


def test_bare_term_is_a_prefix_stem_match_not_a_bare_substring():
    """RU morphology needs stems; a raw substring would fire inside other words."""
    rules = parse_topic_rules("нейросет\nai")
    assert score_posts(["Нейросети и нейросетью правят миром сегодня тут"], rules).hits == 1
    # "ai" must not fire inside "said"/"chain": the stem anchors at a word start.
    assert score_posts(["He said the supply chain remains broken again today"], rules).hits == 0
    assert score_posts(["AI agents are shipping into production this week now"], rules).hits == 1


def test_word_and_re_escape_hatches_and_invalid_rules_are_reported():
    rules = parse_topic_rules("word:кодинг\nre:gpt-?[0-9]\nre:(unclosed\nword:\n")
    assert score_posts(["Вайбкодинг — это когда ты просто просишь и оно работает"], rules).hits == 1
    assert score_posts(["We shipped it on gpt5 last night and nothing exploded"], rules).hits == 1
    errors = topic_rule_errors(rules)
    assert len(errors) == 2
    assert any("unclosed" in e for e in errors)
    assert all("denies NOTHING" not in e for e in errors)


def test_missing_vocabulary_file_falls_back_to_the_built_in_floor(tmp_path):
    """A missing/empty file must NEVER re-open the default-allow hole."""
    rules = load_topic_rules(tmp_path / "nope.txt")
    assert rules == parse_topic_rules(DEFAULT_TOPIC_VOCABULARY_TEXT)
    assert score_posts([AI], rules).hits == 1


def test_operator_file_only_adds_to_the_built_in_floor(tmp_path):
    path = tmp_path / "topics.txt"
    path.write_text("квантов\n", encoding="utf-8")
    rules = load_topic_rules(path)
    assert len(rules) == len(parse_topic_rules(DEFAULT_TOPIC_VOCABULARY_TEXT)) + 1
    assert score_posts(["Квантовые вычисления наконец добрались до продакшена"], rules).hits == 1


# ── the metric ────────────────────────────────────────────────────


def test_score_is_hits_over_scoreable_posts_and_short_posts_are_not_scoreable():
    rules = parse_topic_rules("нейросет")
    posts = [AI.replace("модель", "нейросеть"), OFF, OFF, "ок"]
    score = score_posts(posts, rules)
    assert score.read == 4
    assert score.scored == 3  # "ок" is below the minimum length
    assert score.hits == 1
    assert score.score == pytest.approx(1 / 3)
    assert score.status == "thin"  # 3 < MIN_SCOREABLE_POSTS


def test_thin_and_unreadable_evidence_are_distinct_statuses():
    rules = parse_topic_rules("нейросет")
    assert score_posts([], rules).status == "thin"
    assert score_posts([AI] * MIN_SCOREABLE_POSTS, rules).status == "ok"


# ── the gate ──────────────────────────────────────────────────────


def _channel(**over):
    base = {"kind": "channel", "entity_id": 111, "username": "somefeed", "title": "Some feed"}
    base.update(over)
    return base


def test_gate_is_no_longer_default_allow():
    deny = load_deny_rules(None)
    ok = TopicScore(score=0.9, hits=9, scored=10, read=10, status="ok")
    bad = TopicScore(score=0.1, hits=1, scored=10, read=10, status="ok")
    thin = TopicScore(score=1.0, hits=2, scored=2, read=2, status="thin")
    assert broadcast_channel_is_a_news_source(_channel(), deny, topic_score=ok) is True
    assert broadcast_channel_is_a_news_source(_channel(), deny, topic_score=bad) is False
    assert broadcast_channel_is_a_news_source(_channel(), deny, topic_score=thin) is False
    # No score at all is NOT a pass — that was the default-allow hole.
    assert broadcast_channel_is_a_news_source(_channel(), deny, topic_score=None) is False


def _classify(candidate, scores, parents=None):
    return classify(
        candidate,
        tracked=None,
        deny_rules=load_deny_rules(None),
        linked_parents=parents or {},
        topic_scores=scores,
    )


def test_passing_channel_enrols_and_the_score_is_in_the_reason():
    score = TopicScore(score=0.83, hits=20, scored=24, read=25, status="ok")
    d = _classify(_channel(), {("channel", 111): score})
    assert d.decision == "enroll-both"
    assert d.topic_score is score
    assert "0.83" in d.reason


def test_failing_channel_with_REAL_evidence_still_reaches_LEADS_with_its_score():
    """Scope split: the gate governs the PUBLIC news surface, not lead breadth."""
    score = TopicScore(score=0.04, hits=1, scored=25, read=25, status="ok")
    d = _classify(_channel(), {("channel", 111): score})
    assert d.decision == "enroll-leads"
    assert "0.04" in d.reason and "news" in d.reason


def test_thin_evidence_quarantines_instead_of_flipping_a_coin():
    score = TopicScore(score=1.0, hits=3, scored=3, read=3, status="thin")
    d = _classify(_channel(), {("channel", 111): score})
    assert d.decision == "quarantine"
    assert "too few" in d.reason or "thin" in d.reason


def test_unreadable_posts_quarantine_and_name_the_proxy_failure():
    score = TopicScore(score=0.0, hits=0, scored=0, read=0, status="unreadable", detail="HTTP 502")
    d = _classify(_channel(), {("channel", 111): score})
    assert d.decision == "quarantine"
    assert "HTTP 502" in d.reason


def test_discussion_chat_of_a_FAILING_parent_never_reaches_chat_sources():
    """The @Music_Producers_Chat path: the deny list should not have to catch it."""
    parent = _channel(entity_id=222, username="erdman_music", title="Leo Erdman Lab")
    chat = {"kind": "megagroup", "entity_id": 333, "username": "Music_Producers_Chat", "title": "Чат"}
    scores = {("channel", 222): TopicScore(score=0.27, hits=7, scored=26, read=26, status="ok")}
    d = classify(
        chat,
        tracked=None,
        deny_rules=parse_topic_rules("") and load_deny_rules(None),
        linked_parents={333: parent},
        topic_scores=scores,
    )
    assert d.decision == "enroll-leads"
    assert d.news_target is None


def test_chat_with_UNREADABLE_posts_never_reaches_chat_sources_on_parent_evidence_alone():
    """The parent is what the gate scores — but chat_sources is the same public
    lane, so a chat we could not read at all is still a guess."""
    parent = _channel(entity_id=222, username="ainews", title="AI news")
    chat = {"kind": "megagroup", "entity_id": 333, "username": "ainews_chat", "title": "Чат"}
    scores = {
        ("channel", 222): TopicScore(score=0.9, hits=27, scored=30, read=30, status="ok"),
        ("linked_chat", 333): TopicScore(
            score=0.0, hits=0, scored=0, read=0, status="unreadable", detail="bounded at 80 post reads/run"
        ),
    }
    d = _classify(chat, scores, parents={333: parent})
    assert d.decision == "enroll-leads"
    assert d.news_target is None
    assert "bounded at 80" in d.reason


# ── Correction 2: chat_sources is a PUBLIC surface, gated on the chat's OWN score
#
# The chat lane merged to main (1db5342) and is DEPLOYED: chat_sources.txt is a
# live digest input the moment the file exists, not a staging file. A passing
# PARENT is therefore not enough — the live dry-run showed parent-gated chats
# scoring as low as 0.08. The chat must pass on its own posts, on the SAME metric
# and the SAME 0.35 line (no second, hand-tuned number), or it goes to leads only.


def test_chat_below_the_threshold_on_its_OWN_posts_goes_to_leads_only():
    parent = _channel(entity_id=222, username="ainews", title="AI news")
    chat = {"kind": "megagroup", "entity_id": 333, "username": "ainews_chat", "title": "Чат"}
    scores = {
        ("channel", 222): TopicScore(score=0.9, hits=27, scored=30, read=30, status="ok"),
        ("linked_chat", 333): TopicScore(score=0.08, hits=2, scored=24, read=24, status="ok"),
    }
    d = _classify(chat, scores, parents={333: parent})
    assert d.decision == "enroll-leads"
    assert d.news_target is None
    assert "0.08" in d.reason and f"{TOPIC_SCORE_THRESHOLD:.2f}" in d.reason


def test_chat_that_passes_on_its_own_posts_reaches_chat_sources():
    parent = _channel(entity_id=222, username="ainews", title="AI news")
    chat = {"kind": "megagroup", "entity_id": 333, "username": "ainews_chat", "title": "Чат"}
    scores = {
        ("channel", 222): TopicScore(score=0.9, hits=27, scored=30, read=30, status="ok"),
        ("linked_chat", 333): TopicScore(score=0.62, hits=15, scored=24, read=24, status="ok"),
    }
    d = _classify(chat, scores, parents={333: parent})
    assert d.decision == "enroll-both"
    assert d.news_target == "chat_sources"


def test_chat_with_THIN_own_evidence_goes_to_leads_only_like_an_unreadable_one():
    parent = _channel(entity_id=222, username="ainews", title="AI news")
    chat = {"kind": "megagroup", "entity_id": 333, "username": "ainews_chat", "title": "Чат"}
    scores = {
        ("channel", 222): TopicScore(score=0.9, hits=27, scored=30, read=30, status="ok"),
        ("linked_chat", 333): TopicScore(score=1.0, hits=3, scored=3, read=17, status="thin"),
    }
    d = _classify(chat, scores, parents={333: parent})
    assert d.decision == "enroll-leads"
    assert d.news_target is None
    assert str(MIN_SCOREABLE_POSTS) in d.reason


def test_the_chat_lane_uses_the_SAME_line_as_the_news_gate():
    """One calibrated threshold, not two: the boundary case proves it."""
    parent = _channel(entity_id=222, username="ainews", title="AI news")
    chat = {"kind": "megagroup", "entity_id": 333, "username": "ainews_chat", "title": "Чат"}
    parent_score = TopicScore(score=0.9, hits=27, scored=30, read=30, status="ok")
    at_line = TopicScore(score=TOPIC_SCORE_THRESHOLD, hits=9, scored=24, read=24, status="ok")
    below = TopicScore(score=TOPIC_SCORE_THRESHOLD - 0.01, hits=8, scored=24, read=24, status="ok")
    assert _classify(chat, {("channel", 222): parent_score, ("linked_chat", 333): at_line},
                     parents={333: parent}).news_target == "chat_sources"
    assert _classify(chat, {("channel", 222): parent_score, ("linked_chat", 333): below},
                     parents={333: parent}).news_target is None


def test_the_report_no_longer_calls_a_chat_sources_write_inert():
    """That line said 'STAGED ONLY: nothing reads chat_sources.txt'. It is now
    false — a live consumer would read it — and it would mislead the operator."""
    report = ScanReport(added_chat=["https://t.me/ainews_chat"])
    text = render_report(report)
    assert "STAGED ONLY" not in text
    assert "nothing" not in text.lower().split("chat_sources.txt +=")[1][:200]
    assert "public" in text.lower()


# ── reading posts: paced, bounded, cached ─────────────────────────


def test_reader_is_bounded_paced_and_reads_each_entity_at_most_once(tmp_path, monkeypatch):
    from src import telegram_dialog_scan as mod

    monkeypatch.setattr(mod, "TOPIC_READ_MAX", 3)
    calls: list[tuple[str, int]] = []
    sleeps: list[float] = []
    monkeypatch.setattr(mod.time, "sleep", lambda s: sleeps.append(s))

    def reader(kind, entity_id):
        calls.append((kind, entity_id))
        return [AI] * 20

    dialogs = [
        {"kind": "channel", "entity_id": i, "username": f"feed{i}", "title": f"f{i}", "usernames": []}
        for i in range(1, 6)
    ]
    # the 5th channel is ALSO the parent of a group -> still one read
    dialogs.append({"kind": "megagroup", "entity_id": 99, "username": "g", "title": "g", "usernames": []})
    dialogs[4]["linked_chat_id"] = 99

    paths = _paths(tmp_path)
    report = run_scan(paths=paths, dialogs=dialogs, dry_run=True, post_reader=reader)
    assert len(calls) == 3
    assert len(set(calls)) == 3
    assert sleeps and all(s == mod.TOPIC_READ_PACING_SECONDS for s in sleeps)
    assert any("bounded at 3" in e for e in report.errors)


def test_report_shows_the_score_for_every_gated_decision(tmp_path):
    def reader(kind, entity_id):
        return [AI] * 20 if entity_id == 1 else [OFF] * 20

    dialogs = [
        {"kind": "channel", "entity_id": 1, "username": "good", "title": "good", "usernames": []},
        {"kind": "channel", "entity_id": 2, "username": "bad", "title": "bad", "usernames": []},
    ]
    report = run_scan(paths=_paths(tmp_path), dialogs=dialogs, dry_run=True, post_reader=reader)
    text = render_report(report)
    assert "score" in text
    assert "1.00" in text and "0.00" in text


# ── Change 2: the proxy enrolment endpoint ────────────────────────


class FakeRequest:
    def __init__(self, app, body, *, token="k"):
        self.app = app
        self.headers = {"Authorization": f"Bearer {token}"} if token else {}
        self._body = body

    async def json(self):
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


class FakeProxy(TelegramProxy):
    """The REAL enrol_lead_source, with only the two stores swapped for temp dbs.

    Subclassed rather than duck-typed on purpose: a stand-in with its own copy of
    the method would pin the stand-in, not the route.
    """

    def __init__(self, digest_db: Path, join_db: Path):  # noqa: D107 — no Telethon start
        from src.telegram_proxy import JoinStore

        self._digest_store = TelegramDigestStore(digest_db)
        self._join_store = JoinStore(join_db)

    def _get_digest_store(self):
        return self._digest_store

    def _get_join_store(self):
        return self._join_store


def _call(proxy, body, *, token="k"):
    app = {"proxy": proxy}
    return asyncio.run(_enrol_lead_source(FakeRequest(app, body, token=token)))


@pytest.fixture()
def proxy(tmp_path, monkeypatch):
    from src import config

    monkeypatch.setattr(config, "TELEGRAM_PROXY_API_KEY", "k")
    return FakeProxy(tmp_path / "digest.db", tmp_path / "join.db")


def _body(resp: web.Response) -> dict:
    return json.loads(resp.text)


def test_endpoint_rejects_a_missing_or_wrong_bearer(proxy):
    with pytest.raises(web.HTTPUnauthorized):
        _call(proxy, {"entity_id": 5, "kind": "channel"}, token="nope")
    with pytest.raises(web.HTTPUnauthorized):
        _call(proxy, {"entity_id": 5, "kind": "channel"}, token=None)


@pytest.mark.parametrize(
    "body",
    [
        {"kind": "channel"},                                  # no entity_id
        {"entity_id": 0, "kind": "channel"},                  # not a real peer
        {"entity_id": "abc", "kind": "channel"},
        {"entity_id": 5, "kind": "user"},                     # not an enrollable kind
        {"entity_id": 5, "kind": "chat"},
        {"entity_id": 5},                                     # no kind
        {"entity_id": 5, "kind": "channel", "username": "bad handle!"},
        {"entity_id": 5, "kind": "channel", "username": "ab"},
    ],
)
def test_endpoint_validates_every_field(proxy, body):
    with pytest.raises(web.HTTPBadRequest):
        _call(proxy, body)


def test_endpoint_cannot_write_an_arbitrary_role_or_a_link(proxy):
    _call(proxy, {"entity_id": 7, "kind": "channel", "title": "T", "username": "feed",
                  "role": "news", "linked_channel_key": "channel:1", "status": "pending"})
    with sqlite3.connect(proxy._digest_store._db_path) as con:
        con.row_factory = sqlite3.Row
        row = con.execute("SELECT * FROM digest_sources").fetchone()
    assert row["role"] == LEAD_SOURCE_ROLE
    assert row["linked_channel_key"] is None


def test_endpoint_cannot_trigger_or_alter_a_join(proxy):
    """A channel enrolment writes NO join row; a linked_chat writes an INERT one."""
    _call(proxy, {"entity_id": 7, "kind": "channel", "title": "T", "username": "feed"})
    with sqlite3.connect(proxy._join_store._db_path) as con:
        assert con.execute("SELECT COUNT(*) FROM joins").fetchone()[0] == 0

    _call(proxy, {"entity_id": 8, "kind": "linked_chat", "title": "C", "username": "somechat"})
    with sqlite3.connect(proxy._join_store._db_path) as con:
        con.row_factory = sqlite3.Row
        row = con.execute("SELECT * FROM joins").fetchone()
    # normalize_target() supplies BOTH columns — not "@Name"/"username".
    assert (row["kind"], row["target"]) == normalize_target("somechat")
    assert row["status"] == "joined"  # inert: the paced loop only picks pending/floodwait


def test_endpoint_never_disturbs_an_outstanding_request_sent_row(proxy):
    kind, target = normalize_target("somechat")
    with sqlite3.connect(proxy._join_store._db_path) as con:
        con.execute(
            "INSERT INTO joins(target, kind, status, created_at, updated_at) VALUES (?,?,?,?,?)",
            (target, kind, "request_sent", "t0", "t0"),
        )
    resp = _body(_call(proxy, {"entity_id": 8, "kind": "linked_chat", "title": "C", "username": "somechat"}))
    with sqlite3.connect(proxy._join_store._db_path) as con:
        con.row_factory = sqlite3.Row
        rows = con.execute("SELECT * FROM joins").fetchall()
    assert len(rows) == 1
    assert rows[0]["status"] == "request_sent"
    assert rows[0]["updated_at"] == "t0"
    assert resp["join_row_created"] is False


def test_a_lead_enrolment_can_never_evict_a_chat_from_the_digest(proxy):
    key = _peer_key("channel", 7)
    proxy._digest_store.add_source_role(
        peer_key=key, entity_id=7, title="Real title", username="feed",
        kind="channel", linked_channel_key=None, role="news",
    )
    resp = _body(_call(proxy, {"entity_id": 7, "kind": "channel", "title": "join-queue junk", "username": None}))
    with sqlite3.connect(proxy._digest_store._db_path) as con:
        con.row_factory = sqlite3.Row
        row = con.execute("SELECT * FROM digest_sources WHERE peer_key = ?", (key,)).fetchone()
    assert set(row["role"].split(",")) == {"news", LEAD_SOURCE_ROLE}
    assert row["title"] == "Real title" and row["username"] == "feed"  # never upserted over
    assert resp["created"] is False


def test_endpoint_is_idempotent_so_the_next_run_can_safely_re_attempt(proxy):
    body = {"entity_id": 9, "kind": "linked_chat", "title": "C", "username": "chatx"}
    first = _body(_call(proxy, body))
    second = _body(_call(proxy, body))
    assert first["created"] is True and second["created"] is False
    with sqlite3.connect(proxy._digest_store._db_path) as con:
        assert con.execute("SELECT COUNT(*) FROM digest_sources").fetchone()[0] == 1


# ── Change 2: the scanner's ordering guarantee across HTTP ────────


def _paths(tmp_path: Path) -> ScanPaths:
    return ScanPaths(
        sources=tmp_path / "sources.txt",
        chat_sources=tmp_path / "chat_sources.txt",
        mirror=tmp_path / "mirror.txt",
        state=tmp_path / "state.json",
        deny=tmp_path / "deny.txt",
        topics=tmp_path / "topics.txt",
        lock=tmp_path / "scan.lock",
    )


def _news_dialog():
    return {"kind": "channel", "entity_id": 42, "username": "goodfeed", "title": "Good", "usernames": []}


def test_a_failed_enrolment_call_blocks_the_sources_line_AND_the_seen_entry(tmp_path):
    paths = _paths(tmp_path)
    paths.sources.write_text("https://t.me/other\n", encoding="utf-8")

    def enroller(**kwargs):
        raise RuntimeError("HTTP 503 from /v1/sources/lead-enrol")

    report = run_scan(
        paths=paths,
        dialogs=[_news_dialog()],
        post_reader=lambda kind, eid: [AI] * 20,
        lead_enroller=enroller,
    )
    assert report.added_news == []
    assert "https://t.me/goodfeed" not in paths.sources.read_text(encoding="utf-8")
    assert 42 not in json.loads(paths.state.read_text(encoding="utf-8"))["seen"]
    assert any("503" in e and "goodfeed" in e for e in report.errors)


def test_an_AMBIGUOUS_timeout_is_reported_and_never_retried_inside_the_run(tmp_path):
    paths = _paths(tmp_path)
    calls: list[int] = []

    def enroller(**kwargs):
        calls.append(kwargs["entity_id"])
        raise asyncio.TimeoutError()

    report = run_scan(
        paths=paths,
        dialogs=[_news_dialog()],
        post_reader=lambda kind, eid: [AI] * 20,
        lead_enroller=enroller,
    )
    assert calls == [42]  # exactly once — no blind retry into a possible double-write
    assert 42 not in json.loads(paths.state.read_text(encoding="utf-8"))["seen"]
    assert any("AMBIGUOUS" in e for e in report.errors)
    assert not paths.sources.exists() or "goodfeed" not in paths.sources.read_text(encoding="utf-8")


def test_a_successful_enrolment_writes_the_news_line_and_the_seen_entry(tmp_path):
    paths = _paths(tmp_path)
    seen_calls = []

    def enroller(**kwargs):
        seen_calls.append(kwargs)
        return {"ok": True, "created": True, "peer_key": _peer_key(kwargs["kind"], kwargs["entity_id"])}

    report = run_scan(
        paths=paths,
        dialogs=[_news_dialog()],
        post_reader=lambda kind, eid: [AI] * 20,
        lead_enroller=enroller,
    )
    assert report.added_news == ["https://t.me/goodfeed"]
    assert report.added_leads == [42]
    assert 42 in json.loads(paths.state.read_text(encoding="utf-8"))["seen"]
    assert seen_calls[0]["kind"] == "channel" and seen_calls[0]["username"] == "goodfeed"
