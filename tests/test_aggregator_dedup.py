"""tests/test_aggregator_dedup.py — Feature A1: cross-day dedup (7-day rolling window).

Record-what-SHIPPED design: structured stories are staged in the ledger row at
gate time and PROMOTED into a persistent `published_stories` table only after a
digest is actually posted. Two layers: a deterministic same-URL backstop in
`run_gates`, and a semantic feed (`recent_headlines`) carried into the draft
input for the LLM. All migrations are additive + idempotent; tests use tmp dbs
only (never the live ~/telegram-aggregator/ledger.db).
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from src import telegram_aggregator_tool
from src.telegram_aggregator import build_draft_input
from src.telegram_aggregator_gates import Story, run_gates
from src.telegram_aggregator_publish import (
    DigestLedger,
    _norm_title,
    deserialize_stories,
    publish_next,
    serialize_stories,
)
from src.telegram_aggregator_tool import main
from src.telegram_digest import TelegramDigestStore


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _story(headline, *, summary="Своими словами о событии.", links=None):
    return Story(headline=headline, summary=summary, source_links=links or ["https://t.me/chan/1"])


def _ledger(tmp_path):
    return DigestLedger(tmp_path / "ledger.db")


def _row(tmp_path, date_key):
    con = sqlite3.connect(tmp_path / "ledger.db")
    con.row_factory = sqlite3.Row
    try:
        return con.execute(
            "SELECT messages_json, stories_json, status FROM digests WHERE date_key = ?",
            (date_key,),
        ).fetchone()
    finally:
        con.close()


def _count_published(tmp_path):
    con = sqlite3.connect(tmp_path / "ledger.db")
    try:
        return con.execute("SELECT COUNT(*) FROM published_stories").fetchone()[0]
    finally:
        con.close()


class FakeTransport:
    def __init__(self):
        self.calls = []

    def send_message(self, chat, text):
        self.calls.append((chat, text))
        return len(self.calls)


def _iso(days_ago: int) -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=days_ago)).isoformat()


# ---------------------------------------------------------------------------
# Step 1 — (de)serialization + normalization helpers  [pure]
# ---------------------------------------------------------------------------
def test_serialize_deserialize_stories_roundtrip():
    stories = [
        Story("H1", "s1", ["https://t.me/a/1", "https://t.me/a/2"]),
        Story("H2", "s2", ["https://t.me/b/3"]),
    ]
    assert deserialize_stories(serialize_stories(stories)) == stories
    assert deserialize_stories(None) == []
    assert deserialize_stories("not json") == []
    assert deserialize_stories("[1, 2, 3]") == []  # items not objects -> skipped


def test_norm_title_collapses_ws_lowercases_and_caps():
    assert _norm_title("  Hello   World  ") == "hello world"
    assert _norm_title("НоВаЯ Модель") == "новая модель"
    assert len(_norm_title("Х" * 300)) == 200


# ---------------------------------------------------------------------------
# Step 2 — idempotent migration: stories_json col + published_stories table
# ---------------------------------------------------------------------------
def test_migration_adds_stories_store_idempotent(tmp_path):
    db = tmp_path / "ledger.db"
    # hand-create a LEGACY digests table (pre-A1: no stories_json column)
    con = sqlite3.connect(db)
    con.execute(
        """
        CREATE TABLE digests (
            date_key TEXT PRIMARY KEY,
            messages_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            sent_count INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    con.commit()
    con.close()

    DigestLedger(db)
    DigestLedger(db)  # second construction must be a no-op, raise nothing

    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    try:
        cols = {r["name"] for r in con.execute("PRAGMA table_info(digests)").fetchall()}
        names = {r["name"] for r in con.execute("SELECT name FROM sqlite_master").fetchall()}
    finally:
        con.close()
    assert "stories_json" in cols
    assert "published_stories" in names
    assert "idx_published_stories_url" in names
    assert "idx_published_stories_date" in names


# ---------------------------------------------------------------------------
# Step 3 — window read methods (configurable window; nothing older blocks)
# ---------------------------------------------------------------------------
def test_published_urls_and_headlines_since_window(tmp_path):
    ledger = _ledger(tmp_path)
    d1, d3, d8 = _iso(1), _iso(3), _iso(8)
    cutoff7 = _iso(7)
    con = ledger._connect()  # noqa: SLF001 — seed the store directly
    try:
        rows = [
            (d8, "old", "https://t.me/o/1", "Old story"),
            (d3, "mid", "https://t.me/m/2", "Mid story"),
            (d1, _norm_title("Dup story"), "https://t.me/d/3", "Dup story"),
            (d1, _norm_title("Dup story"), "https://t.me/d/4", "Dup story"),  # same headline, 2nd url
        ]
        for date_key, nt, url, headline in rows:
            con.execute(
                "INSERT INTO published_stories(date_key, norm_title, url, headline, published_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (date_key, nt, url, headline, _iso(0)),
            )
        con.commit()
    finally:
        con.close()

    # URLs: everything in-window (D-3, D-1x2), D-8 excluded (nothing older than 7 days blocks)
    assert ledger.published_urls_since(cutoff7) == {
        "https://t.me/m/2",
        "https://t.me/d/3",
        "https://t.me/d/4",
    }
    # Headlines: deduped by norm_title, newest-first, D-8 excluded
    heads = ledger.published_headlines_since(cutoff7)
    assert heads == [
        {"date": d1, "headline": "Dup story"},
        {"date": d3, "headline": "Mid story"},
    ]
    # Window is configurable: a tighter cutoff drops D-3 too
    assert ledger.published_urls_since(_iso(2)) == {"https://t.me/d/3", "https://t.me/d/4"}


# ---------------------------------------------------------------------------
# Step 4 — upsert_draft stages structured stories; posted rows stay frozen
# ---------------------------------------------------------------------------
def test_upsert_draft_stages_stories_json(tmp_path):
    ledger = _ledger(tmp_path)
    payload = serialize_stories([_story("H1")])
    ledger.upsert_draft("2026-07-14", ["msg"], stories_json=payload)
    assert _row(tmp_path, "2026-07-14")["stories_json"] == payload


def test_upsert_locked_posted_row_keeps_stories_json(tmp_path):
    ledger = _ledger(tmp_path)
    original = serialize_stories([_story("H1")])
    ledger.upsert_draft("2026-07-14", ["v1"], stories_json=original)
    ledger.approve()
    assert ledger.begin_send("2026-07-14")
    ledger.mark_posted("2026-07-14")
    # a later upsert on a posted row must NOT touch messages OR stories
    ledger.upsert_draft("2026-07-14", ["v2"], stories_json=serialize_stories([_story("H2")]))
    row = _row(tmp_path, "2026-07-14")
    assert json.loads(row["messages_json"]) == ["v1"]
    assert row["stories_json"] == original
    assert row["status"] == "posted"


def test_upsert_draft_default_stories_json_is_null(tmp_path):
    """2-arg callers stay green: stories_json defaults to NULL."""
    ledger = _ledger(tmp_path)
    ledger.upsert_draft("2026-07-14", ["msg"])
    assert _row(tmp_path, "2026-07-14")["stories_json"] is None


# ---------------------------------------------------------------------------
# Step 5 — promotion into published_stories only AFTER posted
# ---------------------------------------------------------------------------
def test_publish_records_shipped_stories(tmp_path):
    ledger = _ledger(tmp_path)
    s1 = Story("Заголовок один", "s1", ["https://t.me/a/1", "https://t.me/a/2"])
    s2 = Story("Заголовок два", "s2", ["https://t.me/b/3"])
    ledger.upsert_draft("2026-07-14", ["msg"], stories_json=serialize_stories([s1, s2]))
    ledger.approve()
    result = publish_next(ledger, FakeTransport(), "@chan")
    assert result["status"] == "posted"

    assert ledger.published_urls_since("2026-07-14") == {
        "https://t.me/a/1",
        "https://t.me/a/2",
        "https://t.me/b/3",
    }
    con = sqlite3.connect(tmp_path / "ledger.db")
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            "SELECT date_key, norm_title, url, headline FROM published_stories ORDER BY url"
        ).fetchall()
    finally:
        con.close()
    assert len(rows) == 3
    assert {r["date_key"] for r in rows} == {"2026-07-14"}
    a1 = next(r for r in rows if r["url"] == "https://t.me/a/1")
    assert a1["norm_title"] == "заголовок один"
    assert a1["headline"] == "Заголовок один"


def test_record_published_stories_idempotent(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.upsert_draft(
        "2026-07-14", ["m"], stories_json=serialize_stories([_story("H1", links=["https://t.me/a/1"])])
    )
    assert ledger.record_published_stories("2026-07-14") == 1
    assert ledger.record_published_stories("2026-07-14") == 1  # delete-then-insert
    assert _count_published(tmp_path) == 1


def test_record_published_stories_empty_is_noop(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.upsert_draft("2026-07-14", ["m"])  # no stories staged
    assert ledger.record_published_stories("2026-07-14") == 0
    assert _count_published(tmp_path) == 0


def test_dry_run_does_not_record(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.upsert_draft(
        "2026-07-14", ["m"], stories_json=serialize_stories([_story("H1")])
    )
    ledger.approve()
    assert publish_next(ledger, None, None, dry_run=True)["status"] == "dry-run"
    assert _count_published(tmp_path) == 0


def test_publish_null_stories_records_nothing(tmp_path):
    """Legacy rows (NULL stories_json) publish fine and promote nothing."""
    ledger = _ledger(tmp_path)
    ledger.upsert_draft("2026-07-14", ["msg one", "msg two"])
    ledger.approve()
    assert publish_next(ledger, FakeTransport(), "@chan")["status"] == "posted"
    assert _count_published(tmp_path) == 0


# ---------------------------------------------------------------------------
# Step 6 — deterministic same-URL backstop in run_gates
# ---------------------------------------------------------------------------
LINK_A = "https://t.me/chan_a/10"
LINK_B = "https://t.me/chan_b/20"
LINK_C = "https://t.me/chan_c/30"
KNOWN = {LINK_A, LINK_B, LINK_C}


def test_gate_drops_link_published_in_window():
    stories = [
        _story("Первый", links=[LINK_A]),
        _story("Второй", links=[LINK_B]),
        _story("Третий", links=[LINK_C]),
    ]
    result = run_gates(stories, known_links=KNOWN, source_texts=[], blocked_links={LINK_A})
    assert _story("Первый", links=[LINK_A]) not in result.stories
    assert len(result.stories) == 2
    assert any("already published" in e for e in result.errors)


def test_gate_blocked_links_defaults_empty():
    stories = [
        _story("Первый", links=[LINK_A]),
        _story("Второй", links=[LINK_B]),
        _story("Третий", links=[LINK_C]),
    ]
    result = run_gates(stories, known_links=KNOWN, source_texts=[])
    assert result.ok and len(result.stories) == 3


# ---------------------------------------------------------------------------
# Step 7 — build_draft_input carries recent_headlines  [pure]
# ---------------------------------------------------------------------------
def test_build_draft_input_includes_recent_headlines(tmp_path):
    store = TelegramDigestStore(tmp_path / "agg.db")
    recent = [{"date": "2026-07-13", "headline": "Вчерашняя новость"}]
    doc = build_draft_input(store, window_hours=24, recent_headlines=recent)
    assert doc["recent_headlines"] == recent


def test_build_draft_input_recent_headlines_defaults_empty(tmp_path):
    store = TelegramDigestStore(tmp_path / "agg.db")
    doc = build_draft_input(store, window_hours=24)
    assert doc["recent_headlines"] == []


# ---------------------------------------------------------------------------
# Step 8 — CLI wiring: window helper + render-input feed + gate backstop/staging
# ---------------------------------------------------------------------------
@pytest.fixture()
def state(monkeypatch, tmp_path):
    monkeypatch.setenv("AGGREGATOR_STATE_DIR", str(tmp_path / "state"))
    return tmp_path / "state"


def _write_input(state, links_texts, date="2026-07-14"):
    payload = {
        "date": date,
        "window_hours": 24,
        "posts": [
            {"channel": "A", "username": "chan", "link": l, "text": t,
             "views": 1, "forwards": 0, "posted_at": f"{date}T10:00:00+00:00"}
            for l, t in links_texts
        ],
    }
    path = state / "drafts" / f"{date}-input.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False))
    return path


def _write_draft(state, stories, date="2026-07-14"):
    path = state / "drafts" / f"{date}-draft.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"stories": stories}, ensure_ascii=False))
    return path


def test_dedup_window_days_env_default_and_override(monkeypatch):
    monkeypatch.delenv("AGGREGATOR_DEDUP_WINDOW_DAYS", raising=False)
    assert telegram_aggregator_tool._dedup_window_days() == 7
    monkeypatch.setenv("AGGREGATOR_DEDUP_WINDOW_DAYS", "14")
    assert telegram_aggregator_tool._dedup_window_days() == 14
    monkeypatch.setenv("AGGREGATOR_DEDUP_WINDOW_DAYS", "garbage")
    assert telegram_aggregator_tool._dedup_window_days() == 7
    monkeypatch.setenv("AGGREGATOR_DEDUP_WINDOW_DAYS", "0")
    assert telegram_aggregator_tool._dedup_window_days() == 7


def test_dedup_cutoff_spans_exactly_window_days(monkeypatch):
    """FIX 4a: with an INCLUSIVE (date_key >= cutoff) window, N days must span
    exactly N distinct dates — cutoff = today - (N-1), not today - N."""
    monkeypatch.delenv("AGGREGATOR_DEDUP_WINDOW_DAYS", raising=False)
    # default 7 -> today plus the 6 prior days = 7 distinct dates
    assert telegram_aggregator_tool._dedup_cutoff() == _iso(6)
    monkeypatch.setenv("AGGREGATOR_DEDUP_WINDOW_DAYS", "1")
    assert telegram_aggregator_tool._dedup_cutoff() == _iso(0)  # today only
    monkeypatch.setenv("AGGREGATOR_DEDUP_WINDOW_DAYS", "14")
    assert telegram_aggregator_tool._dedup_cutoff() == _iso(13)


def _seed_published(state, date_key, stories):
    ledger = DigestLedger(state / "ledger.db")
    ledger.upsert_draft(date_key, ["seed"], stories_json=serialize_stories(stories))
    ledger.approve(date_key)
    ledger.begin_send(date_key)
    ledger.mark_posted(date_key)
    ledger.record_published_stories(date_key)


def test_render_input_emits_recent_headlines(state, capsys):
    # aggregator.db must exist for build_draft_input
    TelegramDigestStore(state / "aggregator.db")
    seed_date = _iso(1)  # inside the 7-day window regardless of wall clock
    _seed_published(state, seed_date, [_story("Вчерашний заголовок", links=["https://t.me/x/9"])])
    out_path = state / "input.json"
    rc = main(["render-input", "--out", str(out_path)])
    assert rc == 0
    doc = json.loads(out_path.read_text())
    assert any(h["headline"] == "Вчерашний заголовок" for h in doc["recent_headlines"])


def test_gate_drops_story_with_url_published_last_7_days(state, capsys):
    blocked = "https://t.me/chan/1"
    _seed_published(state, _iso(2), [_story("Старая новость", links=[blocked])])

    links = [(f"https://t.me/chan/{i}", "длинный исходный текст про ИИ " * 10) for i in (1, 2, 3, 4)]
    input_path = _write_input(state, links)
    stories = [
        {"headline": f"Сюжет {i}", "summary": "Своими словами.", "source_links": [f"https://t.me/chan/{i}"]}
        for i in (1, 2, 3, 4)
    ]
    draft_path = _write_draft(state, stories)

    rc = main(["gate", "--draft", str(draft_path), "--input", str(input_path), "--date", "2026-07-14"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["stories"] == 3  # the blocked one dropped, three survive
    assert any("already published" in e for e in out["dropped"])
    row = _row(state, "2026-07-14")
    assert blocked not in row["messages_json"]


def test_gate_stages_structured_stories(state, capsys):
    links = [(f"https://t.me/chan/{i}", "длинный исходный текст про ИИ " * 10) for i in (1, 2, 3)]
    input_path = _write_input(state, links)
    stories = [
        {"headline": f"Сюжет {i}", "summary": "Своими словами.", "source_links": [f"https://t.me/chan/{i}"]}
        for i in (1, 2, 3)
    ]
    draft_path = _write_draft(state, stories)
    rc = main(["gate", "--draft", str(draft_path), "--input", str(input_path), "--date", "2026-07-14"])
    assert rc == 0
    row = _row(state, "2026-07-14")
    staged = deserialize_stories(row["stories_json"])
    assert len(staged) == 3
    assert {s.headline for s in staged} == {"Сюжет 1", "Сюжет 2", "Сюжет 3"}
