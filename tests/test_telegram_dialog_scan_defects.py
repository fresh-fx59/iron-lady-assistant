"""Regression tests for the 13 defects the 2026-07-30 adversarial review confirmed.

Every test here asserts BEHAVIOUR (what got written, what the operator receives,
what the ledger looks like afterwards) rather than parsing, because the reviewed
bugs shipped precisely under tests that asserted a rule *parses*. Each one fails
on the pre-fix code.
"""
from __future__ import annotations

import fcntl
import io
import json
import os
import sqlite3
import types
from pathlib import Path

import pytest

from src.telegram_dialog_scan import (
    ScanPaths,
    classify,
    deny_match,
    load_deny_rules,
    render_report,
    run_scan,
)
from src.telegram_digest import LEAD_SOURCE_ROLE, TelegramDigestStore, sync_joined_sources
from src.telegram_proxy import normalize_target

# ── shim: run_scan's durable half now goes over POST /v1/sources/lead-enrol ──
# These tests were written when the scanner opened the join/digest dbs itself.
# The write MOVED to the proxy (it runs as the user that owns 0700
# /var/lib/iron-lady; the scanner does not) — so the assertions below still pin
# the SAME invariants, now through the real endpoint implementation, against the
# same temp dbs. Two injected callables replace what used to be direct sqlite:
# a post_reader (the topical gate's evidence) and a lead_enroller (the endpoint).
from src.telegram_digest import TelegramDigestStore as _DigestStore  # noqa: E402
from src.telegram_dialog_scan import run_scan as _run_scan  # noqa: E402
from src.telegram_proxy import JoinStore as _JoinStore, TelegramProxy as _Proxy  # noqa: E402

class _AlwaysPass(dict):
    """topic_scores stand-in for the tests that predate the gate: every lookup is
    a clean pass, so these cases keep testing what they were written to test."""

    def get(self, key, default=None):
        from src.telegram_dialog_scan import TopicScore

        return TopicScore(score=1.0, hits=20, scored=20, read=20, status="ok")


_PASS = _AlwaysPass()


_TOPICAL_POST = "Новая модель OpenAI ускоряет инференс агентов в проде на 40 процентов"


class _LocalEnroller(_Proxy):
    """The REAL proxy method, pointed at the test's temp dbs."""

    def __init__(self, digest_db, join_db):
        self._digest_store = _DigestStore(digest_db)
        self._join_store = _JoinStore(join_db)


def _default_reader(kind, entity_id):
    return [_TOPICAL_POST] * 20


def run_scan(*, paths, post_reader=None, lead_enroller="default", **kwargs):
    if lead_enroller == "default":
        def lead_enroller(**payload):
            return _LocalEnroller(paths.digest_db, paths.join_db).enrol_lead_source(**payload)
    return _run_scan(
        paths=paths,
        post_reader=post_reader or _default_reader,
        lead_enroller=lead_enroller,
        **kwargs,
    )



def dialog(
    entity_id: int,
    kind: str,
    *,
    title: str | None = None,
    username: str | None = None,
    usernames: list[str] | None = None,
    linked_chat_id: int | None = None,
    linked_chat_lookup: str = "ok",
) -> dict:
    handles = usernames if usernames is not None else ([username] if username else [])
    return {
        "entity_id": entity_id,
        "title": title or f"dialog {entity_id}",
        "kind": kind,
        "username": (handles[0] if handles else None) if username is None else username,
        "usernames": handles,
        "is_broadcast": kind == "channel",
        "is_megagroup": kind == "megagroup",
        "participants_count": None,
        "linked_chat_id": linked_chat_id,
        "linked_chat_lookup": linked_chat_lookup,
    }


@pytest.fixture()
def paths(tmp_path: Path) -> ScanPaths:
    state = tmp_path / "state"
    vault = tmp_path / "vault"
    state.mkdir()
    vault.mkdir()
    sources = state / "sources.txt"
    sources.write_text("# news sources\nhttps://t.me/already_news\n")
    (vault / "mirror.txt").write_text("# news sources\nhttps://t.me/already_news\n")
    (state / "deny.txt").write_text("")
    return ScanPaths(
        sources=sources,
        chat_sources=state / "chat_sources.txt",
        mirror=vault / "mirror.txt",
        state=state / "dialog_scan_seen.json",
        deny=state / "deny.txt",
        topics=state / "topics.txt",
        join_db=state / "telegram_join.db",
        digest_db=state / "telegram_digest.db",
    )


def _notifier(sent: list[str], result: bool = True):
    def notify(text: str) -> bool:
        sent.append(text)
        return result

    return notify


def _joins_rows(join_db: Path) -> list[tuple]:
    with sqlite3.connect(join_db) as con:
        return con.execute("SELECT target, kind, status, entity_id FROM joins ORDER BY target").fetchall()


def _seen(paths: ScanPaths, surface: str = "leads") -> set[int]:
    """Per-surface since 2026-07-31: `seen` split into `decided.{leads,news}`."""
    return {int(i) for i in json.loads(paths.state.read_text())["decided"][surface]}


# ── 1. the scanner can authenticate, and any failure reaches the operator ──


def test_cli_passes_the_file_delivered_proxy_key_explicitly(tmp_path: Path, monkeypatch) -> None:
    """config.TELEGRAM_PROXY_API_KEY is read at IMPORT time, before load_file_env()."""
    from src import telegram_dialog_scan_tool as tool

    key_file = tmp_path / "proxy.key"
    key_file.write_text("s3cret-from-file\n")
    monkeypatch.delenv("TELEGRAM_PROXY_API_KEY", raising=False)
    monkeypatch.setenv("TELEGRAM_PROXY_API_KEY_FILE", str(key_file))
    monkeypatch.setenv("TELEGRAM_PROXY_BASE_URL", "http://127.0.0.1:8787")
    seen: dict = {}

    class FakeClient:
        def __init__(self, **kwargs) -> None:
            seen.update(kwargs)

        async def list_dialogs(self, *, limit: int, with_linked: bool = False):
            return []

    monkeypatch.setattr(tool, "TelegramProxyClient", FakeClient)
    monkeypatch.setattr(tool, "resolve_scan_paths", lambda: _paths_in(tmp_path))

    rc = tool._cmd_scan(types.SimpleNamespace(limit=500, dry_run=True, no_notify=True))

    assert rc == 0
    assert seen.get("api_key") == "s3cret-from-file"


def _paths_in(tmp_path: Path) -> ScanPaths:
    state = tmp_path / "cli-state"
    state.mkdir(exist_ok=True)
    (state / "sources.txt").write_text("")
    return ScanPaths(
        sources=state / "sources.txt",
        chat_sources=state / "chat_sources.txt",
        mirror=state / "mirror.txt",
        state=state / "seen.json",
        deny=state / "deny.txt",
        topics=state / "topics.txt",
        join_db=state / "join.db",
        digest_db=state / "digest.db",
    )


def test_cli_notifies_and_fails_when_the_run_raises_before_it_can_report(tmp_path: Path, monkeypatch) -> None:
    """A 404 / auth failure / locked db must not be a silent nightly no-op."""
    from src import telegram_dialog_scan_tool as tool

    sent: list[str] = []

    class Exploding:
        def __init__(self, **kwargs) -> None:
            raise RuntimeError("TELEGRAM_PROXY_API_KEY is not configured.")

    monkeypatch.setattr(tool, "TelegramProxyClient", Exploding)
    monkeypatch.setattr(tool, "notify_operator", _notifier(sent))
    monkeypatch.setattr(tool, "resolve_scan_paths", lambda: _paths_in(tmp_path))

    rc = tool._cmd_scan(types.SimpleNamespace(limit=500, dry_run=False, no_notify=False))

    assert rc == 1
    assert len(sent) == 1
    assert "TELEGRAM_PROXY_API_KEY" in sent[0]


# ── 2. no silent half-write ───────────────────────────────────────


def test_a_failing_durable_write_never_leaves_a_channel_live_in_the_public_digest(paths: ScanPaths) -> None:
    """The reviewer's repro: read-only digest db + a new channel."""
    TelegramDigestStore(paths.digest_db)  # create it, then make it unwritable
    os.chmod(paths.digest_db, 0o444)
    sent: list[str] = []
    try:
        report = run_scan(
            paths=paths,
            dialogs=[dialog(200, "channel", username="second_chan", title="Second")],
            notifier=_notifier(sent),
        )
    finally:
        os.chmod(paths.digest_db, 0o644)

    # Nothing that feeds the PUBLIC digest may exist without its durable half.
    assert "second_chan" not in paths.sources.read_text()
    assert "second_chan" not in paths.mirror.read_text()
    assert report.added_news == []
    # …and the failure is REPORTED, naming what was not written.
    assert report.errors, "a failed enrolment must be reported"
    assert any("second_chan" in e or "200" in e for e in report.errors)
    assert len(sent) == 1
    # …and it is NOT hidden from the next run.
    if paths.state.exists():
        assert 200 not in _seen(paths)


# ── 3. the report never truncates away the actions ────────────────


def test_first_run_shaped_report_fits_the_4000_char_cap_with_the_actions_intact(paths: ScanPaths) -> None:
    # 150 unaddressable groups: enrollable nowhere, but each one is a candidate
    # on its first run and therefore a table row. (DMs no longer qualify at all —
    # `user`/`bot` dialogs are eligible for no surface, so they never classify.)
    dialogs = [dialog(1000 + i, "megagroup", title=f"Private chat {i}") for i in range(150)]
    dialogs.append(dialog(2000, "channel", username="fresh_ai_news", title="Fresh AI"))

    report = run_scan(paths=paths, dialogs=dialogs)

    assert len(report.text) <= 4000, f"notify_operator would cut this ({len(report.text)} chars)"
    assert "sources.txt +=" in report.text
    assert "TRUNCATED" in report.text  # truncation is explicit, not silent


def test_errors_survive_the_cap_even_with_a_huge_table(paths: ScanPaths) -> None:
    report = run_scan(
        paths=paths,
        dialogs=[dialog(3000 + i, "megagroup", title=f"Private chat {i}") for i in range(200)],
    )
    report.errors.append("SOMETHING BROKE: the operator must see this")
    text = render_report(report)

    assert len(text) <= 4000
    assert "SOMETHING BROKE" in text


# ── 4. the joins row uses the ledger's canonical primary key ──────


def test_joins_row_is_written_under_normalize_targets_canonical_key(paths: ScanPaths) -> None:
    run_scan(paths=paths, dialogs=[dialog(300, "megagroup", username="Some_Dev_Chat", title="Dev chat")])

    expected_kind, expected_target = normalize_target("Some_Dev_Chat")
    assert _joins_rows(paths.join_db) == [(expected_target, expected_kind, "joined", 300)]


def test_an_outstanding_join_request_is_not_duplicated_or_dangled(paths: ScanPaths) -> None:
    """A real `request_sent` row for the same peer must stay the ONE row for it."""
    kind, target = normalize_target("Some_Dev_Chat")
    with sqlite3.connect(paths.join_db) as con:
        con.execute(
            """CREATE TABLE joins (target TEXT PRIMARY KEY, kind TEXT NOT NULL, status TEXT NOT NULL,
               entity_id INTEGER, error TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
               joined_at TEXT, attempts INTEGER NOT NULL DEFAULT 0, retry_at TEXT)"""
        )
        con.execute(
            "INSERT INTO joins(target, kind, status, created_at, updated_at) VALUES (?, ?, 'request_sent', 'x', 'x')",
            (target, kind),
        )

    run_scan(paths=paths, dialogs=[dialog(301, "megagroup", username="Some_Dev_Chat", title="Dev chat")])

    rows = _joins_rows(paths.join_db)
    assert len(rows) == 1, f"duplicate ledger row: {rows}"
    assert rows[0][2] == "request_sent"  # the genuine outstanding request is untouched


def test_a_handle_less_chat_uses_the_proxys_own_id_target_convention(paths: ScanPaths) -> None:
    parent = dialog(310, "channel", username="already_news", linked_chat_id=311)
    child = dialog(311, "megagroup", username=None, title="Обсуждение — Дискуссия")

    run_scan(paths=paths, dialogs=[parent, child])

    rows = _joins_rows(paths.join_db)
    assert rows == [("id:311", "linked", "joined", 311)]


# ── 5. `notified: true` is never a lie ────────────────────────────


def test_a_failed_notification_is_reported_not_claimed_as_delivered(paths: ScanPaths) -> None:
    sent: list[str] = []

    report = run_scan(
        paths=paths,
        dialogs=[dialog(400, "channel", username="fresh_ai_news")],
        notifier=_notifier(sent, result=False),  # notify_operator's unconfigured-token path
    )

    assert sent  # it tried
    assert report.notified is False
    assert any("notification" in e.lower() for e in report.errors)


# ── 6c. an alias already in sources.txt is not re-added ───────────


def test_a_channel_tracked_under_an_alias_is_not_added_again_under_its_primary_handle(
    paths: ScanPaths,
) -> None:
    candidate = dialog(500, "channel", usernames=["primary_alias", "already_news"], title="Aliased")

    report = run_scan(paths=paths, dialogs=[candidate])

    assert report.added_news == []
    # Per-surface tracking makes this peer a live LEADS candidate, so it now
    # reaches the sources.txt line builder for the first time — the alias dedup
    # has to hold there, not only in the freshness filter.
    assert report.added_leads == [500]
    assert "https://t.me/primary_alias" not in paths.sources.read_text()


# ── 7. every deny-rule form actually denies ───────────────────────


@pytest.mark.parametrize(
    "rule_line,candidate",
    [
        ("https://t.me/spam_chan", dialog(600, "channel", username="spam_chan")),
        ("t.me/spam_chan", dialog(601, "channel", username="spam_chan")),
        ("@SPAM_CHAN", dialog(602, "channel", username="spam_chan")),
        # The marked form every Telegram client shows — and the deny file's own example.
        ("id:-1001609072825", dialog(1609072825, "megagroup", username="music_chat")),
        ("id:1609072825", dialog(1609072825, "megagroup", username="music_chat")),
        # A secondary handle must be denied too, case-insensitively.
        ("@alias_handle", dialog(603, "channel", usernames=["Primary", "Alias_Handle"])),
        ("word:КРИПТО", dialog(604, "channel", username="ok_handle", title="Всё про Крипто")),
        ("re:^всё про", dialog(605, "channel", username="ok_handle", title="Всё про Крипто")),
    ],
)
def test_every_deny_rule_form_actually_quarantines(rule_line, candidate, paths: ScanPaths) -> None:
    paths.deny.write_text(rule_line + "\n")
    rules = load_deny_rules(paths.deny)

    assert deny_match(rules, candidate) is not None, f"{rule_line} matched nothing"
    decision = classify(candidate, tracked=None, deny_rules=rules, linked_parents={}, topic_scores=_PASS)
    assert decision.decision == "quarantine", decision.reason


def test_a_malformed_deny_rule_is_reported_not_silently_auto_enrolled(paths: ScanPaths) -> None:
    paths.deny.write_text("re:[unclosed\nid:not-a-number\nnot a handle at all\n")
    sent: list[str] = []

    report = run_scan(paths=paths, dialogs=[dialog(700, "channel", username="fresh_ai_news")], notifier=_notifier(sent))

    assert any("unclosed" in e for e in report.errors), report.errors
    assert any("not-a-number" in e for e in report.errors), report.errors
    assert sent and "unclosed" in sent[0]


# ── 8. removing a deny line really does re-open the dialog ────────


def test_deleting_a_deny_line_reopens_the_dialog_as_the_file_documents(paths: ScanPaths) -> None:
    paths.deny.write_text("@borderline_chan\n")
    dialogs = [dialog(800, "channel", username="borderline_chan", title="Borderline")]

    first = run_scan(paths=paths, dialogs=dialogs)
    assert [d.decision for d in first.decisions] == ["quarantine"]

    paths.deny.write_text("")  # operator removes the rule
    second = run_scan(paths=paths, dialogs=dialogs)

    assert second.added_news == ["https://t.me/borderline_chan"]


def test_the_same_quarantine_does_not_page_the_operator_every_night(paths: ScanPaths) -> None:
    paths.deny.write_text("@borderline_chan\n")
    dialogs = [dialog(801, "channel", username="borderline_chan", title="Borderline")]
    sent: list[str] = []

    run_scan(paths=paths, dialogs=dialogs, notifier=_notifier(sent))
    assert len(sent) == 1  # first sighting reports

    second = run_scan(paths=paths, dialogs=dialogs, notifier=_notifier(sent))

    assert len(sent) == 1, "a known quarantine must not re-report"
    assert second.notified is False
    assert second.requarantined == [801]


# ── 9. chat_sources is described truthfully ───────────────────────


def test_the_report_calls_a_chat_sources_write_a_PUBLIC_surface(paths: ScanPaths) -> None:
    """This used to say "STAGED ONLY: nothing reads chat_sources.txt". That
    premise expired on 2026-07-31: the chat lane merged as 1db5342 and is
    deployed, so the file is a live digest input the moment it exists. Telling
    the operator a public write is inert is worse than saying nothing."""
    parent = dialog(900, "channel", username="already_news", linked_chat_id=901)
    # NOTE (2026-07-31): this child used to be handle-less and was asserted to
    # land in chat_sources as the bare id "901". The live run proved that write
    # inert — a handle-less chat's messages carry link=None and never survive to
    # the draft — so citability is now a precondition and the case that reaches
    # chat_sources is the citable one. The report's WORDING is what this test is
    # about, and that is unchanged.
    child = dialog(901, "megagroup", username="already_news_chat", title="Discussion")

    report = run_scan(paths=paths, dialogs=[parent, child])

    assert report.added_chat == ["https://t.me/already_news_chat"]
    assert "STAGED" not in report.text
    assert "PUBLIC" in report.text
    assert "chat_sources" in report.text


def test_a_written_chat_sources_line_reduces_to_the_key_the_scanner_compares(paths: ScanPaths) -> None:
    """Input-gate: what we WRITE must reduce to the key we later dedup against."""
    from src.telegram_dialog_scan import _chat_source_keys, chat_source_key

    parent = dialog(910, "channel", username="already_news", linked_chat_id=911)
    child = dialog(911, "megagroup", username="Chat_Handle", title="Discussion")

    run_scan(paths=paths, dialogs=[parent, child])

    written = _chat_source_keys(paths.chat_sources.read_text())
    assert chat_source_key("Chat_Handle", 911) in written


# ── 10. the role collision is impossible ──────────────────────────


def test_a_lead_collect_after_an_enrolment_keeps_the_chat_in_the_digest(paths: ScanPaths) -> None:
    """The manufactured role flip: scanner enrols, digest collect tags, lead collect sweeps."""
    parent = dialog(1100, "channel", username="already_news", linked_chat_id=1101)
    child = dialog(1101, "megagroup", username=None, title="Already chat")
    run_scan(paths=paths, dialogs=[parent, child])

    store = TelegramDigestStore(paths.digest_db)
    # what telegram_digest.collect_digest writes for a tracked channel's linked chat
    store.upsert_source(
        peer_key="linked_chat:1101",
        entity_id=1101,
        title="Already chat",
        username=None,
        kind="linked_chat",
        linked_channel_key="channel:1100",
    )
    # …and then a LEAD collect runs its sync over every joined row.
    sync_joined_sources(store, paths.join_db)

    digest_keys = {s.peer_key for s in store.list_sources(["digest"])}
    lead_keys = {s.peer_key for s in store.list_sources([LEAD_SOURCE_ROLE])}
    assert "linked_chat:1101" in digest_keys, "the lead sweep evicted a digest source"
    assert "linked_chat:1101" in lead_keys
    titles = {s.peer_key: s.title for s in store.list_sources()}
    assert titles["linked_chat:1101"] == "Already chat"  # descriptive fields not clobbered


def test_an_existing_source_row_is_never_rewritten_by_the_scanner(paths: ScanPaths) -> None:
    store = TelegramDigestStore(paths.digest_db)
    store.upsert_source(
        peer_key="channel:1200",
        entity_id=1200,
        title="Curated title",
        username="fresh_ai_news",
        kind="channel",
        linked_channel_key=None,
        role="aggregator",
    )

    run_scan(paths=paths, dialogs=[dialog(1200, "channel", username="fresh_ai_news", title="Autoscanned")])

    row = {s.peer_key: s for s in store.list_sources()}["channel:1200"]
    assert row.title == "Curated title"
    assert "aggregator" in row.role


# ── 11. concurrency + guarded reads ───────────────────────────────


def test_a_second_concurrent_run_writes_nothing(paths: ScanPaths) -> None:
    before = paths.sources.read_text()
    lock_path = paths.lock_path()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
    fcntl.flock(fd, fcntl.LOCK_EX)
    try:
        report = run_scan(paths=paths, dialogs=[dialog(1300, "channel", username="fresh_ai_news")])
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)

    assert paths.sources.read_text() == before
    assert report.added_news == []
    assert not paths.state.exists()


def test_an_unreadable_ledger_is_reported_and_blocks_every_write(paths: ScanPaths, tmp_path: Path) -> None:
    """The reviewer hit IsADirectoryError; an unknown ledger must not read as empty."""
    paths.sources.unlink()
    paths.sources.mkdir()
    sent: list[str] = []

    report = run_scan(
        paths=paths,
        dialogs=[dialog(1400, "channel", username="fresh_ai_news")],
        notifier=_notifier(sent),
    )

    assert report.errors and any("sources" in e for e in report.errors)
    assert report.added_news == [] and report.added_leads == []
    assert not paths.state.exists()
    assert len(sent) == 1


def test_a_degraded_linked_chat_lookup_is_recorded_as_an_error(paths: ScanPaths) -> None:
    report = run_scan(
        paths=paths,
        dialogs=[
            dialog(1500, "channel", username="some_news", linked_chat_lookup="floodwait 42s — sweep stopped"),
            dialog(1501, "megagroup", username=None, title="Its chat"),
        ],
    )

    assert any("floodwait" in e.lower() for e in report.errors), report.errors


# ── 14. the unpinned commit claims ────────────────────────────────


def test_a_run_that_changed_nothing_notifies_nobody(paths: ScanPaths) -> None:
    dialogs = [dialog(1600, "channel", username="already_news"), dialog(1601, "user")]
    # @already_news is in sources.txt, so the NEWS surface was always settled;
    # per-surface tracking means the first run still has its LEADS half to do.
    # "changed nothing" is the run after that one.
    run_scan(paths=paths, dialogs=dialogs)
    sent: list[str] = []

    report = run_scan(paths=paths, dialogs=dialogs, notifier=_notifier(sent))

    assert sent == []
    assert report.notified is False


# ===========================================================================
# PROD RUN 2026-07-31 08:54 — three defects found by running the scanner for
# real on contabo-prod. All three are the "silent write" shape again: the write
# lands, the telling fails.
# ===========================================================================


class _FakeResp:
    def __init__(self, message_id=256):
        self._mid = message_id

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return json.dumps({"result": {"message_id": self._mid}}).encode()


def _telegram_like_urlopen(captured: dict):
    """Stand-in for api.telegram.org that behaves the way the REAL API did:
    with parse_mode=HTML an unescaped `<` is a hard 400, not a warning."""
    import urllib.error
    import urllib.parse

    def fake_urlopen(request, timeout=None):
        payload = dict(urllib.parse.parse_qsl(request.data.decode()))
        captured.update(payload)
        captured["url"] = request.full_url
        if payload.get("parse_mode") == "HTML" and "<" in payload.get("text", ""):
            raise urllib.error.HTTPError(
                request.full_url, 400, "Bad Request", {},  # type: ignore[arg-type]
                io.BytesIO(json.dumps({"ok": False, "description": "can't parse entities"}).encode()),
            )
        return _FakeResp()

    return fake_urlopen


def test_operator_report_with_angle_brackets_is_actually_delivered(paths: ScanPaths, monkeypatch) -> None:
    """PROD 2026-07-31: `HTTP Error 400` — notify_operator sent the plain-text
    report with parse_mode=HTML, and Telegram rejected the WHOLE message because
    the report says `... scores 0.19 < 0.35 ...`. The report reached nobody."""
    import src.telegram_aggregator_publish as pub

    monkeypatch.setenv("AGGREGATOR_ALERT_BOT_TOKEN", "tok123")
    monkeypatch.setenv("AGGREGATOR_OPERATOR_CHAT_ID", "1000000001")
    captured: dict = {}
    monkeypatch.setattr(pub.urllib.request, "urlopen", _telegram_like_urlopen(captured))

    parent = dialog(100, "channel", username="ai_news_parent", linked_chat_id=200)
    chat = dialog(200, "megagroup", username="ai_news_chat")  # off-topic => "... < 0.35 ..."
    other = dialog(101, "channel", username="ai_news_two", linked_chat_id=300)
    naked = dialog(300, "megagroup", title="R&D <lab> chat")  # title reaches the table verbatim

    def reader(kind, entity_id):
        return ["просто болтовня о погоде и котиках"] * 20 if kind == "linked_chat" else [_TOPICAL_POST] * 20

    report = run_scan(
        paths=paths,
        dialogs=[parent, chat, other, naked],
        post_reader=reader,
        notifier=pub.notify_operator,
    )

    assert "<" in report.text and "&" in report.text and "@ai_news_parent" in report.text
    assert report.notified is True, report.errors
    assert report.notify_failed is False
    assert "parse_mode" not in captured, "an operator report is PLAIN TEXT, not HTML"
    assert captured["text"] == report.text, "the operator must receive the report verbatim"


def test_report_cap_matches_the_alert_cap_and_the_marker_survives(paths: ScanPaths) -> None:
    """The renderer budgets to MAX_REPORT_CHARS and notify_operator slices at its
    own cap: if they ever disagree, the truncation marker itself gets cut off and
    the operator cannot tell a truncated report from a complete one."""
    from src.telegram_aggregator_publish import OPERATOR_ALERT_CAP
    from src.telegram_dialog_scan import MAX_REPORT_CHARS, TRUNCATION_MARKER, ScanReport

    assert MAX_REPORT_CHARS == OPERATOR_ALERT_CAP

    report = ScanReport(total_dialogs=3)
    report.errors = [f"ledger {i} unreadable: " + "x" * 200 for i in range(40)]  # blows the budget alone
    report.decisions = [
        classify(dialog(900 + i, "channel", username=f"chan_{i}"), tracked=None, deny_rules=[],
                 linked_parents={}, topic_scores=_PASS)
        for i in range(5)
    ]
    text = render_report(report)
    assert len(text) <= MAX_REPORT_CHARS
    assert text.endswith(TRUNCATION_MARKER), text[-160:]
    assert text[:OPERATOR_ALERT_CAP] == text


# ── 2. the topical gate staged five chats that can NEVER be published ──


def test_handle_less_chat_of_a_news_parent_goes_to_leads_only(paths: ScanPaths) -> None:
    """PROD: all five staged chats were handle-less (username: null). Their
    messages get link=None from the proxy, build_draft_input drops link-less
    rows, and _T_ME rejects the t.me/c/<id>/<msg> shape — so chat_sources bought
    nothing and cost a read of each chat on every collect run."""
    parent = dialog(100, "channel", username="ai_news_parent")
    chat = dialog(2771751570, "megagroup", title="закрытый чат", linked_chat_id=100)

    d = classify(chat, tracked=None, deny_rules=[], linked_parents={2771751570: parent}, topic_scores=_PASS)

    assert d.decision == "enroll-leads"
    assert d.news_target is None
    assert d.citable is False
    assert "no citable public handle" in d.reason
    assert d.citability_blocked is True


def test_citable_chat_of_a_news_parent_still_reaches_chat_sources(paths: ScanPaths) -> None:
    parent = dialog(100, "channel", username="ai_news_parent")
    chat = dialog(200, "megagroup", username="ai_news_chat", linked_chat_id=100)

    d = classify(chat, tracked=None, deny_rules=[], linked_parents={200: parent}, topic_scores=_PASS)

    assert d.decision == "enroll-both"
    assert d.news_target == "chat_sources"
    assert d.citable is True
    assert d.citability_blocked is False


def test_citability_uses_the_link_shape_the_digest_will_publish(paths: ScanPaths) -> None:
    """A handle Telegram would never mint (too short) cannot form a t.me link the
    publish gate accepts — derive citability from THAT, not from 'is truthy'."""
    from src.telegram_aggregator_gates import is_citable_link
    from src.telegram_dialog_scan import citable_handle

    assert is_citable_link("https://t.me/ai_news_chat/17") is True
    assert is_citable_link("https://t.me/c/2771751570/17") is False
    assert citable_handle({"username": "ab"}) is None
    assert citable_handle({"username": None}) is None
    assert citable_handle({"username": "@ai_news_chat"}) == "ai_news_chat"


def test_every_chat_decision_reports_citability_in_the_table(paths: ScanPaths) -> None:
    parent = dialog(100, "channel", username="ai_news_parent", linked_chat_id=200)
    other = dialog(101, "channel", username="ai_news_two", linked_chat_id=300)
    handled = dialog(200, "megagroup", username="ai_news_chat")
    naked = dialog(300, "megagroup", title="без хэндла")
    report = run_scan(paths=paths, dialogs=[parent, other, handled, naked], dry_run=True)

    by_id = {d.entity_id: d for d in report.decisions}
    assert by_id[200].citable is True and by_id[300].citable is False
    assert "cite" in report.text
    naked_row = next(line for line in report.text.splitlines() if "без хэндла" in line)
    assert " no " in naked_row


def test_a_non_citable_chat_is_re_examined_next_run_and_promoted_when_it_gains_a_handle(
    paths: ScanPaths,
) -> None:
    """`seen` is permanent, so a chat parked there can never be reconsidered. A
    chat blocked ONLY on citability goes into the re-evaluated bucket instead."""
    parent = dialog(100, "channel", username="ai_news_parent", linked_chat_id=300)
    naked = dialog(300, "megagroup", title="чат без хэндла")

    first = run_scan(paths=paths, dialogs=[parent, naked])
    assert 300 in first.added_leads
    state = json.loads(paths.state.read_text())
    assert 300 in state["news_pending"]
    assert 300 in state["decided"]["leads"], "the lead half IS done"
    assert 300 not in state["decided"]["news"], "and the news half is not — that is the point"

    # it gains a public handle
    now_public = dialog(300, "megagroup", title="чат без хэндла", username="ai_news_chat")
    second = run_scan(paths=paths, dialogs=[parent, now_public])
    assert second.added_chat == ["https://t.me/ai_news_chat"]
    assert 300 in json.loads(paths.state.read_text())["decided"]["news"]
    assert 300 not in json.loads(paths.state.read_text())["news_pending"]


def test_a_still_non_citable_chat_does_not_page_the_operator_every_night(paths: ScanPaths) -> None:
    parent = dialog(100, "channel", username="ai_news_parent", linked_chat_id=300)
    naked = dialog(300, "megagroup", title="чат без хэндла")
    run_scan(paths=paths, dialogs=[parent, naked])

    sent: list[str] = []
    second = run_scan(paths=paths, dialogs=[parent, naked], notifier=_notifier(sent))

    assert [d.entity_id for d in second.decisions] == []
    assert second.still_pending == [300]
    assert sent == []


# ── 3. `wrote_nothing` said the opposite of what it means ──


def test_dry_run_reports_that_it_wrote_nothing(paths: ScanPaths) -> None:
    """PROD: a --dry-run that provably wrote nothing reported wrote_nothing:false.
    The field measured 'this run did not REFUSE to write'."""
    before = paths.sources.read_text()
    report = run_scan(paths=paths, dialogs=[dialog(400, "channel", username="fresh_ai_chan")], dry_run=True)

    assert paths.sources.read_text() == before
    assert not paths.chat_sources.exists()
    assert not paths.state.exists()
    assert report.wrote_nothing is True
    assert report.refused_to_write is False


def test_a_run_that_writes_reports_wrote_nothing_false(paths: ScanPaths) -> None:
    report = run_scan(paths=paths, dialogs=[dialog(400, "channel", username="fresh_ai_chan")])
    assert report.added_news == ["https://t.me/fresh_ai_chan"]
    assert report.wrote_nothing is False
    assert report.refused_to_write is False


def test_cli_json_reports_wrote_nothing_truthfully_on_a_dry_run(tmp_path: Path, monkeypatch, capsys) -> None:
    """The operator reads this JSON line, not the dataclass."""
    from src import telegram_dialog_scan_tool as tool

    class FakeClient:
        def __init__(self, **kwargs) -> None:
            pass

        async def list_dialogs(self, *, limit: int, with_linked: bool = False):
            return [dialog(400, "channel", username="fresh_ai_chan")]

        async def read_messages(self, **kwargs):
            return [{"text": _TOPICAL_POST}] * 20

    monkeypatch.setenv("TELEGRAM_PROXY_API_KEY", "k")
    monkeypatch.setattr(tool, "TelegramProxyClient", FakeClient)
    monkeypatch.setattr(tool, "resolve_scan_paths", lambda: _paths_in(tmp_path))

    tool._cmd_scan(types.SimpleNamespace(limit=500, dry_run=True, no_notify=True))

    line = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert line["wrote_nothing"] is True
    assert line["refused_to_write"] is False
