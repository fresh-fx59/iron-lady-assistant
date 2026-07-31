"""Per-PIPELINE tracking: "tracked on one surface" is not "done with, forever".

The 2026-07-31 production defect: `_already_tracked()` short-circuited on
`entity_id in tracked.lead_entity_ids`, so the moment an entity became a LEAD
source the scanner stopped considering it for anything — including the NEWS
surface it had never been evaluated for. Live, that made `@moneyforstartup_chat`
(citable, own score 0.48 behind a 1.00 parent) impossible to stage into
`chat_sources.txt`, permanently, along with 101 other lead-tracked entities.

The mirror case is the same bug from the other side: a channel already in
`sources.txt` was `_already_tracked`, so it could never be lead-enrolled.

These tests pin the general rule — a candidate is evaluated for every surface it
is eligible for and NOT yet on — plus the two disciplines that keep the fix from
becoming nightly noise: repeat outcomes report as a COUNT, and a run with nothing
new reads nothing and writes nothing.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.telegram_dialog_scan import ScanPaths, run_scan as _run_scan
from src.telegram_digest import LEAD_SOURCE_ROLE, _peer_key

AI = "Новая модель OpenAI ускоряет инференс агентов в проде на 40 процентов"
OFF = "Ребята, кто идёт на концерт в субботу? Билеты ещё есть, встречаемся у входа"


def dialog(
    entity_id: int,
    kind: str,
    *,
    title: str | None = None,
    username: str | None = None,
    linked_chat_id: int | None = None,
) -> dict:
    return {
        "entity_id": entity_id,
        "title": title or f"dialog {entity_id}",
        "kind": kind,
        "username": username,
        "usernames": [username] if username else [],
        "is_broadcast": kind == "channel",
        "is_megagroup": kind == "megagroup",
        "participants_count": None,
        "linked_chat_id": linked_chat_id,
        "linked_chat_lookup": "ok",
    }


def lead_row(entity_id: int, kind: str = "linked_chat") -> dict:
    return {"peer_key": _peer_key(kind, entity_id), "entity_id": entity_id, "role": LEAD_SOURCE_ROLE}


@pytest.fixture()
def paths(tmp_path: Path) -> ScanPaths:
    state = tmp_path / "state"
    state.mkdir()
    (state / "sources.txt").write_text("# news\nhttps://t.me/parent_news\n", encoding="utf-8")
    (state / "deny.txt").write_text("", encoding="utf-8")
    (state / "topics.txt").write_text("", encoding="utf-8")
    return ScanPaths(
        sources=state / "sources.txt",
        chat_sources=state / "chat_sources.txt",
        mirror=tmp_path / "missing-mirror.txt",
        state=state / "dialog_scan_seen.json",
        deny=state / "deny.txt",
        topics=state / "topics.txt",
    )


class Reader:
    """Post reader that COUNTS its reads — the budget is the whole point here."""

    def __init__(self, texts: dict[tuple[str, int], list[str]] | None = None, default: list[str] | None = None):
        self.texts = texts or {}
        self.default = default if default is not None else [AI] * 20
        self.calls: list[tuple[str, int]] = []

    def __call__(self, kind: str, entity_id: int) -> list[str]:
        self.calls.append((kind, entity_id))
        return self.texts.get((kind, entity_id), self.default)


def run(paths: ScanPaths, dialogs, *, tracked, reader=None, enrolled=None, **kwargs):
    """run_scan with a recording enroller; `enrolled` collects the durable half."""
    seen_ids = enrolled if enrolled is not None else []

    def lead_enroller(**payload):
        seen_ids.append(int(payload["entity_id"]))
        return {}

    return _run_scan(
        paths=paths,
        dialogs=dialogs,
        post_reader=reader or Reader(),
        lead_enroller=lead_enroller,
        tracked_reader=lambda: tracked,
        notifier=lambda text: True,
        **kwargs,
    )


def state_of(paths: ScanPaths) -> dict:
    return json.loads(paths.state.read_text(encoding="utf-8"))


def chat_lines(paths: ScanPaths) -> list[str]:
    if not paths.chat_sources.exists():
        return []
    return [ln for ln in paths.chat_sources.read_text(encoding="utf-8").splitlines() if ln and not ln.startswith("#")]


# ── 1. the defect, exactly as production hit it ───────────────────


def test_a_lead_tracked_chat_is_still_a_chat_sources_candidate(paths: ScanPaths) -> None:
    """@moneyforstartup_chat: lead-enrolled at 08:54, therefore invisible at 09:29."""
    parent = dialog(500, "channel", username="parent_news", linked_chat_id=600)
    chat = dialog(600, "megagroup", username="moneyforstartup_chat")
    tracked = {"digest_sources": [lead_row(600)], "joins": []}

    report = run(paths, [parent, chat], tracked=tracked)

    assert 600 in report.new_dialogs, "a lead-tracked chat must still be a NEWS candidate"
    assert report.added_chat == ["https://t.me/moneyforstartup_chat"]
    assert chat_lines(paths) == ["https://t.me/moneyforstartup_chat"]


def test_a_news_tracked_channel_is_still_a_leads_candidate(paths: ScanPaths) -> None:
    """The mirror image: sources.txt membership used to block lead enrolment too."""
    channel = dialog(500, "channel", username="parent_news")  # already in sources.txt
    enrolled: list[int] = []

    report = run(paths, [channel], tracked={"digest_sources": [], "joins": []}, enrolled=enrolled)

    assert enrolled == [500], "a channel already on the news surface must still reach the lead pipeline"
    assert report.added_news == [], "and must NOT be re-added to sources.txt"


# ── 2. already on a surface ⇒ never re-proposed for that surface ──


def test_an_entity_on_both_surfaces_is_not_fresh_at_all(paths: ScanPaths) -> None:
    paths.chat_sources.write_text("https://t.me/moneyforstartup_chat\n", encoding="utf-8")
    parent = dialog(500, "channel", username="parent_news", linked_chat_id=600)
    chat = dialog(600, "megagroup", username="moneyforstartup_chat")
    tracked = {"digest_sources": [lead_row(600), lead_row(500, "channel")], "joins": []}
    reader = Reader()

    report = run(paths, [parent, chat], tracked=tracked, reader=reader)

    assert report.new_dialogs == []
    assert reader.calls == [], "nothing outstanding ⇒ no post reads at all"
    assert report.added_chat == [] and report.added_news == []


# ── 3. the two re-checked buckets must not collide ────────────────


def test_a_citability_blocked_chat_and_a_lead_tracked_chat_report_once_each(paths: ScanPaths) -> None:
    """No double-report, no double-write when both re-check reasons are live."""
    parent = dialog(500, "channel", username="parent_news", linked_chat_id=600)
    citable = dialog(600, "megagroup", username="moneyforstartup_chat")
    blocked = dialog(700, "megagroup", title="private chat")  # no handle ⇒ never citable
    tracked = {"digest_sources": [lead_row(600), lead_row(700)], "joins": []}

    report = run(paths, [parent, citable, blocked], tracked=tracked)

    ids = [d.entity_id for d in report.decisions]
    assert sorted(ids) == sorted(set(ids)), "each entity reports at most one row"
    assert 700 in ids and 600 in ids
    assert report.added_chat == ["https://t.me/moneyforstartup_chat"], "written once"
    assert chat_lines(paths) == ["https://t.me/moneyforstartup_chat"]


# ── 4. a steady state exists: nothing new ⇒ empty, cheap run ──────


def test_second_run_reads_nothing_writes_nothing_and_stays_silent(paths: ScanPaths) -> None:
    parent = dialog(500, "channel", username="parent_news", linked_chat_id=600)
    chat = dialog(600, "megagroup", username="moneyforstartup_chat")
    off = dialog(800, "channel", username="off_topic")
    dialogs = [parent, chat, off]
    tracked = {"digest_sources": [lead_row(600)], "joins": []}
    reader = Reader(texts={("channel", 800): [OFF] * 20})

    run(paths, dialogs, tracked=tracked, reader=reader)
    first_reads = len(reader.calls)
    assert first_reads > 0

    # everything the first run enrolled is now tracked
    tracked2 = {
        "digest_sources": [lead_row(600), lead_row(500, "channel"), lead_row(800, "channel")],
        "joins": [],
    }
    paths.sources.write_text(paths.sources.read_text(encoding="utf-8"), encoding="utf-8")
    reader2 = Reader(texts={("channel", 800): [OFF] * 20})
    sent: list[str] = []
    report2 = _run_scan(
        paths=paths,
        dialogs=dialogs,
        post_reader=reader2,
        lead_enroller=lambda **kw: {},
        tracked_reader=lambda: tracked2,
        notifier=lambda text: (sent.append(text), True)[1],
    )

    assert report2.new_dialogs == [], "a decided entity leaves the fresh list"
    assert reader2.calls == [], "steady state costs ZERO post reads"
    assert report2.added_chat == [] and report2.added_news == []
    assert sent == [], "nothing new ⇒ the operator is not paged"


def test_a_repeat_non_citable_chat_reports_as_a_count_not_a_row(paths: ScanPaths) -> None:
    parent = dialog(500, "channel", username="parent_news", linked_chat_id=700)
    blocked = dialog(700, "megagroup", title="private chat")
    dialogs = [parent, blocked]
    tracked = {"digest_sources": [lead_row(700), lead_row(500, "channel")], "joins": []}

    first = run(paths, dialogs, tracked=tracked)
    assert 700 in [d.entity_id for d in first.decisions]

    reader2 = Reader()
    second = run(paths, dialogs, tracked=tracked, reader=reader2)

    assert 700 not in [d.entity_id for d in second.decisions], "repeat outcome ⇒ count, not a row"
    assert second.still_pending == [700]
    assert ("linked_chat", 700) not in reader2.calls, "a non-citable chat costs no post read"


# ── 5. read economy: only reads that can change an outcome ────────


def test_a_chat_whose_parent_is_known_not_to_be_news_costs_no_reads(paths: ScanPaths) -> None:
    """Once the parent has been scored and rejected, its chat is free to re-check."""
    parent = dialog(500, "channel", username="off_topic", linked_chat_id=600)
    chat = dialog(600, "megagroup", username="somechat")
    dialogs = [parent, chat]
    tracked = {"digest_sources": [], "joins": []}
    reader = Reader(default=[OFF] * 20)

    run(paths, dialogs, tracked=tracked, reader=reader)
    assert ("channel", 500) in reader.calls

    tracked2 = {"digest_sources": [lead_row(500, "channel"), lead_row(600)], "joins": []}
    reader2 = Reader(default=[OFF] * 20)
    second = run(paths, dialogs, tracked=tracked2, reader=reader2)

    assert reader2.calls == [], "a parent already decided as NOT-news answers for free"
    assert second.added_chat == []


# ── 6. evidence we could not get never settles a surface ──────────


def test_unreadable_own_posts_do_not_settle_the_news_surface(paths: ScanPaths) -> None:
    parent = dialog(500, "channel", username="parent_news", linked_chat_id=600)
    chat = dialog(600, "megagroup", username="somechat")

    def boom(kind: str, entity_id: int):
        if (kind, entity_id) == ("linked_chat", 600):
            raise RuntimeError("flood wait")
        return [AI] * 20

    run(paths, [parent, chat], tracked={"digest_sources": [lead_row(600)], "joins": []}, reader=boom)

    decided = set(state_of(paths)["decided"]["news"])
    assert 600 not in decided, "no evidence ⇒ retried, never parked as decided"

    reader = Reader()
    second = run(paths, [parent, chat], tracked={"digest_sources": [lead_row(600)], "joins": []}, reader=reader)
    assert second.added_chat == ["https://t.me/somechat"], "the retry succeeds once the read works"


# ── 7. state file: per-surface, and it migrates ───────────────────


def test_v3_state_migrates_and_forces_one_news_re_evaluation(paths: ScanPaths) -> None:
    """v3 `seen` cannot mean "news-decided": it was written from ALL dialog ids,
    including the ones `_already_tracked` filtered out before classification."""
    paths.state.write_text(
        json.dumps({"version": 3, "seen": [600], "quarantined": [], "pending_citable": []}), encoding="utf-8"
    )
    parent = dialog(500, "channel", username="parent_news", linked_chat_id=600)
    chat = dialog(600, "megagroup", username="moneyforstartup_chat")

    report = run(paths, [parent, chat], tracked={"digest_sources": [lead_row(600)], "joins": []})

    assert report.added_chat == ["https://t.me/moneyforstartup_chat"]
    state = state_of(paths)
    assert state["version"] == 4
    assert 600 in state["decided"]["news"] and 600 in state["decided"]["leads"]


def test_a_failed_lead_enrolment_still_settles_the_news_surface(paths: ScanPaths) -> None:
    """Per-pipeline both ways: one surface failing must not re-open the other."""
    off = dialog(800, "channel", username="off_topic")

    def enroller(**payload):
        raise RuntimeError("proxy down")

    report = _run_scan(
        paths=paths,
        dialogs=[off],
        post_reader=Reader(default=[OFF] * 20),
        lead_enroller=enroller,
        tracked_reader=lambda: {"digest_sources": [], "joins": []},
        notifier=lambda text: True,
    )

    assert any("lead enrolment FAILED" in e for e in report.errors)
    state = state_of(paths)
    assert 800 in state["decided"]["news"], "the news verdict had real evidence — it stands"
    assert 800 not in state["decided"]["leads"], "the lead half failed — it retries"


# ── 8. the blocking invariant is untouched ────────────────────────


def test_an_unreadable_tracked_state_still_blocks_every_write(paths: ScanPaths) -> None:
    parent = dialog(500, "channel", username="parent_news", linked_chat_id=600)
    chat = dialog(600, "megagroup", username="moneyforstartup_chat")

    def broken() -> dict:
        raise RuntimeError("502 from the proxy")

    report = _run_scan(
        paths=paths,
        dialogs=[parent, chat],
        post_reader=Reader(),
        lead_enroller=lambda **kw: {},
        tracked_reader=broken,
        notifier=lambda text: True,
    )

    assert report.refused_to_write is True
    assert report.added_chat == [] and report.added_leads == []
    assert not paths.chat_sources.exists()
    assert report.state_written is False
