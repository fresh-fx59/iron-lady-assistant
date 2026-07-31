"""The nightly report must answer ONE question first: "do I need to do anything?"

Operator verdict on the 2026-07-31 live report (112 dialogs, 57 candidates, 8
enrolments, no errors): *"I do not understand what me expected to do."* Every
fact in it was true and none of it needed them — it just never said so.

These tests pin the answer, not the prose: the first line is a VERDICT, the
things that need a human carry a paste-able action, and the routine majority
collapses to counts. They are renderer-level on purpose — the rendering IS the
defect under test, and a Decision list is the cheapest honest way to state one.
"""
from __future__ import annotations

import re

import pytest

from src.telegram_dialog_scan import (
    HOLD_BELOW_THRESHOLD,
    HOLD_DENY_RULE,
    HOLD_NOT_CITABLE,
    HOLD_NOT_ENROLLABLE,
    HOLD_THIN_EVIDENCE,
    MAX_REPORT_CHARS,
    Decision,
    ScanPaths,
    ScanReport,
    TopicScore,
    operator_actions,
    render_report,
    run_scan,
)


def _d(
    entity_id: int,
    decision: str,
    hold: str = "",
    *,
    kind: str = "channel",
    username: str | None = None,
    reason: str = "because",
    news_target: str | None = None,
    score: TopicScore | None = None,
) -> Decision:
    return Decision(
        entity_id=entity_id,
        title=f"peer {entity_id}",
        kind=kind,
        username=username,
        decision=decision,
        reason=reason,
        news_target=news_target,
        topic_score=score,
        citable=username is not None,
        hold=hold,
    )


def _live_run() -> ScanReport:
    """THE run the operator complained about: 112 dialogs, 57 candidates
    evaluated, 8 enrolled, 3 unchanged since last run, zero errors."""
    enrolled = [
        _d(3141219398 + i, "enroll-both", kind="megagroup", username=f"chat_{i}",
           news_target="chat_sources", score=TopicScore(0.72, 21, 29, 30, "ok"))
        for i in range(8)
    ]
    below = [
        _d(4000 + i, "enroll-leads", HOLD_BELOW_THRESHOLD, username=f"low_{i}",
           score=TopicScore(0.10, 3, 30, 30, "ok"))
        for i in range(34)
    ]
    not_citable = [
        _d(5000 + i, "enroll-leads", HOLD_NOT_CITABLE, kind="megagroup") for i in range(12)
    ]
    skips = [_d(6000 + i, "skip", HOLD_NOT_ENROLLABLE, kind="user") for i in range(3)]
    return ScanReport(
        decisions=[*enrolled, *below, *not_citable, *skips],
        new_dialogs=[d.entity_id for d in [*enrolled, *below, *not_citable, *skips]],
        added_chat=[f"https://t.me/chat_{i}" for i in range(8)],
        added_leads=[d.entity_id for d in [*enrolled, *below, *not_citable]],
        still_pending=[7001, 7002, 7003],
        total_dialogs=112,
        by_kind={"channel": 60, "megagroup": 52},
        state_written=True,
        sources_path="/var/lib/iron-lady-aggregator/sources.txt",
        deny_path="/var/lib/iron-lady-aggregator/dialog_scan_deny.txt",
        mirror_path="/home/op/vault/areas/telegram/sources.md",
    )


# ── 1. the first line answers the question ────────────────────────


def test_the_live_run_that_needed_nothing_says_so_on_line_one() -> None:
    text = render_report(_live_run())

    assert text.splitlines()[0] == "dialog-scan: NOTHING TO DO"
    # …and the old headline ("57 new") is no longer the first thing they read.
    assert not text.startswith("dialog-scan: 112 dialogs")


def test_something_that_needs_a_human_says_so_on_line_one_with_a_count() -> None:
    report = _live_run()
    report.errors = ["state file write FAILED (/var/lib/x/state.json): No space left on device"]

    text = render_report(report)

    assert text.splitlines()[0] == "dialog-scan: NEEDS YOU: 1"


def test_every_action_item_carries_a_command_not_a_decision_row() -> None:
    """'Never make them infer an action from a decision row.'"""
    report = _live_run()
    report.decisions.append(
        _d(9001, "quarantine", HOLD_THIN_EVIDENCE, username="thin_chan",
           reason="topical gate: too few readable posts (2 scoreable of 30 read, need 8) — refusing to guess",
           score=TopicScore(0.0, 0, 2, 30, "thin"))
    )

    text = render_report(report)
    action_block = text.split("\n\n")[1]

    assert text.splitlines()[0] == "dialog-scan: NEEDS YOU: 1"
    assert "@thin_chan" in action_block
    # An exact, paste-able either/or — both live paths named in full.
    assert ">> /var/lib/iron-lady-aggregator/dialog_scan_deny.txt" in action_block
    assert ">> /var/lib/iron-lady-aggregator/sources.txt" in action_block


def test_a_vault_mirror_edit_names_the_file_and_the_lines() -> None:
    report = _live_run()
    report.added_news = ["https://t.me/fresh_ai_news"]
    report.mirror_pending = ["https://t.me/fresh_ai_news"]

    text = render_report(report)

    assert text.splitlines()[0] == "dialog-scan: NEEDS YOU: 1"
    assert "/home/op/vault/areas/telegram/sources.md" in text
    assert "https://t.me/fresh_ai_news" in text


# ── 2. the boundary: routine outcomes must NEVER page ──────────────


@pytest.mark.parametrize(
    "decision, hold",
    [
        ("enroll-both", ""),               # routine enrolment
        ("enroll-leads", HOLD_BELOW_THRESHOLD),   # routine below-threshold reject
        ("enroll-leads", HOLD_NOT_CITABLE),       # routine non-citable hold
        ("quarantine", HOLD_DENY_RULE),           # the operator's OWN rule firing
        ("skip", HOLD_NOT_ENROLLABLE),            # DM / bot / own channel
    ],
)
def test_the_system_working_as_designed_is_not_an_action(decision: str, hold: str) -> None:
    report = ScanReport(
        decisions=[_d(1, decision, hold, username="x", news_target="sources" if not hold else None)],
        new_dialogs=[1],
        total_dialogs=1,
        by_kind={"channel": 1},
    )

    assert operator_actions(report) == []
    assert render_report(report).splitlines()[0] == "dialog-scan: NOTHING TO DO"


def test_a_quarantine_the_scanner_refused_to_guess_on_does_page() -> None:
    """The one non-error page: the scanner is EXPLICITLY asking a human."""
    report = ScanReport(
        decisions=[_d(1, "quarantine", HOLD_THIN_EVIDENCE, username="thin_chan",
                      score=TopicScore(0.0, 0, 2, 30, "thin"))],
        new_dialogs=[1],
        total_dialogs=1,
        by_kind={"channel": 1},
    )

    assert len(operator_actions(report)) == 1


def test_a_repeat_quarantine_is_a_count_not_a_nightly_page() -> None:
    """A report that cries wolf nightly is the same defect in a different costume."""
    report = ScanReport(requarantined=[1, 2], new_dialogs=[], total_dialogs=9, by_kind={"channel": 9})

    text = render_report(report)

    assert operator_actions(report) == []
    assert text.splitlines()[0] == "dialog-scan: NOTHING TO DO"
    assert "2" in text and "unchanged since last run" in text


def test_a_notification_failure_pages_and_a_refused_write_is_stated_not_double_counted() -> None:
    """One event, one item: `refused_to_write` always travels WITH the error that
    caused it, so counting it separately would inflate the only number the
    operator reads."""
    report = _live_run()
    report.notify_failed = True
    report.refused_to_write = True
    report.errors = ["operator notification FAILED — this report reached NOBODY; check the journal"]

    text = render_report(report)

    assert text.splitlines()[0] == "dialog-scan: NEEDS YOU: 1"
    assert "NOTHING WAS WRITTEN" in text.upper()


# ── 2b. a healthy night sends NOTHING AT ALL (operator, 2026-07-31) ─


def _scan_paths(tmp_path) -> ScanPaths:
    return ScanPaths(
        sources=tmp_path / "sources.txt",
        chat_sources=tmp_path / "chat_sources.txt",
        mirror=tmp_path / "mirror.md",
        state=tmp_path / "state.json",
        deny=tmp_path / "deny.txt",
        topics=tmp_path / "topics.txt",
        lock=tmp_path / "dialog-scan.lock",
    )


_TOPICAL = "Новая модель OpenAI ускоряет инференс агентов в проде на 40 процентов"


def _channel(entity_id: int, username: str) -> dict:
    return {"kind": "channel", "entity_id": entity_id, "username": username, "title": username,
            "usernames": []}


def test_a_night_that_needs_nothing_sends_no_message_at_all(tmp_path) -> None:
    """Not a quieter message — NO message. The journal is the audit trail."""
    sent: list[str] = []
    report = run_scan(
        paths=_scan_paths(tmp_path),
        dialogs=[_channel(1, "ai_feed"), _channel(2, "ai_news")],
        dry_run=True,
        notifier=lambda text: sent.append(text) or True,
        tracked_reader=lambda: {},
        post_reader=lambda kind, entity_id: [_TOPICAL] * 20,
    )

    assert [d.decision for d in report.decisions] == ["enroll-both", "enroll-both"], report.text
    assert sent == [], "a run that needs nothing from the operator must not message them"
    # …and the three outcomes stay distinguishable.
    assert report.notify_skipped is True
    assert report.notified is False
    assert report.notify_failed is False


def test_a_night_that_needs_you_still_sends(tmp_path) -> None:
    sent: list[str] = []

    def unreadable(kind: str, entity_id: int) -> list[str]:
        raise RuntimeError("403 CHANNEL_PRIVATE")

    report = run_scan(
        paths=_scan_paths(tmp_path),
        dialogs=[_channel(1, "mystery_chan")],
        dry_run=True,
        notifier=lambda text: sent.append(text) or True,
        tracked_reader=lambda: {},
        post_reader=unreadable,
    )

    assert len(sent) == 1, report.text
    assert sent[0].splitlines()[0].startswith("dialog-scan: NEEDS YOU:")
    assert report.notified is True
    assert report.notify_skipped is False


# ── 3. honest header numbers ──────────────────────────────────────


def test_enrolled_comes_before_held_which_comes_before_evaluated() -> None:
    text = render_report(_live_run())

    enrolled_at = text.index("enrolled:")
    held_at = text.index("held:")
    evaluated_at = text.index("evaluated:")
    assert enrolled_at < held_at < evaluated_at, text


def test_the_counts_are_the_real_ones_not_the_candidate_count() -> None:
    text = render_report(_live_run())
    counts = {k: v for k, v in re.findall(r"^(enrolled|held|evaluated): (\d+)", text, re.M)}

    assert counts["enrolled"] == "8"       # what actually landed
    assert counts["held"] == "49"          # 34 + 12 + 3, and none of it is an action
    assert counts["evaluated"] == "57"     # the number that used to be the headline


def test_held_is_broken_down_by_reason() -> None:
    text = render_report(_live_run())
    held_line = next(line for line in text.splitlines() if line.startswith("held:"))

    assert "34 below the topical threshold" in held_line
    assert "12 no citable public handle" in held_line
    assert "3 unchanged since last run" in held_line


def test_routine_detail_collapses_to_counts_and_points_at_the_journal() -> None:
    text = render_report(_live_run())

    assert "journalctl -u telegram-dialog-scan" in text
    # The 8 real changes are listed; the 49 routine holds are not 49 rows in the
    # "what changed" list.
    changed = text.split("changed:")[1].split("\n\n")[0]
    assert changed.count("\n-") <= 8


# ── 4. self-healing, said out loud, once ──────────────────────────


def test_the_report_says_no_curation_is_expected_exactly_once() -> None:
    text = render_report(_live_run())

    assert text.lower().count("self-healing") == 1
    assert "re-added automatically" in text
    assert "Silence is NOT a removal reason" in text


# ── 5. the action section leads and survives the cap ──────────────


def test_actions_lead_and_survive_the_cap_against_a_huge_table() -> None:
    report = _live_run()
    report.decisions += [
        _d(8000 + i, "enroll-leads", HOLD_BELOW_THRESHOLD, username=f"noise_{i}",
           reason="broadcast channel scores 0.05 < 0.35 -> NOT a news source; leads only " + "x" * 60,
           score=TopicScore(0.05, 1, 30, 30, "ok"))
        for i in range(200)
    ]
    report.decisions.append(
        _d(9001, "quarantine", HOLD_THIN_EVIDENCE, username="thin_chan",
           reason="topical gate: too few readable posts (2 scoreable of 30 read, need 8) — refusing to guess",
           score=TopicScore(0.0, 0, 2, 30, "thin"))
    )
    report.errors = ["news sources.txt append FAILED (/var/lib/x/sources.txt): Read-only file system"]

    text = render_report(report)

    assert len(text) <= MAX_REPORT_CHARS
    assert text.splitlines()[0] == "dialog-scan: NEEDS YOU: 2"
    # BOTH actions present, whole, above everything else…
    assert "@thin_chan" in text
    assert "Read-only file system" in text
    assert text.index("ACTION 1") < text.index("ACTION 2") < text.index("enrolled:")
    # …and it is the TABLE that gets cut, explicitly.
    assert "TABLE TRUNCATED" in text


def test_an_action_flood_drops_whole_items_with_a_count_never_half_a_command() -> None:
    report = ScanReport(total_dialogs=3, by_kind={"channel": 3})
    report.errors = [f"ledger {i} unreadable: " + "x" * 200 for i in range(40)]

    text = render_report(report)

    assert len(text) <= MAX_REPORT_CHARS
    assert text.splitlines()[0] == "dialog-scan: NEEDS YOU: 40"
    assert "more item(s) need you" in text
    # No half-rendered action: the last ACTION line is complete.
    assert not text.rstrip().endswith("ACTION")
