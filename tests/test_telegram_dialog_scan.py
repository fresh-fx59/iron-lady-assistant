"""Daily NEW-dialog scanner: classification matrix, add-only enrolment, idempotency.

Every test injects a faked `/v1/dialogs` payload — no network, no Telethon, no
second client (see AUTH_KEY_DUPLICATED). The scanner's whole risk surface is
"what does it decide, and what does it write", so that is what is pinned here.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.telegram_dialog_scan import (
    OWN_PUBLISHING_CHANNELS,
    ScanPaths,
    Tracked,
    classify,
    load_deny_rules,
    parse_deny_rules,
    run_scan,
)
from src.telegram_digest import LEAD_SOURCE_ROLE, TelegramDigestStore

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
    linked_chat_id: int | None = None,
    participants_count: int | None = None,
) -> dict:
    return {
        "entity_id": entity_id,
        "title": title or f"dialog {entity_id}",
        "kind": kind,
        "username": username,
        "usernames": [username] if username else [],
        "is_broadcast": kind == "channel",
        "is_megagroup": kind == "megagroup",
        "participants_count": participants_count,
        "linked_chat_id": linked_chat_id,
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


# ── deny rules ────────────────────────────────────────────────────


def test_parse_deny_rules_reads_every_form_and_skips_comments() -> None:
    rules = parse_deny_rules(
        "\n".join(
            [
                "# personal, never enroll",
                "@zhkparusa",
                "pifagortrade",
                "id:-1001234",
                "word:крипто",
                "re:^travelbelka",
                "",
            ]
        )
    )

    assert [rule.kind for rule in rules] == ["user", "user", "id", "word", "re"]
    assert rules[0].value == "zhkparusa"
    # The marked -100 form is normalised to the raw positive id the proxy emits;
    # test_telegram_dialog_scan_defects.py pins that it actually MATCHES.
    assert rules[2].value == "1234"


def test_default_deny_rules_cover_the_operators_personal_channels(paths: ScanPaths) -> None:
    rules = load_deny_rules(paths.deny)
    denied = {rule.value for rule in rules if rule.kind == "user"}

    for handle in ("zhkparusa", "pifagortrade", "slezisatoshisliv2", "travelbelka_cards", "startupoftheday"):
        assert handle in denied


def test_deny_file_extends_the_defaults(paths: ScanPaths) -> None:
    paths.deny.write_text("@operator_extension\n")

    rules = load_deny_rules(paths.deny)

    values = {rule.value for rule in rules}
    assert "operator_extension" in values
    assert "zhkparusa" in values  # defaults still apply


# ── classification matrix ─────────────────────────────────────────


@pytest.mark.parametrize(
    "candidate,expected_decision",
    [
        (dialog(1, "channel", username="fresh_ai_news"), "enroll-both"),
        (dialog(2, "megagroup", username="some_dev_chat"), "enroll-leads"),
        (dialog(3, "group", username=None, linked_chat_id=None), "skip"),
        (dialog(4, "user"), "skip"),
        (dialog(5, "bot"), "skip"),
        (dialog(6, "channel", username="ai_daily_summary"), "skip"),
        (dialog(7, "channel", username="ai_in_modern_world"), "skip"),
        (dialog(8, "channel", username="zhkparusa"), "quarantine"),
        (dialog(9, "megagroup", username="pifagortrade_chat"), "quarantine"),
        (dialog(10, "channel", username=None), "skip"),
    ],
)
def test_classification_matrix(candidate, expected_decision, paths: ScanPaths) -> None:
    result = classify(candidate, tracked=None, deny_rules=load_deny_rules(paths.deny), linked_parents={}, topic_scores=_PASS)

    assert result.decision == expected_decision, result.reason
    assert result.reason


def test_own_publishing_channels_are_hard_coded_not_deny_file_dependent() -> None:
    assert {"ai_daily_summary", "ai_in_modern_world"} <= OWN_PUBLISHING_CHANNELS


def test_quarantine_reports_which_rule_fired(paths: ScanPaths) -> None:
    result = classify(
        dialog(11, "channel", username="zhkparusa"),
        tracked=None,
        deny_rules=load_deny_rules(paths.deny),
        linked_parents={},
        topic_scores=_PASS,
    )

    assert result.decision == "quarantine"
    assert "zhkparusa" in result.reason


def test_no_username_group_is_enrolled_when_it_is_a_tracked_channels_discussion_chat(
    paths: ScanPaths,
) -> None:
    parent = dialog(20, "channel", username="already_news", linked_chat_id=21)

    result = classify(
        dialog(21, "megagroup", username=None),
        tracked=None,
        deny_rules=[],
        linked_parents={21: parent},
        topic_scores=_PASS,
    )

    # UPDATED 2026-07-31 (live run): enrolled, yes — but into LEADS only. A
    # handle-less chat cannot be cited in a published digest, so chat_sources
    # would stage a permanently inert input. See the citability tests in
    # test_telegram_dialog_scan_defects.py.
    assert result.decision == "enroll-leads"
    assert result.news_target is None
    assert result.citable is False
    assert result.citability_blocked is True
    assert "already_news" in result.reason


def test_handle_less_chat_of_a_tracked_but_not_news_eligible_parent_goes_to_leads_only() -> None:
    # Parent has no username of its own => it is not (and cannot be) a news
    # source, so its chat must not reach the digest — but it IS verifiable.
    parent = dialog(23, "channel", username=None, title="AI для своих", linked_chat_id=24)
    tracked = Tracked(digest_peer_keys={"channel:23"})

    result = classify(dialog(24, "megagroup", username=None), tracked=tracked, deny_rules=[], linked_parents={24: parent}, topic_scores=_PASS)

    assert result.decision == "enroll-leads"
    assert result.news_target is None


def test_handle_less_chat_of_an_untracked_parent_is_never_enrolled() -> None:
    parent = dialog(25, "channel", username=None, title="Stranger", linked_chat_id=26)

    result = classify(
        dialog(26, "megagroup", username=None), tracked=Tracked(), deny_rules=[], linked_parents={26: parent}
    )

    assert result.decision == "skip"
    assert "no linked parent" in result.reason


def test_group_with_a_username_goes_to_leads_only_not_the_news_feed(paths: ScanPaths) -> None:
    result = classify(dialog(22, "megagroup", username="random_chat"), tracked=None, deny_rules=[], linked_parents={}, topic_scores=_PASS)

    assert result.decision == "enroll-leads"
    assert result.news_target is None


# ── enrolment ─────────────────────────────────────────────────────


def test_run_scan_enrolls_a_new_channel_into_news_and_leads(paths: ScanPaths) -> None:
    report = run_scan(paths=paths, dialogs=[dialog(30, "channel", username="fresh_ai_news", title="Fresh AI")])

    assert report.added_news == ["https://t.me/fresh_ai_news"]
    assert "https://t.me/fresh_ai_news" in paths.sources.read_text()
    assert "https://t.me/fresh_ai_news" in paths.mirror.read_text()
    assert report.mirror_pending == []
    lead_ids = {source.entity_id for source in TelegramDigestStore(paths.digest_db).list_sources([LEAD_SOURCE_ROLE])}
    assert 30 in lead_ids


def test_run_scan_writes_groups_to_chat_sources_in_the_shared_format(paths: ScanPaths) -> None:
    parent = dialog(40, "channel", username="already_news", linked_chat_id=41)
    child = dialog(41, "megagroup", username="already_news_chat", title="Already chat")

    report = run_scan(paths=paths, dialogs=[parent, child])

    # t.me URL form — the ONLY form written now. The bare-id form the reader
    # still accepts (the other branch's format) can only ever describe a
    # handle-less chat, and those are refused chat_sources outright since the
    # 2026-07-31 live run proved them inert in the digest.
    assert paths.chat_sources.read_text().strip().splitlines()[-1] == "https://t.me/already_news_chat"
    assert report.added_chat == ["https://t.me/already_news_chat"]


def test_run_scan_never_touches_existing_lines(paths: ScanPaths) -> None:
    before = paths.sources.read_text()

    run_scan(paths=paths, dialogs=[dialog(50, "channel", username="fresh_ai_news")])

    after = paths.sources.read_text()
    assert after.startswith(before)  # add-only: a bad add is a one-line revert


def test_run_scan_is_idempotent(paths: ScanPaths) -> None:
    dialogs = [dialog(60, "channel", username="fresh_ai_news"), dialog(61, "megagroup", username="dev_chat")]

    first = run_scan(paths=paths, dialogs=dialogs)
    snapshot = (paths.sources.read_text(), paths.chat_sources.read_text() if paths.chat_sources.exists() else "")
    second = run_scan(paths=paths, dialogs=dialogs)

    assert first.new_dialogs and not second.new_dialogs
    assert second.added_news == [] and second.added_chat == [] and second.added_leads == []
    assert (paths.sources.read_text(), paths.chat_sources.read_text() if paths.chat_sources.exists() else "") == snapshot
    assert second.notified is False


def test_run_scan_does_not_repropose_what_a_pipeline_already_tracks(paths: ScanPaths) -> None:
    """PER-SURFACE (2026-07-31): sources.txt membership settles the NEWS surface
    and nothing else. The channel is still a LEADS candidate — the mirror image
    of the lead_entity_ids short-circuit that blocked chat_sources for 102 ids."""
    report = run_scan(paths=paths, dialogs=[dialog(70, "channel", username="already_news")])

    assert report.added_news == [], "never a second sources.txt line"
    assert report.added_leads == [70], "but the lead pipeline had never seen it"


def test_run_scan_reports_a_missing_vault_mirror_instead_of_silently_skipping_it(paths: ScanPaths) -> None:
    paths.mirror.unlink()

    report = run_scan(paths=paths, dialogs=[dialog(80, "channel", username="fresh_ai_news")])

    assert report.added_news == ["https://t.me/fresh_ai_news"]
    assert report.mirror_pending == ["https://t.me/fresh_ai_news"]
    assert "mirror" in report.text.lower()


# ── dry run + reporting ───────────────────────────────────────────


def test_dry_run_writes_nothing_at_all(paths: ScanPaths) -> None:
    before = (paths.sources.read_text(), paths.mirror.read_text())

    report = run_scan(
        paths=paths,
        dialogs=[dialog(90, "channel", username="fresh_ai_news"), dialog(91, "megagroup", username="dev_chat")],
        dry_run=True,
    )

    assert (paths.sources.read_text(), paths.mirror.read_text()) == before
    assert not paths.chat_sources.exists()
    assert not paths.state.exists()
    assert not paths.digest_db.exists()
    assert not paths.join_db.exists()
    assert report.notified is False
    assert "enroll-both" in report.text and "dev_chat" in report.text


def test_dry_run_twice_still_sees_the_same_new_dialogs(paths: ScanPaths) -> None:
    dialogs = [dialog(92, "channel", username="fresh_ai_news")]

    assert len(run_scan(paths=paths, dialogs=dialogs, dry_run=True).new_dialogs) == 1
    assert len(run_scan(paths=paths, dialogs=dialogs, dry_run=True).new_dialogs) == 1


def test_nothing_new_means_no_report_at_all(paths: ScanPaths) -> None:
    sent: list[str] = []

    report = run_scan(paths=paths, dialogs=[dialog(100, "user"), dialog(101, "bot")], notifier=sent.append)

    # user/bot dialogs are never enrollable and never worth waking the operator.
    assert sent == []
    assert report.notified is False


def test_a_new_enrolment_notifies_the_operator_once(paths: ScanPaths) -> None:
    sent: list[str] = []

    def notifier(text: str) -> bool:
        # Mirrors notify_operator's contract: True = delivered. run_scan USES the
        # return value, so a test double must be honest about delivery.
        sent.append(text)
        return True

    report = run_scan(paths=paths, dialogs=[dialog(110, "channel", username="fresh_ai_news")], notifier=notifier)

    assert len(sent) == 1
    assert "fresh_ai_news" in sent[0]
    assert report.notified is True


def test_state_file_records_a_decision_per_surface_not_a_global_seen_flag(paths: ScanPaths) -> None:
    """The old `seen` was written from EVERY dialog id — including ids the global
    tracked-filter dropped before classification, which is what made "lead-tracked"
    mean "done with, forever". Only surfaces this run actually settled are recorded."""
    run_scan(paths=paths, dialogs=[dialog(120, "channel", username="fresh_ai_news"), dialog(121, "user")])

    decided = json.loads(paths.state.read_text())["decided"]
    assert 120 in decided["news"] and 120 in decided["leads"]
    # A DM is enrollable nowhere, so it is never a candidate and needs no record.
    assert 121 not in decided["news"] and 121 not in decided["leads"]
