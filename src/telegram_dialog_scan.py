"""src/telegram_dialog_scan.py — daily NEW-dialog scanner + auto-enroller.

`/v1/channels` only ever returned broadcast peers, so a hand-joined group (or a
pending join request an admin finally approved) was invisible to both pipelines.
This walks the new `GET /v1/dialogs`, diffs against what the scanner has already
seen AND what each pipeline tracks, classifies the rest by EXPLICIT RULES (no
LLM), and enrolls the survivors. Two hard safety properties, because it writes
live pipeline inputs: ADD-ONLY (so a bad auto-add is a one-line revert) and
never upsert over an existing digest_sources row (that upsert rewrites `role`
and would silently turn a news source into a lead source). Reports to the
operator only when something changed or broke.
"""
from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

from .telegram_aggregator import parse_sources, resolve_paths
from .telegram_digest import LEAD_SOURCE_ROLE, TelegramDigestStore, _peer_key, sync_joined_sources

logger = logging.getLogger(__name__)

# The operator's OWN publishing channels — enrolling them feeds the digest its
# own output. Hard-coded never-enroll, never deny-file-dependent.
OWN_PUBLISHING_CHANNELS = {"ai_daily_summary", "ai_in_modern_world"}

# Seeded deny list: both pipelines are topical (RU AI/tech news, AI-services
# leads) and the account also holds the operator's PERSONAL channels, which
# would poison both. Extend by editing config/dialog_scan_deny.txt; this is the
# floor. Prefer "quarantine when unsure" over any topic inference.
DEFAULT_DENY_RULES_TEXT = """
@zhkparusa
@pifagortrade
@slezisatoshisliv2
@travelbelka_cards
@startupoftheday
word:pifagortrade
word:satoshi
word:travelbelka
word:zhkparusa
"""

_VAULT_MIRROR = "/home/claude-developer/personal-os/references/telegram-aggregator-sources.txt"
_REPO_DENY = Path(__file__).resolve().parent.parent / "config" / "dialog_scan_deny.txt"


@dataclass(frozen=True)
class DenyRule:
    kind: str  # user | id | word | re
    value: str

    def label(self) -> str:
        return f"{self.kind}:{self.value}"


@dataclass(frozen=True)
class ScanPaths:
    sources: Path
    chat_sources: Path
    mirror: Path
    state: Path
    deny: Path
    join_db: Path
    digest_db: Path


@dataclass(frozen=True)
class Decision:
    entity_id: int
    title: str
    kind: str
    username: str | None
    decision: str  # enroll-news | enroll-leads | enroll-both | quarantine | skip
    reason: str
    news_target: str | None = None  # "sources" | "chat_sources" | None


@dataclass
class Tracked:
    news_handles: set[str] = field(default_factory=set)
    chat_entries: set[str] = field(default_factory=set)
    digest_peer_keys: set[str] = field(default_factory=set)
    lead_entity_ids: set[int] = field(default_factory=set)
    join_targets: set[str] = field(default_factory=set)
    seen_ids: set[int] = field(default_factory=set)


@dataclass
class ScanReport:
    decisions: list[Decision] = field(default_factory=list)
    new_dialogs: list[int] = field(default_factory=list)
    added_news: list[str] = field(default_factory=list)
    added_chat: list[str] = field(default_factory=list)
    added_leads: list[int] = field(default_factory=list)
    mirror_pending: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    total_dialogs: int = 0
    by_kind: dict[str, int] = field(default_factory=dict)
    dry_run: bool = False
    text: str = ""
    notified: bool = False


def resolve_scan_paths() -> ScanPaths:
    """State lives beside the aggregator's; every path is env-overridable."""
    agg = resolve_paths()
    env = os.getenv
    return ScanPaths(
        sources=agg.sources_path,
        chat_sources=Path(env("AGGREGATOR_CHAT_SOURCES_PATH") or agg.sources_path.parent / "chat_sources.txt"),
        mirror=Path(env("DIALOG_SCAN_MIRROR_PATH") or _VAULT_MIRROR),
        state=Path(env("DIALOG_SCAN_STATE_PATH") or agg.state_dir / "dialog_scan_seen.json"),
        deny=Path(env("DIALOG_SCAN_DENY_PATH") or _REPO_DENY),
        join_db=Path(env("TELEGRAM_PROXY_JOIN_DB_PATH") or "/var/lib/iron-lady/memory/telegram_join.db"),
        digest_db=Path(env("TELEGRAM_DIGEST_DB_PATH") or "/var/lib/iron-lady/memory/telegram_digest.db"),
    )


def parse_deny_rules(text: str) -> list[DenyRule]:
    """One rule per line: `id:<n>` | `word:<substr>` | `re:<regex>` | @handle."""
    rules: list[DenyRule] = []
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        kind, _, value = line.partition(":")
        kind, value = kind.strip().lower(), value.strip()
        if kind in {"id", "word", "re"} and value:
            rules.append(DenyRule(kind, value if kind == "re" else value.lower()))
        else:
            rules.append(DenyRule("user", line.lstrip("@").strip().lower()))
    return rules


def load_deny_rules(path: Path | None) -> list[DenyRule]:
    """Defaults are the floor; the operator's file only ever ADDS to them."""
    rules = parse_deny_rules(DEFAULT_DENY_RULES_TEXT)
    if path and path.exists():
        rules.extend(parse_deny_rules(path.read_text(encoding="utf-8")))
    return rules


def deny_match(rules: Sequence[DenyRule], candidate: dict[str, Any]) -> DenyRule | None:
    handles = {str(h).lower() for h in (candidate.get("usernames") or []) if h}
    if candidate.get("username"):
        handles.add(str(candidate["username"]).lower())
    haystack = " ".join([str(candidate.get("title") or "").lower(), *sorted(handles)])
    for rule in rules:
        if rule.kind == "user" and rule.value in handles:
            return rule
        if rule.kind == "id" and rule.value == str(candidate.get("entity_id")):
            return rule
        if rule.kind == "word" and rule.value in haystack:
            return rule
        if rule.kind == "re":
            try:
                if re.search(rule.value, haystack, re.IGNORECASE):
                    return rule
            except re.error:
                logger.warning("bad deny regex ignored: %s", rule.value)
    return None


def _news_eligible(candidate: dict[str, Any], rules: Sequence[DenyRule]) -> bool:
    handle = (candidate.get("username") or "").lower()
    return bool(
        candidate.get("kind") == "channel"
        and handle
        and handle not in OWN_PUBLISHING_CHANNELS
        and deny_match(rules, candidate) is None
    )


def _parent_tracked(parent: dict[str, Any], tracked: Tracked | None) -> bool:
    """Is this parent channel something a pipeline already knows about?"""
    if tracked is None:
        return True  # no ledger to check against — presence in the dialog list is all we have
    handle = (parent.get("username") or "").lower()
    return bool(
        (handle and handle in tracked.news_handles)
        or _peer_key("channel", int(parent.get("entity_id") or 0)) in tracked.digest_peer_keys
    )


def classify(
    candidate: dict[str, Any],
    *,
    tracked: Tracked | None,
    deny_rules: Sequence[DenyRule],
    linked_parents: dict[int, dict[str, Any]],
) -> Decision:
    """Explicit rules only, checked in order. When unsure: never enroll."""
    kind = str(candidate.get("kind") or "user")
    handle = (candidate.get("username") or "").strip() or None
    entity_id = int(candidate.get("entity_id") or 0)

    def out(decision: str, reason: str, news_target: str | None = None) -> Decision:
        return Decision(entity_id, str(candidate.get("title") or ""), kind, handle, decision, reason, news_target)

    if kind in {"user", "bot"}:
        return out("skip", f"never-enroll: {kind} dialog (DM / bot / Saved Messages)")
    if handle and handle.lower() in OWN_PUBLISHING_CHANNELS:
        return out("skip", f"never-enroll: operator's own publishing channel @{handle}")
    rule = deny_match(deny_rules, candidate)
    if rule is not None:
        return out("quarantine", f"deny rule fired: {rule.label()}")

    if kind == "channel":
        if not handle:
            return out("skip", "never-enroll: broadcast channel with no username (unaddressable)")
        return out("enroll-both", f"broadcast channel @{handle} -> news + leads", "sources")

    if kind in {"megagroup", "group"}:
        parent = linked_parents.get(entity_id)
        if parent is not None and _news_eligible(parent, deny_rules):
            return out(
                "enroll-both",
                f"discussion chat of news channel @{parent.get('username')} -> leads + chat_sources",
                "chat_sources",
            )
        if handle:
            return out("enroll-leads", f"group @{handle} -> leads (news needs an operator chat_sources add)")
        # No handle of its own, but a tracked parent makes it verifiable and the
        # joins door keys on entity_id, so leads still works. News does not: the
        # parent is not a news source, so its chat has no business in the digest.
        if parent is not None and _parent_tracked(parent, tracked):
            return out("enroll-leads", f"discussion chat of tracked non-news channel {parent.get('title')!r} -> leads")
        return out("skip", "never-enroll: no username AND no linked parent (unaddressable, unverifiable)")

    return out("quarantine", f"unknown peer kind {kind!r} — refusing to guess")


def _chat_source_keys(text: str) -> set[str]:
    """chat_sources.txt accepts a t.me URL or a bare numeric id (other branch's format)."""
    keys: set[str] = set()
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        if re.fullmatch(r"-?\d+", line):
            keys.add(line)
        else:
            keys.update(name.lower() for name in parse_sources(line))
    return keys


def load_tracked(paths: ScanPaths) -> Tracked:
    tracked = Tracked()
    if paths.sources.exists():
        tracked.news_handles = {n.lower() for n in parse_sources(paths.sources.read_text(encoding="utf-8"))}
    if paths.chat_sources.exists():
        tracked.chat_entries = _chat_source_keys(paths.chat_sources.read_text(encoding="utf-8"))
    if paths.state.exists():
        try:
            tracked.seen_ids = {int(i) for i in json.loads(paths.state.read_text())["seen"]}
        except Exception as exc:  # noqa: BLE001 — a corrupt state file must not stop the scan
            logger.warning("dialog-scan state unreadable (%s); treating every dialog as new", exc)
    if paths.digest_db.exists():
        with sqlite3.connect(paths.digest_db, timeout=10) as con:
            for peer_key, entity_id, role in con.execute(
                "SELECT peer_key, entity_id, role FROM digest_sources"
            ).fetchall():
                tracked.digest_peer_keys.add(str(peer_key))
                if role == LEAD_SOURCE_ROLE:
                    tracked.lead_entity_ids.add(int(entity_id))
    if paths.join_db.exists():
        try:
            with sqlite3.connect(paths.join_db, timeout=10) as con:
                tracked.join_targets = {str(r[0]) for r in con.execute("SELECT target FROM joins").fetchall()}
        except sqlite3.OperationalError:
            pass  # joins table not initialised yet
    return tracked


def _already_tracked(candidate: dict[str, Any], tracked: Tracked) -> bool:
    entity_id = int(candidate.get("entity_id") or 0)
    handle = (candidate.get("username") or "").lower()
    return bool(
        entity_id in tracked.seen_ids
        or entity_id in tracked.lead_entity_ids
        or (handle and handle in tracked.news_handles)
        or (handle and handle in tracked.chat_entries)
        or str(entity_id) in tracked.chat_entries
    )


def _append_lines(path: Path, lines: Sequence[str], stamp: str) -> None:
    """ADD-ONLY append with a dated header; existing bytes are never rewritten."""
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    prefix = "" if (not existing or existing.endswith("\n")) else "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"{prefix}# added by dialog-scan {stamp}\n" + "\n".join(lines) + "\n")


def _register_leads(paths: ScanPaths, items: Sequence[Decision], tracked: Tracked) -> tuple[list[int], list[str]]:
    """Groups go through the EXISTING door: joins row -> sync_joined_sources().

    A broadcast channel cannot: sync_joined_sources hard-codes kind="linked_chat"
    (right for a joined megagroup, wrong for a channel — the reader would use the
    wrong peer kind), so a channel is inserted directly with kind="channel".
    Either way an EXISTING peer_key/target is left completely alone.
    """
    added: list[int] = []
    errors: list[str] = []
    store = TelegramDigestStore(paths.digest_db)
    group_rows: list[tuple[str, int]] = []
    now = datetime.now(timezone.utc).isoformat()
    for item in items:
        kind = "channel" if item.kind == "channel" else "linked_chat"
        if _peer_key(kind, item.entity_id) in tracked.digest_peer_keys:
            continue
        if kind == "channel":
            store.upsert_source(
                peer_key=_peer_key("channel", item.entity_id),
                entity_id=item.entity_id,
                title=item.title or f"channel:{item.entity_id}",
                username=item.username,
                kind="channel",
                linked_channel_key=None,
                role=LEAD_SOURCE_ROLE,
            )
        else:
            target = f"@{item.username}" if item.username else (item.title or f"dialog:{item.entity_id}")
            if target in tracked.join_targets:
                continue
            group_rows.append((target, item.entity_id))
        added.append(item.entity_id)

    if group_rows:
        try:
            paths.join_db.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(paths.join_db, timeout=30) as con:
                con.execute(
                    """CREATE TABLE IF NOT EXISTS joins (
                        target TEXT PRIMARY KEY, kind TEXT NOT NULL, status TEXT NOT NULL,
                        entity_id INTEGER, error TEXT, created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL, joined_at TEXT,
                        attempts INTEGER NOT NULL DEFAULT 0, retry_at TEXT)"""
                )
                # status='joined' is the truth (we are already in these dialogs) AND
                # inert: the proxy's join loop only selects pending/floodwait rows.
                con.executemany(
                    "INSERT OR IGNORE INTO joins(target, kind, status, entity_id, created_at, updated_at, joined_at)"
                    " VALUES (?, 'username', 'joined', ?, ?, ?, ?)",
                    [(t, e, now, now, now) for t, e in group_rows],
                )
            sync_joined_sources(store, paths.join_db)
        except sqlite3.Error as exc:
            errors.append(f"lead enrolment failed for {[t for t, _ in group_rows]}: {exc}")
            added = [i for i in added if i not in {e for _, e in group_rows}]
    return added, errors


def render_report(report: ScanReport) -> str:
    kinds = ", ".join(f"{k}={v}" for k, v in sorted(report.by_kind.items()))
    head = "dialog-scan" + (" (DRY RUN)" if report.dry_run else "")
    lines = [f"{head}: {report.total_dialogs} dialogs ({kinds}); {len(report.new_dialogs)} new"]
    if report.decisions:
        lines += ["", f"{'decision':<13} {'kind':<10} {'id':>14}  handle / title — why"]
        for d in sorted(report.decisions, key=lambda d: (d.decision, d.kind)):
            who = f"@{d.username}" if d.username else (d.title or f"dialog:{d.entity_id}")
            lines.append(f"{d.decision:<13} {d.kind:<10} {d.entity_id:>14}  {who} — {d.reason}")
    if report.added_news:
        lines += ["", f"news sources.txt += {report.added_news}"]
    if report.added_chat:
        lines.append(f"news chat_sources.txt += {report.added_chat}")
    if report.added_leads:
        lines.append(f"lead sources += {report.added_leads}")
    if report.mirror_pending:
        lines += ["", f"ACTION: vault mirror needs a HUMAN edit (missing/unwritable): {report.mirror_pending}"]
    if report.errors:
        lines += ["", "ERRORS:", *[f"- {e}" for e in report.errors]]
    return "\n".join(lines)


def run_scan(
    *,
    paths: ScanPaths,
    dialogs: Iterable[dict[str, Any]],
    dry_run: bool = False,
    notifier: Callable[[str], Any] | None = None,
) -> ScanReport:
    report = ScanReport(dry_run=dry_run)
    dialogs = list(dialogs)
    report.total_dialogs = len(dialogs)
    for candidate in dialogs:
        kind = str(candidate.get("kind") or "unknown")
        report.by_kind[kind] = report.by_kind.get(kind, 0) + 1

    tracked = load_tracked(paths)
    deny_rules = load_deny_rules(paths.deny)
    # Built from ALL dialogs, before the tracked filter: an already-tracked
    # channel is still the parent that makes its discussion chat verifiable.
    linked_parents = {
        int(d["linked_chat_id"]): d for d in dialogs if d.get("linked_chat_id") and d.get("kind") == "channel"
    }

    fresh = [d for d in dialogs if not _already_tracked(d, tracked)]
    report.new_dialogs = [int(d.get("entity_id") or 0) for d in fresh]
    report.decisions = [
        classify(d, tracked=tracked, deny_rules=deny_rules, linked_parents=linked_parents) for d in fresh
    ]

    stamp = datetime.now(timezone.utc).date().isoformat()
    news_lines = [
        f"https://t.me/{d.username}"
        for d in report.decisions
        if d.news_target == "sources" and d.username and d.username.lower() not in tracked.news_handles
    ]
    chat_lines = [
        line
        for line in (
            (f"https://t.me/{d.username}" if d.username else str(d.entity_id))
            for d in report.decisions
            if d.news_target == "chat_sources"
        )
        if line.lower() not in tracked.chat_entries
    ]
    lead_items = [d for d in report.decisions if d.decision in {"enroll-leads", "enroll-both"}]

    if not dry_run:
        if news_lines:
            _append_lines(paths.sources, news_lines, stamp)
            report.added_news = news_lines
            # The runbook makes sources.txt + the vault mirror a PAIRED edit; if we
            # cannot do the pair ourselves, say so loudly instead of drifting.
            if paths.mirror.exists():
                try:
                    _append_lines(paths.mirror, news_lines, stamp)
                except OSError as exc:
                    report.errors.append(f"vault mirror append failed: {exc}")
                    report.mirror_pending = news_lines
            else:
                report.mirror_pending = news_lines
        if chat_lines:
            _append_lines(paths.chat_sources, chat_lines, stamp)
            report.added_chat = chat_lines
        if lead_items:
            report.added_leads, errors = _register_leads(paths, lead_items, tracked)
            report.errors.extend(errors)
        paths.state.parent.mkdir(parents=True, exist_ok=True)
        seen = sorted(tracked.seen_ids | {int(d.get("entity_id") or 0) for d in dialogs})
        paths.state.write_text(json.dumps({"updated": stamp, "seen": seen}, ensure_ascii=False))

    report.text = render_report(report)
    # Problems-and-changes only: a run whose new dialogs are all skips (a DM, a
    # bot) stays silent. Quarantines DO report — the operator decides.
    worth_saying = bool(report.errors) or any(d.decision != "skip" for d in report.decisions)
    if worth_saying and notifier is not None and not dry_run:
        notifier(report.text)
        report.notified = True
    return report
