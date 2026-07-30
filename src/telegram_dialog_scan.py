"""src/telegram_dialog_scan.py — daily NEW-dialog scanner + auto-enroller.

`/v1/channels` only ever returned broadcast peers, so a hand-joined group (or a
pending join request an admin finally approved) was invisible to both pipelines.
This walks the new `GET /v1/dialogs`, diffs against what the scanner has already
seen AND what each pipeline tracks, classifies the rest by EXPLICIT RULES (no
LLM), and enrolls the survivors.

Three hard safety properties, because it writes live pipeline inputs:

  * ADD-ONLY — a bad auto-add is a one-line revert.
  * NEVER rewrite an existing digest_sources row (that upsert used to rewrite
    `role` and silently turned a news source into a lead source; the role column
    is now a SET and `add_source_role` only ever grows it).
  * NO SILENT HALF-WRITE — the durable sqlite half goes first, per item; the
    visible sources.txt half is appended only for items whose durable half
    completed; and an id enters the `seen` state file ONLY when everything it
    needed actually happened. Every failure names exactly what was and was not
    written.

Reports to the operator only when something changed or broke.
"""
from __future__ import annotations

import fcntl
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
from .telegram_digest import LEAD_SOURCE_ROLE, TelegramDigestStore, _peer_key
from .telegram_proxy import normalize_target, parse_public_username

logger = logging.getLogger(__name__)

# The operator's OWN publishing channels — enrolling them feeds the digest its
# own output. Hard-coded never-enroll, never deny-file-dependent.
OWN_PUBLISHING_CHANNELS = {"ai_daily_summary", "ai_in_modern_world"}

# Seeded deny list: both pipelines are topical (RU AI/tech news, AI-services
# leads) and the account also holds the operator's PERSONAL channels, which
# would poison both. Extend by editing the deny file; this is the floor.
# Prefer "quarantine when unsure" over any topic inference.
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

# notify_operator() hands the text to the Bot API sliced at 4000 chars. The
# report is RENDERED to that budget instead of being cut by it, so the parts the
# operator must act on can never be the parts that fall off the end.
MAX_REPORT_CHARS = 4000

# A username Telegram would actually accept (used to reject a malformed deny rule).
_HANDLE_RE = re.compile(r"^[a-z0-9_]{3,32}$")

_STATE_VERSION = 2


@dataclass(frozen=True)
class DenyRule:
    kind: str  # user | id | word | re | invalid
    value: str
    error: str = ""

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
    lock: Path | None = None

    def lock_path(self) -> Path:
        return self.lock or self.state.parent / "dialog-scan.lock"


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
    quarantined_ids: set[int] = field(default_factory=set)
    # DISQUALIFYING read failures: an unreadable ledger means "I don't know what
    # is already tracked", which must never be treated as "nothing is tracked".
    errors: list[str] = field(default_factory=list)


@dataclass
class ScanReport:
    decisions: list[Decision] = field(default_factory=list)
    new_dialogs: list[int] = field(default_factory=list)
    added_news: list[str] = field(default_factory=list)
    added_chat: list[str] = field(default_factory=list)
    added_leads: list[int] = field(default_factory=list)
    mirror_pending: list[str] = field(default_factory=list)
    ledger_notes: list[str] = field(default_factory=list)
    requarantined: list[int] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    total_dialogs: int = 0
    by_kind: dict[str, int] = field(default_factory=dict)
    dry_run: bool = False
    skipped_locked: bool = False
    wrote_nothing: bool = False
    text: str = ""
    notified: bool = False
    notify_failed: bool = False


def resolve_scan_paths() -> ScanPaths:
    """State lives beside the aggregator's; every path is env-overridable."""
    agg = resolve_paths()
    env = os.getenv
    return ScanPaths(
        sources=agg.sources_path,
        chat_sources=Path(env("AGGREGATOR_CHAT_SOURCES_PATH") or agg.sources_path.parent / "chat_sources.txt"),
        mirror=Path(env("DIALOG_SCAN_MIRROR_PATH") or _VAULT_MIRROR),
        state=Path(env("DIALOG_SCAN_STATE_PATH") or agg.state_dir / "dialog_scan_seen.json"),
        # Default is the in-repo SEED only. In production DIALOG_SCAN_DENY_PATH
        # points at the operator-editable copy in the aggregator state dir: the
        # repo checkout is `git checkout --force`-ed by the draft runner, which
        # would discard (or block) an operator edit made in place.
        deny=Path(env("DIALOG_SCAN_DENY_PATH") or _REPO_DENY),
        join_db=Path(env("TELEGRAM_PROXY_JOIN_DB_PATH") or "/var/lib/iron-lady/memory/telegram_join.db"),
        digest_db=Path(env("TELEGRAM_DIGEST_DB_PATH") or "/var/lib/iron-lady/memory/telegram_digest.db"),
        lock=Path(env("DIALOG_SCAN_LOCK_PATH") or agg.state_dir / "dialog-scan.lock"),
    )


# ── deny rules ────────────────────────────────────────────────────


def canonical_entity_id(raw: Any) -> int | None:
    """One canonical id for the THREE forms the same peer is written in.

    Telethon (and therefore `/v1/dialogs`) exposes the raw positive `entity.id`
    (1609072825). Telegram clients — and every "copy id" button — show the MARKED
    form (-1001609072825 for a channel/supergroup, -4712345 for a legacy group),
    which is also what the deny file's own documented example uses. Collapsing
    all of them to the positive id is what makes `id:` rules actually fire;
    comparing raw strings is what made them inert.
    """
    text = str(raw or "").strip()
    if not text:
        return None
    if text.startswith("-100"):
        text = text[len("-100"):]
    elif text.startswith("-"):
        text = text[1:]
    if not text.isdigit():
        return None
    return int(text)


def parse_deny_rules(text: str) -> list[DenyRule]:
    """One rule per line: `id:<n>` | `word:<substr>` | `re:<regex>` | a handle.

    The handle form accepts every shape an operator will actually paste — `@foo`,
    `foo`, `t.me/foo`, `https://t.me/foo` (the form sources.txt itself uses) —
    because `partition(":")` on a URL used to yield kind="https" and fall through
    to an inert `user` rule. Anything unrecognisable becomes an `invalid` rule so
    it gets REPORTED (a typo'd `re:` used to only warn to the journal, and the
    dialog it was meant to stop auto-enrolled).
    """
    rules: list[DenyRule] = []
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        prefix, sep, value = line.partition(":")
        kind, value = prefix.strip().lower(), value.strip()
        if sep and kind in {"id", "word", "re"}:
            if not value:
                rules.append(DenyRule("invalid", line, f"empty {kind}: rule"))
            elif kind == "re":
                try:
                    re.compile(value)
                except re.error as exc:
                    rules.append(DenyRule("invalid", line, f"invalid regex ({exc})"))
                else:
                    rules.append(DenyRule("re", value))
            elif kind == "id":
                canonical = canonical_entity_id(value)
                if canonical is None:
                    rules.append(DenyRule("invalid", line, "id: needs a number (-100…, -… or raw)"))
                else:
                    rules.append(DenyRule("id", str(canonical)))
            else:
                rules.append(DenyRule("word", value.lower()))
            continue
        handle = parse_public_username(line).lower()
        if _HANDLE_RE.match(handle):
            rules.append(DenyRule("user", handle))
        else:
            rules.append(DenyRule("invalid", line, "not a handle, a t.me link, or an id:/word:/re: rule"))
    return rules


def load_deny_rules(path: Path | None) -> list[DenyRule]:
    """Defaults are the floor; the operator's file only ever ADDS to them."""
    rules = parse_deny_rules(DEFAULT_DENY_RULES_TEXT)
    if path is None:
        return rules
    try:
        if path.exists():
            rules.extend(parse_deny_rules(path.read_text(encoding="utf-8")))
    except OSError as exc:
        rules.append(DenyRule("invalid", str(path), f"deny file unreadable ({exc})"))
    return rules


def deny_rule_errors(rules: Sequence[DenyRule]) -> list[str]:
    """Unparseable rules, phrased for the operator's report — never silent."""
    return [
        f"deny rule ignored (it denies NOTHING): {rule.value!r} — {rule.error}"
        for rule in rules
        if rule.kind == "invalid"
    ]


def _all_handles(candidate: dict[str, Any]) -> set[str]:
    handles = {str(h).lower().lstrip("@") for h in (candidate.get("usernames") or []) if h}
    if candidate.get("username"):
        handles.add(str(candidate["username"]).lower().lstrip("@"))
    return {h for h in handles if h}


def deny_match(rules: Sequence[DenyRule], candidate: dict[str, Any]) -> DenyRule | None:
    """Match the title AND every username, case-insensitively, for every form."""
    handles = _all_handles(candidate)
    haystack = " ".join([str(candidate.get("title") or ""), *sorted(handles)]).lower()
    entity_id = canonical_entity_id(candidate.get("entity_id"))
    for rule in rules:
        if rule.kind == "user" and rule.value in handles:
            return rule
        if rule.kind == "id" and entity_id is not None and rule.value == str(entity_id):
            return rule
        if rule.kind == "word" and rule.value in haystack:
            return rule
        if rule.kind == "re":
            try:
                if re.search(rule.value, haystack, re.IGNORECASE):
                    return rule
            except re.error:
                continue  # already surfaced by deny_rule_errors()
    return None


# ── the one classification policy decision ────────────────────────


def broadcast_channel_is_a_news_source(candidate: dict[str, Any], rules: Sequence[DenyRule]) -> bool:
    """DEFAULT-ALLOW: any broadcast channel with an undenied handle IS a news source.

    THE one classification policy decision in this module, isolated here so that
    gating it is a one-place change. Today the only bars are: it must be a
    broadcast channel, it must have a public handle, it must not be one of the
    operator's own publishing channels, and no deny rule may fire.

    Consequence of default-allow, stated plainly: a channel the account joins for
    ANY reason — a shop, a friend's blog, a one-off announcement feed — becomes a
    source of the PUBLIC @ai_daily_summary digest on the next 01:47 UTC scan
    unless the operator denied it FIRST. There is no topic check; the deny list is
    the entire brake. (An operator-facing gate — propose-only, or a topic
    allow-list — replaces the body of this function and nothing else.)
    """
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
    return bool(
        (_all_handles(parent) & tracked.news_handles)
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
    if _all_handles(candidate) & OWN_PUBLISHING_CHANNELS:
        return out("skip", f"never-enroll: operator's own publishing channel @{handle}")
    rule = deny_match(deny_rules, candidate)
    if rule is not None:
        return out("quarantine", f"deny rule fired: {rule.label()}")

    if kind == "channel":
        if not handle:
            return out("skip", "never-enroll: broadcast channel with no username (unaddressable)")
        if broadcast_channel_is_a_news_source(candidate, deny_rules):
            return out("enroll-both", f"broadcast channel @{handle} -> news + leads", "sources")
        return out("enroll-leads", f"broadcast channel @{handle} -> leads only (not a news source)")

    if kind in {"megagroup", "group"}:
        parent = linked_parents.get(entity_id)
        if parent is not None and broadcast_channel_is_a_news_source(parent, deny_rules):
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


# ── ledger reads (every one guarded) ──────────────────────────────


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


def chat_source_key(username: str | None, entity_id: int) -> str:
    """The key a chat_sources.txt LINE reduces to, for dedup.

    The written line is a t.me URL, but `_chat_source_keys` reduces the file to
    bare lowercase handles / ids — comparing the raw URL against that set could
    never match, so every run re-appended the same chat.
    """
    return (username or "").lower() or str(entity_id)


def load_tracked(paths: ScanPaths) -> Tracked:
    """Every read guarded: a locked, corrupt, or wrong-type input is REPORTED.

    A read failure lands in `Tracked.errors`, and run_scan then refuses to write
    at all — an unknown ledger must never be mistaken for an empty one (that
    would re-enroll everything already tracked).
    """
    tracked = Tracked()
    try:
        if paths.sources.exists():
            tracked.news_handles = {n.lower() for n in parse_sources(paths.sources.read_text(encoding="utf-8"))}
    except (OSError, UnicodeDecodeError) as exc:
        tracked.errors.append(f"news sources unreadable ({paths.sources}): {exc}")
    try:
        if paths.chat_sources.exists():
            tracked.chat_entries = _chat_source_keys(paths.chat_sources.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError) as exc:
        tracked.errors.append(f"chat_sources unreadable ({paths.chat_sources}): {exc}")
    if paths.state.exists():
        try:
            state = json.loads(paths.state.read_text(encoding="utf-8"))
            tracked.seen_ids = {int(i) for i in state.get("seen") or []}
            tracked.quarantined_ids = {int(i) for i in state.get("quarantined") or []}
        except Exception as exc:  # noqa: BLE001 — a corrupt state file must not stop the scan
            # Deliberately NOT a blocking error: "treat every dialog as new" is
            # safe here because the pipeline ledgers are the real dedup authority.
            logger.warning("dialog-scan state unreadable (%s); treating every dialog as new", exc)
    try:
        if paths.digest_db.exists():
            with sqlite3.connect(paths.digest_db, timeout=10) as con:
                con.execute("PRAGMA busy_timeout=5000")
                for peer_key, entity_id, role in con.execute(
                    "SELECT peer_key, entity_id, role FROM digest_sources"
                ).fetchall():
                    tracked.digest_peer_keys.add(str(peer_key))
                    if LEAD_SOURCE_ROLE in {r.strip() for r in str(role or "").split(",")}:
                        tracked.lead_entity_ids.add(int(entity_id))
    except (sqlite3.Error, OSError) as exc:
        tracked.errors.append(f"digest db unreadable ({paths.digest_db}): {exc}")
    try:
        if paths.join_db.exists():
            with sqlite3.connect(paths.join_db, timeout=10) as con:
                con.execute("PRAGMA busy_timeout=5000")
                tracked.join_targets = {str(r[0]) for r in con.execute("SELECT target FROM joins").fetchall()}
    except sqlite3.OperationalError as exc:
        if "no such table" not in str(exc).lower():
            tracked.errors.append(f"join db unreadable ({paths.join_db}): {exc}")
    except (sqlite3.Error, OSError) as exc:
        tracked.errors.append(f"join db unreadable ({paths.join_db}): {exc}")
    return tracked


def _already_tracked(candidate: dict[str, Any], tracked: Tracked) -> bool:
    """Note what is NOT in here: quarantined ids.

    A quarantined dialog is re-classified against the CURRENT deny rules on every
    run, which is what makes the deny file's documented behaviour ("delete a line
    to re-open that dialog") actually true.
    """
    entity_id = int(candidate.get("entity_id") or 0)
    handles = _all_handles(candidate)
    return bool(
        entity_id in tracked.seen_ids
        or entity_id in tracked.lead_entity_ids
        # ALL handles, not just the primary: a channel already in sources.txt
        # under an alias would otherwise be added again under its main handle.
        or (handles & tracked.news_handles)
        or (handles & tracked.chat_entries)
        or str(entity_id) in tracked.chat_entries
    )


# ── writes ────────────────────────────────────────────────────────


def _append_lines(path: Path, lines: Sequence[str], stamp: str) -> None:
    """ADD-ONLY append with a dated header; existing bytes are never rewritten."""
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    prefix = "" if (not existing or existing.endswith("\n")) else "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"{prefix}# added by dialog-scan {stamp}\n" + "\n".join(lines) + "\n")


_JOINS_DDL = """CREATE TABLE IF NOT EXISTS joins (
    target TEXT PRIMARY KEY, kind TEXT NOT NULL, status TEXT NOT NULL,
    entity_id INTEGER, error TEXT, created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL, joined_at TEXT,
    attempts INTEGER NOT NULL DEFAULT 0, retry_at TEXT)"""


def join_ledger_key(username: str | None, entity_id: int) -> tuple[str, str]:
    """The joins-table (kind, target) PRIMARY KEY for a dialog we are already in.

    `joins.target` is a canonical key, not a display string: `normalize_target()`
    (telegram_proxy) lowercases and strips the @/t.me wrapper and returns the
    `public`/`private` kind the proxy actually emits. For a handle-less
    discussion chat there is no public username, so we reuse the proxy's OWN
    convention for that case — target `id:<n>`, kind `linked`
    (telegram_proxy.py, the linked-chat join path). Writing `@Name` with
    kind='username' instead created a DUPLICATE row alongside the real one, left
    a genuine outstanding join request dangling, and made the dedup guard
    unfireable.
    """
    if username:
        return normalize_target(username)
    return "linked", f"id:{int(entity_id)}"


def _insert_join_row(join_db: Path, *, target: str, kind: str, entity_id: int, now: str) -> bool:
    """INSERT OR IGNORE: an EXISTING row (e.g. a real `request_sent`) is untouched."""
    join_db.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(join_db, timeout=30) as con:
        con.execute("PRAGMA busy_timeout=5000")
        con.execute(_JOINS_DDL)
        # status='joined' is the truth (we are already in these dialogs) AND
        # inert: the proxy's join loop only selects pending/floodwait rows.
        cur = con.execute(
            "INSERT OR IGNORE INTO joins(target, kind, status, entity_id, created_at, updated_at, joined_at)"
            " VALUES (?, ?, 'joined', ?, ?, ?, ?)",
            (target, kind, entity_id, now, now, now),
        )
        return cur.rowcount == 1


@dataclass
class LeadEnrolment:
    ok_ids: set[int] = field(default_factory=set)  # lead state is correct now
    added: list[int] = field(default_factory=list)  # newly written this run
    errors: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def _register_leads(paths: ScanPaths, items: Sequence[Decision], tracked: Tracked) -> LeadEnrolment:
    """The DURABLE half of an enrolment: sqlite, per item, isolated failures.

    ORDERING RATIONALE (the 2026-07-30 silent half-write): sources.txt and the
    vault mirror feed the PUBLIC digest, while these sqlite writes are the
    durable, transactional record. When the text files went first, a read-only
    digest db left a channel live in the public digest with nothing recording
    that it had never been lead-enrolled — and the state file then hid it
    forever. So the durable write happens FIRST and per item; the caller appends
    a sources.txt line only for `ok_ids`; and only fully-completed ids reach
    `seen`. Every failure names its item, so the report says exactly what was and
    was not written.

    A broadcast channel cannot go through sync_joined_sources (it hard-codes
    kind="linked_chat" — right for a joined megagroup, wrong for a channel), so
    it is inserted directly with kind="channel". Either way an EXISTING
    peer_key/target is left completely alone.
    """
    result = LeadEnrolment()
    if not items:
        return result
    now = datetime.now(timezone.utc).isoformat()
    try:
        store = TelegramDigestStore(paths.digest_db)
    except (sqlite3.Error, OSError) as exc:
        result.errors.append(
            f"lead enrolment IMPOSSIBLE (digest db {paths.digest_db} unusable: {exc}); "
            f"NOTHING was written for {[d.username or d.entity_id for d in items]}"
        )
        return result

    for item in items:
        kind = "channel" if item.kind == "channel" else "linked_chat"
        peer_key = _peer_key(kind, item.entity_id)
        who = f"@{item.username}" if item.username else (item.title or f"dialog:{item.entity_id}")
        if peer_key in tracked.digest_peer_keys:
            # Already a source row: leave it exactly as it is (a re-upsert is how
            # a news source used to be silently demoted to a lead source).
            result.ok_ids.add(item.entity_id)
            continue
        try:
            if kind == "linked_chat":
                join_kind, target = join_ledger_key(item.username, item.entity_id)
                if not _insert_join_row(
                    paths.join_db, target=target, kind=join_kind, entity_id=item.entity_id, now=now
                ):
                    result.notes.append(
                        f"joins ledger already owns target {target!r} ({who}) — its row was left untouched"
                    )
            store.add_source_role(
                peer_key=peer_key,
                entity_id=item.entity_id,
                title=item.title or f"{kind}:{item.entity_id}",
                username=item.username,
                kind=kind,
                linked_channel_key=None,
                role=LEAD_SOURCE_ROLE,
            )
        except (sqlite3.Error, OSError) as exc:
            result.errors.append(
                f"lead enrolment FAILED for {who} (id={item.entity_id}): {exc}; "
                "NOTHING was written for it (no news line, no state entry) — it retries next run"
            )
            continue
        result.ok_ids.add(item.entity_id)
        result.added.append(item.entity_id)
    return result


def _acquire_lock(path: Path) -> tuple[int | None, str | None]:
    """Whole-run mutex, same flock pattern as the aggregator's draft-runner.lock."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
    except OSError as exc:
        return None, f"cannot open the run lock {path}: {exc}"
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        return None, None  # someone else holds it — not an error
    return fd, None


def _release_lock(fd: int | None) -> None:
    if fd is None:
        return
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


# ── reporting ─────────────────────────────────────────────────────


_DECISION_ORDER = {"enroll-both": 0, "enroll-news": 1, "enroll-leads": 2, "quarantine": 3, "skip": 4}


def render_report(report: ScanReport, *, max_chars: int = MAX_REPORT_CHARS) -> str:
    """ACTIONS and ERRORS first, the table last and BOUNDED.

    The per-dialog table used to come first, so on the first run (150 DMs + one
    new channel) the 18 625-char report was cut at 4000 by notify_operator and
    the operator never saw the `sources.txt +=` line or the ERRORS block — the
    only two things they had to act on. Truncation is now explicit, and it can
    only ever eat table rows.
    """
    kinds = ", ".join(f"{k}={v}" for k, v in sorted(report.by_kind.items()))
    head = "dialog-scan" + (" (DRY RUN — nothing was written)" if report.dry_run else "")
    if report.skipped_locked:
        return f"{head}: another dialog-scan run holds the lock — this pass did nothing."
    lines = [f"{head}: {report.total_dialogs} dialogs ({kinds}); {len(report.new_dialogs)} new"]

    if report.added_news:
        lines += ["", f"news sources.txt += {report.added_news}"]
    if report.added_chat:
        lines.append(
            f"chat_sources.txt += {report.added_chat} — STAGED ONLY: nothing on this branch reads "
            "chat_sources.txt, so these take effect once the chat-collector lane is live."
        )
    if report.added_leads:
        lines.append(f"lead sources += {report.added_leads}")
    if report.mirror_pending:
        lines += ["", f"ACTION: vault mirror needs a HUMAN edit (missing/unwritable): {report.mirror_pending}"]
    if report.ledger_notes:
        lines += ["", "NOTES:", *[f"- {n}" for n in report.ledger_notes]]
    if report.requarantined:
        lines.append(
            f"({len(report.requarantined)} dialog(s) still quarantined by the deny list — unchanged; "
            "delete the rule to re-open them.)"
        )
    if report.wrote_nothing and not report.dry_run:
        lines += ["", "NOTHING WAS WRITTEN THIS RUN (see ERRORS)."]
    if report.errors:
        lines += ["", "ERRORS:", *[f"- {e}" for e in report.errors]]

    fixed = "\n".join(lines)
    if not report.decisions:
        return fixed

    rows = [
        f"{d.decision:<13} {d.kind:<10} {d.entity_id:>14}  "
        f"{('@' + d.username) if d.username else (d.title or f'dialog:{d.entity_id}')} — {d.reason}"
        for d in sorted(report.decisions, key=lambda d: (_DECISION_ORDER.get(d.decision, 9), d.kind))
    ]
    header = f"\n\n{'decision':<13} {'kind':<10} {'id':>14}  handle / title — why"
    notice_room = 120  # room for the explicit truncation notice
    budget = max_chars - len(fixed) - len(header) - notice_room
    shown: list[str] = []
    used = 0
    for row in rows:
        if used + len(row) + 1 > budget:
            break
        shown.append(row)
        used += len(row) + 1
    text = fixed + header + "\n" + "\n".join(shown)
    if len(shown) < len(rows):
        text += (
            f"\n… TABLE TRUNCATED: {len(shown)} of {len(rows)} rows shown "
            "(full table: journalctl -u telegram-dialog-scan)"
        )
    return text


# ── the run ───────────────────────────────────────────────────────


def run_scan(
    *,
    paths: ScanPaths,
    dialogs: Iterable[dict[str, Any]],
    dry_run: bool = False,
    notifier: Callable[[str], Any] | None = None,
    max_report_chars: int = MAX_REPORT_CHARS,
) -> ScanReport:
    report = ScanReport(dry_run=dry_run)
    dialogs = list(dialogs)
    report.total_dialogs = len(dialogs)
    for candidate in dialogs:
        kind = str(candidate.get("kind") or "unknown")
        report.by_kind[kind] = report.by_kind.get(kind, 0) + 1

    lock_fd: int | None = None
    if not dry_run:
        lock_fd, lock_error = _acquire_lock(paths.lock_path())
        if lock_fd is None:
            report.wrote_nothing = True
            if lock_error is None:
                report.skipped_locked = True
                report.text = render_report(report, max_chars=max_report_chars)
                return report
            report.errors.append(lock_error)
            report.text = render_report(report, max_chars=max_report_chars)
            _notify(report, notifier, max_report_chars)
            return report
    try:
        return _run_scan_locked(
            report=report,
            paths=paths,
            dialogs=dialogs,
            dry_run=dry_run,
            notifier=notifier,
            max_report_chars=max_report_chars,
        )
    finally:
        _release_lock(lock_fd)


def _notify(report: ScanReport, notifier: Callable[[str], Any] | None, max_report_chars: int) -> None:
    """Use the notifier's RETURN VALUE: `notified: true` must never be a lie.

    notify_operator() returns False when the token or the chat id is missing,
    which the caller used to ignore — a job whose ONLY output is a Telegram
    message then reported success while reaching nobody.
    """
    if notifier is None:
        return
    if bool(notifier(report.text)):
        report.notified = True
        return
    report.notify_failed = True
    report.errors.append(
        "operator notification FAILED (missing alert token / chat id, or the send errored) — "
        "this report reached NOBODY; check the journal"
    )
    report.text = render_report(report, max_chars=max_report_chars)


def _run_scan_locked(
    *,
    report: ScanReport,
    paths: ScanPaths,
    dialogs: list[dict[str, Any]],
    dry_run: bool,
    notifier: Callable[[str], Any] | None,
    max_report_chars: int,
) -> ScanReport:
    tracked = load_tracked(paths)
    report.errors.extend(tracked.errors)
    deny_rules = load_deny_rules(paths.deny)
    report.errors.extend(deny_rule_errors(deny_rules))
    # Degraded input from the proxy: a channel whose linked-chat lookup failed
    # classifies DIFFERENTLY (its discussion chat looks parentless), so that is a
    # reported error, never a silent reclassification.
    for candidate in dialogs:
        lookup = str(candidate.get("linked_chat_lookup") or "ok")
        if lookup != "ok" and str(candidate.get("kind")) == "channel":
            report.errors.append(
                f"linked-chat lookup {lookup} for "
                f"@{candidate.get('username') or candidate.get('entity_id')}"
                " — its discussion chat may be misclassified this run"
            )

    # Built from ALL dialogs, before the tracked filter: an already-tracked
    # channel is still the parent that makes its discussion chat verifiable.
    linked_parents = {
        int(d["linked_chat_id"]): d for d in dialogs if d.get("linked_chat_id") and d.get("kind") == "channel"
    }

    fresh = [d for d in dialogs if not _already_tracked(d, tracked)]
    report.new_dialogs = [int(d.get("entity_id") or 0) for d in fresh]
    decisions = [classify(d, tracked=tracked, deny_rules=deny_rules, linked_parents=linked_parents) for d in fresh]
    # A quarantine the state file already knows about is NOT news: report it the
    # first time, then only as a count, so the same deny hit never pages nightly.
    report.requarantined = [
        d.entity_id for d in decisions if d.decision == "quarantine" and d.entity_id in tracked.quarantined_ids
    ]
    known = set(report.requarantined)
    report.decisions = [d for d in decisions if d.entity_id not in known]

    stamp = datetime.now(timezone.utc).date().isoformat()
    lead_items = [d for d in decisions if d.decision in {"enroll-leads", "enroll-both"}]

    if tracked.errors:
        # An unknown ledger is NOT an empty ledger: writing now would re-enroll
        # everything that is already tracked. Refuse, and say so.
        report.wrote_nothing = True
        report.errors.append(
            "refusing to write this run: a pipeline ledger could not be read, and treating an "
            "unreadable ledger as empty would re-enroll sources that are already tracked"
        )
    elif not dry_run:
        # 1. DURABLE half first (sqlite, per item) — see _register_leads.
        enrolment = _register_leads(paths, lead_items, tracked)
        report.added_leads = enrolment.added
        report.errors.extend(enrolment.errors)
        report.ledger_notes.extend(enrolment.notes)

        # 2. VISIBLE half — only for items whose durable half completed.
        news_lines = [
            f"https://t.me/{d.username}"
            for d in decisions
            if d.news_target == "sources"
            and d.username
            and d.username.lower() not in tracked.news_handles
            and d.entity_id in enrolment.ok_ids
        ]
        chat_lines = [
            (f"https://t.me/{d.username}" if d.username else str(d.entity_id))
            for d in decisions
            if d.news_target == "chat_sources"
            and d.entity_id in enrolment.ok_ids
            and chat_source_key(d.username, d.entity_id) not in tracked.chat_entries
        ]
        news_written = False
        if news_lines:
            try:
                _append_lines(paths.sources, news_lines, stamp)
            except OSError as exc:
                report.errors.append(
                    f"news sources.txt append FAILED ({paths.sources}): {exc}; "
                    f"{news_lines} were NOT added (they ARE lead-enrolled; retried next run)"
                )
            else:
                news_written = True
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
            try:
                _append_lines(paths.chat_sources, chat_lines, stamp)
            except OSError as exc:
                report.errors.append(
                    f"chat_sources append FAILED ({paths.chat_sources}): {exc}; {chat_lines} were NOT added"
                )
            else:
                report.added_chat = chat_lines

        # 3. STATE last, and only for ids whose whole handling completed. An id
        #    that failed anywhere stays OUT of `seen`, so the next run retries it
        #    instead of hiding it forever.
        incomplete = {d.entity_id for d in lead_items if d.entity_id not in enrolment.ok_ids}
        if news_lines and not news_written:
            incomplete |= {d.entity_id for d in decisions if d.news_target == "sources"}
        quarantined = {d.entity_id for d in decisions if d.decision == "quarantine"}
        all_ids = {int(d.get("entity_id") or 0) for d in dialogs}
        seen = (tracked.seen_ids | all_ids) - incomplete - quarantined
        # Quarantined ids live in their OWN set, never in `seen`: that is what
        # lets a removed deny rule genuinely re-open a dialog. Ids missing from
        # today's dialog list keep their remembered quarantine.
        state_quarantined = quarantined | (tracked.quarantined_ids - all_ids)
        try:
            paths.state.parent.mkdir(parents=True, exist_ok=True)
            paths.state.write_text(
                json.dumps(
                    {
                        "version": _STATE_VERSION,
                        "updated": stamp,
                        "seen": sorted(seen),
                        "quarantined": sorted(state_quarantined),
                    },
                    ensure_ascii=False,
                )
            )
        except OSError as exc:
            report.errors.append(
                f"state file write FAILED ({paths.state}): {exc}; this run's enrolments ARE live "
                "but will be re-proposed next run (the pipeline ledgers still dedup them)"
            )

    report.text = render_report(report, max_chars=max_report_chars)
    # Problems-and-changes only: a run whose new dialogs are all skips (a DM, a
    # bot) stays silent. Quarantines DO report — the operator decides — but only
    # the first time each one fires.
    worth_saying = bool(report.errors) or any(d.decision != "skip" for d in report.decisions)
    if worth_saying:
        _notify(report, notifier, max_report_chars)
    return report
