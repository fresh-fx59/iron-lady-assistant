"""src/telegram_dialog_scan.py — daily NEW-dialog scanner + auto-enroller.

`/v1/channels` only ever returned broadcast peers, so a hand-joined group (or a
pending join request an admin finally approved) was invisible to both pipelines.
This walks the new `GET /v1/dialogs`, diffs against what the scanner has already
decided AND what each pipeline tracks, classifies the rest by EXPLICIT RULES (no
LLM), and enrolls the survivors.

TRACKING IS PER PIPELINE (2026-07-31, third pass — see `outstanding_surfaces`).
It used to be one global "already tracked?" flag that short-circuited on
`entity_id in tracked.lead_entity_ids`, so the moment a peer became a LEAD source
the scanner stopped considering it for anything — including the NEWS surface it
had never once been evaluated for. In production that left 102 lead-tracked
entities permanently unable to reach `sources.txt` / `chat_sources.txt`, among
them @moneyforstartup_chat (citable, own score 0.48 behind a 1.00 parent, i.e.
exactly the chat that should have been staged). The same flag ran the other way
too: a channel in `sources.txt` was "tracked", so it could never be lead-enrolled.
A candidate is now evaluated for EVERY surface it is eligible for and is not
already on, and the state file records a decision PER SURFACE.

Two disciplines keep that from turning into nightly noise, because ~100 entities
suddenly become news candidates against an 80-read budget:

  * ONLY READS THAT CAN CHANGE AN OUTCOME. A chat with no citable handle can
    never be staged, and a chat whose parent is in `sources.txt` (or already
    news-decided against) needs no parent read — both answers come from ledgers
    we already hold. After one pass the steady state costs ZERO post reads.
  * A REPEAT OUTCOME IS A COUNT, NOT A ROW — the discipline `requarantined`
    already used, now covering every peer whose news surface stays open for a
    reason the operator has already been told (`news_pending`). Nothing new ⇒ an
    empty, silent, free run.

NEWS enrolment is TOPICALLY GATED (2026-07-31): a candidate broadcast channel is
scored deterministically against an operator-editable RU/EN vocabulary over its
own recent posts, and only a passing score reaches the PUBLIC digest. Leads
enrolment is unchanged — private, breadth is the point — but reports the score.
CHAT enrolment is gated the SAME way (2026-07-31, second pass): `chat_sources.txt`
is a PUBLIC-SURFACE write — the chat lane is live in prod (1db5342), so a chat in
that file feeds the digest's draft input as soon as the file exists. A passing
parent channel is therefore not enough; the discussion chat must clear the same
0.35 line on its OWN posts, or it goes to the lead pipeline only.

Both halves of an enrolment go over the proxy, not the filesystem:
/var/lib/iron-lady is 0700 iron-lady and this runs as claude-developer. The WRITE
is POST /v1/sources/lead-enrol (see _register_leads); the corresponding READ —
what the pipelines already track — is GET /v1/sources/tracked (see load_tracked).
Reading it over the filesystem is what silently returned an EMPTY tracked set in
production and re-proposed ~47 already-enrolled entities in a single night.

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

import asyncio
import fcntl
import json
import logging
import os
import re
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

from .telegram_aggregator import parse_sources, resolve_paths
from .telegram_aggregator_gates import is_citable_link
from .telegram_aggregator_publish import OPERATOR_ALERT_CAP
from .telegram_digest import LEAD_SOURCE_ROLE, _peer_key
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
_REPO_TOPICS = Path(__file__).resolve().parent.parent / "config" / "dialog_scan_topics.txt"

# ── the topical gate ──────────────────────────────────────────────
#
# Built-in vocabulary = the FLOOR, exactly like DEFAULT_DENY_RULES_TEXT: deleting
# the operator file cannot re-open the default-allow hole, it can only lose the
# operator's ADDITIONS. Bilingual on purpose — these channels post in Russian and
# English and the digest itself is Russian. Bare lines are STEMS (see
# config/dialog_scan_topics.txt for the full contract and the calibration).
DEFAULT_TOPIC_VOCABULARY_TEXT = """
# — RU stems —
нейросет
нейронк
нейросеть
искусственн
интеллект
ии
машинн
обучени
модел
промпт
генеративн
генерац
датасет
алгоритм
разработ
программир
кодинг
бэкенд
фронтенд
девопс
облач
данны
аналитик
автоматизац
чат-бот
агент
ассистент
стартап
технолог
цифров
приложени
платформ
релиз
токен
гпт
чатгпт
дипсик
гигачат
опенсорс
вайб
подписк
интеграц
сервис
продукт
инструмент
запрос
контекст
джун
сеньор
вакансия
фичи
фича
софт
железо
видеокарт
чип
дата-центр
# — EN / latin —
ai
llm
llms
gpt
chatgpt
openai
anthropic
claude
gemini
llama
mistral
qwen
deepseek
grok
transformer
neural
embedding
rag
agent
agents
dataset
training
inference
benchmark
opensource
open-source
github
python
javascript
typescript
docker
kubernetes
api
sdk
saas
startup
chatbot
automation
cloud
framework
library
mcp
cursor
copilot
codex
devin
prompt
token
model
coding
developer
devops
backend
frontend
nvidia
huggingface
langchain
notebook
release
"""

# THE METRIC, stated once and testable (config/dialog_scan_topics.txt repeats it
# for the operator). Every constant here was CALIBRATED against the live account
# on 2026-07-31, not chosen by feel — see that file's calibration block.
TOPIC_READ_POSTS = 30          # most recent posts requested per candidate
TOPIC_MIN_POST_CHARS = 20      # shorter posts carry no lexical signal
MIN_SCOREABLE_POSTS = 8        # fewer than this ⇒ thin evidence ⇒ quarantine
TOPIC_SCORE_THRESHOLD = 0.35   # empty band on live data is (0.27, 0.36)

# Same pacing discipline as the linked-chat sweep (0.4 s / 80 lookups per run):
# this gate adds ONE proxy read per NEW candidate, and the nightly steady state
# is 0-3 of them. The bound only ever bites on a first run / a bulk join night.
TOPIC_READ_PACING_SECONDS = 0.4
TOPIC_READ_MAX = 80

# A word STARTS here: the stem "ai" must not fire inside "said"/"chain", and the
# stem "нейросет" must still cover "нейросети"/"нейросетью".
_WORD_START = r"(?<![0-9A-Za-zА-Яа-яЁё_])"

# notify_operator() hands the text to the Bot API sliced at OPERATOR_ALERT_CAP.
# The report is RENDERED to that budget instead of being cut by it, so the parts
# the operator must act on can never be the parts that fall off the end. The cap
# is IMPORTED, not re-typed: two modules agreeing on 4000 by coincidence is how
# the truncation marker itself ends up sliced off.
MAX_REPORT_CHARS = OPERATOR_ALERT_CAP

# Appended when the report is cut. render_report() guarantees the finished text
# ends with this and fits the cap, so "truncated" is never itself truncated.
TRUNCATION_MARKER = "\n… REPORT TRUNCATED (full report: journalctl -u telegram-dialog-scan)"

def citable_handle(candidate: dict[str, Any]) -> str | None:
    """The handle a PUBLISHED citation of this peer would be built from — or None.

    ONE derivation, taken from the SAME field the message link is: the proxy
    builds `link = https://t.me/<handle>/<id>` from `entity_username(entity)`,
    and /v1/dialogs reports that very value as the record's `username` (it is
    `_dialog_usernames(entity)[0]`, same legacy-then-`usernames` order). This is
    deliberately NOT `_all_handles()`, which is the wider set used for deny
    matching — the two CAN disagree, and citability must follow the link.

    The shape is then checked against the publish gate's own pattern, so a peer
    is "citable" only if the link the digest would emit is one the gate accepts.

    Why it gates enrolment (prod 2026-07-31): a handle-less peer's messages get
    `link=None`, `build_draft_input` drops link-less rows, and the gate rejects
    the `t.me/c/<id>/<msg>` form a private peer would otherwise produce. Staging
    such a peer as a digest input buys nothing and costs a read of it on every
    collect run (5 chats x 5 runs/day) plus the corpus rows.
    """
    handle = str(candidate.get("username") or "").strip().lstrip("@")
    if not handle:
        return None
    return handle if is_citable_link(f"https://t.me/{handle}/1") else None


# A username Telegram would actually accept (used to reject a malformed deny rule).
_HANDLE_RE = re.compile(r"^[a-z0-9_]{3,32}$")

# 4 makes the "done with it" memory PER-SURFACE (`decided.leads` / `decided.news`)
# and generalises `pending_citable` into `news_pending`. A v3 file migrates on
# read: its `seen` list becomes `decided.leads` and `decided.news` starts EMPTY —
# v3 `seen` was written from every dialog id, including the ones the old global
# `_already_tracked` filtered out before they were ever classified, so it cannot
# be trusted to mean "the news surface was evaluated". That costs exactly one
# re-evaluation pass (see the module docstring).
_STATE_VERSION = 4

# The two pipelines this scanner feeds. A candidate is evaluated for EVERY
# surface it is eligible for and is not already on; being tracked on one says
# nothing about the other.
SURFACE_LEADS = "leads"
SURFACE_NEWS = "news"
SURFACES = (SURFACE_LEADS, SURFACE_NEWS)


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
    topics: Path
    # OPTIONAL, and unset in production. The durable lead-enrolment ledgers live
    # in /var/lib/iron-lady (drwx------ iron-lady:iron-lady) and this scanner runs
    # as claude-developer, so it cannot open them at all — the enrolment now goes
    # through POST /v1/sources/lead-enrol on the proxy, which already runs as the
    # owning user. They stay configurable for local tests; a path that IS
    # configured but unreadable is still a blocking error (unchanged invariant).
    join_db: Path | None = None
    digest_db: Path | None = None
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
    # The topical score that GATED this decision (the candidate's own for a
    # channel; its parent channel's for a discussion chat). None only when no
    # gate applied — a DM, a bot, an own-publishing channel, a deny hit.
    topic_score: "TopicScore | None" = None
    # Can this peer ever be CITED in a published digest? Reported for every
    # enrollable peer (channel / group / megagroup), None for the kinds no
    # publication path exists for at all (a DM, a bot).
    citable: bool | None = None
    # True when citability is the ONE thing standing between this peer and the
    # public surface. A handle is a thing a peer GAINS, so these are re-checked
    # every run — for FREE, no post read: citability is read off the dialog row.
    citability_blocked: bool = False
    # Did this run actually SETTLE the news surface for this peer — i.e. decide
    # it on evidence we hold, so there is nothing left to reconsider? False for
    # every "we could not get the evidence" and every "the blocker is a thing
    # that can change for free" outcome; those stay outstanding and are reported
    # as a COUNT, never as a repeated row. Only a settled surface enters
    # `decided`, which is what makes a steady state possible at all.
    news_settled: bool = False


@dataclass
class Tracked:
    news_handles: set[str] = field(default_factory=set)
    chat_entries: set[str] = field(default_factory=set)
    digest_peer_keys: set[str] = field(default_factory=set)
    lead_entity_ids: set[int] = field(default_factory=set)
    join_targets: set[str] = field(default_factory=set)
    # PER-SURFACE "we are done with this id here". Never global: an id decided
    # for leads is still a news candidate, and vice versa.
    decided: dict[str, set[int]] = field(default_factory=lambda: {s: set() for s in SURFACES})
    quarantined_ids: set[int] = field(default_factory=set)
    # News is still OUTSTANDING for a reason we have already told the operator
    # about (no citable handle yet, parent is not a news source yet, evidence we
    # could not get). Re-evaluated every run; reported as a COUNT after the first.
    news_pending_ids: set[int] = field(default_factory=set)
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
    still_pending: list[int] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    total_dialogs: int = 0
    by_kind: dict[str, int] = field(default_factory=dict)
    dry_run: bool = False
    skipped_locked: bool = False
    # RENAMED from `wrote_nothing` (prod 2026-07-31): this flag has always meant
    # "this run REFUSED to write" (lock held, or a ledger unreadable) — the
    # operator read `wrote_nothing: false` on a --dry-run that provably wrote
    # nothing, because the run simply had not refused anything.
    refused_to_write: bool = False
    state_written: bool = False
    text: str = ""
    notified: bool = False
    notify_failed: bool = False

    @property
    def wrote_nothing(self) -> bool:
        """Literally what it says: this run left no durable trace anywhere."""
        return not (self.added_news or self.added_chat or self.added_leads or self.state_written)


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
        # Same reasoning as the deny file: the live copy is the operator-editable
        # one beside sources.txt, the in-repo file is only the seed.
        topics=Path(env("DIALOG_SCAN_TOPICS_PATH") or _REPO_TOPICS),
        # Unset by default — see ScanPaths. Only a DELIBERATE override reads them.
        join_db=Path(env("DIALOG_SCAN_JOIN_DB_PATH")) if env("DIALOG_SCAN_JOIN_DB_PATH") else None,
        digest_db=Path(env("DIALOG_SCAN_DIGEST_DB_PATH")) if env("DIALOG_SCAN_DIGEST_DB_PATH") else None,
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


# ── topical scoring (deterministic, no LLM) ───────────────────────


@dataclass(frozen=True)
class TopicRule:
    kind: str  # stem | word | re | invalid
    value: str
    error: str = ""


@dataclass(frozen=True)
class TopicScore:
    """One candidate's topical fit, and the evidence it rests on.

    `status` is the part that keeps the gate honest:
      ok         — enough readable posts to decide (>= MIN_SCOREABLE_POSTS)
      thin       — the channel exists but has too little readable text
      unreadable — the proxy read itself failed / was never attempted
    Only `ok` may ever produce an enrolment; the other two QUARANTINE.
    """

    score: float
    hits: int
    scored: int
    read: int
    status: str
    detail: str = ""
    terms: tuple[str, ...] = ()

    def passes(self) -> bool:
        return self.status == "ok" and self.score >= TOPIC_SCORE_THRESHOLD

    def label(self) -> str:
        if self.status == "unreadable":
            return f"unreadable ({self.detail})" if self.detail else "unreadable"
        return f"{self.score:.2f} ({self.hits}/{self.scored} posts)"


def parse_topic_rules(text: str) -> list[TopicRule]:
    """One term per line: a bare STEM, or the `word:` / `re:` escape hatches.

    Deliberately the same three-shape grammar as the deny file so the operator
    learns one syntax; the only difference is what a BARE line means (a handle
    there, a stem here), which config/dialog_scan_topics.txt states up front.
    """
    rules: list[TopicRule] = []
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        prefix, sep, value = line.partition(":")
        kind, value = prefix.strip().lower(), value.strip()
        if sep and kind in {"word", "re"}:
            if not value:
                rules.append(TopicRule("invalid", line, f"empty {kind}: rule"))
            elif kind == "re":
                try:
                    re.compile(value)
                except re.error as exc:
                    rules.append(TopicRule("invalid", line, f"invalid regex ({exc})"))
                else:
                    rules.append(TopicRule("re", value))
            else:
                rules.append(TopicRule("word", value.lower()))
            continue
        rules.append(TopicRule("stem", line.lower()))
    return rules


def load_topic_rules(path: Path | None) -> list[TopicRule]:
    """Built-ins are the FLOOR; the operator file only ever ADDS to them.

    A missing or unreadable vocabulary file must never widen the gate — that is
    exactly the default-allow hole this change closes — so the fallback is the
    full built-in vocabulary, not an empty one.
    """
    rules = parse_topic_rules(DEFAULT_TOPIC_VOCABULARY_TEXT)
    if path is None:
        return rules
    try:
        if path.exists():
            rules.extend(parse_topic_rules(path.read_text(encoding="utf-8")))
    except OSError as exc:
        rules.append(TopicRule("invalid", str(path), f"topic file unreadable ({exc})"))
    return rules


def topic_rule_errors(rules: Sequence[TopicRule]) -> list[str]:
    return [
        f"topic rule ignored (it scores NOTHING): {rule.value!r} — {rule.error}"
        for rule in rules
        if rule.kind == "invalid"
    ]


def _compile_topic_rules(rules: Sequence[TopicRule]) -> list[tuple[str, "re.Pattern[str]"]]:
    compiled: list[tuple[str, re.Pattern[str]]] = []
    for rule in rules:
        try:
            if rule.kind == "stem":
                compiled.append((rule.value, re.compile(_WORD_START + re.escape(rule.value), re.IGNORECASE)))
            elif rule.kind == "word":
                compiled.append((rule.value, re.compile(re.escape(rule.value), re.IGNORECASE)))
            elif rule.kind == "re":
                compiled.append((rule.value, re.compile(rule.value, re.IGNORECASE)))
        except re.error:
            continue  # already surfaced by topic_rule_errors()
    return compiled


def score_posts(texts: Sequence[str], rules: Sequence[TopicRule]) -> TopicScore:
    """THE metric. hits / scoreable posts, over the posts we could actually read.

    * scoreable post = at least TOPIC_MIN_POST_CHARS of text. A media-only or
      one-word post carries no lexical signal and must not dilute (or inflate)
      the denominator.
    * a post HITS when at least ONE vocabulary term matches. Measured against the
      live account, requiring 2+ distinct terms per post collapsed a genuine
      source (@naiznankuo) to 0.00 while barely moving the off-topic ones — so
      one term it is, and the THRESHOLD does the separating.
    * the score is a FRACTION, so a chatty 30-post chat and a terse 10-post feed
      are comparable; the post COUNT is kept separately as the evidence gate.
    """
    scoreable = [t for t in texts if len(str(t or "").strip()) >= TOPIC_MIN_POST_CHARS]
    compiled = _compile_topic_rules(rules)
    hits = 0
    seen_terms: dict[str, int] = {}
    for text in scoreable:
        matched = [label for label, pattern in compiled if pattern.search(text)]
        if matched:
            hits += 1
            for label in matched:
                seen_terms[label] = seen_terms.get(label, 0) + 1
    score = (hits / len(scoreable)) if scoreable else 0.0
    status = "ok" if len(scoreable) >= MIN_SCOREABLE_POSTS else "thin"
    terms = tuple(t for t, _ in sorted(seen_terms.items(), key=lambda kv: (-kv[1], kv[0]))[:5])
    return TopicScore(
        score=score, hits=hits, scored=len(scoreable), read=len(texts), status=status, terms=terms
    )


def unreadable_score(detail: str) -> TopicScore:
    return TopicScore(score=0.0, hits=0, scored=0, read=0, status="unreadable", detail=detail)


def collect_topic_scores(
    candidates: Sequence[tuple[str, int]],
    *,
    reader: Callable[[str, int], Sequence[str]] | None,
    rules: Sequence[TopicRule],
    sleeper: Callable[[float], Any] | None = None,
) -> tuple[dict[tuple[str, int], TopicScore], list[str]]:
    """Read each candidate's recent posts ONCE, paced and bounded.

    Same discipline the linked-chat sweep already uses (0.4 s apart, 80 per run):
    a nightly enrolment path must not turn into a rate-limit incident. Reads are
    keyed by (kind, entity_id) and cached for the run, so a channel that is also
    the parent of a new discussion chat costs one read, not two. Anything beyond
    the bound is NOT guessed at — it becomes an `unreadable` score, i.e. a
    quarantine with a stated reason, and the overflow is reported.
    """
    scores: dict[tuple[str, int], TopicScore] = {}
    errors: list[str] = []
    if reader is None:
        for key in candidates:
            scores[key] = unreadable_score("no post reader configured")
        return scores, errors
    sleep = sleeper or time.sleep
    for index, key in enumerate(dict.fromkeys(candidates)):
        if index >= TOPIC_READ_MAX:
            scores[key] = unreadable_score(f"bounded at {TOPIC_READ_MAX} post reads/run")
            continue
        if index:
            sleep(TOPIC_READ_PACING_SECONDS)
        kind, entity_id = key
        try:
            texts = list(reader(kind, entity_id))
        except Exception as exc:  # noqa: BLE001 — per-candidate isolation
            scores[key] = unreadable_score(f"{type(exc).__name__}: {exc}")
            continue
        scores[key] = score_posts(texts, rules)
    overflow = sum(1 for s in scores.values() if s.detail.startswith("bounded at"))
    if overflow:
        errors.append(
            f"topical gate bounded at {TOPIC_READ_MAX} post reads this run; {overflow} candidate(s) "
            "were left UNREAD rather than guessed at (an unread channel is quarantined, an unread "
            "discussion chat is held out of chat_sources and goes to leads only). They stay out of "
            "`seen`, so the next run reads them first as the decided ones drop off the fresh list"
        )
    return scores, errors


# ── the one classification policy decision ────────────────────────


def broadcast_channel_is_a_news_source(
    candidate: dict[str, Any],
    rules: Sequence[DenyRule],
    *,
    topic_score: TopicScore | None,
) -> bool:
    """GATED (2026-07-31): a broadcast channel is a news source only if its OWN
    RECENT POSTS score topical.

    THE one classification policy decision in this module, still isolated here.
    It used to be DEFAULT-ALLOW: any broadcast channel with an undenied handle
    became a source of the PUBLIC @ai_daily_summary digest on the next 01:47 UTC
    scan — a shop, a friend's blog, a one-off announcement feed — and the deny
    list was the entire brake. The operator's decision (2026-07-31) replaced that
    with a deterministic score over the channel's real posts; no LLM, because a
    nightly enrolment path has to be testable and auditable, and because this
    pipeline already delivers its critical decisions by code.

    SCOPE — this gate governs NEWS enrolment only, i.e. the PUBLIC surface:
      * pass  -> the channel is a news source (sources.txt + the digest);
      * fail with real evidence -> NOT a news source, but still enrolled into the
        LEAD pipeline, which is private and where breadth is the point. Its score
        is reported either way, so the operator can see what is arriving;
      * thin / unreadable evidence -> neither. `classify` quarantines it with a
        stated reason. NEVER guess on evidence we do not have: a coin-flip
        enrolment here is a public-channel problem, and the retry is free.

    `topic_score=None` is deliberately NOT a pass — that is the default-allow
    hole, and it must stay closed if a caller forgets to score.
    """
    handle = (candidate.get("username") or "").lower()
    return bool(
        candidate.get("kind") == "channel"
        and handle
        and handle not in OWN_PUBLISHING_CHANNELS
        and deny_match(rules, candidate) is None
        and topic_score is not None
        and topic_score.passes()
    )


def _parent_tracked(parent: dict[str, Any], tracked: Tracked | None) -> bool:
    """Is this parent channel something a pipeline already knows about?"""
    if tracked is None:
        return True  # no ledger to check against — presence in the dialog list is all we have
    return bool(
        (_all_handles(parent) & tracked.news_handles)
        or _peer_key("channel", int(parent.get("entity_id") or 0)) in tracked.digest_peer_keys
    )


def parent_news_status(
    parent: dict[str, Any] | None,
    tracked: Tracked | None,
    deny_rules: Sequence[DenyRule],
    *,
    topic_score: TopicScore | None,
) -> str:
    """`yes` | `no` | `unknown` — and it answers FREE whenever it can.

    A discussion chat can only reach `chat_sources.txt` behind a parent that is a
    news source, so this is the gate that decides whether reading the chat (or
    the parent) can change anything at all. Two of the three answers cost no read:

      * the parent's handle is IN sources.txt  -> `yes`, by the ledger;
      * the parent is already `decided` for news and is NOT in sources.txt
        -> `no`, because that decision was only ever recorded on real evidence.

    Only a parent we have never scored is `unknown`, and only `unknown` buys a
    read. This is what keeps the steady state at zero reads: after one pass every
    parent channel is either in sources.txt or news-decided.

    `no` is deliberately NOT recorded as a settled news verdict for the CHILD (see
    classify): the parent may be added to sources.txt tomorrow, and re-asking is
    free, so the chat stays outstanding and simply reports as a count.
    """
    if parent is None:
        return "no"
    if tracked is not None and (_all_handles(parent) & tracked.news_handles):
        return "yes"
    if broadcast_channel_is_a_news_source(parent, deny_rules, topic_score=topic_score):
        return "yes"
    if topic_score is not None and topic_score.status == "ok":
        return "no"  # real evidence, and it failed the line
    if tracked is not None and int(parent.get("entity_id") or 0) in tracked.decided.get(SURFACE_NEWS, set()):
        return "no"  # scored and rejected on an earlier run; nothing to re-read
    return "unknown"


def _parent_evidence(parent: dict[str, Any], tracked: Tracked | None, score: TopicScore | None) -> str:
    """What we can honestly say about the parent WITHOUT having read it."""
    if score is not None:
        return score.label()
    if tracked is not None and (_all_handles(parent) & tracked.news_handles):
        return "already in sources.txt"
    return "not scored"


def classify(
    candidate: dict[str, Any],
    *,
    tracked: Tracked | None,
    deny_rules: Sequence[DenyRule],
    linked_parents: dict[int, dict[str, Any]],
    topic_scores: dict[tuple[str, int], TopicScore] | None = None,
) -> Decision:
    """Explicit rules only, checked in order. When unsure: never enroll."""
    kind = str(candidate.get("kind") or "user")
    handle = (candidate.get("username") or "").strip() or None
    entity_id = int(candidate.get("entity_id") or 0)
    # `or {}` would discard an EMPTY-but-present mapping — identity, not truthiness.
    scores: dict[tuple[str, int], TopicScore] = {} if topic_scores is None else topic_scores

    # Reported for every peer a publication path could exist for, whatever the
    # decision — the operator asked to SEE citability, not just be gated by it.
    citable = citable_handle(candidate) is not None if kind in {"channel", "megagroup", "group"} else None

    def out(
        decision: str,
        reason: str,
        news_target: str | None = None,
        score: TopicScore | None = None,
        citability_blocked: bool = False,
        news_settled: bool = False,
    ) -> Decision:
        return Decision(
            entity_id, str(candidate.get("title") or ""), kind, handle, decision, reason, news_target, score,
            citable, citability_blocked, news_settled,
        )

    # A `skip` is structural and permanent — it settles every surface at once.
    if kind in {"user", "bot"}:
        return out("skip", f"never-enroll: {kind} dialog (DM / bot / Saved Messages)", news_settled=True)
    if _all_handles(candidate) & OWN_PUBLISHING_CHANNELS:
        return out("skip", f"never-enroll: operator's own publishing channel @{handle}", news_settled=True)
    rule = deny_match(deny_rules, candidate)
    if rule is not None:
        return out("quarantine", f"deny rule fired: {rule.label()}")

    if kind == "channel":
        if not handle:
            return out(
                "skip", "never-enroll: broadcast channel with no username (unaddressable)", news_settled=True
            )
        if citable is False:
            # Same class as the chat case below: sources.txt is a PUBLIC-surface
            # write and the digest cites t.me/<handle>/<id>. A handle that cannot
            # form a link the publish gate accepts can never be cited — but a
            # handle is a thing a peer GAINS, so this is re-checked (for free,
            # no post read) rather than settled.
            return out(
                "enroll-leads",
                f"broadcast channel @{handle} has no citable public handle "
                "(cannot form a t.me/<handle>/<id> link the digest gate accepts) "
                "-> leads only; re-checked every run, promoted if it gains one",
                score=scores.get(("channel", entity_id)),
                citability_blocked=True,
            )
        score = scores.get(("channel", entity_id))
        # NEVER GUESS: no evidence is not "probably fine", and it is not "probably
        # bad" either — it is a quarantine the operator sees, with the reason.
        if score is None or score.status == "unreadable":
            detail = score.detail if score is not None else "not scored"
            return out("quarantine", f"topical gate: posts unreadable ({detail}) — refusing to guess", score=score)
        if score.status == "thin":
            return out(
                "quarantine",
                f"topical gate: too few readable posts ({score.scored} scoreable of {score.read} read, "
                f"need {MIN_SCOREABLE_POSTS}) — refusing to guess",
                score=score,
            )
        if broadcast_channel_is_a_news_source(candidate, deny_rules, topic_score=score):
            return out(
                "enroll-both",
                f"broadcast channel @{handle} scores {score.label()} >= {TOPIC_SCORE_THRESHOLD:.2f} "
                f"[{', '.join(score.terms)}] -> news + leads",
                "sources",
                score,
                news_settled=True,
            )
        return out(
            "enroll-leads",
            f"broadcast channel @{handle} scores {score.label()} < {TOPIC_SCORE_THRESHOLD:.2f} "
            "-> NOT a news source; leads only (private pipeline, breadth is the point)",
            score=score,
            news_settled=True,
        )

    if kind in {"megagroup", "group"}:
        parent = linked_parents.get(entity_id)
        # The chat's own score is reported as evidence but only gates the PUBLIC
        # surface: there is no labelled set of discussion chats to calibrate a
        # chat threshold against, and inventing one would be picking a number by
        # feel. This is what makes the @Music_Producers_Chat case (parent
        # @erdman_music scores 0.27) a RULE rather than a hand-added deny line.
        own_score = scores.get(("linked_chat", entity_id))
        parent_score = (
            scores.get(("channel", int(parent.get("entity_id") or 0))) if parent is not None else None
        )
        status = parent_news_status(parent, tracked, deny_rules, topic_score=parent_score)
        # CITABILITY FIRST — it is read off the dialog row, needs no post read at
        # all, and it is decisive: all five chats the live 2026-07-31 run staged
        # were handle-less, so every one of them was inert in the digest while
        # still costing a read per collect run. Leads still take it: that pipeline
        # keys on entity_id and never publishes a link. Checking it before the
        # own-post score is what makes a non-citable chat cost nothing, nightly.
        if citable_handle(candidate) is None:
            verifiable = parent is not None and (status == "yes" or _parent_tracked(parent, tracked))
            if not handle and not verifiable:
                # Unaddressable AND unverifiable: enrollable into nothing, ever.
                return out(
                    "skip",
                    "never-enroll: no username AND no linked parent (unaddressable, unverifiable)",
                    news_settled=True,
                )
            who = f"@{parent.get('username')}" if parent is not None else "no linked channel"
            return out(
                "enroll-leads",
                f"discussion chat ({who}) has no citable public handle ⇒ cannot be cited in a published "
                "digest (its messages carry link=None and are dropped before the draft) -> leads only; "
                "re-checked every run at no read cost, promoted if it ever gains one",
                score=own_score,
                citability_blocked=True,
            )
        if status == "yes":
            shown = _parent_evidence(parent, tracked, parent_score) if parent is not None else "n/a"
            # chat_sources.txt is a PUBLIC-SURFACE write (the chat lane merged as
            # 1db5342 and is deployed: the digest reads that file the moment it
            # exists), so a passing PARENT is not enough — the live dry-run found
            # parent-gated chats scoring 0.08. The chat must clear the SAME
            # calibrated 0.35 line on its OWN posts. Everything short of a pass
            # goes to leads only, with the reason reported; but only a REAL score
            # settles the news surface — unreadable or thin evidence is retried.
            if own_score is None or own_score.status == "unreadable":
                detail = own_score.detail if own_score is not None else "not scored"
                return out(
                    "enroll-leads",
                    f"discussion chat of news channel @{parent.get('username')} (parent {shown}), but its "
                    f"OWN posts are unreadable ({detail}) -> leads only; chat_sources needs evidence we "
                    "could not get, so it is retried next run",
                    score=own_score,
                )
            if own_score.status == "thin":
                return out(
                    "enroll-leads",
                    f"discussion chat of news channel @{parent.get('username')} (parent {shown}), but its "
                    f"OWN evidence is thin ({own_score.scored} scoreable of {own_score.read} read, need "
                    f"{MIN_SCOREABLE_POSTS}) -> leads only; retried next run",
                    score=own_score,
                )
            if not own_score.passes():
                return out(
                    "enroll-leads",
                    f"discussion chat of news channel @{parent.get('username')} (parent {shown}), but the "
                    f"chat itself scores {own_score.label()} < {TOPIC_SCORE_THRESHOLD:.2f} on its own posts "
                    "-> leads only; chat_sources is a PUBLIC digest input",
                    score=own_score,
                    news_settled=True,
                )
            return out(
                "enroll-both",
                f"discussion chat of news channel @{parent.get('username')} (parent {shown}, chat itself "
                f"{own_score.label()} >= {TOPIC_SCORE_THRESHOLD:.2f}) -> leads + chat_sources",
                "chat_sources",
                own_score,
                news_settled=True,
            )
        if status == "unknown":
            detail = parent_score.detail if parent_score is not None else "not scored"
            return out(
                "enroll-leads",
                f"group @{handle} -> leads; chat_sources undecided because its parent "
                f"@{parent.get('username')} could not be scored ({detail}) — retried next run",
                score=own_score,
            )
        # status == "no". NOT settled: the parent may be added to sources.txt
        # tomorrow, and re-asking costs nothing (the answer comes from the ledger
        # and the state file, never from a read), so this chat stays a candidate
        # and simply reports as a count instead of a nightly row.
        if parent is not None:
            shown = _parent_evidence(parent, tracked, parent_score)
            return out(
                "enroll-leads",
                f"group @{handle} -> leads (parent @{parent.get('username')} scores {shown} — not a news "
                "source, so chat_sources stays closed; re-checked free every run)",
                score=own_score,
            )
        return out(
            "enroll-leads",
            f"group @{handle} -> leads (no linked news channel; chat_sources needs an operator add)",
            score=own_score,
        )

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


def _read_tracked_over_proxy(
    tracked: Tracked, tracked_reader: Callable[[], dict[str, Any]] | None
) -> None:
    """Fill the two pipeline-ledger views from GET /v1/sources/tracked.

    THREE outcomes, and the code must keep them apart:
      * reader missing        -> "I have no way to know" == FAILURE. Not silence.
      * reader raises / junk  -> FAILURE, named, with the exception text.
      * reader returns []     -> a SUCCESSFUL read of an empty pipeline; writes
                                 proceed. A pipeline with nothing enrolled yet
                                 must still be able to enroll its first source.
    """
    if tracked_reader is None:
        tracked.errors.append(
            "tracked-state read UNAVAILABLE: no digest/join db path is configured and no proxy "
            "reader was supplied, so 'is this already enrolled?' cannot be answered — refusing to "
            "treat that as 'nothing is tracked' (that re-enrolls every existing source)"
        )
        return
    try:
        payload = tracked_reader()
        if not isinstance(payload, dict):
            raise TypeError(f"expected a JSON object, got {type(payload).__name__}")
        sources = payload.get("digest_sources") or []
        joins = payload.get("joins") or []
        if not isinstance(sources, list) or not isinstance(joins, list):
            raise TypeError("`digest_sources` and `joins` must both be lists")
        peer_keys: set[str] = set()
        lead_ids: set[int] = set()
        for item in sources:
            peer_key = str(item["peer_key"])
            if peer_key:
                peer_keys.add(peer_key)
            if LEAD_SOURCE_ROLE in {r.strip() for r in str(item.get("role") or "").split(",")}:
                lead_ids.add(int(item["entity_id"]))
        targets = {str(t) for t in joins}
    except Exception as exc:  # noqa: BLE001 — HTTP, JSON and shape failures alike
        tracked.errors.append(
            f"tracked-state read FAILED over the proxy (GET /v1/sources/tracked): "
            f"{type(exc).__name__}: {exc}"
        )
        return
    tracked.digest_peer_keys = peer_keys
    tracked.lead_entity_ids = lead_ids
    tracked.join_targets = targets


def load_tracked(
    paths: ScanPaths, tracked_reader: Callable[[], dict[str, Any]] | None = None
) -> Tracked:
    """Every read guarded: a locked, corrupt, or wrong-type input is REPORTED.

    A read failure lands in `Tracked.errors`, and run_scan then refuses to write
    at all — an unknown ledger must never be mistaken for an empty one (that
    would re-enroll everything already tracked).

    The two PIPELINE ledgers (digest_sources, joins) are read through
    `tracked_reader` — in production `GET /v1/sources/tracked` on the proxy,
    which owns 0700 /var/lib/iron-lady. Direct db paths stay as the test/override
    path and WIN when configured. Having NEITHER is a failure, not an empty
    ledger: that combination is exactly what shipped on 2026-07-31 and made the
    scanner re-propose ~47 already-enrolled entities in one night.
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
            decided = state.get("decided")
            if isinstance(decided, dict):
                tracked.decided = {s: {int(i) for i in decided.get(s) or []} for s in SURFACES}
            else:
                # v<=3 MIGRATION. `seen` meant "handled", but it was written from
                # EVERY dialog id — including the ones the old global filter threw
                # away before classification — so it is only trustworthy as "the
                # lead half is done". The news half starts empty on purpose: one
                # re-evaluation pass is the price of the repair, and it converges.
                tracked.decided = {
                    SURFACE_LEADS: {int(i) for i in state.get("seen") or []},
                    SURFACE_NEWS: set(),
                }
            tracked.quarantined_ids = {int(i) for i in state.get("quarantined") or []}
            tracked.news_pending_ids = {
                int(i) for i in (state.get("news_pending") or state.get("pending_citable") or [])
            }
        except Exception as exc:  # noqa: BLE001 — a corrupt state file must not stop the scan
            # Deliberately NOT a blocking error: "treat every dialog as new" is
            # safe here because the pipeline ledgers are the real dedup authority.
            logger.warning("dialog-scan state unreadable (%s); treating every dialog as new", exc)
    if paths.digest_db is None and paths.join_db is None:
        _read_tracked_over_proxy(tracked, tracked_reader)
        return tracked
    try:
        if paths.digest_db is not None and paths.digest_db.exists():
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
        if paths.join_db is not None and paths.join_db.exists():
            with sqlite3.connect(paths.join_db, timeout=10) as con:
                con.execute("PRAGMA busy_timeout=5000")
                tracked.join_targets = {str(r[0]) for r in con.execute("SELECT target FROM joins").fetchall()}
    except sqlite3.OperationalError as exc:
        if "no such table" not in str(exc).lower():
            tracked.errors.append(f"join db unreadable ({paths.join_db}): {exc}")
    except (sqlite3.Error, OSError) as exc:
        tracked.errors.append(f"join db unreadable ({paths.join_db}): {exc}")
    return tracked


def eligible_surfaces(candidate: dict[str, Any]) -> set[str]:
    """Which pipelines could this peer EVER belong to? Kind alone answers it."""
    if str(candidate.get("kind") or "user") in {"user", "bot"}:
        return set()  # a DM / bot dialog is enrollable nowhere, so never a candidate
    return set(SURFACES)


def on_surface(candidate: dict[str, Any], tracked: Tracked, surface: str) -> bool:
    """Is this peer ALREADY on that surface, per the surface's own ledger?

    The ledgers — not the state file — are the authority on membership, so an
    operator's hand-added line counts exactly like one of ours.
    """
    entity_id = int(candidate.get("entity_id") or 0)
    if surface == SURFACE_LEADS:
        kind = "channel" if str(candidate.get("kind")) == "channel" else "linked_chat"
        # A digest_sources row of ANY role counts: _register_leads deliberately
        # never rewrites an existing row (that upsert once demoted a news source
        # to a lead source), so there is nothing left for us to do about it.
        return entity_id in tracked.lead_entity_ids or _peer_key(kind, entity_id) in tracked.digest_peer_keys
    handles = _all_handles(candidate)
    # ALL handles, not just the primary: a channel already in sources.txt under
    # an alias would otherwise be added again under its main handle.
    return bool(
        (handles & tracked.news_handles)
        or (handles & tracked.chat_entries)
        or str(entity_id) in tracked.chat_entries
    )


def outstanding_surfaces(candidate: dict[str, Any], tracked: Tracked) -> set[str]:
    """THE freshness rule, and the whole point of the 2026-07-31 repair.

    This used to be one global `_already_tracked()` that short-circuited on
    `entity_id in tracked.lead_entity_ids`. So the instant a peer became a LEAD
    source it stopped being considered for anything — including the NEWS surface
    it had never been evaluated for. Live, that left 102 lead-tracked entities
    permanently unable to reach `sources.txt` / `chat_sources.txt`, including
    @moneyforstartup_chat (citable, own score 0.48 behind a 1.00 parent). The
    same bug ran the other way too: a channel in sources.txt was "tracked", so it
    could never be lead-enrolled.

    Tracking is now PER PIPELINE. A surface is outstanding when the peer is
    eligible for it, is not on it, and we have not already decided it there.

    Note what is NOT consulted here: the quarantine bucket. A quarantined dialog
    settles no surface, so it comes back every run and is re-classified against
    the CURRENT deny rules — which is what makes the deny file's documented
    behaviour ("delete a line to re-open that dialog") actually true. The
    citability bucket needs no special case any more either: a peer blocked only
    on citability simply never settles its news surface.
    """
    entity_id = int(candidate.get("entity_id") or 0)
    return {
        surface
        for surface in eligible_surfaces(candidate)
        if entity_id not in tracked.decided.get(surface, set())
        and not on_surface(candidate, tracked, surface)
    }


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


def _register_leads(
    paths: ScanPaths,
    items: Sequence[Decision],
    tracked: Tracked,
    enroller: Callable[..., Any] | None,
) -> LeadEnrolment:
    """The DURABLE half of an enrolment — now ONE authenticated proxy call per item.

    WHY IT MOVED OFF THE FILESYSTEM (2026-07-31). This used to open
    /var/lib/iron-lady/memory/{telegram_join,telegram_digest}.db directly. That
    directory is `drwx------ iron-lady:iron-lady` and the scanner unit runs as
    `claude-developer`, which cannot even traverse it — so the lead half could
    never work as written. Of the two ways out, the operator took the endpoint:
    `telegram-proxy-giedi.service` ALREADY runs as the user that owns those dbs,
    already has Bearer auth, and every other claude-developer-side job in this
    fleet reaches iron-lady state exactly this way (telegram-lead-scorer.nix
    LoadCredentials the same key and reads /v1/lead-candidates). Relaxing a 0700
    directory holding session-adjacent state is a far wider blast radius than one
    authenticated, narrowly-scoped route that can only ever add a lead role.

    ORDERING RATIONALE, unchanged and now stretched across HTTP (the 2026-07-30
    silent half-write): sources.txt and the vault mirror feed the PUBLIC digest,
    the proxy call is the durable record. The durable write happens FIRST and per
    item; the caller appends a sources.txt line only for `ok_ids`; and only
    fully-completed ids reach `seen`. Every failure names its item.

    THE ONE NEW FAILURE MODE the HTTP boundary adds is AMBIGUITY: a timeout means
    the write may or may not have landed. That id is treated exactly like a
    failure (no news line, no `seen` entry) and is NOT retried inside this run —
    a blind retry is how you double-write. It is safe to re-attempt NEXT run
    because the endpoint is idempotent by construction (INSERT OR IGNORE on the
    joins row, `add_source_role` never rewriting an existing digest row).
    """
    result = LeadEnrolment()
    if not items:
        return result
    if enroller is None:
        result.errors.append(
            "lead enrolment IMPOSSIBLE (no proxy enroller configured); "
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
            response = enroller(
                entity_id=item.entity_id,
                kind=kind,
                title=item.title or f"{kind}:{item.entity_id}",
                username=item.username,
            )
        except (TimeoutError, asyncio.TimeoutError) as exc:
            result.errors.append(
                f"lead enrolment AMBIGUOUS for {who} (id={item.entity_id}): the proxy call timed out "
                f"({type(exc).__name__}: {exc}) — the write MAY have landed. Nothing else was written "
                "for it (no news line, no state entry) and it was NOT retried this run; the endpoint is "
                "idempotent, so the next run re-attempts it safely"
            )
            continue
        except Exception as exc:  # noqa: BLE001 — per-item isolation, every one reported
            result.errors.append(
                f"lead enrolment FAILED for {who} (id={item.entity_id}): {type(exc).__name__}: {exc}; "
                "NOTHING was written for it (no news line, no state entry) — it retries next run"
            )
            continue
        payload = response if isinstance(response, dict) else {}
        if payload.get("join_note"):
            result.notes.append(f"{payload['join_note']} ({who})")
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


def _clamp(text: str, max_chars: int) -> str:
    """LAST word on length. The row loop budgets, but the FIXED part (errors,
    action lines) has no budget at all — a run with 40 unreadable-ledger errors
    renders past the cap, notify_operator then slices at exactly the cap, and the
    operator receives a report that looks complete and is not."""
    if len(text) <= max_chars:
        return text
    return text[: max_chars - len(TRUNCATION_MARKER)].rstrip() + TRUNCATION_MARKER


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
        return _clamp(f"{head}: another dialog-scan run holds the lock — this pass did nothing.", max_chars)
    lines = [f"{head}: {report.total_dialogs} dialogs ({kinds}); {len(report.new_dialogs)} new"]

    if report.added_news:
        lines += ["", f"news sources.txt += {report.added_news}"]
    if report.added_chat:
        lines.append(
            f"chat_sources.txt += {report.added_chat} — PUBLIC surface: the chat lane is LIVE in "
            "prod (1db5342), so these chats feed the digest's draft input from the next collect on."
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
    if report.still_pending:
        lines.append(
            f"({len(report.still_pending)} peer(s) still lead-only for the same reason as last run — no "
            "citable handle, or no parent that is a news source. Re-checked every run at no read cost, "
            "promoted automatically the moment that changes.)"
        )
    if report.refused_to_write and not report.dry_run:
        lines += ["", "NOTHING WAS WRITTEN THIS RUN (see ERRORS)."]
    if report.errors:
        lines += ["", "ERRORS:", *[f"- {e}" for e in report.errors]]

    fixed = "\n".join(lines)
    if not report.decisions:
        return _clamp(fixed, max_chars)

    def _cite_cell(d: Decision) -> str:
        return f"{('-' if d.citable is None else ('yes' if d.citable else 'no')):^5}"

    def _score_cell(d: Decision) -> str:
        if d.topic_score is None:
            return "     -"
        if d.topic_score.status == "unreadable":
            return "  n/a "
        return f"{d.topic_score.score:>6.2f}"

    rows = [
        f"{d.decision:<13} {d.kind:<10} {_score_cell(d)} {_cite_cell(d)}  {d.entity_id:>14}  "
        f"{('@' + d.username) if d.username else (d.title or f'dialog:{d.entity_id}')} — {d.reason}"
        for d in sorted(report.decisions, key=lambda d: (_DECISION_ORDER.get(d.decision, 9), d.kind))
    ]
    header = (
        f"\n\n{'decision':<13} {'kind':<10} {'score':>6} {'cite':^5}  {'id':>14}  handle / title — why"
        f"\n(topical score = share of the last {TOPIC_READ_POSTS} readable posts matching the topic "
        f"vocabulary; pass >= {TOPIC_SCORE_THRESHOLD:.2f} with >= {MIN_SCOREABLE_POSTS} posts. "
        f"cite = the peer has a public handle, i.e. its messages can carry a t.me/<handle>/<id> "
        f"link a published digest may cite; a non-citable peer is lead-only.)"
    )
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
    return _clamp(text, max_chars)


# ── the run ───────────────────────────────────────────────────────


def run_scan(
    *,
    paths: ScanPaths,
    dialogs: Iterable[dict[str, Any]],
    dry_run: bool = False,
    notifier: Callable[[str], Any] | None = None,
    max_report_chars: int = MAX_REPORT_CHARS,
    post_reader: Callable[[str, int], Sequence[str]] | None = None,
    lead_enroller: Callable[..., Any] | None = None,
    tracked_reader: Callable[[], dict[str, Any]] | None = None,
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
            report.refused_to_write = True
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
            post_reader=post_reader,
            lead_enroller=lead_enroller,
            tracked_reader=tracked_reader,
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
    post_reader: Callable[[str, int], Sequence[str]] | None = None,
    lead_enroller: Callable[..., Any] | None = None,
    tracked_reader: Callable[[], dict[str, Any]] | None = None,
) -> ScanReport:
    tracked = load_tracked(paths, tracked_reader)
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

    outstanding = {int(d.get("entity_id") or 0): outstanding_surfaces(d, tracked) for d in dialogs}
    fresh = [d for d in dialogs if outstanding[int(d.get("entity_id") or 0)]]
    report.new_dialogs = [int(d.get("entity_id") or 0) for d in fresh]

    # Score ONLY what the gate actually needs, in the order the report shows it:
    # every fresh broadcast channel, every fresh discussion chat (evidence for the
    # operator), and the PARENT of a fresh discussion chat even when that parent is
    # already tracked — a chat cannot reach the public digest without its parent
    # passing. Reads are deduped by key, paced, and bounded; see collect_topic_scores.
    topic_rules = load_topic_rules(paths.topics)
    report.errors.extend(topic_rule_errors(topic_rules))
    wanted: list[tuple[str, int]] = []
    for candidate in fresh:
        entity_id = int(candidate.get("entity_id") or 0)
        kind = str(candidate.get("kind") or "")
        if deny_match(deny_rules, candidate) is not None:
            continue
        if kind == "channel" and (candidate.get("username") or ""):
            if not (_all_handles(candidate) & OWN_PUBLISHING_CHANNELS):
                wanted.append(("channel", entity_id))
        elif kind in {"megagroup", "group"}:
            # ONLY reads that can change an outcome. This is the whole answer to
            # "97 lead-tracked entities against an 80-read budget": most of them
            # are decided by facts we already hold.
            parent = linked_parents.get(entity_id)
            if parent is None or deny_match(deny_rules, parent) is not None:
                continue
            status = parent_news_status(parent, tracked, deny_rules, topic_score=None)
            if citable_handle(candidate) is None:
                # Never a chat_sources candidate, so its own posts decide nothing.
                # The parent is still worth one read in exactly one case: a
                # handle-less chat is only enrollable at all if its parent is
                # verifiable, and an unscored, untracked parent is the one thing
                # we cannot answer for free.
                if candidate.get("username") or _parent_tracked(parent, tracked) or status != "unknown":
                    continue
                wanted.append(("channel", int(parent.get("entity_id") or 0)))
                continue
            if status == "no":
                continue  # parent is not a news source and we know it for free
            if status == "unknown":
                wanted.append(("channel", int(parent.get("entity_id") or 0)))
            wanted.append(("linked_chat", entity_id))
    topic_scores, topic_errors = collect_topic_scores(wanted, reader=post_reader, rules=topic_rules)
    report.errors.extend(topic_errors)

    decisions = [
        classify(
            d,
            tracked=tracked,
            deny_rules=deny_rules,
            linked_parents=linked_parents,
            topic_scores=topic_scores,
        )
        for d in fresh
    ]
    # A quarantine the state file already knows about is NOT news: report it the
    # first time, then only as a count, so the same deny hit never pages nightly.
    report.requarantined = [
        d.entity_id for d in decisions if d.decision == "quarantine" and d.entity_id in tracked.quarantined_ids
    ]
    # Same discipline, now for EVERY peer whose news surface is still open for a
    # reason we already told the operator about — no citable handle, a parent
    # that is not a news source, evidence we could not get. Say it once, then
    # only as a count, or 30+ lead-tracked chats page the operator every night.
    news_unsettled = {d.entity_id for d in decisions if d.decision != "quarantine" and not d.news_settled}
    report.still_pending = [
        d.entity_id for d in decisions if d.entity_id in news_unsettled and d.entity_id in tracked.news_pending_ids
    ]
    known = set(report.requarantined) | set(report.still_pending)
    report.decisions = [d for d in decisions if d.entity_id not in known]
    # "new" must mean NEW. A peer whose surface stays open for a reason already
    # reported (no citable handle, no news-source parent) is a candidate every
    # run by design — counting it as new would put a permanent non-zero "N new"
    # in the header of a run that found nothing.
    report.new_dialogs = [i for i in report.new_dialogs if i not in known]

    stamp = datetime.now(timezone.utc).date().isoformat()
    lead_items = [d for d in decisions if d.decision in {"enroll-leads", "enroll-both"}]

    if tracked.errors:
        # An unknown ledger is NOT an empty ledger: writing now would re-enroll
        # everything that is already tracked. Refuse, and say so.
        report.refused_to_write = True
        report.errors.append(
            "refusing to write this run: a pipeline ledger could not be read, and treating an "
            "unreadable ledger as empty would re-enroll sources that are already tracked"
        )
    elif not dry_run:
        # 1. DURABLE half first (sqlite, per item) — see _register_leads.
        enrolment = _register_leads(paths, lead_items, tracked, lead_enroller)
        report.added_leads = enrolment.added
        report.errors.extend(enrolment.errors)
        report.ledger_notes.extend(enrolment.notes)

        # 2. VISIBLE half — only for items whose durable half completed.
        # The dedup is over ALL of the peer's handles, not just the primary one:
        # a channel listed in sources.txt under an alias is now a legitimate
        # LEADS candidate (per-surface tracking), so it reaches this list, and
        # matching on the primary handle alone would append it a second time.
        handles_of = {int(d.get("entity_id") or 0): _all_handles(d) for d in dialogs}
        news_lines = [
            f"https://t.me/{d.username}"
            for d in decisions
            if d.news_target == "sources"
            and d.username
            and not (handles_of.get(d.entity_id, set()) & tracked.news_handles)
            and d.entity_id in enrolment.ok_ids
        ]
        # No bare-id branch any more: `news_target == "chat_sources"` now IMPLIES a
        # citable handle (classify refuses otherwise), so every line we write is a
        # link the digest can actually cite.
        chat_lines = [
            f"https://t.me/{d.username}"
            for d in decisions
            if d.news_target == "chat_sources"
            and d.entity_id in enrolment.ok_ids
            and chat_source_key(d.username, d.entity_id) not in tracked.chat_entries
            and not (handles_of.get(d.entity_id, set()) & tracked.chat_entries)
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

        # 3. STATE last, PER SURFACE, and only for the surfaces whose handling
        #    actually completed. This is the half that used to be global: `seen`
        #    was filled from EVERY dialog id, so an entity the old filter dropped
        #    before classification was marked done for a pipeline that had never
        #    looked at it. Now a surface is recorded only when this run settled
        #    it; anything else comes back tomorrow.
        quarantined = {d.entity_id for d in decisions if d.decision == "quarantine"}
        all_ids = {int(d.get("entity_id") or 0) for d in dialogs}
        # LEADS settles when the durable half landed (or there was nothing to do).
        leads_settled = {d.entity_id for d in decisions if d.decision == "skip"} | enrolment.ok_ids
        # NEWS settles only on evidence — and only if the visible write that the
        # verdict implied actually happened.
        news_settled = {d.entity_id for d in decisions if d.news_settled}
        if news_lines and not news_written:
            news_settled -= {d.entity_id for d in decisions if d.news_target == "sources"}
        if chat_lines and not report.added_chat:
            news_settled -= {d.entity_id for d in decisions if d.news_target == "chat_sources"}
        # A durable-half failure must not settle the news line we never wrote.
        news_settled -= {d.entity_id for d in lead_items if d.entity_id not in enrolment.ok_ids and d.news_target}
        # Quarantined ids settle NOTHING: that is what lets a removed deny rule
        # genuinely re-open a dialog. Ids missing from today's dialog list keep
        # their remembered quarantine.
        decided = {
            SURFACE_LEADS: (tracked.decided[SURFACE_LEADS] | leads_settled) - quarantined,
            SURFACE_NEWS: (tracked.decided[SURFACE_NEWS] | news_settled) - quarantined,
        }
        state_quarantined = quarantined | (tracked.quarantined_ids - all_ids)
        # Everything classified whose news surface is STILL open: reported once,
        # then as a count. A chat that gained a handle settles and drops out.
        state_pending = {
            d.entity_id
            for d in decisions
            if d.decision != "quarantine" and d.entity_id not in decided[SURFACE_NEWS]
        } | (tracked.news_pending_ids - all_ids)
        try:
            paths.state.parent.mkdir(parents=True, exist_ok=True)
            paths.state.write_text(
                json.dumps(
                    {
                        "version": _STATE_VERSION,
                        "updated": stamp,
                        "decided": {s: sorted(decided[s]) for s in SURFACES},
                        "quarantined": sorted(state_quarantined),
                        "news_pending": sorted(state_pending),
                    },
                    ensure_ascii=False,
                )
            )
        except OSError as exc:
            report.errors.append(
                f"state file write FAILED ({paths.state}): {exc}; this run's enrolments ARE live "
                "but will be re-proposed next run (the pipeline ledgers still dedup them)"
            )
        else:
            report.state_written = True

    report.text = render_report(report, max_chars=max_report_chars)
    # Problems-and-changes only: a run whose new dialogs are all skips (a DM, a
    # bot) stays silent. Quarantines DO report — the operator decides — but only
    # the first time each one fires.
    worth_saying = bool(report.errors) or any(d.decision != "skip" for d in report.decisions)
    if worth_saying:
        _notify(report, notifier, max_report_chars)
    return report
