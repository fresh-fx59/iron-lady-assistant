"""src/telegram_aggregator.py — public daily digest pipeline (aggregator).

Standalone consumer of the @giedi_0 read proxy. Reuses TelegramDigestStore on its
OWN db file (never the lead/digest db — upsert_source overwrites role, and the
lead pipeline owns that file). State lives under AGGREGATOR_STATE_DIR because the
pipeline runs as claude-developer (the draft stage needs the Max OAuth session),
not as the iron-lady service user.
"""
from __future__ import annotations

import logging
import os
import re
import sqlite3
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, MutableMapping

from .telegram_digest import TelegramDigestStore

logger = logging.getLogger(__name__)

AGG_ROLE = "aggregator"

_FILE_ENV_KEYS = (
    "TELEGRAM_PROXY_API_KEY",
    "TELEGRAM_AGGREGATOR_BOT_TOKEN",
    "AGGREGATOR_ALERT_BOT_TOKEN",
)

_USERNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{3,31}$")

# ── discussion-chat lane (change 1) ───────────────────────────────
# A joined megagroup is a Channel with broadcast=False, which the proxy reads under
# kind "linked_chat" — the same peer-kind the lead pipeline already uses.
CHAT_KIND = "linked_chat"

# Admission floor, in characters. The channel lane's draft-input floor is 80; chat
# traffic is overwhelmingly one-liners, so the ingest floor is set 2.5x higher: a
# message must be at least a paragraph (~30 Russian words) for the drafter to have
# anything to summarise and for the no-verbatim gate to have real source text.
# Measured on the live corpus (2026-07-30, newest ~190 messages per chat):
# data_secrets_chat 22/190 and llm_driven_products 48/196 clear 200 chars.
CHAT_MIN_CHARS = 200

# HARD per-chat bound per collect run — the load-bearing part of this design.
# Measured rates (live, 2026-07-30): the news corpus takes ~70 messages/day across
# 53 channels (978/14d), the median channel ~1.3/day; the loudest allowlist
# candidates run 205/day (data_secrets_chat), ~28/day (hyper_llm,
# llm_driven_products) and the busiest LEAD chats up to 536/day. After the quality
# gate the same samples admit ~2% (data_secrets_chat, llm_driven_products) but 74%
# for hyper_llm — a curated feed-mirror chat that forwards ~25 posts/day, most of
# them from channels we do NOT track. So the gate alone does NOT bound a chat: one
# feed-mirror chat would add ~16 messages/day, ~23% of the whole daily corpus.
# collect runs 5x/day (timers 02:17/08:17/14:17/20:17 + the 06:17 draft run), so
# cap=2 puts a ceiling of ~10 messages/day on ANY single chat: ~14% of the channel
# baseline, ~7x a median channel, and a 95% cut of the loudest chat's raw rate. The
# cap, not the chat's volume, decides the contribution.
CHAT_MAX_PER_RUN = 2

# Messages read per chat per run. 205/day = ~51 per 6h interval, so 150 gives ~3x
# headroom; a burst beyond it is deliberately skipped (the cursor jumps forward —
# see collect_chats) because the digest only ever looks at the last 24h.
CHAT_READ_LIMIT = 150

# Lookback for the cross-lane dedup key set: the draft window is 24h, so 72h covers
# a story a chat echoes a day or two after the channel posted it.
CHAT_DEDUP_WINDOW_HOURS = 72

_URL_RE = re.compile(r"https?://\S", re.IGNORECASE)
_TME_ID_RE = re.compile(r"^(?:https?://)?t\.me/c/(\d+)(?:/\d+)?$", re.IGNORECASE)
# A public post link as it appears quoted inside a chat message.
_TME_POST_RE = re.compile(
    r"(?:https?://)?t\.me/(?!c/|\+)[A-Za-z][A-Za-z0-9_]{3,31}/\d+", re.IGNORECASE
)


@dataclass(frozen=True)
class ChatSource:
    """One allowlist entry: either a public handle or a bare entity id.

    ``form`` is ``"username"`` (resolvable only through a channel's
    ``linked_chat_username``) or ``"id"`` (an internal, positive Telethon entity
    id — the only way to name a discussion group with no public handle).
    """

    form: str
    value: str


@dataclass(frozen=True)
class ResolvedChat:
    entity_id: int
    title: str
    username: str | None
    parent_channel_id: int | None
    parent_channel_key: str | None

    @property
    def peer_key(self) -> str:
        return f"{CHAT_KIND}:{self.entity_id}"

    @property
    def citable(self) -> bool:
        """Whether a message here can carry a public ``t.me/<handle>/<id>`` link.

        A handle-less group yields ``link=None`` from the proxy; ``build_draft_input``
        drops link-less rows and the draft gates only accept
        ``https://t.me/<username>/<id>``, so such a chat is corpus-only today — it is
        ingested and visible to later analysis, but it cannot be cited in the digest.
        """
        return bool(self.username)


@dataclass(frozen=True)
class AggregatorPaths:
    state_dir: Path
    db_path: Path
    sources_path: Path
    chat_sources_path: Path
    drafts_dir: Path


def resolve_paths() -> AggregatorPaths:
    state_dir = Path(
        os.getenv("AGGREGATOR_STATE_DIR", "/home/claude-developer/telegram-aggregator")
    )
    sources_raw = os.getenv("AGGREGATOR_SOURCES_PATH", "").strip()
    sources_path = Path(sources_raw) if sources_raw else state_dir / "sources.txt"
    chat_sources_raw = os.getenv("AGGREGATOR_CHAT_SOURCES_PATH", "").strip()
    chat_sources_path = (
        Path(chat_sources_raw) if chat_sources_raw else state_dir / "chat_sources.txt"
    )
    drafts_dir = state_dir / "drafts"
    drafts_dir.mkdir(parents=True, exist_ok=True)
    return AggregatorPaths(
        state_dir=state_dir,
        db_path=state_dir / "aggregator.db",
        sources_path=sources_path,
        chat_sources_path=chat_sources_path,
        drafts_dir=drafts_dir,
    )


def load_file_env(env: MutableMapping[str, str] | None = None) -> None:
    """FOO_FILE=/path -> FOO=<file contents> for the known secret keys.

    Never overwrites an already-set FOO; missing files are a silent no-op so
    dry environments (tests, pre-enrollment) keep working.
    """
    target = env if env is not None else os.environ
    for key in _FILE_ENV_KEYS:
        if target.get(key):
            continue
        path_raw = target.get(f"{key}_FILE", "").strip()
        if not path_raw:
            continue
        path = Path(path_raw)
        if not path.exists():
            continue
        target[key] = path.read_text().strip()


def parse_sources(text: str) -> list[str]:
    """One source per line: @username / t.me/username / bare username.

    Comments (#...) and blanks skipped; t.me/+invite links skipped (not
    usernames); order-preserving dedup, case-insensitive key.
    """
    seen: set[str] = set()
    out: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        candidate = line
        candidate = re.sub(r"^https?://", "", candidate)
        candidate = re.sub(r"^t\.me/", "", candidate)
        candidate = candidate.lstrip("@").strip().rstrip("/")
        if candidate.startswith("+"):
            continue
        if not _USERNAME_RE.match(candidate):
            continue
        key = candidate.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(candidate)
    return out


def parse_chat_sources(text: str) -> list[ChatSource]:
    """Parse the discussion-chat ALLOWLIST — same conventions as ``parse_sources``.

    Deliberately a separate file from ``sources.txt``: the broadcast list stays
    clean, and a chat is only ever ingested because a human wrote it down (there is
    no "all joined chats" mode — the account is in 97 of them).

    Accepted forms, and what each maps to:

    | line                        | maps to                                          |
    |-----------------------------|--------------------------------------------------|
    | ``@handle`` / ``t.me/handle`` / ``handle`` | a PUBLIC chat handle, resolved against the ``linked_chat_username`` of a joined channel. Only such a chat can be cited in the digest. |
    | ``1788662720``              | a bare internal (positive) entity id             |
    | ``-1001788662720``          | the ``-100``-marked id a Telegram client shows   |
    | ``id:1788662720``           | the same, written explicitly                     |
    | ``t.me/c/1788662720/12``    | the internal deep link of a handle-less chat     |

    The id forms exist because a linked discussion group usually has NO public
    username — 33 of the 55 linked chats in the live dialogs (2026-07-30). An id-only
    chat is ingested but is *not citable*; see ``ResolvedChat.citable``.

    ``t.me/+invite`` links are skipped (not resolvable identities). Comments (#…)
    and blanks are skipped; order-preserving dedup, case-insensitive per form.
    """
    seen: set[tuple[str, str]] = set()
    out: list[ChatSource] = []
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        deep_link = _TME_ID_RE.match(line)
        if deep_link:
            entry = ChatSource("id", deep_link.group(1))
        else:
            candidate = re.sub(r"^https?://", "", line)
            candidate = re.sub(r"^t\.me/", "", candidate)
            candidate = candidate.lstrip("@").strip().rstrip("/")
            if candidate.lower().startswith("id:"):
                candidate = candidate[3:].strip()
            if candidate.startswith("+"):
                continue
            digits = candidate.lstrip("-")
            if digits.isdigit():
                # Normalise the marked form (-100<id>) to the positive internal id
                # Telethon reports in a dialog, which is what the proxy reads by.
                # Only a NEGATIVE -100… prefix is a marked id; a bare 100… id is a
                # real internal id and must not be mangled.
                if candidate.startswith("-100") and len(digits) > 3:
                    entry = ChatSource("id", digits[3:])
                else:
                    entry = ChatSource("id", digits)
            elif _USERNAME_RE.match(candidate):
                entry = ChatSource("username", candidate)
            else:
                continue
        key = (entry.form, entry.value.lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(entry)
    return out


def resolve_chat_sources(
    chat_sources: list[ChatSource], channels: list[Any]
) -> tuple[list[ResolvedChat], list[str]]:
    """Map allowlist entries onto joined discussion chats.

    A discussion group is ``broadcast=False``, so it never appears in
    ``list_channels`` itself — it is reachable only through the
    ``linked_chat_id`` / ``linked_chat_title`` / ``linked_chat_username`` of its
    parent channel record. That is exactly what a handle entry is resolved
    against; an id entry needs no lookup at all (the proxy reads a joined chat by
    id), and only borrows the title/parent from a channel record when one links to it.
    """
    by_username: dict[str, Any] = {}
    by_id: dict[int, Any] = {}
    for channel in channels:
        linked_id = getattr(channel, "linked_chat_id", None)
        if not linked_id:
            continue
        by_id[int(linked_id)] = channel
        handle = (getattr(channel, "linked_chat_username", None) or "").strip().lower()
        if handle:
            by_username.setdefault(handle, channel)

    resolved: list[ResolvedChat] = []
    unresolved: list[str] = []
    seen_ids: set[int] = set()
    for source in chat_sources:
        if source.form == "username":
            channel = by_username.get(source.value.lower())
            if channel is None:
                unresolved.append(source.value)
                continue
            entity_id = int(channel.linked_chat_id)
            username = getattr(channel, "linked_chat_username", None)
        else:
            entity_id = int(source.value)
            channel = by_id.get(entity_id)
            username = getattr(channel, "linked_chat_username", None) if channel else None
        if entity_id in seen_ids:
            continue
        seen_ids.add(entity_id)
        title = (
            (getattr(channel, "linked_chat_title", None) or "").strip() if channel else ""
        ) or f"chat:{entity_id}"
        parent_id = int(channel.entity_id) if channel is not None else None
        resolved.append(
            ResolvedChat(
                entity_id=entity_id,
                title=title,
                username=username,
                parent_channel_id=parent_id,
                parent_channel_key=f"channel:{parent_id}" if parent_id is not None else None,
            )
        )
    return resolved, unresolved


def chat_admission_verdict(
    message: dict[str, Any],
    *,
    dup_channel_ids: set[int],
    min_chars: int = CHAT_MIN_CHARS,
) -> str:
    """``"admit"`` or the reason this chat message is not digest material.

    Pure code, no LLM — the ingest path must stay deterministic, offline and free.
    A conversation is mostly chatter; the rules below say what "news" looks like in
    a chat, and were checked against ~570 live messages from three real chats:

    * ``service`` / ``via-bot`` — join/leave/pin events and inline-bot output are
      never news (the proxy already drops text-less messages; kept as defence).
    * ``nested-reply`` — a reply to another *comment* is conversation ABOUT
      something, not a new item. Note the mechanics: in a linked discussion group
      EVERY top-level comment carries ``reply_to`` (pointing at the auto-forwarded
      parent post), and only a reply-to-a-reply sets a *different*
      ``reply_to_top_id``. Dropping every ``reply_to`` would make a linked chat
      contribute nothing, so only the nested form is rejected. In a plain group a
      direct reply has no ``reply_to_top_id`` and survives this rule — the length
      and carrier rules below are what actually filter it.
    * ``too-short`` — under ``min_chars`` there is nothing to summarise.
    * ``forward-of-tracked-channel`` — the parent-post echo (and any forward from a
      channel already in the corpus): we hold the original, with its view count.
    * ``no-carrier`` — news arrives in a chat as a LINK or as a FORWARD. A long
      opinion with neither is exactly the chatter this lane must not import.
    """
    raw = message.get("raw_json") or {}
    if not isinstance(raw, dict):
        raw = {}
    text = str(message.get("text") or "").strip()
    if raw.get("action"):
        return "service"
    if raw.get("via_bot_id"):
        return "via-bot"
    reply_to = raw.get("reply_to") or {}
    if isinstance(reply_to, dict):
        top_id = reply_to.get("reply_to_top_id")
        if top_id and top_id != reply_to.get("reply_to_msg_id"):
            return "nested-reply"
    if len(text) < min_chars:
        return "too-short"
    forward = raw.get("fwd_from") or {}
    if not isinstance(forward, dict):
        forward = {}
    from_id = forward.get("from_id") or {}
    channel_id = from_id.get("channel_id") if isinstance(from_id, dict) else None
    if channel_id and int(channel_id) in dup_channel_ids:
        return "forward-of-tracked-channel"
    if not (_URL_RE.search(text) or forward):
        return "no-carrier"
    return "admit"


def _agg_connect(store: TelegramDigestStore) -> sqlite3.Connection:
    con = sqlite3.connect(store._db_path)  # noqa: SLF001 — same-package, own db file
    con.row_factory = sqlite3.Row
    return con


def _recent_corpus_index(
    store: TelegramDigestStore, *, window_hours: int
) -> tuple[set[str], set[str]]:
    """``(text keys, post links)`` of everything already in the corpus's window.

    The text keys reuse ``_dedup_key`` (NFKC + lower + collapsed whitespace) — the
    SAME normalisation ``build_draft_input`` dedups with, so a chat echo collapses
    on exactly the key the drafter would have collapsed on. Prior chat rows are
    included too, so yesterday's echo is not re-admitted today.

    The link set exists because the text key alone misses the OTHER echo shape,
    the dominant one in live data: a manual repost that PREPENDS the source link,
    which shifts the first 120 chars so the key no longer matches. A quoted
    ``t.me/<channel>/<id>`` is exact identity, not a heuristic — if we already hold
    that post, the chat message is a duplicate of it.
    """
    cutoff = (_utc_now() - timedelta(hours=window_hours)).isoformat()
    con = _agg_connect(store)
    try:
        rows = con.execute(
            """
            SELECT m.text AS text, m.link AS link
            FROM digest_messages m
            JOIN digest_sources s ON s.peer_key = m.peer_key
            WHERE s.role = ? AND m.posted_at >= ?
            """,
            (AGG_ROLE, cutoff),
        ).fetchall()
    finally:
        con.close()
    keys = {_dedup_key(str(row["text"] or "")) for row in rows if row["text"]}
    links = {_normalize_post_link(row["link"]) for row in rows if row["link"]}
    return keys, links - {""}


def _normalize_post_link(link: str | None) -> str:
    """``https://t.me/chan/12?single`` -> ``t.me/chan/12`` (comparable identity)."""
    text = str(link or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"^https?://", "", text)
    return text.split("?", 1)[0].rstrip("/")


def _quoted_post_links(text: str) -> set[str]:
    return {
        _normalize_post_link(match.group(0)) for match in _TME_POST_RE.finditer(text or "")
    }


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


async def collect(
    client: Any,
    store: TelegramDigestStore,
    sources: list[str],
    *,
    collect_limit: int = 200,
    channels: list[Any] | None = None,
) -> dict[str, Any]:
    """Resolve @usernames to joined dialogs via the proxy, ingest incrementally.

    Resolution uses list_channels (the account has joined the sources via the
    paced join loop); unresolved names are reported, not fatal — the join loop
    may still be pacing its way through the list. Per-source failures (FloodWait,
    network) skip that source this pass; watermark untouched -> retried next pass.

    ``channels`` lets a caller supply the already-fetched dialog list: list_channels
    is a FULL dialog sweep plus one GetFullChannel per channel (~60 RPCs), so the
    chat lane must reuse this one rather than sweep again in the same run.
    """
    if channels is None:
        channels = await client.list_channels(limit=500)
    by_username = {
        (c.username or "").lower(): c for c in channels if getattr(c, "username", None)
    }

    resolved = 0
    unresolved: list[str] = []
    collected = 0
    failed = 0

    for name in sources:
        channel = by_username.get(name.lower())
        if channel is None:
            unresolved.append(name)
            continue
        resolved += 1
        entity_id = int(channel.entity_id)
        peer_key = f"channel:{entity_id}"
        store.upsert_source(
            peer_key=peer_key,
            entity_id=entity_id,
            title=(channel.title or name).strip(),
            username=channel.username,
            kind="channel",
            linked_channel_key=None,
            role=AGG_ROLE,
        )
        last_id = store.last_message_id(peer_key)
        try:
            messages = await client.read_messages(
                kind="channel",
                entity_id=entity_id,
                min_id=last_id,
                limit=collect_limit,
                recent_first=last_id == 0,
            )
        except Exception as exc:  # noqa: BLE001 — per-source isolation
            failed += 1
            logger.warning("aggregator collect: skipping %s this pass: %s", name, exc)
            continue
        latest = last_id
        for message in messages:
            posted_raw = message.get("posted_at")
            posted_at = (
                datetime.fromisoformat(posted_raw)
                if isinstance(posted_raw, str) and posted_raw
                else _utc_now()
            )
            if store.insert_message(
                peer_key=peer_key,
                message_id=int(message["message_id"]),
                posted_at=posted_at,
                sender_id=message.get("sender_id"),
                views=message.get("views"),
                forwards=message.get("forwards"),
                replies=message.get("replies"),
                link=message.get("link"),
                text=str(message.get("text", "")).strip(),
                raw_json=message.get("raw_json") or {},
            ):
                collected += 1
            latest = max(latest, int(message["message_id"]))
        store.mark_collected(peer_key, latest if latest > 0 else None)

    return {
        "resolved": resolved,
        "unresolved": unresolved,
        "collected_messages": collected,
        "failed_sources": failed,
    }


async def collect_chats(
    client: Any,
    store: TelegramDigestStore,
    chat_sources: list[ChatSource],
    *,
    channels: list[Any] | None = None,
    read_limit: int = CHAT_READ_LIMIT,
    max_per_run: int = CHAT_MAX_PER_RUN,
    min_chars: int = CHAT_MIN_CHARS,
    dedup_window_hours: int = CHAT_DEDUP_WINDOW_HOURS,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Ingest ALLOWLISTED discussion chats into the aggregator corpus.

    This is an INPUT GATE: everything that decides whether a chat message may
    become digest material happens here, at the boundary, so nothing downstream
    (draft input, gates, publish) has to learn that chats exist beyond the
    ``origin`` label. Three bounds, in order:

    1. the allowlist (a human wrote the chat down — never "all joined chats");
    2. ``chat_admission_verdict`` — a deterministic quality gate;
    3. ``max_per_run`` — a hard per-chat ceiling on what survives, applied AFTER
       the gate, keeping the best candidates of the batch by
       (carries a link, longer text, newer) — see CHAT_MAX_PER_RUN for the numbers.

    Reads are recency-first from the ``digest_sources`` watermark, and the watermark
    advances past every message SEEN (not just the admitted ones): the cap must not
    turn into a stalled cursor that re-reads the same window forever. A burst larger
    than ``read_limit`` therefore skips its oldest excess on purpose — the digest
    only ever looks at the last 24h (this is also the 2026-07-24 lead-feed lesson:
    never crawl a busy chat oldest-first).

    ``dry_run=True`` performs the reads and the full gate but writes NOTHING (no
    source row, no message, no watermark) and reports the admitted text, so the
    operator can judge quality before turning the lane on.
    """
    if channels is None:
        channels = await client.list_channels(limit=500)
    resolved, unresolved = resolve_chat_sources(chat_sources, channels)

    # A forward from a channel whose posts we already ingest is a duplicate, not a
    # find; the same is true of the auto-forwarded parent post of a linked chat.
    tracked_channel_ids = {
        s.entity_id for s in store.list_sources(roles=(AGG_ROLE,)) if s.kind == "channel"
    }
    dedup_keys, corpus_links = _recent_corpus_index(
        store, window_hours=dedup_window_hours
    )

    collected = 0
    failed = 0
    reports: list[dict[str, Any]] = []

    for chat in resolved:
        peer_key = chat.peer_key
        if not dry_run:
            store.upsert_source(
                peer_key=peer_key,
                entity_id=chat.entity_id,
                title=chat.title,
                username=chat.username,
                kind=CHAT_KIND,
                linked_channel_key=chat.parent_channel_key,
                role=AGG_ROLE,
            )
        last_id = store.last_seen_message_id(peer_key)
        try:
            messages = await client.read_messages(
                kind=CHAT_KIND,
                entity_id=chat.entity_id,
                min_id=last_id,
                limit=read_limit,
                recent_first=True,
            )
        except Exception as exc:  # noqa: BLE001 — per-source isolation
            failed += 1
            logger.warning(
                "aggregator chat collect: skipping %s this pass: %s", peer_key, exc
            )
            continue

        dup_channel_ids = set(tracked_channel_ids)
        if chat.parent_channel_id is not None:
            dup_channel_ids.add(chat.parent_channel_id)

        rejected: dict[str, int] = {}
        candidates: list[tuple[tuple[int, int, int], dict[str, Any], str, set[str]]] = []
        latest_seen = last_id
        for message in messages:
            message_id = int(message["message_id"])
            latest_seen = max(latest_seen, message_id)
            verdict = chat_admission_verdict(
                message, dup_channel_ids=dup_channel_ids, min_chars=min_chars
            )
            if verdict != "admit":
                rejected[verdict] = rejected.get(verdict, 0) + 1
                continue
            text = str(message.get("text") or "").strip()
            key = _dedup_key(text)
            if key in dedup_keys:
                rejected["duplicate-text"] = rejected.get("duplicate-text", 0) + 1
                continue
            quoted = _quoted_post_links(text)
            if quoted & corpus_links:
                rejected["echo-of-corpus-post"] = rejected.get("echo-of-corpus-post", 0) + 1
                continue
            rank = (1 if _URL_RE.search(text) else 0, len(text), message_id)
            candidates.append((rank, message, key, quoted))

        candidates.sort(key=lambda item: item[0], reverse=True)
        admitted = candidates[:max_per_run]
        over_cap = len(candidates) - len(admitted)
        if over_cap:
            rejected["over-cap"] = over_cap

        texts: list[dict[str, Any]] = []
        chat_collected = 0
        for _, message, key, quoted in sorted(
            admitted, key=lambda item: int(item[1]["message_id"])
        ):
            # Feed the admitted item back into the index so a second chat quoting the
            # same post (or repeating the same text) in this very run is deduped too.
            dedup_keys.add(key)
            corpus_links |= quoted
            own_link = _normalize_post_link(message.get("link"))
            if own_link:
                corpus_links.add(own_link)
            text = str(message.get("text") or "").strip()
            texts.append(
                {
                    "message_id": int(message["message_id"]),
                    "link": message.get("link"),
                    "posted_at": message.get("posted_at"),
                    "text": text,
                }
            )
            if dry_run:
                continue
            posted_raw = message.get("posted_at")
            posted_at = (
                datetime.fromisoformat(posted_raw)
                if isinstance(posted_raw, str) and posted_raw
                else _utc_now()
            )
            if store.insert_message(
                peer_key=peer_key,
                message_id=int(message["message_id"]),
                posted_at=posted_at,
                sender_id=message.get("sender_id"),
                views=message.get("views"),
                forwards=message.get("forwards"),
                replies=message.get("replies"),
                link=message.get("link"),
                text=text,
                raw_json=message.get("raw_json") or {},
            ):
                chat_collected += 1
        collected += chat_collected
        if not dry_run:
            store.mark_collected(peer_key, latest_seen if latest_seen > 0 else None)

        report = {
            "peer_key": peer_key,
            "title": chat.title,
            "username": chat.username,
            # False ⇒ the proxy cannot build a t.me link here, so build_draft_input
            # drops these rows: the chat is corpus-only, never cited in a digest.
            "citable": chat.citable,
            "seen": len(messages),
            "admitted": len(admitted),
            "rejected": rejected,
        }
        if dry_run:
            report["texts"] = texts
        reports.append(report)

    return {
        "resolved": len(resolved),
        "unresolved": unresolved,
        "collected_messages": collected,
        "failed_sources": failed,
        "dry_run": dry_run,
        "chats": reports,
    }


def _dedup_key(text: str) -> str:
    norm = unicodedata.normalize("NFKC", text).lower()
    norm = " ".join(norm.split())
    return norm[:120]


def build_draft_input(
    store: TelegramDigestStore,
    *,
    window_hours: int = 24,
    max_posts: int = 150,
    recent_headlines: list[dict] | None = None,
) -> dict[str, Any]:
    cutoff = (_utc_now() - timedelta(hours=window_hours)).isoformat()
    con = sqlite3.connect(store._db_path)  # noqa: SLF001 — same-package, own db file
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            """
            SELECT s.title AS channel, s.username AS username, s.kind AS kind,
                   m.link, m.text, m.views, m.forwards, m.posted_at
            FROM digest_messages m
            JOIN digest_sources s ON s.peer_key = m.peer_key
            WHERE s.role = ? AND m.posted_at >= ?
            ORDER BY COALESCE(m.views, 0) DESC
            """,
            (AGG_ROLE, cutoff),
        ).fetchall()
    finally:
        con.close()

    best: dict[str, sqlite3.Row] = {}
    for row in rows:  # rows arrive views-DESC, so first wins per dedup key
        text = (row["text"] or "").strip()
        if len(text) < 80 or not row["link"]:
            continue
        best.setdefault(_dedup_key(text), row)

    posts = [
        {
            "channel": r["channel"],
            "username": r["username"],
            # Origin comes straight from digest_sources.kind — the corpus already
            # distinguishes a broadcast source from a discussion chat, so marking
            # origin needs NO new column and NO migration. Chat rows also carry
            # views=NULL, so the views-DESC ordering above puts them last: they can
            # only ever fill slots the channels left empty under max_posts.
            "origin": "chat" if r["kind"] == CHAT_KIND else "channel",
            "link": r["link"],
            "text": (r["text"] or "").strip(),
            "views": r["views"],
            "forwards": r["forwards"],
            "posted_at": r["posted_at"],
        }
        for r in list(best.values())[:max_posts]
    ]
    return {
        "date": _utc_now().date().isoformat(),
        "window_hours": window_hours,
        "posts": posts,
        # Prior-window shipped headlines: the LLM must not repeat these (A1
        # semantic dedup). Kept ledger-free / pure — the CLI supplies the list.
        "recent_headlines": recent_headlines or [],
    }
