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
    "IRONLADY_NOTIFY_BOT_TOKEN",
)

_USERNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{3,31}$")


@dataclass(frozen=True)
class AggregatorPaths:
    state_dir: Path
    db_path: Path
    sources_path: Path
    drafts_dir: Path


def resolve_paths() -> AggregatorPaths:
    state_dir = Path(
        os.getenv("AGGREGATOR_STATE_DIR", "/home/claude-developer/telegram-aggregator")
    )
    sources_raw = os.getenv("AGGREGATOR_SOURCES_PATH", "").strip()
    sources_path = Path(sources_raw) if sources_raw else state_dir / "sources.txt"
    drafts_dir = state_dir / "drafts"
    drafts_dir.mkdir(parents=True, exist_ok=True)
    return AggregatorPaths(
        state_dir=state_dir,
        db_path=state_dir / "aggregator.db",
        sources_path=sources_path,
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


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


async def collect(
    client: Any,
    store: TelegramDigestStore,
    sources: list[str],
    *,
    collect_limit: int = 200,
) -> dict[str, Any]:
    """Resolve @usernames to joined dialogs via the proxy, ingest incrementally.

    Resolution uses list_channels (the account has joined the sources via the
    paced join loop); unresolved names are reported, not fatal — the join loop
    may still be pacing its way through the list. Per-source failures (FloodWait,
    network) skip that source this pass; watermark untouched -> retried next pass.
    """
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


def _dedup_key(text: str) -> str:
    norm = unicodedata.normalize("NFKC", text).lower()
    norm = " ".join(norm.split())
    return norm[:120]


def build_draft_input(
    store: TelegramDigestStore,
    *,
    window_hours: int = 24,
    max_posts: int = 150,
) -> dict[str, Any]:
    cutoff = (_utc_now() - timedelta(hours=window_hours)).isoformat()
    con = sqlite3.connect(store._db_path)  # noqa: SLF001 — same-package, own db file
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            """
            SELECT s.title AS channel, s.username AS username,
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
    }
