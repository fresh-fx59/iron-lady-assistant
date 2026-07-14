"""src/telegram_aggregator_publish.py — deterministic rendering + Bot API publish.

Rendering, splitting, link emission, footer, and the actual send are ALL code —
the model never touches the wire format (deliver-critical-values-by-code rule).
Publisher design (injectable Transport, 2-phase ledger, dry-run) mirrors
dzen-autopilot's post.py, adapted for multi-message digests.
"""
from __future__ import annotations

import html
import json
import logging
import os
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from .telegram_aggregator_gates import Story

logger = logging.getLogger(__name__)

_MESSAGE_CAP = 4000  # under the 4096 Bot API ceiling; split at story boundaries


def _render_story(index: int, story: Story) -> str:
    links = " · ".join(
        f'<a href="{html.escape(link, quote=True)}">{html.escape(_link_label(link))}</a>'
        for link in story.source_links
    )
    return (
        f"{index}. <b>{html.escape(story.headline)}</b>\n"
        f"{html.escape(story.summary)}\n"
        f"Источники: {links}"
    )


def _link_label(link: str) -> str:
    path = urllib.parse.urlparse(link).path.strip("/")
    return "@" + path.split("/")[0] if path else link


def _fit_block(block: str) -> str:
    """Truncate an over-cap story block by shortening ONLY the summary line.

    A block is `headline\\nsummary\\nИсточники: ...`. If it exceeds the cap even
    on its own (no other block to share blame with), shrink the summary line —
    never the headline, never the links line — so the whole block fits, and mark
    the cut with a trailing "…".
    """
    if len(block) <= _MESSAGE_CAP:
        return block
    lines = block.split("\n")
    if len(lines) != 3:
        # Not the expected headline/summary/links shape — nothing safe to trim.
        return block
    headline, summary, links_line = lines
    overhead = len(headline) + len(links_line) + 2  # two '\n' joins
    budget = _MESSAGE_CAP - overhead - 1  # reserve 1 char for the "…" marker
    if budget < 0:
        budget = 0
    truncated_summary = summary[:budget].rstrip() + "…"
    return f"{headline}\n{truncated_summary}\n{links_line}"


def render_messages(stories: list[Story], *, date_label: str, footer: str) -> list[str]:
    header = f"📰 <b>AI-дайджест — {html.escape(date_label)}</b>"
    blocks = [_fit_block(_render_story(i + 1, s)) for i, s in enumerate(stories)]
    footer_block = html.escape(footer)

    messages: list[str] = []
    current = header
    for block in blocks:
        candidate = f"{current}\n\n{block}"
        # Always flush on overflow — even when `current` is still just the bare
        # header. `_fit_block` only guarantees a block fits the cap on its own;
        # prefixed with the header it can still overflow, and the old
        # `current != header` guard forced that combination through anyway
        # (that was the "oversized story ships over cap" bug).
        if len(candidate) > _MESSAGE_CAP:
            messages.append(current)
            current = block
        else:
            current = candidate
    # attach footer to the last message, splitting once more if it would overflow
    with_footer = f"{current}\n\n{footer_block}"
    if len(with_footer) > _MESSAGE_CAP:
        messages.append(current)
        messages.append(footer_block)
    else:
        messages.append(with_footer)
    return messages


class Transport(Protocol):
    def send_message(self, chat: str, text: str) -> int: ...


class BotApiTransport:
    """Bot API sender: HTML parse mode, no link previews, honors retry_after once."""

    def __init__(self, token: str) -> None:
        self._token = token.strip()

    def send_message(self, chat: str, text: str) -> int:
        payload = {
            "chat_id": chat,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        for attempt in (1, 2):
            data = urllib.parse.urlencode(payload).encode()
            request = urllib.request.Request(
                f"https://api.telegram.org/bot{self._token}/sendMessage", data=data
            )
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    body = json.loads(response.read().decode())
                return int(body["result"]["message_id"])
            except urllib.error.HTTPError as exc:  # noqa: PERF203
                body = json.loads(exc.read().decode() or "{}")
                retry_after = (body.get("parameters") or {}).get("retry_after")
                if exc.code == 429 and retry_after and attempt == 1:
                    time.sleep(min(int(retry_after), 60))
                    continue
                raise
        raise RuntimeError("unreachable")


class DigestLedger:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS digests (
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
            # Additive migration for dbs created before send-accounting existed.
            # ADD COLUMN with a constant DEFAULT is metadata-only in SQLite — it
            # does not rewrite the table, and every existing row reads back as
            # sent_count=0.
            digest_cols = {
                row["name"] for row in con.execute("PRAGMA table_info(digests)").fetchall()
            }
            if "sent_count" not in digest_cols:
                con.execute("ALTER TABLE digests ADD COLUMN sent_count INTEGER NOT NULL DEFAULT 0")

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self._db_path)
        con.row_factory = sqlite3.Row
        return con

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def upsert_draft(self, date_key: str, messages: list[str]) -> int:
        now = self._now()
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO digests(date_key, messages_json, status, created_at, updated_at)
                VALUES (?, ?, 'pending', ?, ?)
                ON CONFLICT(date_key) DO UPDATE SET
                    messages_json = CASE WHEN digests.status IN ('pending','approved')
                                         THEN excluded.messages_json ELSE digests.messages_json END,
                    status = CASE WHEN digests.status IN ('pending','approved')
                                  THEN 'pending' ELSE digests.status END,
                    updated_at = CASE WHEN digests.status IN ('pending','approved')
                                      THEN excluded.updated_at ELSE digests.updated_at END
                """,
                (date_key, json.dumps(messages, ensure_ascii=False), now, now),
            )
        return len(messages)

    def approve(self, date_key: str | None = None) -> str | None:
        with self._connect() as con:
            if date_key is None:
                row = con.execute(
                    "SELECT date_key FROM digests WHERE status = 'pending' ORDER BY date_key DESC LIMIT 1"
                ).fetchone()
                if row is None:
                    return None
                date_key = str(row["date_key"])
            cur = con.execute(
                "UPDATE digests SET status = 'approved', updated_at = ? WHERE date_key = ? AND status = 'pending'",
                (self._now(), date_key),
            )
            return date_key if cur.rowcount == 1 else None

    def next_approved(self) -> tuple[str, list[str]] | None:
        with self._connect() as con:
            row = con.execute(
                "SELECT date_key, messages_json FROM digests WHERE status = 'approved' ORDER BY date_key ASC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        return str(row["date_key"]), list(json.loads(row["messages_json"]))

    def has_stuck_sending(self) -> bool:
        with self._connect() as con:
            row = con.execute("SELECT 1 FROM digests WHERE status = 'sending' LIMIT 1").fetchone()
        return row is not None

    def begin_send(self, date_key: str) -> bool:
        with self._connect() as con:
            cur = con.execute(
                "UPDATE digests SET status = 'sending', updated_at = ? WHERE date_key = ? AND status = 'approved'",
                (self._now(), date_key),
            )
            return cur.rowcount == 1

    def revert_to_approved(self, date_key: str) -> None:
        with self._connect() as con:
            con.execute(
                "UPDATE digests SET status = 'approved', updated_at = ? WHERE date_key = ? AND status = 'sending'",
                (self._now(), date_key),
            )

    def record_sent(self, date_key: str, sent_count: int) -> None:
        with self._connect() as con:
            con.execute(
                "UPDATE digests SET sent_count = ?, updated_at = ? WHERE date_key = ?",
                (sent_count, self._now(), date_key),
            )

    def mark_posted(self, date_key: str) -> None:
        with self._connect() as con:
            con.execute(
                "UPDATE digests SET status = 'posted', updated_at = ? WHERE date_key = ?",
                (self._now(), date_key),
            )

    def mark_failed(self, date_key: str, error: str) -> None:
        with self._connect() as con:
            con.execute(
                "UPDATE digests SET status = 'failed', error = ?, updated_at = ? WHERE date_key = ?",
                (error[:500], self._now(), date_key),
            )


def publish_next(
    ledger: DigestLedger,
    transport: Transport | None,
    chat: str | None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    if ledger.has_stuck_sending():
        # A digest died mid-send. Posting anything else risks duplicates in the
        # public channel — freeze and make a human look (mirror dzen post.py).
        logger.error("aggregator publish: stuck 'sending' row — publishing blocked")
        return {"status": "blocked"}
    item = ledger.next_approved()
    if item is None:
        return {"status": "skipped"}
    date_key, messages = item
    effective_dry = dry_run or transport is None or not chat
    if not ledger.begin_send(date_key):
        return {"status": "skipped"}
    if effective_dry:
        for i, message in enumerate(messages, 1):
            print(f"[dry-run] {date_key} message {i}/{len(messages)} -> {chat or '<unset>'}\n{message}\n")
        ledger.revert_to_approved(date_key)
        return {"status": "dry-run", "date_key": date_key, "messages": len(messages)}
    sent = 0
    try:
        for i, message in enumerate(messages, 1):
            transport.send_message(chat, message)
            sent = i
            ledger.record_sent(date_key, sent)
            time.sleep(1.0)  # pace multi-message digests well under flood limits
    except Exception as exc:  # noqa: BLE001
        # Do NOT mark_failed: the row stays 'sending', which already blocks all
        # future publishing (has_stuck_sending). That is the point — some of
        # `messages` already went out to the public channel, so a human must
        # inspect sent_count before this date_key can be touched again; auto
        # -reverting or auto-failing here would let publish_next re-send the
        # already-posted messages and double-post.
        logger.error("aggregator publish: send failed for %s: %s", date_key, exc)
        return {
            "status": "failed",
            "date_key": date_key,
            "error": str(exc),
            "sent": sent,
            "total": len(messages),
        }
    ledger.mark_posted(date_key)
    return {"status": "posted", "date_key": date_key, "messages": len(messages)}


def notify_operator(text: str) -> bool:
    """Operator ping via the iron-lady bot token — plain code, no LLM in the path."""
    token = os.getenv("IRONLADY_NOTIFY_BOT_TOKEN", "").strip()
    chat_id = os.getenv("AGGREGATOR_OPERATOR_CHAT_ID", "").strip()
    if not token or not chat_id:
        return False
    try:
        BotApiTransport(token).send_message(chat_id, text[:4000])
        return True
    except Exception as exc:  # noqa: BLE001 — notification must never kill the pipeline
        logger.warning("aggregator notify failed: %s", exc)
        return False
