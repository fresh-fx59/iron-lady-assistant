"""src/telegram_aggregator_publish.py — deterministic rendering + Bot API publish.

Rendering, splitting, link emission, footer, and the actual send are ALL code —
the model never touches the wire format (deliver-critical-values-by-code rule).
Publisher design (injectable Transport, 2-phase ledger, dry-run) mirrors
dzen-autopilot's post.py, adapted for multi-message digests.

**ONE post = image + full text.** When the day has a hero image the digest ships
as a SINGLE `sendRichMessage` post (Bot API **10.2**, 2026-07-14): the image is a
media block inside a rich-HTML document that carries the whole digest, up to
32768 chars. This replaced the old two-post shape (`sendPhoto` with a 1024-char
caption, then the digest as separate `sendMessage`s) — the 1024 caption ceiling
never fit a real digest (measured 1292-3097 chars), so *every* image day posted
twice. **Requires Bot API >= 10.2** — if the method ever goes away the endpoint
answers 404 (verified: a bogus method 404s while an empty rich body 400s), which
PROVES nothing was posted and degrades to the old photo+text plan instead of
freezing the channel. That plan stays reachable as the documented fallback.
"""
from __future__ import annotations

import html
import json
import logging
import os
import socket
import sqlite3
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from .telegram_aggregator_gates import Story

logger = logging.getLogger(__name__)

_MESSAGE_CAP = 4000  # under the 4096 Bot API ceiling; split at story boundaries

# The operator-alert ceiling, exported so a caller that RENDERS to a budget and
# this module that SLICES to one can never drift apart (2026-07-31: the scanner
# budgeted to its own 4000 and this sliced at a literal 4000 — the same number by
# luck, which is exactly how a truncation marker ends up cut in half).
OPERATOR_ALERT_CAP = 4000
_CAPTION_CAP = 1024  # Telegram sendPhoto caption ceiling (degrade path only)
_RICH_CAP = 32768  # Bot API 10.2 rich-message text ceiling (chars)
_RICH_MEDIA_ID = "hero"  # InputRichMessageMedia.id <-> tg://photo?id= <-> attach://


class PhotoNotSent(Exception):
    """A media-send failure that PROVES nothing reached Telegram.

    Raised only for file/DNS/connection errors (and, on the rich path, a 404 =
    method does not exist) — the request body demonstrably never left the host,
    was never built, or hit a method Telegram has no handler for. Any other
    failure (HTTP error after receipt, response-read timeout, JSON/KeyError
    parsing the OK response) is *ambiguous*: Telegram may already hold the post,
    so it must NOT be treated as PhotoNotSent — falling back to another plan
    would double-post. Name kept for compatibility; it covers send_photo and
    send_rich_message alike."""


# --- cross-day dedup (A1): structured-story (de)serialization + normalization ---
def serialize_stories(stories: list[Story]) -> str:
    """JSON-encode staged stories for the ledger row (headline/summary/links only)."""
    return json.dumps(
        [
            {"headline": s.headline, "summary": s.summary, "source_links": list(s.source_links)}
            for s in stories
        ],
        ensure_ascii=False,
    )


def deserialize_stories(raw: str | None) -> list[Story]:
    """Tolerant inverse of serialize_stories: any malformed input -> []."""
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(data, list):
        return []
    out: list[Story] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        links = item.get("source_links")
        if not isinstance(links, list):
            continue
        try:
            out.append(
                Story(
                    headline=str(item["headline"]),
                    summary=str(item["summary"]),
                    source_links=[str(x) for x in links],
                )
            )
        except KeyError:
            continue
    return out


def _norm_title(headline: str) -> str:
    """Normalized dedup key for a headline — mirrors telegram_aggregator._dedup_key
    (NFKC + lower + collapse whitespace), capped at 200 chars."""
    norm = unicodedata.normalize("NFKC", headline).lower()
    norm = " ".join(norm.split())
    return norm[:200]


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


def render_rich_html(messages: list[str], *, media_id: str = _RICH_MEDIA_ID) -> str:
    """One rich-HTML document (Bot API 10.2) carrying image + the WHOLE digest.

    Input is `render_messages`' output — the already-approved text, verbatim — so
    the wording, the `Источники:` links and the escaping are exactly
    `_render_story`'s and are never forked here. The messages are rejoined into
    the single document they were split from (`render_messages` splits only at
    the `\\n\\n` block boundaries it inserted), then each block becomes one rich
    block: header/story blocks are `<p>`, the trailing block — the footer, by
    construction always last — is `<footer>`. In-block newlines become `<br>`
    because rich HTML collapses raw newlines.

    Rich HTML accepts a NARROWER tag set than normal Bot API HTML; the tags used
    here (`<p> <br> <b> <a href> <figure> <img> <footer>`) and the entities
    `html.escape` emits (`&lt; &gt; &amp; &quot;`) are all in it.
    """
    document = "\n\n".join(messages)
    blocks = [b for b in document.split("\n\n") if b.strip()]
    # Media "can be specified only as a separate block": one leading <figure>
    # referencing the uploaded photo by id (the id is bound to the multipart file
    # part via attach:// in BotApiTransport.send_rich_message).
    parts = [f'<figure><img src="tg://photo?id={media_id}"/></figure>']
    last = len(blocks) - 1
    for i, block in enumerate(blocks):
        tag = "footer" if i == last and len(blocks) > 1 else "p"
        parts.append(f"<{tag}>{block.replace(chr(10), '<br>')}</{tag}>")
    return "".join(parts)


def _build_photo_text_ops(
    messages: list[str], image_path: str, short_caption: str
) -> list[tuple]:
    """The pre-10.2 photo plan — now the documented degrade path only.

    - A SINGLE message that fits the caption cap -> the whole digest rides as the
      photo's caption (one clean photo post, no trailing text).
    - Otherwise -> a leading photo with a short caption, then the full digest
      text as separate message(s), i.e. TWO+ posts.

    NOTE (known, deliberately unchanged): the cap check compares `len()` of raw
    HTML against a ceiling Telegram applies to *visible* text, so it under-counts
    the budget. Harmless (conservative) and out of scope for the one-post work.
    """
    if len(messages) == 1 and len(messages[0]) <= _CAPTION_CAP:
        return [("photo", image_path, messages[0])]
    return [("photo", image_path, short_caption)] + [("text", m) for m in messages]


def _build_send_ops(
    messages: list[str], image_path: str | None, short_caption: str
) -> list[tuple]:
    """Plan the ordered send operations for a digest.

    - No image -> one text op per message (the pre-A2 behavior).
    - Image -> a SINGLE rich op: image + the whole digest in one post.
    - Image whose rich HTML would blow the 32768 rich cap -> the legacy
      photo+text plan (`_build_photo_text_ops`).

    Rich ops are ``("rich", rich_html, path)``; photo ops are
    ``("photo", path, caption)``; text ops are ``("text", msg)``.
    """
    if not image_path:
        return [("text", m) for m in messages]
    rich_html = render_rich_html(messages)
    if len(rich_html) <= _RICH_CAP:
        return [("rich", rich_html, image_path)]
    logger.warning(
        "aggregator publish: rich html %d chars > %d cap — falling back to photo+text",
        len(rich_html),
        _RICH_CAP,
    )
    return _build_photo_text_ops(messages, image_path, short_caption)


def _send_plans(
    messages: list[str], image_path: str | None, short_caption: str
) -> list[list[tuple]]:
    """The primary plan plus its ordered degrade rungs.

    A rung is tried ONLY when the previous one failed in a way that PROVES
    nothing reached Telegram (`PhotoNotSent` before any op succeeded) — see
    publish_next. Rungs: rich (one post) -> photo+text -> text-only.
    """
    plans = [_build_send_ops(messages, image_path, short_caption)]
    if image_path:
        photo_text = _build_photo_text_ops(messages, image_path, short_caption)
        if photo_text != plans[0]:
            plans.append(photo_text)
        plans.append([("text", m) for m in messages])
    return plans


def _multipart(
    fields: dict[str, str],
    *,
    files: dict[str, tuple[str, str, bytes]],
    boundary: str,
) -> bytes:
    """Encode a multipart/form-data body (stdlib only — no `requests`).

    `files` maps the form field name -> (filename, content_type, bytes). Field
    names matter on the wire: sendPhoto wants `photo`, while sendRichMessage
    references its upload by the name an `attach://<name>` points at.
    """
    parts: list[bytes] = []
    for name, value in fields.items():
        parts.append(f"--{boundary}".encode())
        parts.append(f'Content-Disposition: form-data; name="{name}"'.encode())
        parts.append(b"")
        parts.append(str(value).encode("utf-8"))
    for name, (filename, content_type, file_bytes) in files.items():
        parts.append(f"--{boundary}".encode())
        parts.append(
            f'Content-Disposition: form-data; name="{name}"; filename="{filename}"'.encode()
        )
        parts.append(f"Content-Type: {content_type}".encode())
        parts.append(b"")
        parts.append(file_bytes)
    parts.append(f"--{boundary}--".encode())
    parts.append(b"")
    return b"\r\n".join(parts)


def _read_media(path: str) -> bytes:
    """Read a media file for upload. Unreadable => PhotoNotSent: the request body
    was never even built, so nothing can be live on the channel."""
    try:
        return Path(path).read_bytes()
    except OSError as exc:
        raise PhotoNotSent(f"cannot read media {path!r}: {exc}") from exc


class Transport(Protocol):
    def send_message(self, chat: str, text: str, *, parse_mode: str | None = "HTML") -> int: ...
    def send_photo(self, chat: str, photo_path: str, caption: str) -> int: ...
    def send_rich_message(self, chat: str, rich_html: str, photo_path: str) -> int: ...


class BotApiTransport:
    """Bot API sender: HTML parse mode, no link previews, honors retry_after once."""

    def __init__(self, token: str) -> None:
        self._token = token.strip()

    def send_message(self, chat: str, text: str, *, parse_mode: str | None = "HTML") -> int:
        """`parse_mode=None` omits the field entirely => Telegram sends the body
        VERBATIM. The digest's own posts are HTML and keep the default; anything
        that is plain text must say so, because HTML mode rejects the WHOLE
        message on the first bare `<` (prod 2026-07-31: a dialog-scan report
        containing "scores 0.19 < 0.35" 400'd and reached nobody). Escaping the
        report instead would mangle the operator's numbers and only move the
        cliff to the next unescaped character."""
        payload = {
            "chat_id": chat,
            "text": text,
            "disable_web_page_preview": True,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
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

    def _upload(
        self,
        method: str,
        fields: dict[str, str],
        files: dict[str, tuple[str, str, bytes]],
        *,
        not_sent_on_404: bool = False,
    ) -> int:
        """POST one multipart media send; honors a single 429 `retry_after`.

        The failure CLASSIFICATION lives here, once, for every media method —
        it decides whether the caller may safely re-send by another route, so a
        wrong answer double-posts to the public channel:

        - HTTP status  => Telegram ANSWERED, so it may already hold the post =>
          ambiguous, re-raise. Exception: 404 with `not_sent_on_404` means the
          method itself does not exist, which PROVES nothing was posted.
        - DNS / connection error => the body never reached Telegram => PhotoNotSent.
        - timeout (socket.timeout/TimeoutError) => ambiguous, re-raise.
        """
        boundary = uuid.uuid4().hex
        body = _multipart(fields, files=files, boundary=boundary)
        headers = {"Content-Type": f"multipart/form-data; boundary={boundary}"}
        for attempt in (1, 2):
            request = urllib.request.Request(
                f"https://api.telegram.org/bot{self._token}/{method}",
                data=body,
                headers=headers,
            )
            try:
                with urllib.request.urlopen(request, timeout=60) as response:
                    parsed = json.loads(response.read().decode())
                return int(parsed["result"]["message_id"])
            except urllib.error.HTTPError as exc:  # noqa: PERF203
                err = json.loads(exc.read().decode() or "{}")
                retry_after = (err.get("parameters") or {}).get("retry_after")
                if exc.code == 429 and retry_after and attempt == 1:
                    time.sleep(min(int(retry_after), 60))
                    continue
                if exc.code == 404 and not_sent_on_404:
                    raise PhotoNotSent(
                        f"{method} unavailable on this Bot API endpoint (404)"
                    ) from exc
                raise
            except urllib.error.URLError as exc:
                # URLError is HTTPError's parent, so this branch sees only the
                # NON-HTTP transport failures.
                if isinstance(exc.reason, (socket.gaierror, ConnectionError)):
                    raise PhotoNotSent(
                        f"{method} to {fields.get('chat_id')!r} never reached "
                        f"Telegram: {exc.reason}"
                    ) from exc
                raise
        raise RuntimeError("unreachable")

    def send_photo(self, chat: str, photo_path: str, caption: str) -> int:
        """Legacy photo send — the degrade path now that rich messages ship one post.

        Read the file once; caption is hard-capped at the Bot API ceiling. A file
        we can't read never left the host => PhotoNotSent (safe to degrade at the
        caller).
        """
        file_bytes = _read_media(photo_path)
        caption = (caption or "")[:_CAPTION_CAP]
        return self._upload(
            "sendPhoto",
            {"chat_id": chat, "caption": caption, "parse_mode": "HTML"},
            {"photo": (Path(photo_path).name, "image/png", file_bytes)},
        )

    def send_rich_message(self, chat: str, rich_html: str, photo_path: str) -> int:
        """ONE post = image + full text, via Bot API 10.2 `sendRichMessage`.

        The photo is uploaded in the same multipart body under the field name
        `_RICH_MEDIA_ID`; `InputRichMessageMedia.media` points at it with
        `attach://<name>`, and the rich HTML places it with
        `<img src="tg://photo?id=<name>">`. Returns the single Message's id.
        """
        file_bytes = _read_media(photo_path)
        rich_message = {
            "html": rich_html,
            "media": [
                {
                    "id": _RICH_MEDIA_ID,
                    "media": {"type": "photo", "media": f"attach://{_RICH_MEDIA_ID}"},
                }
            ],
        }
        return self._upload(
            "sendRichMessage",
            {"chat_id": chat, "rich_message": json.dumps(rich_message, ensure_ascii=False)},
            {_RICH_MEDIA_ID: (Path(photo_path).name, "image/png", file_bytes)},
            # A 404 here means the endpoint has no sendRichMessage (pre-10.2) =>
            # nothing was posted => degrade instead of freezing the pipeline.
            not_sent_on_404=True,
        )


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
                    sent_count INTEGER NOT NULL DEFAULT 0,
                    stories_json TEXT,
                    image_path TEXT
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
            # A1 cross-day dedup: stage structured stories on the digest row so
            # publish can PROMOTE them into the published window. Nullable ADD
            # COLUMN is metadata-only; legacy rows read back as NULL (= no-op).
            if "stories_json" not in digest_cols:
                con.execute("ALTER TABLE digests ADD COLUMN stories_json TEXT")
            # A2 infographic: the gate-generated hero image path (NULL => text
            # -only publish). Nullable ADD COLUMN is metadata-only; legacy rows
            # read back as NULL.
            if "image_path" not in digest_cols:
                con.execute("ALTER TABLE digests ADD COLUMN image_path TEXT")
            # Persistent record of what actually SHIPPED (one row per story×url),
            # the source of truth for the rolling dedup window. Additive +
            # idempotent, exactly like the digests guards above.
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS published_stories (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    date_key     TEXT NOT NULL,
                    norm_title   TEXT NOT NULL,
                    url          TEXT NOT NULL,
                    headline     TEXT NOT NULL,
                    published_at TEXT NOT NULL
                )
                """
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_published_stories_url ON published_stories(url)"
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_published_stories_date ON published_stories(date_key)"
            )

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self._db_path)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA busy_timeout=5000")
        return con

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def upsert_draft(
        self, date_key: str, messages: list[str], stories_json: str | None = None
    ) -> int:
        now = self._now()
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO digests(date_key, messages_json, stories_json, status, created_at, updated_at)
                VALUES (?, ?, ?, 'pending', ?, ?)
                ON CONFLICT(date_key) DO UPDATE SET
                    messages_json = CASE WHEN digests.status IN ('pending','approved')
                                         THEN excluded.messages_json ELSE digests.messages_json END,
                    stories_json = CASE WHEN digests.status IN ('pending','approved')
                                        THEN excluded.stories_json ELSE digests.stories_json END,
                    status = CASE WHEN digests.status IN ('pending','approved')
                                  THEN 'pending' ELSE digests.status END,
                    updated_at = CASE WHEN digests.status IN ('pending','approved')
                                      THEN excluded.updated_at ELSE digests.updated_at END
                """,
                (date_key, json.dumps(messages, ensure_ascii=False), stories_json, now, now),
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

    # --- A2 infographic: hero-image path (set at gate, read at publish) ---
    def set_image_path(self, date_key: str, image_path: str) -> None:
        with self._connect() as con:
            con.execute(
                "UPDATE digests SET image_path = ?, updated_at = ? WHERE date_key = ?",
                (image_path, self._now(), date_key),
            )

    def image_path_for(self, date_key: str) -> str | None:
        with self._connect() as con:
            row = con.execute(
                "SELECT image_path FROM digests WHERE date_key = ?", (date_key,)
            ).fetchone()
        if row is None or row["image_path"] is None:
            return None
        return str(row["image_path"])

    def status_for(self, date_key: str) -> str | None:
        """Current status of the digest row (None if the day has no row).

        Lets the gate skip a wasted gpt-image call on a day that is already
        final (posted/sending/failed): upsert_draft is a no-op for such rows, so
        nothing new can publish and the image would never be used."""
        with self._connect() as con:
            row = con.execute(
                "SELECT status FROM digests WHERE date_key = ?", (date_key,)
            ).fetchone()
        if row is None:
            return None
        return str(row["status"])

    # --- A1 cross-day dedup: window reads + promotion ---
    def published_urls_since(self, cutoff_date: str) -> set[str]:
        """Set of source urls that SHIPPED on or after cutoff_date (inclusive).

        date_key is an ISO date, so lexical >= is chronological >=. The window
        is the caller's parameter — nothing about 7 days is baked in here."""
        with self._connect() as con:
            rows = con.execute(
                "SELECT DISTINCT url FROM published_stories WHERE date_key >= ?",
                (cutoff_date,),
            ).fetchall()
        return {row["url"] for row in rows}

    def published_headlines_since(self, cutoff_date: str) -> list[dict]:
        """Shipped headlines in the window, deduped by norm_title, newest-first.

        Feeds the LLM's semantic dedup via the draft input's recent_headlines."""
        with self._connect() as con:
            rows = con.execute(
                "SELECT DISTINCT date_key, headline, norm_title FROM published_stories "
                "WHERE date_key >= ? ORDER BY date_key DESC",
                (cutoff_date,),
            ).fetchall()
        seen: set[str] = set()
        out: list[dict] = []
        for row in rows:
            if row["norm_title"] in seen:
                continue
            seen.add(row["norm_title"])
            out.append({"date": row["date_key"], "headline": row["headline"]})
        return out

    def record_published_stories(self, date_key: str) -> int:
        """Promote the row's staged stories into published_stories (one row per
        story×url). Idempotent: delete-then-insert, so re-running a posted day is
        a no-op on the count. NULL/empty stories_json -> 0 (legacy rows)."""
        with self._connect() as con:
            row = con.execute(
                "SELECT stories_json FROM digests WHERE date_key = ?", (date_key,)
            ).fetchone()
            stories = deserialize_stories(row["stories_json"] if row else None)
            con.execute("DELETE FROM published_stories WHERE date_key = ?", (date_key,))
            now = self._now()
            count = 0
            for story in stories:
                norm_title = _norm_title(story.headline)
                for url in story.source_links:
                    con.execute(
                        "INSERT INTO published_stories(date_key, norm_title, url, headline, published_at) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (date_key, norm_title, url, story.headline, now),
                    )
                    count += 1
        return count


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

    # Resolve the gate-generated hero image, if any. A stale/missing file path
    # degrades silently to text-only — never block a good digest on the image.
    stored_image = ledger.image_path_for(date_key)
    image_path = stored_image if (stored_image and Path(stored_image).exists()) else None
    try:
        date_label = datetime.fromisoformat(date_key).strftime("%d.%m.%Y")
    except ValueError:
        date_label = date_key
    short_caption = f"📰 AI-дайджест — {html.escape(date_label)}"

    if not ledger.begin_send(date_key):
        return {"status": "skipped"}

    plans = _send_plans(messages, image_path, short_caption)
    ops = plans[0]
    if effective_dry:
        for i, op in enumerate(ops, 1):
            if op[0] == "rich":
                # ONE post: image + the whole digest. Describe the plan, then the
                # rich HTML itself so a human can still review what will ship.
                print(
                    f"[dry-run] {date_key} op {i}/{len(ops)} RICH -> "
                    f"{chat or '<unset>'}\nimage: {op[2]}\nchars: {len(op[1])}"
                    f" (cap {_RICH_CAP})\n{op[1]}\n"
                )
            elif op[0] == "photo":
                print(
                    f"[dry-run] {date_key} op {i}/{len(ops)} PHOTO {op[1]} -> "
                    f"{chat or '<unset>'}\ncaption: {op[2]}\n"
                )
            else:
                print(
                    f"[dry-run] {date_key} op {i}/{len(ops)} TEXT -> "
                    f"{chat or '<unset>'}\n{op[1]}\n"
                )
        ledger.revert_to_approved(date_key)
        return {"status": "dry-run", "date_key": date_key, "messages": len(messages)}

    sent = 0

    def _dispatch(plan: list[tuple]) -> None:
        nonlocal sent
        for op in plan:
            if op[0] == "rich":
                transport.send_rich_message(chat, op[1], op[2])
            elif op[0] == "photo":
                transport.send_photo(chat, op[1], op[2])
            else:
                transport.send_message(chat, op[1])
            sent += 1
            ledger.record_sent(date_key, sent)
            time.sleep(1.0)  # pace multi-op digests well under flood limits

    # Walk the degrade rungs (rich -> photo+text -> text-only). A rung is only
    # abandoned for the next one when its LEADING media op PROVABLY never reached
    # Telegram (PhotoNotSent while sent == 0): the digest has not gone out at all,
    # so re-planning cannot duplicate anything, and an image glitch must not lose
    # the digest. Every AMBIGUOUS failure (HTTP-after-receipt, read timeout, parse
    # error) — and any failure after something already went out — stops the chain:
    # Telegram may already hold that post, so another plan would double-post.
    failure: Exception | None = None
    for rung, plan in enumerate(plans):
        try:
            _dispatch(plan)
            failure = None
            break
        except Exception as exc:  # noqa: BLE001, PERF203
            failure = exc
            provably_nothing_sent = (
                sent == 0
                and bool(plan)
                and plan[0][0] in ("rich", "photo")
                and isinstance(exc, PhotoNotSent)
            )
            if provably_nothing_sent and rung + 1 < len(plans):
                logger.warning(
                    "aggregator publish: %s plan failed for %s (nothing sent); "
                    "degrading to the next plan: %s",
                    plan[0][0],
                    date_key,
                    exc,
                )
                continue
            # Do NOT mark_failed / revert: the row stays 'sending', which blocks
            # all future publishing until a human inspects sent_count —
            # auto-retrying here would double-post.
            logger.error("aggregator publish: send failed for %s: %s", date_key, exc)
            break
    if failure is not None:
        return {
            "status": "failed",
            "date_key": date_key,
            "error": str(failure),
            "sent": sent,
            "total": len(messages),
        }
    ledger.mark_posted(date_key)
    # Promote what actually shipped into the rolling dedup window — only now,
    # after the post is confirmed out, so a gated-but-never-published draft never
    # poisons future dedup. Legacy rows (NULL stories_json) promote nothing.
    ledger.record_published_stories(date_key)
    return {"status": "posted", "date_key": date_key, "messages": len(messages)}


def notify_operator(text: str) -> bool:
    """Operator FAILURE alert via @alex_monitoring_alert_bot — plain code, no LLM.

    Problems-only by operator ask (2026-07-15): success paths must not call this.
    Unconfigured token => silent False (failures stay visible in the journal)."""
    # Resolve *_FILE-delivered secrets here, not at call sites: the runner's
    # python heredocs call this directly and (2026-07-15 bug) got a silent False
    # because only IRONLADY_NOTIFY_BOT_TOKEN_FILE was set in the unit env.
    from .telegram_aggregator import load_file_env

    load_file_env()
    token = os.getenv("AGGREGATOR_ALERT_BOT_TOKEN", "").strip()
    chat_id = os.getenv("AGGREGATOR_OPERATOR_CHAT_ID", "").strip()
    if not token or not chat_id:
        return False
    try:
        # PLAIN TEXT, always: every caller of this function builds its message in
        # plain code (Russian sentences, scanner reports, exception strings) and
        # none of them emits HTML. Sending them as HTML is a latent 400 on the
        # first `<`, `&` or stray tag-like token — i.e. exactly on the alerts that
        # carry the most detail.
        BotApiTransport(token).send_message(chat_id, text[:OPERATOR_ALERT_CAP], parse_mode=None)
        return True
    except Exception as exc:  # noqa: BLE001 — notification must never kill the pipeline
        logger.warning("aggregator notify failed: %s", exc)
        return False
