"""Telegram user-account collection and briefing helpers for daily digests."""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

from . import config
from .telegram_proxy_client import TelegramProxyClient

logger = logging.getLogger(__name__)

# Sources tagged with this role feed the legacy topic-digest briefing. Lead
# groups the parser account JOINED are ingested under role="lead" (M2) and are
# deliberately kept OUT of the digest briefing.
DEFAULT_SOURCE_ROLE = "digest"
LEAD_SOURCE_ROLE = "lead"


@dataclass(frozen=True)
class SourceRecord:
    peer_key: str
    entity_id: int
    title: str
    username: str | None
    kind: str
    linked_channel_key: str | None
    role: str = DEFAULT_SOURCE_ROLE


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat()


def _peer_key(kind: str, entity_id: int) -> str:
    return f"{kind}:{entity_id}"


def lead_message_link(peer_key: str, message_id: int) -> str:
    """Derive a ``t.me`` deep link for a lead-group message from its ``peer_key``.

    Lead groups are private supergroups the parser account joined; they carry no
    public username, so the only stable deep link is the internal form
    ``https://t.me/c/<internal_id>/<message_id>`` where ``internal_id`` is the
    entity id with the ``-100`` marked-channel prefix stripped (Telethon stores
    the raw positive id, but we normalise a marked id too for safety). Returns
    ``''`` when the ``peer_key`` is not derivable (malformed / non-numeric id).
    """
    if not peer_key or ":" not in peer_key:
        return ""
    _, _, id_part = peer_key.rpartition(":")
    id_part = id_part.strip()
    if not id_part:
        return ""
    # Normalise to the positive internal id used in t.me/c/<id>/<msg> deep links.
    if id_part.startswith("-100"):
        internal = id_part[len("-100"):]
    else:
        internal = id_part.lstrip("-")
    if not internal.isdigit():
        return ""
    try:
        msg = int(message_id)
    except (TypeError, ValueError):
        return ""
    return f"https://t.me/c/{internal}/{msg}"


def _truncate(text: str, limit: int) -> str:
    compact = " ".join((text or "").split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


class TelegramDigestStore:
    def __init__(self, db_path: Path | None = None) -> None:
        self._db_path = db_path or config.TELEGRAM_DIGEST_DB_PATH
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self._db_path)
        con.row_factory = sqlite3.Row
        # This db is now shared by TWO processes: the M2 collect timer (writes
        # digest_messages every 30m) and the proxy (reads the lead-candidate
        # feed AND writes the lead_senders identity cache). Without these two
        # PRAGMAs a reader/writer that meets a held lock raises "database is
        # locked" immediately. busy_timeout makes it wait for the lock (up to 5s)
        # instead; WAL lets readers and a writer coexist. Both are cheap and set
        # once per connection (journal_mode=WAL is persisted in the db header, so
        # re-issuing it is a no-op).
        con.execute("PRAGMA busy_timeout=5000")
        con.execute("PRAGMA journal_mode=WAL")
        return con

    def _init_db(self) -> None:
        with self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS digest_sources (
                    peer_key TEXT PRIMARY KEY,
                    entity_id INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    username TEXT,
                    kind TEXT NOT NULL,
                    linked_channel_key TEXT,
                    role TEXT NOT NULL DEFAULT 'digest',
                    last_collected_message_id INTEGER,
                    last_collected_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            # Additive migration for dbs created before the role split. ADD COLUMN
            # with a constant DEFAULT is a metadata-only change in SQLite — it does
            # NOT rewrite the (272MB) table, and every existing row reads back as
            # role='digest', so legacy digest sources keep behaving exactly as before.
            source_cols = {
                row["name"] for row in con.execute("PRAGMA table_info(digest_sources)").fetchall()
            }
            if "role" not in source_cols:
                con.execute(
                    "ALTER TABLE digest_sources ADD COLUMN role TEXT NOT NULL DEFAULT 'digest'"
                )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_digest_sources_role ON digest_sources(role)"
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS digest_messages (
                    peer_key TEXT NOT NULL,
                    message_id INTEGER NOT NULL,
                    posted_at TEXT NOT NULL,
                    sender_id INTEGER,
                    views INTEGER,
                    forwards INTEGER,
                    replies INTEGER,
                    link TEXT,
                    text TEXT,
                    raw_json TEXT,
                    PRIMARY KEY(peer_key, message_id)
                )
                """
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_digest_messages_posted_at ON digest_messages(posted_at DESC)"
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_digest_messages_peer_time ON digest_messages(peer_key, posted_at DESC)"
            )
            # Sender-identity cache for the lead scorer. Additive CREATE TABLE IF
            # NOT EXISTS (same pattern as the join tables) — the proxy resolves a
            # flagged lead's sender via get_entity ONCE and caches it here so a
            # re-scan never re-hits Telegram. Kept in the digest db alongside the
            # lead messages it annotates.
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS lead_senders (
                    sender_id INTEGER PRIMARY KEY,
                    username TEXT,
                    name TEXT,
                    is_bot INTEGER,
                    resolved_at TEXT
                )
                """
            )
            # Additive migration: the cache now holds ANY resolved Telegram
            # entity (user/chat/channel), not just message senders, so it carries
            # a display ``title`` and its ``kind``. ADD COLUMN with no default is a
            # metadata-only change in SQLite (no table rewrite); legacy rows read
            # back with NULL title/kind and get_lead_sender falls title→name for
            # them, so old cached users keep resolving exactly as before.
            sender_cols = {
                row["name"] for row in con.execute("PRAGMA table_info(lead_senders)").fetchall()
            }
            if "title" not in sender_cols:
                con.execute("ALTER TABLE lead_senders ADD COLUMN title TEXT")
            if "kind" not in sender_cols:
                con.execute("ALTER TABLE lead_senders ADD COLUMN kind TEXT")

    def upsert_source(
        self,
        *,
        peer_key: str,
        entity_id: int,
        title: str,
        username: str | None,
        kind: str,
        linked_channel_key: str | None,
        role: str = DEFAULT_SOURCE_ROLE,
    ) -> None:
        now = _isoformat(_utc_now())
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO digest_sources(peer_key, entity_id, title, username, kind, linked_channel_key, role, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(peer_key) DO UPDATE SET
                    title = excluded.title,
                    username = excluded.username,
                    kind = excluded.kind,
                    linked_channel_key = excluded.linked_channel_key,
                    role = excluded.role,
                    updated_at = excluded.updated_at
                """,
                (peer_key, entity_id, title, username, kind, linked_channel_key, role, now, now),
            )

    def last_message_id(self, peer_key: str) -> int:
        with self._connect() as con:
            row = con.execute(
                "SELECT COALESCE(MAX(message_id), 0) AS max_message_id FROM digest_messages WHERE peer_key = ?",
                (peer_key,),
            ).fetchone()
            return int(row["max_message_id"] or 0)

    def insert_message(
        self,
        *,
        peer_key: str,
        message_id: int,
        posted_at: datetime,
        sender_id: int | None,
        views: int | None,
        forwards: int | None,
        replies: int | None,
        link: str | None,
        text: str,
        raw_json: dict[str, Any],
    ) -> bool:
        raw = json.dumps(raw_json, ensure_ascii=False)
        with self._connect() as con:
            cur = con.execute(
                """
                INSERT OR IGNORE INTO digest_messages
                (peer_key, message_id, posted_at, sender_id, views, forwards, replies, link, text, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    peer_key,
                    message_id,
                    _isoformat(posted_at),
                    sender_id,
                    views,
                    forwards,
                    replies,
                    link,
                    text,
                    raw,
                ),
            )
            return cur.rowcount == 1

    def mark_collected(self, peer_key: str, message_id: int | None) -> None:
        now = _isoformat(_utc_now())
        with self._connect() as con:
            con.execute(
                """
                UPDATE digest_sources
                SET last_collected_message_id = COALESCE(?, last_collected_message_id),
                    last_collected_at = ?,
                    updated_at = ?
                WHERE peer_key = ?
                """,
                (message_id, now, now, peer_key),
            )

    def source_count(self) -> int:
        with self._connect() as con:
            row = con.execute("SELECT COUNT(*) AS count FROM digest_sources").fetchone()
            return int(row["count"] or 0)

    # ── lead-candidate feed (a read; consumed by the scorer) ──────────
    def lead_candidates(self, *, since_id: int = 0, limit: int = 500) -> list[dict[str, Any]]:
        """Return lead-group messages with a resolvable sender, after ``since_id``.

        The stable incremental cursor is ``digest_messages.rowid`` (a plain rowid
        table — the composite PK does not suppress it). Only sources tagged
        ``role='lead'`` and messages with a non-NULL ``sender_id`` are returned,
        ordered by rowid ASC so the caller can page forward by feeding back the
        last ``id`` it saw. This method only reads; it opens no Telethon client.
        (Note the store as a whole is NOT read-only — ``upsert_lead_sender``
        writes the ``lead_senders`` cache.)
        """
        since_id = max(0, int(since_id))
        limit = max(1, min(2000, int(limit)))
        with self._connect() as con:
            rows = con.execute(
                """
                SELECT m.rowid AS id, m.peer_key AS peer_key, s.title AS chat_title,
                       m.message_id AS message_id, m.posted_at AS posted_at,
                       m.sender_id AS sender_id, m.text AS text
                FROM digest_messages m
                JOIN digest_sources s ON s.peer_key = m.peer_key
                WHERE s.role = ? AND m.sender_id IS NOT NULL AND m.rowid > ?
                ORDER BY m.rowid ASC
                LIMIT ?
                """,
                (LEAD_SOURCE_ROLE, since_id, limit),
            ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            peer_key = str(row["peer_key"])
            message_id = int(row["message_id"])
            items.append(
                {
                    "id": int(row["id"]),
                    "peer_key": peer_key,
                    "chat_title": str(row["chat_title"] or ""),
                    "message_id": message_id,
                    "posted_at": str(row["posted_at"] or ""),
                    "sender_id": int(row["sender_id"]),
                    "text": str(row["text"] or ""),
                    "link": lead_message_link(peer_key, message_id),
                }
            )
        return items

    # ── lead-sender identity cache ────────────────────────────────────
    def get_lead_sender(self, sender_id: int) -> dict[str, Any] | None:
        with self._connect() as con:
            row = con.execute(
                "SELECT sender_id, username, name, is_bot, resolved_at, title, kind "
                "FROM lead_senders WHERE sender_id = ?",
                (int(sender_id),),
            ).fetchone()
        if row is None:
            return None
        name = str(row["name"] or "")
        # Legacy rows predate the title column — fall back to the stored name so a
        # cache hit on an old user still yields a display label.
        title = row["title"]
        title = str(title) if title is not None else name
        return {
            "sender_id": int(row["sender_id"]),
            "username": row["username"],
            "name": name,
            "is_bot": bool(row["is_bot"]),
            "resolved_at": row["resolved_at"],
            "title": title,
            "kind": row["kind"],
        }

    def upsert_lead_sender(
        self,
        *,
        sender_id: int,
        username: str | None,
        name: str,
        is_bot: bool,
        title: str | None = None,
        kind: str | None = None,
    ) -> None:
        # title defaults to name so a caller that only knows a display name (or an
        # older caller that never learned about titles) still populates the column.
        title = name if title is None else title
        now = _isoformat(_utc_now())
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO lead_senders(sender_id, username, name, is_bot, resolved_at, title, kind)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(sender_id) DO UPDATE SET
                    username = excluded.username,
                    name = excluded.name,
                    is_bot = excluded.is_bot,
                    resolved_at = excluded.resolved_at,
                    title = excluded.title,
                    kind = excluded.kind
                """,
                (int(sender_id), username, name, 1 if is_bot else 0, now, title, kind),
            )

    def list_sources(self, roles: Sequence[str] | None = None) -> list[SourceRecord]:
        query = (
            "SELECT peer_key, entity_id, title, username, kind, linked_channel_key, role "
            "FROM digest_sources"
        )
        params: tuple[Any, ...] = ()
        if roles:
            placeholders = ", ".join("?" for _ in roles)
            query += f" WHERE role IN ({placeholders})"
            params = tuple(roles)
        query += " ORDER BY kind, title"
        with self._connect() as con:
            rows = con.execute(query, params).fetchall()
        return [
            SourceRecord(
                peer_key=str(row["peer_key"]),
                entity_id=int(row["entity_id"]),
                title=str(row["title"]),
                username=row["username"],
                kind=str(row["kind"]),
                linked_channel_key=row["linked_channel_key"],
                role=str(row["role"]),
            )
            for row in rows
        ]

    def recent_message_count(self, window_hours: int) -> int:
        cutoff = _isoformat(_utc_now() - timedelta(hours=window_hours))
        with self._connect() as con:
            row = con.execute(
                "SELECT COUNT(*) AS count FROM digest_messages WHERE posted_at >= ?",
                (cutoff,),
            ).fetchone()
            return int(row["count"] or 0)

    def render_briefing(
        self,
        *,
        window_hours: int,
        per_source_limit: int = 8,
        source_limit: int = 80,
    ) -> str:
        cutoff = _isoformat(_utc_now() - timedelta(hours=window_hours))
        with self._connect() as con:
            sources = con.execute(
                """
                SELECT s.peer_key, s.title, s.username, s.kind, s.linked_channel_key,
                       COUNT(m.message_id) AS message_count,
                       MAX(m.posted_at) AS latest_posted_at
                FROM digest_sources s
                JOIN digest_messages m ON m.peer_key = s.peer_key
                WHERE m.posted_at >= ? AND s.role = ?
                GROUP BY s.peer_key, s.title, s.username, s.kind, s.linked_channel_key
                ORDER BY latest_posted_at DESC
                LIMIT ?
                """,
                (cutoff, DEFAULT_SOURCE_ROLE, source_limit),
            ).fetchall()

            lines = [
                "# Telegram digest briefing",
                f"Generated at: {_isoformat(_utc_now())}",
                f"Window hours: {window_hours}",
                f"Sources with activity: {len(sources)}",
                "",
                f"Summarize this into a short Russian digest for {config.DIGEST_RECIPIENT_NAME}.",
                "Cover what happened over the last window across channel posts and linked discussion chats.",
                "Focus on important events, what people discussed, repeated signals across sources, and what changed.",
                "Include only a few source links when they are truly important.",
                "",
            ]

            for source in sources:
                kind_label = "linked_chat" if source["kind"] == "linked_chat" else "channel"
                lines.append(
                    f"## {source['title']} [{kind_label}] messages={source['message_count']}"
                )
                messages = con.execute(
                    """
                    SELECT message_id, posted_at, views, forwards, replies, link, text
                    FROM digest_messages
                    WHERE peer_key = ? AND posted_at >= ?
                    ORDER BY posted_at DESC
                    LIMIT ?
                    """,
                    (source["peer_key"], cutoff, per_source_limit),
                ).fetchall()
                for message in messages:
                    stats: list[str] = []
                    if message["views"] is not None:
                        stats.append(f"views={message['views']}")
                    if message["forwards"] is not None:
                        stats.append(f"forwards={message['forwards']}")
                    if message["replies"] is not None:
                        stats.append(f"replies={message['replies']}")
                    stat_suffix = f" ({', '.join(stats)})" if stats else ""
                    lines.append(
                        f"- {message['posted_at']}: {_truncate(message['text'] or '', 500)}{stat_suffix}"
                    )
                    if message["link"]:
                        lines.append(f"  link: {message['link']}")
                lines.append("")
            return "\n".join(lines).strip() + "\n"


def sync_joined_sources(
    store: TelegramDigestStore,
    join_db_path: Path | None = None,
) -> int:
    """Mirror the parser account's JOINED lead groups into digest_sources.

    Reads the JOIN store (written by the proxy join loop) for rows the account
    actually joined and upserts each as a role="lead" digest source keyed by
    ``linked_chat:<entity_id>`` — the same peer-kind the reader already knows how
    to read (a joined megagroup is a Channel with broadcast=False → kind
    "linked_chat"). Idempotent: re-running upserts the same rows and adds nothing.

    Returns the number of lead sources synced.
    """
    path = Path(join_db_path) if join_db_path else config.TELEGRAM_PROXY_JOIN_DB_PATH
    # The join loop owns this file; if it has not been created yet there is simply
    # nothing to sync. Never CREATE it here (a stray empty file would confuse the
    # join loop) — just no-op.
    if not path.exists():
        return 0

    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            """
            SELECT target, entity_id
            FROM joins
            WHERE status = 'joined' AND entity_id IS NOT NULL
            """
        ).fetchall()
    except sqlite3.OperationalError:
        # joins table not initialised yet — nothing to sync.
        return 0
    finally:
        con.close()

    synced = 0
    for row in rows:
        entity_id = int(row["entity_id"])
        title = str(row["target"] or f"lead:{entity_id}").strip() or f"lead:{entity_id}"
        store.upsert_source(
            peer_key=_peer_key("linked_chat", entity_id),
            entity_id=entity_id,
            title=title,
            username=None,
            kind="linked_chat",
            linked_channel_key=None,
            role=LEAD_SOURCE_ROLE,
        )
        synced += 1
    return synced


async def collect_digest(
    *,
    db_path: Path | None = None,
    brief_path: Path | None = None,
    window_hours: int | None = None,
    source_limit: int | None = None,
    collect_limit: int | None = None,
    roles: Iterable[str] | None = None,
    join_db_path: Path | None = None,
) -> dict[str, Any]:
    return await _collect_digest_via_proxy(
        db_path=db_path,
        brief_path=brief_path,
        window_hours=window_hours,
        source_limit=source_limit,
        collect_limit=collect_limit,
        roles=roles,
        join_db_path=join_db_path,
    )


async def _collect_digest_via_proxy(
    *,
    db_path: Path | None = None,
    brief_path: Path | None = None,
    window_hours: int | None = None,
    source_limit: int | None = None,
    collect_limit: int | None = None,
    roles: Iterable[str] | None = None,
    join_db_path: Path | None = None,
) -> dict[str, Any]:
    store = TelegramDigestStore(db_path)
    brief_target = brief_path or config.TELEGRAM_DIGEST_BRIEF_PATH
    window_hours = window_hours or config.TELEGRAM_DIGEST_WINDOW_HOURS
    source_limit = source_limit or config.TELEGRAM_DIGEST_SOURCE_LIMIT
    collect_limit = collect_limit or config.TELEGRAM_DIGEST_COLLECT_LIMIT
    # Default to the legacy digest pipeline so an unqualified collect is unchanged.
    role_filter = tuple(roles) if roles else (DEFAULT_SOURCE_ROLE,)

    client = TelegramProxyClient()

    collected_messages = 0
    tracked_sources = 0
    failed_sources = 0
    synced_sources = 0

    # A LEAD collect first mirrors the JOINED groups into the store, then reads
    # ONLY those — never touching the legacy digest sources (no catch-up burst).
    if LEAD_SOURCE_ROLE in role_filter:
        synced_sources = sync_joined_sources(store, join_db_path)

    known_sources = store.list_sources(roles=role_filter)
    if known_sources:
        for source in known_sources:
            tracked_sources += 1
            try:
                collected_messages += await _collect_proxy_messages_for_peer(
                    client=client,
                    store=store,
                    peer_key=source.peer_key,
                    kind=source.kind,
                    entity_id=source.entity_id,
                    collect_limit=collect_limit,
                )
            except Exception as exc:  # noqa: BLE001 — per-source isolation
                # FloodWait / proxy / network error on ONE source must not abort
                # the pass. The watermark is left untouched so this source is
                # retried next pass; the healthy sources keep collecting.
                failed_sources += 1
                logger.warning(
                    "digest collect: skipping source %s (%s) this pass: %s",
                    source.peer_key,
                    source.role,
                    exc,
                )
                continue
    elif role_filter == (DEFAULT_SOURCE_ROLE,):
        # First-run bootstrap only applies to the legacy digest pipeline: lead
        # sources come exclusively from sync_joined_sources, never list_channels.
        channels = await client.list_channels(limit=source_limit)
        for channel in channels:
            channel_key = _peer_key("channel", int(channel.entity_id))
            store.upsert_source(
                peer_key=channel_key,
                entity_id=int(channel.entity_id),
                title=(channel.title or "Unnamed channel").strip(),
                username=channel.username,
                kind="channel",
                linked_channel_key=None,
            )
            tracked_sources += 1
            try:
                collected_messages += await _collect_proxy_messages_for_peer(
                    client=client,
                    store=store,
                    peer_key=channel_key,
                    kind="channel",
                    entity_id=int(channel.entity_id),
                    collect_limit=collect_limit,
                )
            except Exception as exc:  # noqa: BLE001 — per-source isolation
                failed_sources += 1
                logger.warning(
                    "digest collect: skipping channel %s this pass: %s", channel_key, exc
                )

            if channel.linked_chat_id:
                linked_key = _peer_key("linked_chat", int(channel.linked_chat_id))
                store.upsert_source(
                    peer_key=linked_key,
                    entity_id=int(channel.linked_chat_id),
                    title=((channel.linked_chat_title or "Unnamed linked chat")).strip(),
                    username=channel.linked_chat_username,
                    kind="linked_chat",
                    linked_channel_key=channel_key,
                )
                tracked_sources += 1
                try:
                    collected_messages += await _collect_proxy_messages_for_peer(
                        client=client,
                        store=store,
                        peer_key=linked_key,
                        kind="linked_chat",
                        entity_id=int(channel.linked_chat_id),
                        collect_limit=collect_limit,
                    )
                except Exception as exc:  # noqa: BLE001 — per-source isolation
                    failed_sources += 1
                    logger.warning(
                        "digest collect: skipping linked chat %s this pass: %s",
                        linked_key,
                        exc,
                    )

    # The digest briefing belongs to the digest pipeline; a lead-only collect is
    # pure ingestion (the lead scorer reads the store later) and must not clobber it.
    if DEFAULT_SOURCE_ROLE in role_filter:
        brief = store.render_briefing(window_hours=window_hours)
        brief_target.write_text(brief)
    recent_count = store.recent_message_count(window_hours)
    return {
        "status": "ok",
        "should_alert": False,
        "change_type": "collected",
        "summary": (
            f"Collected {collected_messages} new messages across {tracked_sources} sources "
            f"(roles={','.join(role_filter)}; {failed_sources} skipped this pass). "
            f"Recent window contains {recent_count} messages."
        ),
        "payload": {
            "brief_path": str(brief_target),
            "collected_messages": collected_messages,
            "tracked_sources": tracked_sources,
            "failed_sources": failed_sources,
            "synced_sources": synced_sources,
            "roles": list(role_filter),
            "recent_messages": recent_count,
            "transport": "telegram_proxy",
        },
    }


async def _collect_proxy_messages_for_peer(
    *,
    client: TelegramProxyClient,
    store: TelegramDigestStore,
    peer_key: str,
    kind: str,
    entity_id: int,
    collect_limit: int,
) -> int:
    last_message_id = store.last_message_id(peer_key)
    latest_seen = last_message_id
    inserted_count = 0
    recent_first = last_message_id == 0
    messages = await client.read_messages(
        kind=kind,
        entity_id=entity_id,
        min_id=last_message_id,
        limit=collect_limit,
        recent_first=recent_first,
    )
    for message in messages:
        posted_at_raw = message.get("posted_at")
        posted_at = (
            datetime.fromisoformat(posted_at_raw)
            if isinstance(posted_at_raw, str) and posted_at_raw
            else _utc_now()
        )
        inserted = store.insert_message(
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
        )
        if inserted:
            inserted_count += 1
        latest_seen = max(latest_seen, int(message["message_id"]))
    store.mark_collected(peer_key, latest_seen if latest_seen > 0 else None)
    return inserted_count


def collect_digest_sync(**kwargs: Any) -> dict[str, Any]:
    return asyncio.run(collect_digest(**kwargs))
