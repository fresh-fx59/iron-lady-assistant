from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import random
import sqlite3
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from aiohttp import web

from . import config
from .telegram_digest import TelegramDigestStore
from .telegram_proxy_crypto import (
    TelegramProxyCredentials,
    decrypt_credentials,
    load_decryption_key,
)

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat()


# ── Join-target parsing ───────────────────────────────────────────
# A target can arrive in many shapes; we normalise it to a canonical primary key
# so re-enqueuing the same chat in a different shape is idempotent (one DB row).
#   public  → bare, lower-cased username        ("foo")
#   private → "+" + case-sensitive invite hash  ("+ABCdef")   (hashes are base64url)
#   linked  → "id:<entity_id>"   (discovered discussion group behind a channel)
_TME_HOSTS = ("t.me/", "telegram.me/", "telesco.pe/")


def _strip_scheme_host(raw: str) -> str:
    """Drop a leading ``https://``/``http://`` scheme and a ``t.me/`` style host.

    Case-insensitive on scheme/host only — the remaining path (which may be a
    case-sensitive invite hash) is returned untouched.
    """
    s = (raw or "").strip()
    low = s.lower()
    for scheme in ("https://", "http://"):
        if low.startswith(scheme):
            s = s[len(scheme):]
            low = s.lower()
            break
    for host in _TME_HOSTS:
        if low.startswith(host):
            s = s[len(host):]
            break
    return s.strip()


def classify_target(raw: str) -> str:
    """Return ``"private"`` for invite-link shapes, else ``"public"``."""
    s = (raw or "").strip().lower()
    if "joinchat/" in s:
        return "private"
    if _strip_scheme_host(raw).startswith("+"):
        return "private"
    return "public"


def parse_invite_hash(raw: str) -> str:
    """Extract the invite hash from any private-link shape.

    Handles ``t.me/+HASH``, ``https://t.me/+HASH``, ``t.me/joinchat/HASH``,
    ``https://t.me/joinchat/HASH``, bare ``+HASH`` and a bare hash. Strips through
    ``/joinchat/`` or ``+``, then drops any leading ``+``. Case is preserved
    (invite hashes are case-sensitive).
    """
    s = (raw or "").strip()
    if "/joinchat/" in s:
        s = s.split("/joinchat/", 1)[1]
    elif "joinchat/" in s:
        s = s.split("joinchat/", 1)[1]
    if "+" in s:
        s = s.split("+", 1)[1]
    s = s.lstrip("+").strip().strip("/")
    # Drop any trailing path/query noise, keep the first segment.
    s = s.split("/", 1)[0].split("?", 1)[0]
    return s


def parse_public_username(raw: str) -> str:
    """Extract the bare username from any public-link shape (``@foo``/``t.me/foo``)."""
    s = _strip_scheme_host(raw).lstrip("@").strip().strip("/")
    s = s.split("/", 1)[0].split("?", 1)[0]
    return s


def normalize_target(raw: str) -> tuple[str, str]:
    """Return ``(kind, canonical_target)`` — the stable primary key for the queue."""
    kind = classify_target(raw)
    if kind == "private":
        return "private", "+" + parse_invite_hash(raw)
    return "public", parse_public_username(raw).lower()


class JoinStore:
    """SQLite-backed, fully-resumable persistence for the paced join queue.

    Mirrors the ``TelegramDigestStore`` conventions (same ``memory/`` dir, same
    connect/init pattern). Holds four tables:

      * ``joins``               — one row per target, its status + outcome. Carries
        an ``attempts`` counter and ``retry_at`` backoff so a TRANSIENT failure is
        retried (bounded) rather than permanently dropped.
      * ``join_events``         — one row per REAL network-join action (Unix-epoch
        ts). The ROLLING trailing-24h count over this table is the AUTHORITATIVE
        cap; a per-calendar-day bucket allowed ~2× cap across a UTC-midnight
        rollover, so this replaces it for enforcement.
      * ``join_daily_counter``  — per-UTC-day count, kept only as a secondary
        DISPLAY metric now that the rolling window enforces the cap.
      * ``join_meta``           — small key/value store for the global
        ``floodwait_until`` gate, the durable ``next_join_allowed_at`` pacing
        deadline, and the ``channels_too_much`` stop flag.

    Ban-safety invariant: the count + mark + rolling-ts for a real join are written
    in ONE transaction (``commit_network_join``), so a crash can never keep the
    real join while dropping the count.
    """

    def __init__(self, db_path: Path | None = None) -> None:
        self._db_path = Path(db_path) if db_path else config.TELEGRAM_PROXY_JOIN_DB_PATH
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self._db_path)
        con.row_factory = sqlite3.Row
        return con

    def _init_db(self) -> None:
        with self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS joins (
                    target TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    entity_id INTEGER,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    joined_at TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    retry_at TEXT
                )
                """
            )
            # Additive migration for dbs created before the transient-retry fix.
            cols = {row["name"] for row in con.execute("PRAGMA table_info(joins)").fetchall()}
            if "attempts" not in cols:
                con.execute("ALTER TABLE joins ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0")
            if "retry_at" not in cols:
                con.execute("ALTER TABLE joins ADD COLUMN retry_at TEXT")
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_joins_status ON joins(status, created_at)"
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS join_daily_counter (
                    day TEXT PRIMARY KEY,
                    count INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            # ROLLING-window ledger: one row per REAL network-join action, stored as
            # a Unix epoch (REAL) so the trailing-24h cap is a numeric comparison
            # (no ISO string-sort pitfalls) and cannot be reset by a UTC-midnight
            # rollover the way a per-calendar-day bucket can.
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS join_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL
                )
                """
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_join_events_ts ON join_events(ts)"
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS join_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
                """
            )

    # ── queue mutations ───────────────────────────────────────────
    def upsert_pending(self, target: str, kind: str) -> bool:
        """Insert a pending row; leave an existing row untouched. Idempotent.

        Returns ``True`` iff a new row was created (so callers can tell enqueued
        from skipped). A re-enqueue never resets a joined/dead/failed row.
        """
        now = _isoformat(_utc_now())
        with self._connect() as con:
            cur = con.execute(
                """
                INSERT INTO joins(target, kind, status, created_at, updated_at)
                VALUES (?, ?, 'pending', ?, ?)
                ON CONFLICT(target) DO NOTHING
                """,
                (target, kind, now, now),
            )
            return cur.rowcount == 1

    def mark(
        self,
        target: str,
        status: str,
        *,
        entity_id: int | None = None,
        error: str | None = None,
        joined: bool = False,
    ) -> None:
        now = _isoformat(_utc_now())
        with self._connect() as con:
            con.execute(
                """
                UPDATE joins
                SET status = ?,
                    updated_at = ?,
                    entity_id = COALESCE(?, entity_id),
                    error = ?,
                    joined_at = CASE WHEN ? THEN ? ELSE joined_at END
                WHERE target = ?
                """,
                (status, now, entity_id, error, 1 if joined else 0, now, target),
            )

    def next_candidate(self) -> dict[str, Any] | None:
        """Return the next retryable target.

        Ordering: fresh ``pending`` first, then ``floodwait`` (cleared by the
        account-wide gate) and ``retry`` rows whose per-target backoff has
        elapsed. A ``retry`` row still inside its backoff window is skipped so a
        transient blip does not get hammered.
        """
        now_iso = _isoformat(_utc_now())
        with self._connect() as con:
            row = con.execute(
                """
                SELECT target, kind, status, entity_id, attempts
                FROM joins
                WHERE status IN ('pending', 'floodwait')
                   OR (status = 'retry' AND (retry_at IS NULL OR retry_at <= ?))
                ORDER BY (status = 'pending') DESC, created_at ASC
                LIMIT 1
                """,
                (now_iso,),
            ).fetchone()
        return dict(row) if row is not None else None

    def targets_by_status(self, status: str, limit: int = 50) -> list[dict[str, Any]]:
        """Rows in a given terminal/retry status — surfaced in join_status so
        dropped or retrying targets are visible, not silently lost."""
        with self._connect() as con:
            rows = con.execute(
                """
                SELECT target, kind, attempts, error, updated_at
                FROM joins WHERE status = ?
                ORDER BY updated_at DESC LIMIT ?
                """,
                (status, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def mark_transient_retry(
        self,
        target: str,
        *,
        error: str | None,
        max_attempts: int,
        backoff_seconds: float,
    ) -> tuple[str, int]:
        """Bump the attempt counter for a TRANSIENT failure and keep the target
        retryable with an (exponential) backoff until the cap is reached, then
        mark it terminally ``failed``. Returns ``(status, attempts)``.

        This is what stops a one-off network blip / RPCError from permanently
        dropping a target from the campaign.
        """
        now = _utc_now()
        with self._connect() as con:
            row = con.execute(
                "SELECT attempts FROM joins WHERE target = ?", (target,)
            ).fetchone()
            attempts = (int(row["attempts"]) if row and row["attempts"] is not None else 0) + 1
            if attempts >= max_attempts:
                status = "failed"
                retry_at = None
            else:
                status = "retry"
                delay = backoff_seconds * (2 ** (attempts - 1))
                delay = min(delay, 3600.0)  # cap the backoff at 1h
                retry_at = _isoformat(now + timedelta(seconds=delay))
            con.execute(
                """
                UPDATE joins
                SET status = ?, updated_at = ?, error = ?, attempts = ?, retry_at = ?
                WHERE target = ?
                """,
                (status, _isoformat(now), error, attempts, retry_at, target),
            )
        return status, attempts

    def pending_targets(self, limit: int = 20) -> list[str]:
        with self._connect() as con:
            rows = con.execute(
                """
                SELECT target FROM joins
                WHERE status IN ('pending', 'floodwait')
                ORDER BY (status = 'pending') DESC, created_at ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [str(row["target"]) for row in rows]

    def count_by_status(self) -> dict[str, int]:
        with self._connect() as con:
            rows = con.execute(
                "SELECT status, COUNT(*) AS n FROM joins GROUP BY status"
            ).fetchall()
        return {str(row["status"]): int(row["n"]) for row in rows}

    def pending_count(self) -> int:
        with self._connect() as con:
            row = con.execute(
                "SELECT COUNT(*) AS n FROM joins WHERE status = 'pending'"
            ).fetchone()
        return int(row["n"] or 0)

    # ── daily counter (cap enforcement, restart-safe) ─────────────
    def joined_today(self, day: str) -> int:
        with self._connect() as con:
            row = con.execute(
                "SELECT count FROM join_daily_counter WHERE day = ?",
                (day,),
            ).fetchone()
        return int(row["count"]) if row is not None else 0

    def increment_daily(self, day: str) -> int:
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO join_daily_counter(day, count) VALUES (?, 1)
                ON CONFLICT(day) DO UPDATE SET count = count + 1
                """,
                (day,),
            )
            row = con.execute(
                "SELECT count FROM join_daily_counter WHERE day = ?",
                (day,),
            ).fetchone()
        return int(row["count"]) if row is not None else 0

    # ── rolling-window cap (AUTHORITATIVE; Telegram limits are ROLLING) ──
    def rolling_join_count(self, since: datetime) -> int:
        """Count REAL network joins with ts strictly after ``since``. Call with
        ``now - 24h`` to enforce the trailing-24h cap. This cannot be reset by a
        UTC-midnight rollover the way the per-calendar-day counter can."""
        with self._connect() as con:
            row = con.execute(
                "SELECT COUNT(*) AS n FROM join_events WHERE ts > ?",
                (since.timestamp(),),
            ).fetchone()
        return int(row["n"] or 0)

    def commit_network_join(
        self,
        target: str,
        *,
        status: str = "joined",
        entity_id: int | None = None,
        error: str | None = None,
        joined: bool = True,
        ts: datetime | None = None,
    ) -> None:
        """Record a REAL network-join action ATOMICALLY in ONE transaction:
        mark the target row, append the rolling-window timestamp, and bump the
        per-day display counter. Because the count is committed in the same
        transaction as the mark, a crash can never keep the real join while
        dropping the count (which would let the account do cap+1 real joins)."""
        now = ts or _utc_now()
        now_iso = _isoformat(now)
        day = now.strftime("%Y-%m-%d")
        with self._connect() as con:
            con.execute(
                """
                UPDATE joins
                SET status = ?,
                    updated_at = ?,
                    entity_id = COALESCE(?, entity_id),
                    error = ?,
                    joined_at = CASE WHEN ? THEN ? ELSE joined_at END
                WHERE target = ?
                """,
                (status, now_iso, entity_id, error, 1 if joined else 0, now_iso, target),
            )
            con.execute("INSERT INTO join_events(ts) VALUES (?)", (now.timestamp(),))
            con.execute(
                """
                INSERT INTO join_daily_counter(day, count) VALUES (?, 1)
                ON CONFLICT(day) DO UPDATE SET count = count + 1
                """,
                (day,),
            )

    # ── meta (floodwait gate + stop flag) ─────────────────────────
    def set_meta(self, key: str, value: str | None) -> None:
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO join_meta(key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    def get_meta(self, key: str) -> str | None:
        with self._connect() as con:
            row = con.execute(
                "SELECT value FROM join_meta WHERE key = ?", (key,)
            ).fetchone()
        return None if row is None else row["value"]

    def set_floodwait_until(self, until: datetime | None) -> None:
        self.set_meta("floodwait_until", _isoformat(until) if until else None)

    def get_floodwait_until(self) -> datetime | None:
        raw = self.get_meta("floodwait_until")
        if not raw:
            return None
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            return None

    # ── durable inter-join pacing (survives restart, jitter and all) ──
    def set_next_join_allowed_at(self, until: datetime | None) -> None:
        self.set_meta("next_join_allowed_at", _isoformat(until) if until else None)

    def get_next_join_allowed_at(self) -> datetime | None:
        raw = self.get_meta("next_join_allowed_at")
        if not raw:
            return None
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            return None


@dataclass(frozen=True)
class ProxyChannelRecord:
    entity_id: int
    title: str
    username: str | None
    linked_chat_id: int | None
    linked_chat_title: str | None
    linked_chat_username: str | None


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


def _message_payload(message: Any, entity_username: str | None) -> dict[str, Any]:
    replies = None
    reply_info = getattr(message, "replies", None)
    if reply_info is not None:
        replies = getattr(reply_info, "replies", None)
    posted_at = getattr(message, "date", None)
    return {
        "message_id": int(message.id),
        "posted_at": (
            posted_at.astimezone(timezone.utc).isoformat()
            if posted_at is not None
            else None
        ),
        "sender_id": getattr(message, "sender_id", None),
        "views": getattr(message, "views", None),
        "forwards": getattr(message, "forwards", None),
        "replies": replies,
        "link": f"https://t.me/{entity_username}/{message.id}" if entity_username else None,
        "text": (getattr(message, "message", None) or "").strip(),
        "raw_json": _json_safe(message.to_dict()),
    }


# A bare id that fails EVERY resolve tier (get_entity, marked PeerChannel, and the
# joined-dialogs cache) is memoized as unresolvable for this long, so a
# persistently-unresolvable sender returns the error envelope from the memo without
# re-attempting get_entity / a dialog enumeration every run. Short TTL so a group we
# later join (or a FloodWait that clears) can still resolve on a subsequent pass.
_UNRESOLVABLE_MEMO_TTL_S = 300.0


class TelegramProxy:
    def __init__(self) -> None:
        self._client = None
        self._channel_cls = None
        self._get_full_channel_request = None
        self._create_channel_request = None
        self._invite_to_channel_request = None
        self._export_chat_invite_request = None
        self._entity_cache: dict[tuple[str, int], Any] = {}
        # Prime-once guard: the joined-dialogs enumeration (iter_dialogs(limit=None))
        # is a heavy, ban-sensitive full sweep — do it at most ONCE per process. Once
        # primed, unresolvable-id misses serve from the existing _entity_cache and
        # never trigger a fresh enumeration.
        self._dialogs_primed = False
        # Negative cache: entity_id -> monotonic expiry. Set when an id fails every
        # resolve tier, so a persistently-unresolvable sender short-circuits.
        self._unresolvable_ids: dict[int, float] = {}
        self._lock = asyncio.Lock()
        self._session_lock_fd: int | None = None
        self._allowed_channel_ids = set(config.TELEGRAM_PROXY_ALLOWED_CHANNEL_IDS)
        self._allowed_chat_ids = set(config.TELEGRAM_PROXY_ALLOWED_CHAT_IDS)
        # ── Paced join capability ────────────────────────────────
        # Telethon v1 request/type classes are loaded in _start_locked (kept None
        # until then; tests inject fakes directly).
        self._join_channel_request = None
        self._check_chat_invite_request = None
        self._import_chat_invite_request = None
        self._chat_invite_already_cls = None
        self._join_store: JoinStore | None = None
        # Digest-db handle for the lead-candidate feed + the lead-sender identity
        # cache. NOT read-only: the feed reads, but resolve_sender WRITES the
        # lead_senders cache — so the proxy is a second reader AND writer of the
        # db the M2 collect timer also writes (WAL + busy_timeout in
        # TelegramDigestStore._connect let them coexist). Lazily opened; never a
        # 2nd Telethon client.
        self._digest_store: TelegramDigestStore | None = None
        self._join_lock = asyncio.Lock()  # serialises join passes (loop vs manual tick)
        self._join_task: asyncio.Task | None = None
        # Set of dialog entity ids already joined — preloaded once per run for
        # idempotency (skip targets we are already in). None ⇒ not yet loaded.
        self._joined_dialog_ids: set[int] | None = None
        # Daily cap clamped defensively at the hard ceiling — an over-large value
        # can never take effect. Tests may override in place.
        self._daily_cap = min(
            config._HARD_MAX_JOIN_DAILY_CAP,
            max(1, int(config.TELEGRAM_JOIN_DAILY_CAP)),
        )
        self._join_min_delay = config.TELEGRAM_JOIN_MIN_DELAY_SECONDS
        self._join_max_delay = config.TELEGRAM_JOIN_MAX_DELAY_SECONDS
        self._join_idle_poll = config.TELEGRAM_JOIN_IDLE_POLL_SECONDS
        # Transient-error retry: keep a target retryable (with backoff) up to this
        # many attempts before giving up, so a network blip never silently drops it.
        self._join_max_attempts = config.TELEGRAM_JOIN_MAX_ATTEMPTS
        self._join_retry_backoff = config.TELEGRAM_JOIN_RETRY_BACKOFF_SECONDS

    def _acquire_session_lock(self) -> None:
        """Take an exclusive, non-blocking OS lock on the session lockfile.

        Guarantees a single live holder of the Telegram user session on this
        host. A second proxy connecting with the same session makes Telegram
        rotate the auth key (AUTH_KEY_DUPLICATED) and force-logout the account,
        so we refuse to connect rather than race.
        """
        lock_path = Path(config.TELEGRAM_PROXY_LOCK_PATH)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        # O_CLOEXEC so a forked child never inherits (and silently holds) the lock.
        fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR | os.O_CLOEXEC, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(fd)
            raise RuntimeError(
                "another telegram-proxy already holds the session lock; refusing "
                "to connect (would trigger AUTH_KEY_DUPLICATED)"
            ) from exc
        self._session_lock_fd = fd

    def _release_session_lock(self) -> None:
        fd = self._session_lock_fd
        if fd is None:
            return
        self._session_lock_fd = None
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    async def start(self) -> None:
        # Acquire the singleton lock BEFORE connecting so two proxies can never
        # bring up the same session concurrently.
        self._acquire_session_lock()
        try:
            await self._start_locked()
        except Exception:
            self._release_session_lock()
            raise
        # Bring up the background join loop only after the session is live. It is
        # inert until an operator enqueues targets, so it is safe to always run.
        if config.TELEGRAM_JOIN_LOOP_ENABLED and self._join_task is None:
            self._join_task = asyncio.create_task(self._join_loop())

    async def _start_locked(self) -> None:
        creds = self._load_credentials()
        try:
            from telethon import TelegramClient
            from telethon.sessions import StringSession
            from telethon.tl.functions.channels import GetFullChannelRequest
            from telethon.tl.functions.channels import CreateChannelRequest, InviteToChannelRequest
            from telethon.tl.functions.channels import JoinChannelRequest
            from telethon.tl.functions.messages import ExportChatInviteRequest
            from telethon.tl.functions.messages import (
                CheckChatInviteRequest,
                ImportChatInviteRequest,
            )
            from telethon.tl.types import Channel, ChatInviteAlready
        except Exception as exc:  # pragma: no cover - dependency failure
            raise RuntimeError(f"Telethon import failed: {exc}") from exc

        session: object
        if creds.session_string:
            session = StringSession(creds.session_string)
        else:
            session_path = creds.session_path or str(config.TELEGRAM_PROXY_SESSION_PATH)
            Path(session_path).parent.mkdir(parents=True, exist_ok=True)
            session = session_path

        self._client = TelegramClient(session, creds.api_id, creds.api_hash)
        self._channel_cls = Channel
        self._get_full_channel_request = GetFullChannelRequest
        self._create_channel_request = CreateChannelRequest
        self._invite_to_channel_request = InviteToChannelRequest
        self._export_chat_invite_request = ExportChatInviteRequest
        self._join_channel_request = JoinChannelRequest
        self._check_chat_invite_request = CheckChatInviteRequest
        self._import_chat_invite_request = ImportChatInviteRequest
        self._chat_invite_already_cls = ChatInviteAlready
        await self._client.connect()
        if not await self._client.is_user_authorized():
            raise RuntimeError("Telegram proxy user session is not authorized.")

    async def stop(self) -> None:
        try:
            task = self._join_task
            self._join_task = None
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception:  # pragma: no cover - defensive
                    logger.debug("join loop raised on shutdown", exc_info=True)
            if self._client is not None:
                await self._client.disconnect()
        finally:
            self._release_session_lock()

    def _load_credentials(self) -> TelegramProxyCredentials:
        key = load_decryption_key()
        return decrypt_credentials(config.TELEGRAM_PROXY_ENCRYPTED_CREDENTIALS, key)

    def _require_client(self):
        if self._client is None:
            raise RuntimeError("Telegram proxy client is not started.")
        return self._client

    async def list_channels(self, *, limit: int, lookup: str | None = None) -> list[ProxyChannelRecord]:
        client = self._require_client()
        lookup_value = lookup.strip().lower() if lookup else None
        async with self._lock:
            entity_by_id: dict[int, Any] = {}
            channels: list[Any] = []
            dialog_limit = None if lookup_value is None else limit
            async for dialog in client.iter_dialogs(limit=dialog_limit):
                entity = dialog.entity
                if not isinstance(entity, self._channel_cls):
                    continue
                entity_by_id[int(entity.id)] = entity
                if getattr(entity, "broadcast", False):
                    if lookup_value:
                        entity_id = str(int(entity.id))
                        username = (getattr(entity, "username", None) or "").strip().lower()
                        title = (getattr(entity, "title", None) or "").strip().lower()
                        if lookup_value not in {entity_id, username, title}:
                            continue
                    channels.append(entity)
                    if lookup_value:
                        break
                    if len(channels) >= limit:
                        break

            records: list[ProxyChannelRecord] = []
            for entity in channels:
                linked_chat_id = None
                linked_chat_title = None
                linked_chat_username = None
                try:
                    full = await client(self._get_full_channel_request(entity))
                    linked_chat_id = getattr(full.full_chat, "linked_chat_id", None)
                    if linked_chat_id:
                        linked_entity = entity_by_id.get(int(linked_chat_id))
                        if linked_entity is None:
                            linked_entity = await client.get_entity(int(linked_chat_id))
                        linked_chat_title = getattr(linked_entity, "title", None)
                        linked_chat_username = getattr(linked_entity, "username", None)
                        self._entity_cache[("linked_chat", int(linked_chat_id))] = linked_entity
                except Exception:
                    logger.debug(
                        "Could not resolve linked chat for channel=%s",
                        getattr(entity, "id", None),
                        exc_info=True,
                    )

                self._entity_cache[("channel", int(entity.id))] = entity
                records.append(
                    ProxyChannelRecord(
                        entity_id=int(entity.id),
                        title=(getattr(entity, "title", None) or "Unnamed channel").strip(),
                        username=getattr(entity, "username", None),
                        linked_chat_id=int(linked_chat_id) if linked_chat_id else None,
                        linked_chat_title=linked_chat_title.strip() if linked_chat_title else None,
                        linked_chat_username=linked_chat_username,
                    )
                )
            return records

    async def read_messages(
        self,
        *,
        kind: str,
        entity_id: int,
        min_id: int,
        limit: int,
        recent_first: bool = False,
    ) -> list[dict[str, Any]]:
        client = self._require_client()
        entity = await self._resolve_entity(kind=kind, entity_id=entity_id)
        username = getattr(entity, "username", None)
        items: list[dict[str, Any]] = []
        iter_kwargs: dict[str, Any] = {
            "limit": limit,
            "reverse": not recent_first,
        }
        if min_id > 0:
            iter_kwargs["min_id"] = min_id
        async with self._lock:
            async for message in client.iter_messages(entity, **iter_kwargs):
                payload = _message_payload(message, username)
                if payload["text"]:
                    items.append(payload)
        return items

    async def _resolve_entity(self, *, kind: str, entity_id: int):
        self._authorize_entity(kind=kind, entity_id=entity_id)
        cache_key = (kind, entity_id)
        cached = self._entity_cache.get(cache_key)
        if cached is not None:
            return cached
        client = self._require_client()
        try:
            async with self._lock:
                entity = await client.get_entity(entity_id)
        except ValueError:
            await self._prime_entity_cache_from_dialogs()
            cached = self._entity_cache.get(cache_key)
            if cached is not None:
                return cached
            raise web.HTTPNotFound(text="Entity is not available in the current Telegram dialogs.") from None
        self._entity_cache[cache_key] = entity
        return entity

    async def create_group(self, *, title: str, members: list[str]) -> dict[str, Any]:
        client = self._require_client()
        if not title.strip():
            raise web.HTTPBadRequest(text="Missing title.")
        invited: list[str] = []
        failed_invites: list[dict[str, str]] = []
        async with self._lock:
            created = await client(
                self._create_channel_request(
                    title=title.strip(),
                    about="",
                    megagroup=True,
                )
            )
            if not created.chats:
                raise RuntimeError("Telegram did not return created chat.")
            channel = created.chats[0]

            invite_link = None
            try:
                invite = await client(self._export_chat_invite_request(peer=channel))
                invite_link = getattr(invite, "link", None)
            except Exception:
                invite_link = None

            for raw_member in members:
                member = raw_member.strip()
                if not member:
                    continue
                try:
                    entity = await client.get_input_entity(member)
                    await client(self._invite_to_channel_request(channel=channel, users=[entity]))
                    invited.append(member)
                except Exception as exc:
                    failed_invites.append({"member": member, "error": str(exc)})

        return {
            "chat_id": int(channel.id),
            "title": title.strip(),
            "invite_link": invite_link,
            "invited": invited,
            "failed_invites": failed_invites,
        }

    async def _prime_entity_cache_from_dialogs(self) -> None:
        client = self._require_client()
        async with self._lock:
            # Prime-once: enumerate the joined dialogs at most once per process.
            # A repeated resolve of an unresolvable id must NOT re-sweep every
            # dialog — once primed, callers serve from the existing entity cache.
            if self._dialogs_primed:
                return
            async for dialog in client.iter_dialogs(limit=None):
                entity = dialog.entity
                entity_id = getattr(entity, "id", None)
                if entity_id is None:
                    continue
                entity_id = int(entity_id)
                if isinstance(entity, self._channel_cls):
                    cache_kind = "channel" if getattr(entity, "broadcast", False) else "linked_chat"
                    self._entity_cache[(cache_kind, entity_id)] = entity
                    if cache_kind == "linked_chat":
                        self._entity_cache[("chat", entity_id)] = entity
                else:
                    self._entity_cache[("chat", entity_id)] = entity
            self._dialogs_primed = True

    def _authorize_entity(self, *, kind: str, entity_id: int) -> None:
        if kind == "channel":
            if self._allowed_channel_ids and entity_id not in self._allowed_channel_ids:
                raise web.HTTPForbidden(text="Channel is not allowlisted.")
            return
        if kind in {"linked_chat", "chat"}:
            if self._allowed_chat_ids and entity_id not in self._allowed_chat_ids:
                raise web.HTTPForbidden(text="Chat is not allowlisted.")
            if not self._allowed_chat_ids and kind == "chat":
                raise web.HTTPForbidden(text="Direct chat access is disabled.")
            return
        raise web.HTTPBadRequest(text="Unsupported entity kind.")

    # ── Lead-candidate feed + sender resolution ───────────────────
    def _get_digest_store(self) -> TelegramDigestStore:
        if self._digest_store is None:
            self._digest_store = TelegramDigestStore()
        return self._digest_store

    def lead_candidates(self, *, since_id: int, limit: int) -> dict[str, Any]:
        """Page the lead-candidate feed from the digest db (this call only reads).

        Returns the contract envelope ``{items, max_id, count}``. ``max_id`` is
        the largest rowid returned so the caller can pass it straight back as the
        next ``since_id``; when nothing new is available it echoes ``since_id`` so
        the cursor never rewinds.
        """
        store = self._get_digest_store()
        items = store.lead_candidates(since_id=since_id, limit=limit)
        # items are ordered by rowid ASC, so the last one carries the max rowid.
        max_id = items[-1]["id"] if items else since_id
        return {"items": items, "max_id": max_id, "count": len(items)}

    @staticmethod
    def _entity_kind(entity: Any) -> str:
        """Classify a resolved Telethon entity as ``user``/``chat``/``channel``.

        Duck-typed so it works on both real Telethon types and the lightweight
        fakes tests inject:
          * a **Channel** (broadcast channel OR megagroup) carries the
            ``broadcast``/``megagroup`` flags — a megagroup is a Channel with
            ``megagroup=True``, so both map to ``channel``;
          * a basic-group **Chat** has a ``title`` but no personal name;
          * everything else is a **User**.
        """
        if hasattr(entity, "broadcast") or hasattr(entity, "megagroup"):
            return "channel"
        has_person_name = bool(getattr(entity, "first_name", None)) or bool(
            getattr(entity, "last_name", None)
        )
        if getattr(entity, "title", None) and not has_person_name:
            return "chat"
        return "user"

    @classmethod
    def _entity_identity(cls, entity: Any) -> tuple[str, str | None, str, bool]:
        """Return ``(kind, username, title, is_bot)`` for a resolved entity.

        ``title`` is the display label: a user's ``"<first> <last>"`` trimmed,
        else the chat/channel ``title``. ``username`` is ``None`` for a private
        group / a basic group with no public handle.
        """
        kind = cls._entity_kind(entity)
        username = getattr(entity, "username", None)
        is_bot = bool(getattr(entity, "bot", False))
        if kind == "user":
            first = (getattr(entity, "first_name", None) or "").strip()
            last = (getattr(entity, "last_name", None) or "").strip()
            title = " ".join(part for part in (first, last) if part)
            if not title:  # deleted account / name-less user
                title = (getattr(entity, "title", None) or "").strip()
        else:
            title = (getattr(entity, "title", None) or "").strip()
        return kind, username, title, is_bot

    @staticmethod
    def _entity_envelope(
        entity_id: int,
        *,
        kind: str | None,
        username: str | None,
        title: str,
        is_bot: bool,
        cached: bool,
        error: str | None = None,
    ) -> dict[str, Any]:
        """The uniform resolve envelope shared by hit/miss/error paths.

        Backward-compat: ``sender_id`` (== ``id``) and ``name`` (== ``title``)
        are kept so existing callers keep working; every key is always present.
        """
        return {
            "id": entity_id,
            "sender_id": entity_id,
            "kind": kind,
            "username": username,
            "title": title,
            "name": title,
            "is_bot": is_bot,
            "cached": cached,
            "error": error,
        }

    @staticmethod
    def _marked_channel_peer(entity_id: int) -> Any:
        """The ``-100``-prefixed marked (``PeerChannel``) form of a bare id.

        A bare internal id (e.g. a megagroup ``1976968455``) does not always
        resolve directly; retried as its marked channel peer it does. Returns
        ``None`` for a non-positive id (already marked/invalid).
        """
        if entity_id <= 0:
            return None
        try:
            from telethon.tl.types import PeerChannel
        except Exception:  # pragma: no cover - dependency failure
            return None
        return PeerChannel(entity_id)

    @staticmethod
    def _entity_matches_id(entity: Any, entity_id: int) -> bool:
        """True iff the resolved entity's real id equals the requested ``entity_id``.

        Guards the PeerChannel fallback: forcing a possibly-USER bare id into a
        channel peer can, on numeric id-space overlap, resolve an UNRELATED
        channel — caching its title/username as this id's identity would mis-label
        the sender. Real Telethon entities always carry ``.id``; an entity with no
        discernible id (only a test stand-in) can't be verified and is accepted.
        """
        real = getattr(entity, "id", None)
        if real is None:
            return True
        try:
            return int(real) == int(entity_id)
        except (TypeError, ValueError):
            return False

    def _is_unresolvable_memoized(self, entity_id: int) -> bool:
        """True while ``entity_id`` is within its unresolvable-memo TTL. Expired
        entries are dropped on read so the memo self-prunes."""
        expiry = self._unresolvable_ids.get(entity_id)
        if expiry is None:
            return False
        if time.monotonic() >= expiry:
            self._unresolvable_ids.pop(entity_id, None)
            return False
        return True

    def _memoize_unresolvable(self, entity_id: int) -> None:
        self._unresolvable_ids[entity_id] = time.monotonic() + _UNRESOLVABLE_MEMO_TTL_S

    async def _dialog_entity_by_id(self, entity_id: int) -> Any:
        """Look an entity up by bare id in the joined-dialogs cache.

        The joined lead groups are already in ``iter_dialogs``, so priming the
        cache is cheap and hits no per-entity Telegram lookup (no ban risk).
        """
        for (_kind, cached_id), entity in self._entity_cache.items():
            if cached_id == entity_id:
                return entity
        await self._prime_entity_cache_from_dialogs()
        for (_kind, cached_id), entity in self._entity_cache.items():
            if cached_id == entity_id:
                return entity
        return None

    async def _resolve_entity_identity(self, entity_id: int) -> Any:
        """Resolve ANY Telegram entity (user/chat/channel) on the EXISTING client.

        Cheapest tier first:
          1. ``get_entity(id)`` — users and any peer already in the session;
          2. ``get_entity(PeerChannel(id))`` — a bare chat/channel internal id
             via its ``-100``-marked form;
          3. the joined-dialogs entity cache — a group we are a member of.
        A transient failure (FloodWait/RPC) from tier 1 propagates unchanged so
        the caller can fail open; only a "not found" (``ValueError``) falls
        through to the cheaper local tiers. An id that already failed every tier
        recently short-circuits from the negative memo without any network work.
        """
        # Negative cache: a persistently-unresolvable id returns the not-found
        # error WITHOUT re-attempting get_entity or a dialog enumeration.
        if self._is_unresolvable_memoized(entity_id):
            raise LookupError(f"entity {entity_id} is not resolvable (memoized)")
        client = self._require_client()
        async with self._lock:
            try:
                return await client.get_entity(entity_id)
            except ValueError:
                peer = self._marked_channel_peer(entity_id)
                if peer is not None:
                    try:
                        resolved = await client.get_entity(peer)
                    except ValueError:
                        resolved = None
                    # Verify the coerced-channel resolution is actually THIS id — a
                    # numeric id-space overlap could otherwise resolve an unrelated
                    # channel and cache its identity for a different sender.
                    if resolved is not None and self._entity_matches_id(resolved, entity_id):
                        return resolved
                    if resolved is not None:
                        logger.warning(
                            "resolve %s: PeerChannel fallback resolved a DIFFERENT id "
                            "(%s) — discarding to avoid mis-labelling",
                            entity_id, getattr(resolved, "id", None),
                        )
        # Tier 3 primes from dialogs, which takes self._lock itself — so it must
        # run OUTSIDE the block above (asyncio.Lock is not re-entrant).
        entity = await self._dialog_entity_by_id(entity_id)
        if entity is not None:
            return entity
        # Every tier failed — memoize so repeated misses don't re-do the work.
        self._memoize_unresolvable(entity_id)
        raise LookupError(f"entity {entity_id} is not resolvable")

    async def resolve_sender(self, sender_id: int) -> dict[str, Any]:
        """Resolve ANY Telegram entity's identity, caching it in the digest db.

        Generalised from message-sender resolution: the same ``get_entity`` path
        resolves a user, a basic-group chat, or a channel/megagroup, so the lead
        scorer can resolve BOTH a lead's sender and its group through one call.

        Cache hit ⇒ no network call (``cached=true``). Cache miss ⇒ one
        ``get_entity`` on the EXISTING client (with a marked-id + dialogs-cache
        fallback), then cache. FloodWait/RPC/any lookup error ⇒ a tolerant
        envelope with an ``error`` field at HTTP 200 (the caller treats an
        unresolved entity as acceptable); the failure is NOT cached so a later
        retry can still succeed.
        """
        sender_id = int(sender_id)
        store = self._get_digest_store()
        cached = store.get_lead_sender(sender_id)
        if cached is not None:
            return self._entity_envelope(
                sender_id,
                kind=cached["kind"],
                username=cached["username"],
                title=cached["title"],
                is_bot=bool(cached["is_bot"]),
                cached=True,
            )
        try:
            entity = await self._resolve_entity_identity(sender_id)
        except Exception as exc:  # noqa: BLE001 — fail open; caller tolerates
            return self._entity_envelope(
                sender_id,
                kind=None,
                username=None,
                title="",
                is_bot=False,
                cached=False,
                error=type(exc).__name__,
            )
        kind, username, title, is_bot = self._entity_identity(entity)
        store.upsert_lead_sender(
            sender_id=sender_id,
            username=username,
            name=title,
            is_bot=is_bot,
            title=title,
            kind=kind,
        )
        return self._entity_envelope(
            sender_id,
            kind=kind,
            username=username,
            title=title,
            is_bot=is_bot,
            cached=False,
        )

    # ── Paced, ban-safe JOIN ──────────────────────────────────────
    def _get_join_store(self) -> JoinStore:
        if self._join_store is None:
            self._join_store = JoinStore()
        return self._join_store

    def _join_delay_seconds(self) -> float:
        """Randomized human-like delay BETWEEN joins (ban-safety pacing)."""
        return random.uniform(self._join_min_delay, self._join_max_delay)

    def enqueue_targets(self, targets: list[str]) -> dict[str, Any]:
        """Upsert pending rows (idempotent). Classifies public vs private by shape."""
        store = self._get_join_store()
        enqueued = 0
        skipped = 0
        seen: set[str] = set()
        for raw in targets:
            if raw is None:
                continue
            text = str(raw).strip()
            if not text:
                continue
            kind, canonical = normalize_target(text)
            if not canonical or canonical in {"+"}:
                skipped += 1
                continue
            if canonical in seen:
                # Duplicate within the same request payload — count once.
                skipped += 1
                continue
            seen.add(canonical)
            if store.upsert_pending(canonical, kind):
                enqueued += 1
            else:
                skipped += 1
        return {
            "enqueued": enqueued,
            "skipped": skipped,
            "total_pending": store.pending_count(),
        }

    def join_status(self) -> dict[str, Any]:
        store = self._get_join_store()
        now = _utc_now()
        day = now.strftime("%Y-%m-%d")
        floodwait_until = store.get_floodwait_until()
        next_allowed = store.get_next_join_allowed_at()
        return {
            "counts_by_status": store.count_by_status(),
            # Rolling trailing-24h count is what the cap is enforced on.
            "joined_last_24h": store.rolling_join_count(now - timedelta(hours=24)),
            # Per-calendar-day count kept ONLY as a secondary display metric.
            "joined_today": store.joined_today(day),
            "daily_cap": self._daily_cap,
            "floodwait_until": _isoformat(floodwait_until),
            "next_join_allowed_at": _isoformat(next_allowed),
            "channels_too_much": store.get_meta("channels_too_much") == "1",
            "next_pending": store.pending_targets(limit=20),
            # Surface non-success outcomes so dropped/retrying targets are visible.
            "retry": store.targets_by_status("retry", limit=50),
            "failed": store.targets_by_status("failed", limit=50),
            "dead": store.targets_by_status("dead", limit=50),
        }

    async def join_tick(self) -> dict[str, Any]:
        """Run ONE processing pass now (respecting cap + FloodWait). Testable step."""
        return await self._run_join_pass()

    async def _run_join_pass(self) -> dict[str, Any]:
        # Serialise passes so a manual /tick can never race the background loop
        # into issuing two joins back-to-back (which would defeat the pacing).
        async with self._join_lock:
            return await self._run_join_pass_locked()

    async def _run_join_pass_locked(self) -> dict[str, Any]:
        store = self._get_join_store()
        now = _utc_now()

        # ── GATE 0: DURABLE inter-join pacing. The randomized spacing between
        # joins is PERSISTED (next_join_allowed_at, jitter and all), so neither a
        # process restart running a pass immediately on resume NOR a burst of
        # rapid /v1/join/tick calls can fire joins back-to-back. Checked first,
        # before any network work, so both the loop and /tick honour it.
        next_allowed = store.get_next_join_allowed_at()
        if next_allowed is not None and now < next_allowed:
            return {
                "action": "skipped",
                "reason": "pacing",
                "network": False,
                "next_join_allowed_at": _isoformat(next_allowed),
                "wait_s": max(0.0, (next_allowed - now).total_seconds()),
            }

        # ── GATE 1: FloodWait. Re-checked before EVERY join; we NEVER retry
        # before the window elapses (an early retry escalates to PeerFloodError =
        # multi-day / permanent ban).
        floodwait_until = store.get_floodwait_until()
        if floodwait_until is not None and now < floodwait_until:
            return {
                "action": "skipped",
                "reason": "floodwait",
                "network": False,
                "floodwait_until": _isoformat(floodwait_until),
            }

        # ── GATE 2: account hit the ~500-dialog ceiling; stop for good.
        if store.get_meta("channels_too_much") == "1":
            return {"action": "skipped", "reason": "channels_too_much", "network": False}

        # ── GATE 3: ROLLING DAILY CAP. Telegram limits are ROLLING, not calendar
        # — count real network joins in the trailing 24h and refuse at the cap.
        # This is PERSISTED, so a restart cannot reset it, and a window straddling
        # UTC midnight cannot do 2× cap (which a per-calendar-day bucket allowed).
        joined_window = store.rolling_join_count(now - timedelta(hours=24))
        if joined_window >= self._daily_cap:
            return {
                "action": "skipped",
                "reason": "daily_cap_reached",
                "network": False,
                "joined_last_24h": joined_window,
                "daily_cap": self._daily_cap,
            }

        # Idempotency: preload the ids of dialogs we are already in (once per run)
        # so we skip targets already joined without issuing a join request.
        await self._ensure_dialog_preload()

        candidate = store.next_candidate()
        if candidate is None:
            return {"action": "idle", "reason": "no_pending", "network": False}

        result = await self._attempt_join(candidate)
        # A network request actually reached Telegram → arm the DURABLE pacing
        # deadline (persisted jitter) so the next pass — loop or /tick, even after
        # a restart — waits out the full random gap before the next join.
        if result.get("network"):
            store.set_next_join_allowed_at(
                _utc_now() + timedelta(seconds=self._join_delay_seconds())
            )
        return result

    async def _ensure_dialog_preload(self, *, force: bool = False) -> None:
        if self._joined_dialog_ids is not None and not force:
            return
        ids: set[int] = set()
        client = self._require_client()
        async with self._lock:
            async for dialog in client.iter_dialogs(limit=None):
                entity = getattr(dialog, "entity", None)
                entity_id = getattr(entity, "id", None)
                if entity_id is not None:
                    ids.add(int(entity_id))
        self._joined_dialog_ids = ids

    async def _client_call(self, request: Any) -> Any:
        client = self._require_client()
        async with self._lock:
            return await client(request)

    async def _get_entity_for_join(self, ref: Any) -> Any:
        client = self._require_client()
        async with self._lock:
            return await client.get_entity(ref)

    def _is_already_joined(self, entity: Any) -> bool:
        entity_id = getattr(entity, "id", None)
        if entity_id is None or self._joined_dialog_ids is None:
            return False
        return int(entity_id) in self._joined_dialog_ids

    def _remember_joined(self, entity_id: int | None) -> None:
        if entity_id is None:
            return
        if self._joined_dialog_ids is None:
            self._joined_dialog_ids = set()
        self._joined_dialog_ids.add(int(entity_id))

    async def _attempt_join(self, candidate: dict[str, Any]) -> dict[str, Any]:
        target = str(candidate["target"])
        kind = str(candidate["kind"])
        store = self._get_join_store()
        try:
            if kind == "public":
                return await self._join_public(target)
            if kind == "private":
                return await self._join_private(target)
            if kind == "linked":
                return await self._join_linked(target)
            # Unknown kind is a TERMINAL programming/data error, not transient.
            store.mark(target, "failed", error=f"unknown kind {kind}")
            return {"action": "failed", "target": target, "network": False, "error": "unknown kind"}
        except Exception as exc:  # noqa: BLE001 - dispatched by type below
            return self._handle_join_error(target, exc)

    async def _join_public(self, target: str) -> dict[str, Any]:
        store = self._get_join_store()
        name = parse_public_username(target)
        entity = await self._get_entity_for_join(name)
        entity_id = int(getattr(entity, "id", 0)) or None
        if self._is_already_joined(entity):
            store.mark(target, "joined", entity_id=entity_id, joined=True)
            return {"action": "joined", "target": target, "entity_id": entity_id,
                    "already": True, "network": False}
        await self._client_call(self._join_channel_request(entity))
        # Count + mark + rolling-ts recorded ATOMICALLY (crash cannot drop the count).
        store.commit_network_join(target, entity_id=entity_id)
        self._remember_joined(entity_id)
        linked = await self._discover_and_enqueue_linked(entity)
        return {"action": "joined", "target": target, "entity_id": entity_id,
                "network": True, "linked_enqueued": linked}

    async def _join_private(self, target: str) -> dict[str, Any]:
        store = self._get_join_store()
        invite_hash = parse_invite_hash(target)
        info = await self._client_call(self._check_chat_invite_request(invite_hash))
        # Already a member — the invite check resolves the chat; skip the import
        # entirely (no join request issued, so it does NOT count toward the cap).
        if self._chat_invite_already_cls is not None and isinstance(
            info, self._chat_invite_already_cls
        ):
            chat = getattr(info, "chat", None)
            entity_id = int(getattr(chat, "id", 0)) or None
            store.mark(target, "joined", entity_id=entity_id, joined=True)
            self._remember_joined(entity_id)
            return {"action": "joined", "target": target, "entity_id": entity_id,
                    "already": True, "network": False}
        result = await self._client_call(self._import_chat_invite_request(invite_hash))
        chat = self._first_chat(result)
        entity_id = int(getattr(chat, "id", 0)) or None
        store.commit_network_join(target, entity_id=entity_id)
        self._remember_joined(entity_id)
        linked = None
        if chat is not None:
            linked = await self._discover_and_enqueue_linked(chat)
        return {"action": "joined", "target": target, "entity_id": entity_id,
                "network": True, "linked_enqueued": linked}

    async def _join_linked(self, target: str) -> dict[str, Any]:
        store = self._get_join_store()
        linked_id = int(target.split(":", 1)[1])
        entity = self._entity_cache.get(("linked_join", linked_id))
        if entity is None:
            entity = await self._get_entity_for_join(linked_id)
        entity_id = int(getattr(entity, "id", 0)) or linked_id
        if self._is_already_joined(entity):
            store.mark(target, "joined", entity_id=entity_id, joined=True)
            return {"action": "joined", "target": target, "entity_id": entity_id,
                    "already": True, "network": False}
        await self._client_call(self._join_channel_request(entity))
        store.commit_network_join(target, entity_id=entity_id)
        self._remember_joined(entity_id)
        return {"action": "joined", "target": target, "entity_id": entity_id, "network": True}

    @staticmethod
    def _first_chat(result: Any) -> Any:
        chats = getattr(result, "chats", None) or []
        return chats[0] if chats else None

    async def _discover_and_enqueue_linked(self, entity: Any) -> int | None:
        """After joining a BROADCAST channel, enqueue its linked discussion group.

        The real leads live in the discussion chat, so we chain-enqueue it as a
        fresh pending target (it will be joined later, paced like any other).
        """
        if not getattr(entity, "broadcast", False):
            return None
        try:
            full = await self._client_call(self._get_full_channel_request(entity))
            linked_id = getattr(full.full_chat, "linked_chat_id", None)
        except Exception:
            logger.debug("linked-chat discovery failed", exc_info=True)
            return None
        if not linked_id:
            return None
        linked_id = int(linked_id)
        for chat in getattr(full, "chats", None) or []:
            if int(getattr(chat, "id", 0)) == linked_id:
                self._entity_cache[("linked_join", linked_id)] = chat
                break
        self._get_join_store().upsert_pending(f"id:{linked_id}", "linked")
        return linked_id

    def _handle_join_error(self, target: str, exc: Exception) -> dict[str, Any]:
        from telethon.errors import (
            ChannelsTooMuchError,
            FloodWaitError,
            InviteHashExpiredError,
            InviteHashInvalidError,
            InviteRequestSentError,
            UserAlreadyParticipantError,
        )

        store = self._get_join_store()
        # Already a participant → treat as success. A join request DID reach
        # Telegram, so it counts toward the rolling cap (recorded atomically).
        if isinstance(exc, UserAlreadyParticipantError):
            store.commit_network_join(target, status="joined", joined=True)
            return {"action": "joined", "target": target,
                    "already_participant": True, "network": True}
        # FloodWait → persist the deadline and STOP. Never retry before it passes.
        if isinstance(exc, FloodWaitError):
            seconds = int(getattr(exc, "seconds", 0) or 0)
            until = _utc_now() + timedelta(seconds=seconds)
            store.set_floodwait_until(until)
            store.mark(target, "floodwait", error=f"floodwait {seconds}s")
            return {"action": "floodwait", "target": target, "seconds": seconds,
                    "floodwait_until": _isoformat(until), "network": False}
        # Dialog ceiling hit — mark and stop the whole campaign.
        if isinstance(exc, ChannelsTooMuchError):
            store.set_meta("channels_too_much", "1")
            store.mark(target, "toomuch", error="channels too much")
            return {"action": "channels_too_much", "target": target, "network": False}
        # Admin-approval invite links: request sent, awaiting approval. The
        # request reached Telegram, so it counts (recorded atomically).
        if isinstance(exc, InviteRequestSentError):
            store.commit_network_join(
                target, status="request_sent", joined=False,
                error="join request sent (awaiting approval)",
            )
            return {"action": "request_sent", "target": target, "network": True}
        # ── TERMINAL: dead invite links. Never retried.
        if isinstance(exc, (InviteHashExpiredError, InviteHashInvalidError)):
            store.mark(target, "dead", error=str(exc))
            return {"action": "dead", "target": target, "network": False, "error": str(exc)}
        # ── TRANSIENT: network blip / one-off RPCError / temporary resolve
        # failure. Keep the target RETRYABLE with a backoff + attempt counter so a
        # transient error never silently drops it from the 63-chat campaign; it
        # terminalizes to 'failed' only once the attempt cap is reached. The
        # network request likely reached Telegram, so mark it as a network action
        # → the pacing gate spaces the next attempt.
        status, attempts = store.mark_transient_retry(
            target,
            error=str(exc),
            max_attempts=self._join_max_attempts,
            backoff_seconds=self._join_retry_backoff,
        )
        action = "retry" if status == "retry" else "failed"
        return {"action": action, "target": target, "network": True,
                "error": str(exc), "attempts": attempts}

    async def _join_loop(self) -> None:
        """Background driver: run one pass, then sleep until the next action is
        due. Pacing between real joins is DURABLE (persisted next_join_allowed_at),
        so the loop sleeps until that deadline — bounded to a 300s poll so a
        cleared FloodWait / pacing window resumes promptly — instead of running a
        pass immediately on resume (which would burst joins across restarts)."""
        logger.info("telegram-proxy join loop started")
        while True:
            try:
                result = await self._run_join_pass()
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - defensive
                logger.exception("join pass failed")
                result = {"action": "error", "network": False}
            delay = self._next_loop_delay(result)
            await asyncio.sleep(max(1.0, delay))

    def _next_loop_delay(self, result: dict[str, Any]) -> float:
        """How long the background loop sleeps after a pass. Honors the DURABLE
        pacing deadline (bounded poll) so spacing is identical across restarts."""
        store = self._get_join_store()
        next_allowed = store.get_next_join_allowed_at()
        now = _utc_now()
        if next_allowed is not None and next_allowed > now:
            return min((next_allowed - now).total_seconds(), 300.0)
        if result.get("network"):
            # Defensive: a network action with no persisted deadline (should not
            # happen) still spaces the next attempt.
            return self._join_delay_seconds()
        return self._join_idle_poll


def _check_auth(request: web.Request) -> None:
    expected = config.TELEGRAM_PROXY_API_KEY
    if not expected:
        raise web.HTTPInternalServerError(text="TELEGRAM_PROXY_API_KEY is not configured.")
    provided = request.headers.get("Authorization", "").strip()
    if provided != f"Bearer {expected}":
        raise web.HTTPUnauthorized(text="Invalid proxy token.")


async def _health(request: web.Request) -> web.Response:
    proxy: TelegramProxy = request.app["proxy"]
    status = "ok" if proxy._client is not None else "starting"
    return web.json_response({"status": status})


async def _list_channels(request: web.Request) -> web.Response:
    _check_auth(request)
    proxy: TelegramProxy = request.app["proxy"]
    limit = max(1, min(500, int(request.query.get("limit", "200"))))
    lookup = request.query.get("lookup", "").strip() or None
    records = await proxy.list_channels(limit=limit, lookup=lookup)
    return web.json_response({"channels": [asdict(record) for record in records]})


async def _read_messages(request: web.Request) -> web.Response:
    _check_auth(request)
    proxy: TelegramProxy = request.app["proxy"]
    kind = request.match_info["kind"]
    entity_id = int(request.match_info["entity_id"])
    min_id = max(0, int(request.query.get("min_id", "0")))
    limit = max(1, min(500, int(request.query.get("limit", "200"))))
    recent_first = request.query.get("recent_first", "0").strip().lower() in {"1", "true", "yes", "on"}
    messages = await proxy.read_messages(
        kind=kind,
        entity_id=entity_id,
        min_id=min_id,
        limit=limit,
        recent_first=recent_first,
    )
    return web.json_response({"messages": messages})


async def _create_group(request: web.Request) -> web.Response:
    _check_auth(request)
    proxy: TelegramProxy = request.app["proxy"]
    try:
        payload = await request.json()
    except Exception:
        raise web.HTTPBadRequest(text="Invalid JSON body.") from None

    title = str(payload.get("title", "")).strip()
    members_raw = payload.get("members", [])
    if isinstance(members_raw, str):
        members = [item.strip() for item in members_raw.split(",") if item.strip()]
    elif isinstance(members_raw, list):
        members = [str(item).strip() for item in members_raw if str(item).strip()]
    else:
        members = []

    try:
        result = await proxy.create_group(title=title, members=members)
    except web.HTTPException:
        raise
    except Exception as exc:
        raise web.HTTPBadGateway(text=f"Create group failed: {exc}") from exc
    return web.json_response({"ok": True, "result": result})


async def _leads_candidates(request: web.Request) -> web.Response:
    _check_auth(request)
    proxy: TelegramProxy = request.app["proxy"]
    try:
        since_id = max(0, int(request.query.get("since_id", "0")))
    except ValueError:
        raise web.HTTPBadRequest(text="since_id must be an integer.") from None
    try:
        limit = max(1, min(2000, int(request.query.get("limit", "500"))))
    except ValueError:
        raise web.HTTPBadRequest(text="limit must be an integer.") from None
    return web.json_response(proxy.lead_candidates(since_id=since_id, limit=limit))


async def _lead_user(request: web.Request) -> web.Response:
    _check_auth(request)
    proxy: TelegramProxy = request.app["proxy"]
    try:
        sender_id = int(request.match_info["sender_id"])
    except (TypeError, ValueError):
        raise web.HTTPBadRequest(text="sender_id must be an integer.") from None
    return web.json_response(await proxy.resolve_sender(sender_id))


async def _join_enqueue(request: web.Request) -> web.Response:
    _check_auth(request)
    proxy: TelegramProxy = request.app["proxy"]
    try:
        payload = await request.json()
    except Exception:
        raise web.HTTPBadRequest(text="Invalid JSON body.") from None

    targets_raw = payload.get("targets", [])
    if isinstance(targets_raw, str):
        targets = [item.strip() for item in targets_raw.split(",") if item.strip()]
    elif isinstance(targets_raw, list):
        targets = [str(item).strip() for item in targets_raw if str(item).strip()]
    else:
        raise web.HTTPBadRequest(text="`targets` must be a list or comma string.")

    result = proxy.enqueue_targets(targets)
    return web.json_response({"ok": True, **result})


async def _join_status(request: web.Request) -> web.Response:
    _check_auth(request)
    proxy: TelegramProxy = request.app["proxy"]
    return web.json_response(proxy.join_status())


async def _join_tick(request: web.Request) -> web.Response:
    _check_auth(request)
    proxy: TelegramProxy = request.app["proxy"]
    try:
        result = await proxy.join_tick()
    except web.HTTPException:
        raise
    except Exception as exc:
        raise web.HTTPBadGateway(text=f"Join tick failed: {exc}") from exc
    return web.json_response({"ok": True, "result": result})


async def _startup(app: web.Application) -> None:
    proxy = TelegramProxy()
    await proxy.start()
    app["proxy"] = proxy


async def _cleanup(app: web.Application) -> None:
    proxy: TelegramProxy = app["proxy"]
    await proxy.stop()


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/health", _health)
    app.router.add_get("/v1/channels", _list_channels)
    app.router.add_get("/v1/messages/{kind}/{entity_id}", _read_messages)
    app.router.add_get("/v1/leads/candidates", _leads_candidates)
    app.router.add_get("/v1/users/{sender_id}", _lead_user)
    app.router.add_post("/v1/telegram/createGroup", _create_group)
    app.router.add_post("/v1/join/enqueue", _join_enqueue)
    app.router.add_get("/v1/join/status", _join_status)
    app.router.add_post("/v1/join/tick", _join_tick)
    app.on_startup.append(_startup)
    app.on_cleanup.append(_cleanup)
    return app


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    web.run_app(
        create_app(),
        host=config.TELEGRAM_PROXY_BIND_HOST,
        port=config.TELEGRAM_PROXY_BIND_PORT,
    )


if __name__ == "__main__":
    main()
