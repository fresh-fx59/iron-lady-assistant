"""Tests for the paced, ban-safe channel/group JOIN capability.

These exercise the ban-safety contract that keeps the @giedi_0 parser account
alive while it joins ~63 chats over several days:

  * invite-hash parsing across every link shape,
  * idempotent enqueue + public/private classification,
  * the DAILY CAP (refused past the cap; a restart does NOT reset the count),
  * FloodWait persists and blocks *every* subsequent join until it elapses,
  * ChatInviteAlready → skip the import,
  * UserAlreadyParticipantError → treated as joined,
  * broadcast-channel joins chain-enqueue their linked discussion group.

The Telethon client is fully mocked — nothing here touches real Telegram.
"""

from __future__ import annotations

import inspect
import sqlite3
import types
from datetime import datetime, timedelta, timezone

import pytest

from telethon.errors import (
    ChannelsTooMuchError,
    FloodWaitError,
    InviteHashExpiredError,
    InviteRequestSentError,
    UserAlreadyParticipantError,
)

import src.telegram_proxy as tp
from src.telegram_proxy import (
    JoinStore,
    TelegramProxy,
    _utc_now,
    classify_target,
    normalize_target,
    parse_invite_hash,
    parse_public_username,
)


# ── Test doubles ───────────────────────────────────────────────────
class FakeEntity:
    def __init__(self, entity_id: int, *, broadcast: bool = False, username: str | None = None):
        self.id = entity_id
        self.broadcast = broadcast
        self.megagroup = not broadcast
        self.username = username
        self.title = username or f"entity-{entity_id}"


# Request markers — the proxy builds requests via injected factories, and the
# fake client dispatches on the marker tuple. This decouples the tests from the
# concrete Telethon TL request classes.
def _join_req(entity):
    return ("join", entity)


def _check_req(invite_hash):
    return ("check", invite_hash)


def _import_req(invite_hash):
    return ("import", invite_hash)


def _full_req(entity):
    return ("full", entity)


class FakeInviteAlready:
    """Stand-in for telethon ChatInviteAlready (has a ``.chat``)."""

    def __init__(self, chat):
        self.chat = chat


class FakeClient:
    """Minimal Telethon stand-in. Records calls; raises injected errors."""

    def __init__(self, *, dialogs=None):
        self._dialogs = list(dialogs or [])
        self.join_calls: list = []
        self.check_calls: list = []
        self.import_calls: list = []
        self.full_calls: list = []
        # target-name -> exception to raise on its join/import
        self.raise_on_join: dict = {}
        self.check_returns: dict = {}
        self.import_returns: dict = {}
        self.full_returns: dict = {}
        self.entities: dict = {}

    async def iter_dialogs(self, *, limit=None):  # noqa: ARG002
        for entity in self._dialogs:
            yield types.SimpleNamespace(entity=entity)

    async def get_entity(self, ref):
        if ref in self.entities:
            return self.entities[ref]
        # Deterministic synthetic entity keyed on the username string.
        return FakeEntity(abs(hash(ref)) % 1_000_000, username=str(ref))

    async def __call__(self, request):
        marker, payload = request
        if marker == "join":
            self.join_calls.append(payload)
            key = getattr(payload, "username", None)
            if key in self.raise_on_join:
                raise self.raise_on_join[key]
            return types.SimpleNamespace(chats=[payload])
        if marker == "check":
            self.check_calls.append(payload)
            if payload in self.raise_on_join:
                raise self.raise_on_join[payload]
            return self.check_returns.get(payload, types.SimpleNamespace(chat=None))
        if marker == "import":
            self.import_calls.append(payload)
            if payload in self.raise_on_join:
                raise self.raise_on_join[payload]
            chat = self.import_returns.get(payload, FakeEntity(777, username="imported"))
            return types.SimpleNamespace(chats=[chat])
        if marker == "full":
            self.full_calls.append(payload)
            return self.full_returns.get(
                int(payload.id),
                types.SimpleNamespace(
                    full_chat=types.SimpleNamespace(linked_chat_id=None), chats=[]
                ),
            )
        raise AssertionError(f"unexpected request marker {marker!r}")


def _make_proxy(
    store: JoinStore,
    client: FakeClient,
    *,
    cap: int = 15,
    min_delay: float = 0.0,
    max_delay: float = 0.0,
    max_attempts: int = 5,
    retry_backoff: float = 0.0,
) -> TelegramProxy:
    proxy = TelegramProxy()
    proxy._client = client
    proxy._join_store = store
    proxy._daily_cap = cap
    # Pacing defaults to 0 so cap/floodwait/etc. tests can issue joins back-to-back;
    # the pacing-specific tests pass a real min_delay to exercise the durable gate.
    proxy._join_min_delay = min_delay
    proxy._join_max_delay = max_delay
    proxy._join_max_attempts = max_attempts
    proxy._join_retry_backoff = retry_backoff
    proxy._join_channel_request = _join_req
    proxy._check_chat_invite_request = _check_req
    proxy._import_chat_invite_request = _import_req
    proxy._get_full_channel_request = _full_req
    proxy._chat_invite_already_cls = FakeInviteAlready
    return proxy


def _today() -> str:
    return _utc_now().strftime("%Y-%m-%d")


# ── 1. Hash parsing / classification ──────────────────────────────
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("t.me/+ABChash", "ABChash"),
        ("https://t.me/+ABChash", "ABChash"),
        ("http://t.me/+ABChash", "ABChash"),
        ("t.me/joinchat/XYZhash", "XYZhash"),
        ("https://t.me/joinchat/XYZhash", "XYZhash"),
        ("+ABChash", "ABChash"),
        ("ABChash", "ABChash"),
        ("https://t.me/+ABChash?foo=1", "ABChash"),
    ],
)
def test_parse_invite_hash_variants(raw, expected):
    assert parse_invite_hash(raw) == expected


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("https://t.me/foo", "public"),
        ("t.me/foo", "public"),
        ("@foo", "public"),
        ("foo", "public"),
        ("t.me/+ABC", "private"),
        ("https://t.me/+ABC", "private"),
        ("https://t.me/joinchat/ABC", "private"),
        ("+ABC", "private"),
    ],
)
def test_classify_target(raw, expected):
    assert classify_target(raw) == expected


def test_parse_public_username_variants():
    assert parse_public_username("https://t.me/Foo") == "Foo"
    assert parse_public_username("@Foo") == "Foo"
    assert parse_public_username("t.me/foo/123") == "foo"


def test_normalize_target_canonical_and_dedup():
    assert normalize_target("https://t.me/Foo") == ("public", "foo")
    assert normalize_target("@foo") == ("public", "foo")
    assert normalize_target("t.me/foo/") == ("public", "foo")
    # invite hashes are case-sensitive → preserved verbatim
    assert normalize_target("t.me/+ABChash") == ("private", "+ABChash")
    assert normalize_target("https://t.me/joinchat/ABChash") == ("private", "+ABChash")


# ── 2. Enqueue upsert idempotency + classification ────────────────
def test_enqueue_is_idempotent_and_classifies(tmp_path):
    store = JoinStore(db_path=tmp_path / "join.db")
    proxy = _make_proxy(store, FakeClient())

    r1 = proxy.enqueue_targets(["https://t.me/foo", "t.me/+HASH1", "@foo"])
    # foo and @foo collapse to one public row; +HASH1 is a second (private) row.
    assert r1["enqueued"] == 2
    assert r1["skipped"] == 1
    assert r1["total_pending"] == 2

    # Re-enqueue the same two chats in different shapes → all skipped, no dupes.
    r2 = proxy.enqueue_targets(["t.me/foo", "https://t.me/joinchat/HASH1"])
    assert r2["enqueued"] == 0
    assert r2["skipped"] == 2
    assert r2["total_pending"] == 2

    kinds = {row: k for row, k in _rows_kind(store)}
    assert kinds["foo"] == "public"
    assert kinds["+HASH1"] == "private"


def _rows_kind(store: JoinStore):
    with store._connect() as con:
        return [(r["target"], r["kind"]) for r in con.execute("SELECT target, kind FROM joins")]


# ── 3. Daily cap enforced + restart-safe ──────────────────────────
@pytest.mark.asyncio
async def test_daily_cap_enforced_and_survives_restart(tmp_path):
    db = tmp_path / "join.db"
    store = JoinStore(db_path=db)
    client = FakeClient()
    proxy = _make_proxy(store, client, cap=2)
    proxy.enqueue_targets(["@a", "@b", "@c", "@d"])

    r1 = await proxy.join_tick()
    r2 = await proxy.join_tick()
    assert r1["action"] == "joined"
    assert r2["action"] == "joined"
    assert len(client.join_calls) == 2

    # Third join in the same day is refused by the cap — no network call.
    r3 = await proxy.join_tick()
    assert r3["action"] == "skipped"
    assert r3["reason"] == "daily_cap_reached"
    assert len(client.join_calls) == 2
    assert store.joined_today(_today()) == 2

    # Simulate a process restart: brand-new store + proxy on the SAME db file.
    store2 = JoinStore(db_path=db)
    proxy2 = _make_proxy(store2, FakeClient(), cap=2)
    assert store2.joined_today(_today()) == 2  # count persisted, not reset
    r4 = await proxy2.join_tick()
    assert r4["action"] == "skipped"
    assert r4["reason"] == "daily_cap_reached"


# ── 4. FloodWait persists + blocks every subsequent join ──────────
@pytest.mark.asyncio
async def test_floodwait_persists_and_blocks_until_elapsed(tmp_path):
    db = tmp_path / "join.db"
    store = JoinStore(db_path=db)
    client = FakeClient()
    # First join hits a FloodWait; a second join WOULD succeed if attempted.
    client.raise_on_join["a"] = FloodWaitError(request=None, capture=3600)
    proxy = _make_proxy(store, client, cap=10)
    proxy.enqueue_targets(["@a", "@b"])

    r1 = await proxy.join_tick()
    assert r1["action"] == "floodwait"
    assert r1["seconds"] == 3600
    assert len(client.join_calls) == 1
    assert store.get_floodwait_until() is not None

    # Next pass must NOT issue any join while the window is open.
    r2 = await proxy.join_tick()
    assert r2["action"] == "skipped"
    assert r2["reason"] == "floodwait"
    assert len(client.join_calls) == 1  # unchanged — no early retry

    # The count did not advance during the FloodWait window.
    assert store.joined_today(_today()) == 0

    # It survives a restart, too.
    store2 = JoinStore(db_path=db)
    proxy2 = _make_proxy(store2, client, cap=10)
    r3 = await proxy2.join_tick()
    assert r3["action"] == "skipped"
    assert r3["reason"] == "floodwait"
    assert len(client.join_calls) == 1

    # Once the deadline passes, joining resumes (pending @b joins first).
    store2.set_floodwait_until(_utc_now() - timedelta(seconds=1))
    r4 = await proxy2.join_tick()
    assert r4["action"] == "joined"
    assert len(client.join_calls) == 2


# ── 5. ChatInviteAlready → skip the import ────────────────────────
@pytest.mark.asyncio
async def test_chat_invite_already_skips_import(tmp_path):
    store = JoinStore(db_path=tmp_path / "join.db")
    client = FakeClient()
    client.check_returns["HASH1"] = FakeInviteAlready(chat=FakeEntity(999, broadcast=False))
    proxy = _make_proxy(store, client, cap=10)
    proxy.enqueue_targets(["t.me/+HASH1"])

    result = await proxy.join_tick()
    assert result["action"] == "joined"
    assert result["already"] is True
    # No import request was issued, and it did NOT count toward the cap.
    assert client.import_calls == []
    assert store.joined_today(_today()) == 0
    assert store.count_by_status().get("joined") == 1


# ── 6. UserAlreadyParticipantError → joined ───────────────────────
@pytest.mark.asyncio
async def test_user_already_participant_marks_joined(tmp_path):
    store = JoinStore(db_path=tmp_path / "join.db")
    client = FakeClient()
    client.raise_on_join["a"] = UserAlreadyParticipantError(request=None)
    proxy = _make_proxy(store, client, cap=10)
    proxy.enqueue_targets(["@a"])

    result = await proxy.join_tick()
    assert result["action"] == "joined"
    assert result["already_participant"] is True
    assert store.count_by_status().get("joined") == 1
    # A join request DID reach Telegram → counts toward the daily cap.
    assert store.joined_today(_today()) == 1


# ── 7. Broadcast join chain-enqueues the linked discussion group ──
@pytest.mark.asyncio
async def test_broadcast_join_enqueues_linked_chat(tmp_path):
    store = JoinStore(db_path=tmp_path / "join.db")
    client = FakeClient()
    channel = FakeEntity(500, broadcast=True, username="chan")
    client.entities["chan"] = channel
    linked = FakeEntity(600, broadcast=False, username="chan_chat")
    client.full_returns[500] = types.SimpleNamespace(
        full_chat=types.SimpleNamespace(linked_chat_id=600),
        chats=[channel, linked],
    )
    proxy = _make_proxy(store, client, cap=10)
    proxy.enqueue_targets(["@chan"])

    result = await proxy.join_tick()
    assert result["action"] == "joined"
    assert result["linked_enqueued"] == 600

    # The linked discussion group is now a pending target keyed by id.
    with store._connect() as con:
        row = con.execute(
            "SELECT kind, status FROM joins WHERE target = ?", ("id:600",)
        ).fetchone()
    assert row is not None
    assert row["kind"] == "linked"
    assert row["status"] == "pending"

    # Joining the linked chat next resolves the cached entity and joins it.
    result2 = await proxy.join_tick()
    assert result2["action"] == "joined"
    assert result2["target"] == "id:600"
    assert linked in client.join_calls


# ── 8. Dead + request_sent + channels_too_much outcomes ───────────
@pytest.mark.asyncio
async def test_dead_invite_marked_dead(tmp_path):
    store = JoinStore(db_path=tmp_path / "join.db")
    client = FakeClient()
    client.raise_on_join["DEADHASH"] = InviteHashExpiredError(request=None)
    proxy = _make_proxy(store, client, cap=10)
    proxy.enqueue_targets(["t.me/+DEADHASH"])

    result = await proxy.join_tick()
    assert result["action"] == "dead"
    assert store.count_by_status().get("dead") == 1
    assert store.joined_today(_today()) == 0


@pytest.mark.asyncio
async def test_invite_request_sent(tmp_path):
    store = JoinStore(db_path=tmp_path / "join.db")
    client = FakeClient()
    client.raise_on_join["APPROVEHASH"] = InviteRequestSentError(request=None)
    proxy = _make_proxy(store, client, cap=10)
    proxy.enqueue_targets(["t.me/+APPROVEHASH"])

    result = await proxy.join_tick()
    assert result["action"] == "request_sent"
    assert store.count_by_status().get("request_sent") == 1


@pytest.mark.asyncio
async def test_channels_too_much_stops(tmp_path):
    store = JoinStore(db_path=tmp_path / "join.db")
    client = FakeClient()
    client.raise_on_join["a"] = ChannelsTooMuchError(request=None)
    proxy = _make_proxy(store, client, cap=10)
    proxy.enqueue_targets(["@a", "@b"])

    r1 = await proxy.join_tick()
    assert r1["action"] == "channels_too_much"
    # Every later pass is now short-circuited by the stop flag.
    r2 = await proxy.join_tick()
    assert r2["action"] == "skipped"
    assert r2["reason"] == "channels_too_much"


# ── 9. Idempotency: skip a target we are already in ───────────────
@pytest.mark.asyncio
async def test_preloaded_dialog_is_skipped_without_join(tmp_path):
    store = JoinStore(db_path=tmp_path / "join.db")
    already = FakeEntity(4242, username="already")
    client = FakeClient(dialogs=[already])
    client.entities["already"] = already
    proxy = _make_proxy(store, client, cap=10)
    proxy.enqueue_targets(["@already"])

    result = await proxy.join_tick()
    assert result["action"] == "joined"
    assert result["already"] is True
    # We were already a member → no join request, no cap consumption.
    assert client.join_calls == []
    assert store.joined_today(_today()) == 0


# ── 10. Pacing delay stays within the configured band ─────────────
def test_join_delay_within_bounds():
    proxy = TelegramProxy()
    proxy._join_min_delay = 60
    proxy._join_max_delay = 300
    for _ in range(200):
        delay = proxy._join_delay_seconds()
        assert 60 <= delay <= 300


# ── 11. The join path never combines with participant/invite calls ─
def test_join_path_never_lists_or_invites_participants():
    """Combining joins with GetParticipants/InviteToChannel/AddChatUser is an
    instant spam signal — assert those never appear in the join code path."""
    banned = ("GetParticipants", "InviteToChannel", "AddChatUser")
    sources = "\n".join(
        inspect.getsource(fn)
        for fn in (
            TelegramProxy._run_join_pass_locked,
            TelegramProxy._attempt_join,
            TelegramProxy._join_public,
            TelegramProxy._join_private,
            TelegramProxy._join_linked,
            TelegramProxy._discover_and_enqueue_linked,
            TelegramProxy._handle_join_error,
        )
    )
    for name in banned:
        assert name not in sources, f"join path must never reference {name}"


# ══ Adversarial-review fixes (PR #3) ═══════════════════════════════════
# FINDING #1 — durable inter-join pacing, FINDING #2 — rolling 24h cap,
# FINDING #3 — atomic increment, FINDING #4 — transient retry.
# These MUST fail against the pre-fix code and pass after.


# ── F1: durable inter-join pacing (survives restart) ──────────────
@pytest.mark.asyncio
async def test_pacing_gate_blocks_second_join_and_survives_restart(tmp_path):
    db = tmp_path / "join.db"
    store = JoinStore(db_path=db)
    client = FakeClient()
    proxy = _make_proxy(store, client, cap=15, min_delay=60, max_delay=60)
    proxy.enqueue_targets(["@a", "@b"])

    r1 = await proxy.join_tick()
    assert r1["action"] == "joined"
    assert len(client.join_calls) == 1

    # A second pass immediately after is blocked by the DURABLE pacing gate,
    # not by an in-memory sleep — no network join is issued.
    r2 = await proxy.join_tick()
    assert r2["action"] == "skipped"
    assert r2["reason"] == "pacing"
    assert len(client.join_calls) == 1

    # Simulate a process restart: a brand-new store + proxy on the SAME db file
    # must still honour the persisted next_join_allowed_at (jitter, not just floor).
    store2 = JoinStore(db_path=db)
    proxy2 = _make_proxy(store2, FakeClient(), cap=15, min_delay=60, max_delay=60)
    assert store2.get_next_join_allowed_at() is not None
    r3 = await proxy2.join_tick()
    assert r3["action"] == "skipped"
    assert r3["reason"] == "pacing"
    assert proxy2._client.join_calls == []

    # Once the persisted deadline passes, joining resumes.
    store2.set_next_join_allowed_at(_utc_now() - timedelta(seconds=1))
    r4 = await proxy2.join_tick()
    assert r4["action"] == "joined"
    assert len(proxy2._client.join_calls) == 1


# ── F1: /tick burst issues a single network join ──────────────────
@pytest.mark.asyncio
async def test_tick_burst_within_min_delay_issues_single_join(tmp_path):
    store = JoinStore(db_path=tmp_path / "join.db")
    client = FakeClient()
    proxy = _make_proxy(store, client, cap=15, min_delay=60, max_delay=60)
    proxy.enqueue_targets([f"@c{i}" for i in range(30)])

    results = [await proxy.join_tick() for _ in range(30)]
    joined = [r for r in results if r["action"] == "joined"]
    paced = [r for r in results if r.get("reason") == "pacing"]

    # 30 rapid ticks within the pacing window → exactly ONE real join.
    assert len(client.join_calls) == 1
    assert len(joined) == 1
    assert len(paced) == 29


# ── F1 via the HTTP /v1/join/tick endpoint (no per-call sleep) ────
@pytest.mark.asyncio
async def test_http_tick_endpoint_burst_issues_single_join(tmp_path, monkeypatch):
    from aiohttp.test_utils import TestClient, TestServer

    import src.config as cfg

    monkeypatch.setattr(cfg, "TELEGRAM_PROXY_API_KEY", "secret", raising=False)

    store = JoinStore(db_path=tmp_path / "join.db")
    client = FakeClient()
    proxy = _make_proxy(store, client, cap=15, min_delay=60, max_delay=60)
    proxy.enqueue_targets([f"@c{i}" for i in range(30)])

    app = tp.create_app()
    # Skip the real startup (connect to Telegram) and cleanup (disconnect the
    # real client); inject our fake proxy directly.
    app.on_startup.clear()
    app.on_cleanup.clear()
    app["proxy"] = proxy

    server = TestServer(app)
    http = TestClient(server)
    await http.start_server()
    try:
        headers = {"Authorization": "Bearer secret"}
        for _ in range(30):
            resp = await http.post("/v1/join/tick", headers=headers)
            assert resp.status == 200
    finally:
        await http.close()

    assert len(client.join_calls) == 1


# ── F2: rolling 24h cap (not a UTC calendar bucket) ───────────────
@pytest.mark.asyncio
async def test_rolling_window_cap_across_utc_midnight(tmp_path, monkeypatch):
    db = tmp_path / "join.db"
    store = JoinStore(db_path=db)

    # 15 real joins at 23:59 on 2026-07-14 (just before UTC midnight).
    t0 = datetime(2026, 7, 14, 23, 59, 0, tzinfo=timezone.utc)
    for i in range(15):
        store.upsert_pending(f"c{i}", "public")
        store.commit_network_join(f"c{i}", entity_id=i + 1, ts=t0)

    # 20 minutes later it is a NEW calendar day (2026-07-15) but still inside the
    # rolling trailing 24h. A calendar bucket would reset to 0 and allow 15 more.
    t1 = datetime(2026, 7, 15, 0, 20, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(tp, "_utc_now", lambda: t1)

    client = FakeClient()
    proxy = _make_proxy(store, client, cap=15)
    proxy.enqueue_targets(["@fresh"])

    result = await proxy.join_tick()
    assert result["action"] == "skipped"
    assert result["reason"] == "daily_cap_reached"
    assert client.join_calls == []  # the 16th within 24h is refused

    assert store.rolling_join_count(t1 - timedelta(hours=24)) == 15
    # Proof the calendar bucket for the NEW day was empty (would have allowed it).
    assert store.joined_today("2026-07-15") == 0


# ── F3: atomic increment (rollback leaves no partial state) ───────
class _ConnWrapper:
    """Wrap a real sqlite connection but raise on a targeted statement, to
    simulate a crash mid-transaction. Delegates transaction control so the real
    connection still ROLLS BACK on the raised exception."""

    def __init__(self, con: sqlite3.Connection, fail_substr: str):
        self._con = con
        self._fail_substr = fail_substr

    def execute(self, sql, *args, **kwargs):
        if self._fail_substr and self._fail_substr in sql:
            raise sqlite3.OperationalError("simulated crash mid-transaction")
        return self._con.execute(sql, *args, **kwargs)

    def __enter__(self):
        self._con.__enter__()
        return self  # so `con.execute(...)` inside the with-block hits us

    def __exit__(self, *exc):
        return self._con.__exit__(*exc)

    def __getattr__(self, name):
        return getattr(self._con, name)


def test_commit_network_join_is_atomic_and_durable(tmp_path):
    db = tmp_path / "join.db"
    store = JoinStore(db_path=db)
    store.upsert_pending("a", "public")

    # Simulate a crash between the network join and the status-commit: force the
    # rolling-window INSERT to fail mid-transaction.
    real_connect = JoinStore._connect.__get__(store, JoinStore)
    store._connect = lambda: _ConnWrapper(real_connect(), "join_events")
    with pytest.raises(sqlite3.OperationalError):
        store.commit_network_join("a", entity_id=1)

    # Nothing partial persisted: a fresh store (restart) sees the row still
    # pending, the daily counter untouched, and NO rolling event.
    store2 = JoinStore(db_path=db)
    assert store2.count_by_status().get("pending") == 1
    assert store2.count_by_status().get("joined") is None
    assert store2.joined_today("2026-07-14") == 0
    assert store2.rolling_join_count(_utc_now() - timedelta(hours=24)) == 0

    # A clean commit records mark + counter + rolling ts atomically and durably.
    store2.commit_network_join("a", entity_id=1)
    store3 = JoinStore(db_path=db)
    assert store3.count_by_status().get("joined") == 1
    assert store3.rolling_join_count(_utc_now() - timedelta(hours=24)) == 1


@pytest.mark.asyncio
async def test_crash_before_commit_does_not_allow_cap_plus_one(tmp_path):
    """cap real joins then a simulated restart: the rolling counter is durable so
    the (cap+1)th join is refused (no cap+1 real joins slip through a crash)."""
    db = tmp_path / "join.db"
    store = JoinStore(db_path=db)
    client = FakeClient()
    proxy = _make_proxy(store, client, cap=2)
    proxy.enqueue_targets(["@a", "@b", "@c"])

    assert (await proxy.join_tick())["action"] == "joined"
    assert (await proxy.join_tick())["action"] == "joined"

    # Restart: brand-new store/proxy on the same db must still see the count.
    store2 = JoinStore(db_path=db)
    proxy2 = _make_proxy(store2, FakeClient(), cap=2)
    r3 = await proxy2.join_tick()
    assert r3["action"] == "skipped"
    assert r3["reason"] == "daily_cap_reached"
    assert proxy2._client.join_calls == []


# ── F4: transient errors retry (bounded), then terminal ───────────
@pytest.mark.asyncio
async def test_transient_error_retries_then_terminal(tmp_path):
    store = JoinStore(db_path=tmp_path / "join.db")
    client = FakeClient()
    # A transient failure on every attempt (network blip, not a terminal invite
    # error) — must NOT be dropped permanently on the first failure.
    client.raise_on_join["a"] = ConnectionError("temporary network blip")
    proxy = _make_proxy(store, client, cap=15, max_attempts=3, retry_backoff=0)
    proxy.enqueue_targets(["@a"])

    r1 = await proxy.join_tick()
    assert r1["action"] == "retry"
    # Still re-offered by next_candidate (backoff=0 ⇒ retry_at already elapsed).
    assert store.next_candidate() is not None

    r2 = await proxy.join_tick()
    assert r2["action"] == "retry"
    assert store.next_candidate() is not None

    # Third attempt hits the attempt cap → terminal 'failed', no longer offered.
    r3 = await proxy.join_tick()
    assert r3["action"] == "failed"
    assert store.next_candidate() is None
    assert store.count_by_status().get("failed") == 1

    # All three attempts actually reached the network (target not silently dropped).
    assert len(client.join_calls) == 3

    # Terminal failures are surfaced in status so the drop is visible.
    st = proxy.join_status()
    assert any(row["target"] == "a" for row in st["failed"])


@pytest.mark.asyncio
async def test_terminal_invite_error_is_not_retried(tmp_path):
    """A terminal InviteHashExpired stays 'dead' — it must not enter the retry
    loop introduced for transient errors."""
    store = JoinStore(db_path=tmp_path / "join.db")
    client = FakeClient()
    client.raise_on_join["DEADHASH"] = InviteHashExpiredError(request=None)
    proxy = _make_proxy(store, client, cap=15, max_attempts=3, retry_backoff=0)
    proxy.enqueue_targets(["t.me/+DEADHASH"])

    r1 = await proxy.join_tick()
    assert r1["action"] == "dead"
    assert store.next_candidate() is None
    assert store.count_by_status().get("dead") == 1
    st = proxy.join_status()
    assert any(row["target"] == "+DEADHASH" for row in st["dead"])
