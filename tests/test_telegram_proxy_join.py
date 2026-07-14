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
import types
from datetime import timedelta

import pytest

from telethon.errors import (
    ChannelsTooMuchError,
    FloodWaitError,
    InviteHashExpiredError,
    InviteRequestSentError,
    UserAlreadyParticipantError,
)

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


def _make_proxy(store: JoinStore, client: FakeClient, *, cap: int = 15) -> TelegramProxy:
    proxy = TelegramProxy()
    proxy._client = client
    proxy._join_store = store
    proxy._daily_cap = cap
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
