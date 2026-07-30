"""tests/test_aggregator_chat_sources.py — the discussion-chat lane (input gate).

Covers the five load-bearing properties of the chat lane:
  1. the allowlist is explicit and accepts BOTH a public handle and a bare id;
  2. a per-chat, per-run hard cap bounds what one busy chat can contribute;
  3. chat echoes of a channel post are deduped away (text key + forward-of-parent);
  4. admission is a deterministic code gate (no LLM in the ingest path);
  5. default OFF + a dry-run that writes nothing.
"""
from __future__ import annotations

from datetime import datetime

from src.telegram_aggregator import (
    AGG_ROLE,
    CHAT_KIND,
    CHAT_MAX_PER_RUN,
    CHAT_MIN_CHARS,
    ChatSource,
    build_draft_input,
    chat_admission_verdict,
    collect_chats,
    parse_chat_sources,
    resolve_chat_sources,
    resolve_paths,
)
from src.telegram_digest import TelegramDigestStore

LONG = "Вышла новая модель, вот подробный разбор что изменилось в бенчмарках и цене. " * 4
assert len(LONG) >= CHAT_MIN_CHARS


# ── fakes ─────────────────────────────────────────────────────────
class FakeChannel:
    def __init__(
        self,
        entity_id,
        username,
        title="T",
        linked_chat_id=None,
        linked_chat_title=None,
        linked_chat_username=None,
    ):
        self.entity_id = entity_id
        self.username = username
        self.title = title
        self.linked_chat_id = linked_chat_id
        self.linked_chat_title = linked_chat_title
        self.linked_chat_username = linked_chat_username


class FakeProxyClient:
    def __init__(self, channels, messages_by_entity):
        self._channels = channels
        self._messages = messages_by_entity
        self.read_calls = []

    async def list_channels(self, *, limit):  # noqa: ARG002
        return self._channels

    async def read_messages(self, *, kind, entity_id, min_id, limit, recent_first=False):
        self.read_calls.append(
            {"kind": kind, "entity_id": entity_id, "min_id": min_id, "limit": limit,
             "recent_first": recent_first}
        )
        payload = self._messages.get(entity_id, [])
        if isinstance(payload, Exception):
            raise payload
        newest_first = sorted(payload, key=lambda m: m["message_id"], reverse=True)
        return [m for m in newest_first if m["message_id"] > min_id][:limit]


def _cmsg(
    mid,
    text=LONG,
    *,
    url=True,
    fwd_channel=None,
    fwd_hidden=False,
    reply_to=None,
    reply_top=None,
    via_bot=None,
    action=None,
    posted="2026-07-30T10:00:00+00:00",
    link=None,
):
    body = text + (" https://example.com/post" if url else "")
    raw: dict = {"_": "Message", "id": mid}
    if fwd_channel is not None:
        raw["fwd_from"] = {
            "_": "MessageFwdHeader",
            "from_id": {"_": "PeerChannel", "channel_id": fwd_channel},
            "channel_post": 7,
        }
    elif fwd_hidden:
        raw["fwd_from"] = {"_": "MessageFwdHeader", "from_id": None, "from_name": "someone"}
    if reply_to is not None:
        raw["reply_to"] = {
            "_": "MessageReplyHeader",
            "reply_to_msg_id": reply_to,
            "reply_to_top_id": reply_top,
        }
    if via_bot is not None:
        raw["via_bot_id"] = via_bot
    if action is not None:
        raw["action"] = action
    return {
        "message_id": mid,
        "posted_at": posted,
        "sender_id": 500 + mid,
        "views": None,
        "forwards": None,
        "replies": None,
        "link": link if link is not None else f"https://t.me/some_chat/{mid}",
        "text": body,
        "raw_json": raw,
    }


# ── 1. the allowlist ──────────────────────────────────────────────
def test_parse_chat_sources_accepts_handles_and_ids():
    text = """
    # linked discussion chats — one per line
    @hyper_llm
    https://t.me/data_secrets_chat
    llm_driven_products
    hyper_llm                      # dedup, case-insensitive
    -1001788662720                 # marked id (what a client shows)
    1645114813                     # bare internal id
    id:2402061247
    https://t.me/c/2420935174/12   # internal deep link
    https://t.me/+privateHashAAA   # invite links are NOT resolvable -> skipped
    """
    assert parse_chat_sources(text) == [
        ChatSource("username", "hyper_llm"),
        ChatSource("username", "data_secrets_chat"),
        ChatSource("username", "llm_driven_products"),
        ChatSource("id", "1788662720"),
        ChatSource("id", "1645114813"),
        ChatSource("id", "2402061247"),
        ChatSource("id", "2420935174"),
    ]


def test_parse_chat_sources_empty_when_only_comments():
    assert parse_chat_sources("# nothing here\n\n") == []


def test_resolve_paths_exposes_chat_sources_path(monkeypatch, tmp_path):
    monkeypatch.setenv("AGGREGATOR_STATE_DIR", str(tmp_path))
    assert resolve_paths().chat_sources_path == tmp_path / "chat_sources.txt"
    monkeypatch.setenv("AGGREGATOR_CHAT_SOURCES_PATH", str(tmp_path / "x" / "chats.txt"))
    assert resolve_paths().chat_sources_path == tmp_path / "x" / "chats.txt"


def test_resolve_chat_sources_maps_handle_and_id_to_linked_chats():
    channels = [
        FakeChannel(
            10, "hyperllm", "HyperLLM",
            linked_chat_id=3850100303, linked_chat_title="HyperAI",
            linked_chat_username="hyper_llm",
        ),
        FakeChannel(
            11, None, "AI и грабли",
            linked_chat_id=1788662720, linked_chat_title="AI и грабли | чат",
            linked_chat_username=None,
        ),
    ]
    resolved, unresolved = resolve_chat_sources(
        [
            ChatSource("username", "hyper_llm"),
            ChatSource("id", "1788662720"),
            ChatSource("username", "nope_chat"),
            ChatSource("id", "999999"),
        ],
        channels,
    )
    assert unresolved == ["nope_chat"]  # a bare id needs no dialog to resolve
    by_id = {c.entity_id: c for c in resolved}
    assert by_id[3850100303].username == "hyper_llm"
    assert by_id[3850100303].parent_channel_id == 10
    assert by_id[3850100303].parent_channel_key == "channel:10"
    assert by_id[3850100303].citable is True
    # id form: title comes from the parent channel's linked_chat_title when known
    assert by_id[1788662720].title == "AI и грабли | чат"
    assert by_id[1788662720].parent_channel_id == 11
    assert by_id[1788662720].citable is False  # no public handle -> no t.me link
    # an id nobody links to still resolves (a standalone group), with no parent
    assert by_id[999999].parent_channel_id is None
    assert by_id[999999].title == "chat:999999"


# ── 2. the deterministic admission gate ───────────────────────────
def test_chat_gate_admits_a_substantial_message_with_a_link():
    assert chat_admission_verdict(_cmsg(1), dup_channel_ids=set()) == "admit"


def test_chat_gate_admits_a_forward_from_an_untracked_channel():
    assert (
        chat_admission_verdict(_cmsg(1, url=False, fwd_channel=777), dup_channel_ids={10})
        == "admit"
    )


def test_chat_gate_rejects_chatter():
    cases = {
        "too-short": _cmsg(1, "ага, согласен"),
        "no-carrier": _cmsg(2, url=False),
        "nested-reply": _cmsg(3, reply_to=90, reply_top=50),
        "service": _cmsg(4, action={"_": "MessageActionChatAddUser"}),
        "via-bot": _cmsg(5, via_bot=1234),
        "forward-of-tracked-channel": _cmsg(6, fwd_channel=10),
    }
    for expected, message in cases.items():
        assert chat_admission_verdict(message, dup_channel_ids={10}) == expected


def test_chat_gate_keeps_a_top_level_comment_under_a_channel_post():
    """In a linked discussion group EVERY comment carries reply_to; only a reply to
    another comment sets a DIFFERENT reply_to_top_id. Dropping all replies would
    make a linked chat contribute nothing, so only nested replies are dropped."""
    assert chat_admission_verdict(_cmsg(1, reply_to=50, reply_top=50), dup_channel_ids=set()) == "admit"
    assert chat_admission_verdict(_cmsg(2, reply_to=50), dup_channel_ids=set()) == "admit"


# ── 3. the per-chat volume bound ──────────────────────────────────
async def test_collect_chats_caps_one_chat_per_run(tmp_path):
    store = TelegramDigestStore(tmp_path / "agg.db")
    chat_id = 3850100303
    channels = [FakeChannel(10, "hyperllm", linked_chat_id=chat_id,
                            linked_chat_username="hyper_llm", linked_chat_title="HyperAI")]
    client = FakeProxyClient(channels, {chat_id: [_cmsg(i) for i in range(1, 31)]})
    result = await collect_chats(
        client, store, [ChatSource("username", "hyper_llm")], channels=channels
    )
    assert result["collected_messages"] == CHAT_MAX_PER_RUN == 2
    report = result["chats"][0]
    assert report["seen"] == 30
    assert report["admitted"] == 2
    assert report["rejected"]["over-cap"] == 28
    rows = store.list_sources(roles=(AGG_ROLE,))
    assert [r.kind for r in rows] == [CHAT_KIND]


async def test_collect_chats_advances_the_cursor_past_everything_seen(tmp_path):
    """The cap must not stall the chat: the cursor advances past every message
    SEEN, not just the ones admitted, so run N+1 reads fresh traffic."""
    store = TelegramDigestStore(tmp_path / "agg.db")
    chat_id = 555
    channels = [FakeChannel(10, "c", linked_chat_id=chat_id, linked_chat_username="ch")]
    client = FakeProxyClient(channels, {chat_id: [_cmsg(i) for i in range(1, 21)]})
    await collect_chats(client, store, [ChatSource("id", str(chat_id))], channels=channels)
    await collect_chats(client, store, [ChatSource("id", str(chat_id))], channels=channels)
    assert client.read_calls[0]["min_id"] == 0
    assert client.read_calls[0]["recent_first"] is True
    assert client.read_calls[1]["min_id"] == 20  # highest id seen, not highest admitted


# ── 4. dedup against the channel corpus ───────────────────────────
def _seed_channel_post(store, entity_id, username, mid, text, posted="2026-07-30T09:00:00+00:00"):
    peer_key = f"channel:{entity_id}"
    store.upsert_source(
        peer_key=peer_key, entity_id=entity_id, title=username, username=username,
        kind="channel", linked_channel_key=None, role=AGG_ROLE,
    )
    store.insert_message(
        peer_key=peer_key, message_id=mid, posted_at=datetime.fromisoformat(posted),
        sender_id=None, views=100, forwards=1, replies=None,
        link=f"https://t.me/{username}/{mid}", text=text, raw_json={},
    )


async def test_collect_chats_dedups_a_chat_echo_of_a_channel_post(tmp_path):
    store = TelegramDigestStore(tmp_path / "agg.db")
    echoed = LONG + " https://example.com/post"
    _seed_channel_post(store, 10, "hyperllm", 5, echoed)
    chat_id = 3850100303
    channels = [FakeChannel(10, "hyperllm", linked_chat_id=chat_id, linked_chat_username="hyper_llm")]
    client = FakeProxyClient(channels, {chat_id: [_cmsg(1)]})  # same text as the post
    result = await collect_chats(
        client, store, [ChatSource("username", "hyper_llm")], channels=channels
    )
    assert result["collected_messages"] == 0
    assert result["chats"][0]["rejected"]["duplicate-text"] == 1


async def test_collect_chats_drops_a_forward_of_the_parent_channel_post(tmp_path):
    """The auto-forward of the parent post lands in the linked chat verbatim; we
    already have the original (with views) via the channel lane."""
    store = TelegramDigestStore(tmp_path / "agg.db")
    _seed_channel_post(store, 10, "hyperllm", 5, "unrelated but long text " * 10)
    chat_id = 3850100303
    channels = [FakeChannel(10, "hyperllm", linked_chat_id=chat_id, linked_chat_username="hyper_llm")]
    client = FakeProxyClient(
        channels,
        {chat_id: [_cmsg(1, text="совсем другой текст " * 15, url=False, fwd_channel=10)]},
    )
    result = await collect_chats(
        client, store, [ChatSource("username", "hyper_llm")], channels=channels
    )
    assert result["collected_messages"] == 0
    assert result["chats"][0]["rejected"]["forward-of-tracked-channel"] == 1


async def test_collect_chats_drops_a_quote_of_a_corpus_post(tmp_path):
    """The other echo shape, seen live: not a forward but a manual repost that
    prepends the source link, which shifts the first 120 chars so the TEXT key no
    longer matches. The quoted t.me link is exact identity — use it."""
    store = TelegramDigestStore(tmp_path / "agg.db")
    _seed_channel_post(store, 10, "deksden_notes", 1027, "Оригинальный пост канала. " * 12)
    chat_id = 3850100303
    channels = [FakeChannel(11, "hyperllm", linked_chat_id=chat_id, linked_chat_username="hyper_llm")]
    quote = "https://t.me/deksden_notes/1027\n\nОригинальный пост канала. " * 6
    client = FakeProxyClient(channels, {chat_id: [_cmsg(1, text=quote, url=False)]})
    result = await collect_chats(
        client, store, [ChatSource("username", "hyper_llm")], channels=channels
    )
    assert result["collected_messages"] == 0
    assert result["chats"][0]["rejected"]["echo-of-corpus-post"] == 1


async def test_collect_chats_keeps_a_quote_of_an_untracked_post(tmp_path):
    store = TelegramDigestStore(tmp_path / "agg.db")
    chat_id = 3850100303
    channels = [FakeChannel(11, "hyperllm", linked_chat_id=chat_id, linked_chat_username="hyper_llm")]
    quote = "https://t.me/some_other_channel/9\n\n" + LONG
    client = FakeProxyClient(channels, {chat_id: [_cmsg(1, text=quote, url=False)]})
    result = await collect_chats(
        client, store, [ChatSource("username", "hyper_llm")], channels=channels
    )
    assert result["collected_messages"] == 1


async def test_collect_chats_dedups_across_two_chats_in_one_run(tmp_path):
    store = TelegramDigestStore(tmp_path / "agg.db")
    channels = [
        FakeChannel(10, "a", linked_chat_id=100, linked_chat_username="a_chat"),
        FakeChannel(11, "b", linked_chat_id=200, linked_chat_username="b_chat"),
    ]
    client = FakeProxyClient(channels, {100: [_cmsg(1)], 200: [_cmsg(1)]})
    result = await collect_chats(
        client, store,
        [ChatSource("username", "a_chat"), ChatSource("username", "b_chat")],
        channels=channels,
    )
    assert result["collected_messages"] == 1
    assert sum(c["rejected"].get("duplicate-text", 0) for c in result["chats"]) == 1


# ── 5. origin marking + default OFF + dry run ─────────────────────
async def test_draft_input_marks_chat_origin(tmp_path):
    store = TelegramDigestStore(tmp_path / "agg.db")
    _seed_channel_post(store, 10, "hyperllm", 5, "Пост канала про релиз модели. " * 6)
    chat_id = 3850100303
    channels = [FakeChannel(10, "hyperllm", linked_chat_id=chat_id, linked_chat_username="hyper_llm")]
    client = FakeProxyClient(channels, {chat_id: [_cmsg(1)]})
    await collect_chats(client, store, [ChatSource("username", "hyper_llm")], channels=channels)
    doc = build_draft_input(store, window_hours=24 * 365 * 10)
    origins = {p["origin"] for p in doc["posts"]}
    assert origins == {"channel", "chat"}
    chat_post = next(p for p in doc["posts"] if p["origin"] == "chat")
    assert chat_post["link"] == f"https://t.me/some_chat/1"


async def test_collect_chats_dry_run_writes_nothing_and_reports_text(tmp_path):
    store = TelegramDigestStore(tmp_path / "agg.db")
    chat_id = 3850100303
    channels = [FakeChannel(10, "hyperllm", linked_chat_id=chat_id, linked_chat_username="hyper_llm")]
    client = FakeProxyClient(
        channels, {chat_id: [_cmsg(1), _cmsg(2, "коротко"), _cmsg(3, url=False)]}
    )
    result = await collect_chats(
        client, store, [ChatSource("username", "hyper_llm")], channels=channels, dry_run=True
    )
    assert result["dry_run"] is True
    assert result["collected_messages"] == 0
    report = result["chats"][0]
    assert report["seen"] == 3
    assert report["admitted"] == 1
    assert report["rejected"] == {"too-short": 1, "no-carrier": 1}
    assert report["texts"] and report["texts"][0]["message_id"] == 1
    # nothing persisted: no source row, no message, no watermark
    assert store.list_sources(roles=(AGG_ROLE,)) == []
    assert store.last_message_id(f"{CHAT_KIND}:{chat_id}") == 0
    assert store.last_seen_message_id(f"{CHAT_KIND}:{chat_id}") == 0


async def test_collect_chats_no_sources_is_a_noop(tmp_path):
    store = TelegramDigestStore(tmp_path / "agg.db")
    client = FakeProxyClient([], {})
    result = await collect_chats(client, store, [], channels=[])
    assert result == {
        "resolved": 0, "unresolved": [], "collected_messages": 0,
        "failed_sources": 0, "dry_run": False, "chats": [],
    }
    assert client.read_calls == []


async def test_collect_chats_isolates_a_failing_chat(tmp_path):
    store = TelegramDigestStore(tmp_path / "agg.db")
    channels = [
        FakeChannel(10, "a", linked_chat_id=100, linked_chat_username="a_chat"),
        FakeChannel(11, "b", linked_chat_id=200, linked_chat_username="b_chat"),
    ]
    client = FakeProxyClient(channels, {100: RuntimeError("FLOOD_WAIT"), 200: [_cmsg(1)]})
    result = await collect_chats(
        client, store,
        [ChatSource("username", "a_chat"), ChatSource("username", "b_chat")],
        channels=channels,
    )
    assert result["failed_sources"] == 1
    assert result["collected_messages"] == 1
