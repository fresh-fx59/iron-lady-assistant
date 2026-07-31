"""tests/test_telegram_aggregator_publish.py"""
from __future__ import annotations

import base64
import json
import sqlite3

import pytest

from src.telegram_aggregator_gates import Story
from src.telegram_aggregator_publish import (
    _RICH_CAP,
    BotApiTransport,
    DigestLedger,
    PhotoNotSent,
    _build_send_ops,
    _render_story,
    publish_next,
    render_messages,
    render_rich_html,
    serialize_stories,
)

FOOTER = "🤖 Дайджест: отбор автоматический, курирование вручную."

# A real 1x1 PNG (base64) so send_photo tests write valid bytes without network.
PNG_B64 = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAF"
    "AAH/iZk9HQAAAABJRU5ErkJggg=="
)
PNG_BYTES = base64.b64decode(PNG_B64)


def _story(i, summary_len=100):
    return Story(
        headline=f"Сюжет {i} <важный>",
        summary=("х" * summary_len) + " & конец.",
        source_links=[f"https://t.me/chan/{i}"],
    )


def _row(tmp_path, date_key):
    con = sqlite3.connect(tmp_path / "ledger.db")
    con.row_factory = sqlite3.Row
    try:
        return con.execute(
            "SELECT messages_json, status, sent_count FROM digests WHERE date_key = ?",
            (date_key,),
        ).fetchone()
    finally:
        con.close()


def test_render_single_message_structure():
    msgs = render_messages([_story(1), _story(2)], date_label="14.07", footer=FOOTER)
    assert len(msgs) == 1
    text = msgs[0]
    assert text.startswith("📰 <b>AI-дайджест — 14.07</b>")
    assert "<b>Сюжет 1 &lt;важный&gt;</b>" in text          # escaped headline
    assert '<a href="https://t.me/chan/1">' in text
    assert text.rstrip().endswith(FOOTER)
    assert "&amp; конец." in text                            # escaped summary


def test_render_splits_at_story_boundary():
    stories = [_story(i, summary_len=390) for i in range(12)]
    msgs = render_messages(stories, date_label="14.07", footer=FOOTER)
    assert len(msgs) >= 2
    assert all(len(m) <= 4000 for m in msgs)
    assert msgs[-1].rstrip().endswith(FOOTER)
    assert FOOTER not in "".join(msgs[:-1])
    joined = "".join(msgs)
    for i in range(12):
        assert f"Сюжет {i}" in joined                        # nothing lost in the split


def test_render_fits_oversized_single_story():
    story = Story(
        headline="Огромный сюжет",
        summary="х" * 5000,
        source_links=["https://t.me/chan/1"],
    )
    msgs = render_messages([story], date_label="14.07", footer=FOOTER)
    assert all(len(m) <= 4000 for m in msgs)
    # links line survives intact
    assert any('<a href="https://t.me/chan/1">' in m for m in msgs)
    # the truncated summary line (immediately before "Источники:") ends with "…"
    block_msg = next(m for m in msgs if "Источники:" in m)
    summary_line = block_msg.split("Источники:")[0].rstrip("\n").splitlines()[-1]
    assert summary_line.endswith("…")


class FakeTransport:
    def __init__(self, fail_at=None, fail_photo=False, fail_rich=False):
        self.calls = []  # send_message: (chat, text) — legacy shape, unchanged
        self.photos = []  # send_photo: (chat, path, caption)
        self.rich = []  # send_rich_message: (chat, rich_html, photo_path)
        self._fail_at = fail_at
        self._fail_photo = fail_photo
        self._fail_rich = fail_rich

    def send_message(self, chat, text):
        # Count only text sends so interleaved photos don't shift fail_at.
        if self._fail_at is not None and len(self.calls) == self._fail_at:
            raise RuntimeError("boom")
        self.calls.append((chat, text))
        return len(self.calls)

    def send_photo(self, chat, photo_path, caption):
        # fail_photo picks the failure KIND:
        #   True / "not_sent" -> PhotoNotSent (photo provably never reached TG)
        #   "ambiguous"       -> RuntimeError (possibly already received)
        #   None / False      -> success
        if self._fail_photo in (True, "not_sent"):
            raise PhotoNotSent("photo not sent")
        if self._fail_photo == "ambiguous":
            raise RuntimeError("photo boom (already received)")
        self.photos.append((chat, photo_path, caption))
        return len(self.photos)

    def send_rich_message(self, chat, rich_html, photo_path):
        # fail_rich picks the failure KIND — same vocabulary as fail_photo:
        #   True / "not_sent" -> PhotoNotSent (nothing reached TG; safe to degrade)
        #   "ambiguous"       -> RuntimeError (TG may already hold the post)
        #   None / False      -> success
        if self._fail_rich in (True, "not_sent"):
            raise PhotoNotSent("rich message not sent")
        if self._fail_rich == "ambiguous":
            raise RuntimeError("rich boom (already received)")
        self.rich.append((chat, rich_html, photo_path))
        return len(self.rich)


def _ledger(tmp_path):
    return DigestLedger(tmp_path / "ledger.db")


def test_ledger_flow_and_publish(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.upsert_draft("2026-07-14", ["msg one", "msg two"])
    assert publish_next(ledger, FakeTransport(), "@chan")["status"] == "skipped"  # not approved
    assert ledger.approve() == "2026-07-14"
    transport = FakeTransport()
    result = publish_next(ledger, transport, "@chan")
    assert result == {"status": "posted", "date_key": "2026-07-14", "messages": 2}
    assert [c[1] for c in transport.calls] == ["msg one", "msg two"]
    # once posted, nothing further to publish
    assert publish_next(ledger, transport, "@chan")["status"] == "skipped"


def test_publish_dry_run_reverts(tmp_path, capsys):
    ledger = _ledger(tmp_path)
    ledger.upsert_draft("2026-07-14", ["msg"])
    ledger.approve()
    result = publish_next(ledger, None, None, dry_run=True)
    assert result["status"] == "dry-run"
    assert "msg" in capsys.readouterr().out
    # still approved -> a later real publish can pick it up
    assert ledger.next_approved() is not None


def test_stuck_sending_blocks(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.upsert_draft("2026-07-13", ["a"])
    ledger.approve()
    assert ledger.begin_send("2026-07-13")            # simulate crash mid-send
    ledger.upsert_draft("2026-07-14", ["b"])
    ledger.approve()
    result = publish_next(ledger, FakeTransport(), "@chan")
    assert result["status"] == "blocked"


def test_upsert_same_day_replaces_pending(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.upsert_draft("2026-07-14", ["v1"])
    ledger.upsert_draft("2026-07-14", ["v2", "v2b"])
    ledger.approve()
    transport = FakeTransport()
    publish_next(ledger, transport, "@chan")
    assert [c[1] for c in transport.calls] == ["v2", "v2b"]


def test_upsert_ignores_locked_posted_row(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.upsert_draft("2026-07-14", ["v1"])
    ledger.approve()
    assert ledger.begin_send("2026-07-14")
    ledger.mark_posted("2026-07-14")
    # a later upsert for the same date_key must NOT touch a posted row's
    # content or status — the ledger is the audit trail of what actually shipped
    ledger.upsert_draft("2026-07-14", ["v2-should-not-apply", "v2b"])
    row = _row(tmp_path, "2026-07-14")
    assert json.loads(row["messages_json"]) == ["v1"]
    assert row["status"] == "posted"


def test_publish_mid_send_crash_leaves_sending_and_blocks(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.upsert_draft("2026-07-14", ["msg one", "msg two", "msg three"])
    ledger.approve()
    transport = FakeTransport(fail_at=1)  # fails on the 2nd send_message call
    result = publish_next(ledger, transport, "@chan")
    assert result["status"] == "failed"
    assert result["sent"] == 1
    assert result["total"] == 3
    row = _row(tmp_path, "2026-07-14")
    assert row["status"] == "sending"
    assert row["sent_count"] == 1
    # the stuck 'sending' row must block all further publishing (no double-post
    # of the messages that already went out)
    result2 = publish_next(ledger, FakeTransport(), "@chan")
    assert result2["status"] == "blocked"


def test_publish_dry_run_reverts_via_public_method(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.upsert_draft("2026-07-14", ["msg"])
    ledger.approve()
    assert ledger.begin_send("2026-07-14")
    row = _row(tmp_path, "2026-07-14")
    assert row["status"] == "sending"
    # revert_to_approved is now public surface (dry-run's state-machine escape hatch)
    ledger.revert_to_approved("2026-07-14")
    row = _row(tmp_path, "2026-07-14")
    assert row["status"] == "approved"


def test_notify_operator_resolves_file_delivered_token(tmp_path, monkeypatch):
    """Regression: the runner heredocs call notify_operator with only the
    *_FILE env set (2026-07-15 silent-False bug) — it must self-resolve."""
    import src.telegram_aggregator_publish as pub

    token_file = tmp_path / "tok"
    token_file.write_text("file-token-123\n")
    monkeypatch.delenv("AGGREGATOR_ALERT_BOT_TOKEN", raising=False)
    monkeypatch.setenv("AGGREGATOR_ALERT_BOT_TOKEN_FILE", str(token_file))
    monkeypatch.setenv("AGGREGATOR_OPERATOR_CHAT_ID", "42")

    captured = {}

    class CapturingTransport:
        def __init__(self, token):
            captured["token"] = token

        def send_message(self, chat, text, *, parse_mode="HTML"):
            captured["chat"] = chat
            captured["text"] = text
            captured["parse_mode"] = parse_mode
            return 1

    monkeypatch.setattr(pub, "BotApiTransport", CapturingTransport)
    assert pub.notify_operator("привет") is True
    assert captured == {
        "token": "file-token-123",
        "chat": "42",
        "text": "привет",
        # an operator alert is PLAIN TEXT: with parse_mode=HTML a report saying
        # "scores 0.19 < 0.35" is a hard 400 and reaches nobody (prod 2026-07-31)
        "parse_mode": None,
    }


def test_botapi_send_message_defaults_to_html_but_can_send_plain(monkeypatch):
    """The digest's own posts ARE HTML; operator alerts are not. One transport,
    the parse mode chosen by the caller — and omitted entirely when plain."""
    import urllib.parse

    import src.telegram_aggregator_publish as pub

    bodies = []

    def fake_urlopen(request, timeout=None):
        bodies.append(dict(urllib.parse.parse_qsl(request.data.decode())))
        return _FakeResp(11)

    monkeypatch.setattr(pub.urllib.request, "urlopen", fake_urlopen)

    BotApiTransport("tok").send_message("@chan", "<b>digest</b>")
    BotApiTransport("tok").send_message("42", "0.19 < 0.35 & @handle", parse_mode=None)

    assert bodies[0]["parse_mode"] == "HTML"
    assert "parse_mode" not in bodies[1]
    assert bodies[1]["text"] == "0.19 < 0.35 & @handle"


# ===========================================================================
# Feature A2 — gpt-image-2 English infographic (image at gate, photo at publish)
# ===========================================================================
def _png(tmp_path, name="digest.png"):
    p = tmp_path / name
    p.write_bytes(PNG_BYTES)
    return p


# --- Step 13: image_path column + accessors ---------------------------------
def test_migration_adds_image_path_idempotent(tmp_path):
    db = tmp_path / "ledger.db"
    con = sqlite3.connect(db)
    con.execute(
        """
        CREATE TABLE digests (
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
    con.commit()
    con.close()

    DigestLedger(db)
    DigestLedger(db)  # idempotent — second construction raises nothing

    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    try:
        cols = {r["name"] for r in con.execute("PRAGMA table_info(digests)").fetchall()}
    finally:
        con.close()
    assert "image_path" in cols


def test_set_and_get_image_path(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.upsert_draft("2026-07-14", ["m"])
    ledger.set_image_path("2026-07-14", "/x/y.png")
    assert ledger.image_path_for("2026-07-14") == "/x/y.png"
    assert ledger.image_path_for("2026-01-01") is None  # unknown date -> None


# --- One post = image + full text (Bot API 10.2 sendRichMessage) ------------
def test_render_rich_html_structure():
    msgs = render_messages([_story(1), _story(2)], date_label="14.07", footer=FOOTER)
    assert len(msgs) == 1  # the gate always trims to one message
    rich = render_rich_html(msgs)
    # the image is the LEADING media block, referenced by tg://photo?id=<id>
    assert rich.startswith('<figure><img src="tg://photo?id=hero"/></figure>')
    # header paragraph, then one paragraph per story, then the footer element
    assert "<p>📰 <b>AI-дайджест — 14.07</b></p>" in rich
    assert f"<footer>{FOOTER}</footer>" in rich
    # every story block keeps _render_story's exact wording + escaping, with the
    # in-block newlines turned into <br> (rich HTML collapses raw newlines)
    for i in (1, 2):
        block = _render_story(i, _story(i)).replace("\n", "<br>")
        assert f"<p>{block}</p>" in rich
    # nothing rides outside a block, and no raw newlines survive
    assert "\n" not in rich


def test_render_rich_html_loses_no_story_when_render_split():
    # A multi-message render (pre-gate-trim shape) rejoins into ONE document.
    stories = [_story(i, summary_len=390) for i in range(12)]
    msgs = render_messages(stories, date_label="14.07", footer=FOOTER)
    assert len(msgs) >= 2
    rich = render_rich_html(msgs)
    for i in range(12):
        assert f"Сюжет {i} " in rich or f"Сюжет {i}<" in rich
    assert rich.count("<figure>") == 1  # exactly one media block
    assert rich.count("<footer>") == 1  # footer emitted once, at the end
    assert rich.endswith("</footer>")


def test_render_rich_html_custom_media_id():
    rich = render_rich_html(["a"], media_id="pic1")
    assert '<img src="tg://photo?id=pic1"/>' in rich


def test_build_send_ops_image_means_one_rich_op():
    # image + single short message -> ONE rich op (was: caption-only photo)
    ops = _build_send_ops(["short"], "/x/y.png", "cap")
    assert len(ops) == 1
    assert ops[0][0] == "rich" and ops[0][2] == "/x/y.png"
    assert "short" in ops[0][1]
    # image + a message over the 1024 caption cap -> STILL one rich op (the whole
    # point: the old plan emitted photo+text = two posts here)
    long_msg = "x" * 1100
    ops = _build_send_ops([long_msg], "/x/y.png", "cap")
    assert len(ops) == 1 and ops[0][0] == "rich" and long_msg in ops[0][1]
    # image + multiple messages -> one rich op carrying all of them
    ops = _build_send_ops(["a", "b"], "/x/y.png", "cap")
    assert len(ops) == 1 and ops[0][0] == "rich"
    assert "<p>a</p>" in ops[0][1] and "<footer>b</footer>" in ops[0][1]
    # no image -> all text, unchanged
    assert _build_send_ops(["a", "b"], None, "cap") == [("text", "a"), ("text", "b")]


def test_build_send_ops_falls_back_to_photo_text_over_rich_cap():
    # A digest whose rich HTML would exceed the 32768-char rich-message ceiling
    # falls back to the documented photo+text plan rather than being rejected.
    messages = ["x" * 4000] * 9
    ops = _build_send_ops(messages, "/x/y.png", "cap")
    assert len(render_rich_html(messages)) > _RICH_CAP
    assert ops == [("photo", "/x/y.png", "cap")] + [("text", m) for m in messages]


def test_build_send_ops_photo_text_fallback_keeps_caption_only_shape():
    # The old caption-only plan stays reachable as the second fallback rung.
    import src.telegram_aggregator_publish as pub

    assert pub._build_photo_text_ops(["short"], "/x/y.png", "cap") == [
        ("photo", "/x/y.png", "short")
    ]
    long_msg = "x" * 1100
    assert pub._build_photo_text_ops([long_msg], "/x/y.png", "cap") == [
        ("photo", "/x/y.png", "cap"),
        ("text", long_msg),
    ]


# --- Step 15: BotApiTransport.send_photo wire shape -------------------------
class _FakeResp:
    def __init__(self, message_id=7):
        self._mid = message_id

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return json.dumps({"result": {"message_id": self._mid}}).encode()


def test_botapi_send_photo_wire_shape(tmp_path, monkeypatch):
    import src.telegram_aggregator_publish as pub

    png = _png(tmp_path)
    captured = {}

    def fake_urlopen(request, timeout=None):
        captured["url"] = request.full_url
        captured["headers"] = dict(request.header_items())
        captured["body"] = request.data
        return _FakeResp(7)

    monkeypatch.setattr(pub.urllib.request, "urlopen", fake_urlopen)

    mid = BotApiTransport("tok123").send_photo("@chan", str(png), "cap")
    assert mid == 7
    assert captured["url"].endswith("/sendPhoto")
    ctype = next(v for k, v in captured["headers"].items() if k.lower() == "content-type")
    assert ctype.startswith("multipart/form-data; boundary=")
    body = captured["body"]
    assert b'name="chat_id"' in body and b"@chan" in body
    assert b'name="caption"' in body and b"cap" in body
    assert b'name="parse_mode"' in body and b"HTML" in body
    assert b'name="photo"; filename=' in body
    assert PNG_BYTES in body


def test_botapi_send_photo_caption_truncated_to_1024(tmp_path, monkeypatch):
    import src.telegram_aggregator_publish as pub

    png = _png(tmp_path)
    captured = {}

    def fake_urlopen(request, timeout=None):
        captured["body"] = request.data
        return _FakeResp(1)

    monkeypatch.setattr(pub.urllib.request, "urlopen", fake_urlopen)

    caption = "A" * 1024 + "B" * 976  # 2000 chars; only the first 1024 survive
    BotApiTransport("tok").send_photo("@chan", str(png), caption)
    body = captured["body"]
    # The caption field carries EXACTLY the first 1024 chars, terminated by the
    # multipart CRLF — the 976 'B's past the cap are gone. (Assert on the caption
    # field itself, not the whole body: the binary PNG contains stray 0x42 bytes.)
    assert b'name="caption"\r\n\r\n' + b"A" * 1024 + b"\r\n" in body
    assert b"B" * 976 not in body  # the dropped tail never made it into the body


# --- BotApiTransport.send_rich_message wire shape + failure kinds -----------
def test_botapi_send_rich_message_wire_shape(tmp_path, monkeypatch):
    import src.telegram_aggregator_publish as pub

    png = _png(tmp_path)
    captured = {}

    def fake_urlopen(request, timeout=None):
        captured["url"] = request.full_url
        captured["headers"] = dict(request.header_items())
        captured["body"] = request.data
        captured["timeout"] = timeout
        return _FakeResp(245)

    monkeypatch.setattr(pub.urllib.request, "urlopen", fake_urlopen)

    rich_html = '<figure><img src="tg://photo?id=hero"/></figure><p>Дайджест</p>'
    mid = BotApiTransport("tok123").send_rich_message("@chan", rich_html, str(png))
    assert mid == 245
    assert captured["url"].endswith("/sendRichMessage")
    assert captured["timeout"] == 60  # upload budget, same as sendPhoto
    ctype = next(v for k, v in captured["headers"].items() if k.lower() == "content-type")
    assert ctype.startswith("multipart/form-data; boundary=")
    body = captured["body"]
    # text fields: chat_id + rich_message (JSON, non-ASCII kept readable)
    assert b'name="chat_id"' in body and b"@chan" in body
    assert b'name="rich_message"' in body
    assert "Дайджест".encode() in body
    # the rich_message JSON declares the media, referencing the file part by name
    start = body.index(b'name="rich_message"\r\n\r\n') + len(b'name="rich_message"\r\n\r\n')
    payload = json.loads(body[start : body.index(b"\r\n--", start)].decode())
    assert payload["html"] == rich_html
    assert payload["media"] == [
        {"id": "hero", "media": {"type": "photo", "media": "attach://hero"}}
    ]
    # ...and the file part is named exactly what attach:// points at
    assert b'name="hero"; filename=' in body
    assert PNG_BYTES in body


def test_botapi_send_rich_message_unreadable_file_is_photo_not_sent(tmp_path):
    # Nothing was ever built, let alone sent => provably-not-sent => safe to degrade.
    with pytest.raises(PhotoNotSent):
        BotApiTransport("tok").send_rich_message("@chan", "<p>x</p>", str(tmp_path / "nope.png"))


def test_botapi_send_rich_message_connection_error_is_photo_not_sent(tmp_path, monkeypatch):
    import src.telegram_aggregator_publish as pub

    png = _png(tmp_path)

    def fake_urlopen(request, timeout=None):
        raise pub.urllib.error.URLError(ConnectionRefusedError("nope"))

    monkeypatch.setattr(pub.urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(PhotoNotSent):
        BotApiTransport("tok").send_rich_message("@chan", "<p>x</p>", str(png))


def test_botapi_send_rich_message_timeout_is_ambiguous(tmp_path, monkeypatch):
    """A read timeout may mean Telegram already accepted the post -> NOT
    PhotoNotSent, so the caller must never degrade (that would double-post)."""
    import src.telegram_aggregator_publish as pub

    png = _png(tmp_path)

    def fake_urlopen(request, timeout=None):
        raise pub.urllib.error.URLError(TimeoutError("timed out"))

    monkeypatch.setattr(pub.urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(pub.urllib.error.URLError):
        BotApiTransport("tok").send_rich_message("@chan", "<p>x</p>", str(png))


def test_botapi_send_rich_message_http_error_is_ambiguous(tmp_path, monkeypatch):
    """Telegram ANSWERED (HTTP status) => it may hold the post => ambiguous."""
    import io

    import src.telegram_aggregator_publish as pub

    png = _png(tmp_path)

    def fake_urlopen(request, timeout=None):
        raise pub.urllib.error.HTTPError(
            request.full_url, 400, "Bad Request", {}, io.BytesIO(b"{}")
        )

    monkeypatch.setattr(pub.urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(pub.urllib.error.HTTPError):
        BotApiTransport("tok").send_rich_message("@chan", "<p>x</p>", str(png))


def test_botapi_send_rich_message_404_is_photo_not_sent(tmp_path, monkeypatch):
    """404 = the endpoint has no sendRichMessage (pre-10.2). Telegram answered,
    but with 'this method does not exist' — nothing was posted, so the publisher
    may degrade instead of freezing the channel forever. (Verified live: a bogus
    method 404s while an empty rich body 400s.)"""
    import io

    import src.telegram_aggregator_publish as pub

    png = _png(tmp_path)

    def fake_urlopen(request, timeout=None):
        raise pub.urllib.error.HTTPError(
            request.full_url, 404, "Not Found", {}, io.BytesIO(b"")
        )

    monkeypatch.setattr(pub.urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(PhotoNotSent):
        BotApiTransport("tok").send_rich_message("@chan", "<p>x</p>", str(png))


def test_botapi_send_photo_shares_failure_classification(tmp_path, monkeypatch):
    """send_photo and send_rich_message run the same classifier (_upload), so pin
    the photo side too: connection error => PhotoNotSent, timeout => ambiguous,
    and a 404 on sendPhoto stays ambiguous (only the rich path opts in)."""
    import io

    import src.telegram_aggregator_publish as pub

    png = _png(tmp_path)
    transport = BotApiTransport("tok")

    def raising(exc):
        def fake_urlopen(request, timeout=None):
            raise exc

        monkeypatch.setattr(pub.urllib.request, "urlopen", fake_urlopen)

    raising(pub.urllib.error.URLError(ConnectionResetError("reset")))
    with pytest.raises(PhotoNotSent):
        transport.send_photo("@chan", str(png), "cap")

    raising(pub.urllib.error.URLError(TimeoutError("timed out")))
    with pytest.raises(pub.urllib.error.URLError):
        transport.send_photo("@chan", str(png), "cap")

    raising(pub.urllib.error.HTTPError("u", 404, "Not Found", {}, io.BytesIO(b"")))
    with pytest.raises(pub.urllib.error.HTTPError):
        transport.send_photo("@chan", str(png), "cap")


def test_botapi_send_photo_unreadable_file_is_photo_not_sent(tmp_path):
    with pytest.raises(PhotoNotSent):
        BotApiTransport("tok").send_photo("@chan", str(tmp_path / "nope.png"), "cap")


# --- Step 16: publish_next media sending + degrade + record -----------------
def test_publish_short_digest_with_image_sends_one_rich_message(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.upsert_draft("2026-07-14", ["short digest"])
    ledger.approve()
    png = _png(tmp_path)
    ledger.set_image_path("2026-07-14", str(png))
    transport = FakeTransport()
    result = publish_next(ledger, transport, "@chan")
    assert result["status"] == "posted"
    assert len(transport.rich) == 1
    chat, rich_html, photo_path = transport.rich[0]
    assert (chat, photo_path) == ("@chan", str(png))
    assert "short digest" in rich_html
    assert transport.photos == [] and transport.calls == []  # exactly ONE post


def test_publish_long_digest_with_image_still_sends_ONE_message(tmp_path):
    """The regression this feature exists for: a >1024-char digest used to ship
    as photo+text (TWO posts). It must now be a single rich message."""
    ledger = _ledger(tmp_path)
    long_msg = "y" * 1200
    ledger.upsert_draft("2026-07-14", [long_msg])
    ledger.approve()
    png = _png(tmp_path)
    ledger.set_image_path("2026-07-14", str(png))
    transport = FakeTransport()
    result = publish_next(ledger, transport, "@chan")
    assert result["status"] == "posted"
    assert len(transport.rich) == 1
    assert long_msg in transport.rich[0][1]  # the FULL text rides along
    assert transport.photos == [] and transport.calls == []
    assert _row(tmp_path, "2026-07-14")["sent_count"] == 1  # one send, not two


def test_publish_text_only_when_no_image(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.upsert_draft("2026-07-14", ["m1", "m2"])
    ledger.approve()
    transport = FakeTransport()
    assert publish_next(ledger, transport, "@chan")["status"] == "posted"
    assert transport.photos == []
    assert [c[1] for c in transport.calls] == ["m1", "m2"]


def _prepare(tmp_path, messages, date_key="2026-07-14"):
    ledger = _ledger(tmp_path)
    ledger.upsert_draft(date_key, messages)
    ledger.approve()
    png = _png(tmp_path)
    ledger.set_image_path(date_key, str(png))
    return ledger, png


def test_publish_degrades_to_photo_caption_when_rich_proven_not_sent(tmp_path):
    """Rung 1 -> rung 2: a provably-not-sent rich message falls back to the old
    photo plan (here the caption-fit shape), so an image glitch never loses the
    digest — and never double-posts."""
    ledger, png = _prepare(tmp_path, ["short digest"])
    transport = FakeTransport(fail_rich="not_sent")
    result = publish_next(ledger, transport, "@chan")
    assert result["status"] == "posted"
    assert transport.rich == []  # nothing landed on the rich path
    assert transport.photos == [("@chan", str(png), "short digest")]
    assert transport.calls == []  # caption carried it -> still one post
    assert _row(tmp_path, "2026-07-14")["status"] == "posted"


def test_publish_degrades_to_photo_then_text_when_rich_proven_not_sent(tmp_path):
    ledger, png = _prepare(tmp_path, ["y" * 1200])
    transport = FakeTransport(fail_rich="not_sent")
    result = publish_next(ledger, transport, "@chan")
    assert result["status"] == "posted"
    assert len(transport.photos) == 1
    assert len(transport.photos[0][2]) <= 1024  # short caption
    assert [c[1] for c in transport.calls] == ["y" * 1200]  # full text after it


def test_publish_degrades_to_text_when_rich_and_photo_both_proven_not_sent(tmp_path):
    """Rung 1 -> 2 -> 3: both media rungs prove nothing reached Telegram, so the
    digest still ships — as text only (the pre-image behavior)."""
    ledger, png = _prepare(tmp_path, ["short digest"])
    transport = FakeTransport(fail_rich="not_sent", fail_photo="not_sent")
    result = publish_next(ledger, transport, "@chan")
    assert result["status"] == "posted"
    assert transport.rich == [] and transport.photos == []
    assert [c[1] for c in transport.calls] == ["short digest"]  # text out exactly once
    assert _row(tmp_path, "2026-07-14")["status"] == "posted"


def test_publish_rich_ambiguous_failure_stays_stuck(tmp_path):
    # The whole digest rides in the ONE rich message. If sendRichMessage fails
    # AMBIGUOUSLY (Telegram may already hold the post), the publisher must NOT
    # fall back to any other plan — that would double-post. It freezes the row at
    # 'sending' instead, blocking re-publish until a human looks.
    ledger, png = _prepare(tmp_path, ["short digest"])
    transport = FakeTransport(fail_rich="ambiguous")
    result = publish_next(ledger, transport, "@chan")
    assert result["status"] == "failed"
    assert transport.photos == [] and transport.calls == []  # no fallback attempted
    assert _row(tmp_path, "2026-07-14")["status"] == "sending"  # stuck -> blocks
    assert publish_next(ledger, FakeTransport(), "@chan")["status"] == "blocked"


def test_publish_degrades_to_text_when_leading_photo_fails(tmp_path):
    """The rung-2 photo plan keeps its own degrade contract: a provably-not-sent
    leading photo (PhotoNotSent at sent==0) drops to text-only."""
    ledger, png = _prepare(tmp_path, ["y" * 1200])
    transport = FakeTransport(fail_rich="not_sent", fail_photo="not_sent")
    result = publish_next(ledger, transport, "@chan")
    assert result["status"] == "posted"
    assert transport.photos == []
    assert [c[1] for c in transport.calls] == ["y" * 1200]


def test_publish_leading_photo_ambiguous_failure_stays_stuck(tmp_path):
    # Same double-post guard on the rung-2 photo plan: an AMBIGUOUS photo failure
    # stops the chain dead rather than re-posting the digest as text.
    ledger, png = _prepare(tmp_path, ["short digest"])
    transport = FakeTransport(fail_rich="not_sent", fail_photo="ambiguous")
    result = publish_next(ledger, transport, "@chan")
    assert result["status"] == "failed"
    assert transport.calls == []  # NO text fallback -> no duplicate post
    assert _row(tmp_path, "2026-07-14")["status"] == "sending"  # stuck -> blocks
    assert publish_next(ledger, FakeTransport(), "@chan")["status"] == "blocked"


def test_publish_stuck_when_text_fails_after_photo(tmp_path):
    ledger, png = _prepare(tmp_path, ["y" * 1200])  # rung 2 = photo(caption) + text
    transport = FakeTransport(fail_rich="not_sent", fail_at=0)  # photo ok, text fails
    result = publish_next(ledger, transport, "@chan")
    assert result["status"] == "failed"
    assert len(transport.photos) == 1  # the photo already went to the channel
    assert _row(tmp_path, "2026-07-14")["status"] == "sending"  # stuck -> blocks
    assert publish_next(ledger, FakeTransport(), "@chan")["status"] == "blocked"


def test_publish_dry_run_describes_rich_plan(tmp_path, capsys):
    ledger, png = _prepare(tmp_path, ["short digest"])
    result = publish_next(ledger, None, None, dry_run=True)
    assert result["status"] == "dry-run"
    out = capsys.readouterr().out
    assert "RICH" in out
    assert str(png) in out  # dry-run references the image
    assert "chars:" in out  # ...and the rich-HTML size, not a caption line
    assert "caption:" not in out
    assert ledger.next_approved() is not None  # reverted, still approved
