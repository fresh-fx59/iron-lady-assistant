"""tests/test_telegram_aggregator_publish.py"""
from __future__ import annotations

import base64
import json
import sqlite3

import pytest

from src.telegram_aggregator_gates import Story
from src.telegram_aggregator_publish import (
    BotApiTransport,
    DigestLedger,
    PhotoNotSent,
    _build_send_ops,
    publish_next,
    render_messages,
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
    def __init__(self, fail_at=None, fail_photo=False):
        self.calls = []  # send_message: (chat, text) — legacy shape, unchanged
        self.photos = []  # send_photo: (chat, path, caption)
        self._fail_at = fail_at
        self._fail_photo = fail_photo

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

        def send_message(self, chat, text):
            captured["chat"] = chat
            captured["text"] = text
            return 1

    monkeypatch.setattr(pub, "BotApiTransport", CapturingTransport)
    assert pub.notify_operator("привет") is True
    assert captured == {"token": "file-token-123", "chat": "42", "text": "привет"}


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


# --- Step 14: send-plan builder ---------------------------------------------
def test_build_send_ops():
    # image valid + single message that fits -> the message rides as the caption
    assert _build_send_ops(["short"], "/x/y.png", "cap") == [("photo", "/x/y.png", "short")]
    # image valid + message over the caption cap -> photo(short caption) then text
    long_msg = "x" * 1100
    assert _build_send_ops([long_msg], "/x/y.png", "cap") == [
        ("photo", "/x/y.png", "cap"),
        ("text", long_msg),
    ]
    # image valid + multiple messages -> photo(short caption) then each text
    assert _build_send_ops(["a", "b"], "/x/y.png", "cap") == [
        ("photo", "/x/y.png", "cap"),
        ("text", "a"),
        ("text", "b"),
    ]
    # no image -> all text
    assert _build_send_ops(["a", "b"], None, "cap") == [("text", "a"), ("text", "b")]


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


# --- Step 16: publish_next photo sending + degrade + record -----------------
def test_publish_single_message_fits_caption_sends_photo_only(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.upsert_draft("2026-07-14", ["short digest"])
    ledger.approve()
    png = _png(tmp_path)
    ledger.set_image_path("2026-07-14", str(png))
    transport = FakeTransport()
    result = publish_next(ledger, transport, "@chan")
    assert result["status"] == "posted"
    assert transport.photos == [("@chan", str(png), "short digest")]
    assert transport.calls == []  # no separate text message


def test_publish_long_message_sends_photo_then_text(tmp_path):
    ledger = _ledger(tmp_path)
    long_msg = "y" * 1200
    ledger.upsert_draft("2026-07-14", [long_msg])
    ledger.approve()
    png = _png(tmp_path)
    ledger.set_image_path("2026-07-14", str(png))
    transport = FakeTransport()
    result = publish_next(ledger, transport, "@chan")
    assert result["status"] == "posted"
    assert len(transport.photos) == 1
    assert len(transport.photos[0][2]) <= 1024  # short caption
    assert [c[1] for c in transport.calls] == [long_msg]  # full text AFTER the photo


def test_publish_text_only_when_no_image(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.upsert_draft("2026-07-14", ["m1", "m2"])
    ledger.approve()
    transport = FakeTransport()
    assert publish_next(ledger, transport, "@chan")["status"] == "posted"
    assert transport.photos == []
    assert [c[1] for c in transport.calls] == ["m1", "m2"]


def test_publish_degrades_to_text_when_leading_photo_fails(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.upsert_draft("2026-07-14", ["short digest"])
    ledger.approve()
    png = _png(tmp_path)
    ledger.set_image_path("2026-07-14", str(png))
    # PhotoNotSent = the photo PROVABLY never reached Telegram -> safe to degrade.
    transport = FakeTransport(fail_photo="not_sent")  # leading photo fails at sent==0
    result = publish_next(ledger, transport, "@chan")
    assert result["status"] == "posted"
    assert transport.photos == []  # photo never landed
    assert [c[1] for c in transport.calls] == ["short digest"]  # text out exactly once
    assert _row(tmp_path, "2026-07-14")["status"] == "posted"


def test_publish_leading_photo_ambiguous_failure_stays_stuck(tmp_path):
    # A caption-fit single-message digest rides ENTIRELY in the photo caption.
    # If send_photo fails AMBIGUOUSLY (Telegram may already hold the upload), the
    # publisher must NOT re-post the digest as text — that would double-post. It
    # freezes the row at 'sending' instead, blocking re-publish until a human
    # looks. This is the exact double-post guard.
    ledger = _ledger(tmp_path)
    ledger.upsert_draft("2026-07-14", ["short digest"])
    ledger.approve()
    png = _png(tmp_path)
    ledger.set_image_path("2026-07-14", str(png))
    transport = FakeTransport(fail_photo="ambiguous")  # possibly-received failure at sent==0
    result = publish_next(ledger, transport, "@chan")
    assert result["status"] == "failed"
    assert transport.calls == []  # NO text fallback -> no duplicate post
    assert _row(tmp_path, "2026-07-14")["status"] == "sending"  # stuck -> blocks
    # the stuck 'sending' row blocks all further publishing
    assert publish_next(ledger, FakeTransport(), "@chan")["status"] == "blocked"


def test_publish_stuck_when_text_fails_after_photo(tmp_path):
    ledger = _ledger(tmp_path)
    long_msg = "y" * 1200  # forces photo(short caption) + text
    ledger.upsert_draft("2026-07-14", [long_msg])
    ledger.approve()
    png = _png(tmp_path)
    ledger.set_image_path("2026-07-14", str(png))
    transport = FakeTransport(fail_at=0)  # photo ok, FIRST text send fails
    result = publish_next(ledger, transport, "@chan")
    assert result["status"] == "failed"
    assert len(transport.photos) == 1  # the photo already went to the channel
    assert _row(tmp_path, "2026-07-14")["status"] == "sending"  # stuck -> blocks
    assert publish_next(ledger, FakeTransport(), "@chan")["status"] == "blocked"


def test_publish_dry_run_mentions_image(tmp_path, capsys):
    ledger = _ledger(tmp_path)
    ledger.upsert_draft("2026-07-14", ["short digest"])
    ledger.approve()
    png = _png(tmp_path)
    ledger.set_image_path("2026-07-14", str(png))
    result = publish_next(ledger, None, None, dry_run=True)
    assert result["status"] == "dry-run"
    assert str(png) in capsys.readouterr().out  # dry-run references the image
    assert ledger.next_approved() is not None  # reverted, still approved
