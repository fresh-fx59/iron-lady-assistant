"""tests/test_telegram_aggregator_publish.py"""
from __future__ import annotations

import json
import sqlite3

import pytest

from src.telegram_aggregator_gates import Story
from src.telegram_aggregator_publish import DigestLedger, publish_next, render_messages

FOOTER = "🤖 Дайджест: отбор автоматический, курирование вручную."


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
    def __init__(self, fail_at=None):
        self.calls = []
        self._fail_at = fail_at

    def send_message(self, chat, text):
        if self._fail_at is not None and len(self.calls) == self._fail_at:
            raise RuntimeError("boom")
        self.calls.append((chat, text))
        return len(self.calls)


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
    monkeypatch.delenv("IRONLADY_NOTIFY_BOT_TOKEN", raising=False)
    monkeypatch.setenv("IRONLADY_NOTIFY_BOT_TOKEN_FILE", str(token_file))
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
