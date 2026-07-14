"""tests/test_telegram_aggregator_publish.py"""
from __future__ import annotations

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
