"""tests/test_telegram_aggregator_tool.py — CLI wiring on temp state dirs."""
from __future__ import annotations

import json

import pytest

from src import telegram_aggregator_tool
from src.telegram_aggregator_gates import Story
from src.telegram_aggregator_publish import DigestLedger
from src.telegram_aggregator_tool import main


@pytest.fixture()
def state(monkeypatch, tmp_path):
    monkeypatch.setenv("AGGREGATOR_STATE_DIR", str(tmp_path / "state"))
    return tmp_path / "state"


def _write_input(state, links_texts):
    payload = {
        "date": "2026-07-14",
        "window_hours": 24,
        "posts": [
            {"channel": "A", "username": "chan", "link": l, "text": t,
             "views": 1, "forwards": 0, "posted_at": "2026-07-14T10:00:00+00:00"}
            for l, t in links_texts
        ],
    }
    path = state / "drafts" / "2026-07-14-input.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False))
    return path


def _write_draft(state, stories):
    path = state / "drafts" / "2026-07-14-draft.json"
    path.write_text(json.dumps({"stories": stories}, ensure_ascii=False))
    return path


def _stories(n, link="https://t.me/chan/1"):
    return [
        {"headline": f"Сюжет {i}", "summary": "Своими словами о событии дня.",
         "source_links": [link]}
        for i in range(n)
    ]


def test_gate_ok_writes_pending_ledger(state, capsys):
    input_path = _write_input(state, [("https://t.me/chan/1", "длинный исходный текст " * 10)])
    draft_path = _write_draft(state, _stories(3))
    rc = main(["gate", "--draft", str(draft_path), "--input", str(input_path), "--date", "2026-07-14"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["status"] == "pending" and out["stories"] == 3
    ledger = DigestLedger(state / "ledger.db")
    assert ledger.approve("2026-07-14") == "2026-07-14"


def test_gate_failure_exits_nonzero(state, capsys):
    input_path = _write_input(state, [("https://t.me/chan/1", "текст")])
    draft_path = _write_draft(state, _stories(3, link="https://t.me/other/999"))  # unknown links
    rc = main(["gate", "--draft", str(draft_path), "--input", str(input_path), "--date", "2026-07-14"])
    assert rc == 1


def test_approve_and_publish_dry_run(state, capsys):
    input_path = _write_input(state, [("https://t.me/chan/1", "длинный исходный текст " * 10)])
    draft_path = _write_draft(state, _stories(3))
    main(["gate", "--draft", str(draft_path), "--input", str(input_path), "--date", "2026-07-14"])
    capsys.readouterr()
    assert main(["approve"]) == 0
    rc = main(["publish", "--dry-run"])
    assert rc == 0
    assert "dry-run" in capsys.readouterr().out


def test_gate_malformed_input_json_is_input_error(state, capsys):
    input_path = state / "drafts" / "bad-input.json"
    input_path.parent.mkdir(parents=True, exist_ok=True)
    input_path.write_text("{not valid json")
    draft_path = _write_draft(state, _stories(1))
    rc = main(["gate", "--draft", str(draft_path), "--input", str(input_path), "--date", "2026-07-14"])
    assert rc == 1
    out = json.loads(capsys.readouterr().out)
    assert out["status"] == "input-error"


def test_gate_missing_draft_file_exits_nonzero(state, capsys):
    input_path = _write_input(state, [("https://t.me/chan/1", "длинный исходный текст " * 10)])
    missing_draft = state / "drafts" / "does-not-exist.json"
    rc = main(["gate", "--draft", str(missing_draft), "--input", str(input_path), "--date", "2026-07-14"])
    assert rc == 1


def test_gate_bad_date_is_input_error(state, capsys):
    input_path = _write_input(state, [("https://t.me/chan/1", "длинный исходный текст " * 10)])
    draft_path = _write_draft(state, _stories(3))
    rc = main(["gate", "--draft", str(draft_path), "--input", str(input_path), "--date", "garbage"])
    assert rc == 1
    out = json.loads(capsys.readouterr().out)
    assert out["status"] == "input-error"


def test_collect_passes_file_delivered_api_key_to_proxy_client(state, monkeypatch, capsys):
    monkeypatch.delenv("TELEGRAM_PROXY_API_KEY", raising=False)
    key_file = state.parent / "proxy_api_key"
    key_file.write_text("file-delivered-secret\n")
    monkeypatch.setenv("TELEGRAM_PROXY_API_KEY_FILE", str(key_file))

    sources_path = state / "sources.txt"
    sources_path.parent.mkdir(parents=True, exist_ok=True)
    sources_path.write_text("@some_channel\n")

    captured: dict = {}

    class _FakeProxyClient:
        def __init__(self, *, base_url=None, api_key=None, timeout_seconds=None):
            captured["base_url"] = base_url
            captured["api_key"] = api_key

        async def list_channels(self, *, limit):
            return []

    monkeypatch.setattr(telegram_aggregator_tool, "TelegramProxyClient", _FakeProxyClient)

    rc = main(["collect"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["status"] == "ok"
    assert out["resolved"] == 0
    assert captured["api_key"] == "file-delivered-secret"
