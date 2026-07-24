"""tests/test_telegram_aggregator_tool.py — CLI wiring on temp state dirs."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from src import telegram_aggregator_tool
from src.telegram_aggregator_gates import Story
from src.telegram_aggregator_image import ImageGenError
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


def test_gate_non_dict_input_json_is_input_error(state, capsys):
    input_path = state / "drafts" / "list-input.json"
    input_path.parent.mkdir(parents=True, exist_ok=True)
    input_path.write_text(json.dumps([1, 2, 3]))
    draft_path = _write_draft(state, _stories(1))
    rc = main(["gate", "--draft", str(draft_path), "--input", str(input_path), "--date", "2026-07-14"])
    assert rc == 1
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 1
    out = json.loads(lines[0])
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


def test_gate_single_message_trims_and_auto_approves(state, capsys):
    """One-post-per-day rule (operator 2026-07-15): oversized drafts trim from
    the importance-ordered tail to fit ONE message; --auto-approve lands the
    ledger row already approved."""
    links = [(f"https://t.me/chan/{i}", "длинный исходный текст про ИИ " * 10) for i in range(12)]
    input_path = _write_input(state, links)
    stories = [
        {"headline": f"Сюжет {i}", "summary": "с" * 340, "source_links": [f"https://t.me/chan/{i}"]}
        for i in range(12)
    ]
    draft_path = _write_draft(state, stories)
    rc = main(["gate", "--draft", str(draft_path), "--input", str(input_path),
               "--date", "2026-07-14", "--auto-approve"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["status"] == "approved"
    assert out["messages"] == 1
    assert out["stories"] + out["trimmed_to_fit"] == 12
    assert out["trimmed_to_fit"] > 0
    ledger = DigestLedger(state / "ledger.db")
    item = ledger.next_approved()
    assert item is not None and item[0] == "2026-07-14"
    assert len(item[1]) == 1 and len(item[1][0]) <= 4000


def test_gate_trim_below_three_is_allowed_and_rerun_after_posted_reports_final(state, capsys):
    """The one-message rule outranks the 3-story gate floor (intentional); a
    same-day re-run after publishing must not lie about approving."""
    links = [(f"https://t.me/chan/{i}", "длинный исходный текст про ИИ " * 10) for i in range(3)]
    input_path = _write_input(state, links)
    huge = [
        {"headline": ("Сюжет " + str(i) + " х" * 100)[:118], "summary": "с" * 399,
         "source_links": [f"https://t.me/chan/{i}"]}
        for i in range(3)
    ]
    # Inflate per-story size via max links? Keep single link; force trim by many stories instead.
    many = [
        {"headline": f"Сюжет {i}", "summary": "с" * 399, "source_links": ["https://t.me/chan/0"]}
        for i in range(3)
    ]
    draft_path = _write_draft(state, many * 4)  # 12 near-max stories -> must trim hard
    rc = main(["gate", "--draft", str(draft_path), "--input", str(input_path),
               "--date", "2026-07-14", "--auto-approve"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["messages"] == 1 and out["status"] == "approved"

    # simulate published, then a re-run: upsert no-ops, approve returns None
    ledger = DigestLedger(state / "ledger.db")
    key, _ = ledger.next_approved()
    assert ledger.begin_send(key)
    ledger.mark_posted(key)
    rc = main(["gate", "--draft", str(draft_path), "--input", str(input_path),
               "--date", "2026-07-14", "--auto-approve"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["status"] == "already-final"


# ===========================================================================
# Feature A2 — gate generates + persists the infographic (never blocks)
# ===========================================================================
def _enable_image(state, monkeypatch):
    key_file = state.parent / "img_key"
    key_file.write_text("img-key\n")
    monkeypatch.setenv("AGGREGATOR_IMAGE_ENABLED", "1")
    monkeypatch.setenv("AGGREGATOR_IMAGE_KEY_FILE", str(key_file))


def test_gate_generates_and_persists_image(state, monkeypatch, capsys):
    _enable_image(state, monkeypatch)

    calls = {}

    def fake_gen(headlines, out_path, *, key_file, date_label, **kw):
        calls["headlines"] = list(headlines)
        calls["date_label"] = date_label
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"\x89PNG\r\n\x1a\nfake-png")
        return out_path

    monkeypatch.setattr(telegram_aggregator_tool, "generate_digest_image", fake_gen)

    input_path = _write_input(state, [("https://t.me/chan/1", "длинный исходный текст " * 10)])
    draft_path = _write_draft(state, _stories(3))
    rc = main(["gate", "--draft", str(draft_path), "--input", str(input_path), "--date", "2026-07-14"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["image"] is True
    assert calls["headlines"]  # RU headlines steer the prompt
    assert calls["date_label"] == "14.07.2026"
    ledger = DigestLedger(state / "ledger.db")
    img = ledger.image_path_for("2026-07-14")
    assert img is not None and Path(img).exists()


def test_gate_image_failure_does_not_block_digest(state, monkeypatch, capsys):
    _enable_image(state, monkeypatch)

    def boom(*a, **k):
        raise ImageGenError("broker down")

    monkeypatch.setattr(telegram_aggregator_tool, "generate_digest_image", boom)

    input_path = _write_input(state, [("https://t.me/chan/1", "длинный исходный текст " * 10)])
    draft_path = _write_draft(state, _stories(3))
    rc = main(["gate", "--draft", str(draft_path), "--input", str(input_path), "--date", "2026-07-14"])
    assert rc == 0  # image failure must NOT fail the gate
    out = json.loads(capsys.readouterr().out)
    assert out["status"] == "pending"
    assert out["image"] is False
    ledger = DigestLedger(state / "ledger.db")
    assert ledger.image_path_for("2026-07-14") is None  # text-only fallback


def test_gate_skips_image_when_day_already_posted(state, monkeypatch, capsys):
    """FIX 4b: a gate re-run on an already-'posted' day must NOT burn a gpt-image
    call — upsert_draft is a no-op for a posted row, so nothing new can publish
    and the image would never be used."""
    _enable_image(state, monkeypatch)

    # Drive the day to 'posted' first, directly through the ledger.
    ledger = DigestLedger(state / "ledger.db")
    ledger.upsert_draft("2026-07-14", ["already posted digest"])
    ledger.approve("2026-07-14")
    ledger.begin_send("2026-07-14")
    ledger.mark_posted("2026-07-14")

    def spy(*a, **k):
        raise AssertionError("image gen must not run for an already-posted day")

    monkeypatch.setattr(telegram_aggregator_tool, "generate_digest_image", spy)

    input_path = _write_input(state, [("https://t.me/chan/1", "длинный исходный текст " * 10)])
    draft_path = _write_draft(state, _stories(3))
    rc = main(["gate", "--draft", str(draft_path), "--input", str(input_path), "--date", "2026-07-14"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["image"] is False  # no image generated for a final day
    assert ledger.status_for("2026-07-14") == "posted"  # row untouched
    assert ledger.image_path_for("2026-07-14") is None


def test_gate_no_image_when_disabled(state, monkeypatch, capsys):
    monkeypatch.delenv("AGGREGATOR_IMAGE_ENABLED", raising=False)

    def spy(*a, **k):
        raise AssertionError("generate_digest_image must not be called when disabled")

    monkeypatch.setattr(telegram_aggregator_tool, "generate_digest_image", spy)

    input_path = _write_input(state, [("https://t.me/chan/1", "длинный исходный текст " * 10)])
    draft_path = _write_draft(state, _stories(3))
    rc = main(["gate", "--draft", str(draft_path), "--input", str(input_path), "--date", "2026-07-14"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["image"] is False
    ledger = DigestLedger(state / "ledger.db")
    assert ledger.image_path_for("2026-07-14") is None
