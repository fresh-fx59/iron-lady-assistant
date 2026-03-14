from __future__ import annotations

import json
from pathlib import Path

import pytest
from aiohttp.test_utils import TestClient, TestServer

from src import browser_takeover


def test_build_setup_payload_installs_extension(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    bundled = tmp_path / "bundled"
    bundled.mkdir()
    (bundled / "manifest.json").write_text('{"manifest_version":3}', encoding="utf-8")
    (bundled / "background.js").write_text("console.log('ok')\n", encoding="utf-8")

    monkeypatch.setattr(browser_takeover, "_default_state_root", lambda: tmp_path / "state")
    monkeypatch.setattr(browser_takeover, "_repo_root", lambda: tmp_path / "repo")
    monkeypatch.setattr(browser_takeover, "_bundled_extension_dir", lambda: bundled)

    payload = browser_takeover.build_setup_payload()

    assert payload["ok"] is True
    assert Path(payload["extension_path"]).exists()
    assert payload["token"]
    assert payload["relay_url"] == "http://127.0.0.1:18792"
    assert payload["relay_ws_url"] == "ws://127.0.0.1:18792"


def test_build_setup_payload_prefers_public_base_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    bundled = tmp_path / "bundled"
    bundled.mkdir()
    (bundled / "manifest.json").write_text('{"manifest_version":3}', encoding="utf-8")
    (bundled / "background.js").write_text("console.log('ok')\n", encoding="utf-8")

    monkeypatch.setattr(browser_takeover, "_default_state_root", lambda: tmp_path / "state")
    monkeypatch.setattr(browser_takeover, "_repo_root", lambda: tmp_path / "repo")
    monkeypatch.setattr(browser_takeover, "_bundled_extension_dir", lambda: bundled)
    browser_takeover._save_settings(
        browser_takeover.RelaySettings(token="secret", public_base_url="https://ila.example/browser")
    )

    payload = browser_takeover.build_setup_payload()

    assert payload["relay_url"] == "https://ila.example/browser"
    assert payload["relay_ws_url"] == "wss://ila.example/browser"


def test_main_targets_reports_relay_errors(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    monkeypatch.setattr(browser_takeover, "_request_json", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("relay down")))

    rc = browser_takeover.main(["targets"])

    assert rc == 1
    assert "relay down" in capsys.readouterr().out


def test_main_snapshot_formats_runtime_evaluate(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    monkeypatch.setattr(
        browser_takeover,
        "_request_json",
        lambda *args, **kwargs: {
            "ok": True,
            "result": {
                "result": {
                    "value": {
                        "title": "Demo",
                        "url": "https://example.com",
                        "text": "hello world",
                    }
                }
            },
        },
    )

    rc = browser_takeover.main(["snapshot", "--tab-id", "7", "--format", "json"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["snapshot"]["title"] == "Demo"


def test_main_navigate_returns_success(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    monkeypatch.setattr(
        browser_takeover,
        "_request_json",
        lambda *args, **kwargs: {"ok": True, "result": {"frameId": "123"}},
    )

    rc = browser_takeover.main(["navigate", "--tab-id", "7", "--url", "https://example.com"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["url"] == "https://example.com"


def test_build_extension_ws_url_uses_public_base_url() -> None:
    settings = browser_takeover.RelaySettings(token="secret", public_base_url="https://ila.example/browser")

    assert (
        browser_takeover._build_extension_ws_url(settings)
        == "wss://ila.example/browser/extension?token=secret"
    )


def test_main_click_returns_success(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    monkeypatch.setattr(
        browser_takeover,
        "_request_json",
        lambda *args, **kwargs: {
            "ok": True,
            "result": {
                "result": {
                    "value": {"ok": True, "selector": "#login"},
                }
            },
        },
    )

    rc = browser_takeover.main(["click", "--tab-id", "7", "--selector", "#login"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["selector"] == "#login"


def test_main_type_returns_success(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    monkeypatch.setattr(
        browser_takeover,
        "_request_json",
        lambda *args, **kwargs: {
            "ok": True,
            "result": {
                "result": {
                    "value": {"ok": True, "selector": "input[name=q]", "textLength": 4, "submitted": True},
                }
            },
        },
    )

    rc = browser_takeover.main(
        ["type", "--tab-id", "7", "--selector", "input[name=q]", "--text", "ozon", "--submit"]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["selector"] == "input[name=q]"
    assert payload["submitted"] is True


@pytest.mark.asyncio
async def test_targets_requires_auth(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(browser_takeover, "_default_state_root", lambda: tmp_path / "state")
    settings = browser_takeover.RelaySettings(token="secret")
    app = browser_takeover.create_app(settings)
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()
    try:
        resp = await client.get("/targets")
        assert resp.status == 401
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_targets_returns_attached_tabs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(browser_takeover, "_default_state_root", lambda: tmp_path / "state")
    settings = browser_takeover.RelaySettings(token="secret")
    app = browser_takeover.create_app(settings)
    app[browser_takeover.RELAY_STATE_KEY].tabs[7] = browser_takeover.AttachedTab(
        tab_id=7, title="Demo", url="https://example.com", attached=True
    )
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()
    try:
        resp = await client.get("/targets", headers={"Authorization": "Bearer secret"})
        assert resp.status == 200
        payload = await resp.json()
        assert payload["targets"][0]["tab_id"] == 7
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_targets_accept_public_path_prefix(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(browser_takeover, "_default_state_root", lambda: tmp_path / "state")
    settings = browser_takeover.RelaySettings(token="secret", public_base_url="https://ila.example/browser-takeover")
    app = browser_takeover.create_app(settings)
    app[browser_takeover.RELAY_STATE_KEY].tabs[9] = browser_takeover.AttachedTab(
        tab_id=9, title="Prefixed", url="https://example.com", attached=True
    )
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()
    try:
        resp = await client.get("/browser-takeover/targets", headers={"Authorization": "Bearer secret"})
        assert resp.status == 200
        payload = await resp.json()
        assert payload["targets"][0]["tab_id"] == 9
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_cdp_requires_connected_extension(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(browser_takeover, "_default_state_root", lambda: tmp_path / "state")
    settings = browser_takeover.RelaySettings(token="secret")
    app = browser_takeover.create_app(settings)
    app[browser_takeover.RELAY_STATE_KEY].tabs[5] = browser_takeover.AttachedTab(
        tab_id=5, title="Demo", url="https://example.com", attached=True
    )
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()
    try:
        resp = await client.post(
            "/cdp",
            headers={"Authorization": "Bearer secret"},
            json={"tab_id": 5, "method": "Page.navigate", "params": {"url": "https://example.com"}},
        )
        assert resp.status == 409
        assert "extension_not_connected" in await resp.text()
    finally:
        await client.close()
