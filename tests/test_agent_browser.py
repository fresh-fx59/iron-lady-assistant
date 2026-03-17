from __future__ import annotations

import json
from pathlib import Path

from src import agent_browser


def test_inspect_setup_linux_prefers_repo_local_when_assets_present(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    (repo / "node_modules" / ".bin").mkdir(parents=True)
    (repo / "node_modules" / ".bin" / "agent-browser").write_text("")
    (repo / "scripts").mkdir()
    monkeypatch.setattr(agent_browser, "_default_state_root", lambda: tmp_path / "state")
    monkeypatch.setattr(agent_browser.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(agent_browser.Path, "home", classmethod(lambda cls: tmp_path))
    (tmp_path / ".cache" / "ms-playwright").mkdir(parents=True)

    status = agent_browser.inspect_setup(repo_root=repo, platform_name="linux")

    assert status.ok is True
    assert status.recommended_path == "linux_repo_local"
    assert "python3 -m src.agent_browser open https://example.com" in status.commands


def test_main_snapshot_returns_json_payload(monkeypatch, capsys) -> None:
    monkeypatch.setattr(agent_browser, "_resolve_config", lambda args: object())

    class _FakeBrowser:
        def __init__(self, _config: object) -> None:
            pass

        def run(self, *args: str) -> str:
            assert args == ("snapshot",)
            return "snapshot-ok"

    monkeypatch.setattr(agent_browser, "AgentBrowser", _FakeBrowser)

    rc = agent_browser.main(["snapshot"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"ok": True, "command": "snapshot", "output": "snapshot-ok"}


def test_main_raw_requires_arguments(capsys) -> None:
    rc = agent_browser.main(["raw"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert "raw requires at least one argument" in payload["error"]
