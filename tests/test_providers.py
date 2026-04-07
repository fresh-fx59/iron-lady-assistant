from __future__ import annotations

import json
import os
from pathlib import Path

from src.providers import Provider, ProviderManager, _normalized_subprocess_path


def test_normalized_subprocess_path_keeps_existing_and_adds_system_bins(tmp_path) -> None:
    custom_bin = tmp_path / "custom-bin"
    custom_bin.mkdir()
    path = _normalized_subprocess_path(str(custom_bin))

    parts = path.split(os.pathsep)
    repo_scripts = str(Path(__file__).resolve().parents[1] / "scripts")
    assert repo_scripts in parts
    assert str(custom_bin) in parts
    assert "/usr/local/bin" in parts
    assert "/usr/bin" in parts
    assert "/bin" in parts


def test_provider_manager_subprocess_env_normalizes_path_and_preserves_provider_env(monkeypatch, tmp_path) -> None:
    custom_bin = tmp_path / "custom-bin"
    custom_bin.mkdir()
    monkeypatch.setenv("PATH", str(custom_bin))
    manager = ProviderManager(watch_config=False)
    provider = Provider(name="demo", description="demo", env={"DEMO_ENV": "1"})

    env = manager.subprocess_env(provider)

    assert env["DEMO_ENV"] == "1"
    assert env["ILA_REPO_ROOT"] == str(Path(__file__).resolve().parents[1])
    assert "/usr/local/bin" in env["PATH"].split(os.pathsep)
    assert str(custom_bin) in env["PATH"].split(os.pathsep)


def test_repo_codex_provider_uses_native_codex_cli_config() -> None:
    providers_path = Path(__file__).resolve().parents[1] / "providers.json"
    data = json.loads(providers_path.read_text())

    codex_provider = next(provider for provider in data["providers"] if provider["name"] == "codex")

    assert codex_provider["cli"] == "codex"
    assert codex_provider.get("env", {}) == {}
