from __future__ import annotations

import json
import os
from pathlib import Path

from src.providers import (
    Provider,
    ProviderManager,
    _expand_env_values,
    _normalized_subprocess_path,
)


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


def test_expand_env_values_expands_from_environ(monkeypatch) -> None:
    monkeypatch.setenv("ILA_TEST_SECRET", "topsecret")
    out = _expand_env_values(
        {"TOKEN": "${ILA_TEST_SECRET}", "PLAIN": "literal", "NUM": 1}
    )
    assert out["TOKEN"] == "topsecret"
    assert out["PLAIN"] == "literal"
    assert out["NUM"] == 1


def test_expand_env_values_leaves_unset_reference_verbatim(monkeypatch) -> None:
    monkeypatch.delenv("ILA_DEFINITELY_UNSET", raising=False)
    out = _expand_env_values({"TOKEN": "${ILA_DEFINITELY_UNSET}"})
    assert out["TOKEN"] == "${ILA_DEFINITELY_UNSET}"


def test_repo_opus_linkapi_provider_is_first_fallback_on_proxy() -> None:
    data = json.loads((Path(__file__).resolve().parents[1] / "providers.json").read_text())
    names = [p["name"] for p in data["providers"]]
    # codex primary, opus-linkapi is the first fallback.
    assert names[0] == "codex"
    assert names[1] == "opus-linkapi"

    opus = data["providers"][1]
    assert opus["cli"] == "claude"
    assert opus["model"] == "claude-opus-4-8"
    assert opus["env"]["ANTHROPIC_BASE_URL"] == "http://127.0.0.1:8317"
    assert opus["env"]["ILA_CLAUDE_MODEL"] == "claude-opus-4-8"
    # Secret must be referenced from env, never hardcoded in git.
    assert opus["env"]["ANTHROPIC_AUTH_TOKEN"] == "${OPENAI_API_KEY}"


def test_load_config_expands_opus_linkapi_token(monkeypatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "proxy-key-xyz")
    manager = ProviderManager(watch_config=False)
    opus = next(p for p in manager.providers if p.name == "opus-linkapi")
    assert opus.env["ANTHROPIC_AUTH_TOKEN"] == "proxy-key-xyz"
    assert opus.model == "claude-opus-4-8"
