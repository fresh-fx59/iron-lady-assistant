"""tests/test_telegram_aggregator.py"""
from __future__ import annotations

import os
from pathlib import Path

from src.telegram_aggregator import load_file_env, parse_sources, resolve_paths


def test_resolve_paths_defaults(monkeypatch, tmp_path):
    monkeypatch.setenv("AGGREGATOR_STATE_DIR", str(tmp_path / "agg"))
    paths = resolve_paths()
    assert paths.state_dir == tmp_path / "agg"
    assert paths.db_path == tmp_path / "agg" / "aggregator.db"
    assert paths.sources_path == tmp_path / "agg" / "sources.txt"
    assert paths.drafts_dir == tmp_path / "agg" / "drafts"
    assert paths.drafts_dir.is_dir()  # created


def test_resolve_paths_sources_override(monkeypatch, tmp_path):
    monkeypatch.setenv("AGGREGATOR_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("AGGREGATOR_SOURCES_PATH", str(tmp_path / "x" / "list.txt"))
    assert resolve_paths().sources_path == tmp_path / "x" / "list.txt"


def test_load_file_env_reads_files(monkeypatch, tmp_path):
    secret = tmp_path / "k"
    secret.write_text("sekret-value\n")
    monkeypatch.delenv("TELEGRAM_PROXY_API_KEY", raising=False)
    monkeypatch.setenv("TELEGRAM_PROXY_API_KEY_FILE", str(secret))
    env: dict[str, str] = dict(os.environ)
    load_file_env(env)
    assert env["TELEGRAM_PROXY_API_KEY"] == "sekret-value"


def test_load_file_env_never_overwrites_existing(monkeypatch, tmp_path):
    secret = tmp_path / "k"
    secret.write_text("from-file")
    env = {"TELEGRAM_PROXY_API_KEY": "already", "TELEGRAM_PROXY_API_KEY_FILE": str(secret)}
    load_file_env(env)
    assert env["TELEGRAM_PROXY_API_KEY"] == "already"


def test_load_file_env_missing_file_is_noop(tmp_path):
    env = {"TELEGRAM_AGGREGATOR_BOT_TOKEN_FILE": str(tmp_path / "absent")}
    load_file_env(env)
    assert "TELEGRAM_AGGREGATOR_BOT_TOKEN" not in env


def test_parse_sources_formats_and_dedup():
    text = """
    # AI channels
    @data_secrets
    https://t.me/neuraldeep
    t.me/llm_under_hood
    data_secrets

    https://t.me/+privateHashAAA   # invite links are NOT usernames -> skipped
    """
    assert parse_sources(text) == ["data_secrets", "neuraldeep", "llm_under_hood"]
