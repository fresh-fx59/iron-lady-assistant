"""The offline memory CLIs must run without any Telegram bot configuration.

`scripts/memory-manager` and its siblings only read and write files under
`memory/`. They used to `from . import config`, which loads `.env` via
`python-dotenv` and `sys.exit(1)`s when `TELEGRAM_BOT_TOKEN` is unset — so the
CLI died in every environment that had no bot token (and with a bare
`ModuleNotFoundError: dotenv` wherever python-dotenv was not installed).

These run in a subprocess with a scrubbed environment on purpose: the autouse
`clean_env` fixture in conftest.py sets a fake TELEGRAM_BOT_TOKEN, which is
exactly what hid this coupling from the rest of the suite.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

OFFLINE_TOOLS = [
    "src.memory_tool",
    "src.worklog_tool",
    "src.summary_inspector_tool",
    "src.lifecycle_tool",
]


def _bare_env() -> dict[str, str]:
    """Environment with every bot variable removed."""
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("TELEGRAM_", "ALLOWED_", "PASSIVE_"))
    }
    env["PYTHONPATH"] = str(REPO_ROOT)
    # A stray .env next to the repo must not resurrect the token either.
    env["DOTENV_PATH"] = "/nonexistent"
    return env


@pytest.mark.parametrize("module", OFFLINE_TOOLS)
def test_offline_tool_imports_without_bot_token(module: str) -> None:
    """Importing an offline CLI must not require TELEGRAM_BOT_TOKEN."""
    result = subprocess.run(
        [sys.executable, "-c", f"import {module}"],
        cwd=REPO_ROOT,
        env=_bare_env(),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"{module} could not be imported without bot config.\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )


@pytest.mark.parametrize("module", ["src.memory_tool", "src.worklog_tool"])
def test_offline_tool_help_without_bot_token(module: str) -> None:
    """`--help` must work without bot config — it builds the argparse defaults."""
    result = subprocess.run(
        [sys.executable, "-m", module, "--help"],
        cwd=REPO_ROOT,
        env=_bare_env(),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"{module} --help failed without bot config.\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    assert "--memory-dir" in result.stdout


def test_memory_tool_list_without_bot_token(tmp_path: Path) -> None:
    """A real command end-to-end: list an empty memory dir, no bot config."""
    memory_dir = tmp_path / "memory"
    memory_dir.mkdir()
    result = subprocess.run(
        [sys.executable, "-m", "src.memory_tool", "--memory-dir", str(memory_dir), "list"],
        cwd=REPO_ROOT,
        env=_bare_env(),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"memory_tool list failed without bot config.\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    assert result.stdout.strip().startswith("[")


def test_memory_dir_default_matches_config() -> None:
    """The config-free resolver and config.MEMORY_DIR must never drift apart."""
    from src import memory_paths

    os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token-12345")
    from src import config

    assert memory_paths.resolve_memory_dir() == config.MEMORY_DIR


def test_memory_dir_honours_env(monkeypatch, tmp_path: Path) -> None:
    """MEMORY_DIR env var still wins, and ~ is expanded."""
    from src import memory_paths

    monkeypatch.setenv("MEMORY_DIR", str(tmp_path / "elsewhere"))
    assert memory_paths.resolve_memory_dir() == tmp_path / "elsewhere"

    monkeypatch.setenv("MEMORY_DIR", "~/mem")
    assert memory_paths.resolve_memory_dir() == Path(os.path.expanduser("~/mem"))

    monkeypatch.delenv("MEMORY_DIR")
    assert memory_paths.resolve_memory_dir() == Path("memory")
