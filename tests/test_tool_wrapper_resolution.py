"""A tool manifest must never make the agent *guess* where its wrapper lives.

`tools/memory-manager.yaml` told the agent to run

    "${ILA_REPO_ROOT:-$HOME/iron-lady-assistant}"/scripts/memory-manager

The box deploys the app to `$HOME` itself (`/var/lib/iron-lady/src`, HOME=
`/var/lib/iron-lady`) and never set `ILA_REPO_ROOT`, so the documented command
expanded to `/var/lib/iron-lady/iron-lady-assistant/scripts/memory-manager` —
"No such file or directory". memory-manager is an always-active tool, so that
dead path was injected into every single turn from 2026-03-23 onward and no
structured memory fact was written after 2026-06-05.

The fix is an input gate: the manifest declares `wrapper:` relative to the
deployment root, the registry resolves it to an absolute path at load time and
substitutes it into the instructions. The agent receives a path that exists;
there is nothing left to guess. A wrapper that cannot be resolved is an ERROR
in the log, not a silent dead command.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from src.plugins.tools_plugin import ToolRegistry

REPO_ROOT = Path(__file__).resolve().parent.parent
REAL_TOOLS_DIR = REPO_ROOT / "tools"


def _write_tool(tools_dir: Path, name: str, body: str) -> Path:
    tools_dir.mkdir(parents=True, exist_ok=True)
    path = tools_dir / f"{name}.yaml"
    path.write_text(body, encoding="utf-8")
    return path


def _stub_deployment(tmp_path: Path, *, create_wrapper: bool = True) -> Path:
    """A deployment root holding tools/ and (optionally) scripts/demo."""
    root = tmp_path / "deployment"
    _write_tool(
        root / "tools",
        "demo",
        "name: demo\n"
        "description: Demo tool.\n"
        "tier: core\n"
        "triggers: [demo]\n"
        "wrapper: scripts/demo\n"
        "instructions: |\n"
        "  Run `{{wrapper}} list` to list things.\n"
        "setup: '{{wrapper}} --help'\n",
    )
    if create_wrapper:
        scripts = root / "scripts"
        scripts.mkdir(parents=True)
        wrapper = scripts / "demo"
        wrapper.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
        wrapper.chmod(0o755)
    return root


def test_wrapper_placeholder_resolves_to_an_absolute_path(tmp_path: Path) -> None:
    root = _stub_deployment(tmp_path)
    registry = ToolRegistry(root / "tools")

    context = registry.build_context("demo please")

    expected = str(root / "scripts" / "demo")
    assert expected in context
    assert "{{wrapper}}" not in context


def test_resolved_wrapper_follows_the_deployment_root_not_the_home_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The regression: HOME must have no say in where the wrapper is."""
    root = _stub_deployment(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "somewhere-else"))
    monkeypatch.delenv("ILA_REPO_ROOT", raising=False)

    registry = ToolRegistry(root / "tools")
    definition = registry._load_full("demo")

    assert definition is not None
    assert definition.wrapper_path == root / "scripts" / "demo"
    assert Path(definition.wrapper_path).exists()


def test_missing_wrapper_is_logged_as_an_error(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A dead wrapper path must be loud at load time, never silently injected."""
    root = _stub_deployment(tmp_path, create_wrapper=False)
    registry = ToolRegistry(root / "tools")

    with caplog.at_level(logging.ERROR, logger="src.plugins.tools_plugin"):
        registry._load_full("demo")

    messages = [record.getMessage() for record in caplog.records]
    assert any("demo" in message and "scripts/demo" in message for message in messages), messages


def test_setup_field_gets_the_same_substitution(tmp_path: Path) -> None:
    root = _stub_deployment(tmp_path)
    registry = ToolRegistry(root / "tools")

    definition = registry._load_full("demo")

    assert definition is not None
    assert definition.setup_script is not None
    assert "{{wrapper}}" not in definition.setup_script
    assert str(root / "scripts" / "demo") in definition.setup_script


def test_tools_without_a_wrapper_are_untouched(tmp_path: Path) -> None:
    root = tmp_path / "deployment"
    _write_tool(
        root / "tools",
        "plain",
        "name: plain\n"
        "description: No wrapper here.\n"
        "triggers: [plain]\n"
        "instructions: |\n"
        "  Just prose, no wrapper.\n",
    )
    registry = ToolRegistry(root / "tools")

    definition = registry._load_full("plain")

    assert definition is not None
    assert definition.wrapper_path is None
    assert "Just prose" in definition.instructions


def test_shipped_memory_manager_manifest_resolves_in_this_checkout() -> None:
    """The real manifest, loaded from the real tools/ dir, must point at a
    wrapper that exists — the check that would have caught the outage."""
    registry = ToolRegistry(REAL_TOOLS_DIR)

    definition = registry._load_full("memory-manager")

    assert definition is not None
    assert definition.wrapper_path == REPO_ROOT / "scripts" / "memory-manager"
    assert definition.wrapper_path.exists()
    assert "{{wrapper}}" not in definition.instructions
    assert str(definition.wrapper_path) in definition.instructions


def test_shipped_memory_manager_manifest_never_guesses_a_root() -> None:
    """No `$HOME/...` or bare `ILA_REPO_ROOT` guessing may come back."""
    raw = (REAL_TOOLS_DIR / "memory-manager.yaml").read_text(encoding="utf-8")

    assert "$HOME/iron-lady-assistant" not in raw
    assert "ILA_REPO_ROOT" not in raw
