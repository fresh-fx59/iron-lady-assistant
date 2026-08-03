"""`scripts/memory-manager` must select an interpreter that HAS the deps.

The box deploys `/var/lib/iron-lady` with no venv and runs the bot from a Nix
python env, so the wrapper's old "venv or else `command -v python3`" fallback
landed on a bare system python3 and died with `ModuleNotFoundError: yaml`.
Selecting the first interpreter that *exists* is not the same as selecting one
that *works*.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
WRAPPER = REPO_ROOT / "scripts" / "memory-manager"


def _stub_repo(tmp_path: Path) -> Path:
    """A checkout with the real wrapper and src, but deliberately no venv."""
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    shutil.copy(WRAPPER, repo / "scripts" / "memory-manager")
    (repo / "src").symlink_to(REPO_ROOT / "src")
    (repo / "memory").mkdir()
    return repo


def _broken_python_dir(tmp_path: Path) -> Path:
    """A PATH entry whose `python3` exists but cannot import the tool."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake = bin_dir / "python3"
    fake.write_text(
        "#!/usr/bin/env bash\n"
        'if [[ "$*" == *"import src.memory_tool"* ]]; then\n'
        '  echo "ModuleNotFoundError: No module named \'yaml\'" >&2\n'
        "  exit 1\n"
        "fi\n"
        "exit 1\n"
    )
    fake.chmod(0o755)
    return bin_dir


def _run(repo: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(repo / "scripts" / "memory-manager"), "list"],
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
    )


def _base_env() -> dict[str, str]:
    return {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("TELEGRAM_", "ALLOWED_", "PASSIVE_", "PYTHONPATH"))
    }


def test_skips_path_python_that_lacks_deps(tmp_path: Path) -> None:
    """A dep-less python3 on PATH must not be chosen when ILA_PYTHON works."""
    repo = _stub_repo(tmp_path)
    env = _base_env()
    env["PATH"] = f"{_broken_python_dir(tmp_path)}:{env.get('PATH', '')}"
    env["ILA_PYTHON"] = sys.executable

    result = _run(repo, env)
    assert result.returncode == 0, (
        f"wrapper failed despite a working ILA_PYTHON.\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    assert result.stdout.strip().startswith("[")


def test_a_bare_stdlib_python_is_enough(tmp_path: Path) -> None:
    """The box's scenario: no venv, no ILA_PYTHON, just the system python3.

    `src.memory_tool` is stdlib-only now (PyYAML moved behind the legacy-profile
    migration), so the interpreter that every NixOS box already has must run it.
    """
    repo = _stub_repo(tmp_path)
    env = _base_env()
    env.pop("ILA_PYTHON", None)
    system_python = shutil.which("python3", path="/run/current-system/sw/bin:/usr/bin:/bin")
    if not system_python:
        pytest.skip("no system python3 outside the test environment")
    env["PATH"] = f"{Path(system_python).parent}:{Path(shutil.which('bash') or '/bin/bash').parent}"

    result = _run(repo, env)
    assert result.returncode == 0, (
        f"a bare system python3 could not run the tool.\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    assert result.stdout.strip().startswith("[")


def test_reports_every_candidate_when_none_work(tmp_path: Path) -> None:
    """With no usable interpreter anywhere, say so and name what was tried."""
    repo = _stub_repo(tmp_path)
    env = _base_env()
    env.pop("ILA_PYTHON", None)
    # PATH holds exactly one directory: a broken python3 plus the bash the
    # wrapper's shebang needs. Nothing else may supply a working interpreter.
    only_dir = _broken_python_dir(tmp_path)
    for utility in ("bash", "dirname", "tail"):
        found = shutil.which(utility)
        assert found, f"{utility} is required to run the wrapper at all"
        (only_dir / utility).symlink_to(found)
    env["PATH"] = str(only_dir)

    result = _run(repo, env)
    assert result.returncode == 1
    assert "no Python interpreter here can import 'src.memory_tool'" in result.stderr
    # Each candidate is listed with the interpreter's OWN reason, so the reader
    # sees which dependency is actually missing instead of a generic verdict.
    assert "No module named 'yaml'" in result.stderr
    assert "python3 -m venv venv" in result.stderr
    # It must not have leaked a raw traceback instead.
    assert "Traceback" not in result.stderr


def test_ila_python_is_preferred_over_venv(tmp_path: Path) -> None:
    """The explicit override wins over a repo venv."""
    repo = _stub_repo(tmp_path)
    venv_bin = repo / "venv" / "bin"
    venv_bin.mkdir(parents=True)
    marker = tmp_path / "venv-was-used"
    fake = venv_bin / "python3"
    fake.write_text(f"#!/usr/bin/env bash\ntouch {marker}\nexit 1\n")
    fake.chmod(0o755)

    env = _base_env()
    env["ILA_PYTHON"] = sys.executable

    result = _run(repo, env)
    assert result.returncode == 0, result.stderr
    assert not marker.exists(), "venv python was probed even though ILA_PYTHON works"
