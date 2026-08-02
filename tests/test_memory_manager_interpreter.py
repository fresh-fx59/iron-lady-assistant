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
    """A PATH entry whose `python3` exists but cannot import yaml."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake = bin_dir / "python3"
    fake.write_text(
        "#!/usr/bin/env bash\n"
        'if [[ "$*" == *"import yaml"* ]]; then\n'
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


def test_reports_every_candidate_when_none_work(tmp_path: Path) -> None:
    """With no usable interpreter anywhere, say so and name what was tried."""
    repo = _stub_repo(tmp_path)
    env = _base_env()
    # Keep bash reachable (the wrapper's shebang needs it) but drop every
    # directory that might hold an interpreter with the deps installed.
    bash_dir = Path(shutil.which("bash") or "/bin/bash").parent
    env["PATH"] = f"{_broken_python_dir(tmp_path)}:{bash_dir}"

    result = _run(repo, env)
    assert result.returncode == 1
    assert "no Python interpreter here can import 'yaml'" in result.stderr
    assert "missing deps" in result.stderr
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
