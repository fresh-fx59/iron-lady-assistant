"""Install on a clean Ubuntu and prove the app imports.

This is what makes the Ubuntu claim a fact instead of an aspiration.
Marked slow: it pulls an image and installs packages.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(not shutil.which("podman"), reason="podman not installed"),
]

SCRIPT = """
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq python3 python3-venv python3-pip sudo >/dev/null
cd /repo
export ILA_SETUP_BOT_TOKEN=123456:FAKE
export ILA_SETUP_USER_IDS=123456789
export ILA_SETUP_SKIP_SYSTEMD=1
bash setup.sh --non-interactive
./venv/bin/python -c "import src.main; print('IMPORT-OK')"
./venv/bin/python contrib/systemd/render.py contrib/systemd/iron-lady-bot.service.in \
  --user ila --dir /repo --python /repo/venv/bin/python --envfile /repo/.env
"""


def test_clean_ubuntu_install(tmp_path: Path) -> None:
    work = tmp_path / "repo"
    subprocess.run(
        ["git", "clone", "--depth", "1", "file://" + str(REPO_ROOT), str(work)],
        check=True,
        capture_output=True,
    )
    result = subprocess.run(
        [
            "podman",
            "run",
            "--rm",
            "-v",
            f"{work}:/repo:Z",
            "docker.io/library/ubuntu:24.04",
            "bash",
            "-lc",
            SCRIPT,
        ],
        capture_output=True,
        text=True,
        timeout=2400,
    )
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    assert "IMPORT-OK" in result.stdout
    assert "ExecStart=/repo/venv/bin/python -m src.main" in result.stdout


def test_system_packages_actually_installed(tmp_path: Path) -> None:
    """The bot shells out to these; assert the installer really provides them."""
    work = tmp_path / "repo"
    subprocess.run(
        ["git", "clone", "--depth", "1", "file://" + str(REPO_ROOT), str(work)],
        check=True,
        capture_output=True,
    )
    script = SCRIPT + """
for bin in ffmpeg tesseract espeak-ng rg git; do
  command -v "$bin" >/dev/null || { echo "MISSING:$bin"; exit 1; }
done
echo 'BINARIES-OK'
"""
    result = subprocess.run(
        [
            "podman",
            "run",
            "--rm",
            "-v",
            f"{work}:/repo:Z",
            "docker.io/library/ubuntu:24.04",
            "bash",
            "-lc",
            script,
        ],
        capture_output=True,
        text=True,
        timeout=2400,
    )
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    assert "BINARIES-OK" in result.stdout
