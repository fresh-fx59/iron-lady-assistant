"""setup.sh must run unattended, so the Ubuntu path is testable."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SETUP = REPO_ROOT / "setup.sh"


def _run(tmp_path: Path, **env_extra) -> subprocess.CompletedProcess[str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    shutil.copy(SETUP, repo / "setup.sh")
    shutil.copytree(REPO_ROOT / "contrib", repo / "contrib")
    (repo / "requirements.txt").write_text("aiogram>=3.22,<4\n", encoding="utf-8")

    env = {k: v for k, v in os.environ.items() if not k.startswith("ILA_SETUP_")}
    env.update(
        {
            "ILA_SETUP_BOT_TOKEN": "123456:FAKE",
            "ILA_SETUP_USER_IDS": "123456789",
            "ILA_SETUP_SKIP_DEPS": "1",
            "ILA_SETUP_SKIP_VENV": "1",
            "ILA_SETUP_SKIP_SYSTEMD": "1",
        }
    )
    env.update(env_extra)
    return subprocess.run(
        ["bash", "setup.sh", "--non-interactive"],
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )


def test_non_interactive_run_succeeds_and_writes_env(tmp_path: Path) -> None:
    result = _run(tmp_path)
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    env_file = tmp_path / "repo" / ".env"
    assert env_file.exists()
    written = env_file.read_text(encoding="utf-8")
    assert "TELEGRAM_BOT_TOKEN=123456:FAKE" in written
    assert "ALLOWED_USER_IDS=123456789" in written


def test_it_never_blocks_on_stdin(tmp_path: Path) -> None:
    """A prompt in non-interactive mode would hang CI; assert it does not."""
    result = _run(tmp_path)
    assert result.returncode == 0
    assert "Paste your bot token" not in result.stdout


def test_missing_required_value_fails_loudly(tmp_path: Path) -> None:
    result = _run(tmp_path, ILA_SETUP_BOT_TOKEN="")
    assert result.returncode != 0
    assert "ILA_SETUP_BOT_TOKEN" in (result.stdout + result.stderr)


def test_service_topology_is_written_to_env(tmp_path: Path) -> None:
    """The installer must record the unit names it chose, per config.BOT_SERVICE."""
    result = _run(tmp_path, ILA_SETUP_BOT_SERVICE="my-bot.service")
    assert result.returncode == 0, result.stdout + result.stderr
    written = (tmp_path / "repo" / ".env").read_text(encoding="utf-8")
    assert "ILA_BOT_SERVICE=my-bot.service" in written


def test_default_unit_name_matches_the_live_box(tmp_path: Path) -> None:
    result = _run(tmp_path)
    assert result.returncode == 0
    written = (tmp_path / "repo" / ".env").read_text(encoding="utf-8")
    assert "ILA_BOT_SERVICE=iron-lady-bot.service" in written


def test_unknown_distro_lists_packages_and_continues(tmp_path: Path) -> None:
    result = _run(tmp_path, ILA_SETUP_SKIP_DEPS="0", ILA_SETUP_FORCE_DISTRO="plan9")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "ffmpeg" in result.stdout
    assert "tesseract" in result.stdout
