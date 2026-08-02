"""Unit templates must render completely and be valid systemd."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

CONTRIB = Path(__file__).resolve().parent.parent / "contrib" / "systemd"
sys.path.insert(0, str(CONTRIB))

TEMPLATES = [
    "iron-lady-bot.service.in",
    "telegram-scheduler.service.in",
    "gmail-gateway.service.in",
    "telegram-proxy@.service.in",
]

VALUES = {
    "USER": "iron-lady",
    "DIR": "/var/lib/iron-lady",
    "PYTHON": "/var/lib/iron-lady/venv/bin/python",
    "ENVFILE": "/var/lib/iron-lady/.env",
}


@pytest.mark.parametrize("name", TEMPLATES)
def test_every_placeholder_is_substituted(name: str) -> None:
    from render import render

    out = render(CONTRIB / name, VALUES)
    assert "@" not in out.replace("telegram-proxy@", "").replace("%i", ""), (
        f"{name} still contains an unsubstituted @PLACEHOLDER@:\n{out}"
    )
    assert VALUES["DIR"] in out


@pytest.mark.parametrize("name", TEMPLATES)
def test_rendered_unit_has_the_required_sections(name: str) -> None:
    from render import render

    out = render(CONTRIB / name, VALUES)
    for section in ("[Unit]", "[Service]", "[Install]"):
        assert section in out, f"{name} is missing {section}"
    assert "ExecStart=" in out


def test_unknown_placeholder_is_an_error(tmp_path: Path) -> None:
    from render import render

    bad = tmp_path / "bad.service.in"
    bad.write_text("[Service]\nExecStart=@NOPE@\n", encoding="utf-8")
    with pytest.raises(KeyError, match="NOPE"):
        render(bad, VALUES)


@pytest.mark.skipif(not shutil.which("systemd-analyze"), reason="systemd-analyze not present")
@pytest.mark.parametrize("name", TEMPLATES)
def test_systemd_accepts_the_rendered_unit(tmp_path: Path, name: str) -> None:
    from render import render

    unit_name = name.replace(".in", "")
    target = tmp_path / unit_name
    target.write_text(render(CONTRIB / name, VALUES), encoding="utf-8")
    result = subprocess.run(
        ["systemd-analyze", "verify", str(target)],
        capture_output=True,
        text=True,
    )
    # verify warns about missing binaries in a sandbox; only syntax errors matter.
    assert "Failed to parse" not in result.stderr, result.stderr
