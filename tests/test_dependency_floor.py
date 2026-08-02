"""The declared floor must be one production actually satisfies.

requirements.txt declared aiogram>=3.25 while the box runs 3.22.0, so code
written against a newer API would import in dev and fail on the box.
"""

from __future__ import annotations

import re
from pathlib import Path

import aiogram
from packaging.requirements import Requirement
from packaging.version import Version

REQUIREMENTS = Path(__file__).resolve().parent.parent / "requirements.txt"

#: The aiogram in the box's Nix python env. Raising this needs a nixpkgs bump.
PRODUCTION_AIOGRAM = Version("3.22.0")


def _requirement(name: str) -> Requirement:
    for line in REQUIREMENTS.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and re.match(rf"^{name}\b", line):
            return Requirement(line)
    raise AssertionError(f"{name} not found in requirements.txt")


def test_production_aiogram_satisfies_the_declared_floor() -> None:
    spec = _requirement("aiogram").specifier
    assert PRODUCTION_AIOGRAM in spec, (
        f"requirements.txt declares aiogram{spec} but production runs "
        f"{PRODUCTION_AIOGRAM} — production has never satisfied it"
    )


def test_the_installed_aiogram_also_satisfies_it() -> None:
    spec = _requirement("aiogram").specifier
    assert Version(aiogram.__version__) in spec


def test_a_major_bump_is_excluded() -> None:
    spec = _requirement("aiogram").specifier
    assert Version("4.0.0") not in spec, "pin an upper bound; aiogram 4 will break this"
