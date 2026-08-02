"""The public NixOS example must leak no infrastructure.

This repo is PUBLIC, so this file checks by **shape**, never by literal: a test
listing the operator's real hostname or Telegram IDs would publish exactly what
it exists to protect (and the repo's pii-guard hook blocks it, correctly).

The complementary check — "the example contains none of the real module's
actual values" — lives in the private vault as `harness/check-nix-example-drift.mjs`,
which is the only place that can read both files.
"""

from __future__ import annotations

import re
from pathlib import Path

EXAMPLE = Path(__file__).resolve().parent.parent / "contrib" / "nixos" / "iron-lady.nix.example"


def _text() -> str:
    return EXAMPLE.read_text(encoding="utf-8")


def test_example_exists() -> None:
    assert EXAMPLE.is_file()


def test_no_provider_hostname_shape() -> None:
    """Bare-metal hostnames look like `vmi1234567` / `srv1234567`."""
    leaked = re.findall(r"\b(?:vmi|srv|vps)\d{5,}\b", _text(), flags=re.IGNORECASE)
    assert not leaked, f"host identifiers in a public example: {leaked}"


def test_no_long_numeric_ids() -> None:
    """Telegram user and chat IDs are 9+ digits; placeholders are all zeros."""
    leaked = [m for m in re.findall(r"-?\b\d{9,}\b", _text()) if set(m.lstrip("-")) != {"0"}]
    # A fixed JSON timestamp in the obsidian registry line is not an identifier.
    leaked = [m for m in leaked if not m.startswith("17000000000")]
    assert not leaked, f"real-looking IDs in a public example: {leaked}"


def test_sops_keys_use_the_placeholder_prefix() -> None:
    """Every secret name must be `yourservice_*`, not a real service prefix."""
    names = set(re.findall(r"/run/secrets/([A-Za-z0-9_]+)", _text()))
    names |= set(re.findall(r'=\s*"([a-z0-9_]*_(?:key|token|secret)[a-z0-9_]*)"', _text()))
    bad = sorted(n for n in names if not n.startswith("yourservice_"))
    assert not bad, f"sops key names not sanitized: {bad}"


def test_placeholders_are_present() -> None:
    text = _text()
    for placeholder in ("your-host", "myaccount", "/path/to/your/vault", "yourservice_"):
        assert placeholder in text, f"expected placeholder {placeholder!r} is missing"


def test_no_absolute_home_or_provider_paths() -> None:
    leaked = re.findall(r"/home/[A-Za-z0-9._-]+", _text())
    assert not leaked, f"absolute home paths in a public example: {leaked}"


def test_it_still_teaches_the_topology() -> None:
    """Sanitized must not mean useless."""
    text = _text()
    for unit in ("iron-lady-bot", "telegram-scheduler", "gmail-gateway", "telegram-proxy"):
        assert unit in text, f"the example no longer shows {unit}"


def test_header_tells_the_reader_what_to_replace() -> None:
    joined = "\n".join(_text().splitlines()[:8])
    assert "Example NixOS module" in joined
    assert "setup.sh" in joined, "non-NixOS readers must be pointed at the generic path"
