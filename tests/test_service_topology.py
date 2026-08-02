"""Unit names are configuration, never literals.

`telegram-bot.service` was hardcoded in four call sites and does not exist on
the box — so self-restart and rollback failed silently while reporting success.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

SRC_ROOT = Path(__file__).resolve().parent.parent / "src"

LIVE_DEFAULTS = {
    "BOT_SERVICE": "iron-lady-bot.service",
    "SCHEDULER_SERVICE": "telegram-scheduler.service",
    "GMAIL_SERVICE": "gmail-gateway.service",
    "PROXY_SERVICE": "telegram-proxy-giedi.service",
}


def _fresh_config(monkeypatch, **env):
    for key in (
        "ILA_BOT_SERVICE",
        "ILA_SCHEDULER_SERVICE",
        "ILA_GMAIL_SERVICE",
        "ILA_PROXY_SERVICE",
    ):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token-12345")
    from src import config

    return importlib.reload(config)


@pytest.mark.parametrize("attr,expected", sorted(LIVE_DEFAULTS.items()))
def test_defaults_are_the_live_unit_names(monkeypatch, attr, expected) -> None:
    """An empty .env must reproduce the operator's box exactly."""
    config = _fresh_config(monkeypatch)
    assert getattr(config, attr) == expected


def test_env_override_wins(monkeypatch) -> None:
    config = _fresh_config(monkeypatch, ILA_BOT_SERVICE="my-bot.service")
    assert config.BOT_SERVICE == "my-bot.service"


def test_bare_name_gains_the_service_suffix(monkeypatch) -> None:
    """Input gate: accept `my-bot`, store `my-bot.service`, so no call site guesses."""
    config = _fresh_config(monkeypatch, ILA_BOT_SERVICE="my-bot")
    assert config.BOT_SERVICE == "my-bot.service"


def test_blank_override_falls_back_to_the_default(monkeypatch) -> None:
    config = _fresh_config(monkeypatch, ILA_BOT_SERVICE="   ")
    assert config.BOT_SERVICE == "iron-lady-bot.service"


def test_the_dead_literal_is_gone_from_src() -> None:
    """The regression guard. This is the bug: a unit name nothing else uses."""
    offenders = []
    for path in sorted(SRC_ROOT.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        for line_no, line in enumerate(text.splitlines(), start=1):
            # A comment naming the dead unit is documentation, not a call. Only
            # code can restart the wrong thing.
            if line.strip().startswith("#"):
                continue
            if "telegram-bot.service" in line:
                offenders.append(f"{path.relative_to(SRC_ROOT.parent)}:{line_no}: {line.strip()}")
    assert not offenders, (
        "hardcoded dead unit name found — use config.BOT_SERVICE:\n  " + "\n  ".join(offenders)
    )
