"""Credentials must never reach a log line.

The provider-selection log kept every `ANTHROPIC_*` variable, including
`ANTHROPIC_AUTH_TOKEN`, so the live LinkAPI key was written to journald in
cleartext on every bot turn (found 2026-08-01).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.redact import is_secret_name, redact_env, redact_text

SRC_ROOT = Path(__file__).resolve().parent.parent / "src"

# Built at runtime, never written as a key-shaped literal: a literal here trips
# the repo's pii-guard pre-commit hook, and rightly so — this is a public repo.
FAKE_KEY = "sk-" + ("NOTAREALKEY" * 3)


def test_the_exact_leak_is_masked() -> None:
    """The real shape of the leaked line."""
    env = {
        "ANTHROPIC_BASE_URL": "https://api.linkapi.ai",
        "ANTHROPIC_AUTH_TOKEN": FAKE_KEY,
        "ANTHROPIC_MODEL": "claude-opus-4-8",
    }
    out = redact_env(env)

    assert FAKE_KEY not in str(out)
    assert out["ANTHROPIC_AUTH_TOKEN"] == "<redacted>"
    # Still diagnostic: the non-secret context survives.
    assert out["ANTHROPIC_BASE_URL"] == "https://api.linkapi.ai"
    assert out["ANTHROPIC_MODEL"] == "claude-opus-4-8"
    assert set(out) == set(env), "keys must be preserved so logs stay useful"


@pytest.mark.parametrize(
    "name",
    [
        "ANTHROPIC_AUTH_TOKEN",
        "OPENAI_API_KEY",
        "TELEGRAM_BOT_TOKEN",
        "LINKAPI_API_KEY",
        "BETTER_AUTH_SECRET",
        "APP_ADMIN_PASSWORD",
        "DATABASE_URL_DSN",
        "TELEGRAM_SESSION_STRING",
    ],
)
def test_credential_names_are_masked(name: str) -> None:
    assert is_secret_name(name) is True
    assert redact_env({name: FAKE_KEY})[name] == "<redacted>"


@pytest.mark.parametrize("name", ["ANTHROPIC_MODEL", "ANTHROPIC_BASE_URL", "HOME", "PATH"])
def test_harmless_names_are_left_alone(name: str) -> None:
    assert is_secret_name(name) is False
    assert redact_env({name: "plain-value"})[name] == "plain-value"


def test_a_new_secret_variable_is_covered_without_being_listed() -> None:
    """Deny-by-default on the name is the point: nobody has to remember."""
    out = redact_env({"SOME_FUTURE_PROVIDER_TOKEN": FAKE_KEY})
    assert out["SOME_FUTURE_PROVIDER_TOKEN"] == "<redacted>"


def test_key_shaped_values_are_masked_even_under_a_harmless_name() -> None:
    out = redact_env({"ANTHROPIC_EXTRA_ARGS": f"--api-key {FAKE_KEY} --verbose"})
    assert FAKE_KEY not in out["ANTHROPIC_EXTRA_ARGS"]
    assert "--verbose" in out["ANTHROPIC_EXTRA_ARGS"]


def test_redact_text_handles_common_provider_prefixes() -> None:
    suffix = "NOTAREALKEY123456"
    for prefix in ("sk-", "xoxb-", "ghp_"):
        token = prefix + suffix
        assert token not in redact_text(f"leaked {token} here")


def test_non_string_values_survive() -> None:
    assert redact_env({"TIMEOUT": 30})["TIMEOUT"] == 30


def test_no_env_dict_reaches_a_log_call_unredacted() -> None:
    """Guard the call site itself.

    `turn_provider_execution` takes its logger as a parameter, so there is no
    module logger to capture. Assert on the source instead: every place that
    logs an env mapping must route it through redact_env. This is the test
    that fails if someone reinstates the raw comprehension.
    """
    source = (SRC_ROOT / "features" / "turn_provider_execution.py").read_text(encoding="utf-8")

    for line_no, line in enumerate(source.splitlines(), start=1):
        if "env.items()" in line and "startswith" in line:
            assert "redact_env" in line, (
                f"turn_provider_execution.py:{line_no} passes a raw env mapping to a "
                f"log call — this is exactly how the LinkAPI key leaked:\n  {line.strip()}"
            )


def test_the_leaking_line_is_actually_covered() -> None:
    """The guard above is worthless if it matches nothing."""
    source = (SRC_ROOT / "features" / "turn_provider_execution.py").read_text(encoding="utf-8")
    matches = [ln for ln in source.splitlines() if "env.items()" in ln and "startswith" in ln]
    assert matches, "the env-logging line vanished — update or delete this guard"
