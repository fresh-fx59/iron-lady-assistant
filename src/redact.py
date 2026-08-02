"""Redaction helpers for anything that reaches a log.

`turn_provider_execution` logged the provider's env dict on every turn, keeping
every key that started with `ANTHROPIC_` — which includes
`ANTHROPIC_AUTH_TOKEN`. The live LinkAPI key was therefore written to journald
in cleartext on each bot turn (found 2026-08-01).

Redaction here is deny-by-default on the *name*: a variable whose name looks
like a credential is masked whatever its value, so a new secret-bearing
variable is covered without anyone remembering to add it.
"""

from __future__ import annotations

import re
from typing import Any, Mapping

#: Substrings that mark a variable name as credential-bearing.
_SECRET_NAME_MARKERS = (
    "TOKEN",
    "KEY",
    "SECRET",
    "PASSWORD",
    "PASSWD",
    "CREDENTIAL",
    "AUTH",
    "SESSION_STRING",
    "DSN",
)

#: Names that contain a marker but hold no secret. Keep this list short.
_SAFE_NAMES = frozenset(
    {
        "ANTHROPIC_MODEL",
        "ANTHROPIC_BASE_URL",
        "KEYBOARD",
    }
)

_REDACTED = "<redacted>"

#: Value-shaped fallback: provider keys (`sk-…`, `xoxb-…`) and long opaque
#: blobs, for the case where the value is logged without its variable name.
_SECRET_VALUE_RE = re.compile(
    r"\b(?:sk-|xox[baprs]-|ghp_|gho_|github_pat_|AIza)[A-Za-z0-9_\-]{8,}"
)


def is_secret_name(name: str) -> bool:
    """True when a variable name looks like it carries a credential."""
    upper = str(name).upper()
    if upper in _SAFE_NAMES:
        return False
    return any(marker in upper for marker in _SECRET_NAME_MARKERS)


def redact_env(env: Mapping[str, Any]) -> dict[str, Any]:
    """Copy of `env` with credential-looking values masked.

    Keys are preserved so logs stay diagnostic — you can still see *that*
    `ANTHROPIC_AUTH_TOKEN` was set, just not what it was.
    """
    redacted: dict[str, Any] = {}
    for key, value in env.items():
        if is_secret_name(key):
            redacted[key] = _REDACTED
        else:
            redacted[key] = redact_text(value) if isinstance(value, str) else value
    return redacted


def redact_text(text: str) -> str:
    """Mask provider-key-shaped substrings inside free text."""
    return _SECRET_VALUE_RE.sub(_REDACTED, text)
