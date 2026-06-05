from __future__ import annotations

import json
import re

_STALE_CODEX_SESSION_ERROR_PATTERNS = (
    re.compile(r"thread/resume", re.IGNORECASE),
    re.compile(r"no rollout found for thread id", re.IGNORECASE),
    re.compile(r"resume failed", re.IGNORECASE),
    re.compile(r"thread id\s+[0-9a-f-]{8,}.*not found", re.IGNORECASE),
)

# Codex (and the proxies it talks to) prefix upstream rejections with this.
_PROVIDER_API_ERROR_PREFIX_RE = re.compile(r"provider api error", re.IGNORECASE)
# Strip the trailing "(request id: ...)" so internal ids never reach the user.
_REQUEST_ID_RE = re.compile(r"\s*\(request id:[^)]*\)", re.IGNORECASE)


def is_stale_codex_session_error(text: str | None) -> bool:
    normalized = (text or "").strip()
    if not normalized:
        return False
    return any(pattern.search(normalized) for pattern in _STALE_CODEX_SESSION_ERROR_PATTERNS)


def _parse_error_envelope(text: str | None) -> dict | None:
    """Return the parsed dict when ``text`` is a JSON ``{"type":"error",...}`` envelope."""
    stripped = (text or "").strip()
    if not (stripped.startswith("{") and stripped.endswith("}")):
        return None
    try:
        data = json.loads(stripped)
    except (ValueError, TypeError):
        return None
    if not isinstance(data, dict):
        return None
    if str(data.get("type", "")).lower() != "error":
        return None
    return data


def is_provider_api_error(text: str | None) -> bool:
    """True when ``text`` is a hard upstream model/provider API error.

    Matches either a codex/proxy ``Provider API error: ...`` line or a raw JSON
    error envelope such as
    ``{"type":"error","status":400,"error":{"message":...}}``. These signal the
    upstream model backend rejected the request, so the bot should fall back to
    another provider rather than surface raw JSON to the user. Callers only
    consult this on responses already flagged ``is_error``, so a JSON-shaped
    successful answer can never be misclassified.
    """
    if not text:
        return False
    if _PROVIDER_API_ERROR_PREFIX_RE.search(text):
        return True
    data = _parse_error_envelope(text)
    if data is None:
        return False
    return "status" in data or isinstance(data.get("error"), dict)


def humanize_provider_api_error(text: str | None) -> str | None:
    """Return a clean one-line message for a provider API error, else ``None``.

    Unwraps the inner ``error.message`` from a JSON envelope (or uses the raw
    ``Provider API error: ...`` line) and strips any ``(request id: ...)`` suffix.
    """
    if not text:
        return None
    message: str | None = None
    data = _parse_error_envelope(text)
    if data is not None:
        err = data.get("error")
        if isinstance(err, dict):
            message = err.get("message")
        message = message or data.get("message")
    if not message:
        if _PROVIDER_API_ERROR_PREFIX_RE.search(text):
            message = text.strip()
        else:
            return None
    cleaned = _REQUEST_ID_RE.sub("", str(message)).strip()
    return cleaned or None
