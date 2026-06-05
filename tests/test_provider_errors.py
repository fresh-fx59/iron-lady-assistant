from src.provider_errors import (
    humanize_provider_api_error,
    is_provider_api_error,
    is_stale_codex_session_error,
)

# The exact envelope codex surfaced on stderr in the reported incident.
_ENVELOPE = (
    '{"type":"error","status":400,"error":{"message":"Provider API error: '
    "Instructions are required (request id: 20260605170650508042777xiPCjBti)\","
    '"type":"invalid_request_error","param":"","code":null}}'
)


def test_is_provider_api_error_detects_json_envelope():
    assert is_provider_api_error(_ENVELOPE) is True


def test_is_provider_api_error_detects_prefix_line():
    assert is_provider_api_error("Provider API error: Instructions are required") is True


def test_is_provider_api_error_detects_bare_status_envelope():
    assert is_provider_api_error('{"type":"error","status":503,"error":{"message":"upstream down"}}') is True


def test_is_provider_api_error_ignores_normal_text():
    assert is_provider_api_error("Sure — coffee and brownie logged at 7.3.") is False
    assert is_provider_api_error("Request idle timed out. Codex stopped producing output.") is False
    assert is_provider_api_error("Claude returned an error.") is False
    assert is_provider_api_error("") is False
    assert is_provider_api_error(None) is False


def test_is_provider_api_error_ignores_non_error_json():
    # A normal JSON-shaped assistant message must not be treated as a backend error.
    assert is_provider_api_error('{"type":"summary","value":42}') is False


def test_humanize_strips_request_id_and_json():
    msg = humanize_provider_api_error(_ENVELOPE)
    assert msg == "Provider API error: Instructions are required"
    assert "request id" not in msg.lower()
    assert "{" not in msg


def test_humanize_handles_prefix_line():
    msg = humanize_provider_api_error(
        "Provider API error: Instructions are required (request id: abc123)"
    )
    assert msg == "Provider API error: Instructions are required"


def test_humanize_returns_none_for_normal_text():
    assert humanize_provider_api_error("hello world") is None
    assert humanize_provider_api_error(None) is None


def test_stale_codex_session_error_still_works():
    assert is_stale_codex_session_error("resume failed: no rollout found for thread id") is True
    assert is_stale_codex_session_error("hello") is False
