import importlib.util
from pathlib import Path


_SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "add_telegram_digest_channel.py"
_SPEC = importlib.util.spec_from_file_location("add_telegram_digest_channel", _SCRIPT_PATH)
assert _SPEC and _SPEC.loader
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)


def test_extract_channel_lookup_supports_url_and_username() -> None:
    assert _MODULE._extract_channel_lookup("https://t.me/ai_engineer_helper") == "ai_engineer_helper"
    assert _MODULE._extract_channel_lookup("@ai_engineer_helper") == "ai_engineer_helper"
    assert _MODULE._extract_channel_lookup("3019299921") == "3019299921"


def test_set_env_value_replaces_or_appends() -> None:
    lines = ["A=1", "B=2"]

    updated = _MODULE._set_env_value(lines, "B", "3")
    assert updated == ["A=1", "B=3"]

    appended = _MODULE._set_env_value(lines, "C", "4")
    assert appended == ["A=1", "B=2", "C=4"]


def test_find_channel_matches_username_title_and_id() -> None:
    channels = [
        {
            "entity_id": 3019299921,
            "title": "AI для Инженеров",
            "username": "ai_engineer_helper",
            "linked_chat_id": 3305897502,
        }
    ]

    assert _MODULE._find_channel(channels, "ai_engineer_helper")["entity_id"] == 3019299921
    assert _MODULE._find_channel(channels, "AI для Инженеров")["entity_id"] == 3019299921
    assert _MODULE._find_channel(channels, "3019299921")["entity_id"] == 3019299921


def test_set_env_value_can_clear_allowlists() -> None:
    lines = ["TELEGRAM_PROXY_ALLOWED_CHANNEL_IDS=1,2", "TELEGRAM_PROXY_ALLOWED_CHAT_IDS=3,4"]

    updated = _MODULE._set_env_value(lines, "TELEGRAM_PROXY_ALLOWED_CHANNEL_IDS", "")
    updated = _MODULE._set_env_value(updated, "TELEGRAM_PROXY_ALLOWED_CHAT_IDS", "")

    assert updated == ["TELEGRAM_PROXY_ALLOWED_CHANNEL_IDS=", "TELEGRAM_PROXY_ALLOWED_CHAT_IDS="]
