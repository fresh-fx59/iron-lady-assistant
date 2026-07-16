from pathlib import Path

from src import config
from src.telegram_digest_tool import _build_daily_prompt


def test_daily_prompt_uses_edge_tts_safe_tool() -> None:
    prompt = _build_daily_prompt(Path("/tmp/brief.md"))

    assert "[[SCHEDULE_DELIVER]]" in prompt
    assert "USE_TOOL: edge-tts-safe" in prompt
    assert "USE_TOOL: sag" not in prompt
    # Recipient name is env-configurable (DIGEST_RECIPIENT_NAME); the public
    # default is a neutral placeholder and no real name is committed. Asserting
    # the configured value is present proves the interpolation happened.
    assert config.DIGEST_RECIPIENT_NAME in prompt
    assert f"{config.DIGEST_RECIPIENT_NAME}'s daily Telegram digest" in prompt
    assert "last 24 hours" in prompt
    assert "linked discussion chats" in prompt
