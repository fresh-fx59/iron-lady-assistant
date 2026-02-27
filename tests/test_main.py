from unittest.mock import AsyncMock

import pytest

from src import main


@pytest.mark.asyncio
async def test_send_startup_notification_sends_ready_as_separate_message(monkeypatch) -> None:
    bot = AsyncMock()
    monkeypatch.setattr(main, "ALLOWED_USER_IDS", {12345})

    await main.send_startup_notification(bot, commit="abc12345")

    assert bot.send_message.await_count == 2
    first = bot.send_message.await_args_list[0].kwargs
    second = bot.send_message.await_args_list[1].kwargs

    assert first["chat_id"] == 12345
    assert "Bot restarted" in first["text"]
    assert "Ready to assist!" in first["text"]
    assert second["chat_id"] == 12345
    assert second["text"] == "💬 Ready to accept messages."
