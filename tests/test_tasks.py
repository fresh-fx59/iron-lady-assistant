import asyncio
from datetime import datetime
from unittest.mock import AsyncMock

import pytest

from src.tasks import BackgroundTask, TaskManager, TaskStatus


@pytest.mark.asyncio
async def test_typing_loop_sends_fallback_when_chat_action_fails() -> None:
    bot = AsyncMock()
    bot.send_chat_action = AsyncMock(side_effect=RuntimeError("chat action unavailable"))
    bot.send_message = AsyncMock()
    manager = TaskManager(bot)
    task = BackgroundTask(
        id="task-1",
        chat_id=123,
        message_thread_id=77,
        user_id=123,
        prompt="x",
        model="sonnet",
        session_id=None,
        status=TaskStatus.RUNNING,
        created_at=datetime.now(),
    )

    typing_task = asyncio.create_task(manager._typing_loop(task))  # noqa: SLF001
    await asyncio.sleep(0.05)
    typing_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await typing_task

    bot.send_message.assert_awaited()
