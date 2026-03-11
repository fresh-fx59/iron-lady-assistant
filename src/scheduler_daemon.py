import asyncio
import logging

from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession

from .config import (
    BOT_TOKEN,
    MEMORY_DIR,
    SCHEDULER_NOTIFY_CHAT_ID,
    SCHEDULER_NOTIFY_LEVEL,
    SCHEDULER_NOTIFY_THREAD_ID,
    TELEGRAM_REQUEST_TIMEOUT_SECONDS,
)
from .scheduler import ScheduleManager
from .tasks import TaskManager


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    bot = Bot(
        token=BOT_TOKEN,
        session=AiohttpSession(timeout=TELEGRAM_REQUEST_TIMEOUT_SECONDS),
    )
    task_manager = TaskManager(bot)
    schedule_manager = ScheduleManager(
        task_manager,
        MEMORY_DIR / "schedules.db",
        notification_bot=bot,
        notification_chat_id=SCHEDULER_NOTIFY_CHAT_ID,
        notification_thread_id=SCHEDULER_NOTIFY_THREAD_ID,
        notify_level=SCHEDULER_NOTIFY_LEVEL,
    )
    task_manager.add_observer(schedule_manager)
    await task_manager.start()
    await schedule_manager.start()
    logging.info(
        "Scheduler daemon started (notify_chat=%s notify_thread=%s notify_level=%s)",
        SCHEDULER_NOTIFY_CHAT_ID,
        SCHEDULER_NOTIFY_THREAD_ID,
        SCHEDULER_NOTIFY_LEVEL,
    )
    try:
        await asyncio.Event().wait()
    finally:
        await schedule_manager.stop()
        await task_manager.stop()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
