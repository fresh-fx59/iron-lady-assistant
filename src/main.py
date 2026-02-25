import asyncio
import logging
import subprocess
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand

from .config import BOT_TOKEN, METRICS_PORT
from .bot import router, provider_manager, task_manager
from .metrics import start_metrics_server


def mark_good_commit() -> None:
    """Mark current git commit as known-good after successful startup."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            commit = result.stdout.strip()
            deploy_dir = Path(__file__).parent.parent / ".deploy"
            deploy_dir.mkdir(exist_ok=True)
            (deploy_dir / "good_commit").write_text(commit)
            logging.info("Marked commit %s as last-known-good", commit[:8])
    except Exception as e:
        logging.warning("Could not mark good commit: %s", e)


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    start_metrics_server(METRICS_PORT)

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    # Initialize task manager
    global task_manager
    from .tasks import TaskManager
    task_manager = TaskManager(bot)
    await task_manager.start()

    await bot.set_my_commands([
        BotCommand(command="start", description="Welcome message"),
        BotCommand(command="new", description="Start fresh conversation"),
        BotCommand(command="model", description="Switch model"),
        BotCommand(command="provider", description="Switch LLM provider"),
        BotCommand(command="status", description="Show current session info"),
        BotCommand(command="memory", description="Show what I remember"),
        BotCommand(command="tools", description="Show available tools"),
        BotCommand(command="bg", description="Run task in background"),
        BotCommand(command="bg-cancel", description="Cancel background task"),
        BotCommand(command="cancel", description="Cancel current request"),
    ])

    mark_good_commit()

    logging.info("Bot starting...")
    try:
        await dp.start_polling(bot)
    finally:
        if task_manager:
            await task_manager.stop()
        provider_manager.shutdown()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
