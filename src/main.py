import asyncio
import logging
import subprocess
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand

from .config import BOT_TOKEN, METRICS_PORT, ALLOWED_USER_IDS, VERSION
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
            short_commit = result.stdout.strip()[:8]
            deploy_dir = Path(__file__).parent.parent / ".deploy"
            deploy_dir.mkdir(exist_ok=True)
            (deploy_dir / "good_commit").write_text(commit)
            logging.info("Marked commit %s as last-known-good", short_commit)
            return short_commit
    except Exception as e:
        logging.warning("Could not mark good commit: %s", e)
        return None


async def send_startup_notification(bot: Bot, commit: str | None = None) -> None:
    """Send startup notification to first admin."""
    if not ALLOWED_USER_IDS:
        return

    try:
        first_admin = min(ALLOWED_USER_IDS)

        lines = ["🚀 <b>Bot restarted</b>\n"]
        lines.append(f"📦 Version: <code>{VERSION}</code>")
        if commit:
            lines.append(f"📦 Commit: <code>{commit}</code>")

        lines.append("\n✅ Ready to assist!")

        message = "\n".join(lines)

        try:
            await bot.send_message(chat_id=first_admin, text=message, parse_mode="HTML")
            logging.info("Sent startup notification to admin %s", first_admin)
        except Exception as e:
            logging.warning("Failed to send startup notification: %s", e)

    except Exception as e:
        logging.warning("Could not send startup notification: %s", e)


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

    short_commit = mark_good_commit()

    # Send startup notification
    await send_startup_notification(bot, short_commit)

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
