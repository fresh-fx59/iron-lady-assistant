import asyncio
import logging
import subprocess
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.utils.backoff import BackoffConfig
from aiogram.types import BotCommand

from .config import (
    BOT_TOKEN,
    METRICS_PORT,
    ALLOWED_USER_IDS,
    VERSION,
    MEMORY_DIR,
    TELEGRAM_REQUEST_TIMEOUT_SECONDS,
    TELEGRAM_POLLING_TIMEOUT_SECONDS,
    TELEGRAM_BACKOFF_MIN_SECONDS,
    TELEGRAM_BACKOFF_MAX_SECONDS,
    TELEGRAM_BACKOFF_FACTOR,
    TELEGRAM_BACKOFF_JITTER,
    EMBEDDED_SCHEDULER_ENABLED,
    SCHEDULER_NOTIFY_LEVEL,
)
from . import bot as bot_module
from .bot import router, provider_manager, task_manager, schedule_manager
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


def ensure_worklog_git_hook() -> None:
    """Ensure local git config points to the managed post-commit hook."""
    repo_root = Path(__file__).parent.parent
    hooks_dir = repo_root / "git-hooks"
    hook_path = hooks_dir / "post-commit"
    if not hook_path.exists():
        logging.info("Skipping git hook install because %s is missing", hook_path)
        return
    try:
        subprocess.run(
            ["git", "-C", str(repo_root), "config", "--local", "core.hooksPath", str(hooks_dir)],
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        )
        logging.info("Configured git hooksPath to %s", hooks_dir)
    except Exception as exc:
        logging.warning("Could not configure git hooksPath: %s", exc)


async def send_startup_notification(bot: Bot, commit: str | None = None) -> None:
    """Send startup notification to active step-plan thread or first admin."""
    if not ALLOWED_USER_IDS and not getattr(bot_module.config, "ALLOWED_CHAT_IDS", set()):
        return

    try:
        target_chat_id: int | None = None
        target_thread_id: int | None = None
        try:
            state = bot_module._load_step_plan_state()  # noqa: SLF001
            if state.get("active"):
                chat_id = int(state.get("chat_id") or 0)
                if chat_id:
                    target_chat_id = chat_id
                    target_thread_id = state.get("message_thread_id")
        except Exception:
            logging.debug("Could not resolve step-plan notification target", exc_info=True)

        if target_chat_id is None:
            try:
                latest_scope_target = bot_module._latest_scope_target()  # noqa: SLF001
                if latest_scope_target:
                    target_chat_id, target_thread_id = latest_scope_target
            except Exception:
                logging.debug("Could not resolve latest scope notification target", exc_info=True)

        if target_chat_id is None and ALLOWED_USER_IDS:
            target_chat_id = min(ALLOWED_USER_IDS)
        if target_chat_id is None and bot_module.config.ALLOWED_CHAT_IDS:
            target_chat_id = min(bot_module.config.ALLOWED_CHAT_IDS)
        if target_chat_id is None:
            return

        lines = ["🚀 <b>Bot restarted</b>\n"]
        lines.append(f"📦 Version: <code>{VERSION}</code>")
        if commit:
            lines.append(f"📦 Commit: <code>{commit}</code>")
        lines.append("\n⏳ Starting up...")
        startup_message = "\n".join(lines)

        try:
            kwargs = {"chat_id": target_chat_id, "text": startup_message, "parse_mode": "HTML"}
            if target_thread_id is not None:
                kwargs["message_thread_id"] = target_thread_id
            await bot.send_message(**kwargs)
            logging.info("Sent startup notification to chat=%s thread=%s", target_chat_id, target_thread_id)
        except Exception as e:
            logging.warning("Failed to send startup notification: %s", e)

    except Exception as e:
        logging.warning("Could not send startup notification: %s", e)


async def send_ready_notification(bot: Bot) -> None:
    """Send ready notification when polling loop is started."""
    if not ALLOWED_USER_IDS and not getattr(bot_module.config, "ALLOWED_CHAT_IDS", set()):
        return
    try:
        target_chat_id: int | None = None
        target_thread_id: int | None = None
        try:
            state = bot_module._load_step_plan_state()  # noqa: SLF001
            if state.get("active"):
                chat_id = int(state.get("chat_id") or 0)
                if chat_id:
                    target_chat_id = chat_id
                    target_thread_id = state.get("message_thread_id")
        except Exception:
            logging.debug("Could not resolve step-plan ready notification target", exc_info=True)

        if target_chat_id is None:
            try:
                latest_scope_target = bot_module._latest_scope_target()  # noqa: SLF001
                if latest_scope_target:
                    target_chat_id, target_thread_id = latest_scope_target
            except Exception:
                logging.debug("Could not resolve latest scope ready target", exc_info=True)

        if target_chat_id is None and ALLOWED_USER_IDS:
            target_chat_id = min(ALLOWED_USER_IDS)
        if target_chat_id is None and bot_module.config.ALLOWED_CHAT_IDS:
            target_chat_id = min(bot_module.config.ALLOWED_CHAT_IDS)
        if target_chat_id is None:
            return

        kwargs = {"chat_id": target_chat_id, "text": "💬 Ready to accept messages."}
        if target_thread_id is not None:
            kwargs["message_thread_id"] = target_thread_id
        await bot.send_message(**kwargs)
        logging.info("Sent ready notification to chat=%s thread=%s", target_chat_id, target_thread_id)
    except Exception as e:
        logging.warning("Could not send ready notification: %s", e)


async def initialize_runtime(bot: Bot) -> tuple[object, object]:
    global task_manager, schedule_manager
    from .tasks import TaskManager
    from .scheduler import ScheduleManager

    task_manager = TaskManager(bot)
    schedule_manager = ScheduleManager(
        task_manager,
        MEMORY_DIR / "schedules.db",
        notify_level=SCHEDULER_NOTIFY_LEVEL,
    )
    await task_manager.start()
    if EMBEDDED_SCHEDULER_ENABLED:
        task_manager.add_observer(schedule_manager)
        await schedule_manager.start()
    else:
        logging.info("Embedded scheduler worker disabled; expecting external scheduler daemon")
    return task_manager, schedule_manager


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    start_metrics_server(METRICS_PORT)
    ensure_worklog_git_hook()

    bot = Bot(
        token=BOT_TOKEN,
        session=AiohttpSession(timeout=TELEGRAM_REQUEST_TIMEOUT_SECONDS),
    )
    dp = Dispatcher()
    dp.include_router(router)
    dp.startup.register(send_ready_notification)

    await initialize_runtime(bot)

    await bot.set_my_commands([
        BotCommand(command="start", description="Welcome message"),
        BotCommand(command="new", description="Start fresh conversation"),
        BotCommand(command="model", description="Switch model"),
        BotCommand(command="provider", description="Switch LLM provider"),
        BotCommand(command="status", description="Show current session info"),
        BotCommand(command="threads", description="Show tracked forum threads"),
        BotCommand(command="memory", description="Show what I remember"),
        BotCommand(command="tools", description="Show available tools"),
        BotCommand(command="rollback", description="Rollback to previous version (admin)"),
        BotCommand(command="selfmod_stage", description="Stage sandbox plugin candidate (admin)"),
        BotCommand(command="selfmod_apply", description="Apply sandbox plugin candidate (admin)"),
        BotCommand(command="schedule_every", description="Create recurring schedule"),
        BotCommand(command="schedule_daily", description="Create daily schedule"),
        BotCommand(command="schedule_weekly", description="Create weekly schedule"),
        BotCommand(command="schedule_list", description="List recurring schedules"),
        BotCommand(command="schedule_history", description="Show scheduled job history"),
        BotCommand(command="schedule_cancel", description="Cancel recurring schedule"),
        BotCommand(command="bg", description="Run task in background"),
        BotCommand(command="bg_cancel", description="Cancel background task"),
        BotCommand(command="cancel", description="Cancel current request"),
    ])

    short_commit = mark_good_commit()

    # Send startup notification
    await send_startup_notification(bot, short_commit)

    logging.info("Bot starting...")
    try:
        await dp.start_polling(
            bot,
            polling_timeout=TELEGRAM_POLLING_TIMEOUT_SECONDS,
            backoff_config=BackoffConfig(
                min_delay=TELEGRAM_BACKOFF_MIN_SECONDS,
                max_delay=TELEGRAM_BACKOFF_MAX_SECONDS,
                factor=TELEGRAM_BACKOFF_FACTOR,
                jitter=TELEGRAM_BACKOFF_JITTER,
            ),
        )
    finally:
        if schedule_manager:
            await schedule_manager.stop()
        if task_manager:
            await task_manager.stop()
        provider_manager.shutdown()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
