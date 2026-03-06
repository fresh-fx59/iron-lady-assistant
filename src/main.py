import asyncio
import logging
import os
import signal
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
    AUTONOMY_ENABLED,
    AUTONOMY_FAILURE_THRESHOLD,
    AUTONOMY_FAILURE_WINDOW_MINUTES,
    AUTONOMY_ALERT_COOLDOWN_MINUTES,
    TELEGRAM_REQUEST_TIMEOUT_SECONDS,
    TELEGRAM_POLLING_TIMEOUT_SECONDS,
    TELEGRAM_BACKOFF_MIN_SECONDS,
    TELEGRAM_BACKOFF_MAX_SECONDS,
    TELEGRAM_BACKOFF_FACTOR,
    TELEGRAM_BACKOFF_JITTER,
)
from .bot import (
    router,
    provider_manager,
    memory_manager,
    get_step_plan_observer,
    get_cost_guardrail_observer,
    resume_step_plan_after_restart,
    bootstrap_step_plan_after_restart,
    resume_scope_snapshots_after_restart,
    set_step_plan_restart_callback,
    should_restart_step_plan_now,
)
from . import bot as bot_module
from .metrics import start_metrics_server
from .autonomy import AutonomyEngine, LearningJournal


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
        lines.append("\n⏳ Starting up...")
        startup_message = "\n".join(lines)

        try:
            await bot.send_message(chat_id=first_admin, text=startup_message, parse_mode="HTML")
            logging.info("Sent startup notification to admin %s", first_admin)
        except Exception as e:
            logging.warning("Failed to send startup notification: %s", e)

    except Exception as e:
        logging.warning("Could not send startup notification: %s", e)


async def send_ready_notification(bot: Bot) -> None:
    """Send ready notification when polling loop is started."""
    if not ALLOWED_USER_IDS:
        return
    try:
        first_admin = min(ALLOWED_USER_IDS)
        await bot.send_message(chat_id=first_admin, text="💬 Ready to accept messages.")
        logging.info("Sent ready notification to admin %s", first_admin)
    except Exception as e:
        logging.warning("Could not send ready notification: %s", e)


async def restart_process_for_step_plan(reason: str) -> bool:
    """Trigger process restart so step plan can continue across clean boots."""
    should_restart, blockers = await should_restart_step_plan_now()
    if not should_restart:
        logging.warning(
            "Step plan restart deferred (%s); active work in other scopes: %s",
            reason,
            blockers,
        )
        return False

    logging.warning("Step plan requested restart: %s", reason)
    await asyncio.sleep(1.0)
    os.kill(os.getpid(), signal.SIGTERM)
    return True


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    start_metrics_server(METRICS_PORT)

    bot = Bot(
        token=BOT_TOKEN,
        session=AiohttpSession(timeout=TELEGRAM_REQUEST_TIMEOUT_SECONDS),
    )
    dp = Dispatcher()
    dp.include_router(router)
    dp.startup.register(send_ready_notification)

    # Initialize task manager
    from .tasks import TaskManager
    from .scheduler import ScheduleManager
    autonomy_engine = AutonomyEngine(
        bot=bot,
        memory_manager=memory_manager,
        journal=LearningJournal(MEMORY_DIR / "learning.db"),
        proactive_enabled=AUTONOMY_ENABLED,
        failure_threshold=AUTONOMY_FAILURE_THRESHOLD,
        failure_window_minutes=AUTONOMY_FAILURE_WINDOW_MINUTES,
        alert_cooldown_minutes=AUTONOMY_ALERT_COOLDOWN_MINUTES,
    )
    set_step_plan_restart_callback(restart_process_for_step_plan)
    bot_module.task_manager = TaskManager(
        bot,
        observers=[autonomy_engine, get_step_plan_observer(), get_cost_guardrail_observer()],
    )
    await bot_module.task_manager.start()
    bot_module.schedule_manager = ScheduleManager(bot_module.task_manager, MEMORY_DIR / "schedules.db")
    await bot_module.schedule_manager.start()

    await bot.set_my_commands([
        BotCommand(command="start", description="Welcome message"),
        BotCommand(command="new", description="Start fresh conversation"),
        BotCommand(command="model", description="Switch model"),
        BotCommand(command="provider", description="Switch LLM provider"),
        BotCommand(command="status", description="Show current session info"),
        BotCommand(command="threads", description="Show tracked forum threads"),
        BotCommand(command="memory", description="Show what I remember"),
        BotCommand(command="memory_forget", description="Remove semantic memory fact by key"),
        BotCommand(command="memory_consolidate", description="Consolidate semantic memory facts"),
        BotCommand(command="tools", description="Show available tools"),
        BotCommand(command="rollback", description="Rollback to previous version (admin)"),
        BotCommand(command="selfmod_stage", description="Stage sandbox plugin candidate (admin)"),
        BotCommand(command="selfmod_apply", description="Apply sandbox plugin candidate (admin)"),
        BotCommand(command="schedule_every", description="Create recurring schedule"),
        BotCommand(command="schedule_daily", description="Create daily schedule"),
        BotCommand(command="schedule_weekly", description="Create weekly schedule"),
        BotCommand(command="schedule_list", description="List recurring schedules"),
        BotCommand(command="schedule_cancel", description="Cancel recurring schedule"),
        BotCommand(command="bg", description="Run task in background"),
        BotCommand(command="bg_cancel", description="Cancel background task"),
        BotCommand(command="stepplan_start", description="Start persisted step plan (admin)"),
        BotCommand(command="stepplan_status", description="Show persisted step plan"),
        BotCommand(command="stepplan_stop", description="Stop persisted step plan"),
        BotCommand(command="cancel", description="Cancel current request"),
    ])

    short_commit = mark_good_commit()

    # Send startup notification
    await send_startup_notification(bot, short_commit)
    await resume_step_plan_after_restart()
    await resume_scope_snapshots_after_restart()
    await bootstrap_step_plan_after_restart()

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
        if bot_module.schedule_manager:
            await bot_module.schedule_manager.stop()
        if bot_module.task_manager:
            await bot_module.task_manager.stop()
        provider_manager.shutdown()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
