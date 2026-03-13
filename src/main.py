import asyncio
import logging
import subprocess
from datetime import datetime, timezone
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

_startup_notice_sent_at: dict[tuple[int, int | None], datetime] = {}


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
            _startup_notice_sent_at[(target_chat_id, target_thread_id)] = datetime.now(timezone.utc)
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

        startup_sent_at = _startup_notice_sent_at.get((target_chat_id, target_thread_id))
        now_utc = datetime.now(timezone.utc)
        if startup_sent_at and (now_utc - startup_sent_at).total_seconds() < 300:
            logging.info(
                "Skipping ready notification for chat=%s thread=%s because startup notice was just sent",
                target_chat_id,
                target_thread_id,
            )
        else:
            kwargs = {"chat_id": target_chat_id, "text": "💬 Ready to accept messages."}
            if target_thread_id is not None:
                kwargs["message_thread_id"] = target_thread_id
            await bot.send_message(**kwargs)
            logging.info("Sent ready notification to chat=%s thread=%s", target_chat_id, target_thread_id)
        await auto_resume_step_plan_after_restart(bot)
    except Exception as e:
        logging.warning("Could not send ready notification: %s", e)


async def auto_resume_step_plan_after_restart(bot: Bot) -> bool:
    """Auto-queue the next step-plan continuation turn after restart."""
    global task_manager

    if task_manager is None:
        return False

    try:
        state = bot_module._load_step_plan_state()  # noqa: SLF001
    except Exception:
        return False

    if not state.get("active") or not state.get("restart_between_steps"):
        return False

    current_task_id = str(state.get("current_task_id") or "").strip()
    if current_task_id:
        existing = await task_manager.get_status(current_task_id)
        existing_status = str(getattr(existing, "status", "") or "").lower() if existing else ""
        if existing_status in {"queued", "running"}:
            logging.info(
                "Skipping step-plan auto-resume; task %s is still %s",
                current_task_id,
                existing_status,
            )
            return False
        # Stale task id after process restart: clear and continue auto-resume.
        state["current_task_id"] = None

    blocked_until_raw = str(state.get("auto_resume_blocked_until") or "").strip()
    if blocked_until_raw:
        try:
            blocked_until = datetime.fromisoformat(blocked_until_raw)
            if blocked_until.tzinfo is None:
                blocked_until = blocked_until.replace(tzinfo=timezone.utc)
            if blocked_until > datetime.now(timezone.utc):
                logging.info("Skipping step-plan auto-resume; blocked until %s", blocked_until.isoformat())
                return False
        except Exception:
            logging.debug("Invalid auto_resume_blocked_until value: %s", blocked_until_raw)

    chat_id = int(state.get("chat_id") or 0)
    if chat_id == 0:
        return False
    thread_id_raw = state.get("message_thread_id")
    thread_id = int(thread_id_raw) if thread_id_raw is not None else None
    user_id = int(state.get("user_id") or (min(ALLOWED_USER_IDS) if ALLOWED_USER_IDS else abs(chat_id)))
    current_index = int(state.get("current_index") or 0)
    steps = state.get("steps") if isinstance(state.get("steps"), list) else []
    next_step = steps[current_index] if current_index < len(steps) else None

    scope_key = bot_module._scope_key(chat_id, thread_id)  # noqa: SLF001
    provider = bot_module.provider_manager.get_provider(scope_key)
    session = bot_module.session_manager.get(chat_id, thread_id)
    model, session_id, provider_cli, resume_arg = bot_module._scheduled_task_backend(  # noqa: SLF001
        session,
        provider,
    )

    step_hint = f"\nCurrent step file: {next_step}" if next_step else ""
    prompt = (
        "continue plan\n"
        "Run the next planned step safely. "
        "After code changes run relevant tests and continue only if tests pass."
        f"{step_hint}"
    )
    task_id = await task_manager.submit(
        chat_id=chat_id,
        user_id=user_id,
        message_thread_id=thread_id,
        prompt=prompt,
        model=model,
        session_id=session_id,
        provider_cli=provider_cli,
        resume_arg=resume_arg,
    )

    state["current_task_id"] = task_id
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    state["last_error"] = ""
    try:
        bot_module._save_step_plan_state(state)  # noqa: SLF001
    except Exception:
        logging.debug("Failed to persist step-plan state after auto-resume submit", exc_info=True)

    notify_kwargs = {
        "chat_id": chat_id,
        "text": (
            f"🔁 Auto-resumed step plan after restart: step {current_index + 1}/{max(1, len(steps))}. "
            f"Task id: <code>{task_id}</code>"
        ),
        "parse_mode": "HTML",
    }
    if thread_id is not None:
        notify_kwargs["message_thread_id"] = thread_id
    await bot.send_message(**notify_kwargs)
    logging.info("Queued step-plan auto-resume task=%s chat=%s thread=%s", task_id, chat_id, thread_id)
    return True


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
    # Keep bot module globals in sync so command handlers use live managers after restart.
    bot_module.task_manager = task_manager
    bot_module.schedule_manager = schedule_manager
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
