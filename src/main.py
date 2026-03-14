import asyncio
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

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
from .tasks import TaskNotificationMode

_startup_notice_sent_at: dict[tuple[int, int | None], datetime] = {}
_lifecycle_replay_task: asyncio.Task | None = None


class _ReplayMessage:
    """Minimal message shim so queued-turn replay can reuse the foreground pipeline."""

    def __init__(
        self,
        *,
        bot: Bot,
        chat_id: int,
        message_thread_id: int | None,
        user_id: int,
        message_id: int | None,
        text: str,
    ) -> None:
        self.bot = bot
        self.chat = SimpleNamespace(id=chat_id)
        self.message_thread_id = message_thread_id
        self.from_user = SimpleNamespace(id=user_id)
        self.message_id = message_id or 0
        self.text = text
        self.caption = None
        self.content_type = "text"
        self.photo = None
        self.voice = None

    async def answer(self, text: str, **kwargs):
        payload = {
            "chat_id": self.chat.id,
            "text": text,
            **kwargs,
        }
        if self.message_thread_id is not None:
            payload.setdefault("message_thread_id", self.message_thread_id)
        return await self.bot.send_message(**payload)


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
    """Keep restart details in logs without posting Telegram startup notices."""
    del bot
    _startup_notice_sent_at.clear()
    logging.info("Bot restarted at version=%s commit=%s", VERSION, commit or "unknown")


async def send_ready_notification(bot: Bot) -> None:
    """Resume queued work once polling starts without posting Telegram notices."""
    try:
        _startup_notice_sent_at.clear()
        await auto_resume_step_plan_after_restart(bot)
    except Exception as e:
        logging.warning("Could not finish restart-ready hook: %s", e)


async def replay_queued_turns_once(bot: Bot) -> int:
    global task_manager

    if task_manager is None:
        return 0
    if bot_module.lifecycle_store.is_draining():  # noqa: SLF001
        return 0

    queued_turns = await asyncio.to_thread(bot_module.lifecycle_store.claim_queued_turns, limit=10)  # noqa: SLF001
    submitted = 0
    for turn in queued_turns:
        try:
            scope_key = bot_module._scope_key(turn.chat_id, turn.message_thread_id)  # noqa: SLF001
            notify_kwargs = {
                "chat_id": turn.chat_id,
                "text": "🔁 Resuming queued request after deploy.",
            }
            if turn.message_thread_id is not None:
                notify_kwargs["message_thread_id"] = turn.message_thread_id
            await bot.send_message(**notify_kwargs)
            if str(getattr(turn, "prompt_format", "augmented")) == "raw":
                state = bot_module._get_state(scope_key)  # noqa: SLF001
                if state.lock.locked():
                    await asyncio.to_thread(bot_module.lifecycle_store.requeue_turn, turn.id)  # noqa: SLF001
                    continue
                replay_message = _ReplayMessage(
                    bot=bot,
                    chat_id=turn.chat_id,
                    message_thread_id=turn.message_thread_id,
                    user_id=turn.user_id,
                    message_id=turn.source_message_id,
                    text=turn.prompt,
                )
                await bot_module._handle_message_inner(replay_message, override_text=turn.prompt)  # noqa: SLF001
                await asyncio.to_thread(bot_module.lifecycle_store.mark_turn_completed, turn.id)  # noqa: SLF001
            else:
                provider = bot_module.provider_manager.get_provider(scope_key)
                session = bot_module.session_manager.get(turn.chat_id, turn.message_thread_id)
                model, session_id, provider_cli, resume_arg = bot_module._scheduled_task_backend(  # noqa: SLF001
                    session,
                    provider,
                )
                task_id = await task_manager.submit(
                    chat_id=turn.chat_id,
                    user_id=turn.user_id,
                    message_thread_id=turn.message_thread_id,
                    prompt=turn.prompt,
                    model=model,
                    session_id=session_id,
                    provider_cli=provider_cli,
                    resume_arg=resume_arg,
                    notification_mode=TaskNotificationMode.DELIVER_RESPONSE,
                    live_feedback=True,
                )
                await asyncio.to_thread(bot_module.lifecycle_store.mark_turn_submitted, turn.id, task_id)  # noqa: SLF001
            submitted += 1
        except Exception:
            logging.exception("Failed to replay queued turn id=%s", turn.id)
            await asyncio.to_thread(bot_module.lifecycle_store.requeue_turn, turn.id)  # noqa: SLF001
    return submitted


async def replay_queued_background_tasks_once() -> int:
    global task_manager

    if task_manager is None:
        return 0
    if bot_module.lifecycle_store.is_draining():  # noqa: SLF001
        return 0

    queued_tasks = await asyncio.to_thread(bot_module.lifecycle_store.claim_queued_background_tasks, limit=10)  # noqa: SLF001
    submitted = 0
    for item in queued_tasks:
        try:
            notification_mode = TaskNotificationMode(item.notification_mode)
            await task_manager.submit(
                chat_id=item.chat_id,
                user_id=item.user_id,
                message_thread_id=item.message_thread_id,
                prompt=item.prompt,
                model=item.model,
                session_id=item.session_id,
                provider_cli=item.provider_cli,
                resume_arg=item.resume_arg,
                notification_mode=notification_mode,
                live_feedback=item.live_feedback,
                feedback_title=item.feedback_title,
                task_id=item.task_id,
            )
            await asyncio.to_thread(bot_module.lifecycle_store.mark_background_task_submitted, item.task_id)  # noqa: SLF001
            submitted += 1
        except Exception:
            logging.exception("Failed to replay queued background task id=%s", item.task_id)
            await asyncio.to_thread(bot_module.lifecycle_store.requeue_background_task, item.task_id)  # noqa: SLF001
    return submitted


async def lifecycle_replay_loop(bot: Bot) -> None:
    while True:
        try:
            await replay_queued_turns_once(bot)
            await replay_queued_background_tasks_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.exception("Lifecycle replay loop iteration failed")
        await asyncio.sleep(2)


async def auto_resume_step_plan_after_restart(bot: Bot) -> bool:
    """Auto-queue the next step-plan continuation turn after restart."""
    global task_manager

    if task_manager is None:
        return False

    try:
        state = bot_module._load_step_plan_state()  # noqa: SLF001
    except Exception:
        return False

    restart_between_steps = bool(state.get("restart_between_steps"))
    steps, current_index = bot_module._step_plan_pending_steps(state)  # noqa: SLF001
    has_pending_steps = current_index < len(steps)

    if not restart_between_steps or not has_pending_steps:
        return False

    if not state.get("active"):
        logging.info(
            "Step-plan state is inactive but has pending steps (%s/%s); re-enabling auto-resume",
            current_index,
            len(steps),
        )
        state["active"] = True

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
    next_action = bot_module._ensure_step_plan_next_action(state)  # noqa: SLF001
    if next_action is None:
        logging.info("Skipping step-plan auto-resume; no valid next_action is available")
        return False

    scope_key = bot_module._scope_key(chat_id, thread_id)  # noqa: SLF001
    provider = bot_module.provider_manager.get_provider(scope_key)
    session = bot_module.session_manager.get(chat_id, thread_id)
    model, session_id, provider_cli, resume_arg = bot_module._scheduled_task_backend(  # noqa: SLF001
        session,
        provider,
    )

    prompt = str(next_action["prompt"])
    task_id = await task_manager.submit(
        chat_id=chat_id,
        user_id=user_id,
        message_thread_id=thread_id,
        prompt=prompt,
        model=model,
        session_id=session_id,
        provider_cli=provider_cli,
        resume_arg=resume_arg,
        notification_mode=TaskNotificationMode.DELIVER_RESPONSE,
        live_feedback=True,
        feedback_title="🔁 <b>Resuming previous step after restart...</b>",
    )

    state["current_task_id"] = task_id
    bot_module._ensure_step_plan_next_action(state)  # noqa: SLF001
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
    global task_manager, schedule_manager, _lifecycle_replay_task
    from .tasks import TaskManager
    from .scheduler import ScheduleManager

    bot_module.lifecycle_store.acknowledge_process_restart()  # noqa: SLF001
    task_manager = TaskManager(bot, lifecycle_store=bot_module.lifecycle_store)
    schedule_manager = ScheduleManager(
        task_manager,
        MEMORY_DIR / "schedules.db",
        notify_level=SCHEDULER_NOTIFY_LEVEL,
    )
    # Keep bot module globals in sync so command handlers use live managers after restart.
    bot_module.task_manager = task_manager
    bot_module.schedule_manager = schedule_manager
    await task_manager.start()
    if _lifecycle_replay_task is None:
        _lifecycle_replay_task = asyncio.create_task(lifecycle_replay_loop(bot))
    if EMBEDDED_SCHEDULER_ENABLED:
        task_manager.add_observer(schedule_manager)
        await schedule_manager.start()
    else:
        logging.info("Embedded scheduler worker disabled; expecting external scheduler daemon")
    return task_manager, schedule_manager


async def main() -> None:
    global _lifecycle_replay_task
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
        BotCommand(command="gmail_connect", description="Start Gmail API setup"),
        BotCommand(command="gmail_status", description="Show Gmail setup status"),
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
        if _lifecycle_replay_task:
            _lifecycle_replay_task.cancel()
            try:
                await _lifecycle_replay_task
            except asyncio.CancelledError:
                pass
            _lifecycle_replay_task = None
        if schedule_manager:
            await schedule_manager.stop()
        if task_manager:
            await task_manager.stop()
        provider_manager.shutdown()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
