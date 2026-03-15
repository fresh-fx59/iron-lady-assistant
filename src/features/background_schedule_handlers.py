from __future__ import annotations

import html
import re
from datetime import datetime
from typing import Any, Callable


async def cmd_bg(
    message: Any,
    command: Any,
    *,
    is_authorized: Callable[[int | None, int | None], bool],
    task_manager: Any,
    command_args_fn: Callable[[Any, Any], str],
    thread_id_fn: Callable[[Any], int | None],
    actor_id_fn: Callable[[Any], int | None],
    session_manager: Any,
    task_backend_fn: Callable[[Any, Any], tuple[str, str | None, str, str | None]],
    current_provider_fn: Callable[[str], Any],
    scope_key_fn: Callable[[int, int | None], str],
    build_augmented_prompt_fn: Callable[[str], str],
) -> None:
    if not is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    if not task_manager:
        await message.answer("Background tasks not available.")
        return

    prompt = command_args_fn(message, command)
    if not prompt:
        await message.answer("Please provide a task to run in background.\n\nExample: /bg write a python script to backup my database")
        return

    chat_id = message.chat.id
    thread_id = thread_id_fn(message)
    session = session_manager.get(chat_id, thread_id)
    provider = current_provider_fn(scope_key_fn(chat_id, thread_id))
    task_model, session_id, provider_cli, resume_arg = task_backend_fn(session, provider)
    full_prompt = build_augmented_prompt_fn(
        prompt,
        chat_id=chat_id,
        message_thread_id=thread_id,
        scope_key=scope_key_fn(chat_id, thread_id),
        session=session,
    )

    try:
        task_id = await task_manager.submit(
            chat_id=chat_id,
            message_thread_id=thread_id,
            user_id=actor_id_fn(message),
            prompt=full_prompt,
            model=task_model,
            session_id=session_id,
            provider_cli=provider_cli,
            resume_arg=resume_arg,
        )
    except RuntimeError as exc:
        await message.answer(str(exc))
        return

    lines = [
        "✅ <b>Task queued</b>",
        "",
        f"<b>Task ID:</b> <code>{task_id}</code>",
        f"<b>Model:</b> {task_model}",
        "",
        "I'll notify you when it completes. You can continue chatting.",
        "",
        "<b>Commands:</b>",
        "/bg-list — List active tasks",
        f"/bg_cancel {task_id} — Cancel this task",
    ]
    await message.answer("\n".join(lines), parse_mode="HTML")


async def cmd_bg_list(
    message: Any,
    *,
    is_authorized: Callable[[int | None, int | None], bool],
    task_manager: Any,
    thread_id_fn: Callable[[Any], int | None],
    task_status: Any,
) -> None:
    if not is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    if not task_manager:
        await message.answer("Background tasks not available.")
        return

    tasks = task_manager.list_user_tasks(message.chat.id, thread_id_fn(message))
    if not tasks:
        await message.answer("No active background tasks.")
        return

    lines = ["<b>Active background tasks:</b>", ""]
    for task in tasks:
        status_emoji = {
            task_status.QUEUED: "⏳",
            task_status.RUNNING: "🔄",
        }.get(task.status, "❓")
        duration = ""
        if task.started_at:
            duration = f" ({(datetime.now() - task.started_at).total_seconds():.0f}s)"
        lines.append(f"{status_emoji} <code>{task.id[:8]}</code> — {task.status.value}{duration}")
        lines.append(f"   {task.prompt[:100]}...")
        lines.append("")

    await message.answer("\n".join(lines), parse_mode="HTML")


async def cmd_bg_cancel(
    message: Any,
    command: Any,
    *,
    is_authorized: Callable[[int | None, int | None], bool],
    task_manager: Any,
    command_args_fn: Callable[[Any, Any], str],
    thread_id_fn: Callable[[Any], int | None],
    task_status: Any,
) -> None:
    if not is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    if not task_manager:
        await message.answer("Background tasks not available.")
        return

    task_id = command_args_fn(message, command)
    if not task_id:
        await message.answer("Please provide a task ID.\n\nExample: /bg_cancel abc123")
        return

    full_task_id = None
    for tid in task_manager.tasks:
        if tid.startswith(task_id):
            full_task_id = tid
            break

    if not full_task_id:
        await message.answer("Task not found.")
        return

    task = await task_manager.get_status(full_task_id)
    if not task or task.chat_id != message.chat.id or task.message_thread_id != thread_id_fn(message):
        await message.answer("Task not found.")
        return

    if task.status not in (task_status.QUEUED, task_status.RUNNING):
        await message.answer(f"Task is already {task.status.value}.")
        return

    cancelled = await task_manager.cancel(full_task_id)
    if cancelled:
        await message.answer(f"✅ Cancelled task <code>{full_task_id[:8]}</code>", parse_mode="HTML")
    else:
        await message.answer("Could not cancel task.")


async def cmd_schedule_every(
    message: Any,
    command: Any,
    *,
    is_authorized: Callable[[int | None, int | None], bool],
    schedule_manager: Any,
    command_args_fn: Callable[[Any, Any], str],
    thread_id_fn: Callable[[Any], int | None],
    session_manager: Any,
    current_provider_fn: Callable[[str], Any],
    scope_key_fn: Callable[[int, int | None], str],
    actor_id_fn: Callable[[Any], int | None],
    task_backend_fn: Callable[[Any, Any], tuple[str, str | None, str, str | None]],
    build_augmented_prompt_fn: Callable[[str], str],
) -> None:
    if not is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    if not schedule_manager:
        await message.answer("Scheduler not available.")
        return

    args = command_args_fn(message, command)
    parts = args.split(maxsplit=1) if args else []
    if len(parts) < 2:
        await message.answer(
            "Usage: /schedule_every <minutes> <task>\n"
            "Example: /schedule_every 60 summarize open PRs"
        )
        return

    try:
        interval_minutes = int(parts[0])
    except ValueError:
        await message.answer("Minutes must be an integer.")
        return

    if interval_minutes < 1 or interval_minutes > 10080:
        await message.answer("Minutes must be between 1 and 10080.")
        return

    task_text = parts[1].strip()
    if not task_text:
        await message.answer("Task text cannot be empty.")
        return

    chat_id = message.chat.id
    thread_id = thread_id_fn(message)
    session = session_manager.get(chat_id, thread_id)
    provider = current_provider_fn(scope_key_fn(chat_id, thread_id))
    task_model, session_id, provider_cli, resume_arg = task_backend_fn(session, provider)
    full_prompt = build_augmented_prompt_fn(
        task_text,
        chat_id=chat_id,
        message_thread_id=thread_id,
        scope_key=scope_key_fn(chat_id, thread_id),
        session=session,
    )

    schedule_id = await schedule_manager.create_every(
        chat_id=chat_id,
        message_thread_id=thread_id,
        user_id=actor_id_fn(message),
        prompt=full_prompt,
        interval_minutes=interval_minutes,
        model=task_model,
        session_id=session_id,
        provider_cli=provider_cli,
        resume_arg=resume_arg,
    )
    await message.answer(
        "✅ Recurring schedule created\n"
        f"<b>ID:</b> <code>{schedule_id[:8]}</code>\n"
        f"<b>Interval:</b> every {interval_minutes} min\n"
        "Use /schedule_list to view schedules.",
        parse_mode="HTML",
    )


async def cmd_schedule_list(
    message: Any,
    *,
    is_authorized: Callable[[int | None, int | None], bool],
    schedule_manager: Any,
    thread_id_fn: Callable[[Any], int | None],
    format_schedule_label_fn: Callable[[Any], str],
    format_active_schedule_summary_fn: Callable[[Any], str],
    format_schedule_run_summary_fn: Callable[[Any], str],
) -> None:
    if not is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    if not schedule_manager:
        await message.answer("Scheduler not available.")
        return

    schedules = await schedule_manager.list_for_chat(message.chat.id, thread_id_fn(message))
    if not schedules:
        await message.answer("No recurring schedules.")
        return

    latest_runs = await schedule_manager.latest_runs_by_schedule([item.id for item in schedules])
    lines = ["<b>Recurring schedules:</b>", ""]
    for item in schedules:
        next_run_local = item.next_run_at.astimezone().strftime("%Y-%m-%d %H:%M")
        schedule_label = format_schedule_label_fn(item)
        lines.append(f"⏱ <code>{item.id[:8]}</code> — {schedule_label}")
        lines.append(f"   next: {next_run_local}")
        if item.current_status:
            lines.append(f"   active: {format_active_schedule_summary_fn(item)}")
        latest_run = latest_runs.get(item.id)
        if latest_run:
            lines.append(f"   last: {format_schedule_run_summary_fn(latest_run)}")
        else:
            lines.append("   last: no executions yet")
        lines.append(f"   {item.prompt[:80]}...")
        lines.append("")
    await message.answer("\n".join(lines), parse_mode="HTML")


async def cmd_schedule_history(
    message: Any,
    command: Any,
    *,
    is_authorized: Callable[[int | None, int | None], bool],
    schedule_manager: Any,
    command_args_fn: Callable[[Any, Any], str],
    thread_id_fn: Callable[[Any], int | None],
    format_schedule_run_status_fn: Callable[[Any], str],
) -> None:
    if not is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    if not schedule_manager:
        await message.answer("Scheduler not available.")
        return

    short_id = command_args_fn(message, command).strip()
    schedule_id: str | None = None
    if short_id:
        schedule_id = await schedule_manager.find_schedule_id_for_chat(
            message.chat.id,
            short_id,
            thread_id_fn(message),
        )
        if not schedule_id:
            await message.answer("Schedule not found.")
            return

    runs = await schedule_manager.list_runs_for_chat(
        message.chat.id,
        thread_id_fn(message),
        schedule_id=schedule_id,
        limit=10,
    )
    if not runs:
        await message.answer("No scheduled job history yet.")
        return

    lines = ["<b>Scheduled job history:</b>", ""]
    for run in runs:
        lines.append(f"🕓 <code>{run.schedule_id[:8]}</code> — {format_schedule_run_status_fn(run)}")
        lines.append(f"   planned: {run.planned_for.astimezone().strftime('%Y-%m-%d %H:%M')}")
        if run.started_at:
            lines.append(f"   started: {run.started_at.astimezone().strftime('%Y-%m-%d %H:%M:%S')}")
        if run.completed_at:
            lines.append(f"   finished: {run.completed_at.astimezone().strftime('%Y-%m-%d %H:%M:%S')}")
        if run.background_task_id:
            lines.append(f"   task: <code>{run.background_task_id[:8]}</code>")
        detail = run.error_text or run.response_preview
        if detail:
            lines.append(f"   result: {html.escape(detail[:160])}")
        lines.append("")
    await message.answer("\n".join(lines), parse_mode="HTML")


async def cmd_schedule_weekly(
    message: Any,
    command: Any,
    *,
    is_authorized: Callable[[int | None, int | None], bool],
    schedule_manager: Any,
    command_args_fn: Callable[[Any, Any], str],
    weekday_to_int_fn: Callable[[str], int | None],
    default_timezone_name_fn: Callable[[], str],
    thread_id_fn: Callable[[Any], int | None],
    session_manager: Any,
    current_provider_fn: Callable[[str], Any],
    scope_key_fn: Callable[[int, int | None], str],
    task_backend_fn: Callable[[Any, Any], tuple[str, str | None, str, str | None]],
    build_augmented_prompt_fn: Callable[[str], str],
    actor_id_fn: Callable[[Any], int | None],
) -> None:
    if not is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    if not schedule_manager:
        await message.answer("Scheduler not available.")
        return

    args = command_args_fn(message, command)
    parts = args.split(maxsplit=2) if args else []
    if len(parts) < 3:
        await message.answer(
            "Usage: /schedule_weekly <day> <HH:MM> <task>\n"
            "Example: /schedule_weekly mon 09:00 check sprint board"
        )
        return

    weekday = weekday_to_int_fn(parts[0])
    if weekday is None:
        await message.answer("Day must be one of: mon,tue,wed,thu,fri,sat,sun.")
        return

    daily_time = parts[1].strip()
    if not re.fullmatch(r"([01]\d|2[0-3]):([0-5]\d)", daily_time):
        await message.answer("Time must be in HH:MM 24-hour format.")
        return

    task_text = parts[2].strip()
    if not task_text:
        await message.answer("Task text cannot be empty.")
        return

    timezone_name = default_timezone_name_fn()
    chat_id = message.chat.id
    thread_id = thread_id_fn(message)
    session = session_manager.get(chat_id, thread_id)
    provider = current_provider_fn(scope_key_fn(chat_id, thread_id))
    task_model, session_id, provider_cli, resume_arg = task_backend_fn(session, provider)
    full_prompt = build_augmented_prompt_fn(
        task_text,
        chat_id=chat_id,
        message_thread_id=thread_id,
        scope_key=scope_key_fn(chat_id, thread_id),
        session=session,
    )

    try:
        schedule_id = await schedule_manager.create_weekly(
            chat_id=chat_id,
            message_thread_id=thread_id,
            user_id=actor_id_fn(message),
            prompt=full_prompt,
            weekly_day=weekday,
            daily_time=daily_time,
            timezone_name=timezone_name,
            model=task_model,
            session_id=session_id,
            provider_cli=provider_cli,
            resume_arg=resume_arg,
        )
    except Exception as exc:
        await message.answer(f"Could not create weekly schedule: {exc}")
        return

    day_label = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][weekday]
    await message.answer(
        "✅ Weekly schedule created\n"
        f"<b>ID:</b> <code>{schedule_id[:8]}</code>\n"
        f"<b>Time:</b> {day_label} {daily_time} ({timezone_name})\n"
        "Use /schedule_list to view schedules.",
        parse_mode="HTML",
    )


async def cmd_schedule_daily(
    message: Any,
    command: Any,
    *,
    is_authorized: Callable[[int | None, int | None], bool],
    schedule_manager: Any,
    command_args_fn: Callable[[Any, Any], str],
    default_timezone_name_fn: Callable[[], str],
    thread_id_fn: Callable[[Any], int | None],
    session_manager: Any,
    current_provider_fn: Callable[[str], Any],
    scope_key_fn: Callable[[int, int | None], str],
    task_backend_fn: Callable[[Any, Any], tuple[str, str | None, str, str | None]],
    build_augmented_prompt_fn: Callable[[str], str],
    actor_id_fn: Callable[[Any], int | None],
) -> None:
    if not is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    if not schedule_manager:
        await message.answer("Scheduler not available.")
        return

    args = command_args_fn(message, command)
    parts = args.split(maxsplit=1) if args else []
    if len(parts) < 2:
        await message.answer(
            "Usage: /schedule_daily <HH:MM> <task>\n"
            "Example: /schedule_daily 09:00 check PR reviews"
        )
        return

    daily_time = parts[0].strip()
    if not re.fullmatch(r"([01]\d|2[0-3]):([0-5]\d)", daily_time):
        await message.answer("Time must be in HH:MM 24-hour format.")
        return

    task_text = parts[1].strip()
    if not task_text:
        await message.answer("Task text cannot be empty.")
        return

    timezone_name = default_timezone_name_fn()
    chat_id = message.chat.id
    thread_id = thread_id_fn(message)
    session = session_manager.get(chat_id, thread_id)
    provider = current_provider_fn(scope_key_fn(chat_id, thread_id))
    task_model, session_id, provider_cli, resume_arg = task_backend_fn(session, provider)
    full_prompt = build_augmented_prompt_fn(
        task_text,
        chat_id=chat_id,
        message_thread_id=thread_id,
        scope_key=scope_key_fn(chat_id, thread_id),
        session=session,
    )

    try:
        schedule_id = await schedule_manager.create_daily(
            chat_id=chat_id,
            message_thread_id=thread_id,
            user_id=actor_id_fn(message),
            prompt=full_prompt,
            daily_time=daily_time,
            timezone_name=timezone_name,
            model=task_model,
            session_id=session_id,
            provider_cli=provider_cli,
            resume_arg=resume_arg,
        )
    except Exception as exc:
        await message.answer(f"Could not create daily schedule: {exc}")
        return

    await message.answer(
        "✅ Daily schedule created\n"
        f"<b>ID:</b> <code>{schedule_id[:8]}</code>\n"
        f"<b>Time:</b> {daily_time} ({timezone_name})\n"
        "Use /schedule_list to view schedules.",
        parse_mode="HTML",
    )


async def cmd_schedule_cancel(
    message: Any,
    command: Any,
    *,
    is_authorized: Callable[[int | None, int | None], bool],
    schedule_manager: Any,
    command_args_fn: Callable[[Any, Any], str],
    thread_id_fn: Callable[[Any], int | None],
) -> None:
    if not is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    if not schedule_manager:
        await message.answer("Scheduler not available.")
        return

    short_id = command_args_fn(message, command)
    if not short_id:
        await message.answer("Usage: /schedule_cancel <schedule_id>")
        return

    schedules = await schedule_manager.list_for_chat(message.chat.id, thread_id_fn(message))
    target = next((s for s in schedules if s.id.startswith(short_id)), None)
    if not target:
        await message.answer("Schedule not found.")
        return

    cancelled = await schedule_manager.cancel(target.id)
    if cancelled:
        await message.answer(f"✅ Cancelled schedule <code>{target.id[:8]}</code>", parse_mode="HTML")
    else:
        await message.answer("Could not cancel schedule.")
