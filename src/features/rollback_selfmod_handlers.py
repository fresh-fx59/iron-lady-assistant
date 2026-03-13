from __future__ import annotations

import asyncio
import html
from typing import Any, Callable

from aiogram.utils.keyboard import InlineKeyboardBuilder


async def cmd_rollback(
    message: Any,
    *,
    is_admin: Callable[[int | None], bool],
    show_rollback_options_fn: Callable[[int, Any, int | None], Any],
    thread_id_fn: Callable[[Any], int | None],
) -> None:
    if not is_admin(message.from_user and message.from_user.id):
        await message.answer("This command is admin-only.")
        return
    await show_rollback_options_fn(message.chat.id, message.bot, thread_id_fn(message))


async def cb_rollback_auto(
    callback: Any,
    *,
    is_admin: Callable[[int | None], bool],
    show_rollback_options_fn: Callable[[int, Any, int | None], Any],
    thread_id_fn: Callable[[Any], int | None],
) -> None:
    if not is_admin(callback.from_user and callback.from_user.id):
        await callback.answer("Admin only", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    await callback.answer()
    await show_rollback_options_fn(
        callback.message.chat.id,
        callback.bot,
        thread_id_fn(callback.message),
    )


async def cb_rollback(
    callback: Any,
    *,
    is_admin: Callable[[int | None], bool],
) -> None:
    if not is_admin(callback.from_user and callback.from_user.id):
        await callback.answer("Admin only", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    target_hash = callback.data.split(":", 1)[1]
    short_hash = target_hash[:8]

    kb = InlineKeyboardBuilder()
    kb.button(text=f"Yes, rollback to {short_hash}", callback_data=f"rollback_confirm:{target_hash}")
    kb.button(text="No, cancel", callback_data="rollback_cancel")
    kb.adjust(1)

    await callback.message.edit_text(
        f"Rollback to commit <code>{short_hash}</code>?\n\nThis will reset the repo and restart the bot service.",
        parse_mode="HTML",
        reply_markup=kb.as_markup(),
    )
    await callback.answer()


async def cb_rollback_confirm(
    callback: Any,
    *,
    is_admin: Callable[[int | None], bool],
    reset_to_commit_fn: Callable[[str], tuple[bool, str]],
    clear_errors_fn: Callable[[str], None],
    scope_key_from_message_fn: Callable[[Any], str],
    restart_service_fn: Callable[[int, Any, int | None], Any],
    thread_id_fn: Callable[[Any], int | None],
) -> None:
    if not is_admin(callback.from_user and callback.from_user.id):
        await callback.answer("Admin only", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    target_hash = callback.data.split(":", 1)[1]
    short_hash = target_hash[:8]
    await callback.answer()
    await callback.message.edit_text(
        f"Rolling back to <code>{short_hash}</code>...",
        parse_mode="HTML",
    )

    ok, details = await asyncio.to_thread(reset_to_commit_fn, target_hash)
    if not ok:
        await callback.message.answer(f"Rollback failed: {details}")
        return

    clear_errors_fn(scope_key_from_message_fn(callback.message))
    await callback.message.answer(
        f"Rollback complete: <code>{short_hash}</code>\nRestarting <code>telegram-bot.service</code>...",
        parse_mode="HTML",
    )
    asyncio.create_task(
        restart_service_fn(callback.message.chat.id, callback.bot, thread_id_fn(callback.message))
    )


async def cb_rollback_cancel(callback: Any) -> None:
    await callback.answer("Rollback cancelled")
    if callback.message:
        await callback.message.edit_text("Rollback cancelled.")


async def cmd_selfmod_stage(
    message: Any,
    command: Any,
    *,
    is_admin: Callable[[int | None], bool],
    command_args_fn: Callable[[Any, Any], str],
    strip_markdown_code_fence_fn: Callable[[str], str],
    self_mod_manager: Any,
) -> None:
    if not is_admin(message.from_user and message.from_user.id):
        await message.answer("This command is admin-only.")
        return

    text = message.text or ""
    header, sep, body = text.partition("\n")
    if command is not None:
        relative_path = command_args_fn(message, command)
    else:
        header_parts = header.split(maxsplit=1)
        relative_path = header_parts[1].strip() if len(header_parts) > 1 else ""
    if not relative_path:
        await message.answer(
            "Usage:\n"
            "/selfmod_stage <relative_plugin_path.py>\n"
            "```python\n# plugin code here\n```",
            parse_mode="Markdown",
        )
        return
    if not sep or not body.strip():
        await message.answer("Provide plugin code on lines after the command.")
        return

    plugin_code = strip_markdown_code_fence_fn(body)
    if not plugin_code:
        await message.answer("Plugin code is empty after parsing.")
        return

    try:
        staged_path = await asyncio.to_thread(
            self_mod_manager.stage_plugin,
            relative_path,
            plugin_code + ("\n" if not plugin_code.endswith("\n") else ""),
        )
    except Exception as exc:
        await message.answer(f"Staging failed: {exc}")
        return

    await message.answer(
        "✅ Staged plugin candidate\n"
        f"<b>Path:</b> <code>{relative_path}</code>\n"
        f"<b>Sandbox file:</b> <code>{staged_path}</code>\n"
        "Next: run /selfmod_apply with this path.",
        parse_mode="HTML",
    )


async def cmd_selfmod_apply(
    message: Any,
    command: Any,
    *,
    is_admin: Callable[[int | None], bool],
    command_args_fn: Callable[[Any, Any], str],
    scope_key_from_message_fn: Callable[[Any], str],
    f08_advisory: Any,
    self_mod_manager: Any,
    truncate_output_fn: Callable[[str], str],
    reload_tooling_fn: Callable[[], None],
) -> None:
    if not is_admin(message.from_user and message.from_user.id):
        await message.answer("This command is admin-only.")
        return

    args = command_args_fn(message, command)
    if not args:
        await message.answer(
            "Usage: /selfmod_apply <relative_plugin_path.py> [test_target]\n"
            "Example: /selfmod_apply tools_plugin.py tests/test_context_plugins.py"
        )
        return

    parts = args.split(maxsplit=1)
    relative_path = parts[0].strip()
    test_target = parts[1].strip() if len(parts) > 1 else "tests/test_context_plugins.py"
    f08_advisory.submit_selfmod_apply(
        scope_key=scope_key_from_message_fn(message),
        relative_path=relative_path,
        test_target=test_target,
    )

    await message.answer(
        f"Applying sandbox candidate <code>{relative_path}</code>\n"
        f"Validation target: <code>{test_target}</code>",
        parse_mode="HTML",
    )

    result = await asyncio.to_thread(
        self_mod_manager.apply_candidate,
        relative_path,
        test_target,
    )

    validation_text = result.validation_output or "(no output)"
    status = "✅ <b>Self-mod apply succeeded</b>" if result.ok else "❌ <b>Self-mod apply failed</b>"
    lines = [
        status,
        f"<b>Result:</b> {result.message}",
        "",
        "<b>Validation output:</b>",
        f"<pre>{html.escape(truncate_output_fn(validation_text))}</pre>",
    ]
    await message.answer("\n".join(lines), parse_mode="HTML")

    if result.ok:
        reload_tooling_fn()
