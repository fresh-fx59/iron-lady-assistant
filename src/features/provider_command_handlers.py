from __future__ import annotations

from typing import Any, Callable

from aiogram.utils.keyboard import InlineKeyboardBuilder


def _is_message_not_modified_error(exc: Exception) -> bool:
    return "message is not modified" in str(exc).lower()


async def cmd_model(
    message: Any,
    command: Any,
    *,
    is_authorized: Callable[[int | None, int | None], bool],
    thread_id_fn: Callable[[Any], int | None],
    scope_key_fn: Callable[[int, int | None], str],
    current_provider_fn: Callable[[str], Any],
    current_model_label_fn: Callable[[Any, Any], str],
    command_args_fn: Callable[[Any, Any], str],
    model_options_fn: Callable[[Any], list[str]],
    is_codex_family_cli_fn: Callable[[str | None], bool],
    session_manager: Any,
) -> None:
    if not is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    chat_id = message.chat.id
    thread_id = thread_id_fn(message)
    scope_key = scope_key_fn(chat_id, thread_id)
    session = session_manager.get(chat_id, thread_id)
    provider = current_provider_fn(scope_key)
    current = current_model_label_fn(session, provider)

    args = command_args_fn(message, command)
    if args:
        requested = args.split()[0]
        options = model_options_fn(provider)
        if requested not in options:
            await message.answer(f"Invalid model: {requested}. Use /model to see options.")
            return

        if is_codex_family_cli_fn(provider.cli):
            chosen = None if requested == "default" else requested
            session_manager.set_codex_model(chat_id, chosen, thread_id)
        else:
            session_manager.set_model(chat_id, requested, thread_id)

        current = current_model_label_fn(session_manager.get(chat_id, thread_id), provider)
        await message.answer(f"Switched to {current}")
        return

    lines = [f"<b>Current model:</b> {current}\n", "<b>Select a model:</b>"]
    keyboard = InlineKeyboardBuilder()
    for model in model_options_fn(provider):
        button_text = f"{'✓ ' if model == current else ''}{model}"
        keyboard.button(text=button_text, callback_data=f"model:{model}")
    keyboard.adjust(2)
    await message.answer("\n".join(lines), reply_markup=keyboard.as_markup(), parse_mode="HTML")


async def cb_model_switch(
    callback: Any,
    *,
    is_authorized: Callable[[int | None, int | None], bool],
    thread_id_fn: Callable[[Any], int | None],
    scope_key_fn: Callable[[int, int | None], str],
    current_provider_fn: Callable[[str], Any],
    model_options_fn: Callable[[Any], list[str]],
    is_codex_family_cli_fn: Callable[[str | None], bool],
    current_model_label_fn: Callable[[Any, Any], str],
    session_manager: Any,
    logger: Any,
) -> None:
    if not callback.message:
        return
    if not is_authorized(callback.from_user and callback.from_user.id, callback.message.chat.id):
        return

    chat_id = callback.message.chat.id
    thread_id = thread_id_fn(callback.message)
    scope_key = scope_key_fn(chat_id, thread_id)
    model = callback.data.split(":", 1)[1]
    logger.info("Chat %s: model selection 'model:%s'", scope_key, model)

    provider = current_provider_fn(scope_key)
    options = model_options_fn(provider)
    if model not in options:
        await callback.answer("Invalid model", show_alert=True)
        return

    if is_codex_family_cli_fn(provider.cli):
        chosen = None if model == "default" else model
        session_manager.set_codex_model(chat_id, chosen, thread_id)
    else:
        session_manager.set_model(chat_id, model, thread_id)

    current = current_model_label_fn(session_manager.get(chat_id, thread_id), provider)
    lines = [f"<b>Current model:</b> {current}\n", "<b>Select a model:</b>"]
    keyboard = InlineKeyboardBuilder()
    for option in options:
        button_text = f"{'✓ ' if option == current else ''}{option}"
        keyboard.button(text=button_text, callback_data=f"model:{option}")
    keyboard.adjust(2)

    try:
        await callback.message.edit_text(
            "\n".join(lines),
            reply_markup=keyboard.as_markup(),
            parse_mode="HTML",
        )
    except Exception as exc:
        if not _is_message_not_modified_error(exc):
            raise
        logger.info("Chat %s: skipped unchanged model selector refresh for '%s'", scope_key, current)
    await callback.answer(f"Switched to {current}")


async def cmd_provider(
    message: Any,
    command: Any,
    *,
    is_authorized: Callable[[int | None, int | None], bool],
    thread_id_fn: Callable[[Any], int | None],
    scope_key_from_message_fn: Callable[[Any], str],
    command_args_fn: Callable[[Any, Any], str],
    provider_manager: Any,
    session_manager: Any,
) -> None:
    if not is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return

    chat_id = message.chat.id
    thread_id = thread_id_fn(message)
    scope_key = scope_key_from_message_fn(message)
    requested = command_args_fn(message, command)
    if requested:
        provider = provider_manager.set_provider(scope_key, requested)
        if not provider:
            available = ", ".join(p.name for p in provider_manager.providers)
            await message.answer(f"Provider not found: {requested}\nAvailable: {available}")
            return
        session_manager.set_provider(chat_id, provider.name, thread_id)
        await message.answer(f"Switched to provider: <b>{provider.name}</b>", parse_mode="HTML")
        return

    current = provider_manager.get_provider(scope_key)
    lines = [f"<b>Current provider:</b> {current.name}\n<i>{current.description}</i>\n", "<b>Select a provider:</b>"]
    keyboard = InlineKeyboardBuilder()
    for provider in provider_manager.providers:
        button_text = f"{'✓ ' if provider.name == current.name else ''}{provider.name}"
        keyboard.button(text=button_text, callback_data=f"provider:{provider.name}")
    keyboard.adjust(2)
    await message.answer("\n".join(lines), reply_markup=keyboard.as_markup(), parse_mode="HTML")


async def cb_provider_switch(
    callback: Any,
    *,
    is_authorized: Callable[[int | None, int | None], bool],
    thread_id_fn: Callable[[Any], int | None],
    scope_key_fn: Callable[[int, int | None], str],
    provider_manager: Any,
    session_manager: Any,
    logger: Any,
) -> None:
    if not callback.message:
        return
    if not is_authorized(callback.from_user and callback.from_user.id, callback.message.chat.id):
        return

    chat_id = callback.message.chat.id
    thread_id = thread_id_fn(callback.message)
    scope_key = scope_key_fn(chat_id, thread_id)
    name = callback.data.split(":", 1)[1]
    logger.info("Chat %s: provider selection 'provider:%s'", scope_key, name)

    provider = provider_manager.set_provider(scope_key, name)
    if not provider:
        await callback.answer("Provider not found", show_alert=True)
        return

    session_manager.set_provider(chat_id, provider.name, thread_id)

    lines = [f"<b>Current provider:</b> {provider.name}\n<i>{provider.description}</i>\n", "<b>Select a provider:</b>"]
    keyboard = InlineKeyboardBuilder()
    for option in provider_manager.providers:
        button_text = f"{'✓ ' if option.name == provider.name else ''}{option.name}"
        keyboard.button(text=button_text, callback_data=f"provider:{option.name}")
    keyboard.adjust(2)

    try:
        await callback.message.edit_text(
            "\n".join(lines),
            reply_markup=keyboard.as_markup(),
            parse_mode="HTML",
        )
    except Exception as exc:
        if not _is_message_not_modified_error(exc):
            raise
        logger.info("Chat %s: skipped unchanged provider selector refresh for '%s'", scope_key, provider.name)
    await callback.answer(f"Switched to {provider.name}")
