from __future__ import annotations

import asyncio
from time import monotonic
from typing import Any, Callable

from ..telegram_status_throttle import (
    EphemeralStatusSuppressedError,
    postpone_ephemeral_status_send,
    send_ephemeral_status,
)


def format_audio_conversion_progress(elapsed_seconds: float) -> str:
    return (
        "🎙️ <b>Converting audio reply...</b>\n"
        f"Elapsed: <code>{elapsed_seconds:.1f}s</code>"
    )


def format_voice_transcription_progress(elapsed_seconds: float) -> str:
    return (
        "🎤 <b>Transcribing voice message...</b>\n"
        f"Elapsed: <code>{elapsed_seconds:.1f}s</code>"
    )


def format_voice_transcription_complete(elapsed_seconds: float) -> str:
    return (
        "✅ <b>Voice message transcribed</b>\n"
        f"Transcription time: <code>{elapsed_seconds:.1f}s</code>"
    )


def format_voice_transcription_failed(elapsed_seconds: float) -> str:
    return (
        "❌ <b>Voice transcription failed</b>\n"
        f"Elapsed before failure: <code>{elapsed_seconds:.1f}s</code>"
    )


def format_audio_conversion_complete(elapsed_seconds: float) -> str:
    return (
        "✅ <b>Audio reply sent</b>\n"
        f"Conversion time: <code>{elapsed_seconds:.1f}s</code>"
    )


def format_audio_conversion_failed(elapsed_seconds: float) -> str:
    return (
        "❌ <b>Audio reply failed</b>\n"
        f"Elapsed before failure: <code>{elapsed_seconds:.1f}s</code>"
    )


async def answer_with_retry(
    send_callable,
    *args,
    floodwait_prefix: str,
    telegram_retry_after_class: type[Exception],
    logger: Any,
    **kwargs,
):
    while True:
        try:
            return await send_callable(*args, **kwargs)
        except telegram_retry_after_class as e:
            logger.warning("%s rate-limited, retry in %ss", floodwait_prefix, e.retry_after)
            await asyncio.sleep(max(0, e.retry_after))


async def answer_text_with_retry(
    message: Any,
    text: str,
    *,
    parse_mode: str | None = None,
    reply_markup=None,
    answer_with_retry_fn: Callable[..., Any],
) -> Any:
    kwargs = {}
    if parse_mode is not None:
        kwargs["parse_mode"] = parse_mode
    if reply_markup is not None:
        kwargs["reply_markup"] = reply_markup
    return await answer_with_retry_fn(
        message.answer,
        text,
        floodwait_prefix="Text reply",
        **kwargs,
    )


async def answer_voice_with_retry(
    message: Any,
    media_input: Any,
    *,
    answer_with_retry_fn: Callable[..., Any],
) -> Any:
    return await answer_with_retry_fn(
        message.answer_voice,
        media_input,
        floodwait_prefix="Voice reply",
    )


async def answer_audio_with_retry(
    message: Any,
    media_input: Any,
    *,
    answer_with_retry_fn: Callable[..., Any],
) -> Any:
    return await answer_with_retry_fn(
        message.answer_audio,
        media_input,
        floodwait_prefix="Audio reply",
    )


async def answer_document_with_retry(
    message: Any,
    media_input: Any,
    *,
    answer_with_retry_fn: Callable[..., Any],
) -> Any:
    return await answer_with_retry_fn(
        message.answer_document,
        media_input,
        floodwait_prefix="Document reply",
    )


async def send_media_reply(
    message: Any,
    media_ref: str,
    *,
    audio_as_voice: bool,
    prepared_media_input_fn: Callable[[str], Any],
    is_voice_compatible_media_fn: Callable[[str], bool],
    is_audio_media_fn: Callable[[str], bool],
    send_audio_with_progress_fn: Callable[[Any, Any], Any],
    answer_document_with_retry_fn: Callable[[Any, Any], Any],
) -> None:
    async with prepared_media_input_fn(media_ref) as media_input:
        if audio_as_voice and is_voice_compatible_media_fn(media_ref):
            await send_audio_with_progress_fn(message, media_input, as_voice=True)
            return
        if is_audio_media_fn(media_ref):
            await send_audio_with_progress_fn(message, media_input, as_voice=False)
            return
        await answer_document_with_retry_fn(message, media_input)


async def send_audio_with_progress(
    message: Any,
    media_input: Any,
    *,
    as_voice: bool,
    thread_id_fn: Callable[[Any], int | None],
    keep_chat_action_fn: Callable[[Any, Any], Any],
    chat_action_typing: Any,
    format_audio_conversion_progress_fn: Callable[[float], str],
    update_audio_conversion_progress_fn: Callable[[Any, int, float], Any],
    answer_voice_with_retry_fn: Callable[[Any, Any], Any],
    answer_audio_with_retry_fn: Callable[[Any, Any], Any],
    format_audio_conversion_complete_fn: Callable[[float], str],
    format_audio_conversion_failed_fn: Callable[[float], str],
    finalize_audio_conversion_progress_fn: Callable[[Any, int, str], Any],
    telegram_api_error_class: type[Exception],
    logger: Any,
) -> None:
    progress_message_id: int | None = None
    progress_task: asyncio.Task | None = None
    started_at = monotonic()
    completed = False
    typing_task = asyncio.create_task(keep_chat_action_fn(message, chat_action_typing))

    try:
        await asyncio.sleep(0)
        try:
            async def _send_progress_message():
                return await message.bot.send_message(
                    chat_id=message.chat.id,
                    message_thread_id=thread_id_fn(message),
                    text=format_audio_conversion_progress_fn(monotonic() - started_at),
                    parse_mode="HTML",
                )
            progress_message = await send_ephemeral_status(message.chat.id, _send_progress_message)
            progress_message_id = progress_message.message_id
            progress_task = asyncio.create_task(
                update_audio_conversion_progress_fn(message, progress_message_id, started_at)
            )
        except EphemeralStatusSuppressedError:
            logger.debug("Audio conversion progress suppressed by chat cooldown")
        except telegram_api_error_class as e:
            retry_after = getattr(e, "retry_after", None)
            if retry_after is not None:
                await postpone_ephemeral_status_send(message.chat.id, retry_after)
            logger.debug("Audio conversion progress message failed: %s", e)

        if as_voice:
            await answer_voice_with_retry_fn(message, media_input)
        else:
            await answer_audio_with_retry_fn(message, media_input)
        completed = True
    finally:
        elapsed_seconds = monotonic() - started_at
        typing_task.cancel()
        if progress_task is not None:
            progress_task.cancel()
        try:
            await typing_task
        except asyncio.CancelledError:
            pass
        if progress_task is not None:
            try:
                await progress_task
            except asyncio.CancelledError:
                pass
        if progress_message_id is not None:
            final_text = (
                format_audio_conversion_complete_fn(elapsed_seconds)
                if completed
                else format_audio_conversion_failed_fn(elapsed_seconds)
            )
            await finalize_audio_conversion_progress_fn(
                message,
                progress_message_id,
                final_text,
            )


async def update_audio_conversion_progress(
    message: Any,
    progress_message_id: int,
    started_at: float,
    *,
    audio_progress_update_interval: float,
    format_audio_conversion_progress_fn: Callable[[float], str],
    telegram_retry_after_class: type[Exception],
    telegram_api_error_class: type[Exception],
    logger: Any,
) -> None:
    try:
        while True:
            await asyncio.sleep(audio_progress_update_interval)
            try:
                await message.bot.edit_message_text(
                    chat_id=message.chat.id,
                    message_id=progress_message_id,
                    text=format_audio_conversion_progress_fn(monotonic() - started_at),
                    parse_mode="HTML",
                )
            except telegram_retry_after_class as e:
                logger.debug("Audio conversion progress rate-limited, retry in %ss", e.retry_after)
                await asyncio.sleep(max(0, e.retry_after))
            except telegram_api_error_class as e:
                if "message is not modified" not in str(e).lower():
                    logger.debug("Audio conversion progress update failed: %s", e)
                    return
    except asyncio.CancelledError:
        return


async def finalize_audio_conversion_progress(
    message: Any,
    progress_message_id: int,
    text: str,
    *,
    telegram_retry_after_class: type[Exception],
    telegram_api_error_class: type[Exception],
    logger: Any,
) -> None:
    try:
        while True:
            try:
                await message.bot.edit_message_text(
                    chat_id=message.chat.id,
                    message_id=progress_message_id,
                    text=text,
                    parse_mode="HTML",
                )
                return
            except telegram_retry_after_class as e:
                logger.debug(
                    "Audio conversion finalization rate-limited, retry in %ss",
                    e.retry_after,
                )
                await asyncio.sleep(max(0, e.retry_after))
            except telegram_api_error_class as e:
                logger.debug("Could not finalize audio conversion progress message: %s", e)
                return
    except asyncio.CancelledError:
        return


async def send_voice_transcription_progress_message(
    message: Any,
    elapsed_seconds: float,
    *,
    thread_id_fn: Callable[[Any], int | None],
    format_voice_transcription_progress_fn: Callable[[float], str],
    telegram_retry_after_class: type[Exception],
    telegram_api_error_class: type[Exception],
    logger: Any,
) -> tuple[int | None, int | None]:
    try:
        async def _send_progress_message():
            return await message.bot.send_message(
                chat_id=message.chat.id,
                message_thread_id=thread_id_fn(message),
                text=format_voice_transcription_progress_fn(elapsed_seconds),
                parse_mode="HTML",
            )
        progress_message = await send_ephemeral_status(message.chat.id, _send_progress_message)
        return progress_message.message_id, None
    except EphemeralStatusSuppressedError:
        return None, None
    except telegram_retry_after_class as e:
        await postpone_ephemeral_status_send(message.chat.id, e.retry_after)
        logger.debug("Voice transcription progress rate-limited, retry in %ss", e.retry_after)
        return None, e.retry_after
    except telegram_api_error_class as e:
        logger.debug("Voice transcription progress message failed: %s", e)
        return None, None


async def update_voice_transcription_progress(
    message: Any,
    progress_message_id: int,
    started_at: float,
    *,
    voice_transcription_progress_interval: float,
    format_voice_transcription_progress_fn: Callable[[float], str],
    telegram_retry_after_class: type[Exception],
    telegram_api_error_class: type[Exception],
    logger: Any,
) -> None:
    try:
        while True:
            await asyncio.sleep(voice_transcription_progress_interval)
            try:
                await message.bot.edit_message_text(
                    chat_id=message.chat.id,
                    message_id=progress_message_id,
                    text=format_voice_transcription_progress_fn(monotonic() - started_at),
                    parse_mode="HTML",
                )
            except telegram_retry_after_class as e:
                logger.debug("Voice transcription progress rate-limited, retry in %ss", e.retry_after)
                await asyncio.sleep(max(0, e.retry_after))
            except telegram_api_error_class as e:
                if "message is not modified" not in str(e).lower():
                    logger.debug("Voice transcription progress update failed: %s", e)
                    return
    except asyncio.CancelledError:
        return


async def publish_voice_transcription_result(
    message: Any,
    *,
    progress_message_id: int | None,
    text: str,
    send_summary: bool,
    answer_text_with_retry_fn: Callable[[Any, str], Any],
    telegram_api_error_class: type[Exception],
    logger: Any,
) -> None:
    if progress_message_id is not None:
        try:
            await message.bot.delete_message(
                chat_id=message.chat.id,
                message_id=progress_message_id,
            )
        except telegram_api_error_class as e:
            logger.debug("Could not delete voice transcription progress message: %s", e)

    if not send_summary:
        return

    try:
        await answer_text_with_retry_fn(message, text, parse_mode="HTML")
    except telegram_api_error_class as e:
        logger.debug("Could not send voice transcription summary message: %s", e)


async def retry_voice_transcription_progress_message(
    message: Any,
    transcription_status_ref: dict[str, int | None],
    started_at: float,
    retry_after: int,
    *,
    send_voice_transcription_progress_message_fn: Callable[[Any, float], Any],
    update_voice_transcription_progress_fn: Callable[[Any, int, float], Any],
) -> None:
    try:
        await asyncio.sleep(max(0, retry_after))
        progress_message_id, next_retry_after = await send_voice_transcription_progress_message_fn(
            message,
            monotonic() - started_at,
        )
        if progress_message_id is not None:
            transcription_status_ref["message_id"] = progress_message_id
            await update_voice_transcription_progress_fn(message, progress_message_id, started_at)
            return
        if next_retry_after is not None:
            await retry_voice_transcription_progress_message(
                message,
                transcription_status_ref,
                started_at,
                next_retry_after,
                send_voice_transcription_progress_message_fn=send_voice_transcription_progress_message_fn,
                update_voice_transcription_progress_fn=update_voice_transcription_progress_fn,
            )
    except asyncio.CancelledError:
        return
