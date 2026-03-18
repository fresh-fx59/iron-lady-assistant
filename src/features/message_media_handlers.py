from __future__ import annotations

import asyncio
import os
import tempfile
from time import monotonic
from typing import Any, Callable


async def handle_voice(
    message: Any,
    *,
    is_authorized: Callable[[int | None, int | None], bool],
    log_incoming_message_fn: Callable[[Any, str], None],
    logger: Any,
    thread_id_fn: Callable[[Any], int | None],
    transcribe_module: Any,
    send_chat_action_once_fn: Callable[[Any, Any], Any],
    keep_chat_action_fn: Callable[[Any, Any], Any],
    chat_action_typing: Any,
    send_voice_transcription_progress_message_fn: Callable[[Any, float], Any],
    update_voice_transcription_progress_fn: Callable[[Any, int, float], Any],
    retry_voice_transcription_progress_message_fn: Callable[[Any, dict[str, int | None], float, int], Any],
    publish_voice_transcription_result_fn: Callable[[Any], Any],
    format_voice_transcription_complete_fn: Callable[[float], str],
    format_voice_transcription_failed_fn: Callable[[float], str],
    handle_message_inner_fn: Callable[[Any, str | None], Any],
    scope_key_from_message_fn: Callable[[Any], str],
    actor_id_fn: Callable[[Any], int],
    lifecycle_upsert_active_scope_fn: Callable[..., None],
    lifecycle_clear_active_scope_fn: Callable[[str], None],
    record_error_fn: Callable[[str], None],
    metrics: Any,
    telegram_api_error_class: type[Exception],
) -> None:
    if not is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return

    log_incoming_message_fn(message, "voice")
    logger.info(
        "Entering handle_voice: chat=%s thread=%s message=%s",
        message.chat.id,
        thread_id_fn(message),
        message.message_id,
    )

    if not transcribe_module.is_available():
        await message.answer(
            "Voice messages are not supported — whisper.cpp is not installed.\n"
            "Run <code>bash setup_whisper.sh</code> on the server to enable.",
            parse_mode="HTML",
        )
        return

    transcription_started_at = monotonic()
    source_message_id = getattr(message, "message_id", None)
    transcription_scope_key = (
        f"{scope_key_from_message_fn(message)}:voice:{source_message_id}"
        if source_message_id is not None
        else f"{scope_key_from_message_fn(message)}:voice:pending"
    )
    transcription_status_ref: dict[str, int | None] = {"message_id": None}
    transcription_status_task: asyncio.Task | None = None
    transcription_status_retry_task: asyncio.Task | None = None
    transcription_completed = False
    transcription_failed_notified = False
    lifecycle_upsert_active_scope_fn(
        scope_key=transcription_scope_key,
        chat_id=message.chat.id,
        message_thread_id=thread_id_fn(message),
        user_id=actor_id_fn(message),
        kind="voice_transcription",
        prompt_preview=f"voice:{message.voice.duration}s",
        source_message_id=source_message_id,
    )
    file_lookup_started_at = monotonic()
    file = await message.bot.get_file(message.voice.file_id)
    file_lookup_elapsed_ms = (monotonic() - file_lookup_started_at) * 1000
    tmp = tempfile.NamedTemporaryFile(suffix=".oga", delete=False)
    await send_chat_action_once_fn(message, chat_action_typing)
    transcription_typing_task = asyncio.create_task(keep_chat_action_fn(message, chat_action_typing))
    try:
        await asyncio.sleep(0)
        (
            transcription_status_message_id,
            transcription_retry_after,
        ) = await send_voice_transcription_progress_message_fn(
            message,
            monotonic() - transcription_started_at,
        )
        transcription_status_ref["message_id"] = transcription_status_message_id
        if transcription_status_message_id is not None:
            transcription_status_task = asyncio.create_task(
                update_voice_transcription_progress_fn(
                    message,
                    transcription_status_message_id,
                    transcription_started_at,
                )
            )
        elif transcription_retry_after is not None:
            transcription_status_retry_task = asyncio.create_task(
                retry_voice_transcription_progress_message_fn(
                    message,
                    transcription_status_ref,
                    transcription_started_at,
                    transcription_retry_after,
                )
            )
        download_started_at = monotonic()
        await message.bot.download_file(file.file_path, tmp.name)
        download_elapsed_ms = (monotonic() - download_started_at) * 1000
        transcribe_started_at = monotonic()
        text = await transcribe_module.transcribe(tmp.name)
        transcribe_elapsed_ms = (monotonic() - transcribe_started_at) * 1000
        transcription_completed = True
        total_pre_llm_elapsed_ms = (monotonic() - transcription_started_at) * 1000
        logger.info("Chat %d: transcribed voice (%ds) → %d chars",
                     message.chat.id, message.voice.duration, len(text))
        logger.info(
            "Voice pipeline timings: chat=%s thread=%s message=%s voice_duration_s=%s "
            "file_lookup_ms=%.1f download_ms=%.1f transcribe_call_ms=%.1f total_pre_llm_ms=%.1f "
            "temp_audio=%s",
            message.chat.id,
            thread_id_fn(message),
            message.message_id,
            message.voice.duration,
            file_lookup_elapsed_ms,
            download_elapsed_ms,
            transcribe_elapsed_ms,
            total_pre_llm_elapsed_ms,
            os.path.basename(tmp.name),
        )
    except Exception:
        logger.exception("Voice transcription failed")
        transcription_failed_notified = True
        await message.answer("Failed to transcribe voice message.")
        return
    finally:
        transcription_typing_task.cancel()
        try:
            await transcription_typing_task
        except asyncio.CancelledError:
            pass
        if transcription_status_task is not None:
            transcription_status_task.cancel()
            try:
                await transcription_status_task
            except asyncio.CancelledError:
                pass
        if transcription_status_retry_task is not None:
            transcription_status_retry_task.cancel()
            try:
                await transcription_status_retry_task
            except asyncio.CancelledError:
                pass
        transcription_elapsed_seconds = monotonic() - transcription_started_at
        transcription_status_message_id = transcription_status_ref["message_id"]
        transcription_final_text = (
            format_voice_transcription_complete_fn(transcription_elapsed_seconds)
            if transcription_completed
            else format_voice_transcription_failed_fn(transcription_elapsed_seconds)
        )
        try:
            await publish_voice_transcription_result_fn(
                message=message,
                progress_message_id=transcription_status_message_id,
                text=transcription_final_text,
                send_summary=transcription_completed or not transcription_failed_notified,
            )
        finally:
            lifecycle_clear_active_scope_fn(transcription_scope_key)
            os.unlink(tmp.name)

    override = f"[Voice message] {text}"
    try:
        await handle_message_inner_fn(message, override)
    except telegram_api_error_class:
        logger.exception("Voice response delivery failed after transcription")
        metrics.MESSAGES_TOTAL.labels(status="error").inc()
        record_error_fn(scope_key_from_message_fn(message))
    except Exception:
        logger.exception("Unhandled exception in handle_voice")
        metrics.MESSAGES_TOTAL.labels(status="error").inc()
        record_error_fn(scope_key_from_message_fn(message))
        try:
            await message.answer("An internal error occurred while processing your voice message.")
        except telegram_api_error_class:
            logger.exception("Voice fallback error delivery failed")


async def handle_text_message(
    message: Any,
    *,
    log_incoming_message_fn: Callable[[Any, str], None],
    logger: Any,
    thread_id_fn: Callable[[Any], int | None],
    handle_message_inner_fn: Callable[[Any, str | None], Any],
    metrics: Any,
    scope_key_from_message_fn: Callable[[Any], str],
    record_error_fn: Callable[[str], None],
    build_rollback_suggestion_markup_fn: Callable[[str, int | None], Any],
) -> None:
    log_incoming_message_fn(message, "text")
    logger.info(
        "Entering handle_message: chat=%s thread=%s message=%s",
        message.chat.id,
        thread_id_fn(message),
        message.message_id,
    )
    try:
        await handle_message_inner_fn(message)
    except Exception:
        logger.exception("Unhandled exception in handle_message")
        metrics.MESSAGES_TOTAL.labels(status="error").inc()
        scope_key = scope_key_from_message_fn(message)
        record_error_fn(scope_key)
        reply_markup = build_rollback_suggestion_markup_fn(
            scope_key,
            message.from_user and message.from_user.id,
        )
        await message.answer(
            "An internal error occurred while processing your request.",
            reply_markup=reply_markup,
        )


async def handle_photo_message(
    message: Any,
    *,
    log_incoming_message_fn: Callable[[Any, str], None],
    logger: Any,
    thread_id_fn: Callable[[Any], int | None],
    handle_message_inner_fn: Callable[[Any, str | None], Any],
    metrics: Any,
    scope_key_from_message_fn: Callable[[Any], str],
    record_error_fn: Callable[[str], None],
    build_rollback_suggestion_markup_fn: Callable[[str, int | None], Any],
) -> None:
    log_incoming_message_fn(message, "photo")
    logger.info(
        "Entering handle_photo_message: chat=%s thread=%s message=%s",
        message.chat.id,
        thread_id_fn(message),
        message.message_id,
    )
    try:
        await handle_message_inner_fn(message)
    except Exception:
        logger.exception("Unhandled exception in handle_photo_message")
        metrics.MESSAGES_TOTAL.labels(status="error").inc()
        scope_key = scope_key_from_message_fn(message)
        record_error_fn(scope_key)
        reply_markup = build_rollback_suggestion_markup_fn(
            scope_key,
            message.from_user and message.from_user.id,
        )
        await message.answer(
            "An internal error occurred while processing your request.",
            reply_markup=reply_markup,
        )


async def handle_document_message(
    message: Any,
    *,
    log_incoming_message_fn: Callable[[Any, str], None],
    logger: Any,
    thread_id_fn: Callable[[Any], int | None],
    handle_message_inner_fn: Callable[[Any, str | None], Any],
    metrics: Any,
    scope_key_from_message_fn: Callable[[Any], str],
    record_error_fn: Callable[[str], None],
    build_rollback_suggestion_markup_fn: Callable[[str, int | None], Any],
) -> None:
    log_incoming_message_fn(message, "document")
    logger.info(
        "Entering handle_document_message: chat=%s thread=%s message=%s",
        message.chat.id,
        thread_id_fn(message),
        message.message_id,
    )
    try:
        await handle_message_inner_fn(message)
    except Exception:
        logger.exception("Unhandled exception in handle_document_message")
        metrics.MESSAGES_TOTAL.labels(status="error").inc()
        scope_key = scope_key_from_message_fn(message)
        record_error_fn(scope_key)
        reply_markup = build_rollback_suggestion_markup_fn(
            scope_key,
            message.from_user and message.from_user.id,
        )
        await message.answer(
            "An internal error occurred while processing your request.",
            reply_markup=reply_markup,
        )


async def handle_forum_topic_created(
    message: Any,
    *,
    is_authorized: Callable[[int | None, int | None], bool],
    touch_thread_context_fn: Callable[[Any], None],
) -> None:
    if not is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    touch_thread_context_fn(message)


async def handle_forum_topic_edited(
    message: Any,
    *,
    is_authorized: Callable[[int | None, int | None], bool],
    touch_thread_context_fn: Callable[[Any], None],
) -> None:
    if not is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return
    touch_thread_context_fn(message)
