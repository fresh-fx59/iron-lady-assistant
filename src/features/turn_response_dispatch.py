from __future__ import annotations

from typing import Any, Callable

from ..provider_errors import humanize_provider_api_error


async def dispatch_turn_response(
    *,
    message: Any,
    state: Any,
    final_response: Any,
    progress: Any,
    scope_key: str,
    provider: Any,
    resume_state_store: Any,
    record_error_fn: Callable[[str], None],
    build_rollback_suggestion_markup_fn: Callable[[str, int | None], Any],
    answer_text_with_retry_fn: Callable[..., Any],
    extract_media_directives_fn: Callable[[str], tuple[str, list[str], bool]],
    strip_tool_directive_lines_fn: Callable[[str], str],
    send_media_reply_fn: Callable[..., Any],
    markdown_to_html_fn: Callable[[str], str],
    split_message_fn: Callable[[str], list[str]],
    strip_html_fn: Callable[[str], str],
    has_recent_outbound_fn: Callable[[str, str], bool],
    remember_outbound_fn: Callable[[str, str], None],
    clear_errors_fn: Callable[[str], None],
    empty_response_fallback_text: str,
    logger: Any,
) -> tuple[bool, int]:
    response_has_user_content = False
    output_size_out = 0

    if state.cancel_requested:
        await progress.finish()
        clear_errors_fn(scope_key)
        return response_has_user_content, output_size_out

    if not final_response:
        record_error_fn(scope_key)
        reply_markup = build_rollback_suggestion_markup_fn(
            scope_key,
            message.from_user and message.from_user.id,
        )
        await answer_text_with_retry_fn(
            message,
            "An internal error occurred while processing your request.",
            reply_markup=reply_markup,
        )
        await progress.finish()
        return response_has_user_content, output_size_out

    if final_response.is_error:
        resume_state_store.record_failure(scope_key=scope_key)
        error_text = final_response.text or "(No response)"
        logger.warning(
            "Chat %d: provider '%s' returned error response: %r",
            message.chat.id,
            provider.name,
            error_text[:500],
        )
        # Never leak a raw upstream provider JSON envelope (with internal
        # request ids) to the user; show the clean inner message instead.
        humanized = humanize_provider_api_error(error_text)
        if humanized:
            error_text = humanized
        record_error_fn(scope_key)
        reply_markup = build_rollback_suggestion_markup_fn(
            scope_key,
            message.from_user and message.from_user.id,
        )
        await answer_text_with_retry_fn(
            message,
            error_text,
            reply_markup=reply_markup,
        )
        await progress.finish()
        return response_has_user_content, output_size_out

    resume_state_store.record_success(
        scope_key=scope_key,
        output_text=final_response.text or "",
    )
    raw_response_text = final_response.text or ""
    clean_text, media_refs, audio_as_voice = extract_media_directives_fn(raw_response_text)
    clean_text = strip_tool_directive_lines_fn(clean_text)
    response_has_user_content = bool(clean_text.strip() or media_refs)
    output_size_out = len(clean_text)
    for media_ref in media_refs:
        try:
            await send_media_reply_fn(
                message,
                media_ref,
                audio_as_voice=audio_as_voice,
            )
        except Exception:
            logger.exception(
                "Chat %d: failed to send media '%s'",
                message.chat.id,
                media_ref,
            )

    chunks: list[str] = []
    if clean_text.strip():
        html = markdown_to_html_fn(clean_text)
        chunks = split_message_fn(html)

    if not chunks and not media_refs:
        logger.warning(
            "Chat %d: Got empty response object - text='%s', is_error=%s, session_id=%s, cost=%.6f",
            message.chat.id,
            repr(final_response.text[:200]) if final_response.text else "None",
            final_response.is_error,
            final_response.session_id,
            final_response.cost_usd,
        )
        chunks = [empty_response_fallback_text]

    for chunk in chunks:
        if not chunk.strip():
            continue
        plain_preview = strip_html_fn(chunk)
        if has_recent_outbound_fn(scope_key, plain_preview):
            logger.info("Chat %s: suppressed duplicate outgoing chunk", scope_key)
            continue
        try:
            await answer_text_with_retry_fn(
                message,
                chunk,
                parse_mode="HTML",
            )
            remember_outbound_fn(scope_key, plain_preview)
        except Exception:
            plain = strip_html_fn(chunk)
            for plain_chunk in split_message_fn(plain):
                if not plain_chunk.strip():
                    continue
                if has_recent_outbound_fn(scope_key, plain_chunk):
                    logger.info("Chat %s: suppressed duplicate plain outgoing chunk", scope_key)
                    continue
                await answer_text_with_retry_fn(message, plain_chunk)
                remember_outbound_fn(scope_key, plain_chunk)

    await progress.finish()
    clear_errors_fn(scope_key)
    return response_has_user_content, output_size_out
