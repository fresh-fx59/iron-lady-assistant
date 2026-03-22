from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class TurnExecutionResult:
    final_response: Any
    provider: Any
    observed_tools: list[str]
    provider_attempts: int
    steering_events_applied: int
    final_provider_name: str
    final_model_name: str


def _is_empty_success_response(response: Any, state: Any) -> bool:
    return bool(
        response
        and not response.is_error
        and not state.cancel_requested
        and not (response.text or "").strip()
    )


async def run_provider_execution_loop(
    *,
    message: Any,
    state: Any,
    session: Any,
    progress: Any,
    typing_task: asyncio.Task,
    scope_key: str,
    chat_id: int,
    thread_id: int | None,
    raw_prompt: str,
    override_text: str | None,
    provider_manager: Any,
    session_manager: Any,
    resume_state_store: Any,
    steering_ledger_store: Any,
    logger: Any,
    current_model_label_fn: Callable[[Any, Any], str],
    is_codex_family_cli_fn: Callable[[str | None], bool],
    find_provider_cli_fn: Callable[[str], str | None],
    as_text_fn: Callable[[str | None], str],
    worklog_subprocess_env_fn: Callable[..., dict[str, str]],
    codex_model_arg_fn: Callable[[Any, Any], str | None],
    run_codex_with_retries_fn: Callable[..., Any],
    run_claude_fn: Callable[..., Any],
    extract_requested_tools_fn: Callable[[str], list[str]],
    inject_tool_request_fn: Callable[[str, str], str],
    build_steering_patch_fn: Callable[[str, list[Any]], str],
    has_high_risk_conflict_fn: Callable[[list[Any]], bool],
) -> TurnExecutionResult:
    final_response: Any = None
    provider = provider_manager.get_provider(scope_key)
    observed_tools: list[str] = []
    provider_attempts = 0
    steering_events_applied = 0
    final_provider_name = provider.name
    final_model_name = current_model_label_fn(session, provider)

    try:
        if provider.cli != "claude" and find_provider_cli_fn(provider.cli) is None:
            fallback = provider_manager.reset(scope_key)
            session_manager.set_provider(chat_id, fallback.name, thread_id)
            await message.answer(
                f"Provider <b>{provider.name}</b> requires missing CLI "
                f"<code>{provider.cli}</code>. Switched to <b>{fallback.name}</b>.",
                parse_mode="HTML",
            )
            provider = fallback
        turn_prompt = override_text
        pending_apply_ids: list[str] = []
        while True:
            effective_prompt = as_text_fn(turn_prompt) or raw_prompt
            env = worklog_subprocess_env_fn(
                provider_manager.subprocess_env(provider),
                chat_id=chat_id,
                message_thread_id=thread_id,
                provider=provider,
                session=session,
            )
            logger.info(
                "Chat %s: using provider '%s' (cli=%s) with env=%s",
                scope_key,
                provider.name,
                provider.cli,
                {k: v for k, v in env.items() if k.startswith("ANTHROPIC_")},
            )
            resume_state_store.record_start(
                scope_key=scope_key,
                task_id=f"msg:{message.message_id}",
                step_id="interactive_turn",
                provider_cli=provider.cli,
                model=current_model_label_fn(session, provider),
                session_id=session.codex_session_id if is_codex_family_cli_fn(provider.cli) else session.claude_session_id,
                input_text=effective_prompt,
                resume_reason="manual_continue" if turn_prompt else "restart",
            )

            if is_codex_family_cli_fn(provider.cli):
                provider_attempts += 1
                codex_model = codex_model_arg_fn(session, provider)
                final_response = await run_codex_with_retries_fn(
                    message,
                    state,
                    session,
                    progress,
                    codex_model,
                    session.codex_session_id,
                    provider.resume_arg,
                    env,
                    provider.cli,
                    override_text=effective_prompt,
                    observed_tools=observed_tools,
                )
            else:
                provider_attempts += 1
                final_response = await run_claude_fn(
                    message,
                    state,
                    session,
                    progress,
                    env,
                    override_text=effective_prompt,
                    observed_tools=observed_tools,
                )
            final_provider_name = provider.name
            final_model_name = current_model_label_fn(session, provider)

            error_text_l = (final_response.text or "").strip().lower() if final_response else ""
            should_fallback = bool(
                final_response
                and final_response.is_error
                and not state.cancel_requested
                and (
                    provider_manager.is_rate_limit_error(final_response.text)
                    or (provider.cli == "claude" and error_text_l == "claude returned an error.")
                )
            )
            if should_fallback:
                next_provider = provider_manager.advance(scope_key)
                if next_provider:
                    reason = (
                        "Rate limited"
                        if provider_manager.is_rate_limit_error(final_response.text)
                        else "Provider error"
                    )
                    await message.answer(
                        f"{reason} on <b>{provider.name}</b>. "
                        f"Switching to <b>{next_provider.name}</b>...",
                        parse_mode="HTML",
                    )
                    logger.info(
                        "Chat %s: fallback from '%s' to '%s' (error=%r)",
                        scope_key, provider.name, next_provider.name, final_response.text,
                    )
                    provider = next_provider
                    session_manager.set_provider(chat_id, next_provider.name, thread_id)
                    env = worklog_subprocess_env_fn(
                        provider_manager.subprocess_env(next_provider),
                        chat_id=chat_id,
                        message_thread_id=thread_id,
                        provider=next_provider,
                        session=session,
                    )
                    if is_codex_family_cli_fn(next_provider.cli):
                        provider_attempts += 1
                        codex_model = codex_model_arg_fn(session, next_provider)
                        final_response = await run_codex_with_retries_fn(
                            message,
                            state,
                            session,
                            progress,
                            codex_model,
                            session.codex_session_id,
                            next_provider.resume_arg,
                            env,
                            next_provider.cli,
                            override_text=effective_prompt,
                            observed_tools=observed_tools,
                        )
                    else:
                        provider_attempts += 1
                        final_response = await run_claude_fn(
                            message,
                            state,
                            session,
                            progress,
                            env,
                            override_text=effective_prompt,
                            observed_tools=observed_tools,
                        )
                    final_provider_name = next_provider.name
                    final_model_name = current_model_label_fn(session, next_provider)

            if _is_empty_success_response(final_response, state):
                logger.warning(
                    "Chat %s: provider '%s' returned empty successful response; retrying once",
                    scope_key,
                    provider.name,
                )
                if is_codex_family_cli_fn(provider.cli):
                    provider_attempts += 1
                    codex_model = codex_model_arg_fn(session, provider)
                    retry_response = await run_codex_with_retries_fn(
                        message,
                        state,
                        session,
                        progress,
                        codex_model,
                        session.codex_session_id,
                        provider.resume_arg,
                        env,
                        provider.cli,
                        override_text=effective_prompt,
                        observed_tools=observed_tools,
                    )
                else:
                    provider_attempts += 1
                    retry_response = await run_claude_fn(
                        message,
                        state,
                        session,
                        progress,
                        env,
                        override_text=effective_prompt,
                        observed_tools=observed_tools,
                    )
                if retry_response:
                    final_response = retry_response

            if _is_empty_success_response(final_response, state):
                next_provider = provider_manager.advance(scope_key)
                if next_provider:
                    await message.answer(
                        f"<b>{provider.name}</b> returned an empty response. "
                        f"Switching to <b>{next_provider.name}</b>...",
                        parse_mode="HTML",
                    )
                    logger.info(
                        "Chat %s: fallback from '%s' to '%s' after empty response",
                        scope_key,
                        provider.name,
                        next_provider.name,
                    )
                    provider = next_provider
                    session_manager.set_provider(chat_id, next_provider.name, thread_id)
                    env = worklog_subprocess_env_fn(
                        provider_manager.subprocess_env(next_provider),
                        chat_id=chat_id,
                        message_thread_id=thread_id,
                        provider=next_provider,
                        session=session,
                    )
                    if is_codex_family_cli_fn(next_provider.cli):
                        provider_attempts += 1
                        codex_model = codex_model_arg_fn(session, next_provider)
                        final_response = await run_codex_with_retries_fn(
                            message,
                            state,
                            session,
                            progress,
                            codex_model,
                            session.codex_session_id,
                            next_provider.resume_arg,
                            env,
                            next_provider.cli,
                            override_text=effective_prompt,
                            observed_tools=observed_tools,
                        )
                    else:
                        provider_attempts += 1
                        final_response = await run_claude_fn(
                            message,
                            state,
                            session,
                            progress,
                            env,
                            override_text=effective_prompt,
                            observed_tools=observed_tools,
                        )
                    final_provider_name = next_provider.name
                    final_model_name = current_model_label_fn(session, next_provider)

            requested_tools = extract_requested_tools_fn(
                final_response.text if final_response else ""
            )
            if (
                requested_tools
                and final_response
                and not final_response.is_error
                and not state.cancel_requested
            ):
                selected_tool = requested_tools[0]
                logger.info(
                    "Chat %d: second-pass tool activation requested: %s",
                    message.chat.id,
                    selected_tool,
                )
                await progress.report_tool("tool_selector", selected_tool)
                forced_prompt = inject_tool_request_fn(effective_prompt, selected_tool)
                if is_codex_family_cli_fn(provider.cli):
                    provider_attempts += 1
                    codex_model = codex_model_arg_fn(session, provider)
                    retry_response = await run_codex_with_retries_fn(
                        message,
                        state,
                        session,
                        progress,
                        codex_model,
                        session.codex_session_id,
                        provider.resume_arg,
                        env,
                        provider.cli,
                        override_text=forced_prompt,
                        observed_tools=observed_tools,
                    )
                else:
                    provider_attempts += 1
                    retry_response = await run_claude_fn(
                        message,
                        state,
                        session,
                        progress,
                        env,
                        override_text=forced_prompt,
                        observed_tools=observed_tools,
                    )
                if retry_response:
                    final_response = retry_response

            if (
                final_response
                and not final_response.is_error
                and not state.cancel_requested
                and final_response.session_id
            ):
                if is_codex_family_cli_fn(provider.cli):
                    session_manager.update_codex_session_id(chat_id, final_response.session_id, thread_id)
                else:
                    session_manager.update_session_id(chat_id, final_response.session_id, thread_id)

            if (
                pending_apply_ids
                and final_response
                and not final_response.is_error
                and not state.cancel_requested
            ):
                steering_ledger_store.mark_applied(scope_key=scope_key, event_ids=pending_apply_ids)
                pending_apply_ids = []

            if not final_response or final_response.is_error or state.cancel_requested:
                break

            unapplied = steering_ledger_store.get_unapplied(scope_key=scope_key)
            if not unapplied:
                break
            if has_high_risk_conflict_fn(unapplied):
                await message.answer(
                    "I received a high-risk follow-up while work is in progress. "
                    "Please clarify the exact intended change in one message."
                )
                break

            pending_apply_ids = [event.event_id for event in unapplied]
            steering_events_applied += len(unapplied)
            await progress.report_tool("steering", f"{len(unapplied)} pending update(s)")
            turn_prompt = build_steering_patch_fn(raw_prompt, unapplied)
            logger.info(
                "Chat %s: applying %d cumulative steering event(s) in continuation",
                scope_key,
                len(unapplied),
            )
    finally:
        typing_task.cancel()
        try:
            await typing_task
        except asyncio.CancelledError:
            pass
        state.process_handle = None
        state.reset_requested = False

    return TurnExecutionResult(
        final_response=final_response,
        provider=provider,
        observed_tools=observed_tools,
        provider_attempts=provider_attempts,
        steering_events_applied=steering_events_applied,
        final_provider_name=final_provider_name,
        final_model_name=final_model_name,
    )
