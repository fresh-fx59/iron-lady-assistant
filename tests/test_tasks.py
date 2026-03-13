import asyncio
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from aiogram.types import FSInputFile

from src.tasks import BackgroundTask, TaskManager, TaskNotificationMode, TaskStatus
from src import bridge
from src.tasks import ToolTimeoutPolicy
from src.media import send_media


@pytest.mark.asyncio
async def test_typing_loop_sends_fallback_when_chat_action_fails() -> None:
    bot = AsyncMock()
    bot.send_chat_action = AsyncMock(side_effect=RuntimeError("chat action unavailable"))
    bot.send_message = AsyncMock()
    manager = TaskManager(bot)
    task = BackgroundTask(
        id="task-1",
        chat_id=123,
        message_thread_id=77,
        user_id=123,
        prompt="x",
        model="sonnet",
        session_id=None,
        status=TaskStatus.RUNNING,
        created_at=datetime.now(),
    )

    typing_task = asyncio.create_task(manager._typing_loop(task))  # noqa: SLF001
    await asyncio.sleep(0.05)
    typing_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await typing_task

    bot.send_message.assert_awaited()


@pytest.mark.asyncio
async def test_execute_task_stream_result_does_not_raise_typeerror(monkeypatch) -> None:
    bot = AsyncMock()
    manager = TaskManager(bot)
    task = BackgroundTask(
        id="task-2",
        chat_id=123,
        message_thread_id=None,
        user_id=123,
        prompt="x",
        model="sonnet",
        session_id=None,
        status=TaskStatus.RUNNING,
        created_at=datetime.now(),
    )

    async def fake_stream_message(**kwargs):  # noqa: ARG001
        yield bridge.StreamEvent(
            event_type=bridge.StreamEventType.RESULT,
            response=bridge.ClaudeResponse(
                text="ok",
                session_id="sess-1",
                is_error=False,
                cost_usd=0.0,
            ),
        )

    monkeypatch.setattr("src.tasks.bridge.stream_message", fake_stream_message)
    await manager._execute_task(task)  # noqa: SLF001

    assert task.status == TaskStatus.COMPLETED
    assert task.response == "ok"


@pytest.mark.asyncio
async def test_execute_task_marks_structured_tool_timeout_and_kills_process(monkeypatch) -> None:
    bot = AsyncMock()
    manager = TaskManager(bot)
    task = BackgroundTask(
        id="task-timeout",
        chat_id=123,
        message_thread_id=None,
        user_id=123,
        prompt="x",
        model="sonnet",
        session_id="sess-1",
        status=TaskStatus.RUNNING,
        created_at=datetime.now(),
    )

    fake_proc = AsyncMock()
    fake_proc.returncode = None
    fake_proc.kill = AsyncMock()
    fake_proc.wait = AsyncMock()

    async def fake_stream_message(**kwargs):  # noqa: ARG001
        process_handle = kwargs.get("process_handle")
        if process_handle is not None:
            process_handle["proc"] = fake_proc
        yield bridge.StreamEvent(
            event_type=bridge.StreamEventType.TOOL_USE,
            tool_name="Bash",
            tool_input="sleep 1000",
        )
        await asyncio.sleep(3600)

    monkeypatch.setattr("src.tasks.bridge.stream_message", fake_stream_message)
    monkeypatch.setattr(
        TaskManager,
        "_TOOL_TIMEOUT_POLICY",
        ToolTimeoutPolicy(
            io_seconds=0.02,
            network_seconds=0.02,
            browser_seconds=0.02,
            local_shell_seconds=0.02,
            default_seconds=0.02,
            retryable_timeout_retries=1,
        ),
    )
    monkeypatch.setattr(TaskManager, "_TASK_TIMEOUT", 2)

    await manager._execute_task(task)  # noqa: SLF001

    assert task.status == TaskStatus.FAILED
    assert task.error is not None
    assert "TOOL_TIMEOUT" in task.error
    assert "tool=Bash" in task.error
    assert "recovery=reset_session" in task.error
    fake_proc.kill.assert_awaited_once()
    fake_proc.wait.assert_awaited_once()


@pytest.mark.asyncio
async def test_execute_task_retries_once_for_idempotent_tool_timeout(monkeypatch) -> None:
    bot = AsyncMock()
    manager = TaskManager(bot)
    task = BackgroundTask(
        id="task-retry",
        chat_id=123,
        message_thread_id=None,
        user_id=123,
        prompt="x",
        model="sonnet",
        session_id="sess-1",
        status=TaskStatus.RUNNING,
        created_at=datetime.now(),
    )

    attempts = {"count": 0}
    timed_out_proc = AsyncMock()
    timed_out_proc.returncode = None
    timed_out_proc.kill = AsyncMock()
    timed_out_proc.wait = AsyncMock()

    async def fake_stream_message(**kwargs):  # noqa: ARG001
        attempts["count"] += 1
        process_handle = kwargs.get("process_handle")
        if attempts["count"] == 1:
            if process_handle is not None:
                process_handle["proc"] = timed_out_proc
            yield bridge.StreamEvent(
                event_type=bridge.StreamEventType.TOOL_USE,
                tool_name="Read",
                tool_input="/tmp/file.txt",
            )
            await asyncio.sleep(3600)
            return

        yield bridge.StreamEvent(
            event_type=bridge.StreamEventType.RESULT,
            response=bridge.ClaudeResponse(
                text="ok-after-retry",
                session_id="sess-2",
                is_error=False,
                cost_usd=0.0,
            ),
        )

    monkeypatch.setattr("src.tasks.bridge.stream_message", fake_stream_message)
    monkeypatch.setattr(
        TaskManager,
        "_TOOL_TIMEOUT_POLICY",
        ToolTimeoutPolicy(
            io_seconds=0.02,
            network_seconds=0.02,
            browser_seconds=0.02,
            local_shell_seconds=0.02,
            default_seconds=0.02,
            retryable_timeout_retries=1,
        ),
    )
    monkeypatch.setattr(TaskManager, "_TASK_TIMEOUT", 2)

    await manager._execute_task(task)  # noqa: SLF001

    assert attempts["count"] == 2
    assert task.status == TaskStatus.COMPLETED
    assert task.response == "ok-after-retry"
    timed_out_proc.kill.assert_awaited_once()


def test_tool_category_file_change_uses_extended_timeout() -> None:
    category = TaskManager._tool_category("file_change")  # noqa: SLF001
    assert category == "file_change"
    assert TaskManager._tool_timeout_seconds(category) == TaskManager._TOOL_TIMEOUT_POLICY.file_change_seconds  # noqa: SLF001


def test_file_change_timeout_is_retryable() -> None:
    assert TaskManager._is_tool_retryable("file_change", "file_change") is True  # noqa: SLF001


@pytest.mark.asyncio
async def test_execute_task_retries_transient_codex_empty_result_once(monkeypatch) -> None:
    bot = AsyncMock()
    manager = TaskManager(bot)
    task = BackgroundTask(
        id="task-codex-retry",
        chat_id=123,
        message_thread_id=None,
        user_id=123,
        prompt="x",
        model="gpt-5-codex",
        session_id="sess-1",
        provider_cli="codex",
        status=TaskStatus.RUNNING,
        created_at=datetime.now(),
    )

    attempts = {"count": 0}

    async def fake_stream_codex_message(**kwargs):  # noqa: ARG001
        attempts["count"] += 1
        if attempts["count"] == 1:
            yield bridge.StreamEvent(
                event_type=bridge.StreamEventType.RESULT,
                response=bridge.ClaudeResponse(
                    text="Codex process exited without producing a result.",
                    session_id="sess-1",
                    is_error=True,
                    cost_usd=0.0,
                ),
            )
            return

        yield bridge.StreamEvent(
            event_type=bridge.StreamEventType.RESULT,
            response=bridge.ClaudeResponse(
                text="ok-after-transient-retry",
                session_id="sess-2",
                is_error=False,
                cost_usd=0.0,
            ),
        )

    monkeypatch.setattr("src.tasks.bridge.stream_codex_message", fake_stream_codex_message)
    monkeypatch.setattr(TaskManager, "_TRANSIENT_ERROR_RETRIES", 1)

    await manager._execute_task(task)  # noqa: SLF001

    assert attempts["count"] == 2
    assert task.status == TaskStatus.COMPLETED
    assert task.response == "ok-after-transient-retry"


@pytest.mark.asyncio
async def test_execute_task_fails_when_transient_codex_retry_exhausted(monkeypatch) -> None:
    bot = AsyncMock()
    manager = TaskManager(bot)
    task = BackgroundTask(
        id="task-codex-retry-exhausted",
        chat_id=123,
        message_thread_id=None,
        user_id=123,
        prompt="x",
        model="gpt-5-codex",
        session_id="sess-1",
        provider_cli="codex",
        status=TaskStatus.RUNNING,
        created_at=datetime.now(),
    )

    attempts = {"count": 0}

    async def fake_stream_codex_message(**kwargs):  # noqa: ARG001
        attempts["count"] += 1
        yield bridge.StreamEvent(
            event_type=bridge.StreamEventType.RESULT,
            response=bridge.ClaudeResponse(
                text="Codex process exited without producing a result.",
                session_id="sess-1",
                is_error=True,
                cost_usd=0.0,
            ),
        )

    monkeypatch.setattr("src.tasks.bridge.stream_codex_message", fake_stream_codex_message)
    monkeypatch.setattr(TaskManager, "_TRANSIENT_ERROR_RETRIES", 1)

    await manager._execute_task(task)  # noqa: SLF001

    assert attempts["count"] == 2
    assert task.status == TaskStatus.FAILED
    assert task.error == "Codex process exited without producing a result."


@pytest.mark.asyncio
async def test_execute_task_treats_codex2_as_codex_family(monkeypatch) -> None:
    bot = AsyncMock()
    manager = TaskManager(bot)
    task = BackgroundTask(
        id="task-codex2-retry",
        chat_id=123,
        message_thread_id=None,
        user_id=123,
        prompt="x",
        model="gpt-5-codex",
        session_id="sess-1",
        provider_cli="codex2",
        status=TaskStatus.RUNNING,
        created_at=datetime.now(),
    )

    captured: dict[str, str] = {}

    async def fake_stream_codex_message(**kwargs):
        captured["cli_name"] = kwargs["cli_name"]
        yield bridge.StreamEvent(
            event_type=bridge.StreamEventType.RESULT,
            response=bridge.ClaudeResponse(
                text="ok",
                session_id="sess-2",
                is_error=False,
                cost_usd=0.0,
            ),
        )

    monkeypatch.setattr("src.tasks.bridge.stream_codex_message", fake_stream_codex_message)

    await manager._execute_task(task)  # noqa: SLF001

    assert captured["cli_name"] == "codex2"
    assert task.status == TaskStatus.COMPLETED
    assert task.response == "ok"


@pytest.mark.asyncio
async def test_silent_notification_mode_suppresses_completion_and_failure_messages(monkeypatch) -> None:
    bot = AsyncMock()
    manager = TaskManager(bot)
    completed = BackgroundTask(
        id="task-silent-complete",
        chat_id=123,
        message_thread_id=None,
        user_id=123,
        prompt="x",
        model="sonnet",
        session_id=None,
        status=TaskStatus.COMPLETED,
        created_at=datetime.now(),
        notification_mode=TaskNotificationMode.SILENT,
        response="ok",
    )
    failed = BackgroundTask(
        id="task-silent-fail",
        chat_id=123,
        message_thread_id=None,
        user_id=123,
        prompt="x",
        model="sonnet",
        session_id=None,
        status=TaskStatus.FAILED,
        created_at=datetime.now(),
        notification_mode=TaskNotificationMode.SILENT,
        error="boom",
    )

    await manager._notify_completion(completed)  # noqa: SLF001
    await manager._notify_failure(failed)  # noqa: SLF001

    bot.send_message.assert_not_called()


@pytest.mark.asyncio
async def test_deliver_response_mode_sends_voice_and_text(monkeypatch, tmp_path) -> None:
    audio_path = tmp_path / "digest.ogg"
    audio_path.write_bytes(b"fake-audio")

    bot = AsyncMock()
    manager = TaskManager(bot)
    task = BackgroundTask(
        id="task-deliver-response",
        chat_id=123,
        message_thread_id=77,
        user_id=123,
        prompt="x",
        model="sonnet",
        session_id=None,
        status=TaskStatus.COMPLETED,
        created_at=datetime.now(),
        notification_mode=TaskNotificationMode.DELIVER_RESPONSE,
        response=(
            "USE_TOOL: sag\n"
            "## Daily digest\n"
            "- Item one\n"
            "[[audio_as_voice]]\n"
            f"MEDIA:{audio_path}\n"
        ),
    )

    await manager._notify_completion(task)  # noqa: SLF001

    bot.send_voice.assert_awaited_once()
    bot.send_audio.assert_not_called()
    bot.send_document.assert_not_called()
    bot.send_message.assert_awaited_once()
    kwargs = bot.send_message.await_args.kwargs
    assert kwargs["chat_id"] == 123
    assert kwargs["message_thread_id"] == 77
    assert "Daily digest" in kwargs["text"]


@pytest.mark.asyncio
async def test_deliver_response_mode_falls_back_to_text_when_media_send_fails(monkeypatch) -> None:
    bot = AsyncMock()
    manager = TaskManager(bot)
    task = BackgroundTask(
        id="task-deliver-fallback",
        chat_id=123,
        message_thread_id=77,
        user_id=123,
        prompt="x",
        model="sonnet",
        session_id=None,
        status=TaskStatus.COMPLETED,
        created_at=datetime.now(),
        notification_mode=TaskNotificationMode.DELIVER_RESPONSE,
        response=(
            "## Daily digest\n"
            "Summary text\n"
            "[[audio_as_voice]]\n"
            "MEDIA:/tmp/missing.ogg\n"
        ),
    )

    async def _raise_send_media(*args, **kwargs):  # noqa: ANN002, ANN003
        raise RuntimeError("media send failed")

    monkeypatch.setattr("src.tasks.send_media", _raise_send_media)

    await manager._notify_completion(task)  # noqa: SLF001

    bot.send_message.assert_awaited_once()
    kwargs = bot.send_message.await_args.kwargs
    assert kwargs["chat_id"] == 123
    assert kwargs["message_thread_id"] == 77
    assert "Daily digest" in kwargs["text"]
    assert "Could not send some media attachments" in kwargs["text"]


@pytest.mark.asyncio
async def test_send_media_snapshots_local_voice_file_before_send(tmp_path) -> None:
    audio_path = tmp_path / "digest.ogg"
    audio_path.write_bytes(b"voice-bytes")

    bot = AsyncMock()
    sent_paths: list[Path] = []

    async def fake_send_voice(*, voice, **kwargs):
        assert isinstance(voice, FSInputFile)
        sent_path = Path(voice.path)
        sent_paths.append(sent_path)
        assert sent_path != audio_path
        assert sent_path.exists()

    bot.send_voice.side_effect = fake_send_voice

    await send_media(bot, 123, 77, str(audio_path), audio_as_voice=True)

    assert sent_paths
    assert not sent_paths[0].exists()


@pytest.mark.asyncio
async def test_submit_queues_background_task_while_lifecycle_draining() -> None:
    bot = AsyncMock()
    queued: list[dict[str, object]] = []

    class StoreStub:
        @staticmethod
        def is_draining() -> bool:
            return True

        @staticmethod
        def enqueue_background_task(**kwargs):
            queued.append(kwargs)
            return 1

    manager = TaskManager(bot, lifecycle_store=StoreStub())

    task_id = await manager.submit(
        chat_id=123,
        user_id=123,
        prompt="x",
        model="sonnet",
        provider_cli="claude",
    )

    assert task_id
    assert queued
    assert queued[0]["task_id"] == task_id
    assert queued[0]["provider_cli"] == "claude"
