import asyncio
import inspect
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from time import monotonic
from typing import AsyncIterator, Final, Protocol, Sequence

from aiogram import Bot

from . import bridge, config, metrics

logger = logging.getLogger(__name__)


class TaskStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class BackgroundTask:
    id: str
    chat_id: int
    message_thread_id: int | None
    user_id: int
    prompt: str
    model: str
    session_id: str | None
    status: TaskStatus
    created_at: datetime
    provider_cli: str = "claude"
    resume_arg: str | None = None
    live_feedback: bool = False
    feedback_title: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    response: str | None = None
    error: str | None = None
    cost_usd: float = 0.0
    duration_ms: int = 0
    num_turns: int = 0
    task: asyncio.Task | None = None


class TaskObserver(Protocol):
    async def on_task_finished(self, task: BackgroundTask) -> None:
        """Called when a task reaches terminal status."""


@dataclass(frozen=True)
class ToolTimeoutPolicy:
    io_seconds: float = 20.0
    network_seconds: float = 90.0
    browser_seconds: float = 120.0
    local_shell_seconds: float = 45.0
    default_seconds: float = 60.0
    retryable_timeout_retries: int = 1


@dataclass(frozen=True)
class ToolExecutionState:
    name: str
    args_preview: str | None
    category: str
    timeout_seconds: float
    started_monotonic: float


@dataclass(frozen=True)
class ToolTimeoutRecord:
    tool_name: str
    args_preview: str
    category: str
    timeout_seconds: float
    recovery_action: str

    def to_error_text(self) -> str:
        return (
            "TOOL_TIMEOUT "
            f"tool={self.tool_name} category={self.category} timeout_s={self.timeout_seconds:.1f} "
            f"recovery={self.recovery_action} args={self.args_preview}"
        )


class TaskManager:
    """Manages background task execution and notifications."""

    _MAX_CONCURRENT: Final[int] = 3  # Max background tasks running at once
    _TASK_TIMEOUT: Final[int] = 600  # 10 minutes max per task
    _TOOL_TIMEOUT_POLICY: Final[ToolTimeoutPolicy] = ToolTimeoutPolicy()

    def __init__(self, bot: Bot, observers: Sequence[TaskObserver] | None = None):
        self.bot = bot
        self._observers = list(observers or [])
        self.tasks: dict[str, BackgroundTask] = {}
        self._queue: list[BackgroundTask] = []
        self._running_tasks: set[str] = set()
        self._queue_lock = asyncio.Lock()
        self._worker_task: asyncio.Task | None = None

    async def start(self) -> None:
        """Start the background worker."""
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(self._worker_loop())

    async def stop(self) -> None:
        """Stop the background worker and cancel running tasks."""
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None

        # Cancel running tasks
        for task_id in self._running_tasks:
            task = self.tasks.get(task_id)
            if task and task.task:
                task.task.cancel()
                task.status = TaskStatus.CANCELLED
                self._running_tasks.discard(task_id)
                try:
                    await task.task
                except asyncio.CancelledError:
                    pass

    async def submit(
        self,
        chat_id: int,
        user_id: int,
        prompt: str,
        model: str,
        session_id: str | None = None,
        message_thread_id: int | None = None,
        provider_cli: str = "claude",
        resume_arg: str | None = None,
        live_feedback: bool = False,
        feedback_title: str | None = None,
        process_handle: dict | None = None,
    ) -> str:
        """Submit a task for background execution. Returns task ID."""
        task = BackgroundTask(
            id=str(uuid.uuid4()),
            chat_id=chat_id,
            message_thread_id=message_thread_id,
            user_id=user_id,
            prompt=prompt,
            model=model,
            session_id=session_id,
            provider_cli=provider_cli,
            resume_arg=resume_arg,
            live_feedback=live_feedback,
            feedback_title=feedback_title,
            status=TaskStatus.QUEUED,
            created_at=datetime.now(),
        )
        self.tasks[task.id] = task

        async with self._queue_lock:
            self._queue.append(task)

        metrics.BG_TASKS_QUEUED.set(len(self._queue))
        metrics.BG_TASKS_ACTIVE.set(len(self.tasks))
        logger.info("Queued task %s for chat %d", task.id, chat_id)

        # Trigger queue processor if idle
        asyncio.create_task(self._process_queue())

        return task.id

    async def get_status(self, task_id: str) -> BackgroundTask | None:
        """Get task status."""
        return self.tasks.get(task_id)

    async def cancel(self, task_id: str) -> bool:
        """Cancel a task."""
        task = self.tasks.get(task_id)
        if not task:
            return False

        # If queued, remove from queue
        if task.status == TaskStatus.QUEUED:
            async with self._queue_lock:
                if task in self._queue:
                    self._queue.remove(task)
            task.status = TaskStatus.CANCELLED
            task.completed_at = datetime.now()
            return True

        # If running, cancel the asyncio task
        if task.status == TaskStatus.RUNNING and task.task:
            task.task.cancel()
            self._running_tasks.discard(task_id)
            try:
                await task.task
            except asyncio.CancelledError:
                pass
            task.status = TaskStatus.CANCELLED
            task.completed_at = datetime.now()
            logger.info("Cancelled task %s", task_id)
            return True

        return False

    def list_user_tasks(self, chat_id: int, message_thread_id: int | None = None) -> list[BackgroundTask]:
        """List all tasks for a chat."""
        return [
            t for t in self.tasks.values()
            if (
                t.chat_id == chat_id
                and t.message_thread_id == message_thread_id
                and t.status in (TaskStatus.QUEUED, TaskStatus.RUNNING)
            )
        ]

    async def _process_queue(self) -> None:
        """Process the task queue."""
        async with self._queue_lock:
            if len(self._running_tasks) >= self._MAX_CONCURRENT or not self._queue:
                return

            task = self._queue.pop(0)

        # Start execution
        self._running_tasks.add(task.id)
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now()

        metrics.BG_TASKS_QUEUED.set(len(self._queue))
        metrics.BG_TASKS_RUNNING.set(len(self._running_tasks))

        task.task = asyncio.create_task(
            self._execute_task(task),
            name=f"bg_task_{task.id}",
        )

        # Clean up completed tasks after a delay
        asyncio.create_task(self._cleanup_task(task.id))

    async def _worker_loop(self) -> None:
        """Worker that continuously processes the queue."""
        while True:
            await self._process_queue()
            await asyncio.sleep(1)

    @staticmethod
    def _normalize_tool_name(tool_name: str | None) -> str:
        return (tool_name or "").strip().lower()

    @classmethod
    def _tool_category(cls, tool_name: str | None) -> str:
        name = cls._normalize_tool_name(tool_name)
        if name in {"read", "write", "edit", "glob", "grep", "ls", "list"}:
            return "io"
        if "browser" in name or name in {"playwright", "puppeteer"}:
            return "browser"
        if any(token in name for token in ("http", "web", "url", "fetch", "search", "weather", "api")):
            return "network"
        if name in {"bash", "shell", "exec_command", "terminal", "command"}:
            return "local_shell"
        return "default"

    @classmethod
    def _tool_timeout_seconds(cls, category: str) -> float:
        policy = cls._TOOL_TIMEOUT_POLICY
        if category == "io":
            return policy.io_seconds
        if category == "network":
            return policy.network_seconds
        if category == "browser":
            return policy.browser_seconds
        if category == "local_shell":
            return policy.local_shell_seconds
        return policy.default_seconds

    @classmethod
    def _is_tool_retryable(cls, tool_name: str | None, category: str) -> bool:
        name = cls._normalize_tool_name(tool_name)
        if category in {"network", "io"}:
            return True
        return name in {"read", "glob", "grep", "web_search", "weather", "summarize"}

    @classmethod
    def _is_stateful_tool(cls, tool_name: str | None, category: str) -> bool:
        name = cls._normalize_tool_name(tool_name)
        return category in {"local_shell", "browser"} or name in {"task", "python", "sql"}

    async def _terminate_process(self, process_handle: dict | None) -> None:
        if not process_handle:
            return
        proc = process_handle.get("proc")
        if not proc:
            return
        try:
            if proc.returncode is None:
                maybe_awaitable = proc.kill()
                if inspect.isawaitable(maybe_awaitable):
                    await maybe_awaitable
                await proc.wait()
        except Exception:
            logger.exception("Failed to terminate timed-out provider subprocess")

    async def _collect_result_event(
        self,
        stream: AsyncIterator[bridge.StreamEvent],
    ) -> tuple[bridge.ClaudeResponse | None, ToolTimeoutRecord | None]:
        """Consume provider stream until RESULT or tool-specific timeout."""
        iterator = stream.__aiter__()
        active_tool: ToolExecutionState | None = None

        while True:
            timeout_s = self._TASK_TIMEOUT
            if active_tool:
                elapsed = max(0.0, monotonic() - active_tool.started_monotonic)
                remaining = active_tool.timeout_seconds - elapsed
                if remaining <= 0:
                    recovery_action = "reset_session" if self._is_stateful_tool(
                        active_tool.name, active_tool.category
                    ) else "preserve_session"
                    return None, ToolTimeoutRecord(
                        tool_name=active_tool.name or "unknown_tool",
                        args_preview=active_tool.args_preview or "-",
                        category=active_tool.category,
                        timeout_seconds=active_tool.timeout_seconds,
                        recovery_action=recovery_action,
                    )
                timeout_s = min(timeout_s, remaining)

            try:
                event = await asyncio.wait_for(iterator.__anext__(), timeout=timeout_s)
            except StopAsyncIteration:
                return None, None
            except asyncio.TimeoutError:
                if active_tool:
                    recovery_action = "reset_session" if self._is_stateful_tool(
                        active_tool.name, active_tool.category
                    ) else "preserve_session"
                    return None, ToolTimeoutRecord(
                        tool_name=active_tool.name or "unknown_tool",
                        args_preview=active_tool.args_preview or "-",
                        category=active_tool.category,
                        timeout_seconds=active_tool.timeout_seconds,
                        recovery_action=recovery_action,
                    )
                raise

            if event.event_type == bridge.StreamEventType.TOOL_USE:
                category = self._tool_category(event.tool_name)
                active_tool = ToolExecutionState(
                    name=event.tool_name or "unknown_tool",
                    args_preview=event.tool_input,
                    category=category,
                    timeout_seconds=self._tool_timeout_seconds(category),
                    started_monotonic=monotonic(),
                )
                continue

            if event.event_type == bridge.StreamEventType.RESULT:
                return event.response, None

        return None

    async def _run_provider_attempt(
        self,
        task: BackgroundTask,
    ) -> tuple[bridge.ClaudeResponse | None, ToolTimeoutRecord | None]:
        process_handle: dict = {}
        if task.provider_cli == "codex":
            stream = bridge.stream_codex_message(
                prompt=task.prompt,
                session_id=task.session_id,
                model=task.model,
                resume_arg=task.resume_arg,
                working_dir=config.CLAUDE_WORKING_DIR,
                process_handle=process_handle,
            )
        else:
            stream = bridge.stream_message(
                prompt=task.prompt,
                session_id=task.session_id,
                model=task.model,
                working_dir=config.CLAUDE_WORKING_DIR,
                process_handle=process_handle,
            )

        try:
            response, tool_timeout = await asyncio.wait_for(
                self._collect_result_event(stream),
                timeout=self._TASK_TIMEOUT,
            )
            if tool_timeout:
                await self._terminate_process(process_handle)
            return response, tool_timeout
        except Exception:
            await self._terminate_process(process_handle)
            raise

    async def _execute_task(self, task: BackgroundTask) -> None:
        """Execute a single background task."""
        typing_task: asyncio.Task | None = None
        try:
            start_time = datetime.now()
            response = None
            if task.live_feedback:
                await self._notify_started(task)
                typing_task = asyncio.create_task(self._typing_loop(task))
            tool_timeout: ToolTimeoutRecord | None = None
            retries_left = self._TOOL_TIMEOUT_POLICY.retryable_timeout_retries

            while True:
                response, tool_timeout = await self._run_provider_attempt(task)
                if not tool_timeout:
                    break

                logger.warning(
                    "task_tool_timeout task_id=%s tool=%s category=%s timeout_s=%.1f args=%s recovery=%s",
                    task.id,
                    tool_timeout.tool_name,
                    tool_timeout.category,
                    tool_timeout.timeout_seconds,
                    tool_timeout.args_preview,
                    tool_timeout.recovery_action,
                )

                retryable = self._is_tool_retryable(
                    tool_timeout.tool_name,
                    tool_timeout.category,
                )
                if not retryable or retries_left <= 0:
                    task.error = tool_timeout.to_error_text()
                    break

                retries_left -= 1
                if tool_timeout.recovery_action == "reset_session":
                    task.session_id = None
                logger.info(
                    "Retrying timed-out task %s (tool=%s, retries_left=%d, recovery=%s)",
                    task.id,
                    tool_timeout.tool_name,
                    retries_left,
                    tool_timeout.recovery_action,
                )

            # Update task with results
            task.completed_at = datetime.now()
            task.duration_ms = int((task.completed_at - start_time).total_seconds() * 1000)

            if response:
                task.response = response.text
                task.session_id = response.session_id or task.session_id
                task.cost_usd = response.cost_usd
                task.num_turns = response.num_turns
                task.status = TaskStatus.COMPLETED if not response.is_error else TaskStatus.FAILED

                if response.is_error:
                    task.error = response.text or "Unknown error"
                    logger.warning("Task %s failed: %s", task.id, task.error)
                    await self._notify_failure(task)
                    metrics.BG_TASKS_TOTAL.labels(status="failed").inc()
                else:
                    logger.info("Task %s completed in %.1fs", task.id, task.duration_ms / 1000)
                    metrics.CLAUDE_REQUESTS_TOTAL.labels(model=task.model, status="success").inc()
                    metrics.BG_TASKS_TOTAL.labels(status="completed").inc()

                    # Notify user of completion
                    await self._notify_completion(task)
            else:
                task.status = TaskStatus.FAILED
                task.error = task.error or "No response received"
                logger.warning("Task %s: no response", task.id)
                metrics.BG_TASKS_TOTAL.labels(status="failed").inc()
                await self._notify_failure(task)

        except asyncio.TimeoutError:
            task.status = TaskStatus.FAILED
            task.error = "Timeout after 10 minutes"
            task.completed_at = datetime.now()
            logger.warning("Task %s timed out", task.id)
            metrics.CLAUDE_REQUESTS_TOTAL.labels(model=task.model, status="timeout").inc()
            metrics.BG_TASKS_TOTAL.labels(status="timeout").inc()
            await self._notify_failure(task)

        except asyncio.CancelledError:
            # Task was cancelled, already handled in cancel()
            task.status = TaskStatus.CANCELLED
            task.completed_at = datetime.now()
            metrics.BG_TASKS_TOTAL.labels(status="cancelled").inc()
            await self._notify_cancelled(task)
            raise

        except Exception as e:
            task.status = TaskStatus.FAILED
            task.error = str(e)
            task.completed_at = datetime.now()
            logger.exception("Task %s crashed", task.id)
            metrics.CLAUDE_REQUESTS_TOTAL.labels(model=task.model, status="error").inc()
            metrics.BG_TASKS_TOTAL.labels(status="failed").inc()
            await self._notify_failure(task)

        finally:
            if typing_task:
                typing_task.cancel()
                try:
                    await typing_task
                except asyncio.CancelledError:
                    pass
            self._running_tasks.discard(task.id)
            metrics.BG_TASKS_RUNNING.set(len(self._running_tasks))
            metrics.BG_TASKS_ACTIVE.set(len(self.tasks))
            await self._notify_observers(task)

    async def _typing_loop(self, task: BackgroundTask) -> None:
        send_failures = 0
        fallback_sent = False
        while True:
            try:
                await self.bot.send_chat_action(
                    chat_id=task.chat_id,
                    message_thread_id=task.message_thread_id,
                    action="typing",
                )
                send_failures = 0
            except Exception as exc:
                send_failures += 1
                logger.warning(
                    "Typing action failed for task %s (attempt=%d): %s",
                    task.id,
                    send_failures,
                    exc,
                )
                if not fallback_sent or send_failures % 6 == 0:
                    try:
                        await self.bot.send_message(
                            chat_id=task.chat_id,
                            message_thread_id=task.message_thread_id,
                            text="⏳ Still working...",
                        )
                        fallback_sent = True
                    except Exception as notify_exc:
                        logger.warning(
                            "Fallback progress notification failed for task %s: %s",
                            task.id,
                            notify_exc,
                        )
            await asyncio.sleep(5)

    async def _notify_started(self, task: BackgroundTask) -> None:
        try:
            title = task.feedback_title or "🔄 <b>Working...</b>"
            await self.bot.send_message(
                chat_id=task.chat_id,
                message_thread_id=task.message_thread_id,
                text=title,
                parse_mode="HTML",
            )
        except Exception as exc:
            logger.warning("Failed to notify task start for %s: %s", task.id, exc)

    async def _notify_observers(self, task: BackgroundTask) -> None:
        for observer in self._observers:
            try:
                await observer.on_task_finished(task)
            except Exception:
                logger.exception("Task observer failed for task %s", task.id)

    async def _notify_completion(self, task: BackgroundTask) -> None:
        """Send notification when a task completes."""
        try:
            # Build status message
            lines = [
                f"✅ <b>Background task completed</b>",
                f"",
                f"<b>Duration:</b> {task.duration_ms / 1000:.1f}s",
                f"<b>Cost:</b> ${task.cost_usd:.4f}",
            ]
            if task.num_turns > 1:
                lines.append(f"<b>Turns:</b> {task.num_turns}")

            lines.append("")
            lines.append(f"<b>Response:</b>")
            # Truncate if too long
            response_preview = (task.response or "")[:3000]
            lines.append(response_preview)
            if len(task.response or "") > 3000:
                lines.append(f"...")
                lines.append(f"(Response truncated, {len(task.response) - 3000} more characters)")

            await self.bot.send_message(
                chat_id=task.chat_id,
                message_thread_id=task.message_thread_id,
                text="\n".join(lines),
                parse_mode="HTML",
            )

        except Exception as e:
            logger.warning("Failed to notify completion for task %s: %s", task.id, e)

    async def _notify_failure(self, task: BackgroundTask) -> None:
        """Send notification when a task fails."""
        try:
            lines = [
                f"❌ <b>Background task failed</b>",
                f"",
                f"<b>Task ID:</b> <code>{task.id[:8]}</code>",
                f"<b>Duration:</b> {task.duration_ms / 1000:.1f}s",
                f"",
                f"<b>Error:</b>",
                f"{task.error or 'Unknown error'}",
            ]
            await self.bot.send_message(
                chat_id=task.chat_id,
                message_thread_id=task.message_thread_id,
                text="\n".join(lines),
                parse_mode="HTML",
            )

        except Exception as e:
            logger.warning("Failed to notify failure for task %s: %s", task.id, e)

    async def _notify_cancelled(self, task: BackgroundTask) -> None:
        """Send notification when a task is cancelled."""
        try:
            lines = [
                f"🚫 <b>Background task cancelled</b>",
                f"",
                f"<b>Task ID:</b> <code>{task.id[:8]}</code>",
            ]
            await self.bot.send_message(
                chat_id=task.chat_id,
                message_thread_id=task.message_thread_id,
                text="\n".join(lines),
                parse_mode="HTML",
            )

        except Exception as e:
            logger.warning("Failed to notify cancellation for task %s: %s", task.id, e)

    async def _cleanup_task(self, task_id: str) -> None:
        """Clean up completed task after 1 hour."""
        await asyncio.sleep(3600)
        task = self.tasks.get(task_id)
        if task and task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED):
            del self.tasks[task_id]
            logger.debug("Cleaned up task %s", task_id)
