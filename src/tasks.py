import asyncio
import inspect
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from time import monotonic
from typing import AsyncIterator, Final, Protocol, Sequence

from aiogram import Bot

from . import bridge, config, metrics
from .formatter import markdown_to_html, split_message, strip_html
from .media import extract_media_directives, send_media, strip_tool_directive_lines
from .sessions import make_scope_key

logger = logging.getLogger(__name__)
_STEP_PLAN_HINT_RE = re.compile(r"\bcontinue\b.*\bplan\b", re.IGNORECASE)


class TaskStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskNotificationMode(str, Enum):
    FULL = "full"
    FAILURES_ONLY = "failures_only"
    SILENT = "silent"
    DELIVER_RESPONSE = "deliver_response"


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
    notification_mode: TaskNotificationMode = TaskNotificationMode.FULL
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
    async def on_task_started(self, task: BackgroundTask) -> None:
        """Called when a task starts execution."""

    async def on_task_finished(self, task: BackgroundTask) -> None:
        """Called when a task reaches terminal status."""


@dataclass(frozen=True)
class ToolTimeoutPolicy:
    io_seconds: float = 20.0
    network_seconds: float = 90.0
    browser_seconds: float = 120.0
    local_shell_seconds: float = 45.0
    file_change_seconds: float = 240.0
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
    _TRANSIENT_ERROR_RETRIES: Final[int] = 1
    _TRANSIENT_CODEX_ERRORS: Final[tuple[str, ...]] = (
        "codex process exited without producing a result",
        "codex process was interrupted by service restart",
    )

    def __init__(
        self,
        bot: Bot,
        observers: Sequence[TaskObserver] | None = None,
        lifecycle_store: object | None = None,
        provider_manager: object | None = None,
    ):
        self.bot = bot
        self._observers = list(observers or [])
        self._lifecycle_store = lifecycle_store
        self._provider_manager = provider_manager
        self.tasks: dict[str, BackgroundTask] = {}
        self._queue: list[BackgroundTask] = []
        self._running_tasks: set[str] = set()
        self._queue_lock = asyncio.Lock()
        self._worker_task: asyncio.Task | None = None

    def add_observer(self, observer: TaskObserver) -> None:
        """Register a task observer."""
        self._observers.append(observer)

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
        notification_mode: TaskNotificationMode = TaskNotificationMode.FULL,
        live_feedback: bool = False,
        feedback_title: str | None = None,
        process_handle: dict | None = None,
        task_id: str | None = None,
    ) -> str:
        """Submit a task for background execution. Returns task ID."""
        effective_task_id = task_id or str(uuid.uuid4())
        if self._lifecycle_store and getattr(self._lifecycle_store, "is_draining", None):
            if self._lifecycle_store.is_draining():
                if getattr(self._lifecycle_store, "enqueue_background_task", None):
                    self._lifecycle_store.enqueue_background_task(
                        task_id=effective_task_id,
                        chat_id=chat_id,
                        message_thread_id=message_thread_id,
                        user_id=user_id,
                        prompt=prompt,
                        model=model,
                        session_id=session_id,
                        provider_cli=provider_cli,
                        resume_arg=resume_arg,
                        notification_mode=notification_mode.value,
                        live_feedback=live_feedback,
                        feedback_title=feedback_title,
                    )
                    logger.info("Queued background task %s behind deploy drain", effective_task_id)
                    return effective_task_id
                raise RuntimeError("Deploy drain in progress; new background work is temporarily blocked.")
        task = BackgroundTask(
            id=effective_task_id,
            chat_id=chat_id,
            message_thread_id=message_thread_id,
            user_id=user_id,
            prompt=prompt,
            model=model,
            session_id=session_id,
            provider_cli=provider_cli,
            resume_arg=resume_arg,
            notification_mode=notification_mode,
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
        await self._notify_task_started(task)

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
        if any(token in name for token in ("file_change", "apply_patch", "edit_file", "replace_file")):
            return "file_change"
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
        if category == "file_change":
            return policy.file_change_seconds
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
        if category in {"network", "io", "file_change"}:
            return True
        return name in {"read", "glob", "grep", "web_search", "weather", "summarize"}

    @classmethod
    def _is_stateful_tool(cls, tool_name: str | None, category: str) -> bool:
        name = cls._normalize_tool_name(tool_name)
        return category in {"local_shell", "browser"} or name in {"task", "python", "sql"}

    @classmethod
    def _is_retryable_provider_error(cls, provider_cli: str, error_text: str | None) -> bool:
        if not provider_cli.startswith("codex"):
            return False
        text = (error_text or "").strip().lower()
        if not text:
            return False
        return any(marker in text for marker in cls._TRANSIENT_CODEX_ERRORS)

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
    ) -> tuple[bridge.ClaudeResponse | None, ToolTimeoutRecord | None, list[str]]:
        """Consume provider stream until RESULT or tool-specific timeout."""
        iterator = stream.__aiter__()
        active_tool: ToolExecutionState | None = None
        observed_tools: list[str] = []

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
                    ), observed_tools
                timeout_s = min(timeout_s, remaining)

            try:
                event = await asyncio.wait_for(iterator.__anext__(), timeout=timeout_s)
            except StopAsyncIteration:
                return None, None, observed_tools
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
                    ), observed_tools
                raise

            if event.event_type == bridge.StreamEventType.TOOL_USE:
                if event.tool_name:
                    observed_tools.append(event.tool_name)
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
                return event.response, None, observed_tools

        return None, None, observed_tools

    async def _run_provider_attempt(
        self,
        task: BackgroundTask,
    ) -> tuple[bridge.ClaudeResponse | None, ToolTimeoutRecord | None, list[str]]:
        process_handle: dict = {}
        subprocess_env = self._task_subprocess_env(task)
        if task.provider_cli.startswith("codex"):
            stream = bridge.stream_codex_message(
                prompt=task.prompt,
                session_id=task.session_id,
                model=task.model,
                resume_arg=task.resume_arg,
                cli_name=task.provider_cli,
                working_dir=config.CLAUDE_WORKING_DIR,
                process_handle=process_handle,
                subprocess_env=subprocess_env,
            )
        else:
            stream = bridge.stream_message(
                prompt=task.prompt,
                session_id=task.session_id,
                model=task.model,
                working_dir=config.CLAUDE_WORKING_DIR,
                process_handle=process_handle,
                subprocess_env=subprocess_env,
            )

        try:
            response, tool_timeout, observed_tools = await asyncio.wait_for(
                self._collect_result_event(stream),
                timeout=self._TASK_TIMEOUT,
            )
            if tool_timeout:
                await self._terminate_process(process_handle)
            return response, tool_timeout, observed_tools
        except Exception:
            await self._terminate_process(process_handle)
            raise

    async def _execute_task(self, task: BackgroundTask) -> None:
        """Execute a single background task."""
        typing_task: asyncio.Task | None = None
        lifecycle_scope_key = f"bg:{task.id}"
        try:
            start_time = datetime.now()
            if self._lifecycle_store and getattr(self._lifecycle_store, "upsert_active_scope", None):
                self._lifecycle_store.upsert_active_scope(
                    scope_key=lifecycle_scope_key,
                    chat_id=task.chat_id,
                    message_thread_id=task.message_thread_id,
                    user_id=task.user_id,
                    kind="background_task",
                    prompt_preview=task.prompt,
                )
            response = None
            if task.live_feedback:
                await self._notify_started(task)
                typing_task = asyncio.create_task(self._typing_loop(task))
            tool_timeout: ToolTimeoutRecord | None = None
            retries_left = self._TOOL_TIMEOUT_POLICY.retryable_timeout_retries
            transient_error_retries_left = self._TRANSIENT_ERROR_RETRIES
            observed_tools: list[str] = []
            provider_attempts = 0

            while True:
                response, tool_timeout, attempt_tools = await self._run_provider_attempt(task)
                provider_attempts += 1
                observed_tools.extend(attempt_tools)
                if not tool_timeout:
                    if (
                        response
                        and response.is_error
                        and transient_error_retries_left > 0
                        and self._is_retryable_provider_error(task.provider_cli, response.text)
                    ):
                        transient_error_retries_left -= 1
                        logger.warning(
                            "Retrying transient provider error for task %s (provider=%s, retries_left=%d): %s",
                            task.id,
                            task.provider_cli,
                            transient_error_retries_left,
                            (response.text or "")[:200],
                        )
                        # Service-restart interruptions can invalidate provider-side resume state.
                        if "interrupted by service restart" in (response.text or "").lower():
                            task.session_id = None
                        await asyncio.sleep(0.3)
                        continue
                    if (
                        response
                        and response.is_error
                        and self._is_fallback_rate_limit_error(response.text)
                    ):
                        if self._advance_task_provider(task):
                            task.session_id = None
                            await asyncio.sleep(0.3)
                            continue
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
            if self._lifecycle_store and getattr(self._lifecycle_store, "clear_active_scope", None):
                self._lifecycle_store.clear_active_scope(lifecycle_scope_key)
            metrics.observe_cost_intelligence_turn(
                scope_key=make_scope_key(task.chat_id, task.message_thread_id),
                provider=task.provider_cli,
                model=task.model,
                mode="background",
                cost_usd=float(task.cost_usd),
                num_turns=int(task.num_turns),
                duration_ms=float(task.duration_ms),
                is_error=task.status == TaskStatus.FAILED,
                is_cancelled=task.status == TaskStatus.CANCELLED,
                is_empty_response=not bool((task.response or "").strip()),
                tool_timeout=bool(task.error and "TOOL_TIMEOUT" in task.error),
                tool_names=observed_tools if "observed_tools" in locals() else [],
                message_size_in=len(task.prompt or ""),
                message_size_out=len(task.response or task.error or ""),
                step_plan_active=bool(_STEP_PLAN_HINT_RE.search(task.prompt or "")),
                steering_event_count=0,
                attempts=max(1, provider_attempts if "provider_attempts" in locals() else 1),
            )
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

    def _task_scope_key(self, task: BackgroundTask) -> str:
        return make_scope_key(task.chat_id, task.message_thread_id)

    def _task_provider(self, task: BackgroundTask):
        if self._provider_manager is None:
            return None
        self._ensure_task_provider_scope(task)
        return self._provider_manager.get_provider(self._task_scope_key(task))

    def _task_subprocess_env(self, task: BackgroundTask) -> dict[str, str] | None:
        provider = self._task_provider(task)
        if provider is None:
            return None
        return self._provider_manager.subprocess_env(provider)

    def _is_fallback_rate_limit_error(self, error_text: str | None) -> bool:
        if self._provider_manager is None:
            return False
        return bool(error_text) and self._provider_manager.is_rate_limit_error(error_text)

    def _ensure_task_provider_scope(self, task: BackgroundTask) -> None:
        if self._provider_manager is None:
            return

        scope_key = self._task_scope_key(task)
        current_provider = self._provider_manager.get_provider(scope_key)
        current_cli = getattr(current_provider, "cli", None)
        if current_cli == task.provider_cli:
            return

        matched_provider = None
        for provider in getattr(self._provider_manager, "providers", []):
            if getattr(provider, "cli", None) == task.provider_cli or getattr(provider, "name", None) == task.provider_cli:
                matched_provider = provider
                break

        if matched_provider is None:
            logger.warning(
                "Background task %s could not align provider scope for provider_cli=%s",
                task.id,
                task.provider_cli,
            )
            return

        self._provider_manager.set_provider(scope_key, matched_provider.name)
        logger.info(
            "Aligned background task %s scope %s from provider_cli=%s to provider=%s (cli=%s)",
            task.id,
            scope_key,
            current_cli,
            matched_provider.name,
            matched_provider.cli,
        )

    def _advance_task_provider(self, task: BackgroundTask) -> bool:
        if self._provider_manager is None:
            return False

        self._ensure_task_provider_scope(task)
        scope_key = self._task_scope_key(task)
        next_provider = self._provider_manager.advance(scope_key)
        if next_provider is None:
            return False

        logger.warning(
            "Background task %s falling back from provider_cli=%s to provider=%s (cli=%s)",
            task.id,
            task.provider_cli,
            getattr(next_provider, "name", "unknown"),
            next_provider.cli,
        )
        task.provider_cli = next_provider.cli
        task.resume_arg = getattr(next_provider, "resume_arg", None)
        if task.provider_cli.startswith("codex") and getattr(next_provider, "model", None):
            task.model = next_provider.model
        return True

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
        if task.notification_mode != TaskNotificationMode.FULL:
            return
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

    async def _notify_task_started(self, task: BackgroundTask) -> None:
        for observer in self._observers:
            callback = getattr(observer, "on_task_started", None)
            if callback is None:
                continue
            try:
                await callback(task)
            except Exception:
                logger.exception("Task observer start hook failed for task %s", task.id)

    async def _notify_completion(self, task: BackgroundTask) -> None:
        """Send notification when a task completes."""
        if task.notification_mode == TaskNotificationMode.SILENT:
            return
        try:
            if task.notification_mode == TaskNotificationMode.DELIVER_RESPONSE:
                await self._deliver_response(task)
                return
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

    async def _deliver_response(self, task: BackgroundTask) -> None:
        clean_text, media_refs, audio_as_voice = extract_media_directives(task.response or "")
        clean_text = strip_tool_directive_lines(clean_text)
        failed_media_refs: list[str] = []

        for media_ref in media_refs:
            try:
                await send_media(
                    self.bot,
                    task.chat_id,
                    task.message_thread_id,
                    media_ref,
                    audio_as_voice=audio_as_voice,
                )
            except Exception:
                logger.warning("Failed to send scheduled media response: %s", media_ref, exc_info=True)
                failed_media_refs.append(media_ref)

        if failed_media_refs:
            failure_note = "\n".join(f"- {ref}" for ref in failed_media_refs)
            clean_text = (
                f"{clean_text}\n\n⚠️ Could not send some media attachments:\n{failure_note}".strip()
            )

        html_chunks: list[str] = []
        if clean_text.strip():
            html_chunks = split_message(markdown_to_html(clean_text))

        if not html_chunks and not media_refs:
            html_chunks = ["I received an empty response from the provider."]

        for chunk in html_chunks:
            if not strip_html(chunk).strip():
                continue
            await self.bot.send_message(
                chat_id=task.chat_id,
                message_thread_id=task.message_thread_id,
                text=chunk,
                parse_mode="HTML",
            )

    async def _notify_failure(self, task: BackgroundTask) -> None:
        """Send notification when a task fails."""
        if task.notification_mode == TaskNotificationMode.SILENT:
            return
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
        if task.notification_mode == TaskNotificationMode.SILENT:
            return
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
