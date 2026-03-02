import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Final, Protocol, Sequence

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
    user_id: int
    prompt: str
    model: str
    session_id: str | None
    status: TaskStatus
    created_at: datetime
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


class TaskManager:
    """Manages background task execution and notifications."""

    _MAX_CONCURRENT: Final[int] = 3  # Max background tasks running at once
    _TASK_TIMEOUT: Final[int] = 600  # 10 minutes max per task

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
        process_handle: dict | None = None,
    ) -> str:
        """Submit a task for background execution. Returns task ID."""
        task = BackgroundTask(
            id=str(uuid.uuid4()),
            chat_id=chat_id,
            user_id=user_id,
            prompt=prompt,
            model=model,
            session_id=session_id,
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

    def list_user_tasks(self, chat_id: int) -> list[BackgroundTask]:
        """List all tasks for a chat."""
        return [
            t for t in self.tasks.values()
            if t.chat_id == chat_id and t.status in (TaskStatus.QUEUED, TaskStatus.RUNNING)
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

    async def _execute_task(self, task: BackgroundTask) -> None:
        """Execute a single background task."""
        try:
            start_time = datetime.now()
            response = None

            # Stream the Claude response with timeout
            async for event in asyncio.wait_for(
                bridge.stream_message(
                    prompt=task.prompt,
                    session_id=task.session_id,
                    model=task.model,
                    working_dir=config.CLAUDE_WORKING_DIR,
                ),
                timeout=self._TASK_TIMEOUT,
            ):
                if event.event_type == bridge.StreamEventType.RESULT:
                    response = event.response
                    break

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
                task.error = "No response received"
                logger.warning("Task %s: no response", task.id)

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
            self._running_tasks.discard(task.id)
            metrics.BG_TASKS_RUNNING.set(len(self._running_tasks))
            metrics.BG_TASKS_ACTIVE.set(len(self.tasks))
            await self._notify_observers(task)

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

            await self.bot.send_message(chat_id=task.chat_id, text="\n".join(lines), parse_mode="HTML")

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
            await self.bot.send_message(chat_id=task.chat_id, text="\n".join(lines), parse_mode="HTML")

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
            await self.bot.send_message(chat_id=task.chat_id, text="\n".join(lines), parse_mode="HTML")

        except Exception as e:
            logger.warning("Failed to notify cancellation for task %s: %s", task.id, e)

    async def _cleanup_task(self, task_id: str) -> None:
        """Clean up completed task after 1 hour."""
        await asyncio.sleep(3600)
        task = self.tasks.get(task_id)
        if task and task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED):
            del self.tasks[task_id]
            logger.debug("Cleaned up task %s", task_id)
