from __future__ import annotations

from src.lifecycle_queue import LifecycleQueueStore


def test_begin_deploy_marks_draining_and_acknowledge_restart_reopens(tmp_path) -> None:
    store = LifecycleQueueStore(tmp_path / "lifecycle.db")

    operation_id = store.begin_deploy(requested_commit="abc123")

    assert operation_id
    assert store.barrier_phase() == "draining"

    store.mark_restarting(operation_id)
    assert store.barrier_phase() == "restarting"

    store.acknowledge_process_restart()
    assert store.barrier_phase() == "open"


def test_enqueue_turn_deduplicates_and_claim_replays_in_order(tmp_path) -> None:
    store = LifecycleQueueStore(tmp_path / "lifecycle.db")

    first = store.enqueue_turn(
        scope_key="123:main",
        chat_id=123,
        message_thread_id=None,
        user_id=7,
        prompt="prompt-1",
        source_message_id=10,
    )
    duplicate = store.enqueue_turn(
        scope_key="123:main",
        chat_id=123,
        message_thread_id=None,
        user_id=7,
        prompt="prompt-1",
        source_message_id=10,
    )
    second = store.enqueue_turn(
        scope_key="123:77",
        chat_id=123,
        message_thread_id=77,
        user_id=7,
        prompt="prompt-2",
        source_message_id=11,
    )

    assert duplicate == first
    claimed = store.claim_queued_turns(limit=10)
    assert [row.id for row in claimed] == [first, second]
    assert [row.status for row in claimed] == ["replaying", "replaying"]
    assert [row.prompt_format for row in claimed] == ["raw", "raw"]

    store.acknowledge_process_restart()
    replayed_again = store.claim_queued_turns(limit=10)
    assert [row.id for row in replayed_again] == [first, second]


def test_enqueue_background_task_deduplicates_and_claims(tmp_path) -> None:
    store = LifecycleQueueStore(tmp_path / "lifecycle.db")

    first = store.enqueue_background_task(
        task_id="task-1",
        chat_id=123,
        message_thread_id=77,
        user_id=7,
        prompt="prompt",
        model="sonnet",
        session_id="sess-1",
        provider_cli="claude",
        resume_arg=None,
        notification_mode="full",
        live_feedback=False,
        feedback_title=None,
    )
    duplicate = store.enqueue_background_task(
        task_id="task-1",
        chat_id=123,
        message_thread_id=77,
        user_id=7,
        prompt="prompt",
        model="sonnet",
        session_id="sess-1",
        provider_cli="claude",
        resume_arg=None,
        notification_mode="full",
        live_feedback=False,
        feedback_title=None,
    )

    assert duplicate == first
    claimed = store.claim_queued_background_tasks(limit=10)
    assert len(claimed) == 1
    assert claimed[0].task_id == "task-1"
    assert claimed[0].status == "replaying"


def test_begin_deploy_queues_later_operations_until_prior_one_completes(tmp_path) -> None:
    store = LifecycleQueueStore(tmp_path / "lifecycle.db")

    first = store.begin_deploy(requested_commit="commit-1")
    second = store.begin_deploy(requested_commit="commit-2")

    assert store.activate_deploy_if_ready(first) == "draining"
    assert store.activate_deploy_if_ready(second) == "queued"

    store.mark_restarting(first)
    store.mark_operation_completed(first)

    assert store.activate_deploy_if_ready(second) == "draining"
    assert store.barrier_phase() == "draining"
