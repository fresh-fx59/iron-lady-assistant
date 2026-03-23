from __future__ import annotations

from src.features.provider_sync_backfill import auto_prepare_new_codex_providers
from src.features.provider_sync_backfill import run_one_time_provider_sync_backfill
from src.features.state_store import ProviderSyncStore, TopicStateStore
from src.memory import MemoryManager


def test_one_time_provider_sync_backfill_seeds_topic_state_and_cursors(tmp_path) -> None:
    memory_manager = MemoryManager(tmp_path)
    memory_manager.add_episode(
        chat_id=1,
        message_thread_id=None,
        scope_key="1:main",
        provider="codex",
        session_type="codex",
        session_id="sess-c1",
        summary="start in codex",
        decisions=["d1"],
    )
    memory_manager.add_episode(
        chat_id=1,
        message_thread_id=None,
        scope_key="1:main",
        provider="codex2",
        session_type="codex",
        session_id="sess-c2",
        summary="continued in codex2",
        decisions=["d2"],
    )
    memory_manager.add_episode(
        chat_id=2,
        message_thread_id=77,
        scope_key="2:77",
        provider="codex",
        session_type="codex",
        session_id="sess-c3",
        summary="thread topic",
        decisions=["thread-decision"],
    )

    topic_state_store = TopicStateStore(tmp_path / "topic_state_store.json")
    provider_sync_store = ProviderSyncStore(tmp_path / "provider_sync_cursors.json")
    result = run_one_time_provider_sync_backfill(
        memory_dir=tmp_path,
        topic_state_store=topic_state_store,
        provider_sync_store=provider_sync_store,
        codex_provider_names=["codex", "codex2", "codex3"],
    )

    assert result["status"] == "ok"
    assert result["scope_count"] == 2
    assert result["backfilled_scope_count"] == 2
    assert result["cursor_updates"] == 3

    scope_main = topic_state_store.get(scope_key="1:main")
    assert scope_main.topic_version == 2
    assert scope_main.events[-1].provider_name == "codex2"

    codex_cursor = provider_sync_store.get(scope_key="1:main", provider_name="codex")
    codex2_cursor = provider_sync_store.get(scope_key="1:main", provider_name="codex2")
    codex3_cursor = provider_sync_store.get(scope_key="1:main", provider_name="codex3")
    assert codex_cursor.last_synced_topic_version == 1
    assert codex2_cursor.last_synced_topic_version == 2
    assert codex3_cursor.last_synced_topic_version == 0

    second_run = run_one_time_provider_sync_backfill(
        memory_dir=tmp_path,
        topic_state_store=topic_state_store,
        provider_sync_store=provider_sync_store,
        codex_provider_names=["codex", "codex2", "codex3"],
    )
    assert second_run["status"] == "skipped"
    assert second_run["reason"] == "already_completed"


def test_auto_prepare_new_codex_provider_creates_missing_cursors(tmp_path) -> None:
    topic_state_store = TopicStateStore(tmp_path / "topic_state_store.json")
    provider_sync_store = ProviderSyncStore(tmp_path / "provider_sync_cursors.json")
    topic_state_store.record_event(scope_key="1:main", provider_name="codex", summary="v1")
    topic_state_store.record_event(scope_key="1:main", provider_name="codex2", summary="v2")
    topic_state_store.record_event(scope_key="1:main", provider_name="codex2", summary="v3")
    provider_sync_store.mark_synced(
        scope_key="1:main",
        provider_name="codex",
        latest_topic_version=3,
    )
    provider_sync_store.mark_synced(
        scope_key="1:main",
        provider_name="codex2",
        latest_topic_version=3,
    )

    result = auto_prepare_new_codex_providers(
        topic_state_store=topic_state_store,
        provider_sync_store=provider_sync_store,
        codex_provider_names=["codex", "codex2", "codex4"],
        catchup_window=2,
    )
    assert result["status"] == "ok"
    assert result["cursors_created"] == 1

    codex4_cursor = provider_sync_store.get(scope_key="1:main", provider_name="codex4")
    assert codex4_cursor.last_synced_topic_version == 1

    second = auto_prepare_new_codex_providers(
        topic_state_store=topic_state_store,
        provider_sync_store=provider_sync_store,
        codex_provider_names=["codex", "codex2", "codex4"],
        catchup_window=2,
    )
    assert second["cursors_created"] == 0
