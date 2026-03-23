from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from src.features.state_store import (
    ProviderSyncStore,
    ResumeStateStore,
    SteeringEvent,
    SteeringLedgerStore,
    TopicStateStore,
)


def test_record_start_and_success_persists(tmp_path) -> None:
    store = ResumeStateStore(tmp_path / "resume_envelopes.json")

    env = store.record_start(
        scope_key="123:main",
        task_id="msg:1",
        step_id="interactive_turn",
        provider_cli="claude",
        model="sonnet",
        session_id="sess-1",
        input_text="hello",
    )

    assert env.scope_key == "123:main"
    assert env.status == "running"
    assert env.input_hash

    store.record_success(scope_key="123:main", output_text="world")

    payload = json.loads((tmp_path / "resume_envelopes.json").read_text(encoding="utf-8"))
    assert payload["123:main"]["status"] == "completed"
    assert payload["123:main"]["output_hash"]


def test_fast_resume_valid_and_rejects_mismatch(tmp_path) -> None:
    store = ResumeStateStore(tmp_path / "resume_envelopes.json")

    store.record_start(
        scope_key="123:main",
        task_id="msg:2",
        step_id="interactive_turn",
        provider_cli="codex",
        model="gpt-5-codex",
        session_id="sess-c",
        input_text="same input",
    )

    ok, reason = store.can_fast_resume(scope_key="123:main", input_text="same input")
    assert ok is True
    assert reason == "ok"

    ok2, reason2 = store.can_fast_resume(scope_key="123:main", input_text="different")
    assert ok2 is False
    assert reason2 == "input_mismatch"


def test_fast_resume_rejects_stale(tmp_path) -> None:
    path = tmp_path / "resume_envelopes.json"
    store = ResumeStateStore(path)

    store.record_start(
        scope_key="123:main",
        task_id="msg:3",
        step_id="interactive_turn",
        provider_cli="claude",
        model="sonnet",
        session_id="",
        input_text="old",
    )

    data = json.loads(path.read_text(encoding="utf-8"))
    old_ts = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    data["123:main"]["updated_at"] = old_ts
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    ok, reason = store.can_fast_resume(scope_key="123:main", input_text="old", ttl_seconds=60)
    assert ok is False
    assert reason == "stale"


def test_steering_ledger_append_get_mark_and_clear(tmp_path) -> None:
    store = SteeringLedgerStore(tmp_path / "steering_ledger.json")

    event = SteeringEvent(
        event_id="evt-1",
        created_at=datetime.now(timezone.utc).isoformat(),
        source_message_id="42",
        event_type="clarify",
        text="Use pytest, not unittest",
        intent_patch="clarify: Use pytest, not unittest",
        conflict_flags=[],
    )
    store.append(scope_key="123:main", event=event)

    unapplied = store.get_unapplied(scope_key="123:main")
    assert len(unapplied) == 1
    assert unapplied[0].event_id == "evt-1"

    store.mark_applied(scope_key="123:main", event_ids=["evt-1"])
    assert store.get_unapplied(scope_key="123:main") == []

    store.clear(scope_key="123:main")
    payload = json.loads((tmp_path / "steering_ledger.json").read_text(encoding="utf-8"))
    assert "123:main" not in payload


def test_provider_sync_store_defaults_and_mark_synced(tmp_path) -> None:
    store = ProviderSyncStore(tmp_path / "provider_sync_cursors.json")

    cursor = store.get(scope_key="123:main", provider_name="codex2")
    assert cursor.last_synced_worklog_id == 0
    assert cursor.last_injected_hash == ""

    updated = store.mark_synced(
        scope_key="123:main",
        provider_name="codex2",
        latest_worklog_id=42,
        latest_topic_version=7,
        injected_hash="abc",
    )
    assert updated.last_synced_worklog_id == 42
    assert updated.last_synced_topic_version == 7
    assert updated.last_injected_hash == "abc"

    reloaded = store.get(scope_key="123:main", provider_name="codex2")
    assert reloaded.last_synced_worklog_id == 42
    assert reloaded.last_synced_topic_version == 7
    assert reloaded.last_injected_hash == "abc"


def test_provider_sync_store_does_not_regress_version(tmp_path) -> None:
    store = ProviderSyncStore(tmp_path / "provider_sync_cursors.json")
    store.mark_synced(scope_key="123:main", provider_name="codex", latest_worklog_id=10, injected_hash="x")
    store.mark_synced(scope_key="123:main", provider_name="codex", latest_worklog_id=5, injected_hash=None)

    cursor = store.get(scope_key="123:main", provider_name="codex")
    assert cursor.last_synced_worklog_id == 10
    assert store.exists(scope_key="123:main", provider_name="codex") is True
    assert store.exists(scope_key="123:main", provider_name="codex2") is False


def test_provider_sync_store_keeps_latest_topic_version(tmp_path) -> None:
    store = ProviderSyncStore(tmp_path / "provider_sync_cursors.json")
    store.mark_synced(scope_key="123:main", provider_name="codex", latest_topic_version=11)
    store.mark_synced(scope_key="123:main", provider_name="codex", latest_topic_version=9)
    cursor = store.get(scope_key="123:main", provider_name="codex")
    assert cursor.last_synced_topic_version == 11


def test_topic_state_store_records_incrementing_versions(tmp_path) -> None:
    store = TopicStateStore(tmp_path / "topic_state_store.json")
    state1 = store.record_event(scope_key="123:main", provider_name="codex", summary="first")
    state2 = store.record_event(scope_key="123:main", provider_name="codex2", summary="second")

    assert state1.topic_version == 1
    assert state2.topic_version == 2
    assert len(state2.events) == 2
    assert state2.events[-1].provider_name == "codex2"


def test_topic_state_store_delta_since_returns_recent_events(tmp_path) -> None:
    store = TopicStateStore(tmp_path / "topic_state_store.json")
    store.record_event(scope_key="123:main", provider_name="codex", summary="v1")
    store.record_event(scope_key="123:main", provider_name="codex", summary="v2")
    store.record_event(scope_key="123:main", provider_name="codex2", summary="v3")

    delta = store.delta_since(scope_key="123:main", after_version=1, limit=10)
    assert delta["latest_topic_version"] == 3
    events = delta["events"]
    assert len(events) == 2
    assert events[0]["version"] == 2
    assert events[1]["version"] == 3


def test_topic_state_store_backfill_scope_seeds_compact_history(tmp_path) -> None:
    store = TopicStateStore(tmp_path / "topic_state_store.json")
    rows = [
        {"provider_name": "codex", "summary": "v1", "updated_at": "2026-03-01T10:00:00+00:00"},
        {"provider_name": "codex2", "summary": "v2", "updated_at": "2026-03-01T10:01:00+00:00"},
        {"provider_name": "codex", "summary": "v3", "updated_at": "2026-03-01T10:02:00+00:00"},
    ]
    state, applied = store.backfill_scope(
        scope_key="123:main",
        events=rows,
        total_event_count=3,
        max_events=2,
    )

    assert applied is True
    assert state.topic_version == 3
    assert len(state.events) == 2
    assert state.events[0].version == 2
    assert state.events[1].version == 3

    state2, applied2 = store.backfill_scope(
        scope_key="123:main",
        events=rows,
        total_event_count=3,
    )
    assert applied2 is False
    assert state2.topic_version == 3
    listed = store.list()
    assert "123:main" in listed
