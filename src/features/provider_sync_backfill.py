from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from .state_store import ProviderSyncStore, TopicStateStore

logger = logging.getLogger(__name__)

_MARKER_FILE = "provider_sync_backfill_v1.json"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_marker(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _save_marker(path: Path, payload: dict[str, object]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _read_worklog_history(memory_dir: Path) -> tuple[list[dict[str, object]], int]:
    db_path = memory_dir / "episodes.db"
    if not db_path.exists():
        return [], 0

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        latest_row = con.execute(
            "SELECT COALESCE(MAX(id), 0) AS latest_id FROM worklog_sessions"
        ).fetchone()
        latest_id = int(latest_row["latest_id"]) if latest_row else 0
        rows = con.execute(
            """
            SELECT
                w.id,
                w.scope_key,
                COALESCE(w.provider, '') AS provider,
                COALESCE(w.summary, '') AS summary,
                COALESCE(w.last_seen_at, w.started_at, '') AS updated_at,
                e.decisions AS episode_decisions
            FROM worklog_sessions w
            LEFT JOIN episodes e ON e.id = w.episode_id
            WHERE COALESCE(w.scope_key, '') != ''
            ORDER BY w.scope_key ASC, w.id ASC
            """
        ).fetchall()
    finally:
        con.close()

    parsed: list[dict[str, object]] = []
    for row in rows:
        decisions: list[str] = []
        decisions_raw = row["episode_decisions"]
        if isinstance(decisions_raw, str) and decisions_raw.strip():
            try:
                loaded = json.loads(decisions_raw)
                if isinstance(loaded, list):
                    decisions = [str(item).strip() for item in loaded if str(item).strip()]
            except Exception:
                decisions = []
        parsed.append(
            {
                "id": int(row["id"]),
                "scope_key": str(row["scope_key"]),
                "provider": str(row["provider"]),
                "summary": str(row["summary"]),
                "updated_at": str(row["updated_at"]),
                "decisions": decisions,
            }
        )
    return parsed, latest_id


def run_one_time_provider_sync_backfill(
    *,
    memory_dir: Path,
    topic_state_store: TopicStateStore,
    provider_sync_store: ProviderSyncStore,
    codex_provider_names: Iterable[str],
    max_topic_events: int = 80,
) -> dict[str, object]:
    """Backfill topic versions and provider cursors from historical worklog once."""
    marker_path = memory_dir / _MARKER_FILE
    marker = _load_marker(marker_path)
    if bool(marker.get("completed")):
        return {"status": "skipped", "reason": "already_completed", "marker_path": str(marker_path)}

    provider_names = [name.strip() for name in codex_provider_names if str(name).strip()]
    rows, latest_worklog_id = _read_worklog_history(memory_dir)
    by_scope: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        by_scope.setdefault(str(row["scope_key"]), []).append(row)

    scope_count = 0
    backfilled_scope_count = 0
    cursor_updates = 0
    for scope_key, scope_rows in by_scope.items():
        scope_count += 1

        existing = topic_state_store.get(scope_key=scope_key)
        provider_last_version: dict[str, int] = {}
        if int(existing.topic_version) <= 0 and not existing.events:
            total = len(scope_rows)
            events_payload = [
                {
                    "provider_name": str(item.get("provider", "") or ""),
                    "summary": str(item.get("summary", "") or "").strip(),
                    "decisions": list(item.get("decisions") or []),
                    "updated_at": str(item.get("updated_at", "") or _now_iso()),
                }
                for item in scope_rows
            ]
            state, applied = topic_state_store.backfill_scope(
                scope_key=scope_key,
                events=events_payload,
                total_event_count=total,
                max_events=max_topic_events,
                skip_if_populated=True,
            )
            if applied:
                backfilled_scope_count += 1
            for idx, item in enumerate(scope_rows, start=1):
                provider_name = str(item.get("provider", "") or "").strip()
                if provider_name:
                    provider_last_version[provider_name] = idx
            latest_topic_version = int(state.topic_version)
        else:
            for event in existing.events:
                provider_name = str(event.provider_name or "").strip()
                if provider_name:
                    provider_last_version[provider_name] = max(
                        int(event.version),
                        int(provider_last_version.get(provider_name, 0)),
                    )
            latest_topic_version = int(existing.topic_version)

        for provider_name in provider_names:
            target_version = int(provider_last_version.get(provider_name, 0))
            if target_version <= 0:
                continue
            cursor = provider_sync_store.get(scope_key=scope_key, provider_name=provider_name)
            if int(cursor.last_synced_topic_version) >= target_version:
                continue
            provider_sync_store.mark_synced(
                scope_key=scope_key,
                provider_name=provider_name,
                latest_worklog_id=latest_worklog_id,
                latest_topic_version=min(target_version, max(0, latest_topic_version)),
            )
            cursor_updates += 1

    result = {
        "status": "ok",
        "completed": True,
        "completed_at": _now_iso(),
        "marker_path": str(marker_path),
        "latest_worklog_id": latest_worklog_id,
        "scope_count": scope_count,
        "backfilled_scope_count": backfilled_scope_count,
        "cursor_updates": cursor_updates,
        "provider_names": provider_names,
    }
    _save_marker(marker_path, result)
    logger.info(
        "Provider sync backfill completed: scopes=%d backfilled=%d cursor_updates=%d latest_worklog_id=%d",
        scope_count,
        backfilled_scope_count,
        cursor_updates,
        latest_worklog_id,
    )
    return result


def auto_prepare_new_codex_providers(
    *,
    topic_state_store: TopicStateStore,
    provider_sync_store: ProviderSyncStore,
    codex_provider_names: Iterable[str],
    catchup_window: int,
) -> dict[str, object]:
    """Prepare newly added codex-family providers for existing scopes automatically."""
    provider_names = [name.strip() for name in codex_provider_names if str(name).strip()]
    if not provider_names:
        return {"status": "noop", "reason": "no_codex_providers"}

    window = max(1, int(catchup_window))
    states = topic_state_store.list()
    created = 0
    scanned = 0
    for scope_key, state in states.items():
        scanned += 1
        latest_topic_version = int(getattr(state, "topic_version", 0) or 0)
        if latest_topic_version <= 0:
            continue
        seed_version = max(0, latest_topic_version - window)
        for provider_name in provider_names:
            if provider_sync_store.exists(scope_key=scope_key, provider_name=provider_name):
                continue
            provider_sync_store.mark_synced(
                scope_key=scope_key,
                provider_name=provider_name,
                latest_topic_version=seed_version,
            )
            created += 1

    return {
        "status": "ok",
        "scopes_scanned": scanned,
        "cursors_created": created,
        "provider_names": provider_names,
        "catchup_window": window,
    }
