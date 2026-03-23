from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hash_text(text: str | None) -> str:
    payload = (text or "").encode("utf-8", errors="ignore")
    return hashlib.sha256(payload).hexdigest()


@dataclass
class ResumeEnvelope:
    scope_key: str
    task_id: str
    step_id: str
    provider_cli: str
    model: str
    session_id: str
    input_hash: str
    output_hash: str
    attempt_no: int
    updated_at: str
    state_version: int
    resume_reason: str
    status: str  # running|completed|failed


class ResumeStateStore:
    """Simple persisted envelope store for restart-safe resume decisions."""

    _STATE_VERSION = 1

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def _load_all_unlocked(self) -> dict[str, ResumeEnvelope]:
        if not self._path.exists():
            return {}
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        if not isinstance(data, dict):
            return {}

        envelopes: dict[str, ResumeEnvelope] = {}
        for scope_key, row in data.items():
            if not isinstance(row, dict):
                continue
            try:
                envelopes[scope_key] = ResumeEnvelope(**row)
            except TypeError:
                continue
        return envelopes

    def _save_all_unlocked(self, envelopes: dict[str, ResumeEnvelope]) -> None:
        payload = {k: asdict(v) for k, v in envelopes.items()}
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self._path)

    def record_start(
        self,
        *,
        scope_key: str,
        task_id: str,
        step_id: str,
        provider_cli: str,
        model: str,
        session_id: str | None,
        input_text: str,
        resume_reason: str = "restart",
    ) -> ResumeEnvelope:
        with self._lock:
            envelopes = self._load_all_unlocked()
            prev = envelopes.get(scope_key)
            attempt_no = (prev.attempt_no + 1) if prev else 1
            env = ResumeEnvelope(
                scope_key=scope_key,
                task_id=task_id,
                step_id=step_id,
                provider_cli=provider_cli,
                model=model,
                session_id=session_id or "",
                input_hash=_hash_text(input_text),
                output_hash="",
                attempt_no=attempt_no,
                updated_at=_now_iso(),
                state_version=self._STATE_VERSION,
                resume_reason=resume_reason,
                status="running",
            )
            envelopes[scope_key] = env
            self._save_all_unlocked(envelopes)
            return env

    def record_success(self, *, scope_key: str, output_text: str | None) -> None:
        with self._lock:
            envelopes = self._load_all_unlocked()
            env = envelopes.get(scope_key)
            if not env:
                return
            env.output_hash = _hash_text(output_text)
            env.status = "completed"
            env.updated_at = _now_iso()
            envelopes[scope_key] = env
            self._save_all_unlocked(envelopes)

    def record_failure(self, *, scope_key: str) -> None:
        with self._lock:
            envelopes = self._load_all_unlocked()
            env = envelopes.get(scope_key)
            if not env:
                return
            env.status = "failed"
            env.updated_at = _now_iso()
            envelopes[scope_key] = env
            self._save_all_unlocked(envelopes)

    def can_fast_resume(
        self,
        *,
        scope_key: str,
        input_text: str,
        ttl_seconds: int = 1800,
    ) -> tuple[bool, str]:
        with self._lock:
            envelopes = self._load_all_unlocked()
            env = envelopes.get(scope_key)
            if not env:
                return False, "missing"
            if env.status != "running":
                return False, "not_running"
            if env.input_hash != _hash_text(input_text):
                return False, "input_mismatch"
            try:
                ts = datetime.fromisoformat(env.updated_at)
            except Exception:
                return False, "bad_timestamp"
            age = (datetime.now(timezone.utc) - ts).total_seconds()
            if age > ttl_seconds:
                return False, "stale"
            return True, "ok"

    def clear(self, *, scope_key: str) -> None:
        with self._lock:
            envelopes = self._load_all_unlocked()
            if scope_key in envelopes:
                envelopes.pop(scope_key, None)
                self._save_all_unlocked(envelopes)


SteeringEventType = Literal[
    "clarify",
    "constraint_add",
    "constraint_remove",
    "priority_shift",
    "correction",
    "cancel",
]


@dataclass
class SteeringEvent:
    event_id: str
    created_at: str
    source_message_id: str
    event_type: SteeringEventType
    text: str
    intent_patch: str
    conflict_flags: list[str]
    applied: bool = False


class SteeringLedgerStore:
    """Append-only per-scope steering ledger with applied markers."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def _load_all_unlocked(self) -> dict[str, list[SteeringEvent]]:
        if not self._path.exists():
            return {}
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        if not isinstance(data, dict):
            return {}

        result: dict[str, list[SteeringEvent]] = {}
        for scope_key, rows in data.items():
            if not isinstance(rows, list):
                continue
            parsed: list[SteeringEvent] = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                try:
                    parsed.append(SteeringEvent(**row))
                except TypeError:
                    continue
            if parsed:
                result[scope_key] = parsed
        return result

    def _save_all_unlocked(self, payload: dict[str, list[SteeringEvent]]) -> None:
        serializable = {k: [asdict(item) for item in rows] for k, rows in payload.items()}
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(json.dumps(serializable, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self._path)

    def append(self, *, scope_key: str, event: SteeringEvent) -> None:
        with self._lock:
            rows = self._load_all_unlocked()
            existing = rows.get(scope_key, [])
            existing.append(event)
            rows[scope_key] = existing
            self._save_all_unlocked(rows)

    def get_unapplied(self, *, scope_key: str) -> list[SteeringEvent]:
        with self._lock:
            rows = self._load_all_unlocked()
            return [item for item in rows.get(scope_key, []) if not item.applied]

    def mark_applied(self, *, scope_key: str, event_ids: list[str]) -> None:
        if not event_ids:
            return
        targets = set(event_ids)
        with self._lock:
            rows = self._load_all_unlocked()
            changed = False
            for item in rows.get(scope_key, []):
                if item.event_id in targets and not item.applied:
                    item.applied = True
                    changed = True
            if changed:
                self._save_all_unlocked(rows)

    def clear(self, *, scope_key: str) -> None:
        with self._lock:
            rows = self._load_all_unlocked()
            if scope_key in rows:
                rows.pop(scope_key, None)
                self._save_all_unlocked(rows)


@dataclass
class ProviderSyncCursor:
    scope_key: str = ""
    provider_name: str = ""
    last_synced_worklog_id: int = 0  # legacy cursor
    last_synced_topic_version: int = 0
    last_injected_hash: str = ""
    updated_at: str = ""


class ProviderSyncStore:
    """Persist per-scope/per-provider sync cursors for context injection."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _key(scope_key: str, provider_name: str) -> str:
        return f"{scope_key}|{provider_name}"

    def _load_all_unlocked(self) -> dict[str, ProviderSyncCursor]:
        if not self._path.exists():
            return {}
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        if not isinstance(data, dict):
            return {}

        cursors: dict[str, ProviderSyncCursor] = {}
        for key, row in data.items():
            if not isinstance(row, dict):
                continue
            try:
                cursors[key] = ProviderSyncCursor(**row)
            except TypeError:
                continue
        return cursors

    def _save_all_unlocked(self, cursors: dict[str, ProviderSyncCursor]) -> None:
        payload = {k: asdict(v) for k, v in cursors.items()}
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self._path)

    def get(self, *, scope_key: str, provider_name: str) -> ProviderSyncCursor:
        key = self._key(scope_key, provider_name)
        with self._lock:
            cursors = self._load_all_unlocked()
            current = cursors.get(key)
            if current is not None:
                return current
            return ProviderSyncCursor(
                scope_key=scope_key,
                provider_name=provider_name,
                last_synced_worklog_id=0,
                last_injected_hash="",
                updated_at=_now_iso(),
            )

    def exists(self, *, scope_key: str, provider_name: str) -> bool:
        key = self._key(scope_key, provider_name)
        with self._lock:
            cursors = self._load_all_unlocked()
            return key in cursors

    def mark_synced(
        self,
        *,
        scope_key: str,
        provider_name: str,
        latest_worklog_id: int | None = None,
        latest_topic_version: int | None = None,
        injected_hash: str | None = None,
    ) -> ProviderSyncCursor:
        key = self._key(scope_key, provider_name)
        with self._lock:
            cursors = self._load_all_unlocked()
            current = cursors.get(key)
            if current is None:
                current = ProviderSyncCursor(
                    scope_key=scope_key,
                    provider_name=provider_name,
                    last_synced_worklog_id=0,
                    last_injected_hash="",
                    updated_at=_now_iso(),
                )

            if latest_worklog_id is not None and latest_worklog_id >= current.last_synced_worklog_id:
                current.last_synced_worklog_id = latest_worklog_id
            if latest_topic_version is not None and latest_topic_version >= current.last_synced_topic_version:
                current.last_synced_topic_version = latest_topic_version
            if injected_hash is not None:
                current.last_injected_hash = injected_hash
            current.updated_at = _now_iso()

            cursors[key] = current
            self._save_all_unlocked(cursors)
            return current


@dataclass
class TopicDeltaEvent:
    version: int
    provider_name: str
    summary: str
    decisions: list[str]
    open_tasks: list[str]
    artifacts: list[str]
    updated_at: str


@dataclass
class TopicState:
    scope_key: str
    topic_version: int
    updated_at: str
    events: list[TopicDeltaEvent]


class TopicStateStore:
    """Persist per-scope topic version and compact event deltas."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def _load_all_unlocked(self) -> dict[str, TopicState]:
        if not self._path.exists():
            return {}
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        if not isinstance(data, dict):
            return {}

        states: dict[str, TopicState] = {}
        for scope_key, row in data.items():
            if not isinstance(row, dict):
                continue
            events_raw = row.get("events")
            if not isinstance(events_raw, list):
                events_raw = []
            events: list[TopicDeltaEvent] = []
            for item in events_raw:
                if not isinstance(item, dict):
                    continue
                try:
                    events.append(TopicDeltaEvent(**item))
                except TypeError:
                    continue
            try:
                states[scope_key] = TopicState(
                    scope_key=str(row.get("scope_key") or scope_key),
                    topic_version=int(row.get("topic_version") or 0),
                    updated_at=str(row.get("updated_at") or _now_iso()),
                    events=events,
                )
            except Exception:
                continue
        return states

    def _save_all_unlocked(self, states: dict[str, TopicState]) -> None:
        payload = {
            key: {
                "scope_key": state.scope_key,
                "topic_version": state.topic_version,
                "updated_at": state.updated_at,
                "events": [asdict(event) for event in state.events],
            }
            for key, state in states.items()
        }
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self._path)

    def get(self, *, scope_key: str) -> TopicState:
        with self._lock:
            states = self._load_all_unlocked()
            existing = states.get(scope_key)
            if existing is not None:
                return existing
            return TopicState(
                scope_key=scope_key,
                topic_version=0,
                updated_at=_now_iso(),
                events=[],
            )

    def list(self) -> dict[str, TopicState]:
        with self._lock:
            return self._load_all_unlocked()

    def record_event(
        self,
        *,
        scope_key: str,
        provider_name: str,
        summary: str,
        decisions: list[str] | None = None,
        open_tasks: list[str] | None = None,
        artifacts: list[str] | None = None,
        max_events: int = 80,
    ) -> TopicState:
        with self._lock:
            states = self._load_all_unlocked()
            current = states.get(scope_key)
            if current is None:
                current = TopicState(
                    scope_key=scope_key,
                    topic_version=0,
                    updated_at=_now_iso(),
                    events=[],
                )
            next_version = current.topic_version + 1
            event = TopicDeltaEvent(
                version=next_version,
                provider_name=provider_name,
                summary=(summary or "").strip(),
                decisions=[item.strip() for item in (decisions or []) if item and item.strip()],
                open_tasks=[item.strip() for item in (open_tasks or []) if item and item.strip()],
                artifacts=[item.strip() for item in (artifacts or []) if item and item.strip()],
                updated_at=_now_iso(),
            )
            current.topic_version = next_version
            current.updated_at = event.updated_at
            current.events.append(event)
            if len(current.events) > max(1, max_events):
                current.events = current.events[-max(1, max_events):]
            states[scope_key] = current
            self._save_all_unlocked(states)
            return current

    def backfill_scope(
        self,
        *,
        scope_key: str,
        events: list[dict[str, object]],
        total_event_count: int | None = None,
        max_events: int = 80,
        skip_if_populated: bool = True,
    ) -> tuple[TopicState, bool]:
        """Seed a scope with historical events while preserving compact storage."""
        with self._lock:
            states = self._load_all_unlocked()
            current = states.get(scope_key)
            if (
                skip_if_populated
                and current is not None
                and (int(current.topic_version) > 0 or bool(current.events))
            ):
                return current, False

            total = int(total_event_count) if total_event_count is not None else len(events)
            total = max(0, total)
            tail_size = max(1, int(max_events))
            tail = list(events[-tail_size:])
            start_version = max(1, total - len(tail) + 1) if tail else 1

            compact_events: list[TopicDeltaEvent] = []
            for idx, item in enumerate(tail):
                summary = str(item.get("summary", "") or "").strip()
                decisions = [str(v).strip() for v in (item.get("decisions") or []) if str(v).strip()]
                open_tasks = [str(v).strip() for v in (item.get("open_tasks") or []) if str(v).strip()]
                artifacts = [str(v).strip() for v in (item.get("artifacts") or []) if str(v).strip()]
                provider_name = str(item.get("provider_name", "") or "").strip()
                updated_at = str(item.get("updated_at", "") or "").strip() or _now_iso()
                compact_events.append(
                    TopicDeltaEvent(
                        version=start_version + idx,
                        provider_name=provider_name,
                        summary=summary,
                        decisions=decisions,
                        open_tasks=open_tasks,
                        artifacts=artifacts,
                        updated_at=updated_at,
                    )
                )

            updated_at = compact_events[-1].updated_at if compact_events else _now_iso()
            state = TopicState(
                scope_key=scope_key,
                topic_version=total,
                updated_at=updated_at,
                events=compact_events,
            )
            states[scope_key] = state
            self._save_all_unlocked(states)
            return state, True

    def delta_since(self, *, scope_key: str, after_version: int, limit: int = 8) -> dict[str, object]:
        state = self.get(scope_key=scope_key)
        filtered = [event for event in state.events if int(event.version) > int(after_version)]
        if limit > 0:
            filtered = filtered[-limit:]
        return {
            "scope_key": scope_key,
            "latest_topic_version": int(state.topic_version),
            "events": [asdict(event) for event in filtered],
        }
