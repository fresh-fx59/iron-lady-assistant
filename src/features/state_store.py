from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from .. import config

logger = logging.getLogger(__name__)


class StateStore:
    """Persistence helper for step-plan and scope snapshot state."""

    def __init__(self, memory_dir: Path) -> None:
        self._step_plan_state_path = memory_dir / "step_plan_state.json"
        self._scope_snapshot_path = memory_dir / "scope_snapshot.json"

    def step_plan_default_state(self) -> dict:
        return {
            "active": False,
            "name": "",
            "folder_path": "",
            "chat_id": 0,
            "message_thread_id": None,
            "user_id": 0,
            "steps": [],
            "current_index": 0,
            "current_task_id": None,
            "restart_between_steps": True,
            "last_error": "",
            "failure_count": 0,
            "last_failed_index": None,
            "auto_resume_blocked_until": "",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    def step_plan_is_blocked(self, state: dict) -> bool:
        raw = str(state.get("auto_resume_blocked_until") or "").strip()
        if not raw:
            return False
        try:
            blocked_until = datetime.fromisoformat(raw)
            if blocked_until.tzinfo is None:
                blocked_until = blocked_until.replace(tzinfo=timezone.utc)
        except Exception:
            return False
        return blocked_until > datetime.now(timezone.utc)

    def load_step_plan_state(self) -> dict:
        if not self._step_plan_state_path.exists():
            return self.step_plan_default_state()
        try:
            data = json.loads(self._step_plan_state_path.read_text(encoding="utf-8"))
        except Exception:
            logger.warning("Failed to read step plan state, resetting", exc_info=True)
            return self.step_plan_default_state()
        if not isinstance(data, dict):
            return self.step_plan_default_state()
        state = self.step_plan_default_state()
        state.update(data)
        return state

    def save_step_plan_state(self, state: dict) -> None:
        payload = self.step_plan_default_state()
        payload.update(state)
        payload["updated_at"] = datetime.now(timezone.utc).isoformat()
        self._step_plan_state_path.parent.mkdir(parents=True, exist_ok=True)
        self._step_plan_state_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    @staticmethod
    def parse_scope_key_components(scope_key: str) -> tuple[int, int | None]:
        chat_raw, _, thread_raw = scope_key.partition(":")
        chat_id = int(chat_raw)
        if not thread_raw or thread_raw == "main":
            return chat_id, None
        return chat_id, int(thread_raw)

    def snapshot_default_record(self, scope_key: str) -> dict:
        chat_id, message_thread_id = self.parse_scope_key_components(scope_key)
        return {
            "scope_key": scope_key,
            "chat_id": chat_id,
            "message_thread_id": message_thread_id,
            "pending_inputs": [],
            "inflight_pending_inputs": [],
            "inflight_pending_hash": "",
            "completed_pending_hashes": [],
            "processing": False,
            "provider": "",
            "active_prompt": "",
            "active_provider_cli": "",
            "active_model": "",
            "active_resume_arg": "",
            "resume_task_id": "",
            "claude_session_id": "",
            "codex_session_id": "",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    def load_scope_snapshots(self) -> dict[str, dict]:
        if not self._scope_snapshot_path.exists():
            return {}
        try:
            data = json.loads(self._scope_snapshot_path.read_text(encoding="utf-8"))
        except Exception:
            logger.warning("Failed to load scope snapshot state", exc_info=True)
            return {}
        if not isinstance(data, dict):
            return {}
        snapshots: dict[str, dict] = {}
        for scope_key, payload in data.items():
            if not isinstance(scope_key, str) or not isinstance(payload, dict):
                continue
            row = self.snapshot_default_record(scope_key)
            row.update(payload)
            snapshots[scope_key] = row
        return snapshots

    def save_scope_snapshots(self, snapshots: dict[str, dict]) -> None:
        self._scope_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._scope_snapshot_path.with_suffix(".tmp")
        tmp_path.write_text(
            json.dumps(snapshots, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp_path.replace(self._scope_snapshot_path)

    def mark_followup_inflight(self, scope_key: str, inputs: list[str]) -> str:
        if not config.SCOPE_SNAPSHOT_ENABLED:
            return ""
        digest = hashlib.sha1("\n".join(inputs).encode("utf-8")).hexdigest()[:16]
        snapshots = self.load_scope_snapshots()
        row = snapshots.get(scope_key, self.snapshot_default_record(scope_key))
        row["inflight_pending_inputs"] = list(inputs)
        row["inflight_pending_hash"] = digest
        row["updated_at"] = datetime.now(timezone.utc).isoformat()
        snapshots[scope_key] = row
        self.save_scope_snapshots(snapshots)
        return digest

    def is_followup_already_completed(self, scope_key: str, pending_hash: str) -> bool:
        if not config.SCOPE_SNAPSHOT_ENABLED or not pending_hash:
            return False
        snapshots = self.load_scope_snapshots()
        row = snapshots.get(scope_key)
        if not row:
            return False
        hashes = row.get("completed_pending_hashes") or []
        return pending_hash in hashes

    def mark_followup_completed(self, scope_key: str, pending_hash: str) -> None:
        if not config.SCOPE_SNAPSHOT_ENABLED or not pending_hash:
            return
        snapshots = self.load_scope_snapshots()
        row = snapshots.get(scope_key, self.snapshot_default_record(scope_key))
        hashes = list(row.get("completed_pending_hashes") or [])
        if pending_hash not in hashes:
            hashes.append(pending_hash)
        row["completed_pending_hashes"] = hashes[-config.SCOPE_SNAPSHOT_COMPLETED_HASHES_LIMIT :]
        row["inflight_pending_inputs"] = []
        row["inflight_pending_hash"] = ""
        row["updated_at"] = datetime.now(timezone.utc).isoformat()
        snapshots[scope_key] = row
        self.save_scope_snapshots(snapshots)


_DEFAULT_STATE_STORE = StateStore(config.MEMORY_DIR)


def get_default_state_store() -> StateStore:
    return _DEFAULT_STATE_STORE

