"""Prompt health invariants and lightweight runtime anomaly tracking."""

from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta, timezone
import re
from pathlib import Path

UTC = timezone.utc

_CLAUDE_MD_VERSION_RE = re.compile(r"Current version:\s*`?([0-9]+\.[0-9]+\.[0-9]+)`?")


class HealthInvariants:
    """Tracks runtime indicators and renders a compact prompt block."""

    def __init__(self) -> None:
        self._provider_events: deque[tuple[datetime, bool]] = deque(maxlen=500)
        self._empty_response_events: deque[tuple[datetime, bool]] = deque(maxlen=500)
        self._progress_errors: deque[datetime] = deque(maxlen=500)
        self._last_logged_signature: str = ""
        self._last_logged_at: datetime | None = None

    def record_provider_result(self, success: bool) -> None:
        self._provider_events.append((datetime.now(UTC), success))

    def record_empty_response(self, is_empty: bool) -> None:
        self._empty_response_events.append((datetime.now(UTC), is_empty))

    def record_progress_channel_error(self) -> None:
        self._progress_errors.append(datetime.now(UTC))

    def build_block(
        self,
        *,
        app_version: str,
        memory_dir: Path,
        max_chars: int = 1200,
        stale_after_hours: int = 72,
        provider_fail_warn_ratio: float = 0.30,
        empty_warn_ratio: float = 0.20,
        min_sample_size: int = 5,
        claude_md_path: Path | None = None,
    ) -> str:
        now = datetime.now(UTC)
        provider_window = now - timedelta(minutes=10)
        progress_window = now - timedelta(minutes=10)
        empty_window = now - timedelta(minutes=30)

        provider_samples = [ok for ts, ok in self._provider_events if ts >= provider_window]
        provider_total = len(provider_samples)
        provider_failures = sum(1 for ok in provider_samples if not ok)
        provider_fail_ratio = (provider_failures / provider_total) if provider_total else 0.0

        empty_samples = [is_empty for ts, is_empty in self._empty_response_events if ts >= empty_window]
        empty_total = len(empty_samples)
        empty_count = sum(1 for is_empty in empty_samples if is_empty)
        empty_ratio = (empty_count / empty_total) if empty_total else 0.0

        progress_error_count = sum(1 for ts in self._progress_errors if ts >= progress_window)
        memory_hours = self._memory_staleness_hours(memory_dir, now)
        stale_memory = memory_hours is not None and memory_hours >= stale_after_hours
        version_synced = self._version_synced(app_version, claude_md_path)

        anomalies: list[str] = []
        if not version_synced:
            anomalies.append("version_sync_mismatch")
        if provider_total >= min_sample_size and provider_fail_ratio >= provider_fail_warn_ratio:
            anomalies.append("provider_failure_spike")
        if empty_total >= min_sample_size and empty_ratio >= empty_warn_ratio:
            anomalies.append("empty_response_spike")
        if progress_error_count >= 3:
            anomalies.append("progress_channel_degraded")
        if stale_memory:
            anomalies.append("memory_stale")

        block_lines = [
            "<health_invariants>",
            f"generated_at: {now.isoformat()}",
            f"version_sync: {'ok' if version_synced else 'mismatch'}",
            (
                f"provider_failures_10m: {provider_failures}/{provider_total}"
                f" ({provider_fail_ratio:.0%})"
            ),
            f"empty_responses_30m: {empty_count}/{empty_total} ({empty_ratio:.0%})",
            f"progress_errors_10m: {progress_error_count}",
            (
                "memory_staleness_hours: unknown"
                if memory_hours is None
                else f"memory_staleness_hours: {memory_hours:.1f}"
            ),
            f"anomalies: {', '.join(anomalies) if anomalies else 'none'}",
            "</health_invariants>",
        ]
        block = "\n".join(block_lines)
        if len(block) > max_chars:
            block = block[: max(0, max_chars - 20)].rstrip() + "\n... [truncated]"

        self._maybe_log_anomalies(anomalies, now)
        return block

    def _memory_staleness_hours(self, memory_dir: Path, now: datetime) -> float | None:
        candidate_files = [
            memory_dir / "user_profile.yaml",
            memory_dir / "episodes.db",
            memory_dir / "identity.yaml",
        ]
        mtimes: list[float] = []
        for path in candidate_files:
            try:
                if path.exists():
                    mtimes.append(path.stat().st_mtime)
            except OSError:
                continue
        if not mtimes:
            return None
        latest = datetime.fromtimestamp(max(mtimes), tz=UTC)
        return (now - latest).total_seconds() / 3600.0

    def _version_synced(self, app_version: str, claude_md_path: Path | None) -> bool:
        if not claude_md_path or not claude_md_path.exists():
            return True
        try:
            text = claude_md_path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            return True
        match = _CLAUDE_MD_VERSION_RE.search(text)
        if not match:
            return True
        return match.group(1).strip() == app_version.strip()

    def _maybe_log_anomalies(self, anomalies: list[str], now: datetime) -> None:
        if not anomalies:
            return
        signature = ",".join(sorted(anomalies))
        cooldown = timedelta(minutes=10)
        if (
            self._last_logged_signature == signature
            and self._last_logged_at is not None
            and (now - self._last_logged_at) < cooldown
        ):
            return
        self._last_logged_signature = signature
        self._last_logged_at = now
        # Logging is intentionally simple: callers can scrape warning logs.
        import logging

        logging.getLogger(__name__).warning(
            "Health invariants detected anomalies: %s",
            signature,
        )
