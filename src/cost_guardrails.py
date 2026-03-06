"""Cost usage event tracking and anomaly guardrails."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

UTC = timezone.utc


@dataclass(frozen=True)
class UsageEvent:
    scope_key: str
    provider: str
    cost_usd: float
    empty_response: bool
    is_error: bool
    recorded_at: datetime
    reason: str


class CostGuardrailEngine:
    def __init__(
        self,
        *,
        max_events_per_scope: int = 500,
        anomaly_cooldown_minutes: int = 10,
    ) -> None:
        self._events_by_scope: dict[str, deque[UsageEvent]] = defaultdict(lambda: deque(maxlen=500))
        self._events_by_scope = defaultdict(lambda: deque(maxlen=max_events_per_scope))
        self._anomaly_cooldown = timedelta(minutes=max(1, anomaly_cooldown_minutes))
        self._last_anomaly_emit: dict[tuple[str, str], datetime] = {}

    def record_event(
        self,
        *,
        scope_key: str,
        provider: str,
        cost_usd: float,
        empty_response: bool,
        is_error: bool,
        reason: str = "",
    ) -> tuple[UsageEvent, list[str]]:
        event = UsageEvent(
            scope_key=scope_key,
            provider=provider or "unknown",
            cost_usd=max(0.0, float(cost_usd or 0.0)),
            empty_response=bool(empty_response),
            is_error=bool(is_error),
            recorded_at=datetime.now(UTC),
            reason=reason or ("zero_cost" if cost_usd <= 0 else "ok"),
        )
        bucket = self._events_by_scope[scope_key]
        bucket.append(event)
        anomalies = self._detect_anomalies(scope_key, event)
        fresh_anomalies: list[str] = []
        for anomaly in anomalies:
            key = (scope_key, anomaly)
            last = self._last_anomaly_emit.get(key)
            if last and (event.recorded_at - last) < self._anomaly_cooldown:
                continue
            self._last_anomaly_emit[key] = event.recorded_at
            fresh_anomalies.append(anomaly)
        return event, fresh_anomalies

    def _detect_anomalies(self, scope_key: str, event: UsageEvent) -> list[str]:
        anomalies: list[str] = []
        bucket = self._events_by_scope.get(scope_key)
        if not bucket:
            return anomalies

        now = event.recorded_at
        hour_ago = now - timedelta(hours=1)
        recent = [e for e in bucket if e.recorded_at >= hour_ago]
        recent_costs = [e.cost_usd for e in recent]
        avg_recent = (sum(recent_costs) / len(recent_costs)) if recent_costs else 0.0

        # 1) sudden cost spike
        if event.cost_usd >= 0.10 and avg_recent > 0 and event.cost_usd >= avg_recent * 3.0:
            anomalies.append("sudden_cost_spike")

        # 2) repeated empty expensive calls
        tail = list(recent)[-3:]
        if (
            len(tail) == 3
            and all(e.empty_response and e.cost_usd >= 0.01 for e in tail)
        ):
            anomalies.append("repeated_empty_expensive_calls")

        # 3) provider-specific drift
        provider_events = [e for e in recent if e.provider == event.provider]
        if len(provider_events) >= 5 and len(recent) >= 5:
            provider_avg = sum(e.cost_usd for e in provider_events) / len(provider_events)
            overall_avg = sum(e.cost_usd for e in recent) / len(recent)
            if overall_avg > 0 and provider_avg >= overall_avg * 1.8:
                anomalies.append("provider_specific_drift")

        deduped: list[str] = []
        seen: set[str] = set()
        for item in anomalies:
            if item in seen:
                continue
            seen.add(item)
            deduped.append(item)
        return deduped
