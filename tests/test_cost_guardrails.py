from src.cost_guardrails import CostGuardrailEngine


def test_cost_guardrail_detects_repeated_empty_expensive_calls() -> None:
    engine = CostGuardrailEngine(anomaly_cooldown_minutes=1)
    scope = "123:main"
    anomalies_seen: list[list[str]] = []
    for _ in range(3):
        _, anomalies = engine.record_event(
            scope_key=scope,
            provider="claude",
            cost_usd=0.02,
            empty_response=True,
            is_error=False,
            reason="test",
        )
        anomalies_seen.append(anomalies)

    assert anomalies_seen[0] == []
    assert anomalies_seen[1] == []
    assert "repeated_empty_expensive_calls" in anomalies_seen[2]


def test_cost_guardrail_cooldown_dedupes_immediate_repeats() -> None:
    engine = CostGuardrailEngine(anomaly_cooldown_minutes=10)
    scope = "123:main"
    for _ in range(3):
        engine.record_event(
            scope_key=scope,
            provider="claude",
            cost_usd=0.02,
            empty_response=True,
            is_error=False,
            reason="test",
        )

    # Same anomaly pattern immediately repeated should be suppressed by cooldown.
    _, anomalies = engine.record_event(
        scope_key=scope,
        provider="claude",
        cost_usd=0.02,
        empty_response=True,
        is_error=False,
        reason="test",
    )
    assert anomalies == []
