from unittest.mock import Mock

from src.metrics import observe_cost_intelligence_turn, start_metrics_server


def test_metrics_server_disabled_when_port_zero(monkeypatch):
    start_mock = Mock()
    monkeypatch.setattr("src.metrics.start_http_server", start_mock)

    start_metrics_server(0)

    start_mock.assert_not_called()


def test_metrics_server_bind_error_is_non_fatal(monkeypatch):
    start_mock = Mock(side_effect=PermissionError("Operation not permitted"))
    monkeypatch.setattr("src.metrics.start_http_server", start_mock)

    start_metrics_server(9101)

    start_mock.assert_called_once_with(9101)


def test_cost_intel_classifies_high_cost_and_tool_inflation():
    categories = observe_cost_intelligence_turn(
        scope_key="test-scope-a",
        provider="codex",
        model="gpt-5-codex",
        mode="foreground",
        cost_usd=0.05,
        num_turns=2,
        duration_ms=1500,
        is_error=False,
        is_cancelled=False,
        is_empty_response=False,
        tool_timeout=False,
        tool_names=["read", "grep", "bash", "task", "edit", "glob"],
        message_size_in=320,
        message_size_out=480,
        step_plan_active=False,
        steering_event_count=0,
        attempts=1,
    )

    assert "high_cost_success" in categories
    assert "tool_driven_cost_inflation" in categories


def test_cost_intel_classifies_error_empty_and_retry():
    categories = observe_cost_intelligence_turn(
        scope_key="test-scope-b",
        provider="claude",
        model="sonnet",
        mode="background",
        cost_usd=0.01,
        num_turns=1,
        duration_ms=800,
        is_error=True,
        is_cancelled=False,
        is_empty_response=True,
        tool_timeout=True,
        tool_names=["web_search"],
        message_size_in=42,
        message_size_out=0,
        step_plan_active=True,
        steering_event_count=1,
        attempts=2,
    )

    assert "cost_with_error" in categories
    assert "cost_with_empty" in categories
    assert "retry_amplified_cost" in categories


def test_cost_intel_classifies_scope_hotspot_after_repeated_success():
    categories = []
    for idx in range(6):
        categories = observe_cost_intelligence_turn(
            scope_key="test-scope-hotspot",
            provider="codex",
            model="gpt-5-codex",
            mode="foreground",
            cost_usd=0.03,
            num_turns=1,
            duration_ms=900 + idx,
            is_error=False,
            is_cancelled=False,
            is_empty_response=False,
            tool_timeout=False,
            tool_names=["read"],
            message_size_in=50,
            message_size_out=120,
            step_plan_active=False,
            steering_event_count=0,
            attempts=1,
        )

    assert "scope_hotspot" in categories
