#!/usr/bin/env python3
"""Validate monitor-only cost observability signals from Prometheus."""

from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


@dataclass
class CheckResult:
    name: str
    status: str
    value: float | None
    threshold: float | None
    details: str


def run_promql(base_url: str, query: str, timeout_s: int) -> Any:
    encoded = urllib.parse.urlencode({"query": query})
    url = f"{base_url.rstrip('/')}/api/v1/query?{encoded}"
    req = urllib.request.Request(url=url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if payload.get("status") != "success":
        raise RuntimeError(f"Prometheus query failed: {payload}")
    return payload.get("data", {}).get("result", [])


def parse_scalar(result: Any) -> float | None:
    if not result:
        return None
    value = result[0].get("value")
    if not value or len(value) < 2:
        return None
    try:
        return float(value[1])
    except (TypeError, ValueError):
        return None


def evaluate(base_url: str, timeout_s: int) -> list[CheckResult]:
    checks: list[CheckResult] = []

    # Guard 1: scrape path must be up, otherwise Grafana will show no bot metrics.
    up = parse_scalar(run_promql(base_url, 'up{job="telegram_bot_metrics"}', timeout_s))
    checks.append(
        CheckResult(
            name="scrape_up",
            status="ok" if up and up >= 1 else "critical",
            value=up,
            threshold=1.0,
            details='Expected up{job="telegram_bot_metrics"} >= 1',
        )
    )

    # Guard 2: key series must exist.
    series_exists = parse_scalar(run_promql(base_url, "absent(telegrambot_messages_total)", timeout_s))
    # absent() returns 1 if missing; no series means healthy presence.
    missing = series_exists == 1.0
    checks.append(
        CheckResult(
            name="series_presence",
            status="critical" if missing else "ok",
            value=1.0 if missing else 0.0,
            threshold=0.0,
            details="Expected telegrambot_messages_total to be present",
        )
    )

    # Attention signal 1: cost-with-error ratio in last hour.
    err_ratio = parse_scalar(
        run_promql(
            base_url,
            'sum(increase(telegrambot_cost_intel_taxonomy_total{category="cost_with_error"}[1h])) '
            "/ clamp_min(sum(increase(telegrambot_claude_requests_total[1h])), 1)",
            timeout_s,
        )
    )
    checks.append(
        CheckResult(
            name="cost_with_error_ratio_1h",
            status="warn" if err_ratio is not None and err_ratio > 0.20 else "ok",
            value=err_ratio,
            threshold=0.20,
            details="Warn when >20% of requests in the last hour are cost_with_error",
        )
    )

    # Attention signal 2: retry-amplified cost frequency.
    retry_count = parse_scalar(
        run_promql(
            base_url,
            'sum(increase(telegrambot_cost_intel_taxonomy_total{category="retry_amplified_cost"}[1h]))',
            timeout_s,
        )
    )
    checks.append(
        CheckResult(
            name="retry_amplified_cost_1h",
            status="warn" if retry_count is not None and retry_count >= 3 else "ok",
            value=retry_count,
            threshold=3.0,
            details="Warn when retry_amplified_cost count in last hour is >=3",
        )
    )

    # Attention signal 3: steering-heavy turns (correlate F17 pressure and cost).
    steering_p95 = parse_scalar(
        run_promql(
            base_url,
            "histogram_quantile(0.95, sum by (le) (rate(telegrambot_cost_intel_steering_event_count_bucket[1h])))",
            timeout_s,
        )
    )
    checks.append(
        CheckResult(
            name="steering_events_p95_1h",
            status="warn" if steering_p95 is not None and steering_p95 > 4 else "ok",
            value=steering_p95,
            threshold=4.0,
            details="Warn when p95 steering_event_count > 4 in the last hour",
        )
    )

    return checks


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate monitor-only cost observability health.")
    parser.add_argument("--prometheus-url", default="http://45.151.30.146:9090")
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--format", choices=["json", "text"], default="json")
    args = parser.parse_args()

    now = datetime.now(UTC).isoformat()
    try:
        checks = evaluate(args.prometheus_url, args.timeout)
    except Exception as exc:  # pragma: no cover - operational fallback
        payload = {
            "timestamp": now,
            "status": "critical",
            "error": str(exc),
            "checks": [],
        }
        if args.format == "json":
            print(json.dumps(payload, ensure_ascii=True))
        else:
            print(f"[critical] validator_error: {exc}")
        return 2

    status = "ok"
    for item in checks:
        if item.status == "critical":
            status = "critical"
            break
        if item.status == "warn" and status != "critical":
            status = "warn"

    payload = {
        "timestamp": now,
        "status": status,
        "checks": [
            {
                "name": item.name,
                "status": item.status,
                "value": item.value,
                "threshold": item.threshold,
                "details": item.details,
            }
            for item in checks
        ],
    }

    if args.format == "json":
        print(json.dumps(payload, ensure_ascii=True))
    else:
        for item in checks:
            print(
                f"[{item.status}] {item.name}: value={item.value} "
                f"threshold={item.threshold} details={item.details}"
            )
        print(f"overall_status={status}")

    if status == "critical":
        return 2
    if status == "warn":
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
