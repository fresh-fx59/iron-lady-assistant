#!/usr/bin/env python3
"""Emit F10 monitor-only baseline summary and alert when 2-week window is complete."""

from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


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


def parse_vector(result: Any, label: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not isinstance(result, list):
        return rows
    for item in result:
        metric = item.get("metric", {})
        value = item.get("value", [])
        try:
            numeric = float(value[1])
        except Exception:
            continue
        rows.append(
            {
                "label": metric.get(label, "unknown"),
                "value": numeric,
            }
        )
    rows.sort(key=lambda row: row["value"], reverse=True)
    return rows


def load_state(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def save_state(path: Path | None, state: dict[str, Any]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=True), encoding="utf-8")


def parse_ts(text: str) -> datetime:
    normalized = text.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def build_report(prometheus_url: str, timeout_s: int, window_days: int) -> dict[str, Any]:
    window = f"[{window_days}d]"
    turns = parse_scalar(
        run_promql(
            prometheus_url,
            f"sum(increase(telegrambot_cost_intel_turn_cost_usd_count{window}))",
            timeout_s,
        )
    )
    total_cost = parse_scalar(
        run_promql(
            prometheus_url,
            f"sum(increase(telegrambot_cost_intel_turn_cost_usd_sum{window}))",
            timeout_s,
        )
    )
    p95_cost = parse_scalar(
        run_promql(
            prometheus_url,
            f"histogram_quantile(0.95, sum by (le) (increase(telegrambot_cost_intel_turn_cost_usd_bucket{window})))",
            timeout_s,
        )
    )
    top_categories = parse_vector(
        run_promql(
            prometheus_url,
            f"topk(5, sum by (category) (increase(telegrambot_cost_intel_taxonomy_total{window})))",
            timeout_s,
        ),
        "category",
    )
    cost_by_mode = parse_vector(
        run_promql(
            prometheus_url,
            "topk(5, sum by (mode) (rate(telegrambot_cost_intel_turn_cost_usd_sum[1h])))",
            timeout_s,
        ),
        "mode",
    )
    return {
        "turns_14d": turns,
        "total_cost_usd_14d": total_cost,
        "p95_cost_usd_14d": p95_cost,
        "top_taxonomy_14d": top_categories,
        "top_cost_mode_1h": cost_by_mode,
    }


def change_state(
    phase: str,
    status: str,
    previous: dict[str, Any] | None,
) -> tuple[bool, str, str]:
    current = {"phase": phase, "status": status}
    if previous is None:
        if phase == "ready" and status == "ok":
            return True, "baseline_ready", "F10 2-week baseline is complete."
        return False, "initial", "Baseline still collecting."

    prev = {"phase": previous.get("phase"), "status": previous.get("status")}
    if prev == current:
        if phase == "ready":
            return False, "unchanged_ready", "Baseline complete; no state change."
        return False, "unchanged_collecting", "Baseline still collecting."

    if status == "critical":
        return True, "new_issue", "Baseline summary failed due to query/runtime error."
    if previous.get("phase") != "ready" and phase == "ready":
        return True, "baseline_ready", "F10 2-week baseline is complete."
    return False, "changed", "State changed but no new issue."


def main() -> int:
    parser = argparse.ArgumentParser(description="F10 baseline summary notifier.")
    parser.add_argument("--prometheus-url", default="http://45.151.30.146:9090")
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--baseline-start", required=True, help="ISO timestamp when baseline started")
    parser.add_argument("--window-days", type=int, default=14)
    parser.add_argument("--state-file")
    parser.add_argument("--alert-on-change", action="store_true")
    parser.add_argument("--format", choices=["json", "text"], default="json")
    args = parser.parse_args()

    now = datetime.now(UTC)
    baseline_start = parse_ts(args.baseline_start)
    baseline_end = baseline_start + timedelta(days=args.window_days)
    collecting = now < baseline_end
    state_path = Path(args.state_file).expanduser() if args.state_file else None

    status = "ok"
    report: dict[str, Any] = {}
    error_text: str | None = None
    try:
        report = build_report(args.prometheus_url, args.timeout, args.window_days)
    except Exception as exc:  # pragma: no cover - operational fallback
        status = "critical"
        error_text = str(exc)

    phase = "collecting" if collecting else "ready"
    previous = load_state(state_path)
    should_alert, change_type, summary = change_state(phase=phase, status=status, previous=previous)
    current_state = {"phase": phase, "status": status, "change_type": change_type}
    save_state(state_path, current_state)

    payload: dict[str, Any] = {
        "timestamp": now.isoformat(),
        "status": status,
        "phase": phase,
        "window_days": args.window_days,
        "baseline_start": baseline_start.isoformat(),
        "baseline_end": baseline_end.isoformat(),
        "remaining_days": max(0.0, (baseline_end - now).total_seconds() / 86400.0),
        "should_alert": should_alert if args.alert_on_change else (phase == "ready" or status == "critical"),
        "change_type": change_type,
        "summary": summary,
        "report": report,
    }
    if error_text:
        payload["error"] = error_text

    if args.format == "json":
        print(json.dumps(payload, ensure_ascii=True))
    else:
        print(f"[{status}] phase={phase} change={change_type} summary={summary}")
        if error_text:
            print(f"error={error_text}")
        elif report:
            print(
                "turns_14d={turns} total_cost_usd_14d={cost} p95_cost_usd_14d={p95}".format(
                    turns=report.get("turns_14d"),
                    cost=report.get("total_cost_usd_14d"),
                    p95=report.get("p95_cost_usd_14d"),
                )
            )
    return 2 if status == "critical" else 0


if __name__ == "__main__":
    sys.exit(main())
