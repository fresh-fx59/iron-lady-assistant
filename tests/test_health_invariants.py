from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.health_invariants import HealthInvariants


def test_build_block_contains_expected_fields(tmp_path: Path) -> None:
    inv = HealthInvariants()
    for _ in range(6):
        inv.record_provider_result(success=False)
        inv.record_empty_response(is_empty=True)
    for _ in range(3):
        inv.record_progress_channel_error()

    block = inv.build_block(
        app_version="1.2.3",
        memory_dir=tmp_path,
        max_chars=1200,
        stale_after_hours=1,
        provider_fail_warn_ratio=0.3,
        empty_warn_ratio=0.2,
        min_sample_size=5,
        claude_md_path=None,
    )

    assert block.startswith("<health_invariants>")
    assert "provider_failures_10m" in block
    assert "empty_responses_30m" in block
    assert "progress_errors_10m" in block
    assert "anomalies:" in block
    assert "provider_failure_spike" in block
    assert "empty_response_spike" in block
    assert "progress_channel_degraded" in block


def test_build_block_respects_max_chars(tmp_path: Path) -> None:
    inv = HealthInvariants()
    block = inv.build_block(
        app_version="1.2.3",
        memory_dir=tmp_path,
        max_chars=120,
        stale_after_hours=72,
        provider_fail_warn_ratio=0.3,
        empty_warn_ratio=0.2,
        min_sample_size=5,
        claude_md_path=None,
    )
    assert len(block) <= 120
    assert block.endswith("[truncated]") or block.endswith("</health_invariants>")


def test_version_sync_mismatch_detected(tmp_path: Path) -> None:
    inv = HealthInvariants()
    claude_md = tmp_path / "CLAUDE.md"
    claude_md.write_text("Current version: `9.9.9`", encoding="utf-8")

    block = inv.build_block(
        app_version="1.2.3",
        memory_dir=tmp_path,
        max_chars=1200,
        stale_after_hours=72,
        provider_fail_warn_ratio=0.3,
        empty_warn_ratio=0.2,
        min_sample_size=5,
        claude_md_path=claude_md,
    )
    assert "version_sync: mismatch" in block
    assert "version_sync_mismatch" in block
