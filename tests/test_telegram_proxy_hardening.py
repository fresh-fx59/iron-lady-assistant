"""Hardening regression tests for the Telegram user-session proxy.

Covers three failure modes that can force an AUTH_KEY_DUPLICATED logout or
silently write the session to the wrong place:

  1. P0 singleton lock — only one proxy may hold the session at a time.
  2. P2 absolute session fallback path — the file-session fallback must never
     resolve relative to an arbitrary cwd.
  3. P2 log_out guard — telethon ``log_out()`` must never be called from src.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from src import config
from src.telegram_proxy import TelegramProxy


# ── 1. P0 singleton lock ──────────────────────────────────────────
def test_second_session_lock_acquisition_fails_fast(tmp_path, monkeypatch) -> None:
    lockfile = tmp_path / "telegram_proxy.lock"
    monkeypatch.setattr(config, "TELEGRAM_PROXY_LOCK_PATH", lockfile)

    first = TelegramProxy()
    first._acquire_session_lock()
    try:
        second = TelegramProxy()
        with pytest.raises(RuntimeError, match="already holds the session lock"):
            second._acquire_session_lock()
    finally:
        first._release_session_lock()

    # Once the first holder releases, the lock is acquirable again.
    third = TelegramProxy()
    third._acquire_session_lock()
    third._release_session_lock()


# ── 2. P2 absolute session fallback path ──────────────────────────
def test_session_fallback_path_is_absolute_by_default() -> None:
    assert config.TELEGRAM_PROXY_SESSION_PATH.is_absolute() is True


# ── 3. P2 log_out guard ───────────────────────────────────────────
def test_no_log_out_call_in_src() -> None:
    src_dir = Path(__file__).resolve().parent.parent / "src"
    offenders: list[str] = []
    for path in src_dir.glob("*.py"):
        text = path.read_text(encoding="utf-8", errors="ignore")
        if re.search(r"\blog_out\s*\(", text):
            offenders.append(path.name)
    assert not offenders, f"log_out( must never be called from src: {offenders}"
