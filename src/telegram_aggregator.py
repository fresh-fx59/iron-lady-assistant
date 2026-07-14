"""src/telegram_aggregator.py — public daily digest pipeline (aggregator).

Standalone consumer of the @giedi_0 read proxy. Reuses TelegramDigestStore on its
OWN db file (never the lead/digest db — upsert_source overwrites role, and the
lead pipeline owns that file). State lives under AGGREGATOR_STATE_DIR because the
pipeline runs as claude-developer (the draft stage needs the Max OAuth session),
not as the iron-lady service user.
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, MutableMapping

logger = logging.getLogger(__name__)

AGG_ROLE = "aggregator"

_FILE_ENV_KEYS = (
    "TELEGRAM_PROXY_API_KEY",
    "TELEGRAM_AGGREGATOR_BOT_TOKEN",
    "IRONLADY_NOTIFY_BOT_TOKEN",
)

_USERNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{3,31}$")


@dataclass(frozen=True)
class AggregatorPaths:
    state_dir: Path
    db_path: Path
    sources_path: Path
    drafts_dir: Path


def resolve_paths() -> AggregatorPaths:
    state_dir = Path(
        os.getenv("AGGREGATOR_STATE_DIR", "/home/claude-developer/telegram-aggregator")
    )
    sources_raw = os.getenv("AGGREGATOR_SOURCES_PATH", "").strip()
    sources_path = Path(sources_raw) if sources_raw else state_dir / "sources.txt"
    drafts_dir = state_dir / "drafts"
    drafts_dir.mkdir(parents=True, exist_ok=True)
    return AggregatorPaths(
        state_dir=state_dir,
        db_path=state_dir / "aggregator.db",
        sources_path=sources_path,
        drafts_dir=drafts_dir,
    )


def load_file_env(env: MutableMapping[str, str] | None = None) -> None:
    """FOO_FILE=/path -> FOO=<file contents> for the known secret keys.

    Never overwrites an already-set FOO; missing files are a silent no-op so
    dry environments (tests, pre-enrollment) keep working.
    """
    target = env if env is not None else os.environ
    for key in _FILE_ENV_KEYS:
        if target.get(key):
            continue
        path_raw = target.get(f"{key}_FILE", "").strip()
        if not path_raw:
            continue
        path = Path(path_raw)
        if not path.exists():
            continue
        target[key] = path.read_text().strip()


def parse_sources(text: str) -> list[str]:
    """One source per line: @username / t.me/username / bare username.

    Comments (#...) and blanks skipped; t.me/+invite links skipped (not
    usernames); order-preserving dedup, case-insensitive key.
    """
    seen: set[str] = set()
    out: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        candidate = line
        candidate = re.sub(r"^https?://", "", candidate)
        candidate = re.sub(r"^t\.me/", "", candidate)
        candidate = candidate.lstrip("@").strip().rstrip("/")
        if candidate.startswith("+"):
            continue
        if not _USERNAME_RE.match(candidate):
            continue
        key = candidate.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(candidate)
    return out
