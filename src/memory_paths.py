"""Config-free resolution of the memory directory.

`src/config.py` is the Telegram bot's runtime configuration: it loads `.env`
through `python-dotenv` and exits the process when `TELEGRAM_BOT_TOKEN` is
missing. That is correct for the bot — and wrong for the offline CLIs
(`memory_tool`, `worklog_tool`, `summary_inspector_tool`, `lifecycle_tool`)
which only read and write files under `memory/` and need no bot credentials at
all. Importing config for a single path constant coupled those CLIs to the
bot's environment and made them unrunnable without a token or a venv.

This module is deliberately stdlib-only so anything that just needs the path
can import it. `config.py` imports it too, so there is exactly one definition
of where memory lives and the two can never drift.
"""

from __future__ import annotations

import os
from pathlib import Path

#: Used when neither the MEMORY_DIR env var nor an explicit path is given.
DEFAULT_MEMORY_DIR = "memory"


def resolve_memory_dir(raw: str | None = None) -> Path:
    """Return the memory directory, honouring MEMORY_DIR and expanding `~`.

    Pure: it never creates the directory and never reads `.env`. Pass `raw` to
    resolve an explicit value (e.g. a CLI argument) with the same rules.
    """
    value = raw if raw is not None else os.getenv("MEMORY_DIR") or None
    if not value:
        return Path(DEFAULT_MEMORY_DIR)
    return Path(os.path.expanduser(value))


def resolve_lifecycle_db_path(memory_dir: Path | None = None) -> Path:
    """Path to the lifecycle queue SQLite DB inside the memory directory."""
    return (memory_dir or resolve_memory_dir()) / "lifecycle.db"
