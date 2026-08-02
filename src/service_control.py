"""Restarting a systemd unit, and telling the truth about the result.

`systemctl restart` on a unit that does not exist exits non-zero. The old code
in scheduler.py never checked, and the rollback handler printed "Restarting…"
regardless — so a rollback reported success having restarted nothing.
"""

from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)


async def restart_unit(unit: str) -> tuple[bool, str]:
    """Restart `unit`. Returns (ok, detail); detail names the unit on failure."""
    proc = await asyncio.create_subprocess_exec(
        "sudo",
        "systemctl",
        "restart",
        unit,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        reason = (stderr or b"").decode().strip() or f"exit code {proc.returncode}"
        detail = reason if unit in reason else f"{unit}: {reason}"
        logger.error("Restart of %s failed: %s", unit, reason)
        return False, detail
    return True, ""
