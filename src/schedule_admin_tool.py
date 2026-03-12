"""Administrative helpers for existing schedules."""

from __future__ import annotations

import argparse
import asyncio
import shlex
from pathlib import Path

from .scheduler import ScheduleManager


class _NoopTaskManager:
    async def submit(self, **kwargs):  # noqa: ANN003
        raise RuntimeError("schedule_admin_tool does not submit tasks")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Administrative helpers for recurring schedules.")
    parser.add_argument(
        "--schedules-db",
        default="memory/schedules.db",
        help="Path to schedules.db (default: memory/schedules.db)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    native = subparsers.add_parser("set-native-remediation", help="Update native remediation options.")
    native.add_argument("--schedule-id", required=True)
    native.add_argument("--auto-remediate", choices=["true", "false"], required=True)
    native.add_argument("--diagnose-command")
    native.add_argument("--remediate-command")
    return parser


async def _run(args: argparse.Namespace) -> int:
    manager = ScheduleManager(_NoopTaskManager(), Path(args.schedules_db))
    updated = await manager.update_native_schedule_options(
        args.schedule_id,
        auto_remediate=args.auto_remediate == "true",
        diagnose_command=shlex.split(args.diagnose_command) if args.diagnose_command else _sentinel(),
        remediate_command=shlex.split(args.remediate_command) if args.remediate_command else _sentinel(),
    )
    if not updated:
        print("schedule not found")
        return 1
    print(f"updated {args.schedule_id}")
    return 0


def _sentinel():
    from .scheduler import _NO_UPDATE

    return _NO_UPDATE


def main() -> int:
    parser = _parser()
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
