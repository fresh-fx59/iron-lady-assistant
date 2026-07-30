"""src/telegram_dialog_scan_tool.py — CLI for the daily NEW-dialog scanner.

    python -m src.telegram_dialog_scan_tool scan [--dry-run] [--limit N]

Prints the classification table on stdout plus one JSON summary line (same shape
as the other pipeline tools), and notifies the operator ONLY when something
changed or broke.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging

from .telegram_aggregator import load_file_env
from .telegram_aggregator_publish import notify_operator
from .telegram_dialog_scan import resolve_scan_paths, run_scan
from .telegram_proxy_client import TelegramProxyClient

logger = logging.getLogger(__name__)


def _cmd_scan(args: argparse.Namespace) -> int:
    load_file_env()
    dialogs = asyncio.run(TelegramProxyClient().list_dialogs(limit=args.limit, with_linked=True))
    report = run_scan(
        paths=resolve_scan_paths(),
        dialogs=dialogs,
        dry_run=args.dry_run,
        notifier=None if args.no_notify else notify_operator,
    )
    print(report.text)
    print(
        json.dumps(
            {
                "stage": "dialog-scan",
                "dry_run": report.dry_run,
                "total_dialogs": report.total_dialogs,
                "by_kind": report.by_kind,
                "new": len(report.new_dialogs),
                "added_news": report.added_news,
                "added_chat": report.added_chat,
                "added_leads": report.added_leads,
                "mirror_pending": report.mirror_pending,
                "errors": report.errors,
                "notified": report.notified,
            },
            ensure_ascii=False,
        )
    )
    return 1 if report.errors else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m src.telegram_dialog_scan_tool")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("scan", help="enumerate dialogs, classify the new ones, enroll the passing ones")
    p.add_argument("--limit", type=int, default=1000)
    p.add_argument("--dry-run", action="store_true", help="print the classification table, write nothing")
    p.add_argument("--no-notify", action="store_true", help="never message the operator (local runs)")
    p.set_defaults(func=_cmd_scan)
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO)
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
