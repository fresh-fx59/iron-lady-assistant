"""src/telegram_dialog_scan_tool.py — CLI for the daily NEW-dialog scanner.

    python -m src.telegram_dialog_scan_tool scan [--dry-run] [--limit N]

Prints the classification table on stdout plus one JSON summary line (same shape
as the other pipeline tools), and notifies the operator ONLY when something
changed or broke — including when the run itself failed, because a silent nightly
failure is indistinguishable from "no new dialogs".
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import traceback

from .telegram_aggregator import load_file_env
from .telegram_aggregator_publish import notify_operator
from .telegram_dialog_scan import TOPIC_READ_POSTS, resolve_scan_paths, run_scan
from .telegram_proxy_client import TelegramProxyClient

logger = logging.getLogger(__name__)


def _cmd_scan(args: argparse.Namespace) -> int:
    # load_file_env() BEFORE reading the env, and then pass the key EXPLICITLY:
    # the unit only delivers TELEGRAM_PROXY_API_KEY_FILE, and
    # config.TELEGRAM_PROXY_API_KEY is an os.getenv evaluated at IMPORT time —
    # i.e. before this call — so the client's config fallback is always empty
    # here. Same shape as telegram_aggregator_tool._cmd_collect.
    load_file_env()
    notifier = None if args.no_notify else notify_operator
    try:
        client = TelegramProxyClient(
            api_key=os.environ.get("TELEGRAM_PROXY_API_KEY") or None,
            base_url=os.environ.get("TELEGRAM_PROXY_BASE_URL") or None,
        )
        dialogs = asyncio.run(client.list_dialogs(limit=args.limit, with_linked=True))

        def post_reader(kind: str, entity_id: int) -> list[str]:
            """The topical gate's evidence: the candidate's own recent posts.

            READ-ONLY, and read through the proxy the account already uses — it
            already follows these dialogs, so nothing is joined or subscribed to
            here. Pacing/bounding lives in collect_topic_scores, next to the same
            discipline the linked-chat sweep uses.
            """
            messages = asyncio.run(
                client.read_messages(
                    kind=kind, entity_id=entity_id, min_id=0, limit=TOPIC_READ_POSTS, recent_first=True
                )
            )
            return [str(m.get("text") or "") for m in messages]

        def lead_enroller(**kwargs: object) -> dict:
            """The durable half of an enrolment, over the proxy (see _register_leads).

            asyncio.TimeoutError is deliberately allowed to escape unwrapped: the
            caller distinguishes it from a clean failure because a timeout is
            AMBIGUOUS — the write may have landed — and must not be retried.
            """
            return asyncio.run(client.enrol_lead_source(**kwargs))  # type: ignore[arg-type]

        report = run_scan(
            paths=resolve_scan_paths(),
            dialogs=dialogs,
            dry_run=args.dry_run,
            notifier=notifier,
            post_reader=post_reader,
            lead_enroller=None if args.dry_run else lead_enroller,
        )
    except Exception as exc:  # noqa: BLE001 — deliberately everything, see below
        # The three likeliest failures (a missing proxy key => RuntimeError, a
        # 404 from an un-deployed route, a locked sqlite db) all raise BEFORE
        # run_scan can notify, and this job's ONLY output is that notification:
        # an uncaught traceback in the journal looks exactly like a quiet "no new
        # dialogs" night.
        detail = f"dialog-scan FAILED before it could report: {type(exc).__name__}: {exc}"
        logger.error("%s\n%s", detail, traceback.format_exc())
        delivered = bool(notifier(detail)) if notifier is not None else False
        print(
            json.dumps(
                {"stage": "dialog-scan", "status": "error", "error": detail, "notified": delivered},
                ensure_ascii=False,
            )
        )
        return 1

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
                "requarantined": len(report.requarantined),
                "wrote_nothing": report.wrote_nothing,
                "skipped_locked": report.skipped_locked,
                "errors": report.errors,
                "notified": report.notified,
                "notify_failed": report.notify_failed,
            },
            ensure_ascii=False,
        )
    )
    # A failed notification is a failed RUN: the only output of this job is that
    # message, so exit non-zero and let the unit's OnFailure= page.
    return 1 if (report.errors or report.notify_failed) else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m src.telegram_dialog_scan_tool")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("scan", help="enumerate dialogs, classify the new ones, enroll the passing ones")
    # 500 matches the /v1/dialogs clamp (which matches /v1/channels'): asking for
    # 1000 every night only ever got silently clamped.
    p.add_argument("--limit", type=int, default=500)
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="print the table and write nothing — but STILL notify, so 'watch it for a night' works",
    )
    p.add_argument("--no-notify", action="store_true", help="never message the operator (local runs)")
    p.set_defaults(func=_cmd_scan)
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO)
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
