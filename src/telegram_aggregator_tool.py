"""src/telegram_aggregator_tool.py — CLI for the public digest pipeline.

collect -> render-input -> (claude -p writes the draft) -> gate -> approve -> publish
Every stage prints one JSON line; the runner script and the operator both drive
the pipeline through this tool only.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from .telegram_aggregator import (
    build_draft_input,
    collect,
    load_file_env,
    parse_sources,
    resolve_paths,
)
from .telegram_aggregator_gates import parse_draft, run_gates
from .telegram_aggregator_publish import (
    BotApiTransport,
    DigestLedger,
    notify_operator,
    publish_next,
    render_messages,
)
from .telegram_digest import TelegramDigestStore
from .telegram_proxy_client import TelegramProxyClient

FOOTER = (
    "🤖 Дайджест собран автоматически, отобран и отредактирован вручную. "
    "Источники — у каждого пункта."
)


def _today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _print(payload: dict) -> None:
    print(json.dumps(payload, ensure_ascii=False))


def _cmd_collect(args: argparse.Namespace) -> int:
    load_file_env()
    paths = resolve_paths()
    if not paths.sources_path.exists():
        _print({"status": "error", "error": f"sources file missing: {paths.sources_path}"})
        return 1
    sources = parse_sources(paths.sources_path.read_text())
    store = TelegramDigestStore(paths.db_path)
    client = TelegramProxyClient()
    result = asyncio.run(collect(client, store, sources, collect_limit=args.collect_limit))
    _print({"status": "ok", **result})
    return 0


def _cmd_render_input(args: argparse.Namespace) -> int:
    paths = resolve_paths()
    store = TelegramDigestStore(paths.db_path)
    doc = build_draft_input(store, window_hours=args.window_hours, max_posts=args.max_posts)
    out = Path(args.out) if args.out else paths.drafts_dir / f"{doc['date']}-input.json"
    out.write_text(json.dumps(doc, ensure_ascii=False, indent=1))
    _print({"status": "ok", "out": str(out), "posts": len(doc["posts"])})
    return 0


def _cmd_gate(args: argparse.Namespace) -> int:
    paths = resolve_paths()
    date_key = args.date or _today()
    input_doc = json.loads(Path(args.input).read_text())
    known_links = {p["link"] for p in input_doc["posts"] if p.get("link")}
    source_texts = [p["text"] for p in input_doc["posts"]]
    try:
        stories = parse_draft(Path(args.draft).read_text())
    except ValueError as exc:
        _print({"status": "parse-error", "error": str(exc)})
        return 1
    result = run_gates(stories, known_links=known_links, source_texts=source_texts)
    if not result.ok:
        _print({"status": "gate-failed", "errors": result.errors})
        return 1
    date_label = datetime.fromisoformat(date_key).strftime("%d.%m.%Y")
    messages = render_messages(result.stories, date_label=date_label, footer=FOOTER)
    ledger = DigestLedger(paths.state_dir / "ledger.db")
    ledger.upsert_draft(date_key, messages)
    _print(
        {
            "status": "pending",
            "date_key": date_key,
            "stories": len(result.stories),
            "messages": len(messages),
            "dropped": result.errors,
        }
    )
    return 0


def _cmd_approve(args: argparse.Namespace) -> int:
    paths = resolve_paths()
    ledger = DigestLedger(paths.state_dir / "ledger.db")
    approved = ledger.approve(args.date)
    _print({"status": "approved" if approved else "nothing-to-approve", "date_key": approved})
    return 0 if approved else 1


def _cmd_publish(args: argparse.Namespace) -> int:
    load_file_env()
    paths = resolve_paths()
    ledger = DigestLedger(paths.state_dir / "ledger.db")
    token = os.getenv("TELEGRAM_AGGREGATOR_BOT_TOKEN", "").strip()
    chat = os.getenv("TELEGRAM_AGGREGATOR_CHANNEL", "").strip()
    transport = BotApiTransport(token) if token else None
    result = publish_next(ledger, transport, chat or None, dry_run=args.dry_run)
    _print(result)
    if result["status"] == "posted":
        notify_operator(f"✅ Дайджест {result['date_key']} опубликован ({result['messages']} сообщ.)")
    elif result["status"] in ("failed", "blocked"):
        notify_operator(f"❌ Публикация дайджеста: {result['status']} — {result.get('error', 'см. журнал')}")
    return 0 if result["status"] in ("posted", "dry-run", "skipped") else 1


def _cmd_status(args: argparse.Namespace) -> int:
    paths = resolve_paths()
    ledger = DigestLedger(paths.state_dir / "ledger.db")
    with ledger._connect() as con:  # noqa: SLF001 — same-package CLI introspection
        rows = con.execute(
            "SELECT date_key, status, updated_at FROM digests ORDER BY date_key DESC LIMIT 14"
        ).fetchall()
    _print({"digests": [dict(r) for r in rows]})
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m src.telegram_aggregator_tool")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("collect")
    p.add_argument("--collect-limit", type=int, default=200)
    p.set_defaults(func=_cmd_collect)

    p = sub.add_parser("render-input")
    p.add_argument("--window-hours", type=int, default=24)
    p.add_argument("--max-posts", type=int, default=150)
    p.add_argument("--out", default=None)
    p.set_defaults(func=_cmd_render_input)

    p = sub.add_parser("gate")
    p.add_argument("--draft", required=True)
    p.add_argument("--input", required=True)
    p.add_argument("--date", default=None)
    p.set_defaults(func=_cmd_gate)

    p = sub.add_parser("approve")
    p.add_argument("--date", default=None)
    p.set_defaults(func=_cmd_approve)

    p = sub.add_parser("publish")
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=_cmd_publish)

    p = sub.add_parser("status")
    p.set_defaults(func=_cmd_status)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
