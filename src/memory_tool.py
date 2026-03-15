"""CLI wrapper for structured memory operations."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from . import config
from .memory import MemoryManager


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="memory-tool")
    parser.add_argument(
        "--memory-dir",
        default=str(config.MEMORY_DIR),
        help="Path to memory directory (default: MEMORY_DIR env/config).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    list_cmd = sub.add_parser("list", help="List memory facts.")
    list_cmd.add_argument("--type", dest="fact_type", default=None)
    list_cmd.add_argument("--min-confidence", type=float, default=0.0)
    list_cmd.add_argument("--include-deleted", action="store_true")

    upsert_cmd = sub.add_parser("upsert", help="Create or replace a fact by key.")
    upsert_cmd.add_argument("--key", required=True)
    upsert_cmd.add_argument("--value", required=True)
    upsert_cmd.add_argument("--type", dest="fact_type", default=None)
    upsert_cmd.add_argument("--confidence", type=float, default=1.0)
    upsert_cmd.add_argument("--source", default="explicit")
    upsert_cmd.add_argument("--updated", default=None)
    upsert_cmd.add_argument("--mode", choices=["replace", "append"], default="replace")

    delete_cmd = sub.add_parser("delete", help="Soft-delete fact by key.")
    delete_cmd.add_argument("--key", required=True)
    delete_cmd.add_argument("--value", default=None)

    sub.add_parser("reclassify", help="Recompute type for all facts.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    manager = MemoryManager(Path(args.memory_dir))

    if args.command == "list":
        facts = manager.list_facts(
            fact_type=args.fact_type,
            min_confidence=float(args.min_confidence),
            include_deleted=bool(args.include_deleted),
        )
        print(json.dumps(facts, ensure_ascii=False, indent=2))
        return 0

    if args.command == "upsert":
        fact = manager.upsert_fact(
            key=args.key,
            value=args.value,
            fact_type=args.fact_type,
            confidence=float(args.confidence),
            source=args.source,
            updated=args.updated,
            mode=args.mode,
        )
        print(json.dumps({"status": "ok", "fact": fact}, ensure_ascii=False, indent=2))
        return 0

    if args.command == "delete":
        removed = manager.delete_fact(args.key, value=args.value)
        print(
            json.dumps(
                {"status": "ok", "removed": removed, "key": args.key, "value": args.value},
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    if args.command == "reclassify":
        updated_count = manager.reclassify_facts()
        print(json.dumps({"status": "ok", "updated_count": updated_count}, ensure_ascii=False, indent=2))
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
