"""CLI for retrieving the latest stored reflection summary and linked metadata."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .memory import MemoryManager
from .memory_paths import resolve_memory_dir


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="summary-inspector")
    parser.add_argument(
        "--memory-dir",
        default=str(resolve_memory_dir()),
        help="Path to memory directory (default: MEMORY_DIR env/config).",
    )
    parser.add_argument(
        "--format",
        choices=("json", "text"),
        default="json",
        help="Output format.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("latest", help="Show the newest stored episode and linked worklog metadata.")
    return parser


def _format_text(payload: dict[str, object]) -> str:
    episode = dict(payload["episode"])
    worklog = payload.get("worklog")
    lines = [
        f"timestamp: {episode['timestamp']}",
        f"summary: {episode['summary']}",
        f"topics: {json.dumps(episode['topics'], ensure_ascii=False)}",
        f"decisions: {json.dumps(episode['decisions'], ensure_ascii=False)}",
        f"entities: {json.dumps(episode['entities'], ensure_ascii=False)}",
    ]
    if not worklog:
        lines.append("worklog: null")
        return "\n".join(lines)

    worklog_dict = dict(worklog)
    lines.extend(
        [
            f"scope_key: {worklog_dict['scope_key']}",
            f"provider: {worklog_dict['provider']}",
            f"session_type: {worklog_dict['session_type']}",
            f"session_id: {worklog_dict['session_id']}",
            f"repo_path: {worklog_dict['repo_path']}",
            f"branch: {worklog_dict['branch']}",
            f"commits: {json.dumps(worklog_dict['commits'], ensure_ascii=False)}",
            f"files: {json.dumps(worklog_dict['files'], ensure_ascii=False)}",
        ]
    )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    manager = MemoryManager(Path(args.memory_dir))

    if args.command != "latest":
        parser.print_help()
        return 1

    payload = manager.latest_episode_details()
    if payload is None:
        print(json.dumps({"status": "empty", "reason": "no_episodes"}, ensure_ascii=False, indent=2))
        return 0

    if args.format == "text":
        print(_format_text(payload))
        return 0

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
