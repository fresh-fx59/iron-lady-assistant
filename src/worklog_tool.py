"""CLI for summary/session/commit linkage stored in the memory SQLite DB."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .memory import MemoryManager
from .memory_paths import resolve_memory_dir
from .sessions import SessionManager


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="worklog-tool")
    parser.add_argument(
        "--memory-dir",
        default=str(resolve_memory_dir()),
        help="Path to memory directory (default: MEMORY_DIR env/config).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    summary_cmd = sub.add_parser("record-summary", help="Persist a reflection summary with session metadata.")
    _add_scope_args(summary_cmd)
    summary_cmd.add_argument("--provider", default=None)
    summary_cmd.add_argument("--session-type", default=None)
    summary_cmd.add_argument("--session-id", default=None)
    summary_cmd.add_argument("--summary", required=True)
    summary_cmd.add_argument("--topics-json", default="[]")
    summary_cmd.add_argument("--decisions-json", default="[]")
    summary_cmd.add_argument("--entities-json", default="[]")
    summary_cmd.add_argument("--repo-path", default=None)
    summary_cmd.add_argument("--branch", default=None)
    summary_cmd.add_argument("--topic-label", default=None)
    summary_cmd.add_argument("--topic-started-at", default=None)

    commit_cmd = sub.add_parser("record-commit", help="Persist git commit metadata for the active session scope.")
    _add_scope_args(commit_cmd)
    commit_cmd.add_argument("--provider", default=None)
    commit_cmd.add_argument("--session-type", default=None)
    commit_cmd.add_argument("--session-id", default=None)
    commit_cmd.add_argument("--repo-path", default=".")
    commit_cmd.add_argument("--branch", default=None)
    commit_cmd.add_argument("--commit", dest="commit_ref", default="HEAD")
    commit_cmd.add_argument("--topic-label", default=None)
    commit_cmd.add_argument("--topic-started-at", default=None)

    auto_commit_cmd = sub.add_parser(
        "auto-record-commit",
        help="Resolve the most recent active session from sessions.json and persist the latest git commit.",
    )
    auto_commit_cmd.add_argument("--repo-path", default=".")
    auto_commit_cmd.add_argument("--commit", dest="commit_ref", default="HEAD")
    auto_commit_cmd.add_argument("--max-age-minutes", type=int, default=360)

    list_cmd = sub.add_parser("list", help="List worklog links from SQLite.")
    list_cmd.add_argument("--query", default=None)
    list_cmd.add_argument("--chat-id", type=int, default=None)
    list_cmd.add_argument("--limit", type=int, default=5)

    return parser


def _add_scope_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--chat-id", type=int, required=True)
    parser.add_argument("--message-thread-id", type=int, default=None)
    parser.add_argument("--scope-key", default=None)


def _scope_key(args: argparse.Namespace) -> str:
    if args.scope_key:
        return str(args.scope_key)
    thread_segment = "main" if args.message_thread_id is None else str(args.message_thread_id)
    return f"{args.chat_id}:{thread_segment}"


def _json_list(raw: str) -> list[str]:
    parsed = json.loads(raw)
    if not isinstance(parsed, list):
        raise ValueError("Expected JSON list")
    return [str(item) for item in parsed]


def _git(*args: str, repo_path: str) -> str:
    result = subprocess.run(
        ["git", "-C", repo_path, *args],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def _resolve_commit_metadata(repo_path: str, commit_ref: str) -> dict[str, object]:
    repo_root = _git("rev-parse", "--show-toplevel", repo_path=repo_path)
    branch = _git("rev-parse", "--abbrev-ref", "HEAD", repo_path=repo_path)
    commit_blob = _git(
        "show",
        "-s",
        "--format=%H%n%h%n%s%n%aI%n%cI",
        commit_ref,
        repo_path=repo_root,
    )
    commit_lines = commit_blob.splitlines()
    if len(commit_lines) < 5:
        raise RuntimeError("Could not parse git commit metadata")
    numstat_output = _git("show", "--numstat", "--format=", commit_lines[0], repo_path=repo_root)
    files: list[dict[str, object]] = []
    for line in numstat_output.splitlines():
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        additions_raw, deletions_raw, path = parts
        additions = None if additions_raw == "-" else int(additions_raw)
        deletions = None if deletions_raw == "-" else int(deletions_raw)
        files.append(
            {
                "path": path,
                "additions": additions,
                "deletions": deletions,
            }
        )
    return {
        "repo_path": repo_root,
        "branch": branch,
        "commit_sha": commit_lines[0],
        "short_sha": commit_lines[1],
        "subject": commit_lines[2],
        "authored_at": commit_lines[3],
        "committed_at": commit_lines[4],
        "files": files,
    }


def _parse_iso_timestamp(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        normalized = raw.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _latest_active_session(max_age_minutes: int) -> dict[str, object] | None:
    now = datetime.now(timezone.utc)
    threshold = now - timedelta(minutes=max_age_minutes)
    manager = SessionManager()
    candidates: list[dict[str, object]] = []
    for scope_key, session in manager.sessions.items():
        session_id = session.codex_session_id or session.claude_session_id
        session_type = "codex" if session.codex_session_id else "claude" if session.claude_session_id else None
        if not session.chat_id or not session_id or not session_type:
            continue
        last_activity = _parse_iso_timestamp(session.last_activity_at)
        if last_activity is None or last_activity < threshold:
            continue
        candidates.append(
            {
                "scope_key": scope_key,
                "chat_id": int(session.chat_id),
                "message_thread_id": session.message_thread_id,
                "provider": session.provider,
                "session_type": session_type,
                "session_id": session_id,
                "topic_label": session.topic_label,
                "topic_started_at": session.topic_started_at,
                "last_activity_at": session.last_activity_at,
            }
        )
    if not candidates:
        return None
    candidates.sort(key=lambda item: str(item.get("last_activity_at") or ""), reverse=True)
    return candidates[0]


def _session_from_env() -> dict[str, object] | None:
    chat_id_raw = os.getenv("ILA_WORKLOG_CHAT_ID")
    scope_key = os.getenv("ILA_WORKLOG_SCOPE_KEY")
    session_type = os.getenv("ILA_WORKLOG_SESSION_TYPE")
    session_id = os.getenv("ILA_WORKLOG_SESSION_ID")
    if not chat_id_raw or not scope_key or not session_type:
        return None
    try:
        chat_id = int(chat_id_raw)
    except ValueError:
        return None
    thread_raw = os.getenv("ILA_WORKLOG_MESSAGE_THREAD_ID")
    message_thread_id = int(thread_raw) if thread_raw and thread_raw.strip() else None
    return {
        "scope_key": scope_key,
        "chat_id": chat_id,
        "message_thread_id": message_thread_id,
        "provider": os.getenv("ILA_WORKLOG_PROVIDER") or None,
        "session_type": session_type,
        "session_id": session_id or None,
        "topic_label": os.getenv("ILA_WORKLOG_TOPIC_LABEL") or None,
        "topic_started_at": os.getenv("ILA_WORKLOG_TOPIC_STARTED_AT") or None,
        "last_activity_at": os.getenv("ILA_WORKLOG_LAST_ACTIVITY_AT") or None,
    }


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    manager = MemoryManager(Path(args.memory_dir))

    if args.command == "record-summary":
        episode_id = manager.add_episode(
            chat_id=args.chat_id,
            message_thread_id=args.message_thread_id,
            scope_key=_scope_key(args),
            provider=args.provider,
            session_type=args.session_type,
            session_id=args.session_id,
            topic_label=args.topic_label,
            topic_started_at=args.topic_started_at,
            repo_path=args.repo_path,
            branch=args.branch,
            summary=args.summary,
            topics=_json_list(args.topics_json),
            decisions=_json_list(args.decisions_json),
            entities=_json_list(args.entities_json),
        )
        print(json.dumps({"status": "ok", "episode_id": episode_id}, ensure_ascii=False, indent=2))
        return 0

    if args.command == "record-commit":
        metadata = _resolve_commit_metadata(args.repo_path, args.commit_ref)
        result = manager.record_commit_link(
            chat_id=args.chat_id,
            message_thread_id=args.message_thread_id,
            scope_key=_scope_key(args),
            provider=args.provider,
            session_type=args.session_type,
            session_id=args.session_id,
            repo_path=str(metadata["repo_path"]),
            branch=args.branch or str(metadata["branch"]),
            commit_sha=str(metadata["commit_sha"]),
            short_sha=str(metadata["short_sha"]),
            subject=str(metadata["subject"]),
            authored_at=str(metadata["authored_at"]),
            committed_at=str(metadata["committed_at"]),
            files=list(metadata["files"]),
            topic_label=args.topic_label,
            topic_started_at=args.topic_started_at,
        )
        print(json.dumps({"status": "ok", **result}, ensure_ascii=False, indent=2))
        return 0

    if args.command == "auto-record-commit":
        session = _session_from_env() or _latest_active_session(args.max_age_minutes)
        if session is None:
            print(json.dumps({"status": "noop", "reason": "no_recent_active_session"}, ensure_ascii=False, indent=2))
            return 0
        metadata = _resolve_commit_metadata(args.repo_path, args.commit_ref)
        result = manager.record_commit_link(
            chat_id=int(session["chat_id"]),
            message_thread_id=session["message_thread_id"],
            scope_key=str(session["scope_key"]),
            provider=session.get("provider"),
            session_type=str(session["session_type"]),
            session_id=str(session["session_id"]),
            repo_path=str(metadata["repo_path"]),
            branch=str(metadata["branch"]),
            commit_sha=str(metadata["commit_sha"]),
            short_sha=str(metadata["short_sha"]),
            subject=str(metadata["subject"]),
            authored_at=str(metadata["authored_at"]),
            committed_at=str(metadata["committed_at"]),
            files=list(metadata["files"]),
            topic_label=session.get("topic_label"),
            topic_started_at=session.get("topic_started_at"),
        )
        print(
            json.dumps(
                {"status": "ok", "resolved_scope": session["scope_key"], **result},
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    if args.command == "list":
        items = manager.list_worklog_links(query=args.query, limit=args.limit, chat_id=args.chat_id)
        print(json.dumps(items, ensure_ascii=False, indent=2))
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
