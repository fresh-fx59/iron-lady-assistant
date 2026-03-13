from __future__ import annotations

import argparse
import sys
import time

from .config import LIFECYCLE_DB_PATH
from .lifecycle_queue import LifecycleQueueStore


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Lifecycle drain/deploy coordination tool")
    sub = parser.add_subparsers(dest="command", required=True)

    begin = sub.add_parser("begin-deploy")
    begin.add_argument("--commit", required=True)
    begin.add_argument("--scope-key", default="deploy:main")
    begin.add_argument("--chat-id", type=int)
    begin.add_argument("--thread-id", type=int)

    wait = sub.add_parser("wait-for-idle")
    wait.add_argument("--timeout", type=int, default=300)
    wait.add_argument("--poll-seconds", type=float, default=1.0)

    ready = sub.add_parser("wait-until-ready")
    ready.add_argument("--operation-id", required=True)
    ready.add_argument("--timeout", type=int, default=600)
    ready.add_argument("--poll-seconds", type=float, default=1.0)

    restarting = sub.add_parser("mark-restarting")
    restarting.add_argument("--operation-id", required=True)

    complete = sub.add_parser("mark-completed")
    complete.add_argument("--operation-id", required=True)

    failed = sub.add_parser("mark-failed")
    failed.add_argument("--operation-id")
    failed.add_argument("--error", required=True)

    status = sub.add_parser("status")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    store = LifecycleQueueStore(LIFECYCLE_DB_PATH)

    if args.command == "begin-deploy":
        operation_id = store.begin_deploy(
            requested_commit=args.commit,
            requested_by_scope=args.scope_key,
            requested_by_chat_id=args.chat_id,
            requested_by_thread_id=args.thread_id,
        )
        print(operation_id)
        return 0

    if args.command == "wait-for-idle":
        deadline = time.monotonic() + max(1, args.timeout)
        while time.monotonic() < deadline:
            active = store.active_scope_count()
            if active == 0:
                print("idle")
                return 0
            checkpointed = store.checkpoint_interactive_scopes()
            if checkpointed:
                active = store.active_scope_count()
                if active == 0:
                    print(f"idle checkpointed={checkpointed}")
                    return 0
            time.sleep(max(0.1, args.poll_seconds))
        print(f"timeout active={store.active_scope_count()}", file=sys.stderr)
        return 1

    if args.command == "wait-until-ready":
        deadline = time.monotonic() + max(1, args.timeout)
        while time.monotonic() < deadline:
            status = store.activate_deploy_if_ready(args.operation_id)
            if status == "draining":
                print("draining")
                return 0
            if status in {"completed", "failed", "cancelled", "missing"}:
                print(f"operation_status={status}", file=sys.stderr)
                return 1
            time.sleep(max(0.1, args.poll_seconds))
        print(f"timeout operation_id={args.operation_id}", file=sys.stderr)
        return 1

    if args.command == "mark-restarting":
        store.mark_restarting(args.operation_id)
        return 0

    if args.command == "mark-completed":
        store.mark_operation_completed(args.operation_id)
        return 0

    if args.command == "mark-failed":
        store.mark_operation_failed(args.operation_id, args.error)
        return 0

    if args.command == "status":
        print(f"phase={store.barrier_phase()} active={store.active_scope_count()}")
        return 0

    parser.error("unknown command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
