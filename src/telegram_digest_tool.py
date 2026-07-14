"""CLI for Telegram digest collection and schedule bootstrapping."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from . import config
from .scheduler import ScheduleManager
from .telegram_digest import TelegramDigestStore, collect_digest_sync


class _NoopTaskManager:
    async def submit(self, **kwargs):  # noqa: ANN003
        raise RuntimeError("submit() is not used by install-time schedule creation")


def _build_collector_prompt(python_bin: str) -> str:
    return (
        "[[SCHEDULE_NATIVE]]\n"
        f"command: {python_bin} -m src.telegram_digest_tool collect\n"
        "Collect Telegram channel and linked discussion-chat messages into the local digest store.\n"
        "Refresh the latest briefing file for the next daily digest run.\n"
        "Do not escalate on steady-state success."
    )


def _build_daily_prompt(brief_path: Path) -> str:
    return (
        "[[SCHEDULE_DELIVER]]\n"
        "USE_TOOL: edge-tts-safe\n"
        "You are preparing Samarin's daily Telegram digest for Alex.\n"
        f"Read the latest collected briefing from: {brief_path}\n"
        "Write in Russian.\n"
        "Keep it short and executive: 5-10 bullets max, then a one-sentence conclusion.\n"
        "Cover the last 24 hours across tracked Telegram channels and linked discussion chats.\n"
        "Focus on what happened, what people discussed, repeated signals across sources, and what changed.\n"
        "Include only a few source links when they are genuinely important.\n"
        "After the text digest, generate a Russian voice note version of the same digest.\n"
        "Return the normal digest text and include the generated audio as a voice note using MEDIA output.\n"
        "Use [[audio_as_voice]] for the audio attachment."
    )


def _cmd_collect(args: argparse.Namespace) -> int:
    payload = collect_digest_sync(
        db_path=Path(args.db_path) if args.db_path else None,
        brief_path=Path(args.brief_path) if args.brief_path else None,
        window_hours=args.window_hours,
        source_limit=args.source_limit,
        collect_limit=args.collect_limit,
        roles=(args.role,) if args.role else None,
        join_db_path=Path(args.join_db_path) if args.join_db_path else None,
    )
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def _cmd_render(args: argparse.Namespace) -> int:
    store = TelegramDigestStore(Path(args.db_path) if args.db_path else None)
    print(
        store.render_briefing(
            window_hours=args.window_hours,
            per_source_limit=args.per_source_limit,
            source_limit=args.source_limit,
        ),
        end="",
    )
    return 0


def _cmd_install(args: argparse.Namespace) -> int:
    manager = ScheduleManager(_NoopTaskManager(), Path(args.schedules_db))
    collector_prompt = _build_collector_prompt(args.python_bin)
    daily_prompt = _build_daily_prompt(Path(args.brief_path))

    import asyncio

    async def _install() -> dict[str, str]:
        collector_id = await manager.create_every(
            chat_id=args.chat_id,
            message_thread_id=args.message_thread_id,
            user_id=args.user_id,
            prompt=collector_prompt,
            interval_minutes=args.collect_interval_minutes,
            model=args.model,
            session_id=None,
            provider_cli=args.provider_cli,
            resume_arg=args.resume_arg,
        )
        daily_id = await manager.create_daily(
            chat_id=args.chat_id,
            message_thread_id=args.message_thread_id,
            user_id=args.user_id,
            prompt=daily_prompt,
            daily_time=args.daily_time,
            timezone_name=args.timezone_name,
            model=args.model,
            session_id=None,
            provider_cli=args.provider_cli,
            resume_arg=args.resume_arg,
        )
        return {"collector_schedule_id": collector_id, "daily_schedule_id": daily_id}

    print(json.dumps(asyncio.run(_install()), ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m src.telegram_digest_tool")
    sub = parser.add_subparsers(dest="command", required=True)

    collect = sub.add_parser("collect")
    collect.add_argument("--db-path", default=str(config.TELEGRAM_DIGEST_DB_PATH))
    collect.add_argument("--brief-path", default=str(config.TELEGRAM_DIGEST_BRIEF_PATH))
    collect.add_argument("--window-hours", type=int, default=config.TELEGRAM_DIGEST_WINDOW_HOURS)
    collect.add_argument("--source-limit", type=int, default=config.TELEGRAM_DIGEST_SOURCE_LIMIT)
    collect.add_argument("--collect-limit", type=int, default=config.TELEGRAM_DIGEST_COLLECT_LIMIT)
    # Role filter. Omitted → the legacy digest pipeline (role='digest', unchanged).
    # '--role lead' first syncs the JOINED groups into the store, then reads ONLY
    # those lead sources incrementally — never the legacy digest sources.
    collect.add_argument("--role", choices=("digest", "lead"), default=None)
    collect.add_argument(
        "--join-db-path",
        default=str(config.TELEGRAM_PROXY_JOIN_DB_PATH),
        help="JOIN store to mirror joined lead groups from (used with --role lead).",
    )
    collect.set_defaults(func=_cmd_collect)

    render = sub.add_parser("render")
    render.add_argument("--db-path", default=str(config.TELEGRAM_DIGEST_DB_PATH))
    render.add_argument("--window-hours", type=int, default=config.TELEGRAM_DIGEST_WINDOW_HOURS)
    render.add_argument("--source-limit", type=int, default=80)
    render.add_argument("--per-source-limit", type=int, default=8)
    render.set_defaults(func=_cmd_render)

    install = sub.add_parser("install")
    install.add_argument("--schedules-db", default=str(config.MEMORY_DIR / "schedules.db"))
    install.add_argument("--brief-path", default=str(config.TELEGRAM_DIGEST_BRIEF_PATH))
    install.add_argument("--python-bin", default=sys.executable)
    install.add_argument("--chat-id", type=int, required=True)
    install.add_argument("--user-id", type=int, required=True)
    install.add_argument("--message-thread-id", type=int, default=None)
    install.add_argument("--daily-time", default="08:00")
    install.add_argument("--timezone-name", default="Europe/Moscow")
    install.add_argument("--collect-interval-minutes", type=int, default=config.TELEGRAM_DIGEST_COLLECT_INTERVAL_MINUTES)
    install.add_argument("--model", default=config.DEFAULT_MODEL)
    install.add_argument("--provider-cli", default="claude")
    install.add_argument("--resume-arg", default=None)
    install.set_defaults(func=_cmd_install)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
