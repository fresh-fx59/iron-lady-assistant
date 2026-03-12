#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio

from telethon import TelegramClient
from telethon.sessions import StringSession


def _prompt(label: str) -> str:
    return input(f"{label}: ").strip()


async def _run(api_id: int, api_hash: str, phone: str) -> None:
    client = TelegramClient(StringSession(), api_id, api_hash)
    try:
        await client.start(phone=lambda: phone)
        print("\nStringSession:\n")
        print(client.session.save())
    finally:
        await client.disconnect()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create a Telethon StringSession for the Telegram read proxy."
    )
    parser.add_argument("--api-id", type=int, help="Telegram API ID from my.telegram.org")
    parser.add_argument("--api-hash", help="Telegram API hash from my.telegram.org")
    parser.add_argument(
        "--phone",
        help="Phone number in international format, for example +79991234567",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    api_id = args.api_id or int(_prompt("api_id"))
    api_hash = args.api_hash or _prompt("api_hash")
    phone = args.phone or _prompt("phone (+79991234567)")
    asyncio.run(_run(api_id, api_hash, phone))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
