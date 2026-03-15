from __future__ import annotations

import argparse
import asyncio
import json
import secrets
from typing import Any

from .gmail_gateway_client import GmailGatewayClient


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m src.gmail_gateway_cli")
    parser.add_argument("--base-url", default=None, help="Override gateway base URL")
    sub = parser.add_subparsers(dest="command", required=True)

    account_get = sub.add_parser("account-get")
    account_get.add_argument("--account", required=True)

    account_connect = sub.add_parser("account-connect")
    account_connect.add_argument("--account", required=True)
    account_connect.add_argument("--redirect-url", required=True)

    account_disconnect = sub.add_parser("account-disconnect")
    account_disconnect.add_argument("--account", required=True)

    oauth_callback = sub.add_parser("oauth-callback")
    oauth_callback.add_argument("--session-id", required=True)
    oauth_callback.add_argument("--gmail-email", required=True)
    oauth_callback.add_argument("--access-token", required=True)
    oauth_callback.add_argument("--refresh-token", required=True)
    oauth_callback.add_argument("--scopes", required=True)

    send = sub.add_parser("send")
    send.add_argument("--account", required=True)
    send.add_argument("--to", required=True, help="Comma-separated recipient list")
    send.add_argument("--subject", required=True)
    send.add_argument("--body", required=True)
    send.add_argument("--idempotency-key", default=None)

    search = sub.add_parser("search")
    search.add_argument("--account", required=True)
    search.add_argument("--query", required=True)
    search.add_argument("--page-size", type=int, default=20)

    read = sub.add_parser("read")
    read.add_argument("--account", required=True)
    read.add_argument("--message-id", required=True)

    trash = sub.add_parser("trash")
    trash.add_argument("--account", required=True)
    trash.add_argument("--message-id", required=True)

    delete = sub.add_parser("delete")
    delete.add_argument("--account", required=True)
    delete.add_argument("--message-id", required=True)

    return parser


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    client = GmailGatewayClient.from_config()
    if args.base_url:
        client = GmailGatewayClient(base_url=args.base_url)

    if args.command == "account-get":
        return await client.get_account(account_id=args.account)
    if args.command == "account-connect":
        return await client.connect_account(account_id=args.account, redirect_url=args.redirect_url)
    if args.command == "account-disconnect":
        await client.disconnect_account(account_id=args.account)
        return {"ok": True}
    if args.command == "oauth-callback":
        return await client.oauth_callback(
            session_id=args.session_id,
            gmail_email=args.gmail_email,
            access_token=args.access_token,
            refresh_token=args.refresh_token,
            scopes=args.scopes,
        )
    if args.command == "send":
        idem = args.idempotency_key or f"idem-{secrets.token_hex(8)}"
        return await client.send_message(
            account_id=args.account,
            to=[item.strip() for item in args.to.split(",") if item.strip()],
            subject=args.subject,
            body_text=args.body,
            idempotency_key=idem,
        )
    if args.command == "search":
        return await client.search_messages(
            account_id=args.account,
            query=args.query,
            page_size=args.page_size,
        )
    if args.command == "read":
        return await client.read_message(account_id=args.account, message_id=args.message_id)
    if args.command == "trash":
        await client.trash_message(account_id=args.account, message_id=args.message_id)
        return {"ok": True}
    if args.command == "delete":
        await client.delete_message(account_id=args.account, message_id=args.message_id)
        return {"ok": True}

    raise RuntimeError(f"Unsupported command: {args.command}")


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    payload = asyncio.run(_run(args))
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
