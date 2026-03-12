from __future__ import annotations

import argparse
import json
import sys

from cryptography.fernet import Fernet


def _cmd_generate_key(_: argparse.Namespace) -> int:
    print(Fernet.generate_key().decode("utf-8"))
    return 0


def _cmd_encrypt(args: argparse.Namespace) -> int:
    payload = {
        "api_id": args.api_id,
        "api_hash": args.api_hash,
    }
    if args.session_string:
        payload["session_string"] = args.session_string
    if args.session_path:
        payload["session_path"] = args.session_path
    token = Fernet(args.key.encode("utf-8")).encrypt(
        json.dumps(payload, separators=(",", ":")).encode("utf-8")
    )
    print(token.decode("utf-8"))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m src.telegram_proxy_crypto_tool")
    sub = parser.add_subparsers(dest="command", required=True)

    generate_key = sub.add_parser("generate-key")
    generate_key.set_defaults(func=_cmd_generate_key)

    encrypt = sub.add_parser("encrypt")
    encrypt.add_argument("--key", required=True)
    encrypt.add_argument("--api-id", type=int, required=True)
    encrypt.add_argument("--api-hash", required=True)
    encrypt.add_argument("--session-string", default="")
    encrypt.add_argument("--session-path", default="")
    encrypt.set_defaults(func=_cmd_encrypt)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
