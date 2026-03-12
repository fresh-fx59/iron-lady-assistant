#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen


REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = REPO_ROOT / ".env"


def _load_env_lines() -> list[str]:
    if not ENV_PATH.exists():
        raise SystemExit(f"Missing env file: {ENV_PATH}")
    return ENV_PATH.read_text(encoding="utf-8").splitlines()


def _read_env(lines: list[str]) -> dict[str, str]:
    data: dict[str, str] = {}
    for line in lines:
        if "=" not in line or line.lstrip().startswith("#"):
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip()
    return data


def _set_env_value(lines: list[str], key: str, value: str) -> list[str]:
    target = f"{key}="
    updated: list[str] = []
    replaced = False
    for line in lines:
        if line.startswith(target):
            updated.append(f"{key}={value}")
            replaced = True
        else:
            updated.append(line)
    if not replaced:
        updated.append(f"{key}={value}")
    return updated


def _csv_to_int_set(raw: str | None) -> set[int]:
    if not raw:
        return set()
    return {int(item.strip()) for item in raw.split(",") if item.strip()}


def _int_set_to_csv(values: set[int]) -> str:
    return ",".join(str(item) for item in sorted(values))


def _extract_channel_lookup(value: str) -> str:
    raw = value.strip()
    if not raw:
        raise SystemExit("Channel value cannot be empty.")
    if raw.startswith("https://") or raw.startswith("http://"):
        parsed = urlparse(raw)
        parts = [part for part in parsed.path.split("/") if part]
        if not parts:
            raise SystemExit("Channel URL does not contain a username or id.")
        return parts[-1].lstrip("@")
    return raw.lstrip("@")


def _proxy_request(base_url: str, api_key: str, path: str, params: dict[str, str] | None = None) -> Any:
    url = f"{base_url.rstrip('/')}{path}"
    if params:
        url = f"{url}?{urlencode(params)}"
    request = Request(url, headers={"Authorization": f"Bearer {api_key}"})
    try:
        with urlopen(request, timeout=60) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"Proxy request failed: HTTP {exc.code}: {body}") from exc


def _find_channel(channels: list[dict[str, Any]], lookup: str) -> dict[str, Any]:
    if lookup.isdigit():
        wanted_id = int(lookup)
        for channel in channels:
            if int(channel["entity_id"]) == wanted_id:
                return channel
    normalized = lookup.lower()
    for channel in channels:
        if (channel.get("username") or "").lower() == normalized:
            return channel
    for channel in channels:
        if channel.get("title", "").strip().lower() == normalized:
            return channel
    available = ", ".join(
        f"@{item['username']}" if item.get("username") else str(item["entity_id"])
        for item in channels[:20]
    )
    raise SystemExit(f"Channel '{lookup}' was not found via proxy. Visible examples: {available}")


def _restart_proxy() -> None:
    try:
        subprocess.run(["sudo", "systemctl", "restart", "telegram-proxy.service"], check=True)
    except subprocess.CalledProcessError as exc:
        raise SystemExit(
            "Automatic proxy restart failed. Run this manually, then rerun the helper or collect step:\n"
            "  sudo systemctl restart telegram-proxy.service"
        ) from exc


def _collect_once() -> dict[str, Any]:
    result = subprocess.run(
        [str(REPO_ROOT / "venv/bin/python"), "-m", "src.telegram_digest_tool", "collect"],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    return json.loads(result.stdout)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Allowlist a Telegram channel for digest collection, restart the proxy, and collect once."
    )
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument(
        "--channel",
        help="Channel username, numeric entity id, or channel URL such as https://t.me/ai_engineer_helper",
    )
    target.add_argument(
        "--all-visible",
        action="store_true",
        help="Clear channel/chat allowlists so the digest includes all subscribed channels and linked discussion chats.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="How many channels to fetch from the proxy when resolving the target.",
    )
    parser.add_argument(
        "--no-collect",
        action="store_true",
        help="Only update allowlists and restart the proxy; skip the immediate collect run.",
    )
    parser.add_argument(
        "--no-restart",
        action="store_true",
        help="Only update .env allowlists; do not restart telegram-proxy.service automatically.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    lines = _load_env_lines()
    env = _read_env(lines)
    base_url = env.get("TELEGRAM_PROXY_BASE_URL", "").strip()
    api_key = env.get("TELEGRAM_PROXY_API_KEY", "").strip()
    if not base_url or not api_key:
        raise SystemExit("TELEGRAM_PROXY_BASE_URL or TELEGRAM_PROXY_API_KEY is missing in .env")

    channel: dict[str, Any] | None = None
    if args.all_visible:
        lines = _set_env_value(lines, "TELEGRAM_PROXY_ALLOWED_CHANNEL_IDS", "")
        lines = _set_env_value(lines, "TELEGRAM_PROXY_ALLOWED_CHAT_IDS", "")
    else:
        lookup = _extract_channel_lookup(args.channel)
        payload = _proxy_request(
            base_url,
            api_key,
            "/v1/channels",
            {"limit": str(args.limit), "lookup": lookup},
        )
        channel = _find_channel(list(payload.get("channels", [])), lookup)

        allowed_channels = _csv_to_int_set(env.get("TELEGRAM_PROXY_ALLOWED_CHANNEL_IDS"))
        allowed_chats = _csv_to_int_set(env.get("TELEGRAM_PROXY_ALLOWED_CHAT_IDS"))
        allowed_channels.add(int(channel["entity_id"]))
        linked_chat_id = channel.get("linked_chat_id")
        if linked_chat_id:
            allowed_chats.add(int(linked_chat_id))

        lines = _set_env_value(lines, "TELEGRAM_PROXY_ALLOWED_CHANNEL_IDS", _int_set_to_csv(allowed_channels))
        lines = _set_env_value(lines, "TELEGRAM_PROXY_ALLOWED_CHAT_IDS", _int_set_to_csv(allowed_chats))
    ENV_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")

    health: dict[str, Any] | None = None
    if not args.no_restart:
        _restart_proxy()
        health = _proxy_request(base_url, api_key, "/health")
    result: dict[str, Any] | None = None
    if not args.no_collect and not args.no_restart:
        result = _collect_once()

    summary = {
        "mode": "all_visible" if args.all_visible else "single_channel",
        "channel": (
            None
            if channel is None
            else {
                "entity_id": channel["entity_id"],
                "title": channel["title"],
                "username": channel.get("username"),
                "linked_chat_id": channel.get("linked_chat_id"),
            }
        ),
        "proxy_health": health,
        "collector_result": result,
    }
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
