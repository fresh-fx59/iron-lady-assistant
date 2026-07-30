from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import aiohttp

from . import config


@dataclass(frozen=True)
class ProxyChannel:
    entity_id: int
    title: str
    username: str | None
    linked_chat_id: int | None
    linked_chat_title: str | None
    linked_chat_username: str | None


class TelegramProxyClient:
    def __init__(
        self,
        *,
        base_url: str | None = None,
        api_key: str | None = None,
        timeout_seconds: float | None = None,
    ) -> None:
        self._base_url = (base_url or config.TELEGRAM_PROXY_BASE_URL).rstrip("/")
        self._api_key = api_key or config.TELEGRAM_PROXY_API_KEY
        self._timeout = timeout_seconds or config.TELEGRAM_PROXY_REQUEST_TIMEOUT_SECONDS

    async def list_channels(self, *, limit: int) -> list[ProxyChannel]:
        payload = await self._get("/v1/channels", params={"limit": str(limit)})
        return [ProxyChannel(**item) for item in payload.get("channels", [])]

    async def read_messages(
        self,
        *,
        kind: str,
        entity_id: int,
        min_id: int,
        limit: int,
        recent_first: bool = False,
    ) -> list[dict[str, Any]]:
        page = await self.read_messages_page(
            kind=kind,
            entity_id=entity_id,
            min_id=min_id,
            limit=limit,
            recent_first=recent_first,
        )
        return page["messages"]

    async def read_messages_page(
        self,
        *,
        kind: str,
        entity_id: int,
        min_id: int,
        limit: int,
        recent_first: bool = False,
    ) -> dict[str, Any]:
        """``{messages, seen, max_seen_id}`` — see ``TelegramProxy.read_messages_page``.

        ``max_seen_id`` covers text-less messages the proxy filtered out, so a
        watermark built from it never stalls on a sticker burst. A proxy that predates
        the field omits it; the fallback then derives what it can from the items,
        which is exactly the pre-fix behaviour and no worse.
        """
        payload = await self._get(
            f"/v1/messages/{kind}/{entity_id}",
            params={
                "min_id": str(min_id),
                "limit": str(limit),
                "recent_first": "1" if recent_first else "0",
            },
        )
        messages = list(payload.get("messages", []))
        max_seen_id = int(payload.get("max_seen_id") or 0)
        for item in messages:
            max_seen_id = max(max_seen_id, int(item["message_id"]))
        return {
            "messages": messages,
            "seen": int(payload.get("seen") or len(messages)),
            "max_seen_id": max_seen_id,
        }

    async def _get(self, path: str, *, params: dict[str, str]) -> dict[str, Any]:
        if not self._base_url:
            raise RuntimeError("TELEGRAM_PROXY_BASE_URL is not configured.")
        if not self._api_key:
            raise RuntimeError("TELEGRAM_PROXY_API_KEY is not configured.")
        headers = {"Authorization": f"Bearer {self._api_key}"}
        timeout = aiohttp.ClientTimeout(total=self._timeout)
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.get(f"{self._base_url}{path}", params=params) as response:
                response.raise_for_status()
                return await response.json()
