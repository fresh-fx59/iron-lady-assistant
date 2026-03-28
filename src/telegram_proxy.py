from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import asdict, dataclass
from datetime import timezone
from pathlib import Path
from typing import Any

from aiohttp import web

from . import config
from .telegram_proxy_crypto import (
    TelegramProxyCredentials,
    decrypt_credentials,
    load_decryption_key,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProxyChannelRecord:
    entity_id: int
    title: str
    username: str | None
    linked_chat_id: int | None
    linked_chat_title: str | None
    linked_chat_username: str | None


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


def _message_payload(message: Any, entity_username: str | None) -> dict[str, Any]:
    replies = None
    reply_info = getattr(message, "replies", None)
    if reply_info is not None:
        replies = getattr(reply_info, "replies", None)
    posted_at = getattr(message, "date", None)
    return {
        "message_id": int(message.id),
        "posted_at": (
            posted_at.astimezone(timezone.utc).isoformat()
            if posted_at is not None
            else None
        ),
        "sender_id": getattr(message, "sender_id", None),
        "views": getattr(message, "views", None),
        "forwards": getattr(message, "forwards", None),
        "replies": replies,
        "link": f"https://t.me/{entity_username}/{message.id}" if entity_username else None,
        "text": (getattr(message, "message", None) or "").strip(),
        "raw_json": _json_safe(message.to_dict()),
    }


class TelegramProxy:
    def __init__(self) -> None:
        self._client = None
        self._channel_cls = None
        self._get_full_channel_request = None
        self._create_channel_request = None
        self._invite_to_channel_request = None
        self._export_chat_invite_request = None
        self._entity_cache: dict[tuple[str, int], Any] = {}
        self._lock = asyncio.Lock()
        self._allowed_channel_ids = set(config.TELEGRAM_PROXY_ALLOWED_CHANNEL_IDS)
        self._allowed_chat_ids = set(config.TELEGRAM_PROXY_ALLOWED_CHAT_IDS)

    async def start(self) -> None:
        creds = self._load_credentials()
        try:
            from telethon import TelegramClient
            from telethon.sessions import StringSession
            from telethon.tl.functions.channels import GetFullChannelRequest
            from telethon.tl.functions.channels import CreateChannelRequest, InviteToChannelRequest
            from telethon.tl.functions.messages import ExportChatInviteRequest
            from telethon.tl.types import Channel
        except Exception as exc:  # pragma: no cover - dependency failure
            raise RuntimeError(f"Telethon import failed: {exc}") from exc

        session: object
        if creds.session_string:
            session = StringSession(creds.session_string)
        else:
            session_path = creds.session_path or str(config.TELEGRAM_PROXY_SESSION_PATH)
            Path(session_path).parent.mkdir(parents=True, exist_ok=True)
            session = session_path

        self._client = TelegramClient(session, creds.api_id, creds.api_hash)
        self._channel_cls = Channel
        self._get_full_channel_request = GetFullChannelRequest
        self._create_channel_request = CreateChannelRequest
        self._invite_to_channel_request = InviteToChannelRequest
        self._export_chat_invite_request = ExportChatInviteRequest
        await self._client.connect()
        if not await self._client.is_user_authorized():
            raise RuntimeError("Telegram proxy user session is not authorized.")

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.disconnect()

    def _load_credentials(self) -> TelegramProxyCredentials:
        key = load_decryption_key()
        return decrypt_credentials(config.TELEGRAM_PROXY_ENCRYPTED_CREDENTIALS, key)

    def _require_client(self):
        if self._client is None:
            raise RuntimeError("Telegram proxy client is not started.")
        return self._client

    async def list_channels(self, *, limit: int, lookup: str | None = None) -> list[ProxyChannelRecord]:
        client = self._require_client()
        lookup_value = lookup.strip().lower() if lookup else None
        async with self._lock:
            entity_by_id: dict[int, Any] = {}
            channels: list[Any] = []
            dialog_limit = None if lookup_value is None else limit
            async for dialog in client.iter_dialogs(limit=dialog_limit):
                entity = dialog.entity
                if not isinstance(entity, self._channel_cls):
                    continue
                entity_by_id[int(entity.id)] = entity
                if getattr(entity, "broadcast", False):
                    if lookup_value:
                        entity_id = str(int(entity.id))
                        username = (getattr(entity, "username", None) or "").strip().lower()
                        title = (getattr(entity, "title", None) or "").strip().lower()
                        if lookup_value not in {entity_id, username, title}:
                            continue
                    channels.append(entity)
                    if lookup_value:
                        break
                    if len(channels) >= limit:
                        break

            records: list[ProxyChannelRecord] = []
            for entity in channels:
                linked_chat_id = None
                linked_chat_title = None
                linked_chat_username = None
                try:
                    full = await client(self._get_full_channel_request(entity))
                    linked_chat_id = getattr(full.full_chat, "linked_chat_id", None)
                    if linked_chat_id:
                        linked_entity = entity_by_id.get(int(linked_chat_id))
                        if linked_entity is None:
                            linked_entity = await client.get_entity(int(linked_chat_id))
                        linked_chat_title = getattr(linked_entity, "title", None)
                        linked_chat_username = getattr(linked_entity, "username", None)
                        self._entity_cache[("linked_chat", int(linked_chat_id))] = linked_entity
                except Exception:
                    logger.debug(
                        "Could not resolve linked chat for channel=%s",
                        getattr(entity, "id", None),
                        exc_info=True,
                    )

                self._entity_cache[("channel", int(entity.id))] = entity
                records.append(
                    ProxyChannelRecord(
                        entity_id=int(entity.id),
                        title=(getattr(entity, "title", None) or "Unnamed channel").strip(),
                        username=getattr(entity, "username", None),
                        linked_chat_id=int(linked_chat_id) if linked_chat_id else None,
                        linked_chat_title=linked_chat_title.strip() if linked_chat_title else None,
                        linked_chat_username=linked_chat_username,
                    )
                )
            return records

    async def read_messages(
        self,
        *,
        kind: str,
        entity_id: int,
        min_id: int,
        limit: int,
        recent_first: bool = False,
    ) -> list[dict[str, Any]]:
        client = self._require_client()
        entity = await self._resolve_entity(kind=kind, entity_id=entity_id)
        username = getattr(entity, "username", None)
        items: list[dict[str, Any]] = []
        iter_kwargs: dict[str, Any] = {
            "limit": limit,
            "reverse": not recent_first,
        }
        if min_id > 0:
            iter_kwargs["min_id"] = min_id
        async with self._lock:
            async for message in client.iter_messages(entity, **iter_kwargs):
                payload = _message_payload(message, username)
                if payload["text"]:
                    items.append(payload)
        return items

    async def _resolve_entity(self, *, kind: str, entity_id: int):
        self._authorize_entity(kind=kind, entity_id=entity_id)
        cache_key = (kind, entity_id)
        cached = self._entity_cache.get(cache_key)
        if cached is not None:
            return cached
        client = self._require_client()
        try:
            async with self._lock:
                entity = await client.get_entity(entity_id)
        except ValueError:
            await self._prime_entity_cache_from_dialogs()
            cached = self._entity_cache.get(cache_key)
            if cached is not None:
                return cached
            raise web.HTTPNotFound(text="Entity is not available in the current Telegram dialogs.") from None
        self._entity_cache[cache_key] = entity
        return entity

    async def create_group(self, *, title: str, members: list[str]) -> dict[str, Any]:
        client = self._require_client()
        if not title.strip():
            raise web.HTTPBadRequest(text="Missing title.")
        invited: list[str] = []
        failed_invites: list[dict[str, str]] = []
        async with self._lock:
            created = await client(
                self._create_channel_request(
                    title=title.strip(),
                    about="",
                    megagroup=True,
                )
            )
            if not created.chats:
                raise RuntimeError("Telegram did not return created chat.")
            channel = created.chats[0]

            invite_link = None
            try:
                invite = await client(self._export_chat_invite_request(peer=channel))
                invite_link = getattr(invite, "link", None)
            except Exception:
                invite_link = None

            for raw_member in members:
                member = raw_member.strip()
                if not member:
                    continue
                try:
                    entity = await client.get_input_entity(member)
                    await client(self._invite_to_channel_request(channel=channel, users=[entity]))
                    invited.append(member)
                except Exception as exc:
                    failed_invites.append({"member": member, "error": str(exc)})

        return {
            "chat_id": int(channel.id),
            "title": title.strip(),
            "invite_link": invite_link,
            "invited": invited,
            "failed_invites": failed_invites,
        }

    async def _prime_entity_cache_from_dialogs(self) -> None:
        client = self._require_client()
        async with self._lock:
            async for dialog in client.iter_dialogs(limit=None):
                entity = dialog.entity
                entity_id = getattr(entity, "id", None)
                if entity_id is None:
                    continue
                entity_id = int(entity_id)
                if isinstance(entity, self._channel_cls):
                    cache_kind = "channel" if getattr(entity, "broadcast", False) else "linked_chat"
                    self._entity_cache[(cache_kind, entity_id)] = entity
                    if cache_kind == "linked_chat":
                        self._entity_cache[("chat", entity_id)] = entity
                else:
                    self._entity_cache[("chat", entity_id)] = entity

    def _authorize_entity(self, *, kind: str, entity_id: int) -> None:
        if kind == "channel":
            if self._allowed_channel_ids and entity_id not in self._allowed_channel_ids:
                raise web.HTTPForbidden(text="Channel is not allowlisted.")
            return
        if kind in {"linked_chat", "chat"}:
            if self._allowed_chat_ids and entity_id not in self._allowed_chat_ids:
                raise web.HTTPForbidden(text="Chat is not allowlisted.")
            if not self._allowed_chat_ids and kind == "chat":
                raise web.HTTPForbidden(text="Direct chat access is disabled.")
            return
        raise web.HTTPBadRequest(text="Unsupported entity kind.")


def _check_auth(request: web.Request) -> None:
    expected = config.TELEGRAM_PROXY_API_KEY
    if not expected:
        raise web.HTTPInternalServerError(text="TELEGRAM_PROXY_API_KEY is not configured.")
    provided = request.headers.get("Authorization", "").strip()
    if provided != f"Bearer {expected}":
        raise web.HTTPUnauthorized(text="Invalid proxy token.")


async def _health(request: web.Request) -> web.Response:
    proxy: TelegramProxy = request.app["proxy"]
    status = "ok" if proxy._client is not None else "starting"
    return web.json_response({"status": status})


async def _list_channels(request: web.Request) -> web.Response:
    _check_auth(request)
    proxy: TelegramProxy = request.app["proxy"]
    limit = max(1, min(500, int(request.query.get("limit", "200"))))
    lookup = request.query.get("lookup", "").strip() or None
    records = await proxy.list_channels(limit=limit, lookup=lookup)
    return web.json_response({"channels": [asdict(record) for record in records]})


async def _read_messages(request: web.Request) -> web.Response:
    _check_auth(request)
    proxy: TelegramProxy = request.app["proxy"]
    kind = request.match_info["kind"]
    entity_id = int(request.match_info["entity_id"])
    min_id = max(0, int(request.query.get("min_id", "0")))
    limit = max(1, min(500, int(request.query.get("limit", "200"))))
    recent_first = request.query.get("recent_first", "0").strip().lower() in {"1", "true", "yes", "on"}
    messages = await proxy.read_messages(
        kind=kind,
        entity_id=entity_id,
        min_id=min_id,
        limit=limit,
        recent_first=recent_first,
    )
    return web.json_response({"messages": messages})


async def _create_group(request: web.Request) -> web.Response:
    _check_auth(request)
    proxy: TelegramProxy = request.app["proxy"]
    try:
        payload = await request.json()
    except Exception:
        raise web.HTTPBadRequest(text="Invalid JSON body.") from None

    title = str(payload.get("title", "")).strip()
    members_raw = payload.get("members", [])
    if isinstance(members_raw, str):
        members = [item.strip() for item in members_raw.split(",") if item.strip()]
    elif isinstance(members_raw, list):
        members = [str(item).strip() for item in members_raw if str(item).strip()]
    else:
        members = []

    try:
        result = await proxy.create_group(title=title, members=members)
    except web.HTTPException:
        raise
    except Exception as exc:
        raise web.HTTPBadGateway(text=f"Create group failed: {exc}") from exc
    return web.json_response({"ok": True, "result": result})


async def _startup(app: web.Application) -> None:
    proxy = TelegramProxy()
    await proxy.start()
    app["proxy"] = proxy


async def _cleanup(app: web.Application) -> None:
    proxy: TelegramProxy = app["proxy"]
    await proxy.stop()


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/health", _health)
    app.router.add_get("/v1/channels", _list_channels)
    app.router.add_get("/v1/messages/{kind}/{entity_id}", _read_messages)
    app.router.add_post("/v1/telegram/createGroup", _create_group)
    app.on_startup.append(_startup)
    app.on_cleanup.append(_cleanup)
    return app


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    web.run_app(
        create_app(),
        host=config.TELEGRAM_PROXY_BIND_HOST,
        port=config.TELEGRAM_PROXY_BIND_PORT,
    )


if __name__ == "__main__":
    main()
