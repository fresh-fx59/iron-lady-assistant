from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from aiohttp import ClientSession, ClientTimeout

from . import config


@dataclass(frozen=True)
class GatewayClientError(Exception):
    status: int
    code: str
    message: str
    retryable: bool


class GmailGatewayClient:
    def __init__(
        self,
        *,
        base_url: str,
        timeout_seconds: float = 15.0,
        service_token: str | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = ClientTimeout(total=timeout_seconds)
        self._service_token = service_token

    @classmethod
    def from_config(cls) -> "GmailGatewayClient":
        return cls(
            base_url=config.GMAIL_GATEWAY_BASE_URL,
            timeout_seconds=config.GMAIL_GATEWAY_TIMEOUT_SECONDS,
        )

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_payload: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any] | None:
        req_headers = dict(headers or {})
        if self._service_token:
            req_headers["Authorization"] = f"Bearer {self._service_token}"
        async with ClientSession(timeout=self._timeout) as session:
            async with session.request(
                method,
                f"{self._base_url}{path}",
                json=json_payload,
                headers=req_headers,
            ) as resp:
                if resp.status == 204:
                    return None
                raw = await resp.read()
                if not raw:
                    if resp.status >= 400:
                        raise GatewayClientError(
                            status=resp.status,
                            code="internal_error",
                            message="Gateway request failed with empty error body",
                            retryable=False,
                        )
                    return None
                payload = json.loads(raw.decode("utf-8"))
                if resp.status >= 400:
                    err = payload.get("error", {})
                    raise GatewayClientError(
                        status=resp.status,
                        code=str(err.get("code", "internal_error")),
                        message=str(err.get("message", "Gateway request failed")),
                        retryable=bool(err.get("retryable", False)),
                    )
                return payload

    async def get_account(self, *, account_id: str) -> dict[str, Any]:
        payload = await self._request("GET", f"/v1/accounts/{account_id}")
        return payload or {}

    async def connect_account(self, *, account_id: str, redirect_url: str) -> dict[str, Any]:
        payload = await self._request(
            "POST",
            f"/v1/accounts/{account_id}/connect",
            json_payload={"redirect_url": redirect_url},
        )
        return payload or {}

    async def oauth_callback(
        self,
        *,
        session_id: str,
        gmail_email: str,
        access_token: str,
        refresh_token: str,
        scopes: str,
        expires_at: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "session_id": session_id,
            "gmail_email": gmail_email,
            "access_token": access_token,
            "refresh_token": refresh_token,
            "scopes": scopes,
        }
        if expires_at is not None:
            body["expires_at"] = expires_at
        payload = await self._request("POST", "/v1/oauth/callback", json_payload=body)
        return payload or {}

    async def disconnect_account(self, *, account_id: str) -> None:
        await self._request("POST", f"/v1/accounts/{account_id}/disconnect")

    async def send_message(
        self,
        *,
        account_id: str,
        to: list[str],
        subject: str,
        body_text: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        payload = await self._request(
            "POST",
            "/v1/messages/send",
            json_payload={
                "account_id": account_id,
                "to": to,
                "subject": subject,
                "body_text": body_text,
            },
            headers={"Idempotency-Key": idempotency_key},
        )
        return payload or {}

    async def search_messages(
        self,
        *,
        account_id: str,
        query: str,
        page_size: int = 20,
    ) -> dict[str, Any]:
        payload = await self._request(
            "POST",
            "/v1/messages/search",
            json_payload={
                "account_id": account_id,
                "query": query,
                "page_size": page_size,
            },
        )
        return payload or {}

    async def read_message(self, *, account_id: str, message_id: str) -> dict[str, Any]:
        payload = await self._request(
            "GET",
            f"/v1/messages/{message_id}",
            headers={"X-Account-Id": account_id},
        )
        return payload or {}

    async def trash_message(self, *, account_id: str, message_id: str) -> None:
        await self._request(
            "POST",
            f"/v1/messages/{message_id}/trash",
            headers={"X-Account-Id": account_id},
        )

    async def delete_message(self, *, account_id: str, message_id: str) -> None:
        await self._request(
            "DELETE",
            f"/v1/messages/{message_id}",
            headers={"X-Account-Id": account_id},
        )
