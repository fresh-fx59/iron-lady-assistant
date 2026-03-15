from __future__ import annotations

import base64
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Any

from aiohttp import ClientSession, ClientTimeout


@dataclass(frozen=True)
class GmailApiError(Exception):
    status: int
    reason: str
    message: str
    retryable: bool


class GmailApiClient:
    def __init__(self, *, timeout_seconds: float = 15.0) -> None:
        self._timeout = ClientTimeout(total=timeout_seconds)

    @staticmethod
    def _build_raw_message(*, to: list[str], subject: str, body_text: str) -> str:
        message = EmailMessage()
        message["To"] = ", ".join(to)
        message["Subject"] = subject
        message.set_content(body_text)
        raw_bytes = message.as_bytes()
        return base64.urlsafe_b64encode(raw_bytes).decode("utf-8").rstrip("=")

    @staticmethod
    def _decode_body(payload: dict[str, Any] | None) -> str:
        if not isinstance(payload, dict):
            return ""
        body = payload.get("body")
        if isinstance(body, dict) and isinstance(body.get("data"), str):
            try:
                encoded = str(body["data"])
                padding = "=" * (-len(encoded) % 4)
                return base64.urlsafe_b64decode((encoded + padding).encode("utf-8")).decode("utf-8", "replace")
            except Exception:
                return ""
        parts = payload.get("parts")
        if isinstance(parts, list):
            for part in parts:
                text = GmailApiClient._decode_body(part if isinstance(part, dict) else None)
                if text:
                    return text
        return ""

    @staticmethod
    def _header_value(payload: dict[str, Any], name: str) -> str:
        headers = payload.get("headers")
        if not isinstance(headers, list):
            return ""
        target = name.lower()
        for item in headers:
            if not isinstance(item, dict):
                continue
            if str(item.get("name", "")).lower() == target:
                return str(item.get("value", ""))
        return ""

    @staticmethod
    def _raise_error(*, status: int, data: dict[str, Any] | None) -> None:
        payload = data if isinstance(data, dict) else {}
        error = payload.get("error", {}) if isinstance(payload, dict) else {}
        reason = "gmail_error"
        details = error.get("errors") if isinstance(error, dict) else None
        if isinstance(details, list) and details:
            first = details[0]
            if isinstance(first, dict) and first.get("reason"):
                reason = str(first.get("reason"))
        retryable = status in {429, 500, 502, 503, 504}
        raise GmailApiError(
            status=status,
            reason=reason,
            message=str(error.get("message") or "Gmail API request failed"),
            retryable=retryable,
        )

    async def _request_json(
        self,
        *,
        method: str,
        url: str,
        access_token: str,
        json_payload: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        async with ClientSession(timeout=self._timeout) as session:
            async with session.request(
                method,
                url,
                headers=headers,
                json=json_payload,
                params=params,
            ) as resp:
                data: dict[str, Any] = await resp.json(content_type=None)
                if resp.status >= 400:
                    self._raise_error(status=resp.status, data=data)
                return data

    async def send_message(
        self,
        *,
        access_token: str,
        to: list[str],
        subject: str,
        body_text: str,
    ) -> str:
        payload = {"raw": self._build_raw_message(to=to, subject=subject, body_text=body_text)}
        data = await self._request_json(
            method="POST",
            url="https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
            access_token=access_token,
            json_payload=payload,
        )
        message_id = str(data.get("id", "")).strip()
        if not message_id:
            raise GmailApiError(
                status=502,
                reason="invalid_response",
                message="Gmail API response missing message id",
                retryable=True,
            )
        return message_id

    async def refresh_access_token(
        self,
        *,
        refresh_token: str,
        client_id: str,
        client_secret: str,
    ) -> tuple[str, str | None]:
        payload = {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        }
        async with ClientSession(timeout=self._timeout) as session:
            async with session.post(
                "https://oauth2.googleapis.com/token",
                data=payload,
            ) as resp:
                data: dict[str, Any] = await resp.json(content_type=None)
                if resp.status >= 400:
                    self._raise_error(status=resp.status, data=data)
                token = str(data.get("access_token", "")).strip()
                if not token:
                    raise GmailApiError(
                        status=502,
                        reason="invalid_response",
                        message="OAuth refresh response missing access_token",
                        retryable=True,
                    )
                expires_in = data.get("expires_in")
                expires_at = None
                if isinstance(expires_in, int):
                    from datetime import UTC, datetime, timedelta

                    expires_at = (datetime.now(UTC) + timedelta(seconds=expires_in)).isoformat()
                return token, expires_at

    async def search_messages(
        self,
        *,
        access_token: str,
        query: str,
        max_results: int,
    ) -> list[dict[str, Any]]:
        data = await self._request_json(
            method="GET",
            url="https://gmail.googleapis.com/gmail/v1/users/me/messages",
            access_token=access_token,
            params={
                "q": query,
                "maxResults": str(max(1, min(max_results, 100))),
            },
        )
        items = data.get("messages")
        if not isinstance(items, list):
            return []
        results: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            msg_id = str(item.get("id", "")).strip()
            if not msg_id:
                continue
            msg = await self.read_message(access_token=access_token, message_id=msg_id)
            results.append(msg)
        return results

    async def read_message(self, *, access_token: str, message_id: str) -> dict[str, Any]:
        data = await self._request_json(
            method="GET",
            url=f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}",
            access_token=access_token,
            params={"format": "full"},
        )
        payload = data.get("payload") if isinstance(data.get("payload"), dict) else {}
        return {
            "message_id": str(data.get("id", "")),
            "thread_id": str(data.get("threadId", "")),
            "subject": self._header_value(payload, "Subject"),
            "from": self._header_value(payload, "From"),
            "snippet": str(data.get("snippet", "")),
            "internal_date": str(data.get("internalDate", "")),
            "body_text": self._decode_body(payload),
            "body_html": None,
            "labels": [str(item) for item in (data.get("labelIds") or [])],
        }

    async def trash_message(self, *, access_token: str, message_id: str) -> None:
        await self._request_json(
            method="POST",
            url=f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}/trash",
            access_token=access_token,
        )

    async def delete_message(self, *, access_token: str, message_id: str) -> None:
        await self._request_json(
            method="DELETE",
            url=f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}",
            access_token=access_token,
        )
