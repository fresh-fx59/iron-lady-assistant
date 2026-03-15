from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from aiohttp import web

from .auth_store import AuthStore
from .message_store import MessageStore
from .models import ErrorCode, ErrorEnvelope
from .sync_store import SyncStore

AUTH_STORE_KEY: web.AppKey[AuthStore] = web.AppKey("auth_store", AuthStore)
MESSAGE_STORE_KEY: web.AppKey[MessageStore] = web.AppKey("message_store", MessageStore)
SYNC_STORE_KEY: web.AppKey[SyncStore] = web.AppKey("sync_store", SyncStore)


def _error_response(
    *,
    status: int,
    code: ErrorCode,
    error_class: str,
    message: str,
    retryable: bool,
    details: dict[str, Any] | None = None,
) -> web.Response:
    payload = ErrorEnvelope(
        code=code,
        error_class=error_class,
        message=message,
        retryable=retryable,
        details=details or {},
    ).to_dict()
    return web.json_response(payload, status=status)


async def _health(_: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def _get_account(request: web.Request) -> web.Response:
    account_id = request.match_info["account_id"]
    store = request.app[AUTH_STORE_KEY]
    account = store.get_account_auth_state(account_id=account_id)
    if account is None:
        return _error_response(
            status=404,
            code=ErrorCode.NOT_FOUND,
            error_class="account.not_found",
            message=f"Account '{account_id}' was not found",
            retryable=False,
        )
    return web.json_response(_account_payload(account))


async def _not_implemented(_: web.Request) -> web.Response:
    return _error_response(
        status=501,
        code=ErrorCode.INTERNAL_ERROR,
        error_class="gateway.not_implemented",
        message="Endpoint is defined in contract but not implemented yet",
        retryable=False,
    )


def _json_body(payload: dict[str, Any], *, field: str, expected: type) -> Any:
    value = payload.get(field)
    if not isinstance(value, expected):
        raise ValueError(f"{field} must be {expected.__name__}")
    return value


def _account_payload(account: Any) -> dict[str, Any]:
    return {
        "account_id": account.account_id,
        "status": account.status,
        "auth_state": account.auth_state,
        "email": account.gmail_email,
        "updated_at": None,
    }


async def _connect_account(request: web.Request) -> web.Response:
    account_id = request.match_info["account_id"]
    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return _error_response(
            status=400,
            code=ErrorCode.INVALID_REQUEST,
            error_class="request.invalid_json",
            message="Request body must be valid JSON",
            retryable=False,
        )
    try:
        redirect_url = str(_json_body(payload, field="redirect_url", expected=str)).strip()
    except ValueError as exc:
        return _error_response(
            status=400,
            code=ErrorCode.INVALID_REQUEST,
            error_class="request.validation_error",
            message=str(exc),
            retryable=False,
        )
    if not redirect_url:
        return _error_response(
            status=400,
            code=ErrorCode.INVALID_REQUEST,
            error_class="request.validation_error",
            message="redirect_url must not be empty",
            retryable=False,
        )
    store = request.app[AUTH_STORE_KEY]
    session = store.start_connect_session(account_id=account_id, redirect_url=redirect_url)
    callback_base = f"{request.scheme}://{request.host}"
    connect_url = f"{callback_base}/v1/oauth/callback?session_id={session.session_id}"
    return web.json_response(
        {
            "connect_url": connect_url,
            "expires_at": session.expires_at,
        },
        status=202,
    )


async def _oauth_callback(request: web.Request) -> web.Response:
    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return _error_response(
            status=400,
            code=ErrorCode.INVALID_REQUEST,
            error_class="request.invalid_json",
            message="Request body must be valid JSON",
            retryable=False,
        )
    required = ("session_id", "gmail_email", "access_token", "refresh_token", "scopes")
    try:
        values = {key: str(_json_body(payload, field=key, expected=str)).strip() for key in required}
    except ValueError as exc:
        return _error_response(
            status=400,
            code=ErrorCode.INVALID_REQUEST,
            error_class="request.validation_error",
            message=str(exc),
            retryable=False,
        )
    if any(not values[key] for key in required):
        return _error_response(
            status=400,
            code=ErrorCode.INVALID_REQUEST,
            error_class="request.validation_error",
            message="session_id, gmail_email, access_token, refresh_token, and scopes are required",
            retryable=False,
        )
    store = request.app[AUTH_STORE_KEY]
    account = store.complete_connect_session(
        session_id=values["session_id"],
        gmail_email=values["gmail_email"],
        access_token=values["access_token"],
        refresh_token=values["refresh_token"],
        scopes=values["scopes"],
        expires_at=str(payload.get("expires_at")) if payload.get("expires_at") is not None else None,
    )
    if account is None:
        return _error_response(
            status=404,
            code=ErrorCode.NOT_FOUND,
            error_class="oauth.session_not_found",
            message="OAuth session was not found",
            retryable=False,
        )
    return web.json_response(_account_payload(account), status=200)


async def _disconnect_account(request: web.Request) -> web.Response:
    account_id = request.match_info["account_id"]
    store = request.app[AUTH_STORE_KEY]
    disconnected = store.disconnect_account(account_id=account_id)
    if not disconnected:
        return _error_response(
            status=404,
            code=ErrorCode.NOT_FOUND,
            error_class="account.not_found",
            message=f"Account '{account_id}' was not found",
            retryable=False,
        )
    return web.Response(status=204)


def _account_exists(request: web.Request, account_id: str) -> bool:
    return request.app[AUTH_STORE_KEY].get_account_auth_state(account_id=account_id) is not None


async def _send_message(request: web.Request) -> web.Response:
    idem_key = request.headers.get("Idempotency-Key", "").strip()
    if not idem_key:
        return _error_response(
            status=400,
            code=ErrorCode.INVALID_REQUEST,
            error_class="request.idempotency_key_required",
            message="Idempotency-Key header is required",
            retryable=False,
        )
    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return _error_response(
            status=400,
            code=ErrorCode.INVALID_REQUEST,
            error_class="request.invalid_json",
            message="Request body must be valid JSON",
            retryable=False,
        )
    try:
        account_id = str(_json_body(payload, field="account_id", expected=str)).strip()
        _json_body(payload, field="to", expected=list)
        _json_body(payload, field="subject", expected=str)
        _json_body(payload, field="body_text", expected=str)
    except ValueError as exc:
        return _error_response(
            status=400,
            code=ErrorCode.INVALID_REQUEST,
            error_class="request.validation_error",
            message=str(exc),
            retryable=False,
        )
    store = request.app[MESSAGE_STORE_KEY]
    request_hash = store.request_hash(payload)
    existing = store.get_idempotency_record(
        account_id=account_id,
        operation="send_message",
        idempotency_key=idem_key,
    )
    if existing is not None:
        if existing["request_hash"] != request_hash:
            return _error_response(
                status=409,
                code=ErrorCode.INVALID_REQUEST,
                error_class="idempotency.conflict",
                message="Idempotency key was already used with a different request payload",
                retryable=False,
            )
        response_json = json.loads(existing["response_json"] or "{}")
        status_code = int(existing["status_code"] or 202)
        return web.json_response(response_json, status=status_code)
    receipt = store.record_send_receipt(
        account_id=account_id,
        idempotency_key=idem_key,
        request_hash=request_hash,
    )
    return web.json_response(
        {
            "receipt_id": receipt.receipt_id,
            "account_id": receipt.account_id,
            "status": receipt.status,
            "provider_message_id": receipt.provider_message_id,
            "queued_at": receipt.queued_at,
            "sent_at": receipt.sent_at,
        },
        status=202,
    )


async def _search_messages(request: web.Request) -> web.Response:
    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return _error_response(
            status=400,
            code=ErrorCode.INVALID_REQUEST,
            error_class="request.invalid_json",
            message="Request body must be valid JSON",
            retryable=False,
        )
    try:
        account_id = str(_json_body(payload, field="account_id", expected=str)).strip()
        query = str(_json_body(payload, field="query", expected=str))
    except ValueError as exc:
        return _error_response(
            status=400,
            code=ErrorCode.INVALID_REQUEST,
            error_class="request.validation_error",
            message=str(exc),
            retryable=False,
        )
    page_size = int(payload.get("page_size", 20))
    page_size = max(1, min(page_size, 100))
    store = request.app[MESSAGE_STORE_KEY]
    messages = store.search_messages(account_id=account_id, query=query, page_size=page_size)
    return web.json_response(
        {
            "messages": [
                {
                    "message_id": m.message_id,
                    "thread_id": m.thread_id,
                    "subject": m.subject,
                    "from": m.from_email,
                    "snippet": m.snippet,
                    "internal_date": m.internal_ts,
                }
                for m in messages
            ]
        }
    )


def _header_account_id(request: web.Request) -> str | None:
    account_id = request.headers.get("X-Account-Id", "").strip()
    return account_id or None


async def _read_message(request: web.Request) -> web.Response:
    account_id = _header_account_id(request)
    if not account_id:
        return _error_response(
            status=400,
            code=ErrorCode.INVALID_REQUEST,
            error_class="request.account_id_required",
            message="X-Account-Id header is required",
            retryable=False,
        )
    message_id = request.match_info["message_id"]
    store = request.app[MESSAGE_STORE_KEY]
    message = store.get_message(account_id=account_id, message_id=message_id)
    if message is None:
        return _error_response(
            status=404,
            code=ErrorCode.NOT_FOUND,
            error_class="message.not_found",
            message=f"Message '{message_id}' was not found",
            retryable=False,
        )
    return web.json_response(
        {
            "message_id": message.message_id,
            "thread_id": message.thread_id,
            "subject": message.subject,
            "from": message.from_email,
            "snippet": message.snippet,
            "internal_date": message.internal_ts,
            "body_text": message.body_text,
            "body_html": message.body_html,
            "labels": message.labels,
        }
    )


async def _trash_or_delete_message(request: web.Request, *, hard_delete: bool) -> web.Response:
    account_id = _header_account_id(request)
    if not account_id:
        return _error_response(
            status=400,
            code=ErrorCode.INVALID_REQUEST,
            error_class="request.account_id_required",
            message="X-Account-Id header is required",
            retryable=False,
        )
    message_id = request.match_info["message_id"]
    label = "DELETED" if hard_delete else "TRASH"
    store = request.app[MESSAGE_STORE_KEY]
    updated = store.add_label(account_id=account_id, message_id=message_id, label=label)
    if not updated:
        return _error_response(
            status=404,
            code=ErrorCode.NOT_FOUND,
            error_class="message.not_found",
            message=f"Message '{message_id}' was not found",
            retryable=False,
        )
    return web.Response(status=202)


async def _trash_message(request: web.Request) -> web.Response:
    return await _trash_or_delete_message(request, hard_delete=False)


async def _delete_message(request: web.Request) -> web.Response:
    return await _trash_or_delete_message(request, hard_delete=True)


async def _bootstrap_sync(request: web.Request) -> web.Response:
    account_id = request.match_info["account_id"]
    if not _account_exists(request, account_id):
        return _error_response(
            status=404,
            code=ErrorCode.NOT_FOUND,
            error_class="account.not_found",
            message=f"Account '{account_id}' was not found",
            retryable=False,
        )
    cursor = request.app[SYNC_STORE_KEY].bootstrap(account_id=account_id)
    return web.json_response(
        {
            "account_id": cursor.account_id,
            "sync_state": cursor.sync_state,
            "last_history_id": cursor.last_history_id,
        },
        status=202,
    )


async def _delta_sync(request: web.Request) -> web.Response:
    account_id = request.match_info["account_id"]
    if not _account_exists(request, account_id):
        return _error_response(
            status=404,
            code=ErrorCode.NOT_FOUND,
            error_class="account.not_found",
            message=f"Account '{account_id}' was not found",
            retryable=False,
        )
    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return _error_response(
            status=400,
            code=ErrorCode.INVALID_REQUEST,
            error_class="request.invalid_json",
            message="Request body must be valid JSON",
            retryable=False,
        )
    try:
        history_id = str(_json_body(payload, field="history_id", expected=str)).strip()
    except ValueError as exc:
        return _error_response(
            status=400,
            code=ErrorCode.INVALID_REQUEST,
            error_class="request.validation_error",
            message=str(exc),
            retryable=False,
        )
    cursor = request.app[SYNC_STORE_KEY].delta(account_id=account_id, history_id=history_id)
    return web.json_response(
        {
            "account_id": cursor.account_id,
            "sync_state": cursor.sync_state,
            "last_history_id": cursor.last_history_id,
            "last_successful_sync_at": cursor.last_successful_sync_at,
        },
        status=202,
    )


async def _renew_watch(request: web.Request) -> web.Response:
    account_id = request.match_info["account_id"]
    if not _account_exists(request, account_id):
        return _error_response(
            status=404,
            code=ErrorCode.NOT_FOUND,
            error_class="account.not_found",
            message=f"Account '{account_id}' was not found",
            retryable=False,
        )
    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return _error_response(
            status=400,
            code=ErrorCode.INVALID_REQUEST,
            error_class="request.invalid_json",
            message="Request body must be valid JSON",
            retryable=False,
        )
    try:
        watch_expiration_ts = str(_json_body(payload, field="watch_expiration_ts", expected=str)).strip()
    except ValueError as exc:
        return _error_response(
            status=400,
            code=ErrorCode.INVALID_REQUEST,
            error_class="request.validation_error",
            message=str(exc),
            retryable=False,
        )
    if not watch_expiration_ts:
        return _error_response(
            status=400,
            code=ErrorCode.INVALID_REQUEST,
            error_class="request.validation_error",
            message="watch_expiration_ts is required",
            retryable=False,
        )
    cursor = request.app[SYNC_STORE_KEY].renew_watch(
        account_id=account_id,
        watch_expiration_ts=watch_expiration_ts,
    )
    return web.json_response(
        {
            "account_id": cursor.account_id,
            "sync_state": cursor.sync_state,
            "watch_expiration_ts": cursor.watch_expiration_ts,
        },
        status=202,
    )


def create_app(*, db_path: Path) -> web.Application:
    app = web.Application()
    app[AUTH_STORE_KEY] = AuthStore(db_path)
    app[MESSAGE_STORE_KEY] = MessageStore(db_path)
    app[SYNC_STORE_KEY] = SyncStore(db_path)
    app.add_routes(
        [
            web.get("/health", _health),
            web.get("/v1/accounts/{account_id}", _get_account),
            web.post("/v1/accounts/{account_id}/connect", _connect_account),
            web.post("/v1/accounts/{account_id}/disconnect", _disconnect_account),
            web.post("/v1/oauth/callback", _oauth_callback),
            web.post("/v1/messages/send", _send_message),
            web.post("/v1/messages/search", _search_messages),
            web.get("/v1/messages/{message_id}", _read_message),
            web.post("/v1/messages/{message_id}/trash", _trash_message),
            web.delete("/v1/messages/{message_id}", _delete_message),
            web.post("/v1/sync/{account_id}/bootstrap", _bootstrap_sync),
            web.post("/v1/sync/{account_id}/delta", _delta_sync),
            web.post("/v1/watch/{account_id}/renew", _renew_watch),
        ]
    )
    return app


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m src.gmail_gateway.http")
    parser.add_argument("--db-path", type=Path, default=Path("memory/gmail_gateway.db"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8791)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    app = create_app(db_path=args.db_path)
    web.run_app(app, host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
