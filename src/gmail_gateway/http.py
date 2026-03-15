from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import logging
from pathlib import Path
from typing import Any

from aiohttp import web

from .. import config
from .auth_store import AuthStore
from .gmail_api import GmailApiClient, GmailApiError
from .message_store import MessageStore
from .models import ErrorCode, ErrorEnvelope
from .observability import GatewayObservability
from .sync_store import SyncStore

logger = logging.getLogger(__name__)

AUTH_STORE_KEY: web.AppKey[AuthStore] = web.AppKey("auth_store", AuthStore)
MESSAGE_STORE_KEY: web.AppKey[MessageStore] = web.AppKey("message_store", MessageStore)
SYNC_STORE_KEY: web.AppKey[SyncStore] = web.AppKey("sync_store", SyncStore)
GMAIL_API_KEY: web.AppKey[GmailApiClient] = web.AppKey("gmail_api", GmailApiClient)
OBS_KEY: web.AppKey[GatewayObservability] = web.AppKey("observability", GatewayObservability)


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


async def _metrics(request: web.Request) -> web.Response:
    return web.json_response({"counters": request.app[OBS_KEY].snapshot()})


@web.middleware
async def _observability_middleware(request: web.Request, handler):
    try:
        response = await handler(request)
    except web.HTTPException as exc:
        bucket = f"{exc.status // 100}xx"
        route = request.path
        request.app[OBS_KEY].inc(f"{request.method} {route} {bucket}")
        logger.info(
            "gmail_gateway_request method=%s path=%s status=%s",
            request.method,
            route,
            exc.status,
        )
        raise
    except Exception:
        route = request.path
        request.app[OBS_KEY].inc(f"{request.method} {route} 5xx")
        logger.exception(
            "gmail_gateway_request method=%s path=%s status=500",
            request.method,
            route,
        )
        raise
    bucket = f"{response.status // 100}xx"
    route = request.path
    request.app[OBS_KEY].inc(f"{request.method} {route} {bucket}")
    logger.info(
        "gmail_gateway_request method=%s path=%s status=%s",
        request.method,
        route,
        response.status,
    )
    return response


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


def _gmail_error_response(
    *,
    request: web.Request,
    account_id: str,
    exc: GmailApiError,
    operation: str,
) -> web.Response:
    auth_store = request.app[AUTH_STORE_KEY]
    if exc.reason in {"invalid_grant", "authError"}:
        auth_store.mark_invalid_grant(account_id=account_id)
        return _error_response(
            status=401,
            code=ErrorCode.REAUTH_REQUIRED,
            error_class="oauth.invalid_grant",
            message=exc.message,
            retryable=False,
            details={"reason": exc.reason},
        )
    if exc.status == 429 or exc.reason in {"userRateLimitExceeded", "rateLimitExceeded"}:
        return _error_response(
            status=429,
            code=ErrorCode.QUOTA_LIMITED,
            error_class="gmail.quota_limited",
            message=exc.message,
            retryable=True,
            details={"reason": exc.reason},
        )
    return _error_response(
        status=502,
        code=ErrorCode.RETRYABLE if exc.retryable else ErrorCode.INTERNAL_ERROR,
        error_class=f"gmail.{operation}_failed",
        message=exc.message,
        retryable=exc.retryable,
        details={"reason": exc.reason},
    )


async def _execute_with_token_refresh(
    *,
    request: web.Request,
    account_id: str,
    operation: str,
    invoke,
):
    auth_store = request.app[AUTH_STORE_KEY]
    token_bundle = auth_store.get_active_token_bundle(account_id=account_id)
    if not token_bundle or not token_bundle.access_token:
        return None, _error_response(
            status=401,
            code=ErrorCode.REAUTH_REQUIRED,
            error_class="oauth.missing_token",
            message=f"Account '{account_id}' is not connected or token is missing",
            retryable=False,
        )
    gmail_api = request.app[GMAIL_API_KEY]
    try:
        return await invoke(token_bundle.access_token), None
    except GmailApiError as exc:
        should_refresh = (
            exc.status == 401
            and exc.reason not in {"invalid_grant", "authError"}
            and token_bundle.refresh_token
            and config.GMAIL_GATEWAY_GOOGLE_CLIENT_ID
            and config.GMAIL_GATEWAY_GOOGLE_CLIENT_SECRET
        )
        if should_refresh:
            try:
                new_access_token, expires_at = await gmail_api.refresh_access_token(
                    refresh_token=str(token_bundle.refresh_token),
                    client_id=config.GMAIL_GATEWAY_GOOGLE_CLIENT_ID,
                    client_secret=config.GMAIL_GATEWAY_GOOGLE_CLIENT_SECRET,
                )
                auth_store.rotate_access_token(
                    token_id=token_bundle.token_id,
                    access_token=new_access_token,
                    expires_at=expires_at,
                )
                return await invoke(new_access_token), None
            except GmailApiError as refresh_exc:
                return None, _gmail_error_response(
                    request=request,
                    account_id=account_id,
                    exc=refresh_exc,
                    operation=f"{operation}_refresh",
                )
        return None, _gmail_error_response(
            request=request,
            account_id=account_id,
            exc=exc,
            operation=operation,
        )


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
    provider_message_id, error_response = await _execute_with_token_refresh(
        request=request,
        account_id=account_id,
        operation="send",
        invoke=lambda access_token: request.app[GMAIL_API_KEY].send_message(
            access_token=access_token,
            to=list(payload["to"]),
            subject=str(payload["subject"]),
            body_text=str(payload["body_text"]),
        ),
    )
    if error_response is not None:
        return error_response
    receipt = store.record_send_receipt(
        account_id=account_id,
        idempotency_key=idem_key,
        request_hash=request_hash,
        status="sent",
        provider_message_id=provider_message_id,
        sent_at=datetime.now(UTC).isoformat(),
        status_code=202,
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
    messages, error_response = await _execute_with_token_refresh(
        request=request,
        account_id=account_id,
        operation="search",
        invoke=lambda access_token: request.app[GMAIL_API_KEY].search_messages(
            access_token=access_token,
            query=query,
            max_results=page_size,
        ),
    )
    if error_response is not None:
        return error_response
    return web.json_response(
        {
            "messages": [
                {
                    "message_id": str(m.get("message_id", "")),
                    "thread_id": str(m.get("thread_id", "")),
                    "subject": str(m.get("subject", "")),
                    "from": str(m.get("from", "")),
                    "snippet": str(m.get("snippet", "")),
                    "internal_date": str(m.get("internal_date", "")),
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
    message, error_response = await _execute_with_token_refresh(
        request=request,
        account_id=account_id,
        operation="read",
        invoke=lambda access_token: request.app[GMAIL_API_KEY].read_message(
            access_token=access_token,
            message_id=message_id,
        ),
    )
    if error_response is not None:
        # Preserve 404 message semantics from original read path.
        return error_response
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
            "message_id": str(message.get("message_id", "")),
            "thread_id": str(message.get("thread_id", "")),
            "subject": str(message.get("subject", "")),
            "from": str(message.get("from", "")),
            "snippet": str(message.get("snippet", "")),
            "internal_date": str(message.get("internal_date", "")),
            "body_text": str(message.get("body_text", "")),
            "body_html": message.get("body_html"),
            "labels": [str(item) for item in (message.get("labels") or [])],
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
    _, error_response = await _execute_with_token_refresh(
        request=request,
        account_id=account_id,
        operation="delete" if hard_delete else "trash",
        invoke=(
            lambda access_token: request.app[GMAIL_API_KEY].delete_message(
                access_token=access_token,
                message_id=message_id,
            )
            if hard_delete
            else request.app[GMAIL_API_KEY].trash_message(
                access_token=access_token,
                message_id=message_id,
            )
        ),
    )
    if error_response is not None:
        return error_response
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
    app = web.Application(middlewares=[_observability_middleware])
    app[AUTH_STORE_KEY] = AuthStore(db_path)
    app[MESSAGE_STORE_KEY] = MessageStore(db_path)
    app[SYNC_STORE_KEY] = SyncStore(db_path)
    app[GMAIL_API_KEY] = GmailApiClient()
    app[OBS_KEY] = GatewayObservability()
    app.add_routes(
        [
            web.get("/health", _health),
            web.get("/internal/metrics", _metrics),
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
