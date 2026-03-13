from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from aiohttp import ClientSession, web

from . import config
from .features.gmail_bootstrap_state import GmailBootstrapSession, GmailBootstrapStateStore
from .gmail_gcp_bootstrap import bootstrap_gcp_project


def _default_state_path() -> Path:
    return config.MEMORY_DIR / "gmail_bootstrap_sessions.json"


def _normalize_base_url(base_url: str) -> str:
    return base_url.rstrip("/")


def build_session_urls(*, base_url: str, session_id: str) -> dict[str, str]:
    root = _normalize_base_url(base_url)
    return {
        "session_page_url": f"{root}/gmail/bootstrap/session/{session_id}",
        "status_url": f"{root}/gmail/bootstrap/api/session/{session_id}",
    }


def _session_payload(base_url: str, session: GmailBootstrapSession) -> dict[str, Any]:
    payload = asdict(session)
    payload["urls"] = build_session_urls(base_url=base_url, session_id=session.session_id)
    if _oauth_ready():
        payload["google_auth_url"] = build_google_auth_url(
            session=session,
            client_id=config.GMAIL_BOOTSTRAP_GOOGLE_CLIENT_ID,
        )
    return payload


def _oauth_ready() -> bool:
    return bool(
        config.GMAIL_BOOTSTRAP_GOOGLE_CLIENT_ID and config.GMAIL_BOOTSTRAP_GOOGLE_CLIENT_SECRET
    )


def build_google_auth_url(*, session: GmailBootstrapSession, client_id: str) -> str:
    query = urlencode(
        {
            "client_id": client_id,
            "redirect_uri": f"{session.callback_base_url}/gmail/bootstrap/google/callback",
            "response_type": "code",
            "scope": " ".join(config.GMAIL_BOOTSTRAP_GOOGLE_SCOPES),
            "access_type": "offline",
            "include_granted_scopes": "true",
            "state": session.session_id,
            "prompt": "consent",
        }
    )
    return f"https://accounts.google.com/o/oauth2/v2/auth?{query}"


def _html_page(title: str, body: str) -> str:
    return (
        "<!doctype html>"
        "<html><head><meta charset='utf-8'>"
        f"<title>{title}</title>"
        "<style>"
        "body{font-family:Georgia,serif;max-width:760px;margin:40px auto;padding:0 16px;"
        "background:#f7f3ea;color:#1f1b16;}"
        "h1,h2{font-family:'Trebuchet MS',sans-serif;}"
        "label{display:block;margin-top:12px;font-weight:bold;}"
        "input{width:100%;padding:10px;margin-top:4px;box-sizing:border-box;}"
        "button{margin-top:16px;padding:10px 16px;background:#2b5f45;color:#fff;border:0;cursor:pointer;}"
        "code{background:#efe7d6;padding:2px 4px;}"
        ".card{background:#fffdf7;border:1px solid #d8ccb8;padding:18px;border-radius:8px;}"
        "</style></head><body>"
        f"{body}</body></html>"
    )


def _render_session_html(base_url: str, session: GmailBootstrapSession) -> str:
    urls = build_session_urls(base_url=base_url, session_id=session.session_id)
    lines = [
        "<div class='card'>",
        f"<h1>Gmail Connect Session</h1>",
        f"<p><strong>Session:</strong> <code>{session.session_id}</code></p>",
        f"<p><strong>Phase:</strong> <code>{session.phase}</code></p>",
        f"<p><strong>Project:</strong> <code>{session.project_id}</code> ({session.project_name})</p>",
        f"<p><strong>Redirect URI:</strong> <code>{session.redirect_uri}</code></p>",
        f"<p><strong>Status API:</strong> <a href='{urls['status_url']}'>{urls['status_url']}</a></p>",
    ]
    if session.manual_console_url:
        lines.append(
            f"<p><strong>Manual checkpoint:</strong> <a href='{session.manual_console_url}'>Google Cloud Console</a></p>"
        )
    if session.failure_reason:
        lines.append(f"<p><strong>Failure:</strong> {session.failure_reason}</p>")
    lines.append(
        "<p>Current slice stores browser-first bootstrap state. Google callback is now wired when bootstrap OAuth config is present; project automation attaches next.</p>"
    )
    if _oauth_ready():
        auth_url = build_google_auth_url(
            session=session,
            client_id=config.GMAIL_BOOTSTRAP_GOOGLE_CLIENT_ID,
        )
        lines.append(
            f"<p><strong>Next action:</strong> <a href='{auth_url}'>Continue with Google login</a></p>"
        )
    lines.append("</div>")
    return _html_page("Gmail Connect Session", "".join(lines))


async def _health(_: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def _landing(request: web.Request) -> web.Response:
    base_url = _normalize_base_url(f"{request.scheme}://{request.host}")
    body = (
        "<div class='card'>"
        "<h1>Connect Gmail</h1>"
        "<p>This browser-first wizard creates a local bootstrap session on your own host.</p>"
        f"<form method='post' action='{base_url}/gmail/bootstrap/start'>"
        "<label>Project ID<input name='project_id' required value='ila-gmail-demo'></label>"
        "<label>Project Name<input name='project_name' required value='Iron Lady Assistant Gmail'></label>"
        f"<label>Callback Base URL<input name='callback_base_url' required value='{base_url}'></label>"
        "<label>OAuth Client Name<input name='oauth_client_name' required value='Iron Lady Assistant Gmail'></label>"
        "<button type='submit'>Start Gmail Setup</button>"
        "</form></div>"
    )
    return web.Response(text=_html_page("Connect Gmail", body), content_type="text/html")


async def _start_session(request: web.Request) -> web.Response:
    store: GmailBootstrapStateStore = request.app["gmail_bootstrap_store"]
    if request.content_type == "application/json":
        payload = await request.json()
    else:
        payload = dict(await request.post())
    project_id = str(payload.get("project_id", "")).strip()
    project_name = str(payload.get("project_name", "")).strip() or "Iron Lady Assistant Gmail"
    callback_base_url = _normalize_base_url(
        str(payload.get("callback_base_url", "")).strip() or f"{request.scheme}://{request.host}"
    )
    oauth_client_name = str(payload.get("oauth_client_name", "")).strip() or "Iron Lady Assistant Gmail"
    if not project_id:
        raise web.HTTPBadRequest(text="project_id is required")

    session = store.start_session(
        project_id=project_id,
        project_name=project_name,
        redirect_uri=f"{callback_base_url}/gmail/oauth/callback",
        callback_base_url=callback_base_url,
        oauth_client_name=oauth_client_name,
    )
    if request.content_type == "application/json":
        return web.json_response(_session_payload(callback_base_url, session), status=201)
    if _oauth_ready():
        raise web.HTTPFound(
            location=build_google_auth_url(
                session=session,
                client_id=config.GMAIL_BOOTSTRAP_GOOGLE_CLIENT_ID,
            )
        )
    raise web.HTTPFound(location=f"/gmail/bootstrap/session/{session.session_id}")


async def _exchange_code_for_token(*, code: str, redirect_uri: str) -> dict[str, Any]:
    async with ClientSession() as session:
        async with session.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code": code,
                "client_id": config.GMAIL_BOOTSTRAP_GOOGLE_CLIENT_ID,
                "client_secret": config.GMAIL_BOOTSTRAP_GOOGLE_CLIENT_SECRET,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            },
        ) as response:
            payload = await response.json()
            if response.status >= 400:
                raise web.HTTPBadRequest(text=json.dumps(payload, ensure_ascii=False))
            return payload


async def _fetch_userinfo(*, access_token: str) -> dict[str, Any]:
    async with ClientSession() as session:
        async with session.get(
            "https://openidconnect.googleapis.com/v1/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
        ) as response:
            payload = await response.json()
            if response.status >= 400:
                raise web.HTTPBadRequest(text=json.dumps(payload, ensure_ascii=False))
            return payload


async def _google_callback(request: web.Request) -> web.Response:
    store: GmailBootstrapStateStore = request.app["gmail_bootstrap_store"]
    if not _oauth_ready():
        raise web.HTTPServiceUnavailable(text="Bootstrap Google OAuth client is not configured.")
    session_id = request.query.get("state", "").strip()
    if not session_id:
        raise web.HTTPBadRequest(text="Missing OAuth state.")
    session = store.get(session_id)
    if session is None:
        raise web.HTTPNotFound(text="Unknown bootstrap session.")
    error = request.query.get("error", "").strip()
    if error:
        store.record_failed(session_id=session_id, reason=f"google_auth_error:{error}")
        raise web.HTTPFound(location=f"/gmail/bootstrap/session/{session_id}")
    code = request.query.get("code", "").strip()
    if not code:
        raise web.HTTPBadRequest(text="Missing authorization code.")

    redirect_uri = f"{session.callback_base_url}/gmail/bootstrap/google/callback"
    token_payload = await _exchange_code_for_token(code=code, redirect_uri=redirect_uri)
    access_token = str(token_payload.get("access_token", "")).strip()
    if not access_token:
        store.record_failed(session_id=session_id, reason="missing_access_token")
        raise web.HTTPFound(location=f"/gmail/bootstrap/session/{session_id}")
    userinfo = await _fetch_userinfo(access_token=access_token)
    email = str(userinfo.get("email", "")).strip() or "unknown"
    store.record_cloud_auth(session_id=session_id, account_email=email)
    try:
        bootstrap_result = await bootstrap_gcp_project(
            access_token=access_token,
            project_id=session.project_id,
            project_name=session.project_name,
        )
    except Exception as exc:
        store.record_failed(session_id=session_id, reason=f"gcp_bootstrap_failed:{exc}")
    else:
        store.record_project_bootstrap(
            session_id=session_id,
            project_number=bootstrap_result["project_number"],
            manual_console_url=bootstrap_result["manual_console_url"],
        )
    raise web.HTTPFound(location=f"/gmail/bootstrap/session/{session_id}")


async def _session_status(request: web.Request) -> web.Response:
    store: GmailBootstrapStateStore = request.app["gmail_bootstrap_store"]
    session_id = request.match_info["session_id"]
    session = store.get(session_id)
    if session is None:
        raise web.HTTPNotFound(text="Unknown bootstrap session.")
    base_url = _normalize_base_url(f"{request.scheme}://{request.host}")
    return web.json_response(_session_payload(base_url, session))


async def _session_page(request: web.Request) -> web.Response:
    store: GmailBootstrapStateStore = request.app["gmail_bootstrap_store"]
    session_id = request.match_info["session_id"]
    session = store.get(session_id)
    if session is None:
        raise web.HTTPNotFound(text="Unknown bootstrap session.")
    base_url = _normalize_base_url(f"{request.scheme}://{request.host}")
    return web.Response(
        text=_render_session_html(base_url, session),
        content_type="text/html",
    )


def create_app(*, state_path: Path | None = None) -> web.Application:
    app = web.Application()
    app["gmail_bootstrap_store"] = GmailBootstrapStateStore(state_path or _default_state_path())
    app.add_routes(
        [
            web.get("/health", _health),
            web.get("/gmail/connect", _landing),
            web.post("/gmail/bootstrap/start", _start_session),
            web.get("/gmail/bootstrap/google/callback", _google_callback),
            web.get("/gmail/bootstrap/session/{session_id}", _session_page),
            web.get("/gmail/bootstrap/api/session/{session_id}", _session_status),
        ]
    )
    return app


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m src.gmail_bootstrap_web")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8781)
    parser.add_argument("--state-path", default=str(_default_state_path()))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    app = create_app(state_path=Path(args.state_path))
    web.run_app(app, host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
