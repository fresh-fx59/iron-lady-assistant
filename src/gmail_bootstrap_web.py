from __future__ import annotations

import argparse
import html
import json
import os
import re
import secrets
import shutil
import subprocess
from dataclasses import asdict
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from aiogram import Bot
from aiohttp import ClientSession, web

from . import config
from .features.gmail_bootstrap_state import GmailBootstrapSession, GmailBootstrapStateStore
from .gmail_gcp_bootstrap import bootstrap_gcp_project
from .gmail_setup_tool import build_manual_checklist


def _default_state_path() -> Path:
    return config.MEMORY_DIR / "gmail_bootstrap_sessions.json"


def _artifact_root() -> Path:
    return config.MEMORY_DIR / "gmail_bootstrap_artifacts"


def _gog_keyring_password_path() -> Path:
    return config.MEMORY_DIR / "gog_keyring_password.txt"


def _bootstrap_oauth_credentials_path() -> Path:
    return config.MEMORY_DIR / "gmail_bootstrap_google_client.json"


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
    payload["phase_label"] = _phase_label(session)
    payload["phase_message"] = _phase_message(session)
    payload["connected"] = session.phase == "completed"
    oauth_credentials = _bootstrap_oauth_credentials()
    if oauth_credentials:
        client_id, _ = oauth_credentials
        payload["google_auth_url"] = build_google_auth_url(
            session=session,
            client_id=client_id,
        )
    return payload


def _find_gog_binary() -> str | None:
    found = shutil.which("gog")
    if found:
        return found
    fallback = Path.home() / ".local" / "bin" / "gog"
    if fallback.exists():
        return str(fallback)
    return None


def _oauth_ready() -> bool:
    return _bootstrap_oauth_credentials() is not None


def _extract_oauth_client_credentials(payload: dict[str, Any]) -> tuple[str, str]:
    candidate = payload.get("installed") or payload.get("web") or payload
    if not isinstance(candidate, dict):
        raise ValueError("OAuth credentials JSON must contain an object payload.")
    client_id = str(candidate.get("client_id", "")).strip()
    client_secret = str(candidate.get("client_secret", "")).strip()
    if not client_id or not client_secret:
        raise ValueError("OAuth credentials must include both client_id and client_secret.")
    return client_id, client_secret


def _bootstrap_oauth_credentials() -> tuple[str, str] | None:
    if config.GMAIL_BOOTSTRAP_GOOGLE_CLIENT_ID and config.GMAIL_BOOTSTRAP_GOOGLE_CLIENT_SECRET:
        return (
            config.GMAIL_BOOTSTRAP_GOOGLE_CLIENT_ID,
            config.GMAIL_BOOTSTRAP_GOOGLE_CLIENT_SECRET,
        )
    path = _bootstrap_oauth_credentials_path()
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if not isinstance(payload, dict):
        return None
    try:
        return _extract_oauth_client_credentials(payload)
    except ValueError:
        return None


def _save_bootstrap_oauth_credentials(raw_text: str) -> tuple[str, str]:
    payload = _validate_credentials_json(raw_text)
    client_id, client_secret = _extract_oauth_client_credentials(payload)
    target_path = _bootstrap_oauth_credentials_path()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(
        json.dumps(
            {
                "client_id": client_id,
                "client_secret": client_secret,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return client_id, client_secret


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
        "p,li{line-height:1.5;}"
        "label{display:block;margin-top:12px;font-weight:bold;}"
        "input{width:100%;padding:10px;margin-top:4px;box-sizing:border-box;}"
        "button{margin-top:16px;padding:10px 16px;background:#2b5f45;color:#fff;border:0;cursor:pointer;}"
        "code{background:#efe7d6;padding:2px 4px;}"
        ".card{background:#fffdf7;border:1px solid #d8ccb8;padding:18px;border-radius:8px;}"
        ".status{padding:12px 14px;border-radius:8px;margin:16px 0;background:#efe7d6;}"
        ".error{background:#f9dfd8;color:#6c2417;}"
        ".muted{color:#5b544c;}"
        ".checklist{background:#f8f4eb;border:1px solid #d8ccb8;border-radius:8px;padding:16px;margin-top:18px;}"
        ".actions a{display:inline-block;margin-right:10px;margin-top:8px;}"
        "</style></head><body>"
        f"{body}</body></html>"
    )


def _phase_label(session: GmailBootstrapSession) -> str:
    labels = {
        "cloud_auth_pending": "Google sign-in needed",
        "cloud_auth_granted": "Preparing your Google Cloud project",
        "oauth_manual_pending": "Gmail OAuth setup needed",
        "credentials_uploaded": "Finishing Gmail authorization",
        "gmail_auth_pending": "Waiting for Gmail authorization callback",
        "completed": "Gmail connected",
        "failed": "Setup needs attention",
    }
    return labels.get(session.phase, session.phase.replace("_", " ").title())


def _phase_message(session: GmailBootstrapSession) -> str:
    if session.phase == "cloud_auth_pending":
        return "Start Google sign-in so the wizard can prepare Gmail API access on this host."
    if session.phase == "cloud_auth_granted":
        return "Google sign-in succeeded. The wizard is preparing your Google Cloud project."
    if session.phase == "oauth_manual_pending":
        return "Create the OAuth client in Google Cloud Console, then upload client_secret.json to continue Gmail authorization."
    if session.phase == "credentials_uploaded":
        return "Credentials were uploaded. The wizard is starting Gmail account authorization."
    if session.phase == "gmail_auth_pending":
        return "Finish the Gmail authorization page opened by Google, then return here for confirmation."
    if session.phase == "completed":
        return "This bot host now has a stored Gmail API connection ready for Gmail operations."
    if session.phase == "failed":
        return _friendly_failure_message(session.failure_reason, session.failure_guidance)
    return "The Gmail setup session is active."


def _friendly_failure_message(reason: str | None, guidance: str | None = None) -> str:
    if guidance:
        return guidance
    if not reason:
        return "The setup failed. Review the session details and retry the current step."
    if reason.startswith("google_auth_error:"):
        return "Google sign-in was cancelled or denied. Use the Google sign-in button to retry."
    if reason.startswith("gcp_bootstrap_failed:"):
        return (
            "Google sign-in worked, but automatic Google Cloud project setup failed. "
            "Check project id availability, billing, or organization policy, then retry."
        )
    if reason.startswith("gog_remote_start_failed:"):
        return "The Gmail authorization step could not be started. Check the uploaded credentials file and local gog installation."
    if reason.startswith("gmail_auth_error:"):
        return "Google returned an error while authorizing Gmail access. Retry the Gmail authorization step."
    if reason.startswith("gog_remote_finish_failed:"):
        return "The Gmail callback returned, but local token storage failed. Check gog and retry."
    return f"Setup error: {reason}"


def _gcp_failure_guidance(error_text: str) -> str:
    lowered = error_text.lower()
    if "billing" in lowered:
        return (
            "Automatic Google Cloud setup was blocked because billing is not enabled for the project. "
            "Open Google Cloud Console, enable billing for the project, then retry Gmail setup."
        )
    if "permission" in lowered or "forbidden" in lowered or "permission_denied" in lowered:
        return (
            "Automatic Google Cloud setup was blocked by permissions. "
            "Use a Google account that can create projects and enable APIs, then retry."
        )
    if "orgpolicy" in lowered or "organization policy" in lowered or "policy" in lowered:
        return (
            "Automatic Google Cloud setup was blocked by organization policy. "
            "Use an allowed Google Cloud project or relax the policy, then retry."
        )
    if "quota" in lowered or "limit" in lowered:
        return (
            "Automatic Google Cloud setup hit a Google Cloud quota or limit. "
            "Use a different project id or clear the Google Cloud limit, then retry."
        )
    return (
        "Automatic Google Cloud project setup failed. "
        "Check project id availability, billing, and organization policy, then retry."
    )


def _render_markdown_checklist(markdown_text: str) -> str:
    lines = markdown_text.splitlines()
    chunks: list[str] = ["<div class='checklist'><h2>Manual Checklist</h2>"]
    in_ul = False
    in_ol = False

    def close_lists() -> None:
        nonlocal in_ul, in_ol
        if in_ul:
            chunks.append("</ul>")
            in_ul = False
        if in_ol:
            chunks.append("</ol>")
            in_ol = False

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            close_lists()
            continue
        if line.startswith("# "):
            close_lists()
            chunks.append(f"<h3>{html.escape(line[2:])}</h3>")
            continue
        if line.startswith("## "):
            close_lists()
            chunks.append(f"<h3>{html.escape(line[3:])}</h3>")
            continue
        if re.match(r"^\d+\.\s", line):
            if in_ul:
                chunks.append("</ul>")
                in_ul = False
            if not in_ol:
                chunks.append("<ol>")
                in_ol = True
            chunks.append(f"<li>{html.escape(re.sub(r'^\\d+\\.\\s+', '', line))}</li>")
            continue
        if line.startswith("- "):
            if in_ol:
                chunks.append("</ol>")
                in_ol = False
            if not in_ul:
                chunks.append("<ul>")
                in_ul = True
            chunks.append(f"<li>{html.escape(line[2:])}</li>")
            continue
        close_lists()
        chunks.append(f"<p>{html.escape(line)}</p>")

    close_lists()
    chunks.append("</div>")
    return "".join(chunks)


def _manual_checklist_text(session: GmailBootstrapSession) -> str | None:
    if session.manual_checklist_path:
        path = Path(session.manual_checklist_path)
        if path.exists():
            return path.read_text(encoding="utf-8")
    if session.phase not in {"oauth_manual_pending", "credentials_uploaded", "gmail_auth_pending", "completed"}:
        return None
    return build_manual_checklist(
        project_id=session.project_id,
        project_name=session.project_name,
        redirect_uri=session.redirect_uri,
        oauth_client_name=session.oauth_client_name,
    )


def _notification_key(session: GmailBootstrapSession) -> str:
    if session.phase == "failed":
        return f"failed:{session.failure_reason or 'unknown'}"
    return session.phase


async def _send_telegram_update(session: GmailBootstrapSession, text: str) -> None:
    if session.telegram_chat_id is None:
        return
    bot = Bot(token=config.BOT_TOKEN)
    try:
        kwargs: dict[str, Any] = {
            "chat_id": session.telegram_chat_id,
            "text": text,
        }
        if session.telegram_thread_id is not None:
            kwargs["message_thread_id"] = session.telegram_thread_id
        await bot.send_message(**kwargs)
    finally:
        await bot.session.close()


async def _notify_telegram_for_session(
    store: GmailBootstrapStateStore,
    session: GmailBootstrapSession | None,
) -> None:
    if session is None:
        return
    notification_key = _notification_key(session)
    if session.last_telegram_notification_key == notification_key:
        return

    text: str | None = None
    if session.phase == "cloud_auth_granted":
        text = "Gmail setup: Google sign-in complete. Preparing the Google Cloud project now."
    elif session.phase == "oauth_manual_pending":
        text = (
            "Gmail setup: automatic project setup is complete. "
            "Open the setup page, complete the Google Cloud Console step, and upload client_secret.json."
        )
    elif session.phase == "gmail_auth_pending":
        account = session.gmail_account_email or "your Gmail account"
        text = (
            f"Gmail setup: credentials uploaded. Finish the Google authorization page for {account}, "
            "then the bot will confirm when connection is complete."
        )
    elif session.phase == "completed":
        account = session.gmail_account_email or "unknown account"
        text = f"Gmail setup complete: {account} is now connected to the bot."
    elif session.phase == "failed":
        text = f"Gmail setup needs attention: {_friendly_failure_message(session.failure_reason, session.failure_guidance)}"

    if text is None:
        return
    await _send_telegram_update(session, text)
    store.record_telegram_notification(
        session_id=session.session_id,
        notification_key=notification_key,
    )


def _render_session_html(base_url: str, session: GmailBootstrapSession) -> str:
    urls = build_session_urls(base_url=base_url, session_id=session.session_id)
    phase_label = _phase_label(session)
    phase_message = _phase_message(session)
    oauth_credentials = _bootstrap_oauth_credentials()
    lines = [
        "<div class='card'>",
        f"<h1>Gmail Connect Session</h1>",
        f"<div class='status{' error' if session.phase == 'failed' else ''}'><strong>{html.escape(phase_label)}</strong><br>{html.escape(phase_message)}</div>",
        f"<p><strong>Project:</strong> <code>{session.project_id}</code> ({session.project_name})</p>",
        f"<p><strong>Redirect URI:</strong> <code>{session.redirect_uri}</code></p>",
        f"<p class='muted'><strong>Session:</strong> <code>{session.session_id}</code></p>",
        f"<p class='muted'><strong>Status API:</strong> <a href='{urls['status_url']}'>{urls['status_url']}</a></p>",
    ]
    if session.manual_console_url:
        lines.append(
            f"<p><strong>Manual checkpoint:</strong> <a href='{session.manual_console_url}' target='_blank' rel='noopener noreferrer'>Google Cloud Console</a></p>"
        )
    if session.failure_reason:
        lines.append(f"<p><strong>Failure detail:</strong> <code>{html.escape(session.failure_reason)}</code></p>")
    if session.failure_guidance:
        lines.append(f"<p><strong>Recovery hint:</strong> {html.escape(session.failure_guidance)}</p>")
    if session.phase == "completed" and session.gmail_account_email:
        lines.append(f"<p><strong>Connected Gmail:</strong> <code>{session.gmail_account_email}</code></p>")
    if session.connected_at:
        lines.append(f"<p><strong>Connected at:</strong> <code>{session.connected_at}</code></p>")
    if oauth_credentials and session.phase in {"cloud_auth_pending", "failed"}:
        client_id, _ = oauth_credentials
        auth_url = build_google_auth_url(
            session=session,
            client_id=client_id,
        )
        lines.append(
            f"<p class='actions'><strong>Next action:</strong> <a href='{auth_url}'>Continue with Google sign-in</a></p>"
        )
    if not oauth_credentials and session.phase in {"cloud_auth_pending", "failed"}:
        google_callback_uri = f"{session.callback_base_url}/gmail/bootstrap/google/callback"
        lines.extend(
            [
                "<h2>One-time Bootstrap Prerequisite</h2>",
                "<p>This host still needs a Google OAuth client for the setup wizard itself.</p>",
                "<h3>Step-by-step</h3>",
                "<ol>",
                "<li>Open Google Cloud Console and select the same project as your uploaded bootstrap OAuth client.</li>",
                "<li>Go to <strong>OAuth consent screen</strong> and set publishing status to <strong>Testing</strong>.</li>",
                "<li>Add your Gmail account to <strong>Test users</strong> (for example <code>fresh.fx59@gmail.com</code>).</li>",
                "<li>Go to <strong>Credentials</strong> and create an <strong>OAuth client ID</strong> (Web application).</li>",
                f"<li>Add redirect URI <code>{html.escape(google_callback_uri)}</code> exactly.</li>",
                "<li>Download the OAuth client JSON and upload it below.</li>",
                "<li>Retry <strong>Continue with Google sign-in</strong>. If you saw 'Access blocked', wait 1-5 minutes after saving test users and try again.</li>",
                "</ol>",
                "<p><a href='https://console.cloud.google.com/apis/credentials' target='_blank' rel='noopener noreferrer'>Open Google Cloud Console credentials</a></p>",
                f"<form method='post' enctype='multipart/form-data' action='{base_url}/gmail/bootstrap/session/{session.session_id}/bootstrap-credentials/upload'>",
                "<label>Bootstrap OAuth JSON<input type='file' name='bootstrap_credentials_file' accept='.json,application/json' required></label>",
                "<button type='submit'>Save Bootstrap Credentials</button>",
                "</form>",
            ]
        )
    checklist_text = _manual_checklist_text(session)
    if checklist_text:
        lines.append(_render_markdown_checklist(checklist_text))
    show_upload_form = session.phase == "oauth_manual_pending" or (
        session.phase == "failed" and checklist_text is not None
    )
    if show_upload_form:
        lines.extend(
            [
                "<h2>Finish Gmail OAuth</h2>",
                "<p>After you download <code>client_secret.json</code> from Google Cloud Console, upload it here to continue.</p>",
                f"<form method='post' enctype='multipart/form-data' action='{base_url}/gmail/bootstrap/session/{session.session_id}/credentials/upload'>",
                "<label>Gmail account email<input name='gmail_account_email' required placeholder='you@gmail.com'></label>",
                "<label>client_secret.json<input type='file' name='credentials_file' accept='.json,application/json' required></label>",
                "<button type='submit'>Upload Credentials and Continue</button>",
                "</form>",
            ]
        )
    can_retry_gmail_auth = (
        session.phase in {"failed", "gmail_auth_pending", "credentials_uploaded"}
        and bool(session.gmail_account_email)
        and bool(session.credentials_path)
    )
    if can_retry_gmail_auth:
        lines.extend(
            [
                "<h2>Retry Gmail Authorization</h2>",
                "<p>If you changed redirect URIs or fixed OAuth settings, start Google authorization again without uploading files again.</p>",
                f"<form method='post' action='{base_url}/gmail/bootstrap/session/{session.session_id}/gmail-auth/restart'>",
                "<button type='submit'>Retry Gmail Authorization</button>",
                "</form>",
            ]
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
        "<p>This browser-first wizard starts a Gmail-only setup session on your own host.</p>"
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
    telegram_chat_id_raw = str(payload.get("telegram_chat_id", "")).strip()
    telegram_thread_id_raw = str(payload.get("telegram_thread_id", "")).strip()
    if not project_id:
        raise web.HTTPBadRequest(text="project_id is required")

    created = store.start_session(
        project_id=project_id,
        project_name=project_name,
        redirect_uri=f"{callback_base_url}/gmail/bootstrap/gog/callback/{{session_id}}",
        callback_base_url=callback_base_url,
        oauth_client_name=oauth_client_name,
        telegram_chat_id=int(telegram_chat_id_raw) if telegram_chat_id_raw else None,
        telegram_thread_id=int(telegram_thread_id_raw) if telegram_thread_id_raw else None,
    )
    artifact_dir = _session_artifact_dir(created.session_id)
    checklist_path = artifact_dir / "MANUAL_CHECKLIST.md"
    checklist_path.write_text(
        build_manual_checklist(
            project_id=created.project_id,
            project_name=created.project_name,
            redirect_uri=created.redirect_uri,
            oauth_client_name=created.oauth_client_name,
        )
        + "\n",
        encoding="utf-8",
    )
    session = store.record_project_bootstrap(
        session_id=created.session_id,
        manual_console_url="https://console.cloud.google.com/apis/credentials",
        manual_checklist_path=str(checklist_path),
    )
    if session is None:
        raise web.HTTPInternalServerError(text="Failed to initialize Gmail setup session.")
    if request.content_type == "application/json":
        return web.json_response(_session_payload(callback_base_url, session), status=201)
    raise web.HTTPFound(location=f"/gmail/bootstrap/session/{session.session_id}")


async def _exchange_code_for_token(
    *,
    code: str,
    redirect_uri: str,
    client_id: str,
    client_secret: str,
) -> dict[str, Any]:
    async with ClientSession() as session:
        async with session.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code": code,
                "client_id": client_id,
                "client_secret": client_secret,
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
    oauth_credentials = _bootstrap_oauth_credentials()
    if oauth_credentials is None:
        raise web.HTTPServiceUnavailable(text="Bootstrap Google OAuth client is not configured.")
    client_id, client_secret = oauth_credentials
    session_id = request.query.get("state", "").strip()
    if not session_id:
        raise web.HTTPBadRequest(text="Missing OAuth state.")
    session = store.get(session_id)
    if session is None:
        raise web.HTTPNotFound(text="Unknown bootstrap session.")
    error = request.query.get("error", "").strip()
    if error:
        failed_session = store.record_failed(session_id=session_id, reason=f"google_auth_error:{error}")
        await _notify_telegram_for_session(store, failed_session)
        raise web.HTTPFound(location=f"/gmail/bootstrap/session/{session_id}")
    code = request.query.get("code", "").strip()
    if not code:
        raise web.HTTPBadRequest(text="Missing authorization code.")

    redirect_uri = f"{session.callback_base_url}/gmail/bootstrap/google/callback"
    token_payload = await _exchange_code_for_token(
        code=code,
        redirect_uri=redirect_uri,
        client_id=client_id,
        client_secret=client_secret,
    )
    access_token = str(token_payload.get("access_token", "")).strip()
    if not access_token:
        failed_session = store.record_failed(
            session_id=session_id,
            reason="missing_access_token",
            guidance="Google returned no access token. Retry Google sign-in from the setup page.",
        )
        await _notify_telegram_for_session(store, failed_session)
        raise web.HTTPFound(location=f"/gmail/bootstrap/session/{session_id}")
    userinfo = await _fetch_userinfo(access_token=access_token)
    email = str(userinfo.get("email", "")).strip() or "unknown"
    cloud_session = store.record_cloud_auth(session_id=session_id, account_email=email)
    await _notify_telegram_for_session(store, cloud_session)
    try:
        bootstrap_result = await bootstrap_gcp_project(
            access_token=access_token,
            project_id=session.project_id,
            project_name=session.project_name,
        )
    except Exception as exc:
        failed_session = store.record_failed(
            session_id=session_id,
            reason=f"gcp_bootstrap_failed:{exc}",
            guidance=_gcp_failure_guidance(str(exc)),
        )
        await _notify_telegram_for_session(store, failed_session)
    else:
        artifact_dir = _session_artifact_dir(session_id)
        checklist_path = artifact_dir / "MANUAL_CHECKLIST.md"
        checklist_path.write_text(
            build_manual_checklist(
                project_id=session.project_id,
                project_name=session.project_name,
                redirect_uri=session.redirect_uri,
                oauth_client_name=session.oauth_client_name,
            )
            + "\n",
            encoding="utf-8",
        )
        project_session = store.record_project_bootstrap(
            session_id=session_id,
            project_number=bootstrap_result["project_number"],
            manual_console_url=bootstrap_result["manual_console_url"],
            manual_checklist_path=str(checklist_path),
        )
        await _notify_telegram_for_session(store, project_session)
    raise web.HTTPFound(location=f"/gmail/bootstrap/session/{session_id}")


def _session_artifact_dir(session_id: str) -> Path:
    path = _artifact_root() / session_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def _validate_credentials_json(raw_text: str) -> dict[str, Any]:
    payload = json.loads(raw_text)
    if not isinstance(payload, dict):
        raise ValueError("Top-level credentials payload must be a JSON object.")
    if "installed" not in payload and "web" not in payload:
        raise ValueError("Credentials JSON must contain `installed` or `web` section.")
    return payload


def _gog_env() -> dict[str, str]:
    env = os.environ.copy()
    if env.get("GOG_KEYRING_PASSWORD", "").strip():
        return env
    path = _gog_keyring_password_path()
    if path.exists():
        password = path.read_text(encoding="utf-8").strip()
    else:
        password = secrets.token_urlsafe(32)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(password + "\n", encoding="utf-8")
        path.chmod(0o600)
    env["GOG_KEYRING_PASSWORD"] = password
    return env


def _run_gog_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, capture_output=True, text=True, check=False, env=_gog_env())


def _extract_first_url(text: str) -> str | None:
    match = re.search(r"https?://[^\s\"']+", text)
    return match.group(0) if match else None


def _import_gog_credentials(credentials_path: Path) -> None:
    gog_path = _find_gog_binary()
    if not gog_path:
        raise RuntimeError("gog binary not found.")
    result = _run_gog_command([gog_path, "auth", "credentials", "set", str(credentials_path)])
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "gog auth credentials set failed")


def _start_gog_remote_auth(*, gmail_account_email: str, redirect_uri: str) -> str:
    gog_path = _find_gog_binary()
    if not gog_path:
        raise RuntimeError("gog binary not found.")
    result = _run_gog_command(
        [
            gog_path,
            "auth",
            "add",
            gmail_account_email,
            "--services",
            "gmail",
            "--remote",
            "--step",
            "1",
            "--redirect-uri",
            redirect_uri,
            "-j",
            "--results-only",
        ]
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "gog auth add step 1 failed")
    url = _extract_first_url("\n".join(part for part in (result.stdout, result.stderr) if part))
    if not url:
        raise RuntimeError("Could not extract Google auth URL from gog output.")
    return url


def _finish_gog_remote_auth(*, gmail_account_email: str, redirect_uri: str, auth_url: str) -> None:
    gog_path = _find_gog_binary()
    if not gog_path:
        raise RuntimeError("gog binary not found.")
    result = _run_gog_command(
        [
            gog_path,
            "auth",
            "add",
            gmail_account_email,
            "--services",
            "gmail",
            "--remote",
            "--step",
            "2",
            "--redirect-uri",
            redirect_uri,
            "--auth-url",
            auth_url,
        ]
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "gog auth add step 2 failed")


async def _upload_credentials(request: web.Request) -> web.Response:
    store: GmailBootstrapStateStore = request.app["gmail_bootstrap_store"]
    session_id = request.match_info["session_id"]
    session = store.get(session_id)
    if session is None:
        raise web.HTTPNotFound(text="Unknown bootstrap session.")
    post = await request.post()
    gmail_account_email = str(post.get("gmail_account_email", "")).strip()
    if not gmail_account_email:
        raise web.HTTPBadRequest(text="gmail_account_email is required.")
    file_field = post.get("credentials_file")
    if file_field is None or not hasattr(file_field, "file"):
        raise web.HTTPBadRequest(text="credentials_file is required.")
    raw_bytes = file_field.file.read()
    raw_text = raw_bytes.decode("utf-8")
    try:
        _validate_credentials_json(raw_text)
    except (json.JSONDecodeError, ValueError) as exc:
        raise web.HTTPBadRequest(text=str(exc)) from exc
    artifact_dir = _session_artifact_dir(session_id)
    credentials_path = artifact_dir / "client_secret.json"
    credentials_path.write_text(raw_text, encoding="utf-8")
    store.record_credentials_uploaded(session_id=session_id, credentials_path=str(credentials_path))
    try:
        _import_gog_credentials(credentials_path)
        auth_url = _start_gog_remote_auth(
            gmail_account_email=gmail_account_email,
            redirect_uri=session.redirect_uri,
        )
        auth_started_session = store.record_gmail_auth_started_for_account(
            session_id=session_id,
            gmail_account_email=gmail_account_email,
        )
        await _notify_telegram_for_session(store, auth_started_session)
    except Exception as exc:
        failed_session = store.record_failed(
            session_id=session_id,
            reason=f"gog_remote_start_failed:{exc}",
            guidance="The Gmail authorization step could not start locally. Check gog and the uploaded client_secret.json, then retry.",
        )
        await _notify_telegram_for_session(store, failed_session)
        raise web.HTTPFound(location=f"/gmail/bootstrap/session/{session_id}")
    raise web.HTTPFound(location=auth_url)


async def _upload_bootstrap_credentials(request: web.Request) -> web.Response:
    store: GmailBootstrapStateStore = request.app["gmail_bootstrap_store"]
    session_id = request.match_info["session_id"]
    session = store.get(session_id)
    if session is None:
        raise web.HTTPNotFound(text="Unknown bootstrap session.")
    post = await request.post()
    file_field = post.get("bootstrap_credentials_file")
    if file_field is None or not hasattr(file_field, "file"):
        raise web.HTTPBadRequest(text="bootstrap_credentials_file is required.")
    raw_bytes = file_field.file.read()
    raw_text = raw_bytes.decode("utf-8")
    try:
        _save_bootstrap_oauth_credentials(raw_text)
    except (json.JSONDecodeError, ValueError) as exc:
        raise web.HTTPBadRequest(text=str(exc)) from exc
    raise web.HTTPFound(location=f"/gmail/bootstrap/session/{session_id}")


async def _restart_gmail_authorization(request: web.Request) -> web.Response:
    store: GmailBootstrapStateStore = request.app["gmail_bootstrap_store"]
    session_id = request.match_info["session_id"]
    session = store.get(session_id)
    if session is None:
        raise web.HTTPNotFound(text="Unknown bootstrap session.")
    if not session.gmail_account_email:
        raise web.HTTPBadRequest(text="gmail_account_email is missing for this session.")
    if not session.credentials_path:
        raise web.HTTPBadRequest(text="credentials_path is missing for this session.")
    credentials_path = Path(session.credentials_path)
    if not credentials_path.exists():
        raise web.HTTPBadRequest(text=f"credentials file not found: {credentials_path}")
    try:
        _import_gog_credentials(credentials_path)
        auth_url = _start_gog_remote_auth(
            gmail_account_email=session.gmail_account_email,
            redirect_uri=session.redirect_uri,
        )
        started = store.record_gmail_auth_started_for_account(
            session_id=session_id,
            gmail_account_email=session.gmail_account_email,
        )
        await _notify_telegram_for_session(store, started)
    except Exception as exc:
        failed_session = store.record_failed(
            session_id=session_id,
            reason=f"gog_remote_restart_failed:{exc}",
            guidance="Could not restart Gmail authorization. Confirm OAuth redirect URI and uploaded credentials, then retry.",
        )
        await _notify_telegram_for_session(store, failed_session)
        raise web.HTTPFound(location=f"/gmail/bootstrap/session/{session_id}")
    raise web.HTTPFound(location=auth_url)


async def _gog_callback(request: web.Request) -> web.Response:
    store: GmailBootstrapStateStore = request.app["gmail_bootstrap_store"]
    session_id = request.match_info["session_id"]
    session = store.get(session_id)
    if session is None:
        raise web.HTTPNotFound(text="Unknown bootstrap session.")
    if not session.gmail_account_email:
        failed_session = store.record_failed(
            session_id=session_id,
            reason="missing_gmail_account_email",
            guidance="The setup session lost the Gmail account email. Return to the setup page and restart the Gmail authorization step.",
        )
        await _notify_telegram_for_session(store, failed_session)
        raise web.HTTPFound(location=f"/gmail/bootstrap/session/{session_id}")
    error = request.query.get("error", "").strip()
    if error:
        failed_session = store.record_failed(
            session_id=session_id,
            reason=f"gmail_auth_error:{error}",
            guidance="Google returned an error during Gmail authorization. Retry the Gmail authorization step from the setup page.",
        )
        await _notify_telegram_for_session(store, failed_session)
        raise web.HTTPFound(location=f"/gmail/bootstrap/session/{session_id}")
    auth_url = f"{request.scheme}://{request.host}{request.path_qs}"
    try:
        _finish_gog_remote_auth(
            gmail_account_email=session.gmail_account_email,
            redirect_uri=session.redirect_uri,
            auth_url=auth_url,
        )
    except Exception as exc:
        error_text = str(exc)
        # Self-heal once for common non-interactive keyring failures by immediately restarting auth.
        if "keyring" in error_text.lower() and "gog_keyring_password" in error_text.lower():
            try:
                if session.credentials_path:
                    _import_gog_credentials(Path(session.credentials_path))
                retry_auth_url = _start_gog_remote_auth(
                    gmail_account_email=session.gmail_account_email,
                    redirect_uri=session.redirect_uri,
                )
                retried = store.record_gmail_auth_started_for_account(
                    session_id=session_id,
                    gmail_account_email=session.gmail_account_email,
                )
                await _notify_telegram_for_session(store, retried)
                raise web.HTTPFound(location=retry_auth_url)
            except web.HTTPException:
                raise
            except Exception:
                pass
        result_session = store.record_failed(
            session_id=session_id,
            reason=f"gog_remote_finish_failed:{exc}",
            guidance="Google authorization completed, but local token storage failed. Check gog on the host, then retry the setup.",
        )
    else:
        result_session = store.record_completed(
            session_id=session_id,
            gmail_account_email=session.gmail_account_email,
        )
    await _notify_telegram_for_session(store, result_session)
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
            web.post("/gmail/bootstrap/session/{session_id}/bootstrap-credentials/upload", _upload_bootstrap_credentials),
            web.post("/gmail/bootstrap/session/{session_id}/credentials/upload", _upload_credentials),
            web.post("/gmail/bootstrap/session/{session_id}/gmail-auth/restart", _restart_gmail_authorization),
            web.get("/gmail/bootstrap/gog/callback/{session_id}", _gog_callback),
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
