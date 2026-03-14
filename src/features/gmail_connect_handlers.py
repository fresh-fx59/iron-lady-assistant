from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen
from uuid import uuid4

from aiogram.utils.keyboard import InlineKeyboardBuilder

from .. import config
from .gmail_bootstrap_state import GmailBootstrapStateStore
from ..gmail_bootstrap_web import build_session_urls


def _local_service_base_url() -> str:
    return f"http://{config.GMAIL_BOOTSTRAP_BIND_HOST}:{config.GMAIL_BOOTSTRAP_BIND_PORT}"


def _healthcheck_url() -> str:
    if config.GMAIL_BOOTSTRAP_HEALTHCHECK_URL:
        return f"{config.GMAIL_BOOTSTRAP_HEALTHCHECK_URL}/health"
    return f"{_local_service_base_url()}/health"


def _public_base_url() -> str:
    if config.GMAIL_BOOTSTRAP_PUBLIC_BASE_URL:
        return config.GMAIL_BOOTSTRAP_PUBLIC_BASE_URL
    return _local_service_base_url()


def _service_state_path() -> Path:
    return config.MEMORY_DIR / "gmail_bootstrap_web.json"


def _http_json(method: str, url: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    body = None
    headers = {}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(url, data=body, method=method, headers=headers)
    with urlopen(request, timeout=5) as response:  # noqa: S310
        raw = response.read().decode("utf-8")
    decoded = json.loads(raw)
    return decoded if isinstance(decoded, dict) else {}


def _service_healthy() -> bool:
    try:
        payload = _http_json("GET", _healthcheck_url())
    except (OSError, URLError, TimeoutError, json.JSONDecodeError):
        return False
    return payload.get("status") == "ok"


def _spawn_bootstrap_service() -> None:
    state_path = _service_state_path()
    state_path.parent.mkdir(parents=True, exist_ok=True)
    stdout_path = config.MEMORY_DIR / "gmail_bootstrap_web.stdout.log"
    stderr_path = config.MEMORY_DIR / "gmail_bootstrap_web.stderr.log"
    repo_root = Path(__file__).resolve().parents[2]
    with stdout_path.open("ab") as stdout_file, stderr_path.open("ab") as stderr_file:
        proc = subprocess.Popen(  # noqa: S603
            [
                sys.executable,
                "-m",
                "src.gmail_bootstrap_web",
                "--host",
                config.GMAIL_BOOTSTRAP_BIND_HOST,
                "--port",
                str(config.GMAIL_BOOTSTRAP_BIND_PORT),
                "--state-path",
                str(config.MEMORY_DIR / "gmail_bootstrap_sessions.json"),
            ],
            cwd=repo_root,
            stdout=stdout_file,
            stderr=stderr_file,
            start_new_session=True,
        )
    state_path.write_text(
        json.dumps(
            {
                "pid": proc.pid,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "local_base_url": _local_service_base_url(),
                "public_base_url": _public_base_url(),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


async def ensure_gmail_bootstrap_service_running() -> tuple[bool, str]:
    if await asyncio.to_thread(_service_healthy):
        return True, _public_base_url()
    if not config.GMAIL_BOOTSTRAP_AUTOSTART:
        return False, "Gmail bootstrap web service is not running and autostart is disabled."
    await asyncio.to_thread(_spawn_bootstrap_service)
    for _ in range(20):
        if await asyncio.to_thread(_service_healthy):
            return True, _public_base_url()
        await asyncio.sleep(0.25)
    return False, "Gmail bootstrap web service did not become healthy after autostart."


def _generate_project_id(chat_id: int, thread_id: int | None) -> str:
    thread_suffix = "main" if thread_id is None else f"t{abs(thread_id) % 1000}"
    unique = uuid4().hex[:6]
    return f"ila-gmail-{abs(chat_id) % 10000}-{thread_suffix}-{unique}"[:30].rstrip("-")


async def create_gmail_bootstrap_session(*, chat_id: int, thread_id: int | None) -> dict[str, Any]:
    payload = {
        "project_id": _generate_project_id(chat_id, thread_id),
        "project_name": "Iron Lady Assistant Gmail",
        "callback_base_url": _public_base_url(),
        "oauth_client_name": "Iron Lady Assistant Gmail",
        "telegram_chat_id": chat_id,
        "telegram_thread_id": thread_id,
    }
    response = await asyncio.to_thread(
        _http_json,
        "POST",
        f"{_local_service_base_url()}/gmail/bootstrap/start",
        payload,
    )
    session_id = str(response.get("session_id", "")).strip()
    if not session_id:
        raise RuntimeError("Gmail bootstrap start did not return a session id.")
    urls = build_session_urls(base_url=_public_base_url(), session_id=session_id)
    response["urls"] = urls
    return response


async def cmd_gmail_connect(
    message: Any,
    *,
    is_authorized,
    thread_id_fn,
    ensure_service_running_fn=ensure_gmail_bootstrap_service_running,
    create_session_fn=create_gmail_bootstrap_session,
) -> None:
    if not is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return

    thread_id = thread_id_fn(message)
    ready, detail = await ensure_service_running_fn()
    if not ready:
        await message.answer(detail)
        return

    try:
        session = await create_session_fn(chat_id=message.chat.id, thread_id=thread_id)
    except Exception as exc:
        await message.answer(f"Failed to prepare Gmail setup: {exc}")
        return

    session_url = session["urls"]["session_page_url"]
    lines = [
        "Gmail setup is ready.",
        "",
        "1. Open the setup page.",
        "2. Complete the Google Cloud OAuth client step shown on the page.",
        "3. Upload client_secret.json and enter your Gmail account.",
        "4. Finish Gmail authorization and return here for confirmation.",
    ]
    kb = InlineKeyboardBuilder()
    kb.button(text="Open Gmail Setup", url=session_url)
    await message.answer("\n".join(lines), reply_markup=kb.as_markup())


async def cmd_gmail_status(
    message: Any,
    *,
    is_authorized,
    thread_id_fn,
) -> None:
    if not is_authorized(message.from_user and message.from_user.id, message.chat.id):
        return

    store = GmailBootstrapStateStore(config.MEMORY_DIR / "gmail_bootstrap_sessions.json")
    thread_id = thread_id_fn(message)
    session = store.latest_for_scope(
        telegram_chat_id=message.chat.id,
        telegram_thread_id=thread_id,
    )
    if session is None:
        await message.answer("No Gmail setup session found for this chat yet. Use /gmail_connect to start.")
        return

    urls = build_session_urls(base_url=_public_base_url(), session_id=session.session_id)
    lines = [
        f"Gmail status: {session.phase.replace('_', ' ')}",
        f"Project: {session.project_id}",
        f"Setup page: {urls['session_page_url']}",
    ]
    if session.gmail_account_email:
        lines.append(f"Gmail account: {session.gmail_account_email}")
    if session.connected_at:
        lines.append(f"Connected at: {session.connected_at}")
    if session.failure_reason:
        lines.append(f"Last error: {session.failure_reason}")
    await message.answer("\n".join(lines))
