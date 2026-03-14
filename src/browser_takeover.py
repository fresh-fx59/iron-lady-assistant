from __future__ import annotations

import argparse
import asyncio
import json
import os
import secrets
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib import error, request

from aiohttp import ClientSession, WSMsgType, web


DEFAULT_PORT = 18792
DEFAULT_HOST = "127.0.0.1"


def _default_state_root() -> Path:
    xdg_home = os.environ.get("XDG_STATE_HOME")
    if xdg_home:
        return Path(xdg_home)
    return Path.home() / ".local" / "state"


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _state_dir() -> Path:
    return _default_state_root() / "iron-lady-assistant" / "browser-takeover"


def _config_path() -> Path:
    return _state_dir() / "relay.json"


def _installed_extension_dir() -> Path:
    return _state_dir() / "chrome-extension"


def _bundled_extension_dir() -> Path:
    return _repo_root() / "assets" / "browser-takeover-extension"


@dataclass
class RelaySettings:
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    token: str = ""


@dataclass
class AttachedTab:
    tab_id: int
    title: str = ""
    url: str = ""
    attached: bool = False
    last_error: str | None = None


@dataclass
class RelayState:
    settings: RelaySettings
    extension_ws: web.WebSocketResponse | None = None
    tabs: dict[int, AttachedTab] = field(default_factory=dict)
    pending: dict[str, asyncio.Future[dict[str, Any]]] = field(default_factory=dict)
    next_request_id: int = 1

    def allocate_request_id(self) -> str:
        value = str(self.next_request_id)
        self.next_request_id += 1
        return value


RELAY_STATE_KEY = web.AppKey("relay_state", RelayState)


def _load_settings() -> RelaySettings:
    path = _config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raw = json.loads(path.read_text(encoding="utf-8"))
        return RelaySettings(
            host=str(raw.get("host") or DEFAULT_HOST),
            port=int(raw.get("port") or DEFAULT_PORT),
            token=str(raw.get("token") or ""),
        )
    settings = RelaySettings(token=secrets.token_urlsafe(24))
    _save_settings(settings)
    return settings


def _save_settings(settings: RelaySettings) -> None:
    path = _config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "host": settings.host,
                "port": settings.port,
                "token": settings.token,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def install_extension() -> Path:
    src = _bundled_extension_dir()
    dest = _installed_extension_dir()
    if not (src / "manifest.json").exists():
        raise RuntimeError(f"Bundled extension is missing: {src}")
    if dest.exists():
        shutil.rmtree(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(src, dest)
    return dest


def build_setup_payload() -> dict[str, Any]:
    settings = _load_settings()
    extension_dir = install_extension()
    return {
        "ok": True,
        "host": settings.host,
        "port": settings.port,
        "token": settings.token,
        "extension_path": str(extension_dir),
        "commands": [
            f"python3 -m src.browser_takeover serve --host {settings.host} --port {settings.port}",
            "Chrome -> chrome://extensions -> enable Developer mode",
            f"Load unpacked -> {extension_dir}",
            f"Open extension options -> set Relay port {settings.port} and token {settings.token}",
            "Click the toolbar button on the tab you want to attach",
            "python3 -m src.browser_takeover targets",
        ],
    }


def _format_setup_text(payload: dict[str, Any]) -> str:
    lines = [
        f"relay: http://{payload['host']}:{payload['port']}",
        f"extension_path: {payload['extension_path']}",
        f"token: {payload['token']}",
        "",
        "next_steps:",
    ]
    for command in payload["commands"]:
        lines.append(f"- {command}")
    return "\n".join(lines)


def _is_authorized(req: web.Request, settings: RelaySettings) -> bool:
    header = req.headers.get("Authorization", "")
    if header == f"Bearer {settings.token}":
        return True
    if req.query.get("token") == settings.token:
        return True
    return False


def _require_authorized(req: web.Request, settings: RelaySettings) -> None:
    if not _is_authorized(req, settings):
        raise web.HTTPUnauthorized(text=json.dumps({"ok": False, "error": "unauthorized"}))


async def _health(_req: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def _targets(req: web.Request) -> web.Response:
    state = req.app[RELAY_STATE_KEY]
    _require_authorized(req, state.settings)
    return web.json_response(
        {
            "ok": True,
            "targets": [
                {
                    "tab_id": tab.tab_id,
                    "title": tab.title,
                    "url": tab.url,
                    "attached": tab.attached,
                    "last_error": tab.last_error,
                }
                for tab in sorted(state.tabs.values(), key=lambda item: item.tab_id)
            ],
        }
    )


async def _cdp(req: web.Request) -> web.Response:
    state = req.app[RELAY_STATE_KEY]
    _require_authorized(req, state.settings)
    if state.extension_ws is None:
        raise web.HTTPConflict(text=json.dumps({"ok": False, "error": "extension_not_connected"}))

    body = await req.json()
    tab_id = int(body.get("tab_id"))
    method = str(body.get("method") or "").strip()
    params = body.get("params") or {}
    if tab_id not in state.tabs:
        raise web.HTTPNotFound(text=json.dumps({"ok": False, "error": "tab_not_found"}))
    if not method:
        raise web.HTTPBadRequest(text=json.dumps({"ok": False, "error": "method_required"}))

    request_id = state.allocate_request_id()
    loop = asyncio.get_running_loop()
    future: asyncio.Future[dict[str, Any]] = loop.create_future()
    state.pending[request_id] = future
    await state.extension_ws.send_json(
        {
            "type": "cdp_command",
            "id": request_id,
            "tabId": tab_id,
            "method": method,
            "params": params,
        }
    )
    try:
        result = await asyncio.wait_for(future, timeout=10.0)
    except TimeoutError as exc:
        state.pending.pop(request_id, None)
        raise web.HTTPGatewayTimeout(text=json.dumps({"ok": False, "error": "timeout"})) from exc
    if result.get("error"):
        raise web.HTTPBadRequest(text=json.dumps({"ok": False, "error": result["error"]}))
    return web.json_response({"ok": True, "result": result.get("result")})


async def _extension_ws(req: web.Request) -> web.StreamResponse:
    state = req.app[RELAY_STATE_KEY]
    _require_authorized(req, state.settings)
    origin = req.headers.get("Origin", "")
    if origin and not origin.startswith("chrome-extension://"):
        raise web.HTTPForbidden(text=json.dumps({"ok": False, "error": "forbidden_origin"}))

    ws = web.WebSocketResponse(heartbeat=20.0)
    await ws.prepare(req)
    state.extension_ws = ws
    await ws.send_json({"type": "welcome"})
    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue
            data = json.loads(msg.data)
            kind = data.get("type")
            if kind == "attach":
                tab = AttachedTab(
                    tab_id=int(data["tabId"]),
                    title=str(data.get("title") or ""),
                    url=str(data.get("url") or ""),
                    attached=True,
                )
                state.tabs[tab.tab_id] = tab
            elif kind == "detach":
                tab_id = int(data["tabId"])
                state.tabs.pop(tab_id, None)
            elif kind == "tab_update":
                tab_id = int(data["tabId"])
                tab = state.tabs.get(tab_id)
                if tab:
                    tab.title = str(data.get("title") or tab.title)
                    tab.url = str(data.get("url") or tab.url)
            elif kind == "cdp_response":
                request_id = str(data.get("id"))
                future = state.pending.pop(request_id, None)
                if future and not future.done():
                    future.set_result(
                        {
                            "result": data.get("result"),
                            "error": data.get("error"),
                        }
                    )
            elif kind == "cdp_event":
                tab_id = int(data.get("tabId"))
                tab = state.tabs.get(tab_id)
                if tab:
                    tab.url = str(data.get("url") or tab.url)
    finally:
        if state.extension_ws is ws:
            state.extension_ws = None
        for future in state.pending.values():
            if not future.done():
                future.cancel()
        state.pending.clear()
    return ws


def create_app(settings: RelaySettings | None = None) -> web.Application:
    app = web.Application()
    app[RELAY_STATE_KEY] = RelayState(settings=settings or _load_settings())
    app.router.add_get("/healthz", _health)
    app.router.add_get("/targets", _targets)
    app.router.add_post("/cdp", _cdp)
    app.router.add_get("/extension", _extension_ws)
    return app


def _request_json(method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = _load_settings()
    url = f"http://{settings.host}:{settings.port}{path}"
    data = None
    headers = {"Authorization": f"Bearer {settings.token}"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = request.Request(url, method=method, data=data, headers=headers)
    try:
        with request.urlopen(req, timeout=10) as res:
            return json.loads(res.read().decode("utf-8"))
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", "ignore")
        raise RuntimeError(body or str(exc)) from exc


def _call_cdp(tab_id: int, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = _request_json(
        "POST",
        "/cdp",
        {
            "tab_id": tab_id,
            "method": method,
            "params": params or {},
        },
    )
    return payload.get("result") or {}


def _snapshot_expression() -> str:
    return """
(() => ({
  title: document.title || "",
  url: location.href || "",
  text: (document.body?.innerText || "").replace(/\\s+/g, " ").trim().slice(0, 4000),
}))()
""".strip()


def _snapshot_tab(tab_id: int) -> dict[str, Any]:
    result = _call_cdp(
        tab_id,
        "Runtime.evaluate",
        {
            "expression": _snapshot_expression(),
            "returnByValue": True,
        },
    )
    payload = (((result.get("result") or {}).get("value")) if isinstance(result, dict) else None) or {}
    return {
        "ok": True,
        "tab_id": tab_id,
        "snapshot": payload,
    }


def _navigate_tab(tab_id: int, url: str) -> dict[str, Any]:
    result = _call_cdp(tab_id, "Page.navigate", {"url": url})
    return {
        "ok": True,
        "tab_id": tab_id,
        "url": url,
        "result": result,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Browser takeover relay for iron-lady-assistant.")
    sub = parser.add_subparsers(dest="command", required=True)

    setup_parser = sub.add_parser("setup", help="Install extension assets and print local setup steps.")
    setup_parser.add_argument("--format", choices=("text", "json"), default="text")

    serve_parser = sub.add_parser("serve", help="Start the local browser takeover relay.")
    serve_parser.add_argument("--host", default=DEFAULT_HOST)
    serve_parser.add_argument("--port", type=int, default=DEFAULT_PORT)

    targets_parser = sub.add_parser("targets", help="List tabs currently attached by the extension.")
    targets_parser.add_argument("--format", choices=("text", "json"), default="json")

    cdp_parser = sub.add_parser("cdp", help="Send a raw CDP command to an attached tab.")
    cdp_parser.add_argument("--tab-id", type=int, required=True)
    cdp_parser.add_argument("--method", required=True)
    cdp_parser.add_argument("--params", default="{}")
    cdp_parser.add_argument("--format", choices=("text", "json"), default="json")

    navigate_parser = sub.add_parser("navigate", help="Navigate an attached tab to a URL.")
    navigate_parser.add_argument("--tab-id", type=int, required=True)
    navigate_parser.add_argument("--url", required=True)
    navigate_parser.add_argument("--format", choices=("text", "json"), default="json")

    snapshot_parser = sub.add_parser("snapshot", help="Read a compact page snapshot from an attached tab.")
    snapshot_parser.add_argument("--tab-id", type=int, required=True)
    snapshot_parser.add_argument("--format", choices=("text", "json"), default="json")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "setup":
        payload = build_setup_payload()
        if args.format == "json":
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(_format_setup_text(payload))
        return 0

    if args.command == "serve":
        settings = _load_settings()
        settings.host = args.host
        settings.port = args.port
        _save_settings(settings)
        web.run_app(create_app(settings), host=settings.host, port=settings.port)
        return 0

    if args.command == "targets":
        try:
            payload = _request_json("GET", "/targets")
        except Exception as exc:
            print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
            return 1
        if args.format == "json":
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print("\n".join(f"{item['tab_id']}: {item['title']} {item['url']}" for item in payload["targets"]))
        return 0

    if args.command == "cdp":
        try:
            payload = _request_json(
                "POST",
                "/cdp",
                {
                    "tab_id": args.tab_id,
                    "method": args.method,
                    "params": json.loads(args.params),
                },
            )
        except Exception as exc:
            print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
            return 1
        if args.format == "json":
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(json.dumps(payload.get("result"), ensure_ascii=False, indent=2))
        return 0

    if args.command == "navigate":
        try:
            payload = _navigate_tab(args.tab_id, args.url)
        except Exception as exc:
            print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
            return 1
        if args.format == "json":
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(f"navigated tab {args.tab_id} to {args.url}")
        return 0

    if args.command == "snapshot":
        try:
            payload = _snapshot_tab(args.tab_id)
        except Exception as exc:
            print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
            return 1
        if args.format == "json":
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            snapshot = payload["snapshot"]
            print(
                "\n".join(
                    [
                        f"title: {snapshot.get('title', '')}",
                        f"url: {snapshot.get('url', '')}",
                        "",
                        str(snapshot.get("text", "")),
                    ]
                ).strip()
            )
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
