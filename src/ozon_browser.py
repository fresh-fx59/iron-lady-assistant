from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus


PRICE_RE = re.compile(r"(\d[\d\s]{0,12})\s*(?:₽|руб\.?)", re.IGNORECASE)
DEFAULT_ALLOWED_DOMAINS = "ozon.ru,*.ozon.ru"
DEFAULT_SESSION = "ozon"
DEFAULT_LOGIN_URL = "https://www.ozon.ru/my/"
ORDER_URL_CANDIDATES = (
    "https://www.ozon.ru/my/orderlist",
    "https://www.ozon.ru/my/orders",
    "https://www.ozon.ru/my/",
)
SETUP_FORMATS = ("text", "json")


def _default_state_root() -> Path:
    xdg_home = os.environ.get("XDG_STATE_HOME")
    if xdg_home:
        return Path(xdg_home)
    return Path.home() / ".local" / "state"


@dataclass
class BrowserConfig:
    repo_root: Path
    profile_path: Path
    download_path: Path
    session: str = DEFAULT_SESSION
    provider: str | None = None
    proxy: str | None = None
    cdp: str | None = None
    headed: bool = False
    allow_domains: str = DEFAULT_ALLOWED_DOMAINS
    max_output: int = 12000
    user_agent: str | None = None
    extra_args: tuple[str, ...] = ()


class BrowserCommandError(RuntimeError):
    pass


@dataclass
class SetupStatus:
    ok: bool
    platform: str
    recommended_path: str
    repo_root: Path
    state_root: Path
    checks: dict[str, bool]
    commands: list[str]
    notes: list[str]


class OzonBrowser:
    def __init__(self, config: BrowserConfig) -> None:
        self.config = config

    def open_login(self) -> dict[str, Any]:
        self.run("open", DEFAULT_LOGIN_URL)
        self.run("wait", "6000")
        return {
            "ok": True,
            "message": "Browser opened on Ozon account page. Complete login manually in the persistent profile.",
            "login_url": DEFAULT_LOGIN_URL,
            "profile_path": str(self.config.profile_path),
            "session": self.config.session,
        }

    def fetch_order_statuses(self) -> dict[str, Any]:
        last_error = ""
        for candidate in ORDER_URL_CANDIDATES:
            try:
                self.run("open", candidate)
                self.run("wait", "7000")
                payload = self.eval_json(_orders_extract_script())
                current_url = self.run("get", "url")
                if self._looks_access_blocked(payload):
                    incident = self._extract_incident(payload)
                    message = "Ozon returned an access-restricted page"
                    if incident:
                        message += f" (incident: {incident})"
                    raise BrowserCommandError(message)
                if self._looks_logged_out(payload, current_url):
                    continue
                payload["current_url"] = current_url
                payload["source_url"] = candidate
                payload["profile_path"] = str(self.config.profile_path)
                payload["session"] = self.config.session
                return payload
            except BrowserCommandError as exc:
                last_error = str(exc)
        raise BrowserCommandError(
            "Unable to retrieve Ozon orders. The session is likely not logged in, "
            f"or Ozon changed the account pages. Last error: {last_error or 'none'}"
        )

    def search_products(self, query: str, max_price: int | None = None, limit: int = 5) -> dict[str, Any]:
        url = f"https://www.ozon.ru/search/?text={quote_plus(query)}"
        self.run("open", url)
        self.run("wait", "7000")
        payload = self.eval_json(_search_extract_script(limit=max(limit, 1)))
        items = payload.get("items", [])
        if max_price is not None:
            items = [item for item in items if item.get("price_rub") is not None and item["price_rub"] <= max_price]
        payload["items"] = items[:limit]
        payload["query"] = query
        payload["max_price"] = max_price
        payload["current_url"] = self.run("get", "url")
        return payload

    def prepare_buy(
        self,
        query: str,
        max_price: int | None,
        quantity: int,
        checkout: bool,
    ) -> dict[str, Any]:
        search_payload = self.search_products(query=query, max_price=max_price, limit=10)
        items = search_payload.get("items", [])
        if not items:
            raise BrowserCommandError("No Ozon products matched the query and price ceiling.")

        chosen = items[0]
        product_url = chosen.get("url")
        if not product_url:
            raise BrowserCommandError("Top Ozon search result did not expose a product URL.")

        self.run("open", product_url)
        self.run("wait", "5000")
        clicked = self.eval_json(_click_by_text_script(["В корзину", "Добавить в корзину", "Add to cart"]))
        if not clicked.get("clicked"):
            raise BrowserCommandError("Could not find an add-to-cart button on the selected Ozon product page.")

        for _ in range(max(quantity - 1, 0)):
            self.eval_json(_click_by_text_script(["+", "Добавить", "Увеличить количество"]))

        checkout_ready = False
        if checkout:
            checkout_click = self.eval_json(
                _click_by_text_script(
                    [
                        "Перейти к оформлению",
                        "Оформить заказ",
                        "В корзину",
                        "Перейти в корзину",
                    ]
                )
            )
            checkout_ready = bool(checkout_click.get("clicked"))
            self.run("wait", "4000")

        return {
            "ok": True,
            "query": query,
            "max_price": max_price,
            "quantity": quantity,
            "selected_item": chosen,
            "add_to_cart": clicked,
            "checkout_ready": checkout_ready,
            "current_url": self.run("get", "url"),
            "next_step": (
                "Inspect the checkout summary and only then run `place-order --confirm`."
                if checkout
                else "Item added to cart. Open cart/checkout before placing the order."
            ),
        }

    def place_order(self, confirm: bool) -> dict[str, Any]:
        if not confirm:
            raise BrowserCommandError("Final order placement requires --confirm.")
        clicked = self.eval_json(
            _click_by_text_script(
                [
                    "Оплатить",
                    "Подтвердить заказ",
                    "Оформить заказ",
                    "Place order",
                    "Pay",
                ]
            )
        )
        if not clicked.get("clicked"):
            raise BrowserCommandError("Could not find a final place-order button on the current Ozon page.")
        self.run("wait", "6000")
        return {
            "ok": True,
            "placed": True,
            "action": clicked,
            "current_url": self.run("get", "url"),
        }

    def close(self) -> dict[str, Any]:
        self.run("close")
        return {"ok": True, "closed": True, "session": self.config.session}

    def eval_json(self, script: str) -> dict[str, Any]:
        raw = self.run("eval", script)
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise BrowserCommandError(f"Browser eval did not return JSON: {raw}") from exc
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError as exc:
                raise BrowserCommandError(f"Browser eval returned nested non-JSON text: {payload}") from exc
        if not isinstance(payload, dict):
            raise BrowserCommandError(f"Browser eval returned unexpected payload type: {type(payload).__name__}")
        return payload

    def run(self, *args: str) -> str:
        command = self._command(*args)
        result = self._run_subprocess(command)
        if args and args[0] != "close" and self._should_restart_daemon(result):
            self._close_daemon()
            result = self._run_subprocess(command)
        if result.returncode != 0:
            stderr = (result.stderr or result.stdout).strip()
            raise BrowserCommandError(stderr or f"agent-browser failed: {' '.join(args)}")
        return result.stdout.strip()

    def _run_subprocess(self, command: list[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            command,
            cwd=self.config.repo_root,
            capture_output=True,
            text=True,
            check=False,
        )

    def _close_daemon(self) -> None:
        close_command = ["npx", "agent-browser", "--session", self.config.session, "close"]
        self._run_subprocess(close_command)

    @staticmethod
    def _should_restart_daemon(result: subprocess.CompletedProcess[str]) -> bool:
        combined = "\n".join(part.strip() for part in (result.stdout, result.stderr) if part).lower()
        return "ignored: daemon already running" in combined

    def _command(self, *args: str) -> list[str]:
        command = [
            "npx",
            "agent-browser",
            "--session",
            self.config.session,
            "--profile",
            str(self.config.profile_path),
            "--download-path",
            str(self.config.download_path),
            "--allowed-domains",
            self.config.allow_domains,
            "--max-output",
            str(self.config.max_output),
        ]
        if self.config.provider:
            command.extend(["--provider", self.config.provider])
        if self.config.proxy:
            command.extend(["--proxy", self.config.proxy])
        if self.config.cdp:
            command.extend(["--cdp", self.config.cdp])
        if self.config.headed:
            command.append("--headed")
        if self.config.user_agent:
            command.extend(["--user-agent", self.config.user_agent])
        if self.config.extra_args:
            command.extend(["--args", ",".join(self.config.extra_args)])
        command.extend(args)
        return command

    @staticmethod
    def _looks_logged_out(payload: dict[str, Any], current_url: str) -> bool:
        page_text = (payload.get("page_text") or "").lower()
        if "/auth" in current_url or "/login" in current_url:
            return True
        signals = ("войти", "вход", "login", "sign in")
        return any(signal in page_text for signal in signals) and not payload.get("orders")

    @staticmethod
    def _looks_access_blocked(payload: dict[str, Any]) -> bool:
        page_text = (payload.get("page_text") or "").lower()
        return "доступ ограничен" in page_text or "access restricted" in page_text

    @staticmethod
    def _extract_incident(payload: dict[str, Any]) -> str | None:
        page_text = str(payload.get("page_text") or "")
        match = re.search(r"Инцидент:\s*([A-Za-z0-9_:-]+)", page_text)
        return match.group(1) if match else None


def _compact_text(value: str) -> str:
    return " ".join(value.split())


def _extract_price_rub(text: str) -> int | None:
    match = PRICE_RE.search(text)
    if not match:
        return None
    digits = re.sub(r"\D", "", match.group(1))
    return int(digits) if digits else None


def _normalize_search_payload(payload: dict[str, Any]) -> dict[str, Any]:
    items = []
    for item in payload.get("items", []):
        title = _compact_text(str(item.get("title") or ""))
        raw_text = _compact_text(str(item.get("raw_text") or ""))
        price = item.get("price_rub")
        if price is None:
            price = _extract_price_rub(raw_text)
        if not title and not raw_text:
            continue
        items.append(
            {
                "title": title or raw_text[:180],
                "url": item.get("url"),
                "price_rub": price,
                "raw_text": raw_text,
            }
        )
    payload["items"] = items
    return payload


def _normalize_orders_payload(payload: dict[str, Any]) -> dict[str, Any]:
    orders = []
    for item in payload.get("orders", []):
        raw_text = _compact_text(str(item.get("raw_text") or ""))
        if not raw_text:
            continue
        order_number = item.get("order_number")
        if not order_number:
            match = re.search(r"(?:заказ|order)\s*[№#]?\s*([A-Z0-9-]{4,})", raw_text, re.IGNORECASE)
            order_number = match.group(1) if match else None
        orders.append(
            {
                "order_number": order_number,
                "status": _compact_text(str(item.get("status") or "")) or None,
                "url": item.get("url"),
                "raw_text": raw_text,
            }
        )
    payload["orders"] = orders
    payload["page_text"] = _compact_text(str(payload.get("page_text") or ""))
    return payload


def _search_extract_script(limit: int) -> str:
    return f"""
(() => {{
  const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
  const isProductLink = (href) => href.includes('/product/');
  const anchors = Array.from(document.querySelectorAll('a[href]')).filter((node) => isProductLink(node.href));
  const items = [];
  for (const anchor of anchors) {{
    const container = anchor.closest('article, section, li, div') || anchor.parentElement || anchor;
    const title = normalize(anchor.textContent);
    const rawText = normalize(container.innerText);
    if (!title && !rawText) continue;
    items.push({{
      title,
      url: anchor.href,
      raw_text: rawText,
    }});
    if (items.length >= {limit}) break;
  }}
  return JSON.stringify({{
    items,
    page_text: normalize(document.body.innerText).slice(0, 4000),
  }});
}})()
""".strip()


def _orders_extract_script() -> str:
    return """
(() => {
  const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
  const bodyText = normalize(document.body.innerText);
  const anchors = Array.from(document.querySelectorAll('a[href]'))
    .filter((node) => /order|orders|my\\//i.test(node.href));
  const orders = [];
  for (const anchor of anchors) {
    const container = anchor.closest('article, section, li, div') || anchor.parentElement || anchor;
    const rawText = normalize(container.innerText);
    if (!rawText) continue;
    const lines = rawText.split(/(?<=[.!?])\\s+|\\s{2,}/).map((line) => line.trim()).filter(Boolean);
    orders.push({
      url: anchor.href,
      raw_text: rawText,
      status: lines[1] || null,
      order_number: lines[0] || null,
    });
  }
  return JSON.stringify({
    orders,
    page_text: bodyText.slice(0, 6000),
  });
})()
""".strip()


def _click_by_text_script(candidates: list[str]) -> str:
    payload = json.dumps(candidates, ensure_ascii=False)
    return f"""
(() => {{
  const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim().toLowerCase();
  const targets = new Set({payload}.map((value) => normalize(value)));
  const nodes = Array.from(document.querySelectorAll('button, a, [role="button"], input[type="submit"]'));
  for (const node of nodes) {{
    const text = normalize(node.innerText || node.textContent || node.value || node.getAttribute('aria-label') || '');
    if (!text) continue;
    if (!targets.has(text) && !Array.from(targets).some((target) => text.includes(target))) continue;
    node.click();
    return JSON.stringify({{
      clicked: true,
      text,
      tag: node.tagName.toLowerCase(),
    }});
  }}
  return JSON.stringify({{ clicked: false, searched: Array.from(targets) }});
}})()
""".strip()


def inspect_setup(repo_root: Path | None = None, platform_name: str | None = None) -> SetupStatus:
    repo = (repo_root or Path(__file__).resolve().parent.parent).resolve()
    platform_key = platform_name or sys.platform
    state_root = _default_state_root() / "iron-lady-assistant" / "ozon-browser"
    scripts_dir = repo / "scripts"

    checks = {
        "node": shutil.which("node") is not None,
        "npm": shutil.which("npm") is not None,
        "npx": shutil.which("npx") is not None,
        "repo_agent_browser": (repo / "node_modules" / ".bin" / "agent-browser").exists(),
        "playwright_cache": (Path.home() / ".cache" / "ms-playwright").exists(),
        "linux_host_installer": (scripts_dir / "install_agent_browser_host.sh").exists(),
        "linux_manual_cdp": (scripts_dir / "start_ozon_chrome_display.sh").exists(),
        "macos_manual_cdp": (scripts_dir / "start_ozon_chrome_macos.sh").exists(),
    }

    commands = [f"cd {repo}"]
    notes = [
        f"Ozon browser state root: {state_root}",
        "The setup command is inspection-only; it does not launch Chrome or modify root-owned packages.",
    ]

    if platform_key == "darwin":
        recommended_path = "macos_cdp"
        if not checks["repo_agent_browser"]:
            commands.append("npm install")
        commands.extend(
            [
                "./scripts/start_ozon_chrome_macos.sh",
                "python3 -m src.ozon_browser --cdp 9222 --session ozon login",
                "python3 -m src.ozon_browser --cdp 9222 --session ozon orders",
            ]
        )
        notes.append("Recommended path on macOS: launch a normal Chrome profile and attach over CDP.")
        ok = checks["node"] and checks["npm"] and checks["npx"] and checks["macos_manual_cdp"]
        return SetupStatus(
            ok=ok,
            platform=platform_key,
            recommended_path=recommended_path,
            repo_root=repo,
            state_root=state_root,
            checks=checks,
            commands=commands,
            notes=notes,
        )

    if platform_key.startswith("linux"):
        if checks["repo_agent_browser"] and checks["playwright_cache"]:
            recommended_path = "linux_manual_cdp"
            commands.extend(
                [
                    "./scripts/start_ozon_chrome_display.sh",
                    "python3 -m src.ozon_browser --cdp 9222 --session ozon login",
                    "python3 -m src.ozon_browser --cdp 9222 --session ozon orders",
                ]
            )
            notes.append("Repo-local agent-browser assets are present, so the manual Chrome + CDP path is the shortest route.")
            ok = checks["node"] and checks["npm"] and checks["npx"] and checks["linux_manual_cdp"]
        else:
            recommended_path = "linux_host_prepare"
            commands.extend(
                [
                    "bash scripts/install_agent_browser_host.sh",
                    "./scripts/start_ozon_chrome_display.sh",
                    "python3 -m src.ozon_browser --cdp 9222 --session ozon login",
                    "python3 -m src.ozon_browser --cdp 9222 --session ozon orders",
                ]
            )
            notes.append("Repo-local browser bits are incomplete, so start with the host installer before manual Chrome + CDP attach.")
            ok = (
                checks["node"]
                and checks["npm"]
                and checks["npx"]
                and checks["linux_host_installer"]
                and checks["linux_manual_cdp"]
            )
        return SetupStatus(
            ok=ok,
            platform=platform_key,
            recommended_path=recommended_path,
            repo_root=repo,
            state_root=state_root,
            checks=checks,
            commands=commands,
            notes=notes,
        )

    recommended_path = "unsupported_platform"
    notes.append(f"Unsupported platform for the built-in Ozon setup helpers: {platform_key}")
    return SetupStatus(
        ok=False,
        platform=platform_key,
        recommended_path=recommended_path,
        repo_root=repo,
        state_root=state_root,
        checks=checks,
        commands=commands,
        notes=notes,
    )


def _setup_to_payload(status: SetupStatus) -> dict[str, Any]:
    return {
        "ok": status.ok,
        "platform": status.platform,
        "recommended_path": status.recommended_path,
        "repo_root": str(status.repo_root),
        "state_root": str(status.state_root),
        "checks": status.checks,
        "commands": status.commands,
        "notes": status.notes,
    }


def _format_setup_text(status: SetupStatus) -> str:
    lines = [
        f"platform: {status.platform}",
        f"recommended_path: {status.recommended_path}",
        f"ok: {'yes' if status.ok else 'no'}",
        f"repo_root: {status.repo_root}",
        f"state_root: {status.state_root}",
        "",
        "checks:",
    ]
    for key in sorted(status.checks):
        lines.append(f"- {key}: {'yes' if status.checks[key] else 'no'}")
    lines.extend(["", "next_commands:"])
    for command in status.commands:
        lines.append(f"- {command}")
    if status.notes:
        lines.extend(["", "notes:"])
        for note in status.notes:
            lines.append(f"- {note}")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ozon automation wrapper built on top of agent-browser.")
    parser.add_argument("--provider", help="agent-browser provider: browseruse, kernel, browserbase, ios")
    parser.add_argument("--proxy", help="Browser proxy URL passed through to agent-browser, e.g. socks5://127.0.0.1:11080")
    parser.add_argument("--cdp", help="Attach to an already-running Chrome/Chromium via CDP port or endpoint.")
    parser.add_argument("--session", default=os.environ.get("OZON_SESSION", DEFAULT_SESSION))
    parser.add_argument("--profile-path", type=Path)
    parser.add_argument("--download-path", type=Path)
    parser.add_argument(
        "--allowed-domains",
        default=DEFAULT_ALLOWED_DOMAINS,
        help="Comma-separated allowed domains. Use '*' to disable the wrapper allowlist.",
    )
    parser.add_argument("--headed", action="store_true", help="Show the browser window instead of headless mode.")
    parser.add_argument("--max-output", type=int, default=12000)
    subparsers = parser.add_subparsers(dest="command", required=True)

    setup_parser = subparsers.add_parser("setup", help="Inspect local Ozon browser prerequisites and print the recommended path.")
    setup_parser.add_argument("--format", choices=SETUP_FORMATS, default="text")

    subparsers.add_parser("login", help="Open the Ozon account page for manual login.")
    subparsers.add_parser("orders", help="Fetch current order statuses from the logged-in Ozon session.")

    search_parser = subparsers.add_parser("search", help="Search Ozon products.")
    search_parser.add_argument("query")
    search_parser.add_argument("--max-price", type=int)
    search_parser.add_argument("--limit", type=int, default=5)

    prepare_parser = subparsers.add_parser("prepare-buy", help="Search, pick the best match, and add it to cart.")
    prepare_parser.add_argument("query")
    prepare_parser.add_argument("--max-price", type=int)
    prepare_parser.add_argument("--quantity", type=int, default=1)
    prepare_parser.add_argument(
        "--checkout",
        action="store_true",
        help="Try to enter the cart/checkout flow after adding the item to cart.",
    )

    place_parser = subparsers.add_parser("place-order", help="Click the final order button on the current checkout page.")
    place_parser.add_argument("--confirm", action="store_true", help="Required to place the order.")

    subparsers.add_parser("close", help="Close the browser session.")
    return parser


def _resolve_config(args: argparse.Namespace) -> BrowserConfig:
    repo_root = Path(__file__).resolve().parent.parent
    state_root = _default_state_root() / "iron-lady-assistant" / "ozon-browser"
    profile_path = args.profile_path or state_root / "profile"
    download_path = args.download_path or state_root / "downloads"
    profile_path.mkdir(parents=True, exist_ok=True)
    download_path.mkdir(parents=True, exist_ok=True)
    return BrowserConfig(
        repo_root=repo_root,
        profile_path=profile_path,
        download_path=download_path,
        session=args.session,
        provider=(args.provider or None),
        proxy=(args.proxy or None),
        cdp=(args.cdp or None),
        headed=bool(args.headed),
        allow_domains=args.allowed_domains,
        max_output=args.max_output,
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "setup":
        status = inspect_setup()
        if args.format == "json":
            print(json.dumps(_setup_to_payload(status), ensure_ascii=False, indent=2))
        else:
            print(_format_setup_text(status))
        return 0

    ozon = OzonBrowser(_resolve_config(args))

    try:
        if args.command == "login":
            payload = ozon.open_login()
        elif args.command == "orders":
            payload = _normalize_orders_payload(ozon.fetch_order_statuses())
        elif args.command == "search":
            payload = _normalize_search_payload(
                ozon.search_products(query=args.query, max_price=args.max_price, limit=args.limit)
            )
        elif args.command == "prepare-buy":
            payload = ozon.prepare_buy(
                query=args.query,
                max_price=args.max_price,
                quantity=args.quantity,
                checkout=args.checkout,
            )
        elif args.command == "place-order":
            payload = ozon.place_order(confirm=bool(args.confirm))
        elif args.command == "close":
            payload = ozon.close()
        else:
            parser.error(f"Unsupported command: {args.command}")
            return 2
    except BrowserCommandError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        return 1

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
