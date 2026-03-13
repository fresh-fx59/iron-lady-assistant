import subprocess
from pathlib import Path

import pytest

from src.ozon_browser import (
    BrowserCommandError,
    BrowserConfig,
    OzonBrowser,
    _extract_price_rub,
    _normalize_orders_payload,
    _normalize_search_payload,
    main,
)


def test_extract_price_rub_parses_ruble_price() -> None:
    assert _extract_price_rub("Цена 1 299 ₽ со скидкой") == 1299
    assert _extract_price_rub("No price here") is None


def test_normalize_search_payload_compacts_text_and_price() -> None:
    payload = _normalize_search_payload(
        {
            "items": [
                {
                    "title": "  Детский   шампунь ",
                    "url": "https://www.ozon.ru/product/1",
                    "raw_text": "Детский шампунь 699 ₽ быстрая доставка",
                }
            ]
        }
    )

    assert payload["items"][0]["title"] == "Детский шампунь"
    assert payload["items"][0]["price_rub"] == 699


def test_normalize_orders_payload_extracts_order_number_and_status() -> None:
    payload = _normalize_orders_payload(
        {
            "orders": [
                {
                    "raw_text": "Заказ №123ABC Передан в доставку курьером",
                    "url": "https://www.ozon.ru/my/order/123ABC",
                }
            ]
        }
    )

    assert payload["orders"][0]["order_number"] == "123ABC"
    assert payload["orders"][0]["status"] is None


def test_place_order_requires_confirmation(tmp_path: Path) -> None:
    browser = OzonBrowser(
        BrowserConfig(
            repo_root=tmp_path,
            profile_path=tmp_path / "profile",
            download_path=tmp_path / "downloads",
        )
    )

    with pytest.raises(BrowserCommandError):
        browser.place_order(confirm=False)


def test_eval_json_decodes_nested_json_string(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    browser = OzonBrowser(
        BrowserConfig(
            repo_root=tmp_path,
            profile_path=tmp_path / "profile",
            download_path=tmp_path / "downloads",
        )
    )

    monkeypatch.setattr(browser, "run", lambda *_args: '"{\\"orders\\": [], \\"page_text\\": \\"Войти\\"}"')

    assert browser.eval_json("ignored") == {"orders": [], "page_text": "Войти"}


def test_command_includes_proxy_cdp_and_allowed_domains(tmp_path: Path) -> None:
    browser = OzonBrowser(
        BrowserConfig(
            repo_root=tmp_path,
            profile_path=tmp_path / "profile",
            download_path=tmp_path / "downloads",
            proxy="socks5://127.0.0.1:11080",
            cdp="9222",
            allow_domains="*",
        )
    )

    command = browser._command("open", "https://www.ozon.ru/my/")

    assert "--proxy" in command
    assert "socks5://127.0.0.1:11080" in command
    assert "--cdp" in command
    assert "9222" in command
    assert "--allowed-domains" in command
    assert "*" in command


def test_fetch_order_statuses_raises_for_access_block(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    browser = OzonBrowser(
        BrowserConfig(
            repo_root=tmp_path,
            profile_path=tmp_path / "profile",
            download_path=tmp_path / "downloads",
        )
    )

    state = {"current_url": "https://www.ozon.ru/my/orderlist"}

    def fake_run(*args: str) -> str:
        if args[:2] == ("get", "url"):
            return state["current_url"]
        return ""

    monkeypatch.setattr(browser, "run", fake_run)
    monkeypatch.setattr(
        browser,
        "eval_json",
        lambda _script: {
            "orders": [],
            "page_text": "Доступ ограничен Инцидент: fab_i_20260313155140_01KKKYAXDA0GDDK5S8DN99K72F",
        },
    )

    with pytest.raises(BrowserCommandError, match="incident: fab_i_20260313155140_01KKKYAXDA0GDDK5S8DN99K72F"):
        browser.fetch_order_statuses()


def test_run_restarts_daemon_once_when_agent_browser_ignores_profile_options(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    browser = OzonBrowser(
        BrowserConfig(
            repo_root=tmp_path,
            profile_path=tmp_path / "profile",
            download_path=tmp_path / "downloads",
            session="ozon",
        )
    )

    calls: list[list[str]] = []

    def fake_run_subprocess(command: list[str]):
        calls.append(command)
        if len(calls) == 1:
            return subprocess.CompletedProcess(
                command,
                0,
                stdout="⚠ --profile ignored: daemon already running. Use 'agent-browser close' first to restart with new options.\nblocked",
                stderr="",
            )
        if command[-1] == "close":
            return subprocess.CompletedProcess(command, 0, stdout="✓ Browser closed", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    monkeypatch.setattr(browser, "_run_subprocess", fake_run_subprocess)

    assert browser.run("open", "https://www.ozon.ru/my/") == "ok"
    assert calls[1] == ["npx", "agent-browser", "--session", "ozon", "close"]


def test_main_reports_error_json(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys) -> None:
    def fake_resolve(_args):
        return BrowserConfig(
            repo_root=tmp_path,
            profile_path=tmp_path / "profile",
            download_path=tmp_path / "downloads",
        )

    def fake_orders(self):
        raise BrowserCommandError("not logged in")

    monkeypatch.setattr("src.ozon_browser._resolve_config", fake_resolve)
    monkeypatch.setattr(OzonBrowser, "fetch_order_statuses", fake_orders)

    rc = main(["orders"])

    assert rc == 1
    assert "not logged in" in capsys.readouterr().out


def test_main_accepts_headed_before_subcommand(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys
) -> None:
    seen: dict[str, bool] = {}

    def fake_resolve(args):
        seen["headed"] = bool(args.headed)
        return BrowserConfig(
            repo_root=tmp_path,
            profile_path=tmp_path / "profile",
            download_path=tmp_path / "downloads",
            headed=bool(args.headed),
        )

    def fake_open_login(self):
        return {"ok": True, "headed": self.config.headed}

    monkeypatch.setattr("src.ozon_browser._resolve_config", fake_resolve)
    monkeypatch.setattr(OzonBrowser, "open_login", fake_open_login)

    rc = main(["--headed", "login"])

    assert rc == 0
    assert seen["headed"] is True
    assert '"headed": true' in capsys.readouterr().out
