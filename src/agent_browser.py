from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_ALLOWED_DOMAINS = "*"
DEFAULT_SESSION = "default"
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


class AgentBrowser:
    def __init__(self, config: BrowserConfig) -> None:
        self.config = config

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


def inspect_setup(repo_root: Path | None = None, platform_name: str | None = None) -> SetupStatus:
    repo = (repo_root or Path(__file__).resolve().parent.parent).resolve()
    platform_key = platform_name or sys.platform
    state_root = _default_state_root() / "iron-lady-assistant" / "agent-browser"
    scripts_dir = repo / "scripts"

    checks = {
        "node": shutil.which("node") is not None,
        "npm": shutil.which("npm") is not None,
        "npx": shutil.which("npx") is not None,
        "repo_agent_browser": (repo / "node_modules" / ".bin" / "agent-browser").exists(),
        "playwright_cache": (Path.home() / ".cache" / "ms-playwright").exists(),
        "linux_host_installer": (scripts_dir / "install_agent_browser_host.sh").exists(),
    }

    commands = [f"cd {repo}"]
    notes = [
        f"Agent-browser state root: {state_root}",
        "This wrapper is for a dedicated automation browser. Use browser_takeover when you need the user's real Chrome tab via the relay/extension.",
    ]

    if platform_key == "darwin":
        recommended_path = "macos_repo_local"
        if not checks["repo_agent_browser"]:
            commands.append("npm install")
        commands.extend(
            [
                "npx agent-browser install",
                "python3 -m src.agent_browser --headed open https://example.com",
                "python3 -m src.agent_browser snapshot",
            ]
        )
        notes.append("Recommended path on macOS: use the repo-local agent-browser install and let it manage its own browser.")
        ok = checks["node"] and checks["npm"] and checks["npx"]
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
            recommended_path = "linux_repo_local"
            commands.extend(
                [
                    "python3 -m src.agent_browser open https://example.com",
                    "python3 -m src.agent_browser snapshot",
                ]
            )
            notes.append("Repo-local agent-browser assets are present, so you can use the generic wrapper immediately.")
            ok = checks["node"] and checks["npm"] and checks["npx"]
        else:
            recommended_path = "linux_host_prepare"
            commands.extend(
                [
                    "bash scripts/install_agent_browser_host.sh",
                    "python3 -m src.agent_browser open https://example.com",
                    "python3 -m src.agent_browser snapshot",
                ]
            )
            notes.append("Start with the host installer to provision agent-browser and browser runtime dependencies.")
            ok = checks["node"] and checks["npm"] and checks["npx"] and checks["linux_host_installer"]
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
    notes.append(f"Unsupported platform for the built-in agent-browser helper: {platform_key}")
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
    parser = argparse.ArgumentParser(description="Generic automation wrapper built on top of agent-browser.")
    parser.add_argument("--provider", help="agent-browser provider: browseruse, kernel, browserbase, ios")
    parser.add_argument("--proxy", help="Browser proxy URL passed through to agent-browser, e.g. socks5://127.0.0.1:11080")
    parser.add_argument("--cdp", help="Attach to an already-running Chrome/Chromium via CDP port or endpoint.")
    parser.add_argument("--session", default=os.environ.get("AGENT_BROWSER_SESSION", DEFAULT_SESSION))
    parser.add_argument("--profile-path", type=Path)
    parser.add_argument("--download-path", type=Path)
    parser.add_argument(
        "--allowed-domains",
        default=DEFAULT_ALLOWED_DOMAINS,
        help="Comma-separated allowed domains. Use '*' to disable the wrapper allowlist.",
    )
    parser.add_argument("--headed", action="store_true", help="Show the browser window instead of headless mode.")
    parser.add_argument("--max-output", type=int, default=12000)
    parser.add_argument("--user-agent", help="Optional user agent passed through to agent-browser.")
    parser.add_argument(
        "--browser-arg",
        dest="browser_args",
        action="append",
        default=None,
        help="Extra browser launch arg passed through to agent-browser. Repeat to add more than one.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    setup_parser = subparsers.add_parser("setup", help="Inspect local agent-browser prerequisites and print the recommended path.")
    setup_parser.add_argument("--format", choices=SETUP_FORMATS, default="text")

    open_parser = subparsers.add_parser("open", help="Open a URL.")
    open_parser.add_argument("url")

    subparsers.add_parser("snapshot", help="Capture the current accessibility snapshot.")

    click_parser = subparsers.add_parser("click", help="Click a selector or element ref.")
    click_parser.add_argument("selector")

    type_parser = subparsers.add_parser("type", help="Type text into the focused element or target selector.")
    type_parser.add_argument("selector")
    type_parser.add_argument("text")

    fill_parser = subparsers.add_parser("fill", help="Fill an input-like selector with text.")
    fill_parser.add_argument("selector")
    fill_parser.add_argument("value")

    wait_parser = subparsers.add_parser("wait", help="Wait for a selector, text, or a time in milliseconds.")
    wait_group = wait_parser.add_mutually_exclusive_group(required=True)
    wait_group.add_argument("--selector")
    wait_group.add_argument("--text")
    wait_group.add_argument("--ms", type=int)

    get_parser = subparsers.add_parser("get", help="Read browser state.")
    get_parser.add_argument("kind", choices=("url", "title", "text"))
    get_parser.add_argument("selector", nargs="?")

    eval_parser = subparsers.add_parser("eval", help="Run JavaScript in the page context.")
    eval_parser.add_argument("script")

    screenshot_parser = subparsers.add_parser("screenshot", help="Capture a screenshot.")
    screenshot_parser.add_argument("output", nargs="?")
    screenshot_parser.add_argument("--full-page", action="store_true")

    raw_parser = subparsers.add_parser("raw", help="Pass a raw command through to agent-browser.")
    raw_parser.add_argument("args", nargs=argparse.REMAINDER)

    subparsers.add_parser("close", help="Close the browser session.")
    return parser


def _resolve_config(args: argparse.Namespace) -> BrowserConfig:
    repo_root = Path(__file__).resolve().parent.parent
    state_root = _default_state_root() / "iron-lady-assistant" / "agent-browser" / args.session
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
        user_agent=(args.user_agent or None),
        extra_args=tuple(args.browser_args or ()),
    )


def _run_command(browser: AgentBrowser, args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "open":
        output = browser.run("open", args.url)
        return {"ok": True, "command": "open", "url": args.url, "output": output}
    if args.command == "snapshot":
        output = browser.run("snapshot")
        return {"ok": True, "command": "snapshot", "output": output}
    if args.command == "click":
        output = browser.run("click", args.selector)
        return {"ok": True, "command": "click", "selector": args.selector, "output": output}
    if args.command == "type":
        output = browser.run("type", args.selector, args.text)
        return {
            "ok": True,
            "command": "type",
            "selector": args.selector,
            "text": args.text,
            "output": output,
        }
    if args.command == "fill":
        output = browser.run("fill", args.selector, args.value)
        return {
            "ok": True,
            "command": "fill",
            "selector": args.selector,
            "value": args.value,
            "output": output,
        }
    if args.command == "wait":
        if args.selector:
            output = browser.run("wait", args.selector)
            return {"ok": True, "command": "wait", "selector": args.selector, "output": output}
        if args.text:
            output = browser.run("wait", "--text", args.text)
            return {"ok": True, "command": "wait", "text": args.text, "output": output}
        output = browser.run("wait", str(args.ms))
        return {"ok": True, "command": "wait", "ms": args.ms, "output": output}
    if args.command == "get":
        command = ["get", args.kind]
        if args.selector:
            command.append(args.selector)
        output = browser.run(*command)
        payload: dict[str, Any] = {"ok": True, "command": "get", "kind": args.kind, "output": output}
        if args.selector:
            payload["selector"] = args.selector
        return payload
    if args.command == "eval":
        output = browser.run("eval", args.script)
        return {"ok": True, "command": "eval", "output": output}
    if args.command == "screenshot":
        command = ["screenshot"]
        if args.output:
            command.append(args.output)
        if args.full_page:
            command.append("--full")
        output = browser.run(*command)
        payload = {"ok": True, "command": "screenshot", "output": output, "full_page": bool(args.full_page)}
        if args.output:
            payload["path"] = args.output
        return payload
    if args.command == "raw":
        raw_args = list(args.args)
        if raw_args and raw_args[0] == "--":
            raw_args = raw_args[1:]
        if not raw_args:
            raise BrowserCommandError("raw requires at least one argument after '--'")
        output = browser.run(*raw_args)
        return {"ok": True, "command": "raw", "args": raw_args, "output": output}
    if args.command == "close":
        output = browser.run("close")
        return {"ok": True, "command": "close", "output": output}
    raise BrowserCommandError(f"Unsupported command: {args.command}")


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

    browser = AgentBrowser(_resolve_config(args))
    try:
        payload = _run_command(browser, args)
    except BrowserCommandError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        return 1

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
