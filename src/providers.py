"""Provider fallback manager.

Loads a chain of LLM providers from ``providers.json`` and automatically
switches to the next one when a rate-limit (or similar) error is detected.

Each provider is a dict with ``name``, ``description``, optional ``cli`` (the
backend executable, e.g. ``claude`` or ``codex``), optional ``model``/``models``,
optional ``resume_arg`` (CLI flag to resume a session), and ``env`` (extra
environment variables passed to the subprocess). The first provider with an
empty ``env`` is assumed to be the native Anthropic backend.
"""

import asyncio
import getpass
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from threading import Thread
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).resolve().parent.parent / "providers.json"
_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPTS_BIN_DIR = _REPO_ROOT / "scripts"


def _normalized_subprocess_path(existing_path: str) -> str:
    """Return PATH with common user-local bin dirs prepended deterministically."""
    existing_parts = [part for part in (existing_path or "").split(os.pathsep) if part]
    user = getpass.getuser()
    home = Path.home()
    candidate_parts = [
        str(_SCRIPTS_BIN_DIR),
        str(home / f".npm-{user}" / "bin"),
        str(home / ".local" / "bin"),
        "/usr/local/bin",
        "/usr/bin",
        "/bin",
    ]
    merged_parts: list[str] = []
    for part in candidate_parts + existing_parts:
        if not part or part in merged_parts:
            continue
        if part.startswith("/usr/") or part == "/bin" or Path(part).is_dir():
            merged_parts.append(part)
    return os.pathsep.join(merged_parts)


class _ConfigFileWatcher:
    """Watches providers.json for changes and reloads ProviderManager."""

    def __init__(self, provider_manager: "ProviderManager") -> None:
        self._manager = provider_manager
        self._observer = Observer()

    def start(self) -> None:
        event_handler = _ConfigEventHandler(self._manager)
        self._observer.schedule(event_handler, str(_CONFIG_PATH.parent))
        self._observer.start()
        logger.info(" Started watching %s for changes", _CONFIG_PATH.name)

    def stop(self) -> None:
        if self._observer.is_alive():
            self._observer.stop()
            self._observer.join()


class _ConfigEventHandler(FileSystemEventHandler):
    """Handles filesystem events for providers.json."""

    def __init__(self, provider_manager: "ProviderManager") -> None:
        super().__init__()
        self._manager = provider_manager
        self._last_reload = 0.0

    def on_modified(self, event) -> None:
        if event.src_path != str(_CONFIG_PATH):
            return
        # Debounce: ignore rapid successive events
        now = time.time()
        if now - self._last_reload < 0.5:
            return
        self._last_reload = now
        self._manager.reload()


@dataclass
class Provider:
    name: str
    description: str
    cli: str = "claude"
    model: str | None = None
    models: list[str] | None = None
    resume_arg: str | None = None
    env: dict[str, str] = field(default_factory=dict)

    def __str__(self) -> str:
        env_str = ", ".join(f"{k}={v}" for k, v in self.env.items()) or "(default env)"
        model_str = f", model={self.model}" if self.model else ""
        return f"Provider({self.name}, cli={self.cli}{model_str}, {env_str})"


@dataclass
class _ProviderConfig:
    providers: list[Provider]
    rate_limit_patterns: list[re.Pattern[str]]
    cooldown_minutes: int


def _expand_env_values(env: dict | None) -> dict[str, str]:
    """Expand ``${VAR}``/``$VAR`` references in provider env values from os.environ.

    Lets secrets (e.g. proxy API keys) live in the bot's ``.env`` instead of the
    git-tracked ``providers.json``. Unset references are left verbatim.
    """
    expanded: dict[str, str] = {}
    for key, value in (env or {}).items():
        expanded[key] = os.path.expandvars(value) if isinstance(value, str) else value
    return expanded


def _load_config() -> _ProviderConfig:
    """Load and parse providers.json."""
    if not _CONFIG_PATH.exists():
        logger.info("No providers.json found — using defaults (claude only)")
        return _ProviderConfig(
            providers=[Provider(name="claude", description="Anthropic Claude")],
            rate_limit_patterns=[],
            cooldown_minutes=30,
        )

    with open(_CONFIG_PATH) as f:
        raw = json.load(f)

    providers = [
        Provider(
            name=p["name"],
            description=p.get("description", p["name"]),
            cli=p.get("cli", "claude"),
            model=p.get("model"),
            models=p.get("models"),
            resume_arg=p.get("resume_arg"),
            env=_expand_env_values(p.get("env", {})),
        )
        for p in raw.get("providers", [])
    ]
    if not providers:
        providers = [Provider(name="claude", description="Anthropic Claude")]

    patterns = [
        re.compile(pat, re.IGNORECASE)
        for pat in raw.get("rate_limit_patterns", [])
    ]

    return _ProviderConfig(
        providers=providers,
        rate_limit_patterns=patterns,
        cooldown_minutes=int(raw.get("cooldown_minutes", 30)),
    )


class ProviderManager:
    """Manages provider fallback chain with per-chat state.

    Usage::

        mgr = ProviderManager()
        provider = mgr.get_provider(chat_id)  # returns current provider
        if mgr.is_rate_limit_error(error_text):
            next_prov = mgr.advance(chat_id)   # switch to next fallback
    """

    def __init__(self, watch_config: bool = True) -> None:
        self._cfg = _load_config()
        # scope_key → index in self._cfg.providers
        self._chat_provider_idx: dict[int | str, int] = {}
        # scope_key → timestamp when fallback was activated
        self._fallback_since: dict[int | str, float] = {}
        self._watcher: _ConfigFileWatcher | None = None

        if watch_config and _CONFIG_PATH.exists():
            self._watcher = _ConfigFileWatcher(self)
            self._watcher.start()

        logger.info(
            "Loaded %d providers: %s",
            len(self._cfg.providers),
            ", ".join(str(p) for p in self._cfg.providers),
        )

    def shutdown(self) -> None:
        """Stop the config file watcher."""
        if self._watcher:
            self._watcher.stop()
            self._watcher = None

    def reload(self) -> None:
        """Reload providers.json from disk."""
        self._cfg = _load_config()
        logger.info("Reloaded providers: %s",
                     ", ".join(p.name for p in self._cfg.providers))

    @property
    def providers(self) -> list[Provider]:
        return self._cfg.providers

    def get_provider(self, chat_id: int | str) -> Provider:
        """Return the current provider for a chat (NO auto-recovery to prevent unexpected switches)."""
        idx = self._chat_provider_idx.get(chat_id, 0)

        # No auto-recovery - provider only changes on explicit user interaction
        # via /provider command or provider manager reset method

        return self._cfg.providers[idx]

    def advance(self, chat_id: int | str) -> Provider | None:
        """Move to the next provider in the chain.

        Returns the new provider, or ``None`` if we've exhausted all fallbacks.
        """
        current_idx = self._chat_provider_idx.get(chat_id, 0)
        next_idx = current_idx + 1

        if next_idx >= len(self._cfg.providers):
            logger.warning("Chat %s: all providers exhausted", chat_id)
            return None

        self._chat_provider_idx[chat_id] = next_idx
        self._fallback_since[chat_id] = time.monotonic()
        provider = self._cfg.providers[next_idx]
        logger.info("Chat %s: switched to provider '%s'", chat_id, provider.name)
        return provider

    def reset(self, chat_id: int | str) -> Provider:
        """Reset to primary provider."""
        self._chat_provider_idx.pop(chat_id, None)
        self._fallback_since.pop(chat_id, None)
        return self._cfg.providers[0]

    def set_provider(self, chat_id: int | str, name: str) -> Provider | None:
        """Manually select a provider by name."""
        for i, p in enumerate(self._cfg.providers):
            if p.name.lower() == name.lower():
                self._chat_provider_idx[chat_id] = i
                if i > 0:
                    self._fallback_since[chat_id] = time.monotonic()
                else:
                    self._fallback_since.pop(chat_id, None)
                logger.info("Chat %s: manually set provider to '%s'", chat_id, p.name)
                return p
        return None

    def is_rate_limit_error(self, text: str) -> bool:
        """Check if an error message indicates a rate limit / quota issue."""
        if not text:
            return False
        return any(pat.search(text) for pat in self._cfg.rate_limit_patterns)

    def subprocess_env(self, provider: Provider) -> dict[str, str]:
        """Build subprocess environment with provider's env vars applied."""
        env = os.environ.copy()
        env.pop("CLAUDECODE", None)
        env["PATH"] = _normalized_subprocess_path(env.get("PATH", ""))
        env.setdefault("ILA_REPO_ROOT", str(_REPO_ROOT))
        env.update(provider.env)
        return env


def is_codex_cli(cli_name: str | None) -> bool:
    return bool(cli_name and cli_name.lower().startswith("codex"))


def codex_family_providers(providers: list[Provider] | list[object]) -> list[object]:
    return [provider for provider in providers if is_codex_cli(getattr(provider, "cli", None))]
