"""Registry for prompt context plugins."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class ContextPlugin(Protocol):
    """Minimal interface for prompt context extensions."""

    name: str

    def build_context(self, user_message: str) -> str:
        """Return context fragment for a user message."""

    def format_for_display(self) -> str:
        """Return human-readable plugin details for bot commands."""


@dataclass
class ContextPluginRegistry:
    """Collects context fragments from registered plugins."""

    plugins: list[ContextPlugin]

    def build_context(self, user_message: str) -> str:
        blocks = []
        for plugin in self.plugins:
            block = plugin.build_context(user_message).strip()
            if block:
                blocks.append(block)
        return "\n\n".join(blocks)

