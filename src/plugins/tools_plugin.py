"""Tool context plugin with lazy loading.

Discovers tool definitions from tools/ directory and injects relevant
tool instructions into the Claude prompt. Two-phase loading:
  Phase 1 (manifest): Always scanned - name, triggers, one-line description
  Phase 2 (full): Loaded on demand - instructions, setup script path

Tools are YAML files in {TOOLS_DIR}/*.yaml with this structure:
  name: web_search
  description: Search the web for current information
  triggers: [search, google, find online, current, latest, news, today]
  instructions: |
    You have web search available via the `websearch` command.
    Usage: websearch "query"
    ...
  setup: tools/bin/websearch  # Optional: path to executable
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


@dataclass
class ToolManifest:
    """Phase 1: lightweight manifest, always in memory."""

    name: str
    description: str
    triggers: list[str]


@dataclass
class ToolDefinition:
    """Phase 2: full tool definition, loaded on demand."""

    manifest: ToolManifest
    instructions: str
    setup_script: str | None


class ToolRegistry:
    """Context plugin that manages tool definitions with lazy loading."""

    name = "tools"

    def __init__(self, tools_dir: Path) -> None:
        self._dir = tools_dir
        self._manifests: list[ToolManifest] = []
        self._manifest_files: dict[str, Path] = {}
        self._cache: dict[str, ToolDefinition] = {}
        self._load_manifests()

    def _load_manifests(self) -> None:
        """Scan YAML files, extract only manifest fields (Phase 1)."""
        if not self._dir.exists():
            logger.debug("Tools directory %s does not exist, no tools loaded", self._dir)
            return

        for yaml_file in sorted(self._dir.glob("*.yaml")):
            try:
                data = yaml.safe_load(yaml_file.read_text())
                if not data:
                    continue
                manifest = ToolManifest(
                    name=data.get("name", yaml_file.stem),
                    description=data.get("description", ""),
                    triggers=data.get("triggers", []),
                )
                self._manifests.append(manifest)
                self._manifest_files[manifest.name] = yaml_file
                logger.debug("Loaded manifest for tool: %s", manifest.name)
            except Exception as exc:
                logger.warning("Failed to load tool from %s: %s", yaml_file, exc)

    def _load_full(self, name: str) -> ToolDefinition | None:
        """Load full tool definition on demand (Phase 2)."""
        if not self._dir.exists():
            return None
        if name in self._cache:
            return self._cache[name]

        yaml_file = self._manifest_files.get(name)
        if not yaml_file:
            for candidate in self._dir.glob("*.yaml"):
                if candidate.stem == name:
                    yaml_file = candidate
                    break
        if not yaml_file:
            logger.warning("Tool definition not found: %s", name)
            return None

        try:
            data = yaml.safe_load(yaml_file.read_text())
            if not data:
                return None
            manifest = ToolManifest(
                name=data.get("name", name),
                description=data.get("description", ""),
                triggers=data.get("triggers", []),
            )
            definition = ToolDefinition(
                manifest=manifest,
                instructions=data.get("instructions", ""),
                setup_script=data.get("setup"),
            )
            self._cache[name] = definition
            logger.debug("Loaded full definition for tool: %s", name)
            return definition
        except Exception as exc:
            logger.warning("Failed to load tool definition %s: %s", name, exc)
            return None

    def match_tools(self, user_message: str) -> list[ToolDefinition]:
        """Match tools by trigger phrase occurrence."""
        msg_lower = user_message.lower()
        matched: list[ToolDefinition] = []
        for manifest in self._manifests:
            for trigger in manifest.triggers:
                if trigger.lower() in msg_lower:
                    full = self._load_full(manifest.name)
                    if full:
                        matched.append(full)
                    break
        return matched

    def build_context(self, user_message: str) -> str:
        """Build XML <tools> block for matched tools."""
        if not self._manifests:
            return ""

        available_lines = [f"- {m.name}: {m.description}" for m in self._manifests]
        available_section = "<available>\n" + "\n".join(available_lines) + "\n</available>"

        matched = self.match_tools(user_message)
        if not matched:
            return f"<tools>\n{available_section}\n</tools>"

        active_lines: list[str] = []
        for tool_def in matched[:3]:
            active_lines.append(f'<tool name="{tool_def.manifest.name}">')
            active_lines.append(tool_def.instructions.strip())
            active_lines.append("</tool>")
        active_section = "<active>\n" + "\n".join(active_lines) + "\n</active>"
        return f"<tools>\n{available_section}\n\n{active_section}\n</tools>"

    def format_for_display(self) -> str:
        """Return HTML-formatted list of available tools for /tools command."""
        if not self._manifests:
            return "<b>No tools configured.</b>\n\nCreate .yaml files in tools/ directory."

        lines = ["<b>Available Tools:</b>"]
        for manifest in self._manifests:
            triggers_str = ", ".join(f'"{t}"' for t in manifest.triggers[:5])
            lines.append(f"\n<b>{manifest.name}</b>")
            lines.append(f"  {manifest.description}")
            lines.append(f"  <i>Triggers:</i> {triggers_str}")
        return "\n".join(lines)
