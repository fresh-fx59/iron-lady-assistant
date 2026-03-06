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
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import yaml

from .. import config

logger = logging.getLogger(__name__)
_USE_TOOL_PATTERN = re.compile(r"^\s*USE_TOOL:\s*([A-Za-z0-9_.-]+)\s*$", re.IGNORECASE | re.MULTILINE)
ToolTier = Literal["core", "extended"]


@dataclass
class ToolManifest:
    """Phase 1: lightweight manifest, always in memory."""

    name: str
    description: str
    triggers: list[str]
    tier: ToolTier
    risky: bool = False


@dataclass
class ToolDefinition:
    """Phase 2: full tool definition, loaded on demand."""

    manifest: ToolManifest
    instructions: str
    setup_script: str | None


class ToolRegistry:
    """Context plugin that manages tool definitions with lazy loading."""

    name = "tools"

    def __init__(
        self,
        tools_dir: Path,
        *,
        denylist: set[str] | None = None,
        require_approval_for_risky: bool = False,
    ) -> None:
        self._dir = tools_dir
        self._manifests: list[ToolManifest] = []
        self._manifest_files: dict[str, Path] = {}
        self._cache: dict[str, ToolDefinition] = {}
        self._denylist = {name.lower() for name in (denylist or set())}
        self._require_approval_for_risky = require_approval_for_risky
        self._load_manifests()

    @staticmethod
    def _normalize_tier(raw_tier: object) -> ToolTier:
        tier = str(raw_tier or "extended").strip().lower()
        return "core" if tier == "core" else "extended"

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
                    tier=self._normalize_tier(data.get("tier")),
                    risky=bool(data.get("risky", False)),
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
                tier=self._normalize_tier(data.get("tier")),
                risky=bool(data.get("risky", False)),
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

    @staticmethod
    def _is_image_generation_message(message: str) -> bool:
        text = (message or "").lower()
        if not text:
            return False
        image_markers = (
            "image generation",
            "generate image",
            "create image",
            "edit image",
            "nano-banana-pro",
            "openai-image-gen",
            "use_tool: nano-banana-pro",
            "use_tool: openai-image-gen",
            "картин",
            "изображ",
            "сгенерируй изображ",
            "создай изображ",
        )
        return any(marker in text for marker in image_markers)

    def _check_guardrails(self, manifest: ToolManifest, user_message: str) -> str | None:
        name = manifest.name.lower()
        if name in self._denylist:
            return "denylisted"
        if (
            config.GEMINI_IMAGE_ONLY_MODE
            and name in {"gemini", "summarize"}
            and not self._is_image_generation_message(user_message)
        ):
            return "gemini-image-only"
        if self._require_approval_for_risky and manifest.risky:
            return "requires-approval"
        return None

    def match_tools(self, user_message: str) -> tuple[list[ToolDefinition], list[str], list[str]]:
        """Select active tools with core-vs-extended policy and guardrails.

        Returns:
            active tools, suggested extended tools, blocked tool names.
        """
        msg_lower = user_message.lower()
        matched: list[ToolDefinition] = []
        suggested_extended: list[str] = []
        blocked: list[str] = []
        seen_names: set[str] = set()
        requested_names = set(self.extract_requested_tools(user_message))

        for name in requested_names:
            full = self._load_full(name)
            if not full:
                continue
            blocked_reason = self._check_guardrails(full.manifest, user_message)
            if blocked_reason:
                blocked.append(f"{full.manifest.name} ({blocked_reason})")
                continue
            if full.manifest.name not in seen_names:
                matched.append(full)
                seen_names.add(full.manifest.name)

        for manifest in self._manifests:
            # Extended tools only activate via explicit USE_TOOL.
            if manifest.tier == "extended":
                if manifest.name.lower() in requested_names:
                    continue
                if any(trigger.lower() in msg_lower for trigger in manifest.triggers):
                    suggested_extended.append(manifest.name)
                continue
            for trigger in manifest.triggers:
                if trigger.lower() in msg_lower:
                    full = self._load_full(manifest.name)
                    if full and full.manifest.name not in seen_names:
                        blocked_reason = self._check_guardrails(full.manifest, user_message)
                        if blocked_reason:
                            blocked.append(f"{full.manifest.name} ({blocked_reason})")
                            break
                        matched.append(full)
                        seen_names.add(full.manifest.name)
                    break
        return matched, suggested_extended, blocked

    def build_context(self, user_message: str) -> str:
        """Build XML <tools> block for matched tools."""
        if not self._manifests:
            return ""

        available_lines = [f"- {m.name}: {m.description}" for m in self._manifests]
        available_lines.append(
            'If a tool is needed, respond with exactly: USE_TOOL: <tool_name>.'
        )
        available_lines.append(
            "Only core tools auto-activate from triggers; extended tools require explicit USE_TOOL."
        )
        available_section = "<available>\n" + "\n".join(available_lines) + "\n</available>"

        matched, suggested_extended, blocked = self.match_tools(user_message)
        hints_lines: list[str] = []
        if suggested_extended:
            deduped = ", ".join(dict.fromkeys(suggested_extended))
            hints_lines.append(f"Extended tools matched by intent: {deduped}")
            hints_lines.append("Activate one explicitly with: USE_TOOL: <tool_name>")
        if blocked:
            hints_lines.append(f"Guardrail-blocked tools: {', '.join(dict.fromkeys(blocked))}")
        hints_section = (
            "<hints>\n" + "\n".join(hints_lines) + "\n</hints>"
            if hints_lines
            else ""
        )

        if not matched:
            if hints_section:
                return f"<tools>\n{available_section}\n\n{hints_section}\n</tools>"
            return f"<tools>\n{available_section}\n</tools>"

        active_lines: list[str] = []
        for tool_def in matched[:3]:
            active_lines.append(f'<tool name="{tool_def.manifest.name}">')
            active_lines.append(tool_def.instructions.strip())
            active_lines.append("</tool>")
        active_section = "<active>\n" + "\n".join(active_lines) + "\n</active>"
        if hints_section:
            return f"<tools>\n{available_section}\n\n{hints_section}\n\n{active_section}\n</tools>"
        return f"<tools>\n{available_section}\n\n{active_section}\n</tools>"

    @staticmethod
    def extract_requested_tools(text: str) -> list[str]:
        """Parse explicit USE_TOOL directives from message text."""
        requested: list[str] = []
        seen: set[str] = set()
        for match in _USE_TOOL_PATTERN.finditer(text or ""):
            name = match.group(1).strip()
            if not name:
                continue
            normalized = name.lower()
            if normalized in seen:
                continue
            requested.append(normalized)
            seen.add(normalized)
        return requested

    def format_for_display(self) -> str:
        """Return HTML-formatted list of available tools for /tools command."""
        if not self._manifests:
            return "<b>No tools configured.</b>\n\nCreate .yaml files in tools/ directory."

        lines = ["<b>Available Tools:</b>"]
        for manifest in self._manifests:
            triggers_str = ", ".join(f'"{t}"' for t in manifest.triggers[:5])
            lines.append(f"\n<b>{manifest.name}</b>")
            lines.append(f"  {manifest.description}")
            lines.append(f"  <i>Tier:</i> {manifest.tier}")
            lines.append(f"  <i>Triggers:</i> {triggers_str}")
        return "\n".join(lines)
