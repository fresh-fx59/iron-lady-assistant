"""Tests for core/plugin context composition."""

from pathlib import Path

import yaml

from src.core.context_plugins import ContextPluginRegistry
from src.plugins.tools_plugin import ToolRegistry as PluginToolRegistry
from src.tools import ToolRegistry as ShimToolRegistry


class _StaticPlugin:
    def __init__(self, name: str, block: str) -> None:
        self.name = name
        self._block = block

    def build_context(self, user_message: str) -> str:
        return self._block

    def format_for_display(self) -> str:
        return self.name


def test_context_plugin_registry_combines_non_empty_blocks() -> None:
    registry = ContextPluginRegistry(
        [
            _StaticPlugin("a", "<a>one</a>"),
            _StaticPlugin("b", " "),
            _StaticPlugin("c", "<c>two</c>"),
        ]
    )
    context = registry.build_context("hello")
    assert context == "<a>one</a>\n\n<c>two</c>"


def test_tools_plugin_loads_yaml_and_matches_triggers(tmp_path: Path) -> None:
    (tmp_path / "web.yaml").write_text(
        "\n".join(
            [
                "name: web_search",
                "description: Search web",
                "tier: core",
                "triggers: [search, latest]",
                "instructions: |",
                "  Use web search.",
            ]
        )
    )
    registry = PluginToolRegistry(tmp_path)
    context = registry.build_context("search latest exchange rates")

    assert "<tools>" in context
    assert "- web_search: Search web" in context
    assert '<tool name="web_search">' in context
    assert "Use web search." in context


def test_tools_module_is_compatibility_shim(tmp_path: Path) -> None:
    (tmp_path / "sample.yaml").write_text("name: sample\ndescription: Sample\ntriggers: [x]")
    shim_registry = ShimToolRegistry(tmp_path)
    plugin_registry = PluginToolRegistry(tmp_path)

    assert type(shim_registry) is type(plugin_registry)


def test_tools_plugin_explicit_use_tool_directive(tmp_path: Path) -> None:
    (tmp_path / "web.yaml").write_text(
        "\n".join(
            [
                "name: web_search",
                "description: Search web",
                "tier: extended",
                "triggers: [search]",
                "instructions: |",
                "  Use web search.",
            ]
        )
    )
    registry = PluginToolRegistry(tmp_path)
    context = registry.build_context("Please decide.\nUSE_TOOL: web_search")

    assert '<tool name="web_search">' in context
    assert "If a tool is needed, respond with exactly: USE_TOOL: <tool_name>." in context


def test_extract_requested_tools_deduplicates_and_normalizes() -> None:
    requested = PluginToolRegistry.extract_requested_tools(
        "USE_TOOL: Web_Search\nnoise\nUSE_TOOL: web_search\nUSE_TOOL: github_pr"
    )
    assert requested == ["web_search", "github_pr"]


def test_extended_tool_does_not_auto_activate_from_trigger(tmp_path: Path) -> None:
    (tmp_path / "discord.yaml").write_text(
        "\n".join(
            [
                "name: discord",
                "description: Discord ops",
                "tier: extended",
                "triggers: [discord]",
                "instructions: |",
                "  Use Discord tool.",
            ]
        )
    )
    registry = PluginToolRegistry(tmp_path)
    context = registry.build_context("post this in discord")

    assert '<tool name="discord">' not in context
    assert "Extended tools matched by intent: discord" in context
    assert "Activate one explicitly with: USE_TOOL: <tool_name>" in context


def test_core_tool_auto_activates_from_trigger(tmp_path: Path) -> None:
    (tmp_path / "web.yaml").write_text(
        "\n".join(
            [
                "name: web_search",
                "description: Search web",
                "tier: core",
                "triggers: [search]",
                "instructions: |",
                "  Use web search.",
            ]
        )
    )
    registry = PluginToolRegistry(tmp_path)
    context = registry.build_context("search this")

    assert '<tool name="web_search">' in context


def test_guardrail_blocks_denylisted_tool(tmp_path: Path) -> None:
    (tmp_path / "web.yaml").write_text(
        "\n".join(
            [
                "name: web_search",
                "description: Search web",
                "tier: core",
                "triggers: [search]",
                "instructions: |",
                "  Use web search.",
            ]
        )
    )
    registry = PluginToolRegistry(tmp_path, denylist={"web_search"})
    context = registry.build_context("search now")

    assert '<tool name="web_search">' not in context
    assert "Guardrail-blocked tools: web_search (denylisted)" in context


def test_guardrail_blocks_risky_when_approval_required(tmp_path: Path) -> None:
    (tmp_path / "discord.yaml").write_text(
        "\n".join(
            [
                "name: discord",
                "description: Discord ops",
                "tier: core",
                "risky: true",
                "triggers: [discord]",
                "instructions: |",
                "  Use Discord tool.",
            ]
        )
    )
    registry = PluginToolRegistry(tmp_path, require_approval_for_risky=True)
    context = registry.build_context("send to discord")

    assert '<tool name="discord">' not in context
    assert "Guardrail-blocked tools: discord (requires-approval)" in context


def test_real_summarize_manifest_is_model_agnostic() -> None:
    manifest_path = Path(__file__).resolve().parents[1] / "tools" / "summarize.yaml"
    manifest = yaml.safe_load(manifest_path.read_text())
    instructions = manifest["instructions"]

    assert "google/gemini-3-flash-preview" not in instructions
    assert 'openai/gpt-5.2' not in instructions
    assert "--model <provider/model>" in instructions
    assert '{ "model": "<provider/model>" }' in instructions


def test_summary_inspector_manifest_points_to_single_command() -> None:
    manifest_path = Path(__file__).resolve().parents[1] / "tools" / "summary-inspector.yaml"
    manifest = yaml.safe_load(manifest_path.read_text())

    assert manifest["tier"] == "core"
    assert "last summarization" in manifest["triggers"]
    assert "python -m src.summary_inspector_tool latest --format json" in manifest["instructions"]


def test_edge_tts_safe_manifest_uses_repo_local_wrapper() -> None:
    manifest_path = Path(__file__).resolve().parents[1] / "tools" / "edge-tts-safe.yaml"
    manifest = yaml.safe_load(manifest_path.read_text())

    assert manifest["tier"] == "extended"
    assert "voice reply" in manifest["triggers"]
    assert "./venv/bin/python -m src.edge_tts_tool speak" in manifest["instructions"]


def test_browser_takeover_manifest_exposes_remote_bridge_commands() -> None:
    manifest_path = Path(__file__).resolve().parents[1] / "tools" / "browser_takeover.yaml"
    manifest = yaml.safe_load(manifest_path.read_text())

    assert manifest["tier"] == "extended"
    assert "browser extension" in manifest["triggers"]
    assert "BROWSER_TAKEOVER_PUBLIC_BASE_URL=https://YOUR-HOST/browser-takeover python3 -m src.browser_takeover setup" in manifest["instructions"]
    assert "python3 -m src.browser_takeover targets --format json" in manifest["instructions"]
