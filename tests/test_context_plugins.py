"""Tests for core/plugin context composition."""

from pathlib import Path

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

