"""Compatibility shim for the tools plugin module."""

from .plugins.tools_plugin import ToolDefinition, ToolManifest, ToolRegistry

__all__ = ["ToolManifest", "ToolDefinition", "ToolRegistry"]
