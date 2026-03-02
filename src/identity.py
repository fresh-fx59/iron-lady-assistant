"""Assistant identity policy loaded from disk and injected into prompts."""

from __future__ import annotations

from pathlib import Path

import yaml

_IDENTITY_TEMPLATE = """\
mission: "Act as a pragmatic engineering copilot focused on reliable outcomes."
operating_mode: "autonomous-with-guardrails"
priorities:
  - "Protect production reliability and user data."
  - "Prefer reversible, low-risk actions first."
  - "Verify outcomes and report concrete results."
boundaries:
  - "Never expose secrets in chat."
  - "Do not perform destructive actions without explicit approval."
  - "Escalate when confidence is low or blast radius is high."
proactivity:
  enabled: true
  low_risk_auto_actions: true
  require_confirmation_for:
    - "destructive_changes"
    - "external_side_effects"
"""


class IdentityManager:
    """Persistent identity contract for behavior + guardrails."""

    def __init__(self, memory_dir: Path) -> None:
        self._path = memory_dir / "identity.yaml"
        if not self._path.exists():
            self._path.write_text(_IDENTITY_TEMPLATE)

    def build_context(self) -> str:
        """Return XML identity context, or empty string if unavailable."""
        try:
            data = yaml.safe_load(self._path.read_text()) or {}
        except Exception:
            return ""

        lines: list[str] = []
        if mission := data.get("mission"):
            lines.append(f"Mission: {mission}")
        if mode := data.get("operating_mode"):
            lines.append(f"Operating mode: {mode}")

        priorities = data.get("priorities") or []
        if priorities:
            lines.append("Priorities:")
            lines.extend(f"- {item}" for item in priorities if item)

        boundaries = data.get("boundaries") or []
        if boundaries:
            lines.append("Boundaries:")
            lines.extend(f"- {item}" for item in boundaries if item)

        proactivity = data.get("proactivity") or {}
        if isinstance(proactivity, dict) and proactivity:
            lines.append(
                f"Proactivity enabled: {bool(proactivity.get('enabled', True))}"
            )
            lines.append(
                "Low-risk auto actions: "
                f"{bool(proactivity.get('low_risk_auto_actions', True))}"
            )
            confirm = proactivity.get("require_confirmation_for") or []
            if confirm:
                lines.append("Require confirmation for:")
                lines.extend(f"- {item}" for item in confirm if item)

        if not lines:
            return ""
        return "<identity>\n" + "\n".join(lines) + "\n</identity>"
