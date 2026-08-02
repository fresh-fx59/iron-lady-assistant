#!/usr/bin/env python3
"""Render a systemd unit template. One substitution pass, no partial output.

Placeholders are @NAME@. An unknown placeholder is an error rather than a
silently empty field — a unit with an empty ExecStart starts and does nothing.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

_PLACEHOLDER = re.compile(r"@([A-Z_]+)@")


def render(template_path: Path, values: dict[str, str]) -> str:
    text = Path(template_path).read_text(encoding="utf-8")

    def _sub(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in values:
            raise KeyError(f"no value for placeholder @{name}@ in {template_path}")
        return values[name]

    return _PLACEHOLDER.sub(_sub, text)


def main() -> int:
    parser = argparse.ArgumentParser(prog="render")
    parser.add_argument("template", type=Path)
    parser.add_argument("--user", required=True)
    parser.add_argument("--dir", required=True)
    parser.add_argument("--python", required=True)
    parser.add_argument("--envfile", required=True)
    args = parser.parse_args()

    print(
        render(
            args.template,
            {"USER": args.user, "DIR": args.dir, "PYTHON": args.python, "ENVFILE": args.envfile},
        ),
        end="",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
