#!/usr/bin/env python3
"""Automated tool updater - fetches tools from popular repositories.

Clones configured repositories, extracts tool/skill definitions, converts
to our YAML format, and syncs to tools/ directory.

Run daily via systemd timer to keep tools up to date.
"""

import json
import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import yaml

# ── Configuration ────────────────────────────────────────

TOOLS_DIR = Path("tools")
TOOLS_DIR.mkdir(exist_ok=True)

# Repositories to fetch tools from (can add more)
REPOS = [
    {
        "name": "openclaw",
        "url": "https://github.com/OpenClaw/OpenClaw.git",
        "type": "openclaw",
        "enabled": True,
    },
    # Future: add AgentSkills, Anthropic Skills, etc.
]

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ── Format Converters ────────────────────────────────────

def convert_openclaw_skill(skill_path: Path) -> dict | None:
    """Convert OpenClaw SKILL.md to our tool YAML format.

    OpenClaw format: YAML frontmatter + markdown content
    Our format: name, description, triggers, instructions, setup
    """
    try:
        content = skill_path.read_text()

        # Parse YAML frontmatter (between --- markers)
        if not content.startswith("---"):
            return None

        parts = content.split("---", 2)
        if len(parts) < 3:
            return None

        frontmatter = yaml.safe_load(parts[1].strip())
        markdown_body = parts[2].strip()

        # Extract fields
        name = frontmatter.get("name", skill_path.parent.name)
        description = frontmatter.get("description", "").strip('"')

        # Extract triggers from description (simple keyword extraction)
        # For now, use the skill name as the main trigger
        triggers = [name]
        if description:
            # Add common words from description
            words = description.lower().split()
            for word in words[:5]:  # Top 5 words as triggers
                if len(word) > 3:  # Only meaningful words
                    triggers.append(word)

        # Build instructions from markdown body
        instructions = markdown_body

        # Extract setup requirements from metadata if present
        setup = None
        openclaw_meta = frontmatter.get("metadata", {}).get("openclaw", {})
        bins = openclaw_meta.get("requires", {}).get("bins", [])
        if bins:
            setup = f"Ensure binary installed: {' '.join(bins)}"

        return {
            "name": name,
            "description": description[:100],  # Limit description length
            "triggers": triggers[:8],  # Limit triggers
            "instructions": instructions[:2000],  # Prevent overly long instructions
            "setup": setup,
        }
    except Exception as e:
        logger.warning(f"Failed to convert {skill_path}: {e}")
        return None


# ── Repository Fetcher ─────────────────────────────────────

def fetch_repository(repo: dict, temp_dir: Path) -> list[dict] | None:
    """Clone repo and extract tools/skills."""
    if not repo["enabled"]:
        return None

    repo_name = repo["name"]
    repo_temp = temp_dir / repo_name

    try:
        # Clone shallow (depth 1) for speed
        logger.info(f"Cloning {repo_name}...")
        subprocess.run(
            ["git", "clone", "--depth", "1", repo["url"], str(repo_temp)],
            check=True,
            capture_output=True,
        )

        # Find skills/tools based on repo type
        tools: list[dict] = []

        if repo["type"] == "openclaw":
            skills_dir = repo_temp / "skills"
            if skills_dir.exists():
                for skill_dir in skills_dir.iterdir():
                    skill_md = skill_dir / "SKILL.md"
                    if skill_md.exists():
                        tool_data = convert_openclaw_skill(skill_md)
                        if tool_data:
                            tools.append(tool_data)

        # Add more repo types as needed...
        # elif repo["type"] == "agentskills":
        #     ...

        logger.info(f"Found {len(tools)} tools in {repo_name}")
        return tools

    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to clone {repo_name}: {e.stderr}")
        return None
    except Exception as e:
        logger.error(f"Error processing {repo_name}: {e}")
        return None


# ── Tool Sync ───────────────────────────────────────────

def write_tool_yaml(tool_data: dict, output_dir: Path) -> tuple[bool, str]:
    """Write tool data to YAML file. Returns (success, message)."""
    name = tool_data["name"]
    filename = output_dir / f"{name}.yaml"

    try:
        # Check if file exists and is identical
        if filename.exists():
            with open(filename) as f:
                existing = yaml.safe_load(f)
            if existing == tool_data:
                return False, f"{name}: unchanged"

        # Write new version
        with open(filename, "w") as f:
            yaml.dump(tool_data, f, default_flow_style=False, sort_keys=False)

        logger.info(f"Updated {filename}")
        return True, f"{name}: updated"

    except Exception as e:
        logger.error(f"Failed to write {filename}: {e}")
        return False, f"{name}: error ({e})"


def sync_tools() -> None:
    """Main sync function - fetch all repos and update tools."""
    logger.info("=== Starting tool sync ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        all_tools: list[dict] = []

        # Fetch from all configured repos
        for repo in REPOS:
            tools = fetch_repository(repo, temp_path)
            if tools:
                all_tools.extend(tools)

        # Write/update tools
        updated = 0
        unchanged = 0
        for tool_data in all_tools:
            success, msg = write_tool_yaml(tool_data, TOOLS_DIR)
            if success:
                updated += 1
            else:
                unchanged += 1

        # Clean up orphaned tools (optional - skip for now to preserve custom tools)

        logger.info(f"=== Sync complete: {updated} updated, {unchanged} unchanged ===")


# ── Entry Point ─────────────────────────────────────────

if __name__ == "__main__":
    sync_tools()