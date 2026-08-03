"""SQL memory must not need PyYAML to start.

Facts moved to SQLite in 77da357 ("migrate persistent memory facts to SQL-only
storage"). The only surviving YAML in `src/memory.py` is a one-shot migration
of a legacy `user_profile.yaml` that most deployments no longer have — yet
`import yaml` sat at module scope, so a third-party dependency gated the whole
memory system. On the box that is exactly how it failed: the wrapper fell
through to a bare system python3 and `memory-manager` died with
`ModuleNotFoundError: No module named 'yaml'` before reaching a single row.

An optional legacy path must cost an optional import.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

_BLOCK_YAML = '''
import sys


class _BlockYaml:
    def find_spec(self, name, path=None, target=None):
        if name == "yaml" or name.startswith("yaml."):
            raise ImportError("No module named 'yaml'")
        return None


sys.meta_path.insert(0, _BlockYaml())
'''


def _env_without_yaml(tmp_path: Path) -> dict[str, str]:
    """PYTHONPATH with a sitecustomize that makes `import yaml` fail."""
    shim = tmp_path / "no-yaml"
    shim.mkdir()
    (shim / "sitecustomize.py").write_text(_BLOCK_YAML, encoding="utf-8")
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("TELEGRAM_", "ALLOWED_", "PASSIVE_"))
    }
    env["PYTHONPATH"] = f"{shim}{os.pathsep}{REPO_ROOT}"
    return env


def test_the_shim_really_blocks_yaml(tmp_path: Path) -> None:
    """Guard the guard: without this, the tests below could pass vacuously."""
    result = subprocess.run(
        [sys.executable, "-c", "import yaml"],
        env=_env_without_yaml(tmp_path),
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "No module named 'yaml'" in result.stderr


def test_memory_manager_constructs_without_pyyaml(tmp_path: Path) -> None:
    memory_dir = tmp_path / "memory"
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from pathlib import Path\n"
            "from src.memory import MemoryManager\n"
            f"MemoryManager(Path({str(memory_dir)!r}))\n"
            "print('constructed')\n",
        ],
        env=_env_without_yaml(tmp_path),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "constructed" in result.stdout
    assert (memory_dir / "episodes.db").exists()


def test_memory_cli_round_trips_a_fact_without_pyyaml(tmp_path: Path) -> None:
    """The end-to-end path the bot actually uses: upsert, then read it back."""
    memory_dir = tmp_path / "memory"
    env = _env_without_yaml(tmp_path)

    upsert = subprocess.run(
        [
            sys.executable,
            "-m",
            "src.memory_tool",
            "--memory-dir",
            str(memory_dir),
            "upsert",
            "--key",
            "no_yaml_probe",
            "--value",
            "written without pyyaml",
            "--type",
            "misc",
        ],
        env=env,
        capture_output=True,
        text=True,
    )
    assert upsert.returncode == 0, upsert.stderr

    listing = subprocess.run(
        [sys.executable, "-m", "src.memory_tool", "--memory-dir", str(memory_dir), "list"],
        env=env,
        capture_output=True,
        text=True,
    )
    assert listing.returncode == 0, listing.stderr
    facts = json.loads(listing.stdout)
    assert any(fact["key"] == "no_yaml_probe" for fact in facts), facts


def test_legacy_profile_migration_is_skipped_not_fatal(tmp_path: Path) -> None:
    """A legacy YAML profile with no PyYAML: warn and carry on, never crash."""
    memory_dir = tmp_path / "memory"
    memory_dir.mkdir()
    (memory_dir / "user_profile.yaml").write_text("name: Alex\n", encoding="utf-8")

    result = subprocess.run(
        [sys.executable, "-m", "src.memory_tool", "--memory-dir", str(memory_dir), "list"],
        env=_env_without_yaml(tmp_path),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    # The file is untouched, so the migration can still run once PyYAML is back.
    assert (memory_dir / "user_profile.yaml").exists()
