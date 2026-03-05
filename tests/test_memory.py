from pathlib import Path

import yaml

from src.memory import MemoryManager


def _memory_dir(tmp_path: Path) -> Path:
    memory_dir = tmp_path / "memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    return memory_dir


def test_forget_fact_removes_matching_key(tmp_path: Path) -> None:
    memory_dir = _memory_dir(tmp_path)
    manager = MemoryManager(memory_dir)
    profile_path = memory_dir / "user_profile.yaml"
    profile = yaml.safe_load(profile_path.read_text(encoding="utf-8"))
    profile["facts"] = [
        {"key": "role", "value": "Java developer", "confidence": 1.0, "source": "explicit", "updated": "2026-03-05"},
        {"key": "location", "value": "Ryazan, Russia", "confidence": 1.0, "source": "explicit", "updated": "2026-03-05"},
    ]
    profile_path.write_text(yaml.safe_dump(profile, sort_keys=False), encoding="utf-8")

    removed = manager.forget_fact("role")

    assert removed is True
    updated = yaml.safe_load(profile_path.read_text(encoding="utf-8"))
    keys = [f["key"] for f in updated.get("facts", [])]
    assert keys == ["location"]


def test_consolidate_facts_deduplicates_and_prunes_low_confidence(tmp_path: Path) -> None:
    memory_dir = _memory_dir(tmp_path)
    manager = MemoryManager(memory_dir)
    profile_path = memory_dir / "user_profile.yaml"
    profile = yaml.safe_load(profile_path.read_text(encoding="utf-8"))
    profile["facts"] = [
        {"key": "role", "value": "developer", "confidence": 0.7, "source": "inferred", "updated": "2026-03-01"},
        {"key": "role", "value": "Java developer", "confidence": 1.0, "source": "explicit", "updated": "2026-03-05"},
        {"key": "hobby", "value": "chess", "confidence": 0.2, "source": "inferred", "updated": "2026-03-04"},
    ]
    profile_path.write_text(yaml.safe_dump(profile, sort_keys=False), encoding="utf-8")

    stats = manager.consolidate_facts(min_confidence=0.4)

    assert stats == {"before": 3, "after": 1, "removed": 2}
    updated = yaml.safe_load(profile_path.read_text(encoding="utf-8"))
    facts = updated.get("facts", [])
    assert len(facts) == 1
    assert facts[0]["key"] == "role"
    assert facts[0]["value"] == "Java developer"


def test_build_context_prefers_query_relevant_facts(tmp_path: Path) -> None:
    memory_dir = _memory_dir(tmp_path)
    manager = MemoryManager(memory_dir)
    profile_path = memory_dir / "user_profile.yaml"
    profile = yaml.safe_load(profile_path.read_text(encoding="utf-8"))
    profile["facts"] = [
        {"key": "role", "value": "Java developer", "confidence": 1.0, "source": "explicit", "updated": "2026-03-05"},
        {"key": "coffee_brewing_method", "value": "portafilter espresso machine", "confidence": 1.0, "source": "explicit", "updated": "2026-03-05"},
        {"key": "location", "value": "Ryazan, Russia", "confidence": 1.0, "source": "explicit", "updated": "2026-03-05"},
    ]
    profile_path.write_text(yaml.safe_dump(profile, sort_keys=False), encoding="utf-8")

    context = manager.build_context("How do I improve espresso extraction?")

    assert "<relevant_facts>" in context
    assert "coffee_brewing_method" in context
    assert "role: Java developer" not in context


def test_build_context_fallback_keeps_relevant_facts_bounded(tmp_path: Path) -> None:
    memory_dir = _memory_dir(tmp_path)
    manager = MemoryManager(memory_dir)
    profile_path = memory_dir / "user_profile.yaml"
    profile = yaml.safe_load(profile_path.read_text(encoding="utf-8"))
    profile["facts"] = [
        {"key": f"k{i}", "value": f"v{i}", "confidence": 0.9, "source": "inferred", "updated": "2026-03-05"}
        for i in range(8)
    ]
    profile_path.write_text(yaml.safe_dump(profile, sort_keys=False), encoding="utf-8")

    context = manager.build_context("Hi")

    # No semantic overlap expected; fallback should not inject all 8 facts.
    assert context.count("- k") == 6
