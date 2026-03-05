from pathlib import Path

from src.context_compiler import build_context


def test_build_context_includes_block_and_matches(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir(parents=True, exist_ok=True)
    (tmp_path / "src" / "demo.py").write_text(
        "def compute_cost_guardrail(value):\n    return value\n",
        encoding="utf-8",
    )
    block = build_context("Please update cost guardrail logic", tmp_path, max_chars=2000)
    assert block.startswith("<context_compiler>")
    assert "keywords:" in block
    assert "demo.py" in block


def test_build_context_respects_max_chars(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir(parents=True, exist_ok=True)
    (tmp_path / "src" / "demo.py").write_text("x = 1\n", encoding="utf-8")
    block = build_context("a b c d e f g h i j k l", tmp_path, max_chars=120)
    assert len(block) <= 120
    assert block.endswith("[truncated]") or block.endswith("</context_compiler>")
