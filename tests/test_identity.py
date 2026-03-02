from pathlib import Path

from src.identity import IdentityManager


def test_identity_manager_seeds_template(tmp_path: Path) -> None:
    manager = IdentityManager(tmp_path)
    identity_file = tmp_path / "identity.yaml"
    assert identity_file.exists()
    text = identity_file.read_text()
    assert "mission:" in text
    assert "boundaries:" in text


def test_identity_context_contains_core_sections(tmp_path: Path) -> None:
    manager = IdentityManager(tmp_path)
    context = manager.build_context()
    assert context.startswith("<identity>")
    assert "Mission:" in context
    assert "Boundaries:" in context
    assert "Proactivity enabled:" in context
