from pathlib import Path

from src.gmail_gateway.schema import REQUIRED_TABLES, ensure_gateway_schema, verify_gateway_schema


def test_ensure_gateway_schema_creates_required_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "gmail-gateway.db"

    ensure_gateway_schema(db_path)

    missing = verify_gateway_schema(db_path)
    assert missing == []


def test_verify_gateway_schema_reports_missing_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "gmail-gateway.db"
    db_path.write_text("", encoding="utf-8")

    missing = verify_gateway_schema(db_path)

    assert missing == list(REQUIRED_TABLES)
