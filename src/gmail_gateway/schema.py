from __future__ import annotations

import sqlite3
from pathlib import Path


SCHEMA_FILE = Path(__file__).resolve().parents[2] / "specs" / "gmail-gateway" / "schema.v1.sql"

REQUIRED_TABLES: tuple[str, ...] = (
    "gateway_accounts",
    "gateway_oauth_tokens",
    "gateway_oauth_sessions",
    "gateway_messages",
    "gateway_sync_cursors",
    "gateway_delivery_receipts",
    "gateway_idempotency_records",
)


def ensure_gateway_schema(db_path: Path, *, schema_path: Path = SCHEMA_FILE) -> None:
    """Create or upgrade the gmail-gateway schema using the canonical SQL file."""
    sql = schema_path.read_text(encoding="utf-8")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    try:
        con.executescript(sql)
        con.commit()
    finally:
        con.close()


def verify_gateway_schema(db_path: Path, *, required_tables: tuple[str, ...] = REQUIRED_TABLES) -> list[str]:
    """Return missing required tables; empty list means schema is ready."""
    con = sqlite3.connect(db_path)
    try:
        found = {
            row[0]
            for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    finally:
        con.close()
    return [name for name in required_tables if name not in found]
