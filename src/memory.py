"""Persistent memory system for the Telegram Claude bot.

Global memory stored as:
  {MEMORY_DIR}/user_profile.yaml  — core profile + semantic facts (Claude edits directly)
  {MEMORY_DIR}/episodes.db        — episodic memory (SQLite with FTS5)

Memory context is injected as XML before each user message. Claude updates
user_profile.yaml via its built-in file tools. Episodic memory is managed
by Python (REFLECT on /new, RECALL via FTS5 search).
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from . import config

logger = logging.getLogger(__name__)

# ── Stop words for keyword extraction ────────────────────────
_STOP_WORDS = frozenset(
    "i me my myself we our ours ourselves you your yours yourself yourselves "
    "he him his himself she her hers herself it its itself they them their "
    "theirs themselves what which who whom this that these those am is are "
    "was were be been being have has had having do does did doing a an the "
    "and but if or because as until while of at by for with about against "
    "between through during before after above below to from up down in out "
    "on off over under again further then once here there when where why how "
    "all both each few more most other some such no nor not only own same so "
    "than too very s t can will just don should now d ll m o re ve y ain "
    "aren couldn didn doesn hadn hasn haven isn ma mightn mustn needn shan "
    "shouldn wasn weren won wouldn could would please hey hi hello yes yeah "
    "ok okay thanks thank sure".split()
)

_PROFILE_TEMPLATE = """\
# User profile and semantic memory.
# Claude: update this file when you learn about the user.
name: null
preferences:
  communication_style: null
  timezone: null
  languages: []
facts: []
# Each fact: {key: str, value: str, confidence: 0.0-1.0, source: explicit|inferred, updated: YYYY-MM-DD}
"""

_EPISODES_SCHEMA = """\
CREATE TABLE IF NOT EXISTS episodes (
    id INTEGER PRIMARY KEY,
    chat_id INTEGER,
    timestamp TEXT,
    summary TEXT,
    topics TEXT,
    decisions TEXT,
    entities TEXT
);
"""

_FTS_SCHEMA = """\
CREATE VIRTUAL TABLE IF NOT EXISTS episodes_fts USING fts5(
    summary, topics, decisions, content=episodes, content_rowid=id
);
"""

# Triggers to keep FTS index in sync with episodes table
_FTS_TRIGGERS = [
    """\
CREATE TRIGGER IF NOT EXISTS episodes_ai AFTER INSERT ON episodes BEGIN
    INSERT INTO episodes_fts(rowid, summary, topics, decisions)
    VALUES (new.id, new.summary, new.topics, new.decisions);
END;
""",
    """\
CREATE TRIGGER IF NOT EXISTS episodes_ad AFTER DELETE ON episodes BEGIN
    INSERT INTO episodes_fts(episodes_fts, rowid, summary, topics, decisions)
    VALUES ('delete', old.id, old.summary, old.topics, old.decisions);
END;
""",
    """\
CREATE TRIGGER IF NOT EXISTS episodes_au AFTER UPDATE ON episodes BEGIN
    INSERT INTO episodes_fts(episodes_fts, rowid, summary, topics, decisions)
    VALUES ('delete', old.id, old.summary, old.topics, old.decisions);
    INSERT INTO episodes_fts(rowid, summary, topics, decisions)
    VALUES (new.id, new.summary, new.topics, new.decisions);
END;
""",
]


class MemoryManager:
    """Global memory manager with YAML profile + SQLite episodic storage."""

    def __init__(self, memory_dir: Path) -> None:
        self._dir = memory_dir
        self._dir.mkdir(parents=True, exist_ok=True)
        self._profile_path = self._dir / "user_profile.yaml"
        self._db_path = self._dir / "episodes.db"

        # Seed profile template if missing
        if not self._profile_path.exists():
            self._profile_path.write_text(_PROFILE_TEMPLATE)

        # Init SQLite
        self._init_db()

    def _init_db(self) -> None:
        """Create episodes table and FTS5 index if they don't exist."""
        con = sqlite3.connect(self._db_path)
        try:
            con.execute(_EPISODES_SCHEMA)
            con.execute(_FTS_SCHEMA)
            for trigger in _FTS_TRIGGERS:
                con.execute(trigger)
            con.commit()
        finally:
            con.close()

    def _ensure_storage(self) -> None:
        """Recreate storage if external cleanup removed the directory or DB file."""
        self._dir.mkdir(parents=True, exist_ok=True)
        if not self._db_path.exists():
            self._init_db()

    def _load_profile(self) -> dict[str, Any]:
        try:
            data = yaml.safe_load(self._profile_path.read_text()) or {}
        except Exception:
            logger.debug("Could not read user_profile.yaml")
            data = {}
        return data if isinstance(data, dict) else {}

    def _write_profile(self, data: dict[str, Any]) -> None:
        self._profile_path.write_text(
            yaml.safe_dump(data, sort_keys=False, allow_unicode=False),
            encoding="utf-8",
        )

    # ── Context building ─────────────────────────────────────

    def build_context(self, user_message: str) -> str:
        """Read memory and return XML context block to prepend to the prompt.

        Returns empty string if all memory is empty/default.
        """
        sections: list[str] = []

        # Core + Semantic from YAML
        try:
            data = yaml.safe_load(self._profile_path.read_text()) or {}
        except Exception:
            logger.debug("Could not read user_profile.yaml")
            data = {}

        # Core profile
        core_lines: list[str] = []
        if data.get("name"):
            core_lines.append(f"Name: {data['name']}")
        prefs = data.get("preferences") or {}
        if prefs.get("communication_style"):
            core_lines.append(f"Style: {prefs['communication_style']}")
        if prefs.get("timezone"):
            core_lines.append(f"Timezone: {prefs['timezone']}")
        if prefs.get("languages"):
            core_lines.append(f"Languages: {', '.join(prefs['languages'])}")
        if core_lines:
            sections.append("<core>\n" + "\n".join(core_lines) + "\n</core>")

        # Semantic facts (confidence >= 0.6)
        facts = data.get("facts") or []
        high_conf = [
            f for f in facts
            if isinstance(f, dict) and float(f.get("confidence", 1.0)) >= 0.6
        ]
        relevant_facts = self._select_relevant_facts(high_conf, user_message)
        if relevant_facts:
            lines = [f"- {f.get('key', '?')}: {f.get('value', '?')}" for f in relevant_facts]
            sections.append("<relevant_facts>\n" + "\n".join(lines) + "\n</relevant_facts>")

        # Episodic — search by keywords from user message
        episodes = self.search_episodes(user_message, limit=5)
        if episodes:
            lines = [f"- {e['timestamp'][:10]}: {e['summary']}" for e in episodes]
            sections.append("<recent_episodes>\n" + "\n".join(lines) + "\n</recent_episodes>")

        if not sections:
            return ""

        return "<memory>\n" + "\n".join(sections) + "\n</memory>"

    def build_instructions(self) -> str:
        """Return memory_instructions block with absolute file path."""
        abs_path = self._profile_path.resolve()
        return (
            "\n<memory_instructions>\n"
            f"You have persistent memory. Your profile + facts file:\n"
            f"  {abs_path}\n"
            "Update it when you learn something worth remembering about the user.\n"
            "Use the YAML format already in the file. Add facts with confidence scores.\n"
            "Do NOT update memory on every message — only when you learn something new.\n"
            "</memory_instructions>"
        )

    # ── Episodic memory (SQLite) ─────────────────────────────

    def add_episode(
        self,
        chat_id: int,
        summary: str,
        topics: list[str] | None = None,
        decisions: list[str] | None = None,
        entities: list[str] | None = None,
    ) -> None:
        """Insert a new episode into the database."""
        self._ensure_storage()
        con = sqlite3.connect(self._db_path)
        try:
            con.execute(
                "INSERT INTO episodes (chat_id, timestamp, summary, topics, decisions, entities) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    chat_id,
                    datetime.now(timezone.utc).isoformat(),
                    summary,
                    json.dumps(topics or []),
                    json.dumps(decisions or []),
                    json.dumps(entities or []),
                ),
            )
            con.commit()
        finally:
            con.close()
        logger.info("Added episode for chat %d: %s", chat_id, summary[:80])

    def search_episodes(self, query: str, limit: int = 5) -> list[dict]:
        """Search episodes via FTS5. Falls back to recent episodes if no query match."""
        self._ensure_storage()
        keywords = self._extract_keywords(query)

        con = sqlite3.connect(self._db_path)
        con.row_factory = sqlite3.Row
        try:
            rows: list[sqlite3.Row] = []

            if keywords:
                fts_query = " OR ".join(keywords)
                try:
                    rows = con.execute(
                        "SELECT e.* FROM episodes e "
                        "JOIN episodes_fts f ON e.id = f.rowid "
                        "WHERE episodes_fts MATCH ? "
                        "ORDER BY rank LIMIT ?",
                        (fts_query, limit),
                    ).fetchall()
                except sqlite3.OperationalError:
                    # FTS query syntax error — fall back to recent
                    pass

            # Fallback: most recent episodes
            if not rows:
                rows = con.execute(
                    "SELECT * FROM episodes ORDER BY timestamp DESC LIMIT ?",
                    (limit,),
                ).fetchall()

            return [dict(r) for r in rows]
        finally:
            con.close()

    def _extract_keywords(self, text: str) -> list[str]:
        """Extract non-stop-word keywords from text for FTS5 search."""
        words = []
        for word in text.lower().split():
            # Strip punctuation
            cleaned = "".join(c for c in word if c.isalnum())
            if cleaned and cleaned not in _STOP_WORDS and len(cleaned) > 2:
                words.append(cleaned)
        return words[:10]  # Cap to prevent huge FTS queries

    def _select_relevant_facts(self, facts: list[dict], query: str, limit: int = 10) -> list[dict]:
        """Select facts relevant to the current query using lightweight keyword overlap."""
        if not facts:
            return []

        query_terms = set(self._extract_keywords(query))
        scored: list[tuple[int, float, str, dict]] = []
        for fact in facts:
            if not isinstance(fact, dict):
                continue
            key = str(fact.get("key", ""))
            value = str(fact.get("value", ""))
            fact_terms = set(self._extract_keywords(f"{key} {value}"))
            overlap = len(query_terms & fact_terms)
            if not overlap:
                continue
            confidence = float(fact.get("confidence", 1.0))
            updated = str(fact.get("updated", ""))
            scored.append((overlap, confidence, updated, fact))

        if scored:
            scored.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
            return [item[3] for item in scored[:limit]]

        # Fallback when query has no direct semantic overlap: keep context bounded.
        fallback = sorted(
            facts,
            key=lambda fact: (
                float(fact.get("confidence", 1.0)),
                str(fact.get("updated", "")),
            ),
            reverse=True,
        )
        return fallback[: min(limit, 6)]

    # ── Display & management ─────────────────────────────────

    def format_for_display(self) -> str:
        """Human-readable memory dump for /memory command."""
        self._ensure_storage()
        parts: list[str] = []

        # Profile
        if self._profile_path.exists():
            content = self._profile_path.read_text().strip()
            parts.append(f"<b>user_profile.yaml</b>\n<pre>{content}</pre>")
        else:
            parts.append("<b>user_profile.yaml</b>\n<i>(not created yet)</i>")

        # Episodes
        con = sqlite3.connect(self._db_path)
        con.row_factory = sqlite3.Row
        try:
            rows = con.execute(
                "SELECT timestamp, summary FROM episodes ORDER BY timestamp DESC LIMIT 10"
            ).fetchall()
        finally:
            con.close()

        if rows:
            lines = [f"- {r['timestamp'][:10]}: {r['summary']}" for r in rows]
            parts.append("<b>Episodes</b> (last 10)\n<pre>" + "\n".join(lines) + "</pre>")
        else:
            parts.append("<b>Episodes</b>\n<i>(none yet)</i>")

        return "\n\n".join(parts)

    def clear(self) -> None:
        """Reset all memory to defaults."""
        self._ensure_storage()
        self._profile_path.write_text(_PROFILE_TEMPLATE)
        con = sqlite3.connect(self._db_path)
        try:
            con.execute("DELETE FROM episodes")
            # Rebuild FTS index
            con.execute("INSERT INTO episodes_fts(episodes_fts) VALUES ('rebuild')")
            con.commit()
        finally:
            con.close()
        logger.info("All memory cleared")

    def forget_fact(self, key: str) -> bool:
        """Remove all facts matching a semantic key. Returns True if any removed."""
        normalized = (key or "").strip().lower()
        if not normalized:
            return False

        data = self._load_profile()
        facts = data.get("facts") or []
        if not isinstance(facts, list):
            return False

        kept: list[dict[str, Any]] = []
        removed = 0
        for fact in facts:
            if not isinstance(fact, dict):
                continue
            fact_key = str(fact.get("key", "")).strip().lower()
            if fact_key == normalized:
                removed += 1
                continue
            kept.append(fact)

        if removed == 0:
            return False

        data["facts"] = kept
        self._write_profile(data)
        logger.info("Removed %d facts for key '%s'", removed, normalized)
        return True

    def consolidate_facts(self, min_confidence: float = 0.4) -> dict[str, int]:
        """De-duplicate facts by key and drop very-low-confidence entries."""
        data = self._load_profile()
        facts = data.get("facts") or []
        if not isinstance(facts, list):
            return {"before": 0, "after": 0, "removed": 0}

        before = len(facts)
        grouped: dict[str, list[dict[str, Any]]] = {}
        dropped_low_conf = 0

        for fact in facts:
            if not isinstance(fact, dict):
                continue
            key = str(fact.get("key", "")).strip()
            if not key:
                continue
            confidence = float(fact.get("confidence", 1.0))
            if confidence < min_confidence:
                dropped_low_conf += 1
                continue
            grouped.setdefault(key.lower(), []).append(fact)

        merged: list[dict[str, Any]] = []
        for variants in grouped.values():
            variants_sorted = sorted(
                variants,
                key=lambda item: (
                    float(item.get("confidence", 1.0)),
                    str(item.get("updated", "")),
                ),
                reverse=True,
            )
            winner = dict(variants_sorted[0])
            for extra in variants_sorted[1:]:
                # Preserve strongest confidence/source while keeping canonical winner.
                winner["confidence"] = max(
                    float(winner.get("confidence", 1.0)),
                    float(extra.get("confidence", 1.0)),
                )
                if winner.get("source") != "explicit" and extra.get("source") == "explicit":
                    winner["source"] = "explicit"
                winner["updated"] = max(
                    str(winner.get("updated", "")),
                    str(extra.get("updated", "")),
                )
            merged.append(winner)

        merged.sort(key=lambda item: str(item.get("key", "")).lower())
        data["facts"] = merged
        self._write_profile(data)

        after = len(merged)
        removed = before - after
        logger.info(
            "Consolidated facts: before=%d after=%d removed=%d dropped_low_conf=%d",
            before,
            after,
            removed,
            dropped_low_conf,
        )
        return {"before": before, "after": after, "removed": removed}
