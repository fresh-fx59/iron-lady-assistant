"""Integration tests for session management contract.

These tests define the expected behavior of session persistence,
which must be preserved during language rewrite.
"""

import json
from pathlib import Path
import pytest

from src.sessions import SessionManager, ChatSession


# ── Contract 1: Session persistence ───────────────────────────────
class TestSessionPersistence:
    """Sessions must persist across restarts and retrieve correctly."""

    def test_new_chat_creates_default_session(self, tmppath):
        """A new chat_id should create a ChatSession with default values."""
        manager = SessionManager()

        session = manager.get(12345)

        assert isinstance(session, ChatSession)
        assert session.claude_session_id is None
        assert session.model == "sonnet"

    def test_get_returns_same_session_for_same_chat(self, tmppath):
        """Getting the same chat_id twice should return the same object."""
        manager = SessionManager()

        session1 = manager.get(12345)
        session2 = manager.get(12345)

        assert session1 is session2

    def test_different_chats_have_independent_sessions(self, tmppath):
        """Different chat_ids should have independent sessions."""
        manager = SessionManager()

        session1 = manager.get(11111)
        session2 = manager.get(22222)

        assert session1 is not session2
        session1.claude_session_id = "sess-1"
        session2.claude_session_id = "sess-2"
        assert session1.claude_session_id == "sess-1"
        assert session2.claude_session_id == "sess-2"


# ── Contract 2: Session file persistence ───────────────────────────
class TestSessionFilePersistence:
    """Session data must be saved to and loaded from sessions.json."""

    def test_update_session_id_saves_to_file(self, tmppath):
        """Setting a session_id should persist to disk."""
        manager = SessionManager()

        manager.update_session_id(12345, "sess-abc123")

        sessions_file = tmppath / "sessions.json"
        assert sessions_file.exists()

        data = json.loads(sessions_file.read_text())
        assert "12345" in data
        assert data["12345"]["claude_session_id"] == "sess-abc123"

    def test_set_model_saves_to_file(self, tmppath):
        """Setting a model should persist to disk."""
        manager = SessionManager()

        manager.set_model(12345, "opus")

        sessions_file = tmppath / "sessions.json"
        data = json.loads(sessions_file.read_text())
        assert data["12345"]["model"] == "opus"

    def test_new_conversation_clears_session_id(self, tmppath):
        """New conversation should set session_id to None."""
        manager = SessionManager()
        manager.update_session_id(12345, "sess-old")

        manager.new_conversation(12345)

        session = manager.get(12345)
        assert session.claude_session_id is None
        # Verify it saved to file
        data = json.loads((tmppath / "sessions.json").read_text())
        assert data["12345"]["claude_session_id"] is None

    def test_loads_existing_sessions_from_file(self, tmppath, clean_sessions_file):
        """Should load existing sessions from sessions.json on startup."""
        # Pre-create sessions.json
        clean_sessions_file.write_text(json.dumps({
            "11111": {"claude_session_id": "sess-111", "model": "opus"},
            "22222": {"claude_session_id": None, "model": "haiku"},
        }))

        manager = SessionManager()

        session1 = manager.get(11111)
        session2 = manager.get(22222)

        assert session1.claude_session_id == "sess-111"
        assert session1.model == "opus"
        assert session2.claude_session_id is None
        assert session2.model == "haiku"


# ── Contract 3: File corruption handling ─────────────────────────────
class TestFileCorruptionHandling:
    """Should handle corrupted or malformed sessions.json gracefully."""

    def test_corrupted_json_creates_empty_manager(self, tmppath, clean_sessions_file):
        """Corrupted sessions.json should not crash; should start fresh."""
        clean_sessions_file.write_text("{invalid json")

        # Should not raise exception
        manager = SessionManager()

        # Should work normally
        session = manager.get(999)
        assert session.claude_session_id is None
        assert session.model == "sonnet"

    def test_invalid_data_types_handled_gracefully(self, tmppath, clean_sessions_file):
        """Invalid data in sessions.json should be handled."""
        clean_sessions_file.write_text(json.dumps({
            "11111": {"claude_session_id": "ok", "model": "sonnet"},
            "22222": {"claude_session_id": "ok", "model": "invalid_model"},  # Invalid model
            "not_a_number": {"claude_session_id": "ok", "model": "sonnet"},  # Invalid chat_id
        }))

        manager = SessionManager()

        # Valid session should load
        session1 = manager.get(11111)
        assert session1.claude_session_id == "ok"

        # Invalid data should not prevent manager from working
        session2 = manager.get(33333)
        assert session2.claude_session_id is None


# ── Contract 4: Multiple fields ─────────────────────────────────────
class TestMultipleSessionFields:
    """Updates to multiple fields should persist correctly."""

    def test_multiple_sessions_save_correctly(self, tmppath):
        """Multiple sessions with different states should all persist."""
        manager = SessionManager()

        manager.update_session_id(11111, "sess-111")
        manager.update_session_id(22222, "sess-222")
        manager.set_model(11111, "opus")
        manager.set_model(22222, "haiku")

        data = json.loads((tmppath / "sessions.json").read_text())

        assert data["11111"]["claude_session_id"] == "sess-111"
        assert data["11111"]["model"] == "opus"
        assert data["22222"]["claude_session_id"] == "sess-222"
        assert data["22222"]["model"] == "haiku"

    def test_new_conversation_doesnt_affect_other_sessions(self, tmppath):
        """Starting new conversation for one chat shouldn't affect others."""
        manager = SessionManager()

        manager.update_session_id(11111, "sess-111")
        manager.update_session_id(22222, "sess-222")
        manager.new_conversation(11111)

        session1 = manager.get(11111)
        session2 = manager.get(22222)

        assert session1.claude_session_id is None
        assert session2.claude_session_id == "sess-222"