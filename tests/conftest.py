"""Shared fixtures and configuration for integration tests."""

import json
import os
import sys
import tempfile
from pathlib import Path
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


# ── Fixture: Clean test environment ───────────────────────────────
@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """Reset environment and working directory for each test."""
    # Save original env
    original_dir = Path.cwd()

    # Create temp directory for test
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        os.chdir(tmppath)

        # Set minimal env
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token-12345")
        monkeypatch.setenv("ALLOWED_USER_IDS", "123456789")
        monkeypatch.setenv("DEFAULT_MODEL", "sonnet")
        monkeypatch.setenv("IDLE_TIMEOUT", "60")
        monkeypatch.setenv("PROGRESS_DEBOUNCE_SECONDS", "1.0")
        monkeypatch.setenv("METRICS_PORT", "0")  # Disable metrics
        monkeypatch.setenv("CLAUDE_WORKING_DIR", str(tmppath / "work"))
        monkeypatch.setenv("DISABLE_REFLECTION", "1")
        monkeypatch.delenv("CLAUDECODE", raising=False)

        # Create working dir
        (tmppath / "work").mkdir(exist_ok=True)

        yield tmppath

    # Restore original directory
    os.chdir(original_dir)


# ── Fixture: Reset session state ────────────────────────────────
@pytest.fixture(autouse=True)
def reset_session_manager():
    """Ensure session manager state is clean between tests."""
    try:
        from src.bot import (
            session_manager,
            provider_manager,
            _chat_states,
            _error_counts,
        )
        session_manager.sessions.clear()
        provider_manager._chat_provider_idx.clear()
        provider_manager._fallback_since.clear()
        _chat_states.clear()
        _error_counts.clear()
    except Exception:
        pass


# ── Fixture: Mock Telegram bot & message ───────────────────────────
@pytest.fixture
def mock_bot():
    """Mock aiogram Bot instance."""
    bot = AsyncMock()
    bot.session = AsyncMock()
    bot.send_message = AsyncMock(return_value=MagicMock(message_id=123))
    bot.edit_message_text = AsyncMock()
    bot.delete_message = AsyncMock()
    bot.send_chat_action = AsyncMock()
    bot.set_my_commands = AsyncMock()
    return bot


@pytest.fixture
def mock_message(mock_bot):
    """Mock Telegram Message with minimal fields."""
    msg = AsyncMock()
    msg.text = "hello"
    msg.chat = AsyncMock()
    msg.chat.id = 123456789
    msg.message_thread_id = None
    msg.bot = mock_bot
    msg.from_user = AsyncMock()
    msg.from_user.id = 123456789
    msg.content_type = "text"
    msg.answer = AsyncMock()
    return msg


# ── Fixture: temp path alias ────────────────────────────────────
@pytest.fixture
def tmppath() -> Path:
    """Alias for current temp working directory set by clean_env."""
    return Path.cwd()


# ── Fixture: Mock subprocess results ──────────────────────────────
@pytest.fixture
def mock_subprocess_lines():
    """Factory for creating mock subprocess output lines."""
    def _lines(events: list[dict]) -> list[str]:
        return [json.dumps(event) + "\n" for event in events]
    return _lines


@pytest.fixture
def mock_successful_response():
    """Standard successful Claude response events."""
    return [
        {"type": "system", "session_id": "sess-123"},
        {"type": "stream_event", "event": {"type": "content_block_start", "content_block": {"type": "text"}}},
        {"type": "stream_event", "event": {"type": "content_block_delta", "delta": {"type": "text_delta", "text": "Hello"}}},
        {"type": "stream_event", "event": {"type": "content_block_stop"}},
        {"type": "assistant", "message": {"content": [{"type": "text", "text": "Hello"}]}},
        {"type": "result", "result": "Hello", "session_id": "sess-123", "is_error": False,
         "total_cost_usd": 0.001, "num_turns": 1, "duration_ms": 500}
    ]


@pytest.fixture
def mock_tool_use_response():
    """Claude response with tool usage."""
    return [
        {"type": "stream_event", "event": {"type": "content_block_start",
         "content_block": {"type": "tool_use", "name": "Read", "input": {"file_path": "/tmp/test.txt"}}}},
        {"type": "stream_event", "event": {"type": "content_block_delta",
         "delta": {"type": "input_json_delta", "partial_json": '{"file_path": "/tmp/test.txt"}'}}},
        {"type": "stream_event", "event": {"type": "content_block_stop"}},
        {"type": "assistant", "message": {"content": [
            {"type": "tool_use", "name": "Read", "input": {"file_path": "/tmp/test.txt"}},
            {"type": "text", "text": "Done reading"}
        ]}},
        {"type": "result", "result": "Done reading", "session_id": "sess-123", "is_error": False,
         "total_cost_usd": 0.002, "num_turns": 2, "duration_ms": 1000}
    ]


@pytest.fixture
def mock_error_response():
    """Claude error response."""
    return [
        {"type": "result", "result": "API error occurred", "session_id": "sess-123", "is_error": True,
         "total_cost_usd": 0.0, "num_turns": 0, "duration_ms": 0}
    ]


# ── Fixture: Async test helpers ───────────────────────────────────
@pytest_asyncio.fixture
async def async_event_loop():
    """Provide fresh event loop for async tests."""
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


# ── Fixture: Session file cleanup ─────────────────────────────────
@pytest.fixture
def clean_sessions_file(tmppath):
    """Ensure sessions.json is clean for each test."""
    sessions_file = tmppath / "sessions.json"
    if sessions_file.exists():
        sessions_file.unlink()
    return sessions_file
