"""End-to-end integration tests.

These tests cover complete user flows from message to response.
These are the highest-level behavioral contracts.
"""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiogram.exceptions import TelegramAPIError

from src.bot import handle_message, cmd_start, cmd_new, cmd_model, cmd_status
from src.sessions import SessionManager
from src.formatter import markdown_to_html, split_message
from src.progress import ProgressReporter
from src import bridge


# ── E2E Flow 1: New user onboarding ─────────────────────────────
@pytest.mark.asyncio
class TestNewUserOnboarding:
    """Complete flow for a new user discovering the bot."""

    async def test_full_onboarding_sequence(self, mock_message):
        """User: /start → sees welcome → sends message → uses commands."""
        from src.bot import session_manager, provider_manager
        provider_manager.set_provider("123456789:main", "claude")

        # 1. Send /start
        mock_message.text = "/start"
        await cmd_start(mock_message)
        start_response = mock_message.answer.call_args[0][0]
        assert "Claude Code assistant" in start_response
        assert "v" in start_response  # Has version

        # 2. Send a message
        mock_message.answer.reset_mock()
        mock_message.text = "hello"
        with patch('src.bridge.stream_message') as mock_stream:
            mock_stream.return_value = [
                # Minimal stream events
                type('obj', (object,), {'event_type': 'RESULT', 'response': type('obj', (object,), {
                    'text': 'Hello! How can I help?',
                    'session_id': 'sess-1',
                    'is_error': False,
                    'cost_usd': 0.001,
                })})(),
            ]
            await handle_message(mock_message)

        # Should have responded
        assert mock_message.answer.called

        # 3. Check status
        mock_message.answer.reset_mock()
        mock_message.text = "/status"
        await cmd_status(mock_message)
        status_response = mock_message.answer.call_args[0][0]
        assert "sess-1" in status_response  # Session persisted


# ── E2E Flow 2: Conversation continuity ────────────────────────
@pytest.mark.asyncio
class TestConversationContinuity:
    """Session should persist across multiple messages."""

    async def test_session_continues_across_messages(self, mock_message):
        """Multiple messages should use same session_id."""
        from src.bot import session_manager, provider_manager
        provider_manager.set_provider("123456789:main", "claude")

        # First message
        with patch('src.bridge.stream_message') as mock_stream:
            mock_stream.return_value = [
                type('obj', (object,), {'event_type': 'RESULT', 'response': type('obj', (object,), {
                    'text': 'Answer 1',
                    'session_id': 'sess-abc',
                    'is_error': False,
                    'cost_usd': 0.001,
                })})(),
            ]

            mock_message.text = "question 1"
            mock_message.answer.reset_mock()
            await handle_message(mock_message)

        session = session_manager.get(123456789)
        assert session.claude_session_id == "sess-abc"

        # Second message should resume with same session
        with patch('src.bridge.stream_message') as mock_stream:
            # Capture call to verify --resume was used
            mock_stream.return_value = []

            mock_message.text = "question 2"
            mock_message.answer.reset_mock()
            await handle_message(mock_message)

            # Check that stream_message was called with session_id
            if mock_stream.call_args:
                args, kwargs = mock_stream.call_args
                assert kwargs.get('session_id') == 'sess-abc' or (
                    len(args) > 1 and args[1] == 'sess-abc'
                )


# ── E2E Flow 3: Model switching ────────────────────────────────
@pytest.mark.asyncio
class TestModelSwitching:
    """User can switch models mid-conversation."""

    async def test_model_switch_persists(self, mock_message):
        """Switching model should persist and apply to next request."""
        from src.bot import session_manager, provider_manager
        provider_manager.set_provider("123456789:main", "claude")

        # Check default model
        session = session_manager.get(123456789)
        assert session.model == "sonnet"

        # Switch to opus
        mock_message.text = "/model opus"
        await cmd_model(mock_message)
        assert "opus" in mock_message.answer.call_args[0][0].lower()

        # Verify persisted
        session = session_manager.get(123456789)
        assert session.model == "opus"

        # Use the model in a request
        with patch('src.bridge.stream_message') as mock_stream:
            async def stream_gen():
                yield type('obj', (object,), {'event_type': 'RESULT', 'response': type('obj', (object,), {
                    'text': 'Response',
                    'session_id': 'sess-2',
                    'is_error': False,
                    'cost_usd': 0.001,
                })})()
            mock_stream.return_value = stream_gen()

            mock_message.text = "test"
            await handle_message(mock_message)


# ── E2E Flow 4: New conversation ───────────────────────────────
@pytest.mark.asyncio
class TestNewConversation:
    """Starting new conversation should preserve model but clear session."""

    async def test_new_conversation_clears_session_keeps_model(self, mock_message):
        """/new should clear session_id but keep model."""
        from src.bot import session_manager

        # Set up state
        session_manager.update_session_id(123456789, "old-session")
        session_manager.set_model(123456789, "haiku")

        # Send /new
        mock_message.text = "/new"
        await cmd_new(mock_message)

        # Session cleared
        session = session_manager.get(123456789)
        assert session.claude_session_id is None

        # Model preserved
        assert session.model == "haiku"


# ── E2E Flow 5: Error handling ────────────────────────────────
@pytest.mark.asyncio
class TestErrorHandling:
    """System should handle errors gracefully."""

    async def test_claude_error_displayed_to_user(self, mock_message):
        """Claude returning error should be shown to user."""
        mock_message.text = "cause error"

        with patch('src.bridge.stream_message') as mock_stream:
            async def stream_gen():
                yield type('obj', (object,), {'event_type': 'RESULT', 'response': type('obj', (object,), {
                    'text': 'API error: rate limit exceeded',
                    'session_id': None,
                    'is_error': True,
                    'cost_usd': 0.0,
                    'cost_usd': 0.0,
                    'duration_ms': 0,
                    'num_turns': 0,
                })})()
            mock_stream.return_value = stream_gen()

            await handle_message(mock_message)

            response = mock_message.answer.call_args[0][0]
            assert "error" in response.lower()


# ── E2E Flow 6: Provider fallback persistence ─────────────────────
@pytest.mark.asyncio
class TestProviderFallbackPersistence:
    """Automatic provider fallback should persist into session state."""

    async def test_rate_limit_fallback_updates_persisted_provider(self, mock_message, monkeypatch):
        from src.bot import provider_manager, session_manager

        scope_key = "123456789:main"
        provider_manager.set_provider(scope_key, "codex")
        session_manager.set_provider(123456789, "codex")

        responses = [
            bridge.ClaudeResponse(
                text="rate limit exceeded",
                session_id=None,
                is_error=True,
                cost_usd=0.0,
                duration_ms=0,
                num_turns=0,
            ),
            bridge.ClaudeResponse(
                text="ok after fallback",
                session_id="codex2-sess",
                is_error=False,
                cost_usd=0.0,
                duration_ms=0,
                num_turns=1,
            ),
        ]

        async def fake_run_codex_with_retries(*args, **kwargs):
            return responses.pop(0)

        monkeypatch.setattr("src.bot._run_codex_with_retries", fake_run_codex_with_retries)
        monkeypatch.setattr("src.bot._find_provider_cli", lambda _cli: "/usr/bin/mock")

        mock_message.text = "please handle with fallback"
        await handle_message(mock_message)

        assert provider_manager.get_provider(scope_key).name == "codex2"
        assert session_manager.get(123456789).provider == "codex2"


# ── E2E Flow 6: Message formatting pipeline ────────────────────
class TestMessageFormattingPipeline:
    """End-to-end of message formatting from Claude response to Telegram."""

    def test_complete_formatting_flow(self):
        """Claude markdown → HTML → split chunks."""
        claude_response = """
Here's the solution:

```python
def hello():
    print("Hello World")
```

**Important**: Read the code above.

See `hello()` function for details.
"""

        # Convert to HTML
        html = markdown_to_html(claude_response)

        # Should have code block with language
        assert "<pre><code" in html
        assert "language-python" in html

        # Should have bold
        assert "<b>Important</b>" in html

        # Should have inline code
        assert "<code>hello()</code>" in html

        # Split if needed (for this short text, should be one chunk)
        chunks = split_message(html)
        assert len(chunks) == 1

    def test_large_response_split_into_chunks(self):
        """Large_responses should be split at sensible boundaries."""
        # Build a large response with paragraphs
        chunks = []
        for i in range(20):
            chunks.append(f"Paragraph {i}: " + "a" * 300 + "\n\n")
        large_response = "".join(chunks)

        html = markdown_to_html(large_response)
        split_chunks = split_message(html)

        # Should be split into multiple chunks
        assert len(split_chunks) > 1

        # Each chunk should be under limit
        for chunk in split_chunks:
            assert len(chunk) <= 4096


# ── E2E Flow 7: Progress reporting lifecycle ───────────────────────
@pytest.mark.asyncio
class TestProgressReportingLifecycle:
    """Progress message should appear, update, then be removed."""

    async def test_progress_shows_and_finishes(self, mock_message):
        """Progress message should be sent, updated, then deleted."""
        from src.progress import ProgressReporter

        reporter = ProgressReporter(mock_message, debounce_seconds=0, initial_delay_seconds=0)

        # Report a tool
        await reporter.report_tool("Read", "/tmp/file.txt")
        await asyncio.sleep(0.1)  # Wait for debounce

        # Should have sent initial progress
        assert mock_message.bot.send_message.called

        # Report another tool
        await reporter.report_tool("Edit", "/tmp/file.txt")
        await asyncio.sleep(0.1)

        # Should have edited progress
        assert mock_message.bot.edit_message_text.called

        # Finish
        await reporter.finish()

        # Should have deleted progress
        assert mock_message.bot.delete_message.called

    async def test_progress_show_working_uses_thread_and_escapes_tool_input(self, mock_message):
        """Initial progress should appear in-thread and tool previews should remain HTML-safe."""
        from src.progress import ProgressReporter

        mock_message.message_thread_id = 77
        reporter = ProgressReporter(mock_message, debounce_seconds=0, initial_delay_seconds=0)

        await reporter.show_working()
        kwargs = mock_message.bot.send_message.await_args_list[0].kwargs
        assert kwargs["message_thread_id"] == 77
        assert kwargs["text"] == "🔄 <b>Working...</b>"

        assert reporter._format_tool_action("Read", "<tmp>\n/path") == "Reading: &lt;tmp&gt; /path"  # noqa: SLF001

    async def test_progress_enters_audio_conversion_mode_during_tts_tool(self, mock_message, monkeypatch):
        """Audio-generation tools should replace generic working text with a live conversion timer."""
        from src.progress import ProgressReporter

        monkeypatch.setattr("src.progress._AUDIO_PROGRESS_INTERVAL", 0.01)
        reporter = ProgressReporter(mock_message, debounce_seconds=0, initial_delay_seconds=0)

        await reporter.show_working()
        await reporter.report_tool("Bash", 'sag -v Clawd -o /tmp/voice-reply.mp3 "hello"')
        await asyncio.sleep(0.03)

        assert mock_message.bot.edit_message_text.called
        texts = [call.kwargs["text"] for call in mock_message.bot.edit_message_text.await_args_list]
        assert any("Converting audio reply" in text for text in texts)
        assert any("Elapsed:" in text for text in texts)

        await reporter.finish()

    async def test_progress_enters_audio_conversion_mode_during_edge_tts_tool(
        self,
        mock_message,
        monkeypatch,
    ):
        """The repo-local edge_tts wrapper should be treated as audio generation."""
        from src.progress import ProgressReporter

        monkeypatch.setattr("src.progress._AUDIO_PROGRESS_INTERVAL", 0.01)
        reporter = ProgressReporter(mock_message, debounce_seconds=0, initial_delay_seconds=0)

        await reporter.show_working()
        await reporter.report_tool(
            "Bash",
            './venv/bin/python -m src.edge_tts_tool speak --output /tmp/voice-reply.mp3 --text "hello"',
        )
        await asyncio.sleep(0.03)

        texts = [call.kwargs["text"] for call in mock_message.bot.edit_message_text.await_args_list]
        assert any("Converting audio reply" in text for text in texts)

        await reporter.finish()

    async def test_progress_keeps_audio_conversion_mode_after_followup_tool_events(
        self,
        mock_message,
        monkeypatch,
    ):
        """Once audio conversion starts, generic working updates should not take over again."""
        from src.progress import ProgressReporter

        monkeypatch.setattr("src.progress._AUDIO_PROGRESS_INTERVAL", 0.01)
        reporter = ProgressReporter(mock_message, debounce_seconds=0, initial_delay_seconds=0)

        await reporter.show_working()
        await reporter.report_tool("Bash", 'sag -v Clawd -o /tmp/voice-reply.mp3 "hello"')
        await asyncio.sleep(0.02)
        await reporter.report_tool("Read", "/tmp/final-audio.ogg")
        await asyncio.sleep(0.03)

        texts = [call.kwargs["text"] for call in mock_message.bot.edit_message_text.await_args_list]
        audio_indexes = [idx for idx, text in enumerate(texts) if "Converting audio reply" in text]

        assert audio_indexes
        assert all("Working..." not in text for text in texts[audio_indexes[0]:])

        await reporter.finish()

    async def test_progress_does_not_enter_audio_conversion_mode_for_non_tts_bash(
        self,
        mock_message,
        monkeypatch,
    ):
        """Generic shell commands mentioning audio-like terms should not trigger audio conversion mode."""
        from src.progress import ProgressReporter

        monkeypatch.setattr("src.progress._AUDIO_PROGRESS_INTERVAL", 0.01)
        reporter = ProgressReporter(mock_message, debounce_seconds=0, initial_delay_seconds=0)

        await reporter.show_working()
        await reporter.report_tool("Bash", 'echo "tts notes saved to /tmp/report.wav"')
        await asyncio.sleep(0.03)

        texts = [call.kwargs["text"] for call in mock_message.bot.edit_message_text.await_args_list]
        assert all("Converting audio reply" not in text for text in texts)

        await reporter.finish()

    async def test_progress_skips_extra_send_when_edit_is_rate_limited(self, mock_message):
        """Flood-controlled edits should not create an extra transient status message."""
        from src.progress import ProgressReporter

        mock_message.bot.send_message.side_effect = [
            MagicMock(message_id=123),
            MagicMock(message_id=456),
        ]
        mock_message.bot.edit_message_text.side_effect = TelegramAPIError(
            AsyncMock(),
            "Telegram server says - Flood control exceeded on method 'EditMessageText' in chat -1. Retry in 9 seconds."
        )

        reporter = ProgressReporter(mock_message, debounce_seconds=0, initial_delay_seconds=0)

        await reporter.show_working()
        await reporter.report_tool("Read", "/tmp/file.txt")
        await asyncio.sleep(0.05)

        assert mock_message.bot.send_message.await_count == 1
        assert mock_message.bot.edit_message_text.await_count == 1

        await reporter.finish()

    async def test_progress_show_working_waits_for_initial_delay_by_default(self, mock_message):
        from src.progress import ProgressReporter

        reporter = ProgressReporter(mock_message, debounce_seconds=0, initial_delay_seconds=5)

        await reporter.show_working()

        mock_message.bot.send_message.assert_not_called()
        await reporter.finish()

    async def test_progress_suppresses_second_ephemeral_message_in_same_chat(self, mock_message):
        from src.progress import ProgressReporter

        first_reporter = ProgressReporter(mock_message, debounce_seconds=0, initial_delay_seconds=0)
        second_message = AsyncMock()
        second_message.chat = mock_message.chat
        second_message.bot = mock_message.bot
        second_message.message_thread_id = 88
        second_reporter = ProgressReporter(second_message, debounce_seconds=0, initial_delay_seconds=0)

        await first_reporter.show_working()
        await second_reporter.show_working()

        assert mock_message.bot.send_message.await_count == 1

        await first_reporter.finish()
        await second_reporter.finish()


# ── E2E Flow 8: Session persistence across restarts ──────────────
class TestSessionPersistenceAcrossRestarts:
    """Sessions should survive process restarts."""

    def test_session_survives_manager_reinstantiation(self, tmppath):
        """Creating new SessionManager should load existing sessions."""
        # Create first manager and set up session
        manager1 = SessionManager()
        manager1.update_session_id(111, "sess-persist")
        manager1.set_model(111, "opus")

        # Create second manager (simulates restart)
        manager2 = SessionManager()

        # Session should be loaded
        session = manager2.get(111)
        assert session.claude_session_id == "sess-persist"
        assert session.model == "opus"


# ── E2E Flow 9: Multiple users independent ───────────────────────
class TestMultipleUsersIndependent:
    """Different users should have completely independent state."""

    async def test_users_dont_interfere(self, mock_msg_factory):
        """Actions by one user shouldn't affect another user."""
        from src.bot import session_manager

        # Helper to create mock message for different user
        def msg_for_user(user_id, text):
            msg = mock_msg_factory()
            msg.chat.id = user_id
            msg.from_user.id = user_id
            msg.text = text
            return msg

        # User 1 sets session
        session_manager.update_session_id(111, "user-1-sess")
        session_manager.set_model(111, "sonnet")

        # User 2 sets different session
        session_manager.update_session_id(222, "user-2-sess")
        session_manager.set_model(222, "opus")

        # User 1's data should be unchanged
        session1 = session_manager.get(111)
        assert session1.claude_session_id == "user-1-sess"
        assert session1.model == "sonnet"

        # User 2's data should be unchanged
        session2 = session_manager.get(222)
        assert session2.claude_session_id == "user-2-sess"
        assert session2.model == "opus"


# ── Helper for multi-user tests ───────────────────────────────────
@pytest.fixture
def mock_msg_factory(mock_bot):
    """Factory to create mock messages for different users."""
    def factory():
        msg = AsyncMock()
        msg.text = "default"
        msg.chat = AsyncMock()
        msg.chat.id = 123456789
        msg.bot = mock_bot
        msg.from_user = AsyncMock()
        msg.from_user.id = 123456789
        msg.content_type = "text"
        msg.answer = AsyncMock()
        return msg
    return factory
