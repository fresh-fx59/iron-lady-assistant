"""Integration tests for bridge/subprocess contract.

These tests define the expected behavior of streaming events from Claude CLI.
These are the core integration contracts that must be preserved.
"""

import asyncio
import os
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.bridge import (
    stream_message,
    stream_codex_message,
    StreamEventType,
    StreamEvent,
    ClaudeResponse,
    _extract_tool_input,
    _extract_tool_input_partial,
)


# ── Contract 1: Event types and data structures ──────────────────
class TestEventTypes:
    """StreamEvent and ClaudeResponse should have correct structure."""

    def test_stream_event_tool_use_structure(self):
        """TOOL_USE event should have tool_name and optionally tool_input."""
        event = StreamEvent(
            event_type=StreamEventType.TOOL_USE,
            tool_name="Bash",
            tool_input="echo hello"
        )
        assert event.event_type == StreamEventType.TOOL_USE
        assert event.tool_name == "Bash"
        assert event.tool_input == "echo hello"

    def test_stream_event_result_structure(self):
        """RESULT event should have response with ClaudeResponse."""
        response = ClaudeResponse(
            text="Hello",
            session_id="sess-123",
            is_error=False,
            cost_usd=0.001,
        )
        event = StreamEvent(event_type=StreamEventType.RESULT, response=response)
        assert event.event_type == StreamEventType.RESULT
        assert event.response is not None
        assert event.response.text == "Hello"

    def test_claude_response_default_values(self):
        """ClaudeResponse should have sensible defaults."""
        response = ClaudeResponse(
            text="test",
            session_id="sess",
            is_error=False,
            cost_usd=0.0,
        )
        assert response.duration_ms == 0
        assert response.num_turns == 0
        assert response.cancelled is False
        assert response.idle_timeout is False


# ── Contract 2: Tool input extraction ────────────────────────────
class TestToolInputExtraction:
    """Tool input extraction should extract the primary meaningful field."""

    def test_bash_extracts_command(self):
        """Bash tool should extract the command field."""
        result = _extract_tool_input("Bash", {"command": "ls -la /tmp"})
        assert result == "ls -la /tmp"

    def test_bash_truncates_long_command(self):
        """Long bash commands should be truncated."""
        cmd = "echo " + "x" * 200
        result = _extract_tool_input("Bash", {"command": cmd})
        assert len(result) <= 83  # "echo " + 80 chars + "..."
        assert result.endswith("...")

    def test_read_extracts_file_path(self):
        """Read tool should extract file_path."""
        result = _extract_tool_input("Read", {"file_path": "/tmp/test.txt"})
        assert result == "/tmp/test.txt"

    def test_edit_extracts_file_path(self):
        """Edit tool should extract file_path."""
        result = _extract_tool_input("Edit", {"file_path": "/tmp/test.py", "old_string": "line1"})
        assert result == "/tmp/test.py"

    def test_write_extracts_file_path(self):
        """Write tool should extract file_path."""
        result = _extract_tool_input("Write", {"file_path": "/tmp/new.txt", "content": "hello"})
        assert result == "/tmp/new.txt"

    def test_grep_extracts_pattern(self):
        """Grep tool should extract pattern."""
        result = _extract_tool_input("Grep", {"pattern": "TODO", "path": "."})
        assert result == "TODO"

    def test_glob_extracts_pattern(self):
        """Glob tool should extract pattern."""
        result = _extract_tool_input("Glob", {"pattern": "**/*.py"})
        assert result == "**/*.py"

    def test_task_extracts_description(self):
        """Task tool should extract description."""
        result = _extract_tool_input("Task", {"description": "Fix the bug in parser"})
        assert result == "Fix the bug in parser"

    def test_askuserquestion_extracts_description(self):
        """AskUserQuestion tool should extract description."""
        result = _extract_tool_input("AskUserQuestion", {
            "questions": [{"question": "Continue?", "header": "Confirm"}]
        })
        # Description in questions might be empty, should handle gracefully
        assert result is not None or result == ""

    def test_unknown_tool_uses_json_dump(self):
        """Unknown tools should JSON-serialize the input."""
        result = _extract_tool_input("UnknownTool", {"foo": "bar", "baz": 123})
        assert '"foo":"bar"' in result
        assert '"baz":123' in result

    def test_empty_input_returns_none(self):
        """Empty input dict should return None."""
        result = _extract_tool_input("Bash", {})
        assert result is None


# ── Contract 3: Partial tool input extraction (streaming) ────────
class TestPartialToolInput:
    """Extraction from incomplete/partial JSON during streaming."""

    def test_partial_json_parses_when_valid(self):
        """Partial JSON that happens to be valid should be parsed."""
        partial = '{"command": "ls"}'
        result = _extract_tool_input_partial("Bash", partial)
        assert result == "ls"

    def test_partial_fallback_regex_for_bash(self):
        """Should use regex fallback for partial bash command."""
        partial = '{"command": "ls -la'
        result = _extract_tool_input_partial("Bash", partial)
        assert "ls -la" in result

    def test_partial_fallback_regex_for_file_tools(self):
        """Should use regex for file path tools."""
        partial = '{"file_path": "/tmp/test'
        result = _extract_tool_input_partial("Read", partial)
        assert "/tmp/test" in result

    def test_partial_truncates_extracted_field(self):
        """Extracted partial field should be truncated."""
        long_field = "x" * 200
        partial = f'{{"command": "{long_field}"'
        result = _extract_tool_input_partial("Bash", partial)
        assert len(result) <= 203  # max_len + "..." for bash (80)


# ── Contract 4: Subprocess environment ────────────────────────────
class TestSubprocessEnvironment:
    """Subprocess should have clean environment."""

    def test_strips_claudecode_env_var(self):
        """Should strip CLAUDECODE from env to avoid nested session guard."""
        from src.bridge import _subprocess_env

        # Set CLAUDECODE
        import os
        original = os.environ.get("CLAUDECODE")
        os.environ["CLAUDECODE"] = "nested-session"

        env = _subprocess_env()

        assert "CLAUDECODE" not in env
        assert "PATH" in env  # Other env vars should be preserved

        # Restore original
        if original is None:
            os.environ.pop("CLAUDECODE", None)
        else:
            os.environ["CLAUDECODE"] = original

    def test_adds_user_npm_bin_to_path(self, monkeypatch, tmp_path):
        """Should include the per-user npm bin path for Codex-style CLIs."""
        from src.bridge import _subprocess_env

        npm_bin = tmp_path / ".npm-tester" / "bin"
        npm_bin.mkdir(parents=True)
        local_bin = tmp_path / ".local" / "bin"
        local_bin.mkdir(parents=True)

        monkeypatch.setenv("PATH", "/usr/bin")
        monkeypatch.setattr("src.bridge.getpass.getuser", lambda: "tester")
        monkeypatch.setattr("src.bridge.Path.home", lambda: tmp_path)

        env = _subprocess_env()
        parts = env["PATH"].split(os.pathsep)

        assert str(npm_bin) in parts
        assert str(local_bin) in parts
        assert "/usr/bin" in parts


# ── Contract 5: Stream event parsing ──────────────────────────────
@pytest.mark.asyncio
class TestStreamEventParsing:
    """Should correctly parse various stream event types from Claude CLI."""

    async def test_yields_text_response(self, mock_subprocess_lines, mock_successful_response):
        """Should extract and yield text from successful response."""
        lines = mock_subprocess_lines(mock_successful_response)

        with patch('asyncio.create_subprocess_exec') as mock_subproc:
            proc = AsyncMock()
            proc.returncode = 0
            proc.stdout = AsyncMock()
            proc.stdout.readline = AsyncMock(side_effect=lines + [b""])
            proc.stderr = AsyncMock()
            proc.stderr.read = AsyncMock(return_value=b"")
            proc.wait = AsyncMock(return_value=0)
            mock_subproc.return_value = proc

            events = []
            async for event in stream_message("hello"):
                events.append(event)

            assert len(events) == 1
            assert events[0].event_type == StreamEventType.RESULT
            assert events[0].response.text == "Hello"
            assert events[0].response.session_id == "sess-123"
            assert not events[0].response.is_error

    async def test_yields_tool_use_events(self, mock_subprocess_lines, mock_tool_use_response):
        """Should yield TOOL_USE events for tool invocation."""
        lines = mock_subprocess_lines(mock_tool_use_response)

        with patch('asyncio.create_subprocess_exec') as mock_subproc:
            proc = AsyncMock()
            proc.returncode = 0
            proc.stdout = AsyncMock()
            proc.stdout.readline = AsyncMock(side_effect=lines + [b""])
            proc.stderr = AsyncMock()
            proc.stderr.read = AsyncMock(return_value=b"")
            proc.wait = AsyncMock(return_value=0)
            mock_subproc.return_value = proc

            events = []
            async for event in stream_message("read file"):
                events.append(event)

            # Should have at least one TOOL_USE event
            tool_events = [e for e in events if e.event_type == StreamEventType.TOOL_USE]
            # Due to stream_event + assistant, may have multiple
            assert len(tool_events) >= 1

            # Should also have RESULT
            result_events = [e for e in events if e.event_type == StreamEventType.RESULT]
            assert len(result_events) == 1

    async def test_handles_is_error_true(self, mock_subprocess_lines, mock_error_response):
        """Should set is_error=True when Claude returns an error."""
        lines = mock_subprocess_lines(mock_error_response)

        with patch('asyncio.create_subprocess_exec') as mock_subproc:
            proc = AsyncMock()
            proc.returncode = 0
            proc.stdout = AsyncMock()
            proc.stdout.readline = AsyncMock(side_effect=lines + [b""])
            proc.stderr = AsyncMock()
            proc.stderr.read = AsyncMock(return_value=b"")
            proc.wait = AsyncMock(return_value=0)
            mock_subproc.return_value = proc

            events = []
            async for event in stream_message("error"):
                events.append(event)

            assert len(events) == 1
            assert events[0].response.is_error
            assert events[0].response.text == "API error occurred"

    async def test_ignores_system_and_user_events(self, mock_subprocess_lines):
        """Should skip system and user event types."""
        lines = mock_subprocess_lines([
            {"type": "system", "session_id": "sess"},
            {"type": "user", "content": "tool_result"},
            {"type": "result", "result": "final", "session_id": "sess",
             "is_error": False, "total_cost_usd": 0.0, "num_turns": 0, "duration_ms": 0}
        ])

        with patch('asyncio.create_subprocess_exec') as mock_subproc:
            proc = AsyncMock()
            proc.returncode = 0
            proc.stdout = AsyncMock()
            proc.stdout.readline = AsyncMock(side_effect=lines + [b""])
            proc.stderr = AsyncMock()
            proc.stderr.read = AsyncMock(return_value=b"")
            proc.wait = AsyncMock(return_value=0)
            mock_subproc.return_value = proc

            events = []
            async for event in stream_message("test"):
                events.append(event)

            assert len(events) == 1
            assert events[0].event_type == StreamEventType.RESULT


# ── Contract 6: Process exit handling ─────────────────────────────
@pytest.mark.asyncio
class TestProcessExitHandling:
    """Should handle process exit gracefully in various scenarios."""

    async def test_nonzero_exit_code_returns_error(self, mock_subprocess_lines):
        """Process with non-zero exit should return error response."""
        # No result event, process exits with error
        lines = mock_subprocess_lines([
            {"type": "system", "session_id": "sess"}
        ])

        with patch('asyncio.create_subprocess_exec') as mock_subproc:
            proc = AsyncMock()
            proc.returncode = 1  # Error code
            proc.stdout = AsyncMock()
            proc.stdout.readline = AsyncMock(side_effect=lines + [b""])
            proc.stderr = AsyncMock()
            proc.stderr.read = AsyncMock(return_value=b"Something went wrong")
            proc.wait = AsyncMock(return_value=1)
            mock_subproc.return_value = proc

            events = []
            async for event in stream_message("test"):
                events.append(event)

            assert len(events) == 1
            assert events[0].response.is_error
            assert "Something went wrong" in events[0].response.text

    async def test_no_result_event_treated_as_error(self, mock_subprocess_lines):
        """Process exiting without result event should return error."""
        lines = mock_subprocess_lines([
            {"type": "stream_event", "event": {"type": "content_block_start",
             "content_block": {"type": "text"}}}
        ])

        with patch('asyncio.create_subprocess_exec') as mock_subproc:
            proc = AsyncMock()
            proc.returncode = 0
            proc.stdout = AsyncMock()
            proc.stdout.readline = AsyncMock(side_effect=lines + [b""])
            proc.stderr = AsyncMock()
            proc.stderr.read = AsyncMock(return_value=b"")
            proc.wait = AsyncMock(return_value=0)
            mock_subproc.return_value = proc

            events = []
            async for event in stream_message("test"):
                events.append(event)

            assert len(events) == 1
            assert events[0].response.is_error
            assert "exited without producing a result" in events[0].response.text.lower()


# ── Contract 7: Command line arguments ────────────────────────────
@pytest.mark.asyncio
class TestCommandLineArgs:
    """Should pass correct arguments to claude subprocess."""

    async def test_default_args(self, mock_subprocess_lines, mock_successful_response):
        """Should include basic required flags."""
        lines = mock_subprocess_lines(mock_successful_response)

        with patch('asyncio.create_subprocess_exec') as mock_subproc:
            proc = AsyncMock()
            proc.returncode = 0
            proc.stdout = AsyncMock()
            proc.stdout.readline = AsyncMock(side_effect=lines + [b""])
            proc.stderr = AsyncMock()
            proc.stderr.read = AsyncMock(return_value=b"")
            proc.wait = AsyncMock(return_value=0)
            mock_subproc.return_value = proc

            async for _ in stream_message("hello"):
                break

            # Check call args
            args, kwargs = mock_subproc.call_args
            assert args[0] == "claude"
            assert args[1] == "-p"
            assert args[2] == "hello"
            assert "--output-format" in args
            assert "stream-json" in args
            assert "--verbose" in args
            assert "--include-partial-messages" in args
            assert "--model" in args
            assert "--dangerously-skip-permissions" in args

    async def test_resume_flag_with_session_id(self, mock_subprocess_lines, mock_successful_response):
        """Should include --resume when session_id is provided."""
        lines = mock_subprocess_lines(mock_successful_response)

        with patch('asyncio.create_subprocess_exec') as mock_subproc:
            proc = AsyncMock()
            proc.returncode = 0
            proc.stdout = AsyncMock()
            proc.stdout.readline = AsyncMock(side_effect=lines + [b""])
            proc.stderr = AsyncMock()
            proc.stderr.read = AsyncMock(return_value=b"")
            proc.wait = AsyncMock(return_value=0)
            mock_subproc.return_value = proc

            async for _ in stream_message("hello", session_id="sess-123"):
                break

            args, _ = mock_subproc.call_args
            assert "--resume" in args
            resume_idx = args.index("--resume")
            assert args[resume_idx + 1] == "sess-123"

    async def test_model_flag_respects_model_param(self, mock_subprocess_lines, mock_successful_response):
        """Should pass the correct model argument."""
        lines = mock_subprocess_lines(mock_successful_response)

        with patch('asyncio.create_subprocess_exec') as mock_subproc:
            proc = AsyncMock()
            proc.returncode = 0
            proc.stdout = AsyncMock()
            proc.stdout.readline = AsyncMock(side_effect=lines + [b""])
            proc.stderr = AsyncMock()
            proc.stderr.read = AsyncMock(return_value=b"")
            proc.wait = AsyncMock(return_value=0)
            mock_subproc.return_value = proc

            async for _ in stream_message("hello", model="opus"):
                break

            args, _ = mock_subproc.call_args
            model_idx = args.index("--model")
            assert args[model_idx + 1] == "opus"

    async def test_working_dir_passed_to_subprocess(self, mock_subprocess_lines, mock_successful_response):
        """Should pass cwd to subprocess when specified."""
        from pathlib import Path

        lines = mock_subprocess_lines(mock_successful_response)

        with patch('asyncio.create_subprocess_exec') as mock_subproc:
            proc = AsyncMock()
            proc.returncode = 0
            proc.stdout = AsyncMock()
            proc.stdout.readline = AsyncMock(side_effect=lines + [b""])
            proc.stderr = AsyncMock()
            proc.stderr.read = AsyncMock(return_value=b"")
            proc.wait = AsyncMock(return_value=0)
            mock_subproc.return_value = proc

            async for _ in stream_message("hello", working_dir="/tmp/test"):
                break

            _, kwargs = mock_subproc.call_args
            assert kwargs["cwd"] == "/tmp/test"


@pytest.mark.asyncio
class TestCodexCommandLineArgs:
    """Should pass correct arguments to codex subprocess."""

    async def test_codex_resume_defaults_to_subcommand(self):
        """Should use `resume` subcommand when session_id is provided."""
        lines = [
            b'{"type":"item.completed","item":{"type":"assistant_message","text":"hello"}}\n'
        ]

        with patch('asyncio.create_subprocess_exec') as mock_subproc:
            proc = AsyncMock()
            proc.returncode = 0
            proc.stdout = AsyncMock()
            proc.stdout.readline = AsyncMock(side_effect=lines + [b""])
            proc.stderr = AsyncMock()
            proc.stderr.read = AsyncMock(return_value=b"")
            proc.wait = AsyncMock(return_value=0)
            mock_subproc.return_value = proc

            async for _ in stream_codex_message("hello", session_id="sess-123"):
                break

            args, _ = mock_subproc.call_args
            assert args[0] == "codex"
            assert args[1] == "exec"
            assert "resume" in args
            resume_idx = args.index("resume")
            assert args[resume_idx + 1] == "sess-123"

    async def test_codex_resume_honors_legacy_flag(self):
        """Should still support providers configured with `--resume`."""
        lines = [
            b'{"type":"item.completed","item":{"type":"assistant_message","text":"hello"}}\n'
        ]

        with patch('asyncio.create_subprocess_exec') as mock_subproc:
            proc = AsyncMock()
            proc.returncode = 0
            proc.stdout = AsyncMock()
            proc.stdout.readline = AsyncMock(side_effect=lines + [b""])
            proc.stderr = AsyncMock()
            proc.stderr.read = AsyncMock(return_value=b"")
            proc.wait = AsyncMock(return_value=0)
            mock_subproc.return_value = proc

            async for _ in stream_codex_message(
                "hello",
                session_id="sess-123",
                resume_arg="--resume",
            ):
                break

            args, _ = mock_subproc.call_args
            assert "--resume" in args
            resume_idx = args.index("--resume")
            assert args[resume_idx + 1] == "sess-123"

    async def test_codex2_uses_requested_cli_binary(self):
        lines = [
            b'{"type":"item.completed","item":{"type":"assistant_message","text":"hello"}}\n'
        ]

        with patch('asyncio.create_subprocess_exec') as mock_subproc:
            proc = AsyncMock()
            proc.returncode = 0
            proc.stdout = AsyncMock()
            proc.stdout.readline = AsyncMock(side_effect=lines + [b""])
            proc.stderr = AsyncMock()
            proc.stderr.read = AsyncMock(return_value=b"")
            proc.wait = AsyncMock(return_value=0)
            mock_subproc.return_value = proc

            async for _ in stream_codex_message("hello", cli_name="codex2"):
                break

            args, _ = mock_subproc.call_args
            assert args[0] == "codex2"


# ── Contract 8: Cancellation support ──────────────────────────────
@pytest.mark.asyncio
class TestCancellationSupport:
    """Should support cancel via process_handle."""

    async def test_populates_process_handle(self, mock_subprocess_lines, mock_successful_response):
        """Should put process in process_handle for cancellation."""
        lines = mock_subprocess_lines(mock_successful_response)

        with patch('asyncio.create_subprocess_exec') as mock_subproc:
            proc = AsyncMock()
            proc.returncode = 0
            proc.stdout = AsyncMock()
            proc.stdout.readline = AsyncMock(side_effect=lines + [b""])
            proc.stderr = AsyncMock()
            proc.stderr.read = AsyncMock(return_value=b"")
            proc.wait = AsyncMock(return_value=0)
            mock_subproc.return_value = proc

            handle = {}
            async for _ in stream_message("hello", process_handle=handle):
                break

            assert "proc" in handle
            assert handle["proc"] is not None


# ── Contract 9: JSON decode error handling ───────────────────────
@pytest.mark.asyncio
class TestJsonDecodeErrorHandling:
    """Should handle malformed JSON lines gracefully."""

    async def test_skips_invalid_json_lines(self, mock_subprocess_lines):
        """Malformed JSON lines should be skipped with warning."""
        lines = [
            b"invalid json\n",
            b'{"type": "result", "result": "final", "session_id": "sess", "is_error": False, "total_cost_usd": 0.0, "num_turns": 0, "duration_ms": 0}\n'
        ]

        with patch('asyncio.create_subprocess_exec') as mock_subproc:
            proc = AsyncMock()
            proc.returncode = 0
            proc.stdout = AsyncMock()
            proc.stdout.readline = AsyncMock(side_effect=lines + [b""])
            proc.stderr = AsyncMock()
            proc.stderr.read = AsyncMock(return_value=b"")
            proc.wait = AsyncMock(return_value=0)
            mock_subproc.return_value = proc

            events = []
            async for event in stream_message("test"):
                events.append(event)

            # Should still process the valid line
            assert len(events) == 1
            assert events[0].event_type == StreamEventType.RESULT
