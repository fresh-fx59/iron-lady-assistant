# Integration Tests

These tests define **observable input-output contracts** for the Telegram Claude Code bot. They are designed to be language-agnostic, making them ideal for verifying that a rewrite to another language preserves the behavior.

## Philosophy

These tests focus on **observable behavior** rather than implementation details:

- **Input → Output**: What messages go in, what comes out
- **State persistence**: How sessions are stored and retrieved
- **External interactions**: Telegram API, Claude CLI subprocess
- **Error conditions**: How the system handles failures

When rewriting to a new language, if these tests pass, the new implementation is functionally equivalent from a user's perspective.

## Test Structure

```
tests/
├── conftest.py              # Shared fixtures (mocks, env setup)
├── test_sessions_integration.py   # Session persistence contracts
├── test_formatter_integration.py   # Markdown → HTML conversion
├── test_bridge_integration.py      # Claude CLI streaming contracts
├── test_bot_commands_integration.py # Bot commands (/start, /new, etc.)
└── test_e2e.py              # End-to-end user flows
```

## Running Tests

```bash
# Install test dependencies
pip install -r requirements-test.txt

# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_sessions_integration.py

# Run with verbose output
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

## Test Categories

### 1. Session Persistence (`test_sessions_integration.py`)

Tests for the `sessions.json` persistence layer:

- Session creation and retrieval
- Saving/loading across restarts
- Handling corrupted files
- Multiple user isolation

**Contract**: The same `sessions.json` file can be loaded by the Python implementation and any future rewrite to get identical session state.

### 2. Formatter (`test_formatter_integration.py`)

Tests for markdown-to-HTML conversion and message splitting:

- Markdown syntax (bold, italic, strikethrough, code blocks)
- HTML escaping for security
- Message splitting at 4096 characters
- Smart splitting at paragraph/line/space boundaries

**Contract**: Given the same markdown input, any implementation should produce the same HTML output and same chunks.

### 3. Bridge (`test_bridge_integration.py`)

Tests for communication with the Claude CLI subprocess:

- Parsing stream-json output events
- Tool input extraction
- Process lifecycle (start, timeout, error handling)
- Command-line argument construction

**Contract**: Any implementation must send the same CLI arguments and parse the same JSON output format to produce equivalent events.

### 4. Bot Commands (`test_bot_commands_integration.py`)

Tests for command handlers:

- `/start` - Welcome message
- `/new` - Clear conversation
- `/model [sonnet|opus|haiku]` - Switch model
- `/status` - Show session info
- `/cancel` - Cancel running request

**Contract**: Authorized users get responses matching expectations; unauthorized users get nothing.

### 5. End-to-End (`test_e2e.py`)

Complete user flows:

- New user onboarding
- Multi-turn conversation continuity
- Model switching mid-conversation
- Error handling
- Multiple user isolation

**Contract**: Real-world user interactions should work identically.

## Writing Contract Tests

When adding new features, add tests that define the **observable contract**:

```python
# ❌ BAD - Tests implementation detail
def test_session_internal_dict_size():
    assert len(session_manager._sessions) == 1

# ✅ GOOD - Tests observable behavior
def test_new_user_has_empty_session_id():
    session = session_manager.get(12345)
    assert session.claude_session_id is None
```

## Language Rewrite Strategy

When porting to a new language:

1. **Run the Python tests first** - Establish baseline pass
2. **Port the tests** to the new language (same assertions, same fixtures)
3. **Implement the feature** in the new language
4. **Make tests pass** - The new implementation should produce identical outputs
5. **Compare sessions.json** - Ensure format is compatible for zero-downtime cutover

## Mocking Strategy

Tests use mocks (not integration with real services):

- **Telegram API**: Mocked to verify correct method calls
- **Claude CLI**: Mocked subprocess with controlled JSON output
- **Filesystem**: Uses temporary directories (via `tmpdir` fixture)

This ensures tests run fast and are deterministic.