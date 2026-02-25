# Claude Code as Telegram Assistant

**Current version: `0.11.8`** — defined in `src/config.py` as `VERSION`.

Telegram bot that bridges messages to Claude Code's `--print` mode via subprocess, providing a conversational AI assistant through Telegram.

## Architecture

- **aiogram 3.x** async Telegram bot with long-polling
- **asyncio subprocess** runs `claude -p` per message with `--output-format stream-json --verbose --include-partial-messages` and `--dangerously-skip-permissions`
- **`--resume <session_id>`** for conversation continuity
- **Streaming output** with idle timeout (default 120s) — checks if subprocess is still alive on timeout; only fails if process actually dies
- **Live progress updates** show current Claude activity (Reading, Editing, Running commands, etc.) with heartbeat animation for long-running tasks
- **Per-chat state** with asyncio.Lock prevents overlapping Claude invocations
- **Persistent memory** — YAML profile + SQLite FTS5 episodic memory, injected as XML context before each message

## Project Structure

```
src/
├── main.py       # Entry point, dispatcher setup, polling, metrics server
├── config.py     # Env vars: BOT_TOKEN, ALLOWED_USER_IDS, DEFAULT_MODEL, IDLE_TIMEOUT, MEMORY_DIR, TOOLS_DIR
├── bot.py        # Telegram handlers: /start, /new, /model, /provider, /status, /memory, /forget, /tools, /cancel
├── memory.py     # Persistent memory: YAML profile + SQLite FTS5 episodic, context injection
├── tools.py      # Tool registry: lazy loads YAML tool definitions, injects context
├── bridge.py     # Runs `claude -p` subprocess, yields stream events (TOOL_USE, RESULT)
├── providers.py  # Provider fallback chain: auto-switches LLM on rate limit
├── progress.py   # ProgressReporter: manages live progress message with debounced edits
├── sessions.py   # Maps chat_id → claude session_id, persists to sessions.json
├── formatter.py  # Markdown→HTML conversion, message splitting
└── metrics.py    # Prometheus metrics: counters, histograms, gauges
```

## Setup

1. Create bot via @BotFather, get token
2. `cp .env.example .env` and fill in values
3. Create a virtual environment and install dependencies:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
4. `python -m src.main` (or use `./run.sh` which activates the venv automatically)

## Bot Commands

- `/start` — Welcome message
- `/new` — Start fresh conversation
- `/model` — Switch model (sonnet|opus|haiku) via inline keyboard
- `/provider` — Switch LLM provider via inline keyboard (auto-switches on rate limit)
- `/status` — Show current session info
- `/memory` — Show what the bot remembers (profile + episodes)
- `/tools` — Show available tools
- `/cancel` — Cancel the current request

## Deployment (systemd)

`run.sh` auto-creates the venv and installs dependencies if missing — fully hands-off on boot. The systemd service has:
- `Restart=always` + `RestartSec=5` — auto-restarts on crash
- `WantedBy=multi-user.target` — starts on boot
- `After=network-online.target` — waits for network

One-time setup:

```bash
sudo cp telegram-bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now telegram-bot.service
```

After that, it starts automatically on every boot and restarts on failure with no interaction needed.

Useful commands:

```bash
sudo systemctl status telegram-bot.service
journalctl -u telegram-bot.service -f
```

## Prometheus Monitoring

The bot exposes metrics on port `9101` (configurable via `METRICS_PORT`).

### Exposed Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `telegrambot_messages_total` | Counter | `status` | Messages received (success/error/unauthorized/busy/cancelled) |
| `telegrambot_claude_requests_total` | Counter | `model`, `status` | Claude CLI invocations (success/error/timeout/cancelled) |
| `telegrambot_claude_response_duration_seconds` | Histogram | `model` | Claude response latency |
| `telegrambot_claude_cost_usd_total` | Counter | `model` | Cumulative API cost in USD |
| `telegrambot_claude_turns_total` | Counter | `model` | Cumulative agentic turns |
| `telegrambot_active_sessions` | Gauge | — | Active chat sessions |
| `process_*` | various | — | Python process metrics (auto-exported) |

### Add to Prometheus

Add a scrape job to your `prometheus.yml`:

```yaml
  - job_name: 'telegram_bot'
    static_configs:
      - targets: ['YOUR_SERVER_IP:9101']
        labels:
          alias: 'Telegram-Claude-Bot'
```

Then reload: `docker exec prometheus kill -HUP 1`

## Versioning & Commit Convention

**IMPORTANT: Update the version on EVERY commit** — this is mandatory. Do not skip version bumps for any reason.

Every commit message **must** start with the version prefix:

```
v0.7.0: Short description of the change
```

Rules:
1. **Bump the version** in `src/config.py` (`VERSION`) with every commit
2. **Update the version** in this file's header to match
3. Use **semver**: bump patch for fixes, minor for features, major for breaking changes
4. The commit message format is: `v<version>: <description>`

## Streaming Format (stream-json)

The CLI flags `--output-format stream-json --verbose --include-partial-messages` produce newline-delimited JSON on stdout. Each line has a `type` field:

| `type` | Description | Relevant fields |
|--------|-------------|-----------------|
| `system` | Session init | Skipped |
| `stream_event` | Real-time API streaming event (wraps Anthropic SSE) | `.event.type`, `.event.content_block`, `.event.delta` |
| `assistant` | Complete assistant message after a turn | `.message.content[]` (text/tool_use blocks) |
| `user` | Tool results fed back to Claude | Skipped |
| `result` | Final output with metadata | `.result`, `.session_id`, `.is_error`, `.total_cost_usd`, `.num_turns`, `.duration_ms` |

### stream_event inner types used for progress

- `content_block_start` → `.content_block.type == "tool_use"` → tool name in `.content_block.name`
- `content_block_delta` → `.delta.type == "input_json_delta"` → partial JSON in `.delta.partial_json`
- `content_block_stop` → signals end of a content block

### Subprocess env

The `CLAUDECODE` env var is stripped from the child process to bypass the nested-session guard when developing inside Claude Code.

## Provider Fallback System

When Claude hits rate limits or quota errors, the bot automatically falls back to alternative LLM providers via LiteLLM proxies.

### Configuration (`providers.json`)

```json
{
  "providers": [
    {"name": "claude", "description": "Anthropic Claude (default)", "env": {}},
    {"name": "glm4.7", "description": "GLM-4.7 via Cloud.ru", "env": {
      "ANTHROPIC_BASE_URL": "http://0.0.0.0:4001",
      "ANTHROPIC_AUTH_TOKEN": "any-placeholder-value"
    }}
  ],
  "rate_limit_patterns": ["rate limit", "overloaded", "429", "quota exceeded"],
  "cooldown_minutes": 30
}
```

### How it works

1. Each request uses the current provider's env vars for the `claude -p` subprocess
2. If the response is an error matching `rate_limit_patterns`, the bot automatically advances to the next provider and retries
3. The user is notified: "Rate limited on **claude**. Switching to **glm4.7**..."
4. After `cooldown_minutes`, the bot auto-recovers to the primary provider
5. Users can manually switch with `/provider [name]`

### Adding a new provider

1. Start a LiteLLM proxy: `litellm --model openai/your-model --api_base https://api.example.com/v1 --alias claude-3-5-sonnet-latest --drop_params --port 4002`
2. Add an entry to `providers.json` with the proxy's `ANTHROPIC_BASE_URL`
3. The bot picks it up on next restart (or `/provider reload` — future feature)

## Memory System

Persistent, global memory that makes the assistant smarter over time. Layered architecture:

| Layer | Storage | Description |
|-------|---------|-------------|
| **Core** | `memory/user_profile.yaml` | User profile: name, timezone, communication style, languages |
| **Semantic** | `memory/user_profile.yaml` | Facts with confidence scores (0.0–1.0), source (explicit/inferred), date |
| **Episodic** | `memory/episodes.db` | Conversation summaries in SQLite with FTS5 full-text search |
| **Working** | In-context (`--resume`) | Current session state, handled by Claude Code natively |

### How it works

1. **Before each message**: `MemoryManager.build_context()` reads YAML profile + searches SQLite FTS5 by keywords from the user's message, builds an XML `<memory>` block prepended to the prompt
2. **Memory instructions**: Absolute path to `user_profile.yaml` is appended so Claude can edit it directly with its file tools
3. **REMEMBER/FORGET**: Claude updates the YAML file naturally — no special command parsing needed
4. **REFLECT**: On `/new`, a background haiku call summarizes the conversation and stores it as an episode in SQLite
5. **RECALL**: FTS5 keyword search against the user's message surfaces relevant past episodes

### Context injection format

```xml
<memory>
<core>Name: Alice / Timezone: UTC+3 / Style: concise technical</core>
<relevant_facts>
- main_project: telegram-claude-bot
- preferred_model: opus
</relevant_facts>
<recent_episodes>
- 2026-02-23: Implemented stream-json format in v0.8.0
</recent_episodes>
</memory>

[user message]
<memory_instructions>
Your profile + facts file: /absolute/path/to/memory/user_profile.yaml
</memory_instructions>
```

### Configuration

- `MEMORY_DIR` env var (default: `memory/` relative to working directory)
- Facts with confidence < 0.6 are stored but not injected into context
- Episode search returns top 5 FTS5 matches, falls back to most recent if no keyword match

## Tool System

Custom tools that extend Claude Code's capabilities (web search, GitHub, APIs) with lazy loading to keep prompts lean.

### Architecture

Two-phase loading:
- **Phase 1 (Manifest)**: All tools' names, descriptions, and trigger keywords are always in context (~20 tokens per tool)
- **Phase 2 (Full)**: When a trigger keyword matches the user's message, the full tool instructions are loaded and injected

Tools are defined as YAML files in `tools/` directory with this structure:
```yaml
name: web_search
description: Search the web for current information
triggers: [search, google, find online, latest news, current events]
instructions: |
  You have a web search tool. Run: websearch "query"
  Returns JSON with title, url, snippet fields.
setup: tools/bin/websearch  # Optional: path to executable
```

### How it works

1. Before each message, `ToolRegistry.match_tools()` scans the user's message for trigger keywords
2. Matched tools' full instructions are loaded (cached for reuse)
3. Context injection builds an XML `<tools>` block with two sections:
   - `<available>`: All tools' manifest summaries (always included)
   - `<active>`: Full instructions for matched tools only
4. Claude sees `<tools>` context and knows when/how to use the external scripts
5. The `setup` field references a script that Claude can run via its built-in bash tool

### Context injection format

```xml
<tools>
<available>
- web_search: Search the web for current information
- github_pr: GitHub pull request operations
</available>
<active>
<tool name="web_search">
You have a web search tool. Run: websearch "query"
Returns JSON with title, url, snippet fields.
</tool>
</active>
</tools>

[user message]
```

### Configuration

- `TOOLS_DIR` env var (default: `tools/` relative to working directory)
- Tools directory is optional — no error if missing
- Maximum 3 active tools injected per message to avoid bloat
- Trigger matching uses substring detection (not word boundaries) for multi-word phrases like "latest news"

### Bot Commands

- `/tools` — List all available tools with trigger keywords