# Claude Code as Telegram Assistant

**Current version: `0.19.6`** — defined in `src/config.py` as `VERSION`.

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
├── core/
│   └── context_plugins.py  # Stable registry for context-producing plugins
├── plugins/
│   └── tools_plugin.py     # Lazy YAML tool plugin used by prompt context pipeline
├── main.py                 # Entry point, dispatcher setup, polling, metrics server
├── config.py               # Env vars: BOT_TOKEN, ALLOWED_USER_IDS, DEFAULT_MODEL, IDLE_TIMEOUT, MEMORY_DIR, TOOLS_DIR
├── bot.py                  # Telegram handlers: /start, /new, /model, /provider, /status, /memory, /tools, /rollback, /selfmod_apply, /schedule_*, /bg, /cancel
├── memory.py               # Persistent memory: YAML profile + SQLite FTS5 episodic, context injection
├── tools.py                # Backward-compatible shim to plugins/tools_plugin.py
├── tasks.py                # Background task manager with queue and completion notifications
├── scheduler.py            # Persistent recurring schedules, submits jobs to background task manager
├── self_modify.py          # Sandboxed self-modification workflow: stage -> validate -> promote -> rollback helper
├── transcribe.py           # Async voice transcription via whisper.cpp subprocess
├── bridge.py               # Runs `claude -p` subprocess, yields stream events (TOOL_USE, RESULT)
├── providers.py            # Provider fallback chain: auto-switches LLM on rate limit
├── progress.py             # ProgressReporter: manages live progress message with debounced edits
├── sessions.py             # Maps chat_id → claude session_id, persists to sessions.json
├── formatter.py            # Markdown→HTML conversion, message splitting
└── metrics.py              # Prometheus metrics: counters, histograms, gauges
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

## Voice Messages (whisper.cpp)

Voice messages are transcribed locally using [whisper.cpp](https://github.com/ggerganov/whisper.cpp) — no API key needed.

### One-time setup

```bash
bash setup_whisper.sh
```

This installs build deps (`cmake`, `g++`, `ffmpeg`), clones and builds whisper.cpp, and downloads the `small` model (~500 MB RAM, ~4s for 30s audio on CPU).

### How it works

1. User sends a voice message in Telegram
2. Bot downloads the `.oga` file, converts to WAV via `ffmpeg`
3. `whisper-cli` transcribes locally (auto-detects Russian/English)
4. Transcribed text is prefixed with `[Voice message]` and passed to Claude/Codex
5. If whisper.cpp is not installed, bot replies with setup instructions

### Configuration

- `WHISPER_BIN` env var — path to whisper-cli binary (default: `whisper.cpp/build/bin/whisper-cli` in repo root)
- `WHISPER_MODEL` env var — path to GGML model file (default: `whisper.cpp/models/ggml-small.bin`)

## Bot Commands

- `/start` — Welcome message
- `/new` — Start fresh conversation
- `/model` — Switch model (sonnet|opus|haiku) via inline keyboard
- `/provider` — Switch LLM provider via inline keyboard (auto-switches on rate limit)
- `/status` — Show current session info
- `/memory` — Show what the bot remembers (profile + episodes)
- `/tools` — Show available tools
- `/rollback` — Show rollback options and restore a previous commit (admin-only)
- `/selfmod_stage <path.py>` + code block — Stage sandbox plugin candidate (admin-only)
- `/selfmod_apply <path.py> [test_target]` — Validate+promote sandbox plugin candidate (admin-only)
- `/schedule_every <minutes> <task>` — Create recurring background task
- `/schedule_daily <HH:MM> <task>` — Create daily recurring background task
- `/schedule_weekly <day> <HH:MM> <task>` — Create weekly recurring background task
- `/schedule_list` — List recurring schedules
- `/schedule_cancel <schedule_id>` — Cancel recurring schedule
- `/bg <task>` — Run a task in background (non-blocking)
- `/bg-list` — List active background tasks
- `/bg-cancel <task_id>` — Cancel a background task
- `/cancel` — Cancel the current request

## Background Tasks

Long-running tasks can be executed in the background without blocking your chat conversation.

### Usage

Start a background task:
```
/bg write a python script to backup my database
```

The bot will queue the task and immediately reply with a task ID, allowing you to continue chatting.

List active tasks:
```
/bg-list
```

Cancel a task:
```
/bg-cancel abc123
```

### Features

- **Queue system**: Up to 3 concurrent background tasks
- **10-minute timeout**: Long tasks are automatically terminated
- **Completion notifications**: You're notified when a background task finishes
- **Status tracking**: Real-time status (queued, running, completed, failed, cancelled)
- **Auto-cleanup**: Completed tasks are removed from memory after 1 hour
- **Memory & tools preserved**: Background tasks have full access to memory and custom tools

### Task lifecycle

1. Submit with `/bg <prompt>` → Task queued
2. Bot processes queue (max 3 concurrent)
3. Claude executes the task (includes memory + tool context)
4. Results delivered via Telegram notification
5. Task cleaned up after 1 hour

## Deployment (systemd)

`run.sh` auto-creates the venv and installs dependencies if missing — fully hands-off on boot. The systemd service has:
- `Restart=always` + `RestartSec=5` — auto-restarts on crash
- `StartLimitBurst=5` + `StartLimitIntervalSec=600` — stops retrying after 5 crashes in 10 min
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
cat .deploy/deploy.log          # persistent deploy/crash log
cat .deploy/good_commit         # last known-good git commit
```

### Crash Loop Protection & Auto-Rollback

Three-layer safety system prevents the bot from going silent after a bad deploy:

**Layer 1 — `run.sh` crash loop detection:**
- Tracks start attempts in `.deploy/start_times` (timestamp + commit hash)
- If 3+ starts within 5 minutes → crash loop detected
- Auto-rolls back to `.deploy/good_commit` via `git reset --hard`
- Sends Telegram notification to the first admin in `ALLOWED_USER_IDS`
- Runs a **smoke test** (`from src.config import VERSION`) before every start
- All events logged to `.deploy/deploy.log` (auto-trimmed at 1MB)

**Layer 2 — `src/main.py` good-commit marker:**
- After `set_my_commands()` succeeds (proves code loaded + token valid + Telegram API reachable), writes current git hash to `.deploy/good_commit`
- This is the commit that rollback will restore to

**Layer 3 — systemd safety net:**
- `StartLimitBurst=5` / `StartLimitIntervalSec=600` — if even rollback fails, systemd stops retrying after 5 attempts in 10 minutes

**State files** (in `.deploy/`, gitignored):
- `good_commit` — full git hash of last known-good version
- `start_times` — recent start attempts for crash detection
- `deploy.log` — persistent log of starts, crashes, rollbacks

### Deploy Procedure

**IMPORTANT: Follow this procedure for every deploy to ensure rollback safety.**

#### Automatic Deployment (GitHub Actions) — with rollback protection

Push to `main` branch triggers `deploy.sh` via GitHub Actions SSH. The script:

1. Saves current commit as rollback target
2. Pulls new code (`git fetch origin main && git reset --hard origin/main`)
3. Installs deps and runs **smoke test** (`from src.config import VERSION`)
4. If smoke test fails → **rollback immediately**, service not restarted, admin notified
5. Clears crash counter, restarts `telegram-bot.service`
6. **Health check**: polls for up to 30s, waiting for `good_commit` to match the new commit
7. If health check fails → **rollback + restart**, admin notified via Telegram
8. If healthy → deploy success, exit 0

This gives full rollback protection for both import-time errors (caught by smoke test) and runtime startup errors (caught by health check).

Required secrets in GitHub repo:
- `SERVER_HOST` - Your server hostname or IP (e.g., `your-server.com` or `1.2.3.4`)
- `SERVER_USER` - SSH username on the server (e.g., `claude-developer`)
- `SSH_PRIVATE_KEY` - Private SSH key content (full key with `-----BEGIN ...-----` headers)
- `SSH_PORT` - SSH port (optional, defaults to 22; only needed if you use a non-standard port)

**Setup SSH key for passwordless deploy:**
```bash
# Generate a new key pair
ssh-keygen -t ed25519 -f ~/.ssh/deploy_key -N ""

# Copy PUBLIC key to your server
ssh-copy-id -i ~/.ssh/deploy_key.pub user@your-server-host

# Test passwordless SSH
ssh -i ~/.ssh/deploy_key user@your-server-host hostname
```

Then paste the **private key** (`~/.ssh/deploy_key`) into GitHub as `SSH_PRIVATE_KEY`.

#### Manual Deployment

You can also run `deploy.sh` directly on the server:
```bash
# Protected deploy (same as GitHub Actions)
./deploy.sh

# Or manual steps:
sudo cp telegram-bot.service /etc/systemd/system/  # only if service file changed
sudo systemctl daemon-reload
sudo systemctl restart telegram-bot.service
sudo systemctl status telegram-bot.service
cat .deploy/deploy.log | tail -5
cat .deploy/good_commit
```

### Startup Notification

When the bot starts, it sends a Telegram notification to the first admin:

```
🚀 Bot restarted

📦 Version: v0.18.47
📦 Commit: abc1234

✅ Ready to assist!
```

If something goes wrong, the bot will auto-rollback after 3 crash restarts. Check `.deploy/deploy.log` for details.

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
| `telegrambot_bg_tasks_active` | Gauge | — | Total active background tasks (queued + running) |
| `telegrambot_bg_tasks_queued` | Gauge | — | Queued background tasks |
| `telegrambot_bg_tasks_running` | Gauge | — | Currently running background tasks |
| `telegrambot_bg_tasks_total` | Counter | `status` | Total background tasks (completed/failed/cancelled/timeout) |
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

**IMPORTANT: Commit after every meaningful action** — do not batch unrelated changes into one commit. Each commit should represent one logical unit of work. Examples of when to commit:
- After implementing a feature or fixing a bug
- After updating documentation
- After refactoring code
- After adding/updating tests

If a task involves multiple steps (e.g. code change + docs update + config change), commit each step separately if they are independently meaningful.

Every commit message **must** start with the version prefix:

```
v0.7.0: Short description of the change
```

Rules:
1. **Bump the version** in `src/config.py` (`VERSION`) with every commit.
2. **Update the version** in this file's header to match.
3. Use **semver** with explicit mapping:
   - `0.0.+1` (patch): fixes, refactors, internal improvements, docs/tests that do not change public behavior.
   - `0.+1.0` (minor): new backward-compatible features/capabilities.
   - `+1.0.0` (major): breaking changes (API/command/behavior contracts, required config migrations).
4. The commit message format is: `v<version>: <description>`.

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
- `/rollback` — Show rollback options and restore a previous commit (admin-only)
- `/selfmod_stage <path.py>` + code block — Stage sandbox plugin candidate (admin-only)
- `/selfmod_apply <path.py> [test_target]` — Validate+promote sandbox plugin candidate (admin-only)
- `/schedule_every <minutes> <task>` — Create recurring background task
- `/schedule_daily <HH:MM> <task>` — Create daily recurring background task
- `/schedule_weekly <day> <HH:MM> <task>` — Create weekly recurring background task
- `/schedule_list` — List recurring schedules
- `/schedule_cancel <schedule_id>` — Cancel recurring schedule
