# Telegram Coding Agent Bot

Telegram bot that runs coding agents in chat: `Claude Code CLI` and `Codex CLI`.
You send messages in Telegram, the bot runs the selected agent, and returns replies/media back to chat.

## Core Capabilities

- Executes real coding tasks from Telegram: code edits, refactors, debugging, and test runs
- Supports multiple agent providers (`Claude Code CLI` and `Codex CLI`) with in-chat provider/model switching
- Preserves per-chat context and memory (profile + recent episodes) for long-running workflows
- Handles media end-to-end: voice transcription, images/files passthrough, and formatted replies
- Offers operational safety: allowlists, cancellation, rollback command, crash-loop protection, and metrics
- Enables cost-aware operation via Codex CLI subscription-based agent usage instead of API-only metering

## What This Repo Provides

- Telegram bot integration with Claude Code
- Telegram bot integration with Codex CLI
- Per-chat sessions and context persistence
- Provider/model switching via commands
- Memory subsystem (profile + episodes)
- Optional recurring scheduler
- Identity policy layer (`memory/identity.yaml`)
- Background self-learning journal + proactive failure alerts
- Crash-loop protection with rollback to last known-good commit
- Prometheus metrics endpoint

## Why Codex CLI Here

- You can run an agent workflow via `Codex CLI` directly, not only via API-style calls.
- Key advantage: for many usage patterns this is cheaper, because work goes through your agent subscription plan instead of per-request API billing.
- In practice, this lowers cost for frequent iterative coding sessions (many small edits/tests in one flow).

## Full Install From Scratch (Ubuntu 22.04/24.04)

This section is the complete zero-to-working setup.

### 1. Prepare server

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y git curl ca-certificates ffmpeg python3 python3-venv python3-pip
```

Install Node.js 18+ (recommended: NodeSource 20.x):

```bash
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs
node -v
npm -v
```

### 2. Install agent CLIs (Claude + Codex)

```bash
npm install -g @anthropic-ai/claude-code
npm install -g @openai/codex
claude
codex
```

Finish login flow in terminal/browser and verify:

```bash
claude --version
codex --version
```

### 3. Create Telegram bot and get token

In Telegram open `@BotFather`:

1. Send `/newbot`
2. Set bot name and username
3. Copy token (`123456:ABC...`)

Optional (recommended):

1. `/setprivacy` -> Disable (if you want bot to read all group messages)
2. `/setcommands` -> add commands from this README

### 4. Find your Telegram IDs

- Personal user ID: message `@userinfobot`
- Group/channel chat ID: add bot and inspect updates or use known `-100...` ID

### 5. Clone repository

```bash
git clone https://github.com/YOUR_USERNAME/claude-code-as-assistant.git
cd claude-code-as-assistant
```

### 6. Configure environment

```bash
cp .env.example .env
nano .env
```

Minimum required variables:

```env
TELEGRAM_BOT_TOKEN=123456:ABCDEF...
ALLOWED_USER_IDS=123456789
DEFAULT_PROVIDER=claude
DEFAULT_MODEL=sonnet
```

Common optional variables:

```env
ALLOWED_CHAT_IDS=-1001234567890
CLAUDE_WORKING_DIR=/home/claude-developer
IDLE_TIMEOUT=120
TELEGRAM_REQUEST_TIMEOUT_SECONDS=90
TELEGRAM_POLLING_TIMEOUT_SECONDS=30
TELEGRAM_BACKOFF_MIN_SECONDS=1.0
TELEGRAM_BACKOFF_MAX_SECONDS=30.0
TELEGRAM_BACKOFF_FACTOR=1.5
TELEGRAM_BACKOFF_JITTER=0.1
PROGRESS_DEBOUNCE_SECONDS=3.0
METRICS_PORT=9101
MEMORY_DIR=memory
TOOLS_DIR=tools
```

### 7. Install Python dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 8. Run setup wizard (recommended)

```bash
bash setup.sh
```

Wizard can:

- validate environment
- guide Telegram setup
- configure defaults
- optionally install systemd service

### 9. First run (manual)

```bash
./run.sh
```

Open Telegram -> send `/start` -> send a test text message.

### 10. Enable auto-start with systemd

```bash
sudo cp telegram-bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now telegram-bot.service
```

Check status/logs:

```bash
sudo systemctl status telegram-bot.service
journalctl -u telegram-bot.service -f
cat .deploy/deploy.log
```

## Quick Start (if server is already prepared)

```bash
git clone https://github.com/YOUR_USERNAME/claude-code-as-assistant.git
cd claude-code-as-assistant
bash setup.sh
./run.sh
```

## Bot Commands

- `/start` - welcome/help
- `/new` - reset current chat session
- `/model` - switch model (inline keyboard)
- `/provider` - switch provider (inline keyboard)
- `/status` - show session/model/provider status
- `/memory` - inspect remembered profile/episodes
- `/tools` - show available tools
- `/cancel` - cancel current in-flight request
- `/rollback` - rollback to previous good version (admin)

## Configuration Reference

All settings are read from `.env`.

- `TELEGRAM_BOT_TOKEN` (required): token from BotFather
- `ALLOWED_USER_IDS` (required): comma-separated allowed Telegram user IDs
- `ALLOWED_CHAT_IDS` (optional): comma-separated allowed group/channel IDs
- `DEFAULT_PROVIDER` (optional): default provider key from `providers.json`
- `DEFAULT_MODEL` (optional): default model alias
- `CLAUDE_WORKING_DIR` (optional): working directory for Claude CLI tasks
- `IDLE_TIMEOUT` (optional, default `120`): seconds before idle timeout
- `TELEGRAM_REQUEST_TIMEOUT_SECONDS` (optional, default `90`): HTTP timeout for each Telegram API request
- `TELEGRAM_POLLING_TIMEOUT_SECONDS` (optional, default `30`): long-poll timeout for `getUpdates`
- `TELEGRAM_BACKOFF_MIN_SECONDS` (optional, default `1.0`): minimum reconnect delay after polling/network errors
- `TELEGRAM_BACKOFF_MAX_SECONDS` (optional, default `30.0`): maximum reconnect delay
- `TELEGRAM_BACKOFF_FACTOR` (optional, default `1.5`): exponential reconnect multiplier
- `TELEGRAM_BACKOFF_JITTER` (optional, default `0.1`): reconnect delay randomization
- `PROGRESS_DEBOUNCE_SECONDS` (optional, default `3.0`): progress update pacing
- `METRICS_PORT` (optional, default `9101`): Prometheus endpoint port (`0` disables)
- `MEMORY_DIR` (optional, default `memory/`): persistent memory path
- `TOOLS_DIR` (optional, default `tools/`): custom tool definitions path
- `AUTONOMY_ENABLED` (optional, default `1`): enable background self-learning and proactive alerts
- `AUTONOMY_FAILURE_THRESHOLD` (optional, default `3`): failures required before proactive alert
- `AUTONOMY_FAILURE_WINDOW_MINUTES` (optional, default `60`): rolling window for failure detection
- `AUTONOMY_ALERT_COOLDOWN_MINUTES` (optional, default `30`): per-chat minimum gap between alerts

## Upgrade and Rollback

Update to latest:

```bash
cd /path/to/claude-code-as-assistant
git pull --ff-only
source venv/bin/activate
pip install -r requirements.txt
sudo systemctl restart telegram-bot.service
```

If broken, rollback:

```bash
git log --oneline -n 10
git checkout <last_good_commit>
sudo systemctl restart telegram-bot.service
```

Runtime crash protection also auto-rolls back if there are repeated startup failures.

## Monitoring

Metrics endpoint:

```text
http://localhost:9101/metrics
```

Example check:

```bash
curl -fsS http://localhost:9101/metrics | head
```

## Troubleshooting

### Bot does not respond

- Verify `.env` values are correct
- Ensure your user/chat IDs are in allowlists
- Confirm service is running: `sudo systemctl status telegram-bot.service`

### `Claude Code CLI is not installed`

```bash
npm install -g @anthropic-ai/claude-code
claude --version
```

### `Codex CLI is not installed`

```bash
npm install -g @openai/codex
codex --version
```

### `TELEGRAM_BOT_TOKEN is not set`

- Re-check `.env`
- Ensure service uses the correct repo directory and env file

### Bot keeps restarting

```bash
journalctl -u telegram-bot.service -n 200 --no-pager
cat .deploy/deploy.log
```

### Voice transcription issues

- Ensure `ffmpeg` is installed
- Verify whisper setup if enabled (`setup_whisper.sh`)

## Project Structure

```text
├── setup.sh
├── run.sh
├── .env.example
├── requirements.txt
├── telegram-bot.service
├── providers.json
├── .deploy/
│   ├── good_commit
│   ├── start_times
│   └── deploy.log
├── sandbox/
└── src/
    ├── core/
    ├── plugins/
    ├── main.py
    ├── config.py
    ├── bot.py
    ├── bridge.py
    ├── sessions.py
    ├── providers.py
    ├── memory.py
    ├── scheduler.py
    ├── self_modify.py
    ├── progress.py
    ├── formatter.py
    └── metrics.py
```

## Security Notes

- Never commit `.env`
- Restrict `ALLOWED_USER_IDS` and `ALLOWED_CHAT_IDS`
- Run bot under non-root user
- Rotate Telegram/LLM credentials if leaked

## License

MIT
