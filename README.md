# Telegram Claude Bot

Chat with Claude AI directly from Telegram. Send a message, get a response — it's that simple.

The bot runs [Claude Code](https://docs.anthropic.com/en/docs/claude-code) under the hood, so you get the full power of Claude as a conversational assistant right in your Telegram chat.

## What You Need

Before starting, make sure you have:

1. **A Linux server** (or any machine that stays online) — a $5/month VPS works fine
2. **Python 3.10+** — pre-installed on most Linux systems
3. **Node.js 18+** — needed to install Claude Code CLI
4. **An Anthropic API key** — sign up at [console.anthropic.com](https://console.anthropic.com/)

## Setup (5 minutes)

### 1. Install Claude Code CLI

```bash
npm install -g @anthropic-ai/claude-code
```

Then authenticate it with your Anthropic API key:

```bash
claude  # follow the prompts to log in
```

### 2. Clone this repo

```bash
git clone https://github.com/YOUR_USERNAME/claude-code-as-assistant.git
cd claude-code-as-assistant
```

### 3. Run the setup wizard

```bash
bash setup.sh
```

The wizard walks you through everything step by step:
- Creating a Telegram bot (via @BotFather)
- Finding your Telegram user ID
- Choosing a Claude model
- Optionally setting up auto-start on boot

That's it. Your bot is running.

### Alternative: Manual Setup

If you prefer to configure things yourself:

```bash
cp .env.example .env    # copy the template
nano .env               # edit with your values
./run.sh                # start the bot
```

## Using the Bot

Open Telegram, find your bot by its username, and start chatting.

| Command | What it does |
|---------|-------------|
| `/start` | Show welcome message |
| `/new` | Clear conversation history and start fresh |
| `/model` | Switch model (sonnet/opus/haiku) via inline keyboard |
| `/provider` | Switch LLM provider via inline keyboard |
| `/status` | Show current session and model info |
| `/memory` | Show what the bot remembers about you |
| `/tools` | Show available tools |
| `/cancel` | Cancel the current request |

Just send any text message and the bot will respond using Claude.

## Running in the Background

### Option A: Auto-start on boot (recommended for servers)

If you chose "yes" during setup, this is already done. Otherwise:

```bash
bash setup.sh   # choose "yes" for auto-start
```

Or manually:

```bash
sudo cp telegram-bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now telegram-bot.service
```

Useful commands:

```bash
sudo systemctl status telegram-bot.service    # check if running
sudo systemctl restart telegram-bot.service   # restart the bot
journalctl -u telegram-bot.service -f         # view live logs
cat .deploy/deploy.log                        # persistent deploy/crash log
```

### Option B: Run in a terminal

```bash
./run.sh
```

Use `screen` or `tmux` to keep it running after you disconnect.

## Configuration

All settings are in the `.env` file. Edit it anytime and restart the bot.

| Setting | Required | Default | Description |
|---------|----------|---------|-------------|
| `TELEGRAM_BOT_TOKEN` | Yes | — | Your bot token from @BotFather |
| `ALLOWED_USER_IDS` | Yes | — | Comma-separated Telegram user IDs |
| `DEFAULT_MODEL` | No | `sonnet` | Default Claude model |
| `CLAUDE_WORKING_DIR` | No | — | Working directory for Claude |
| `IDLE_TIMEOUT` | No | `120` | Seconds without output before timeout |
| `PROGRESS_DEBOUNCE_SECONDS` | No | `3.0` | Min seconds between progress updates |
| `METRICS_PORT` | No | `9101` | Prometheus metrics port (0 to disable) |
| `MEMORY_DIR` | No | `memory/` | Directory for persistent memory storage |
| `TOOLS_DIR` | No | `tools/` | Directory for custom tool definitions |

## Monitoring (Optional)

The bot exposes Prometheus metrics at `http://localhost:9101/metrics` — useful if you run Grafana or similar.

Tracked metrics include: message counts, response times, API costs, and active sessions.

## Troubleshooting

**Bot doesn't respond to messages**
- Check that your Telegram user ID is in `ALLOWED_USER_IDS` in `.env`
- Find your ID by messaging @userinfobot on Telegram

**"Claude Code CLI is not installed"**
- Run `npm install -g @anthropic-ai/claude-code`
- Make sure Node.js 18+ is installed

**"TELEGRAM_BOT_TOKEN is not set"**
- Run `bash setup.sh` or edit `.env` with your token from @BotFather

**Bot crashes or stops responding**
- The bot has built-in crash loop protection: if it crashes 3+ times in 5 minutes, it auto-rolls back to the last working version and notifies you via Telegram
- Check deploy log: `cat .deploy/deploy.log`
- Check system logs: `journalctl -u telegram-bot.service -f`
- Restart manually: `sudo systemctl restart telegram-bot.service`
- If running manually, check the terminal output for errors

**"Still processing your previous message"**
- The bot handles one message at a time per chat. Wait for the current response to finish.

## Project Structure

```
├── setup.sh              # Interactive setup wizard
├── run.sh                # Start the bot (crash protection + auto-installs deps)
├── .env.example          # Configuration template
├── requirements.txt      # Python dependencies
├── telegram-bot.service  # systemd service file
├── providers.json        # LLM provider fallback configuration
├── .deploy/              # Runtime state (gitignored)
│   ├── good_commit       # Last known-good git commit hash
│   ├── start_times       # Recent start timestamps for crash detection
│   └── deploy.log        # Persistent log of deploys, crashes, rollbacks
└── src/
    ├── core/             # Stable orchestration primitives
    │   └── context_plugins.py
    ├── plugins/          # Extensible context/tool modules
    │   └── tools_plugin.py
    ├── main.py           # Entry point, marks good commits on successful start
    ├── config.py         # Configuration loader
    ├── bot.py            # Telegram command handlers
    ├── bridge.py         # Claude Code subprocess bridge
    ├── sessions.py       # Conversation session management
    ├── providers.py      # Provider fallback chain
    ├── memory.py         # Persistent memory (YAML profile + SQLite episodes)
    ├── tools.py          # Backward-compatible shim to tools plugin
    ├── progress.py       # Live progress updates
    ├── formatter.py      # Markdown-to-HTML conversion
    └── metrics.py        # Prometheus metrics
```

## License

MIT
