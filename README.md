# Telegram Persona Assistant

Chat with your assistant directly from Telegram. The bot supports multiple providers and CLIs, including Claude, Codex, and Codex2.

The runtime can switch providers per chat, preserve session state, and fall back between configured backends from `providers.json`.

## What You Need

Before starting, make sure you have:

1. **A Linux server** (or any machine that stays online) — a $5/month VPS works fine
2. **Python 3.10+** — pre-installed on most Linux systems
3. **Node.js 18+** — needed to install Claude Code CLI
4. **At least one provider CLI configured**:
   - Claude: [Claude Code](https://docs.anthropic.com/en/docs/claude-code)
   - Codex: `codex`
   - Codex2: `codex2`

## Setup (5 minutes)

### 1. Install provider CLIs

Install the CLIs you plan to use.

Claude:

```bash
npm install -g @anthropic-ai/claude-code
```

Codex:

```bash
npm install -g @openai/codex
```

Codex2:

```bash
# install/configure the codex2 CLI so `codex2` is available in PATH
```

Then authenticate the providers you installed:

```bash
claude   # follow the prompts to log in
codex    # optional
codex2   # optional
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

## Generic Mobile App Automation MVP

If you need server-side Android app automation (for Ozon or other apps), use the reusable package in [`mobile-automation/`](/home/claude-developer/iron-lady-assistant/mobile-automation/README.md) and the runbook in [`docs/mobile-automation-mvp.md`](/home/claude-developer/iron-lady-assistant/docs/mobile-automation-mvp.md).

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

Just send any text message and the bot will respond using the currently selected provider.

## Codex Instance Helper

If you want a separate Codex home directory and a real executable on `PATH`, use [`create_codex_instance.sh`](/home/claude-developer/iron-lady-assistant/create_codex_instance.sh):

```bash
./create_codex_instance.sh codex3
codex3
```

What it does:
- Creates `~/.<instance_name>`
- Symlinks your `~/.gitconfig`
- Symlinks your `~/.ssh`
- Installs a wrapper executable like `/usr/local/bin/codex3` that runs `codex` with `HOME=~/.codex3`
- Keeps Codex/OpenAI login isolated by default because each instance has its own `HOME`
- Shares `gh` auth only if you explicitly pass `--share-gh-config`

Then authenticate that separate instance once:

```bash
HOME="$HOME/.codex3" codex login
```

If you also want that instance to reuse your normal `gh` login:

```bash
./create_codex_instance.sh codex3 /usr/local/bin/codex3 --share-gh-config
```

If you do not pass `--share-gh-config`, GitHub CLI stays separate too:

```bash
HOME="$HOME/.codex3" gh auth login
```

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
| `ALLOWED_CHAT_IDS` | No | — | Comma-separated allowed chat/channel IDs |
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
- For channels/groups, add their numeric chat IDs to `ALLOWED_CHAT_IDS` (e.g. `-100...`)

**"Claude Code CLI is not installed"**
- Run `npm install -g @anthropic-ai/claude-code`
- Make sure Node.js 18+ is installed

**"Provider CLI 'codex' or 'codex2' is not installed"**
- Install or configure the missing CLI so it is available on `PATH`
- Switch providers with `/provider` only after the CLI is installed

**"TELEGRAM_BOT_TOKEN is not set"**
- Run `bash setup.sh` or edit `.env` with your token from @BotFather

**Bot crashes or stops responding**
- The bot has built-in crash loop protection: if it crashes 3+ times in 5 minutes, it auto-rolls back to the last working version and notifies you via Telegram
- Check deploy log: `cat .deploy/deploy.log`
- Check system logs: `journalctl -u telegram-bot.service -f`
- Restart manually: `sudo systemctl restart telegram-bot.service`
- If running manually, check the terminal output for errors

**"Applied your follow-up to the active run"**
- Mid-flight follow-up messages are treated as cumulative steering updates.
- The bot keeps current progress, applies follow-ups in order, and continues without a hard restart.

## Project Structure

```
├── setup.sh              # Interactive setup wizard
├── create_codex_instance.sh # Create isolated Codex HOME + wrapper executable
├── run.sh                # Start the bot (crash protection + auto-installs deps)
├── .env.example          # Configuration template
├── requirements.txt      # Python dependencies
├── telegram-bot.service  # systemd service file
├── providers.json        # LLM provider fallback configuration
├── .deploy/              # Runtime state (gitignored)
│   ├── good_commit       # Last known-good git commit hash
│   ├── start_times       # Recent start timestamps for crash detection
│   └── deploy.log        # Persistent log of deploys, crashes, rollbacks
├── sandbox/              # Candidate self-modification workspace before promotion
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
    ├── scheduler.py      # Persistent recurring schedule runner
    ├── self_modify.py    # Stage/validate/promote/rollback helpers for sandboxed self-modification
    ├── progress.py       # Live progress updates
    ├── formatter.py      # Markdown-to-HTML conversion
    └── metrics.py        # Prometheus metrics
```

## License

MIT
