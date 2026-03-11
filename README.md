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

## Ozon Browser Automation

For browser-based Ozon buying and order tracking, this repo now includes a dedicated wrapper around Vercel Labs' open-source `agent-browser` CLI:

```bash
cd /home/claude-developer/iron-lady-assistant
npm install
npx agent-browser install
python3 -m src.ozon_browser login --headed
python3 -m src.ozon_browser orders
python3 -m src.ozon_browser prepare-buy "детский шампунь Johnson's" --max-price 700 --checkout
python3 -m src.ozon_browser place-order --confirm
```

Notes:
- Browser state is kept under `~/.local/state/iron-lady-assistant/ozon-browser/`
- `login --headed` is the intended one-time step for manual auth or challenge solving
- If local Linux browser launch fails because of missing shared libraries, keep using the same wrapper but switch to a remote `agent-browser` provider such as `--provider browseruse`, `--provider kernel`, or `--provider browserbase`
- The final purchase step is intentionally separate and requires `--confirm`

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
| `/new` | Clear conversation history and start fresh (keeps provider/model for the same thread) |
| `/model` | Switch model (sonnet/opus/haiku) via inline keyboard |
| `/provider` | Switch LLM provider via inline keyboard |
| `/status` | Show current session and model info |
| `/memory` | Show what the bot remembers about you |
| `/tools` | Show available tools |
| `/cancel` | Cancel the current request |

Just send any text message and the bot will respond using the currently selected provider.

For voice messages, the bot now shows live transcription progress immediately after upload, replaces that transient progress with a persistent final transcription-time summary in chat before the LLM `Working...` phase starts, retries progress delivery if Telegram returns `retry after`, only switches the live progress message into audio-conversion mode for actual TTS-style audio generation commands, keeps that conversion timer pinned instead of reverting to generic `Working...`, falls back to a fresh progress message if Telegram rate-limits edits, and keeps a final conversion-time message in chat after the audio is sent.

Incoming Telegram `text`, `voice`, and `photo` updates are also logged with delivery metadata only (`chat/thread/message/user/content_type/length`, plus voice duration or photo count) so missed-message incidents can be diagnosed from `journalctl` without storing message contents in logs.

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

GitHub Actions deploys can restart services independently via repo variables:

- `RESTART_MAIN_APP_ON_PUSH=true` — restart `telegram-bot.service`
- `RESTART_SCHEDULER_ON_PUSH=true` — restart `telegram-scheduler.service`

If both are unset or false, pushes still deploy code to disk but do not restart either service.

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

Tracked metrics include message counts, response times, API costs, active sessions, and monitor-only F18 cost intelligence telemetry (taxonomy counters, tool-mix buckets, message-size buckets, and per-mode/provider/model cost and duration histograms).

### F18 Attention Validator (Monitor-Only)

Use the validator to detect when cost observability needs operator attention:

```bash
./scripts/validate_cost_observability.py --format text
```

It checks:
- Prometheus scrape health for `telegram_bot_metrics`
- Presence of key bot series (`telegrambot_messages_total`)
- Cost-with-error ratio, retry-amplified-cost frequency, and steering-event pressure

Suggested background schedule:

```cron
*/15 * * * * /home/claude-developer/iron-lady-assistant/scripts/validate_cost_observability.py --format json >> /home/claude-developer/iron-lady-assistant/work-dir/cost_observability_validator.log 2>&1
```

### External Scheduler Daemon

Recurring schedules can run outside the polling bot process:

- set `EMBEDDED_SCHEDULER_ENABLED=0` in `.env`
- install `telegram-scheduler.service`
- run `python3 -m src.scheduler_daemon`

For reboot persistence with systemd:

```bash
sudo install -m 0644 telegram-scheduler.service /etc/systemd/system/telegram-scheduler.service
sudo systemctl daemon-reload
sudo systemctl enable --now telegram-scheduler.service
```

Optional monitoring topic:

- set `SCHEDULER_NOTIFY_CHAT_ID`
- set `SCHEDULER_NOTIFY_THREAD_ID`
- optional: set `SCHEDULER_NOTIFY_LEVEL=all|failures|off` (default: `failures`)

The daemon will execute due schedules in the background and mirror only high-signal events by default: new failures, warn/critical task results, and recoveries from prior problems.
Routine submitted/started/success noise stays silent unless you explicitly set `SCHEDULER_NOTIFY_LEVEL=all`.
Scheduled jobs also preserve the provider runtime they were created with, so a task created from a `codex*` thread will continue running through that same Codex CLI when the standalone daemon picks it up.
`setup.sh` can also generate and install both `telegram-bot.service` and `telegram-scheduler.service` when you choose the external scheduler option.
The bundled systemd units include the per-user npm bin path so `codex` CLIs installed under `~/.npm-<user>/bin` stay available after reboot.

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
├── telegram-scheduler.service # Standalone scheduler daemon service
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
    ├── scheduler_daemon.py # Standalone scheduler runtime
    ├── self_modify.py    # Stage/validate/promote/rollback helpers for sandboxed self-modification
    ├── progress.py       # Live progress updates
    ├── formatter.py      # Markdown-to-HTML conversion
    └── metrics.py        # Prometheus metrics
```

## License

MIT
