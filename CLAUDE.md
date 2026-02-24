# Claude Code as Telegram Assistant

**Current version: `0.6.0`** — defined in `src/config.py` as `VERSION`.

Telegram bot that bridges messages to Claude Code's `--print` mode via subprocess, providing a conversational AI assistant through Telegram.

## Architecture

- **aiogram 3.x** async Telegram bot with long-polling
- **asyncio subprocess** runs `claude -p` per message with `--output-format json` and `--dangerously-skip-permissions`
- **`--resume <session_id>`** for conversation continuity
- **Per-chat asyncio.Lock** prevents overlapping Claude invocations

## Project Structure

```
src/
├── main.py       # Entry point, dispatcher setup, polling, metrics server start
├── config.py     # Env vars: BOT_TOKEN, ALLOWED_USER_IDS, DEFAULT_MODEL, METRICS_PORT
├── bot.py        # Telegram handlers: /start, /new, /model, /status, messages
├── bridge.py     # Runs `claude -p` subprocess, parses JSON response
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
- `/model [sonnet|opus|haiku]` — Switch model
- `/status` — Show current session info

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
| `telegrambot_messages_total` | Counter | `status` | Messages received (success/error/unauthorized/busy) |
| `telegrambot_claude_requests_total` | Counter | `model`, `status` | Claude CLI invocations (success/error/timeout) |
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

Every commit message **must** start with the version prefix:

```
v0.5.0: Short description of the change
```

Rules:
1. **Bump the version** in `src/config.py` (`VERSION`) with every commit
2. **Update the version** in this file's header to match
3. Use **semver**: bump patch for fixes, minor for features, major for breaking changes
4. The commit message format is: `v<version>: <description>`
