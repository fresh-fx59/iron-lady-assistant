# Claude Code as Telegram Assistant

Telegram bot that bridges messages to Claude Code's `--print` mode via subprocess, providing a conversational AI assistant through Telegram.

## Architecture

- **aiogram 3.x** async Telegram bot with long-polling
- **asyncio subprocess** runs `claude -p` per message with `--output-format json`
- **`--resume <session_id>`** for conversation continuity
- **Per-chat asyncio.Lock** prevents overlapping Claude invocations

## Project Structure

```
src/
├── main.py       # Entry point, dispatcher setup, polling
├── config.py     # Env vars: BOT_TOKEN, ALLOWED_USER_IDS, DEFAULT_MODEL
├── bot.py        # Telegram handlers: /start, /new, /model, /status, messages
├── bridge.py     # Runs `claude -p` subprocess, parses JSON response
├── sessions.py   # Maps chat_id → claude session_id, persists to sessions.json
└── formatter.py  # Markdown→HTML conversion, message splitting
```

## Setup

1. Create bot via @BotFather, get token
2. `cp .env.example .env` and fill in values
3. `pip install -r requirements.txt`
4. `python -m src.main`

## Bot Commands

- `/start` — Welcome message
- `/new` — Start fresh conversation
- `/model [sonnet|opus|haiku]` — Switch model
- `/status` — Show current session info
