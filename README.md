# Telegram Coding Agent Bot

Telegram bot that runs coding agents in chat: `Claude Code CLI` and `Codex CLI`.
You send messages in Telegram, the bot runs the selected agent, and returns replies/media back to chat.

## Core Capabilities

- Executes real coding tasks from Telegram: code edits, refactors, debugging, and test runs
- Supports multiple agent providers (`Claude Code CLI` and `Codex CLI`) with in-chat provider/model switching
- Preserves per-chat context and memory (profile + recent episodes) for long-running workflows
- Restores interrupted in-flight work after restart by snapshotting scope state and auto-resuming tasks
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

## One-Line Install (Recommended)

Run one command on a clean Ubuntu server:

```bash
bash <(curl -fsSL https://raw.githubusercontent.com/fresh-fx59/iron-lady-assistant/main/install.sh)
```

What it does:

- installs system dependencies (`git`, `python3`, `ffmpeg`, `nodejs`, etc.)
- installs `Claude Code CLI` and `Codex CLI`
- clones/updates this repo into `~/iron-lady-assistant`
- creates venv and installs Python dependencies
- prompts for `TELEGRAM_BOT_TOKEN` and `ALLOWED_USER_IDS`
- writes `.env`, runs smoke test, installs and starts `telegram-bot.service`

Non-interactive example:

```bash
bash <(curl -fsSL https://raw.githubusercontent.com/fresh-fx59/iron-lady-assistant/main/install.sh) --non-interactive --bot-token "123456:ABCDEF" --allowed-user-ids "123456789"
```

## Bot Commands

- `/start` - welcome/help
- `/new` - reset current chat session
- `/model` - switch model (inline keyboard)
- `/provider` - switch provider (inline keyboard)
- `/status` - show session/model/provider status
- `/memory` - inspect remembered profile/episodes
- `/memory_forget <key>` - remove semantic memory fact(s) by key
- `/memory_consolidate` - de-duplicate memory facts and prune low-confidence noise
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
- `HEALTH_INVARIANTS_ENABLED` (optional, default `1`): inject runtime health-invariants block into prompt context
- `HEALTH_INVARIANTS_MAX_CHARS` (optional, default `1200`): max size of health-invariants block
- `HEALTH_INVARIANTS_STALE_HOURS` (optional, default `72`): memory staleness threshold used by invariants
- `HEALTH_INVARIANTS_PROVIDER_FAIL_WARN_RATIO` (optional, default `0.30`): anomaly threshold for provider failures
- `HEALTH_INVARIANTS_EMPTY_WARN_RATIO` (optional, default `0.20`): anomaly threshold for empty responses
- `HEALTH_INVARIANTS_MIN_SAMPLE_SIZE` (optional, default `5`): minimum sample size before ratio-based anomaly checks
- `CONTEXT_COMPILER_ENABLED` (optional, default `1`): build compact repo-aware context block before tool context
- `CONTEXT_COMPILER_MAX_CHARS` (optional, default `1600`): max size of compiled context block
- `SCOPE_SNAPSHOT_ENABLED` (optional, default `1`): persist per-scope pending queue snapshots for restart recovery
- `SCOPE_SNAPSHOT_MAX_AGE_MINUTES` (optional, default `180`): max snapshot age eligible for restore
- `SCOPE_SNAPSHOT_COMPLETED_HASHES_LIMIT` (optional, default `20`): recent follow-up hashes kept to prevent duplicate replay
- `METRICS_PORT` (optional, default `9101`): Prometheus endpoint port (`0` disables)
- `MEMORY_DIR` (optional, default `memory/`): persistent memory path
- `TOOLS_DIR` (optional, default `tools/`): custom tool definitions path
- `GEMINI_IMAGE_ONLY_MODE` (optional, default `1`): strips Gemini API credentials and blocks Gemini-oriented tools for non-image tasks
- `AUTONOMY_ENABLED` (optional, default `1`): enable background self-learning and proactive alerts
- `AUTONOMY_FAILURE_THRESHOLD` (optional, default `3`): failures required before proactive alert
- `AUTONOMY_FAILURE_WINDOW_MINUTES` (optional, default `60`): rolling window for failure detection
- `AUTONOMY_ALERT_COOLDOWN_MINUTES` (optional, default `30`): per-chat minimum gap between alerts
- `LOCAL_TTS_BIN` (optional, default `espeak` from PATH): local TTS CLI used for voice-bubble replies
- `LOCAL_TTS_VOICE` (optional, default `auto`): voice selection mode/preset (`auto` picks by text script)
- `LOCAL_TTS_VOICE_CYRILLIC` (optional, default `ru`): preferred voice preset for Cyrillic-heavy text
- `LOCAL_TTS_VOICE_LATIN` (optional, default `en`): preferred voice preset for Latin-heavy text
- `LOCAL_TTS_ENGINE` (optional, default `auto`): `auto`/`sherpa`/`espeak` local TTS engine selection
- `LOCAL_TTS_SPEED_WPM` (optional, default `220`): local TTS speech speed in words per minute
- `LOCAL_TTS_SPEED_WPM_CYRILLIC` (optional, default `170`): speech speed for Cyrillic-heavy text
- `LOCAL_TTS_SPEED_WPM_LATIN` (optional, default `220`): speech speed for Latin-heavy text
- `LOCAL_TTS_MAX_CHARS` (optional, default `1200`): max text length sent to TTS after cleanup
- `LOCAL_TTS_VERIFY_INTELLIGIBILITY` (optional, default `1`): run post-TTS intelligibility check (via local whisper.cpp when available)
- `LOCAL_TTS_MIN_INTELLIGIBILITY_SCORE` (optional, default `0.55`): minimum similarity score for accepted voice output
- `LOCAL_TTS_VERIFY_MAX_CHARS` (optional, default `260`): skip verification for long texts to keep latency bounded
- `LOCAL_TTS_OPUS_BITRATE` (optional, default `48k`): Opus bitrate for voice-note encoding quality
- `LOCAL_TTS_FFMPEG_AF` (optional): ffmpeg audio filter chain for intelligibility and loudness normalization
- `SHERPA_ONNX_RUNTIME_DIR` (optional): sherpa runtime dir for offline neural TTS
- `SHERPA_ONNX_MODEL_DIR` (optional): sherpa model dir (for example `vits-piper-ru_RU-ruslan-medium`)

## Upgrade and Rollback

Update to latest:

```bash
cd /path/to/iron-lady-assistant
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

### Voice bubble synthesis issues

- Ensure `espeak` and `ffmpeg` are installed on host
- Check `LOCAL_TTS_BIN`, `LOCAL_TTS_VOICE`, and `LOCAL_TTS_SPEED_WPM` in `.env`
- If `sudo` is unavailable, run rootless bootstrap: `bash setup_local_tts.sh`

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
    ├── health_invariants.py
    ├── formatter.py
    ├── ocr.py
    └── metrics.py
```

## Security Notes

- Never commit `.env`
- Restrict `ALLOWED_USER_IDS` and `ALLOWED_CHAT_IDS`
- Run bot under non-root user
- Rotate Telegram/LLM credentials if leaked

## License

MIT
