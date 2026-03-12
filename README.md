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

For voice messages, the bot now shows live transcription progress immediately after upload, replaces that transient progress with a persistent final transcription-time summary in chat before the LLM `Working...` phase starts, retries progress delivery if Telegram returns `retry after`, only switches the live progress message into audio-conversion mode for actual TTS-style audio generation commands, keeps that conversion timer pinned instead of reverting to generic `Working...`, falls back to a fresh progress message if Telegram rate-limits edits, keeps a final conversion-time message in chat after the audio is sent, and snapshots local audio attachments to a per-send temp file so parallel voice replies cannot overwrite each other mid-delivery.

For photo messages, the bot runs best-effort local OCR (Tesseract) only when an image is uploaded and appends extracted text to the prompt when available; text-only turns never run OCR.

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
| `VOICE_TRANSCRIPTION_MAX_CONCURRENCY` | No | `1` | Max number of concurrent whisper transcription jobs |
| `VOICE_TRANSCRIPTION_THREADS` | No | `cpu_count / max_concurrency` | Threads passed to each `whisper-cli` process |
| `METRICS_PORT` | No | `9101` | Prometheus metrics port (0 to disable) |
| `F08_GOVERNANCE_MODE` | No | `shadow` | F08 rollout mode: `shadow`, `enforce_limited`, `enforce_scoped`, `enforce_full` |
| `F08_ENFORCEMENT_SCOPE` | No | `self_mod_only` | Logical scope label for F08 governance metrics |
| `MEMORY_DIR` | No | `memory/` | Directory for persistent memory storage |
| `TOOLS_DIR` | No | `tools/` | Directory for custom tool definitions |

## Monitoring (Optional)

The bot exposes Prometheus metrics at `http://localhost:9101/metrics` — useful if you run Grafana or similar.

Tracked metrics include message counts, response times, API costs, active sessions, monitor-only F18 cost intelligence telemetry (taxonomy counters, tool-mix buckets, message-size buckets, and per-mode/provider/model cost and duration histograms), and monitor-only F08 governance telemetry (`telegrambot_f08_governance_events_total` + duration histogram).

### F18 Attention Validator (Monitor-Only)

Use the validator to detect when cost observability needs operator attention:

```bash
./scripts/validate_cost_observability.py --format text
```

It checks:
- Prometheus scrape health for `telegram_bot_metrics`
- Presence of key bot series (`telegrambot_messages_total`)
- Cost-with-error ratio, retry-amplified-cost frequency, and steering-event pressure

For recurring runs, prefer native scheduler execution with built-in change detection so routine checks consume zero LLM tokens and only escalate to the model when something new appears:

```text
[[SCHEDULE_NATIVE]]
command: /home/claude-developer/iron-lady-assistant/scripts/validate_cost_observability.py --format json --alert-on-change --state-file /home/claude-developer/iron-lady-assistant/work-dir/cost_observability_validator.state.json
Write a short operator-facing alert only when the validator reports a new issue, changed issue, or recovery.
```

The native command must return JSON with `status`, `should_alert`, `change_type`, and `summary`. When `should_alert=false`, the scheduler stores the run result and stays silent. When `should_alert=true`, the scheduler submits a background LLM task with the validator JSON as escalation context.

Operational policy for validator-backed schedules:

- treat `new_issue` and `worsened_issue` as incident triggers, not passive notifications
- collect deterministic diagnostics first: validator payload, service health, recent logs, and direct dependency checks
- attempt only explicitly safe automatic remediations
- verify after remediation and send one compact outcome report
- treat `recovery` as a correlation/report event unless it still leaves an actionable problem
- keep repeated unchanged incidents quiet inside a dedup/cooldown window

Optional native schedule hooks:

```text
[[SCHEDULE_NATIVE]]
command: /path/to/validator --format json --alert-on-change
diagnose_command: /path/to/diagnose_incident
remediate_command: /path/to/safe_fix
auto_remediate: true
Write a short operator-facing alert only when the validator reports a new issue, changed issue, or recovery.
```

- `diagnose_command` runs automatically before escalation on `warn`/`critical` issue states
- `remediate_command` runs only when `auto_remediate: true`
- after remediation, the scheduler reruns the main validator command and includes post-remediation verification in the final escalation context
- when no custom diagnostics are configured, the scheduler still runs built-in safe diagnostics for known metric-presence incidents

To update an existing native schedule without editing `schedules.db` directly, use the scheduler admin tool:

```bash
python3 -m src.schedule_admin_tool set-native-remediation \
  --schedule-id <schedule_id> \
  --auto-remediate true
```

You can also attach explicit hooks while updating:

```bash
python3 -m src.schedule_admin_tool set-native-remediation \
  --schedule-id <schedule_id> \
  --auto-remediate true \
  --diagnose-command "/path/to/diagnose_incident --json" \
  --remediate-command "/path/to/safe_fix"
```

### Telegram Channel Daily Digest

For Telegram channel monitoring, keep ingestion native and keep the final digest as a normal scheduled LLM task:

- use a read-only Telegram proxy backed by a Telethon user session to collect messages from subscribed channels and linked discussion chats
- refresh a local briefing file during the day
- deliver one daily digest back into the target Telegram topic

Recommended `.env` values for the proxy-backed path:

```bash
TELEGRAM_PROXY_BASE_URL=http://127.0.0.1:8787
TELEGRAM_PROXY_API_KEY=...
TELEGRAM_PROXY_ENCRYPTED_CREDENTIALS=...
TELEGRAM_PROXY_KEY_CREDENTIAL_NAME=telegram_proxy_key
```

Generate the encrypted Telegram credential blob with the repo-local helper:

```bash
venv/bin/python scripts/create_telethon_string_session.py
python3 -m src.telegram_proxy_crypto_tool generate-key
python3 -m src.telegram_proxy_crypto_tool encrypt
```

The session helper prompts for `api_id`, `api_hash`, and `phone`, then walks through the Telegram login and prints the `StringSession`.
The encrypt helper prompts interactively for the Fernet key, `api_id`, `api_hash`, and `StringSession` so these values do not need to appear in shell history.

Keep the generated Fernet key out of `.env`; load it into the proxy service via `LoadCredentialEncrypted=` in `telegram-proxy.service`.

Collector run:

```bash
python3 -m src.telegram_proxy
python3 -m src.telegram_digest_tool collect
python3 -m src.telegram_digest_tool render
```

One-command channel onboarding after the proxy is already configured:

```bash
venv/bin/python scripts/add_telegram_digest_channel.py --channel https://t.me/ai_engineer_helper
```

This helper:
- resolves the channel through the proxy;
- appends the channel id to `TELEGRAM_PROXY_ALLOWED_CHANNEL_IDS`;
- appends the linked discussion chat id to `TELEGRAM_PROXY_ALLOWED_CHAT_IDS` when present;
- restarts `telegram-proxy.service`;
- runs one immediate native collect pass and prints a JSON summary.

To collect from all subscribed channels plus their linked discussion chats, clear the allowlists in one command:

```bash
venv/bin/python scripts/add_telegram_digest_channel.py --all-visible
```

If the current shell cannot run `sudo`, you can still stage the allowlist change without restarting:

```bash
venv/bin/python scripts/add_telegram_digest_channel.py --channel https://t.me/ai_engineer_helper --no-restart --no-collect
sudo systemctl restart telegram-proxy.service
venv/bin/python -m src.telegram_digest_tool collect
```

Install both recurring schedules into `memory/schedules.db`:

```bash
python3 -m src.telegram_digest_tool install \
  --chat-id <telegram_chat_id> \
  --message-thread-id <topic_id> \
  --user-id <telegram_user_id> \
  --daily-time 08:00 \
  --timezone-name Europe/Moscow
```

The installer creates:

- an interval native collector schedule (`[[SCHEDULE_NATIVE]]`) that updates `memory/telegram_digest.db` and `memory/telegram_digest_brief.md`
- a daily delivery schedule (`[[SCHEDULE_DELIVER]]`) that reads the briefing, writes a Russian executive summary, and can attach a voice-note audio reply via the existing `MEDIA:` / `[[audio_as_voice]]` contract
- the daily delivery prompt uses `USE_TOOL: edge-tts-safe` (repo-local edge-tts wrapper), not `sag`

Systemd setup for the proxy:

```bash
sudo install -m 0644 telegram-proxy.service /etc/systemd/system/telegram-proxy.service
sudo systemctl daemon-reload
sudo systemctl enable --now telegram-proxy.service
```

### F08 Governance Validator (Monitor-Only)

Use this validator during rollout to ensure F08 remains in non-interfering mode while baseline data accumulates:

```bash
./scripts/validate_f08_observability.py --format text
```

It checks:
- Prometheus scrape health for `telegram_bot_metrics`
- Presence of F08 metric series (`telegrambot_f08_governance_events_total`)
- Whether non-shadow F08 events appear unexpectedly
- 24h `apply_candidate` failure ratio and rollback-success count

Phase 1 note:
- The bot now emits shadow-only advisory F08 events for chat turns (`chat_turn_advisory`) and self-mod apply reviews (`selfmod_apply_advisory`) without blocking response-path execution.

Scheduler-friendly native mode:

```text
[[SCHEDULE_NATIVE]]
command: /home/claude-developer/iron-lady-assistant/scripts/validate_f08_observability.py --format json --alert-on-change --state-file /home/claude-developer/iron-lady-assistant/work-dir/f08_observability_validator.state.json
Write a short operator-facing alert only when the validator reports a new issue, changed issue, or recovery.
```

### F10 Baseline Summary (Monitor-Only, 14d Gate)

Use this native summary to keep F10 fully non-interfering while collecting cost baseline, then emit a high-signal summary when the 14-day window completes.

One-off/manual run:

```bash
./scripts/f10_baseline_summary.py \
  --format text \
  --baseline-start 2026-03-12T00:00:00+00:00 \
  --window-days 14 \
  --state-file /home/claude-developer/iron-lady-assistant/work-dir/f10_baseline_summary.state.json
```

Scheduler-native mode:

```text
[[SCHEDULE_NATIVE]]
command: /home/claude-developer/iron-lady-assistant/scripts/f10_baseline_summary.py --format json --alert-on-change --baseline-start 2026-03-12T00:00:00+00:00 --window-days 14 --state-file /home/claude-developer/iron-lady-assistant/work-dir/f10_baseline_summary.state.json
Write a concise operator summary only when change_type=baseline_ready, new_issue, or recovery.
```

Expected behavior:
- Before `2026-03-26T00:00:00+00:00`, output stays in `phase=collecting` and remains silent when unchanged.
- At/after `2026-03-26T00:00:00+00:00`, the first transition to `phase=ready` emits a single change-detected summary for optimization planning.
- Runtime path remains unchanged (monitor-only, no model/provider/session/tool interventions).

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
Schedules can also use a native command mode by starting the prompt with `[[SCHEDULE_NATIVE]]` and providing a `command:` line. This is the recommended path for deterministic health checks and validators because it removes routine LLM cost from the steady-state path.
For validator schedules, the intended loop is: native check -> incident classification -> deterministic diagnosis/remediation -> compact report. The LLM is for escalation and summarization, not for the steady-state check itself.
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
