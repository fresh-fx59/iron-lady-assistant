#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# Start the Telegram Claude Code Bot
# Automatically creates venv and installs deps if needed.
# Includes crash loop detection with auto-rollback.
# ─────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
NPM_PREFIX_BIN="$(npm config get prefix 2>/dev/null)/bin"
export PATH="$NPM_PREFIX_BIN:/usr/local/bin:/usr/bin:/bin:$PATH"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }

# ── Deploy state ──────────────────────────────────────────────
DEPLOY_DIR="$SCRIPT_DIR/.deploy"
DEPLOY_LOG="$DEPLOY_DIR/deploy.log"
START_TIMES="$DEPLOY_DIR/start_times"
GOOD_COMMIT_FILE="$DEPLOY_DIR/good_commit"
MAX_CRASHES=3
CRASH_WINDOW=300  # seconds (5 minutes)

mkdir -p "$DEPLOY_DIR"

deploy_log() {
    local msg="$(date '+%Y-%m-%d %H:%M:%S') $*"
    echo "$msg" >> "$DEPLOY_LOG"
    warn "$msg"
}

# Trim deploy.log if over 1MB
if [ -f "$DEPLOY_LOG" ] && [ "$(stat -f%z "$DEPLOY_LOG" 2>/dev/null || stat -c%s "$DEPLOY_LOG" 2>/dev/null || echo 0)" -gt 1048576 ]; then
    tail -500 "$DEPLOY_LOG" > "$DEPLOY_LOG.tmp" && mv "$DEPLOY_LOG.tmp" "$DEPLOY_LOG"
fi

# Send Telegram notification to admin
notify_admin() {
    local message="$1"
    # Source .env to get bot token and user IDs
    local token="${TELEGRAM_BOT_TOKEN:-}"
    local user_ids="${ALLOWED_USER_IDS:-}"
    if [ -z "$token" ] && [ -f .env ]; then
        token=$(grep "^TELEGRAM_BOT_TOKEN=" .env | cut -d'=' -f2- | tr -d '"' | tr -d "'")
        user_ids=$(grep "^ALLOWED_USER_IDS=" .env | cut -d'=' -f2- | tr -d '"' | tr -d "'")
    fi
    if [ -n "$token" ] && [ -n "$user_ids" ]; then
        local admin_id
        admin_id=$(echo "$user_ids" | cut -d',' -f1 | tr -d ' ')
        curl -s "https://api.telegram.org/bot${token}/sendMessage" \
            -d "chat_id=$admin_id" \
            -d "text=$message" \
            -d "parse_mode=Markdown" \
            > /dev/null 2>&1 || true
    fi
}

# ── Crash loop detection ─────────────────────────────────────
CURRENT_COMMIT=$(git rev-parse HEAD 2>/dev/null || echo "unknown")
CURRENT_SHORT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# Record this start attempt
echo "$(date +%s) $CURRENT_COMMIT" >> "$START_TIMES"

# Trim start_times to last 20 entries
if [ -f "$START_TIMES" ]; then
    tail -20 "$START_TIMES" > "$START_TIMES.tmp" && mv "$START_TIMES.tmp" "$START_TIMES"
fi

# Count starts within crash window
CUTOFF=$(($(date +%s) - CRASH_WINDOW))
RECENT_STARTS=$(awk -v cutoff="$CUTOFF" '$1 > cutoff' "$START_TIMES" 2>/dev/null | wc -l | tr -d ' ')

if [ "$RECENT_STARTS" -ge "$MAX_CRASHES" ]; then
    deploy_log "CRASH LOOP: $RECENT_STARTS starts in last ${CRASH_WINDOW}s at commit $CURRENT_SHORT"

    if [ -f "$GOOD_COMMIT_FILE" ]; then
        GOOD_COMMIT=$(cat "$GOOD_COMMIT_FILE")
        if [ "$GOOD_COMMIT" != "$CURRENT_COMMIT" ]; then
            deploy_log "ROLLBACK: $CURRENT_SHORT -> $(echo "$GOOD_COMMIT" | cut -c1-8)"
            git reset --hard "$GOOD_COMMIT" 2>>"$DEPLOY_LOG"
            # Reset crash counter after rollback
            : > "$START_TIMES"
            deploy_log "Rollback complete. Restarting with known-good code."
            notify_admin "⚠️ *Bot crash loop detected*
Rolled back from \`$CURRENT_SHORT\` to \`$(echo "$GOOD_COMMIT" | cut -c1-8)\`.
Check \`.deploy/deploy.log\` for details."
        else
            deploy_log "Already on last-good commit $CURRENT_SHORT. Cannot rollback further."
            notify_admin "⚠️ *Bot crash loop detected*
Already on last-good commit \`$CURRENT_SHORT\`. Manual intervention needed."
        fi
    else
        deploy_log "No last-good commit recorded. Cannot rollback."
        notify_admin "⚠️ *Bot crash loop detected*
No last-good commit on record. Manual intervention needed."
    fi
fi

# ── Check prerequisites ─────────────────────────────────────
if ! command -v python3 &>/dev/null; then
    error "Python 3 is not installed."
    echo "  Install it: https://www.python.org/downloads/"
    echo "  Ubuntu/Debian: sudo apt install python3 python3-venv python3-pip"
    exit 1
fi

if ! command -v claude &>/dev/null; then
    error "Claude Code CLI is not installed."
    echo "  Install it: npm install -g @anthropic-ai/claude-code"
    exit 1
fi

if ! command -v codex &>/dev/null; then
    warn "Codex CLI not found in PATH. Install it first (e.g. npm install -g @openai/codex)."
fi

if ! command -v codex2 &>/dev/null; then
    warn "Codex2 CLI not found in PATH. Install/configure it if you plan to use the codex2 provider."
fi

# ── Check .env exists ────────────────────────────────────────
if [ ! -f .env ]; then
    error "No .env file found!"
    echo ""
    echo "  Quick setup (recommended):  bash setup.sh"
    echo "  Manual setup:               cp .env.example .env  (then edit .env)"
    exit 1
fi

# ── Check bot token is configured ────────────────────────────
if grep -q "^TELEGRAM_BOT_TOKEN=your-bot-token-here" .env 2>/dev/null; then
    error "Bot token not configured in .env file."
    echo "  Edit .env and replace 'your-bot-token-here' with your actual token."
    echo "  Get a token from @BotFather on Telegram."
    exit 1
fi

# ── Create venv if missing ───────────────────────────────────
if [ ! -d "venv" ]; then
    info "First run — setting up Python environment..."
    python3 -m venv venv
fi

# ── Sync dependencies (installs new/updated packages) ───────
venv/bin/pip install --quiet -r requirements.txt

# ── Smoke test ───────────────────────────────────────────────
if ! venv/bin/python3 -c "from src.config import VERSION; print(f'Smoke test OK: v{VERSION}')" 2>>"$DEPLOY_LOG"; then
    deploy_log "SMOKE TEST FAILED at commit $CURRENT_SHORT"
    exit 1
fi

# ── Start the bot ────────────────────────────────────────────
source venv/bin/activate
deploy_log "Starting bot at commit $CURRENT_SHORT"
exec python3 -m src.main
