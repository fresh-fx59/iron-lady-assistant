#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# Protected deploy with smoke test, health check, and rollback.
# Called by GitHub Actions or manually. Safe to run at any time.
# ─────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

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
GOOD_COMMIT_FILE="$DEPLOY_DIR/good_commit"
START_TIMES="$DEPLOY_DIR/start_times"
HEALTH_TIMEOUT=30  # seconds to wait for healthy bot

mkdir -p "$DEPLOY_DIR"

deploy_log() {
    local msg="$(date '+%Y-%m-%d %H:%M:%S') [deploy] $*"
    echo "$msg" >> "$DEPLOY_LOG"
    info "$msg"
}

# Send Telegram notification to admin
notify_admin() {
    local message="$1"
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

# ── 1. Save rollback target ──────────────────────────────────
ROLLBACK_COMMIT=$(git rev-parse HEAD)
ROLLBACK_SHORT=$(git rev-parse --short HEAD)
deploy_log "Deploy started. Rollback target: $ROLLBACK_SHORT"

# ── 2. Fetch new code ────────────────────────────────────────
git fetch origin main
NEW_COMMIT=$(git rev-parse origin/main)
NEW_SHORT=$(echo "$NEW_COMMIT" | cut -c1-7)

if [ "$ROLLBACK_COMMIT" = "$NEW_COMMIT" ]; then
    deploy_log "Already on latest commit $NEW_SHORT. Nothing to deploy."
    exit 0
fi

deploy_log "Deploying $ROLLBACK_SHORT -> $NEW_SHORT"

# ── 3. Switch to new code ────────────────────────────────────
git reset --hard origin/main

# ── 4. Install deps + smoke test ─────────────────────────────
# Create venv if missing
if [ ! -d "venv" ]; then
    info "Creating Python venv..."
    python3 -m venv venv
fi

venv/bin/pip install --quiet -r requirements.txt

if ! venv/bin/python3 -c "from src.config import VERSION; print(f'Smoke test OK: v{VERSION}')" 2>>"$DEPLOY_LOG"; then
    deploy_log "SMOKE TEST FAILED for $NEW_SHORT. Rolling back to $ROLLBACK_SHORT."
    git reset --hard "$ROLLBACK_COMMIT"
    notify_admin "❌ *Deploy failed* (smoke test)

Commit \`$NEW_SHORT\` failed smoke test.
Rolled back to \`$ROLLBACK_SHORT\`.
Service was not restarted — old code still running."
    exit 1
fi

# ── 5. Clear crash counter so run.sh starts fresh ────────────
: > "$START_TIMES"

# ── 6. Restart service ───────────────────────────────────────
deploy_log "Smoke test passed. Restarting service..."
sudo systemctl restart telegram-bot.service

# ── 7. Health check ──────────────────────────────────────────
# Wait for good_commit to match the new commit (written by main.py on successful startup)
deploy_log "Waiting up to ${HEALTH_TIMEOUT}s for health check..."

for i in $(seq 1 "$HEALTH_TIMEOUT"); do
    sleep 1

    # Check if service crashed
    if ! systemctl is-active --quiet telegram-bot.service; then
        deploy_log "Service crashed during startup (attempt $i/${HEALTH_TIMEOUT})"
        break
    fi

    # Check if good_commit was updated to new commit
    if [ -f "$GOOD_COMMIT_FILE" ]; then
        CURRENT_GOOD=$(cat "$GOOD_COMMIT_FILE")
        if [ "$CURRENT_GOOD" = "$NEW_COMMIT" ]; then
            deploy_log "Deploy successful! Bot healthy at $NEW_SHORT (${i}s)"
            echo ""
            echo "Current commit:"
            git log -1 --oneline
            exit 0
        fi
    fi
done

# ── 8. Health check failed — rollback ────────────────────────
deploy_log "HEALTH CHECK FAILED for $NEW_SHORT after ${HEALTH_TIMEOUT}s. Rolling back to $ROLLBACK_SHORT."

git reset --hard "$ROLLBACK_COMMIT"

# Clear crash counter again for clean rollback start
: > "$START_TIMES"

sudo systemctl restart telegram-bot.service

notify_admin "❌ *Deploy failed* (health check)

Commit \`$NEW_SHORT\` did not become healthy within ${HEALTH_TIMEOUT}s.
Rolled back to \`$ROLLBACK_SHORT\` and restarted.
Check \`.deploy/deploy.log\` for details."

exit 1
