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
DEPLOY_IDLE_TIMEOUT="${DEPLOY_IDLE_TIMEOUT:-300}"
RESTART_MAIN_APP="${RESTART_MAIN_APP:-0}"
RESTART_SCHEDULER="${RESTART_SCHEDULER:-0}"
RESTART_PROXY="${RESTART_PROXY:-0}"
RESTART_GMAIL_GATEWAY="${RESTART_GMAIL_GATEWAY:-0}"
RESTART_CODEX_PROXY="${RESTART_CODEX_PROXY:-0}"

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

is_truthy() {
    case "${1,,}" in
        1|true|yes|on) return 0 ;;
        *) return 1 ;;
    esac
}

restart_targets=()
if is_truthy "$RESTART_MAIN_APP"; then
    restart_targets+=("telegram-bot.service")
fi
if is_truthy "$RESTART_SCHEDULER"; then
    restart_targets+=("telegram-scheduler.service")
fi
if is_truthy "$RESTART_PROXY"; then
    restart_targets+=("telegram-proxy.service")
fi
if is_truthy "$RESTART_GMAIL_GATEWAY"; then
    restart_targets+=("gmail-gateway.service")
fi
if is_truthy "$RESTART_CODEX_PROXY"; then
    restart_targets+=("codex-proxy.service")
fi

install_systemd_unit_if_present() {
    local unit_name="$1"
    local src_path="$SCRIPT_DIR/$unit_name"
    local dst_path="/etc/systemd/system/$unit_name"
    if [ ! -f "$src_path" ]; then
        deploy_log "Unit file $unit_name not found in repo; skipping install"
        return 0
    fi
    if [ ! -f "$dst_path" ] || ! cmp -s "$src_path" "$dst_path"; then
        deploy_log "Installing/updating systemd unit: $unit_name"
        sudo install -m 0644 "$src_path" "$dst_path"
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

ALREADY_CURRENT=false
if [ "$ROLLBACK_COMMIT" = "$NEW_COMMIT" ]; then
    deploy_log "Already on latest commit $NEW_SHORT."
    ALREADY_CURRENT=true
else
    deploy_log "Deploying $ROLLBACK_SHORT -> $NEW_SHORT"
    # ── 3. Switch to new code ────────────────────────────────────
    git reset --hard origin/main
fi

# ── 4. Install deps + smoke test ─────────────────────────────
# Create venv if missing
if [ ! -d "venv" ]; then
    info "Creating Python venv..."
    python3 -m venv venv
fi

venv/bin/pip install --quiet -r requirements.txt

if ! venv/bin/python3 -c "from src.config import VERSION; print(f'Smoke test OK: v{VERSION}')" 2>>"$DEPLOY_LOG"; then
    deploy_log "SMOKE TEST FAILED for $NEW_SHORT."
    if [ "$ALREADY_CURRENT" = false ]; then
        deploy_log "Rolling back to $ROLLBACK_SHORT."
        git reset --hard "$ROLLBACK_COMMIT"
        notify_admin "❌ *Deploy failed* (smoke test)

Commit \`$NEW_SHORT\` failed smoke test.
Rolled back to \`$ROLLBACK_SHORT\`.
Service was not restarted — old code still running."
    else
        notify_admin "❌ *Deploy failed* (smoke test)

Commit \`$NEW_SHORT\` failed smoke test.
Service was not restarted."
    fi
    exit 1
fi

# ── 5. Optional restarts ─────────────────────────────────────
if [ ${#restart_targets[@]} -eq 0 ]; then
    deploy_log "Smoke test passed. No restart flags enabled; deploy completed without restarting services."
    echo ""
    echo "Current commit:"
    git log -1 --oneline
    exit 0
fi

if is_truthy "$RESTART_MAIN_APP"; then
    : > "$START_TIMES"
fi

deploy_operation_id=""
if is_truthy "$RESTART_MAIN_APP"; then
    deploy_log "Requesting lifecycle drain before restart"
    if ! deploy_operation_id=$(venv/bin/python3 -m src.lifecycle_tool begin-deploy --commit "$NEW_COMMIT" 2>>"$DEPLOY_LOG"); then
        deploy_log "Failed to request lifecycle drain"
        notify_admin "❌ *Deploy failed* (drain setup)

Commit \`$NEW_SHORT\` could not request deploy drain state.
        Check \`.deploy/deploy.log\` for details."
        exit 1
    fi
    if ! venv/bin/python3 -m src.lifecycle_tool wait-until-ready --operation-id "$deploy_operation_id" --timeout "$DEPLOY_IDLE_TIMEOUT" 2>>"$DEPLOY_LOG"; then
        deploy_log "Timed out waiting for deploy queue turn"
        venv/bin/python3 -m src.lifecycle_tool mark-failed \
            --operation-id "$deploy_operation_id" \
            --error "Timed out waiting for deploy queue turn" \
            2>>"$DEPLOY_LOG" || true
        notify_admin "❌ *Deploy aborted* (queue timeout)

Commit \`$NEW_SHORT\` did not reach the head of the deploy queue within ${DEPLOY_IDLE_TIMEOUT}s.
Old process kept running; restart was skipped."
        exit 1
    fi
    if ! venv/bin/python3 -m src.lifecycle_tool wait-for-idle --timeout "$DEPLOY_IDLE_TIMEOUT" 2>>"$DEPLOY_LOG"; then
        deploy_log "Timed out waiting for active work to drain before restart"
        venv/bin/python3 -m src.lifecycle_tool mark-failed \
            --operation-id "$deploy_operation_id" \
            --error "Timed out waiting for active work to drain before restart" \
            2>>"$DEPLOY_LOG" || true
        notify_admin "❌ *Deploy aborted* (drain timeout)

Commit \`$NEW_SHORT\` was ready, but active work did not drain within ${DEPLOY_IDLE_TIMEOUT}s.
Old process kept running; restart was skipped."
        exit 1
    fi
    venv/bin/python3 -m src.lifecycle_tool mark-restarting --operation-id "$deploy_operation_id" 2>>"$DEPLOY_LOG" || true
fi

deploy_log "Smoke test passed. Restarting: ${restart_targets[*]}"
if is_truthy "$RESTART_CODEX_PROXY"; then
    install_systemd_unit_if_present "codex-proxy.service"
fi
sudo systemctl daemon-reload
sudo systemctl restart "${restart_targets[@]}"

# ── 6. Health check ──────────────────────────────────────────
deploy_log "Waiting up to ${HEALTH_TIMEOUT}s for health check..."

for i in $(seq 1 "$HEALTH_TIMEOUT"); do
    sleep 1

    if is_truthy "$RESTART_MAIN_APP"; then
        if ! systemctl is-active --quiet telegram-bot.service; then
            deploy_log "telegram-bot.service crashed during startup (attempt $i/${HEALTH_TIMEOUT})"
            break
        fi
    fi

    if is_truthy "$RESTART_SCHEDULER"; then
        if ! systemctl is-active --quiet telegram-scheduler.service; then
            deploy_log "telegram-scheduler.service is not active yet (attempt $i/${HEALTH_TIMEOUT})"
            continue
        fi
    fi
    if is_truthy "$RESTART_PROXY"; then
        if ! systemctl is-active --quiet telegram-proxy.service; then
            deploy_log "telegram-proxy.service is not active yet (attempt $i/${HEALTH_TIMEOUT})"
            continue
        fi
    fi
    if is_truthy "$RESTART_GMAIL_GATEWAY"; then
        if ! systemctl is-active --quiet gmail-gateway.service; then
            deploy_log "gmail-gateway.service is not active yet (attempt $i/${HEALTH_TIMEOUT})"
            continue
        fi
    fi
    if is_truthy "$RESTART_CODEX_PROXY"; then
        if ! systemctl is-active --quiet codex-proxy.service; then
            deploy_log "codex-proxy.service is not active yet (attempt $i/${HEALTH_TIMEOUT})"
            continue
        fi
    fi

    if is_truthy "$RESTART_MAIN_APP"; then
        if [ -f "$GOOD_COMMIT_FILE" ]; then
            CURRENT_GOOD=$(cat "$GOOD_COMMIT_FILE")
            if [ "$CURRENT_GOOD" = "$NEW_COMMIT" ]; then
                deploy_log "Deploy successful! Selected services healthy at $NEW_SHORT (${i}s)"
                if [ -n "$deploy_operation_id" ]; then
                    venv/bin/python3 -m src.lifecycle_tool mark-completed --operation-id "$deploy_operation_id" 2>>"$DEPLOY_LOG" || true
                fi
                echo ""
                echo "Current commit:"
                git log -1 --oneline
                exit 0
            fi
        fi
        continue
    fi

    deploy_log "Deploy successful! Selected services healthy at $NEW_SHORT (${i}s)"
    if [ -n "$deploy_operation_id" ]; then
        venv/bin/python3 -m src.lifecycle_tool mark-completed --operation-id "$deploy_operation_id" 2>>"$DEPLOY_LOG" || true
    fi
    echo ""
    echo "Current commit:"
    git log -1 --oneline
    exit 0
done

# ── 7. Health check failed — rollback ────────────────────────
deploy_log "HEALTH CHECK FAILED for $NEW_SHORT after ${HEALTH_TIMEOUT}s."

if [ "$ALREADY_CURRENT" = false ]; then
    deploy_log "Rolling back to $ROLLBACK_SHORT."
    git reset --hard "$ROLLBACK_COMMIT"

    if is_truthy "$RESTART_MAIN_APP"; then
        : > "$START_TIMES"
    fi

    sudo systemctl daemon-reload
    sudo systemctl restart "${restart_targets[@]}"

    notify_admin "❌ *Deploy failed* (health check)

Commit \`$NEW_SHORT\` did not become healthy within ${HEALTH_TIMEOUT}s.
Rolled back to \`$ROLLBACK_SHORT\` and restarted.
Check \`.deploy/deploy.log\` for details."
else
    notify_admin "❌ *Deploy failed* (health check)

Commit \`$NEW_SHORT\` did not become healthy within ${HEALTH_TIMEOUT}s.
Check \`.deploy/deploy.log\` for details."
fi

if [ -n "$deploy_operation_id" ]; then
    venv/bin/python3 -m src.lifecycle_tool mark-failed \
        --operation-id "$deploy_operation_id" \
        --error "Deploy health check failed for $NEW_SHORT" \
        2>>"$DEPLOY_LOG" || true
fi

exit 1
