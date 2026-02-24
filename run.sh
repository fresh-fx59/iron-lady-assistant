#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# Start the Telegram Claude Code Bot
# Automatically creates venv and installs deps if needed.
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

# ── Create venv and install deps if missing ──────────────────
if [ ! -d "venv" ]; then
    info "First run — setting up Python environment..."
    python3 -m venv venv
    venv/bin/pip install --quiet -r requirements.txt
    info "Dependencies installed."
fi

# ── Start the bot ────────────────────────────────────────────
source venv/bin/activate
info "Starting bot..."
exec python3 -m src.main
