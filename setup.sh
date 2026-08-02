#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────
# Interactive setup for Telegram Claude Code Bot
# Run this once:  bash setup.sh
# It walks you through everything — no technical knowledge needed.
# ─────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Non-interactive mode ─────────────────────────────────────────────
# Every prompt must be satisfiable from the environment, so the installer is
# testable in a container and reproducible on a rebuild.
NON_INTERACTIVE=0
for arg in "$@"; do
  case "$arg" in
    --non-interactive) NON_INTERACTIVE=1 ;;
  esac
done

# ask VAR "prompt" [default] — reads ILA_SETUP_<VAR> when non-interactive.
ask() {
  local __var="$1" __prompt="$2" __default="${3:-}"
  local __env_name="ILA_SETUP_${__var}"
  local __env_value="${!__env_name:-}"
  if [[ "$NON_INTERACTIVE" == "1" ]]; then
    printf -v "$__var" '%s' "${__env_value:-$__default}"
    return 0
  fi
  if [[ -n "$__env_value" ]]; then
    printf -v "$__var" '%s' "$__env_value"
    return 0
  fi
  local __reply
  ask __reply "$__prompt"
  printf -v "$__var" '%s' "${__reply:-$__default}"
}

require() {
  local __var="$1"
  if [[ -z "${!__var:-}" ]]; then
    error "ILA_SETUP_${__var} is required in --non-interactive mode."
    exit 1
  fi
}

# ── Colors ───────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

info()    { echo -e "${BLUE}[INFO]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC}   $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
error()   { echo -e "${RED}[ERR]${NC}  $*"; }
header()  { echo -e "\n${BOLD}── $* ──${NC}\n"; }

# ── Pre-flight checks ───────────────────────────────────────────────
header "Checking prerequisites"

MISSING=()

if ! command -v python3 &>/dev/null; then
    MISSING+=("python3")
fi

# pip is only needed if this run actually builds the venv.
if [[ "${ILA_SETUP_SKIP_VENV:-0}" != "1" ]]; then
    if ! command -v pip3 &>/dev/null && ! python3 -m pip --version &>/dev/null 2>&1; then
        MISSING+=("pip (python3-pip)")
    fi
fi

if ! command -v node &>/dev/null || ! command -v npm &>/dev/null; then
    if [[ "$NON_INTERACTIVE" == "1" ]]; then
        warn "node/npm not found — continuing (non-interactive)."
    else
        MISSING+=("node + npm (Node.js 18+)")
    fi
fi

if [ ${#MISSING[@]} -gt 0 ]; then
    error "Missing required software:"
    for m in "${MISSING[@]}"; do
        echo -e "  ${RED}x${NC} $m"
    done
    echo ""
    echo "Install the missing tools first:"
    echo "  - Python 3:   https://www.python.org/downloads/"
    echo "  - Node.js 18+: https://nodejs.org/ (or your OS package manager)"
    echo ""
    exit 1
fi

success "Python 3 found: $(python3 --version)"
command -v node &>/dev/null && success "Node found:     $(node --version)" || true

# ── Codex CLI (primary provider) ─────────────────────────────────────
header "Codex CLI"

# A global npm install is a side effect an unattended run must not perform
# silently — it mutates state outside this directory. Opt in explicitly.
if [[ "$NON_INTERACTIVE" == "1" && "${ILA_SETUP_INSTALL_CODEX:-0}" != "1" ]]; then
    info "Skipping Codex CLI install (set ILA_SETUP_INSTALL_CODEX=1 to enable)."
elif command -v codex &>/dev/null; then
    success "Codex CLI found: $(codex --version 2>/dev/null || echo 'installed')"
else
    info "Codex CLI not found. Installing @openai/codex globally..."
    # Prefer user-scoped npm prefix to avoid sudo; falls back to system prefix.
    if npm config get prefix >/dev/null 2>&1; then
        NPM_PREFIX="$(npm config get prefix)"
    else
        NPM_PREFIX=""
    fi
    if [ -n "$NPM_PREFIX" ] && [ -w "$NPM_PREFIX/bin" ]; then
        npm install -g @openai/codex
    elif [ -w "/usr/local/lib/node_modules" ] 2>/dev/null; then
        npm install -g @openai/codex
    else
        # Configure a user-scoped npm prefix so the install does not need sudo.
        USER_NPM_PREFIX="$HOME/.npm-$(whoami)"
        mkdir -p "$USER_NPM_PREFIX"
        npm config set prefix "$USER_NPM_PREFIX"
        case ":$PATH:" in
            *":$USER_NPM_PREFIX/bin:"*) ;;
            *)
                warn "Adding $USER_NPM_PREFIX/bin to your PATH in ~/.bashrc"
                echo "export PATH=\"$USER_NPM_PREFIX/bin:\$PATH\"" >> "$HOME/.bashrc"
                export PATH="$USER_NPM_PREFIX/bin:$PATH"
                ;;
        esac
        npm install -g @openai/codex
    fi
    if command -v codex &>/dev/null; then
        success "Codex CLI installed: $(codex --version 2>/dev/null || echo 'installed')"
    else
        warn "Codex CLI install appears to have failed. Install manually with: npm install -g @openai/codex"
        if command -v claude &>/dev/null; then
            warn "Falling back to Claude CLI as the primary provider."
        else
            error "Neither codex nor claude CLI is available. Install at least one:"
            echo "  npm install -g @openai/codex"
            echo "  npm install -g @anthropic-ai/claude-code"
            exit 1
        fi
    fi
fi

if command -v claude &>/dev/null; then
    success "Claude CLI found (optional fallback): $(claude --version 2>/dev/null || echo 'installed')"
else
    info "Claude CLI not installed (optional). Run 'npm install -g @anthropic-ai/claude-code' later if you want a Claude fallback."
fi

# ── Optional: CLIProxyAPI ────────────────────────────────────────────
header "CLIProxyAPI (optional)"

echo "CLIProxyAPI is a local OAuth proxy from router-for-me/CLIProxyAPI."
echo "It lets the bot use your ChatGPT Codex subscription over an"
echo "OpenAI-compatible endpoint on 127.0.0.1:8317, and makes a second"
echo "codex-proxy provider available for fallback alongside the native CLI."
echo ""
ask INSTALL_CLIPROXYAPI "Install CLIProxyAPI? [y/N]: "
INSTALL_CLIPROXYAPI="${INSTALL_CLIPROXYAPI:-N}"

if [[ "$INSTALL_CLIPROXYAPI" =~ ^[Yy]$ ]]; then
    CLIPROXY_DIR="$SCRIPT_DIR/third_party/cli-proxy-api"
    CLIPROXY_CONFIG_DIR="$HOME/.cli-proxy-api"
    CLIPROXY_CONFIG="$CLIPROXY_CONFIG_DIR/config.yaml"

    OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
    ARCH_RAW="$(uname -m)"
    case "$ARCH_RAW" in
        x86_64|amd64) ARCH="amd64" ;;
        aarch64|arm64) ARCH="arm64" ;;
        *)
            error "Unsupported architecture: $ARCH_RAW. Install CLIProxyAPI manually from https://github.com/router-for-me/CLIProxyAPI/releases"
            ARCH=""
            ;;
    esac

    if [ -n "$ARCH" ]; then
        mkdir -p "$CLIPROXY_DIR" "$CLIPROXY_CONFIG_DIR"

        info "Fetching latest CLIProxyAPI release metadata..."
        RELEASE_JSON="$(curl -fsSL https://api.github.com/repos/router-for-me/CLIProxyAPI/releases/latest)"
        TAG="$(echo "$RELEASE_JSON" | python3 -c 'import json,sys; print(json.load(sys.stdin)["tag_name"])')"
        VERSION_NUMBER="${TAG#v}"
        ASSET_NAME="CLIProxyAPI_${VERSION_NUMBER}_${OS}_${ARCH}.tar.gz"
        ASSET_URL="https://github.com/router-for-me/CLIProxyAPI/releases/download/${TAG}/${ASSET_NAME}"

        info "Downloading $ASSET_NAME..."
        TMP_TGZ="$(mktemp --suffix=.tar.gz)"
        trap 'rm -f "$TMP_TGZ"' EXIT
        curl -fsSL -o "$TMP_TGZ" "$ASSET_URL"
        tar -xzf "$TMP_TGZ" -C "$CLIPROXY_DIR"
        rm -f "$TMP_TGZ"
        trap - EXIT

        chmod +x "$CLIPROXY_DIR/cli-proxy-api"
        success "CLIProxyAPI $TAG installed at $CLIPROXY_DIR/cli-proxy-api"

        if [ ! -f "$CLIPROXY_CONFIG" ]; then
            # Minimal, Codex-focused config. Bind to localhost, empty api-keys (OAuth-only).
            cat > "$CLIPROXY_CONFIG" << YAMLEOF
host: "127.0.0.1"
port: 8317
auth-dir: "$CLIPROXY_CONFIG_DIR"
api-keys: []
debug: false
remote-management:
  allow-remote: false
  secret-key: ""
  disable-control-panel: true
YAMLEOF
            success "Wrote starter config at $CLIPROXY_CONFIG"
        else
            info "Existing config at $CLIPROXY_CONFIG left untouched."
        fi

        # Register codex-proxy provider in providers.json if not already present.
        python3 - <<PYEOF
import json, pathlib
p = pathlib.Path("$SCRIPT_DIR/providers.json")
data = json.loads(p.read_text())
names = {entry.get("name") for entry in data.get("providers", [])}
if "codex-proxy" not in names:
    entry = {
        "name": "codex-proxy",
        "description": "OpenAI Codex via local CLIProxyAPI (127.0.0.1:8317)",
        "cli": "codex",
        "models": [
            "gpt-5.4", "gpt-5.3-codex", "gpt-5.2-codex",
            "gpt-5.1-codex-max", "gpt-5.1-codex",
            "gpt-5.2", "gpt-5.1", "gpt-5-codex", "gpt-5",
            "gpt-5.1-codex-mini", "gpt-5-codex-mini",
        ],
        "env": {"OPENAI_BASE_URL": "http://127.0.0.1:8317/v1"},
    }
    data["providers"].insert(0, entry)
    p.write_text(json.dumps(data, indent=2) + "\n")
    print("  [OK] Registered codex-proxy provider in providers.json")
else:
    print("  [INFO] codex-proxy already present in providers.json")
PYEOF

        info "Next: authenticate the proxy against your ChatGPT Codex account."
        info "  Headless server: $CLIPROXY_DIR/cli-proxy-api --config $CLIPROXY_CONFIG --codex-device-login"
        info "  With browser:    $CLIPROXY_DIR/cli-proxy-api --config $CLIPROXY_CONFIG --codex-login"
    fi
else
    info "Skipping CLIProxyAPI. You can run setup.sh again later to install it."
fi

# ── Step 1: Telegram Bot Token ───────────────────────────────────────
header "Step 1 — Telegram Bot Token"

echo "You need a Telegram bot token. Here's how to get one:"
echo ""
echo "  1. Open Telegram and search for @BotFather"
echo "  2. Send /newbot"
echo "  3. Choose a name (e.g. 'My Claude Bot')"
echo "  4. Choose a username (must end in 'bot', e.g. 'my_claude_bot')"
echo "  5. BotFather will give you a token like: 123456:ABC-DEF..."
echo ""

BOT_TOKEN=""
while [ -z "$BOT_TOKEN" ]; do
    ask BOT_TOKEN "Paste your bot token here: "
    if [ -z "$BOT_TOKEN" ]; then
        if [[ "$NON_INTERACTIVE" == "1" ]]; then require BOT_TOKEN; fi
        warn "Token cannot be empty. Please try again."
    fi
done
success "Bot token saved."

# ── Step 2: Your Telegram User ID ────────────────────────────────────
header "Step 2 — Your Telegram User ID"

echo "The bot will only respond to authorized users."
echo ""
echo "To find your Telegram user ID:"
echo "  1. Open Telegram and search for @userinfobot"
echo "  2. Send it any message"
echo "  3. It will reply with your user ID (a number like 123456789)"
echo ""

USER_IDS=""
while [ -z "$USER_IDS" ]; do
    ask USER_IDS "Enter your user ID (or multiple IDs separated by commas): "
    if [ -z "$USER_IDS" ]; then
        if [[ "$NON_INTERACTIVE" == "1" ]]; then require USER_IDS; fi
        warn "At least one user ID is required, otherwise the bot won't respond to anyone."
    fi
done
success "Authorized users: $USER_IDS"

# ── Step 3: Default Model ────────────────────────────────────────────
header "Step 3 — Default AI Model"

echo "Choose which Claude model to use by default:"
echo ""
echo "  sonnet  - Fast and smart (recommended for most users)"
echo "  opus    - Most capable, slower and more expensive"
echo "  haiku   - Fastest and cheapest, less capable"
echo ""

DEFAULT_MODEL=""
while true; do
    ask DEFAULT_MODEL "Choose model [sonnet/opus/haiku] (press Enter for sonnet): "
    DEFAULT_MODEL="${DEFAULT_MODEL:-sonnet}"
    if [[ "$DEFAULT_MODEL" =~ ^(sonnet|opus|haiku)$ ]]; then
        break
    fi
    warn "Please type 'sonnet', 'opus', or 'haiku'."
done
success "Default model: $DEFAULT_MODEL"

# ── Step 4: Optional Settings ────────────────────────────────────────
header "Step 4 — Optional Settings (press Enter to skip any)"

ask WORKING_DIR "Working directory for Claude (press Enter for none): "
WORKING_DIR="${WORKING_DIR:-}"

ask TIMEOUT "Response timeout in seconds (press Enter for 300): "
TIMEOUT="${TIMEOUT:-300}"

ask METRICS_PORT "Metrics port (press Enter for 9101): "
METRICS_PORT="${METRICS_PORT:-9101}"

ask BROWSER_TAKEOVER_PUBLIC_BASE_URL "Browser takeover public base URL (optional, e.g. https://your-host.example/browser-takeover): "
BROWSER_TAKEOVER_PUBLIC_BASE_URL="${BROWSER_TAKEOVER_PUBLIC_BASE_URL:-}"

ask EXTERNAL_SCHEDULER "Run recurring schedules in a separate scheduler service? [y/N]: "
EXTERNAL_SCHEDULER="${EXTERNAL_SCHEDULER:-N}"

SCHEDULER_NOTIFY_CHAT_ID=""
SCHEDULER_NOTIFY_THREAD_ID=""
if [[ "$EXTERNAL_SCHEDULER" =~ ^[Yy]$ ]]; then
    info "The polling bot will keep /schedule_* commands, and a standalone daemon will execute due runs."
    ask SCHEDULER_NOTIFY_CHAT_ID "Optional scheduler notification chat ID (press Enter to skip): "
    SCHEDULER_NOTIFY_CHAT_ID="${SCHEDULER_NOTIFY_CHAT_ID:-}"
    ask SCHEDULER_NOTIFY_THREAD_ID "Optional scheduler notification topic/thread ID (press Enter to skip): "
    SCHEDULER_NOTIFY_THREAD_ID="${SCHEDULER_NOTIFY_THREAD_ID:-}"
fi

# ── Service topology ─────────────────────────────────────────────────
# These land in .env so src/config.py and the installed units agree on one name.
ask BOT_SERVICE       "systemd unit name for the bot (Enter for iron-lady-bot.service): " "iron-lady-bot.service"
ask SCHEDULER_SERVICE "systemd unit name for the scheduler (Enter for telegram-scheduler.service): " "telegram-scheduler.service"

# ── Write .env file ──────────────────────────────────────────────────
header "Writing configuration"

if [ -f .env ]; then
    cp .env ".env.backup.$(date +%s)"
    info "Backed up existing .env file."
fi

cat > .env << EOF
# Telegram Bot Configuration
# Generated by setup.sh on $(date)

TELEGRAM_BOT_TOKEN=$BOT_TOKEN
ALLOWED_USER_IDS=$USER_IDS
DEFAULT_MODEL=$DEFAULT_MODEL
CLAUDE_WORKING_DIR=$WORKING_DIR
MAX_RESPONSE_TIMEOUT=$TIMEOUT
METRICS_PORT=$METRICS_PORT
BROWSER_TAKEOVER_PUBLIC_BASE_URL=$BROWSER_TAKEOVER_PUBLIC_BASE_URL
EMBEDDED_SCHEDULER_ENABLED=$([[ "$EXTERNAL_SCHEDULER" =~ ^[Yy]$ ]] && echo 0 || echo 1)
SCHEDULER_NOTIFY_CHAT_ID=$SCHEDULER_NOTIFY_CHAT_ID
SCHEDULER_NOTIFY_THREAD_ID=$SCHEDULER_NOTIFY_THREAD_ID
ILA_BOT_SERVICE=$BOT_SERVICE
ILA_SCHEDULER_SERVICE=$SCHEDULER_SERVICE
EOF

success ".env file created."


# ── System packages ──────────────────────────────────────────────────
# The bot shells out to these; they were previously assumed present, which is
# why it only ever ran where nix had already provided them.
SYSTEM_PACKAGES_APT="ffmpeg tesseract-ocr espeak-ng git ripgrep nodejs"
SYSTEM_PACKAGES_DNF="ffmpeg tesseract espeak-ng git ripgrep nodejs"
SYSTEM_PACKAGES_PACMAN="ffmpeg tesseract espeak-ng git ripgrep nodejs"

detect_distro() {
  if [[ -n "${ILA_SETUP_FORCE_DISTRO:-}" ]]; then echo "${ILA_SETUP_FORCE_DISTRO}"; return; fi
  if [[ -r /etc/os-release ]]; then . /etc/os-release; echo "${ID_LIKE:-$ID}"; return; fi
  echo "unknown"
}

install_system_packages() {
  if [[ "${ILA_SETUP_SKIP_DEPS:-0}" == "1" ]]; then
    info "Skipping system packages (ILA_SETUP_SKIP_DEPS=1)."
    return 0
  fi
  local distro; distro="$(detect_distro)"
  case "$distro" in
    *debian*|*ubuntu*) sudo apt-get update && sudo apt-get install -y $SYSTEM_PACKAGES_APT ;;
    *rhel*|*fedora*)   sudo dnf install -y $SYSTEM_PACKAGES_DNF ;;
    *arch*)            sudo pacman -Sy --noconfirm $SYSTEM_PACKAGES_PACMAN ;;
    *)
      # Not fatal: print the list and continue. Do not stop a user who can read.
      warn "Unrecognised distribution '$distro'. Install these yourself, then re-run:"
      echo "  $SYSTEM_PACKAGES_APT"
      ;;
  esac
}

header "System packages"
install_system_packages

# ── Set up Python virtual environment ────────────────────────────────
header "Installing dependencies"

if [[ "${ILA_SETUP_SKIP_VENV:-0}" == "1" ]]; then
    info "Skipping venv creation (ILA_SETUP_SKIP_VENV=1)."
else
    if [ ! -d "venv" ]; then
        info "Creating Python virtual environment..."
        python3 -m venv venv
    fi
    info "Installing Python packages..."
    venv/bin/pip install --quiet -r requirements.txt
    success "Dependencies installed."
fi

# ── Optionally install as system service ─────────────────────────────
header "Auto-start on boot (optional)"

echo "Would you like the bot to start automatically when your server boots?"
echo "This uses systemd and requires sudo. If you enabled the external scheduler,"
echo "setup.sh will install both the bot service and the scheduler daemon service."
echo ""
ask INSTALL_SERVICE "Set up auto-start? [y/N]: "

if [[ "${ILA_SETUP_SKIP_SYSTEMD:-0}" == "1" ]] || ! command -v systemctl >/dev/null 2>&1; then
    info "systemd not available — skipping unit installation."
    info "Run the bot with:  $SCRIPT_DIR/venv/bin/python -m src.main"
    INSTALL_SERVICE="n"
fi

if [[ "$INSTALL_SERVICE" =~ ^[Yy]$ ]]; then
    SERVICE_USER="$(id -un)"
    # Units come from contrib/systemd templates, shared with the NixOS example,
    # so the two platforms cannot drift into different topologies.
    render_unit() {
        python3 contrib/systemd/render.py "contrib/systemd/$1" \
            --user "$SERVICE_USER" --dir "$SCRIPT_DIR" \
            --python "$SCRIPT_DIR/venv/bin/python" --envfile "$SCRIPT_DIR/.env"
    }

    render_unit iron-lady-bot.service.in | sudo tee "/etc/systemd/system/$BOT_SERVICE" > /dev/null
    success "Installed $BOT_SERVICE"

    if [[ "$EXTERNAL_SCHEDULER" =~ ^[Yy]$ ]]; then
        render_unit telegram-scheduler.service.in | sudo tee "/etc/systemd/system/$SCHEDULER_SERVICE" > /dev/null
        success "Installed $SCHEDULER_SERVICE"
    fi

    if [[ "$INSTALL_CLIPROXYAPI" =~ ^[Yy]$ ]] && [ -x "$SCRIPT_DIR/third_party/cli-proxy-api/cli-proxy-api" ]; then
        CLIPROXY_SERVICE_FILE="$SCRIPT_DIR/cli-proxy-api.service"
        cat > "$CLIPROXY_SERVICE_FILE" << CLIPROXYEOF
[Unit]
Description=CLIProxyAPI (Codex OAuth proxy for Iron Lady Assistant)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SERVICE_USER
WorkingDirectory=$SCRIPT_DIR/third_party/cli-proxy-api
ExecStart=$SCRIPT_DIR/third_party/cli-proxy-api/cli-proxy-api --config $HOME/.cli-proxy-api/config.yaml
Restart=always
RestartSec=5
Environment=HOME=$HOME

NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=$HOME
PrivateTmp=true

[Install]
WantedBy=multi-user.target
CLIPROXYEOF

        sudo cp "$CLIPROXY_SERVICE_FILE" /etc/systemd/system/cli-proxy-api.service
    fi

    sudo systemctl daemon-reload
    sudo systemctl enable --now telegram-bot.service
    if [[ "$EXTERNAL_SCHEDULER" =~ ^[Yy]$ ]]; then
        sudo systemctl enable --now telegram-scheduler.service
    fi
    if [[ "$INSTALL_CLIPROXYAPI" =~ ^[Yy]$ ]] && [ -f /etc/systemd/system/cli-proxy-api.service ]; then
        sudo systemctl enable --now cli-proxy-api.service
    fi

    success "Service installed and started!"
    echo ""
    echo "  Check status:  sudo systemctl status telegram-bot.service"
    if [[ "$EXTERNAL_SCHEDULER" =~ ^[Yy]$ ]]; then
        echo "                 sudo systemctl status telegram-scheduler.service"
    fi
    echo "  View logs:     journalctl -u telegram-bot.service -f"
    if [[ "$EXTERNAL_SCHEDULER" =~ ^[Yy]$ ]]; then
        echo "                 journalctl -u telegram-scheduler.service -f"
    fi
    echo "  Stop:          sudo systemctl stop telegram-bot.service"
    if [[ "$EXTERNAL_SCHEDULER" =~ ^[Yy]$ ]]; then
        echo "                 sudo systemctl stop telegram-scheduler.service"
    fi
    echo "  Restart:       sudo systemctl restart telegram-bot.service"
    if [[ "$EXTERNAL_SCHEDULER" =~ ^[Yy]$ ]]; then
        echo "                 sudo systemctl restart telegram-scheduler.service"
    fi
    if [[ "$INSTALL_CLIPROXYAPI" =~ ^[Yy]$ ]] && [ -f /etc/systemd/system/cli-proxy-api.service ]; then
        echo "                 sudo systemctl restart cli-proxy-api.service"
    fi
else
    info "Skipped. You can run the bot manually with: ./run.sh"
    if [[ "$EXTERNAL_SCHEDULER" =~ ^[Yy]$ ]]; then
        info "Run the standalone scheduler manually with: venv/bin/python3 -m src.scheduler_daemon"
    fi
    if [[ "$INSTALL_CLIPROXYAPI" =~ ^[Yy]$ ]] && [ -x "$SCRIPT_DIR/third_party/cli-proxy-api/cli-proxy-api" ]; then
        info "Run CLIProxyAPI manually with: $SCRIPT_DIR/third_party/cli-proxy-api/cli-proxy-api --config \$HOME/.cli-proxy-api/config.yaml"
    fi
fi

# ── Done! ─────────────────────────────────────────────────────────────
header "Setup complete!"

echo -e "Your bot is ready. Here's what to do next:"
echo ""
echo -e "  ${BOLD}Start the bot:${NC}      ./run.sh"
echo -e "  ${BOLD}Open Telegram:${NC}      Search for your bot by its username"
echo -e "  ${BOLD}Send a message:${NC}     Say hello and start chatting!"
echo ""
if [ -n "$BROWSER_TAKEOVER_PUBLIC_BASE_URL" ]; then
    echo -e "  ${BOLD}Browser takeover:${NC}  python3 -m src.browser_takeover setup"
    echo -e "                     python3 -m src.browser_takeover serve --host 0.0.0.0 --port 18792"
    echo -e "                     (uses BROWSER_TAKEOVER_PUBLIC_BASE_URL from .env)"
    echo ""
fi
echo -e "  ${BOLD}Bot commands:${NC}"
echo "    /start   — Welcome message"
echo "    /new     — Start a fresh conversation"
echo "    /model   — Switch between Claude models"
echo "    /status  — Show current session info"
echo ""
success "Happy chatting!"
