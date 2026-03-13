#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m'

info() { printf "%b[INFO]%b %s\n" "${BLUE}" "${NC}" "$*"; }
ok() { printf "%b[OK]%b   %s\n" "${GREEN}" "${NC}" "$*"; }
warn() { printf "%b[WARN]%b %s\n" "${YELLOW}" "${NC}" "$*"; }
err() { printf "%b[ERR]%b  %s\n" "${RED}" "${NC}" "$*" >&2; }
header() { printf "\n%b== %s ==%b\n" "${BOLD}" "$*" "${NC}"; }

DRY_RUN=0
CHECK_ONLY=0
INSTALL_SYSTEM=1
INSTALL_NPM=1
INSTALL_BROWSER=1
WITH_XVFB=1

usage() {
  cat <<'EOF'
Usage:
  scripts/install_agent_browser_host.sh [options]

Options:
  --dry-run        Print planned actions without changing the machine.
  --check          Check current host readiness only.
  --skip-system    Skip apt-based system package installation.
  --skip-npm       Skip repo-local npm install.
  --skip-browser   Skip `npx agent-browser install`.
  --no-xvfb        Do not install virtual display/debugging packages.
  -h, --help       Show this help.

Examples:
  scripts/install_agent_browser_host.sh --dry-run
  scripts/install_agent_browser_host.sh
  scripts/install_agent_browser_host.sh --skip-system
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=1
      ;;
    --check)
      CHECK_ONLY=1
      ;;
    --skip-system)
      INSTALL_SYSTEM=0
      ;;
    --skip-npm)
      INSTALL_NPM=0
      ;;
    --skip-browser)
      INSTALL_BROWSER=0
      ;;
    --no-xvfb)
      WITH_XVFB=0
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      err "Unknown argument: $1"
      usage
      exit 2
      ;;
  esac
  shift
done

run_cmd() {
  if [[ "${DRY_RUN}" -eq 1 ]]; then
    printf "[dry-run] %s\n" "$*"
    return 0
  fi
  "$@"
}

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

require_cmd() {
  if ! have_cmd "$1"; then
    err "Required command not found: $1"
    exit 1
  fi
}

pkg_installed() {
  dpkg-query -W -f='${Status}' "$1" 2>/dev/null | grep -q "install ok installed"
}

pkg_exists() {
  apt-cache show "$1" >/dev/null 2>&1
}

pick_pkg() {
  local candidate
  for candidate in "$@"; do
    if pkg_exists "${candidate}"; then
      printf "%s\n" "${candidate}"
      return 0
    fi
  done
  return 1
}

as_root() {
  if [[ "$(id -u)" -eq 0 ]]; then
    "$@"
    return
  fi
  if have_cmd sudo; then
    sudo "$@"
    return
  fi
  err "This step needs root privileges, but sudo is not available."
  exit 1
}

load_os_release() {
  if [[ -r /etc/os-release ]]; then
    # shellcheck disable=SC1091
    . /etc/os-release
  else
    err "Cannot read /etc/os-release"
    exit 1
  fi
}

build_system_packages() {
  SYSTEM_PACKAGES=()

  local required_specs=(
    "libatk1.0-0t64 libatk1.0-0"
    "libatk-bridge2.0-0t64 libatk-bridge2.0-0"
    "libatspi2.0-0t64 libatspi2.0-0"
    "libasound2t64 libasound2"
    "libxcomposite1"
    "libxdamage1"
    "libxfixes3"
    "libxrandr2"
    "libgbm1"
    "libnss3"
    "libnspr4"
    "libgtk-3-0"
    "libdrm2"
    "libdbus-1-3"
    "libxkbcommon0"
    "libx11-6"
    "libxcb1"
    "libxext6"
    "libglib2.0-0"
    "libpango-1.0-0"
    "libcairo2"
    "libcups2t64 libcups2"
    "fonts-liberation"
    "ca-certificates"
  )

  local spec selected
  for spec in "${required_specs[@]}"; do
    # shellcheck disable=SC2206
    local candidates=( ${spec} )
    if ! selected="$(pick_pkg "${candidates[@]}")"; then
      err "Could not resolve an apt package for candidates: ${spec}"
      exit 1
    fi
    SYSTEM_PACKAGES+=("${selected}")
  done

  if [[ "${WITH_XVFB}" -eq 1 ]]; then
    local optional_specs=(
      "xvfb"
      "xauth"
      "x11-utils"
    )
    for spec in "${optional_specs[@]}"; do
      # shellcheck disable=SC2206
      local candidates=( ${spec} )
      if selected="$(pick_pkg "${candidates[@]}")"; then
        SYSTEM_PACKAGES+=("${selected}")
      fi
    done
  fi
}

print_host_summary() {
  header "Host Summary"
  printf "Repo: %s\n" "${REPO_ROOT}"
  printf "User: %s\n" "$(whoami)"
  printf "OS: %s %s\n" "${ID:-unknown}" "${VERSION_ID:-unknown}"
  printf "Node: %s\n" "$(node --version 2>/dev/null || echo missing)"
  printf "npm: %s\n" "$(npm --version 2>/dev/null || echo missing)"
  printf "python3: %s\n" "$(python3 --version 2>/dev/null || echo missing)"
}

print_package_plan() {
  header "System Package Plan"
  local pkg
  for pkg in "${SYSTEM_PACKAGES[@]}"; do
    if pkg_installed "${pkg}"; then
      printf "installed  %s\n" "${pkg}"
    else
      printf "missing    %s\n" "${pkg}"
    fi
  done
}

check_status() {
  local failures=0

  header "Checks"
  local required_cmds=(node npm npx python3)
  local cmd
  for cmd in "${required_cmds[@]}"; do
    if have_cmd "${cmd}"; then
      ok "${cmd}: $(command -v "${cmd}")"
    else
      err "${cmd}: missing"
      failures=1
    fi
  done

  if [[ "${INSTALL_SYSTEM}" -eq 1 ]]; then
    print_package_plan
  fi

  if [[ -x "${REPO_ROOT}/node_modules/.bin/agent-browser" ]]; then
    ok "Repo-local agent-browser binary present"
  else
    warn "Repo-local agent-browser binary not found yet"
  fi

  if [[ -d "${REPO_ROOT}/node_modules/agent-browser" ]]; then
    ok "agent-browser package present under node_modules"
  else
    warn "agent-browser package not installed under node_modules"
  fi

  if [[ -d "${HOME}/.cache/ms-playwright" ]]; then
    ok "Playwright browser cache detected at ${HOME}/.cache/ms-playwright"
  else
    warn "Playwright browser cache not detected yet"
  fi

  if (( failures != 0 )); then
    return 1
  fi
  return 0
}

install_system_packages() {
  local missing=()
  local pkg
  for pkg in "${SYSTEM_PACKAGES[@]}"; do
    if ! pkg_installed "${pkg}"; then
      missing+=("${pkg}")
    fi
  done

  if [[ ${#missing[@]} -eq 0 ]]; then
    ok "All required system packages are already installed"
    return 0
  fi

  header "Installing System Packages"
  printf "Packages to install:\n"
  printf "  %s\n" "${missing[@]}"

  if [[ "${DRY_RUN}" -eq 1 ]]; then
    printf "[dry-run] sudo apt-get update\n"
    printf "[dry-run] sudo apt-get install -y %s\n" "${missing[*]}"
    return 0
  fi

  as_root apt-get update
  as_root apt-get install -y "${missing[@]}"
}

install_repo_npm() {
  header "Installing Repo Node Dependencies"
  if [[ "${DRY_RUN}" -eq 1 ]]; then
    printf "[dry-run] (cd %s && npm install)\n" "${REPO_ROOT}"
    return 0
  fi
  (cd "${REPO_ROOT}" && npm install)
}

install_agent_browser_runtime() {
  header "Installing Agent Browser Runtime"
  if [[ "${DRY_RUN}" -eq 1 ]]; then
    printf "[dry-run] (cd %s && npx agent-browser install)\n" "${REPO_ROOT}"
    return 0
  fi
  (cd "${REPO_ROOT}" && npx agent-browser install)
}

main() {
  load_os_release
  require_cmd apt-cache
  require_cmd dpkg-query
  require_cmd python3
  require_cmd node
  require_cmd npm
  require_cmd npx

  if [[ "${ID:-}" != "ubuntu" && "${ID:-}" != "debian" ]]; then
    err "This installer currently supports Ubuntu/Debian-like systems only."
    exit 1
  fi

  build_system_packages
  print_host_summary

  if [[ "${CHECK_ONLY}" -eq 1 ]]; then
    check_status
    exit $?
  fi

  if [[ "${INSTALL_SYSTEM}" -eq 1 ]]; then
    install_system_packages
  fi

  if [[ "${INSTALL_NPM}" -eq 1 ]]; then
    install_repo_npm
  fi

  if [[ "${INSTALL_BROWSER}" -eq 1 ]]; then
    install_agent_browser_runtime
  fi

  header "Post-Install Check"
  check_status

  header "Next Step"
  printf "Try:\n"
  printf "  cd %s\n" "${REPO_ROOT}"
  printf "  python3 -m src.ozon_browser --headed login\n"
  if [[ "${WITH_XVFB}" -eq 1 ]]; then
    printf "If a display is still needed on the VPS, try:\n"
    printf "  xvfb-run -a python3 -m src.ozon_browser --headed login\n"
  fi
}

main "$@"
