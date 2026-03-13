#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
STATE_ROOT="${XDG_STATE_HOME:-$HOME/.local/state}/iron-lady-assistant/ozon-browser"
PROFILE_PATH="${OZON_PROFILE_PATH:-${STATE_ROOT}/manual-chrome-profile}"
DOWNLOAD_PATH="${OZON_DOWNLOAD_PATH:-${STATE_ROOT}/manual-chrome-downloads}"
DISPLAY_NUM="${OZON_DISPLAY:-:99}"
CDP_PORT="${OZON_CDP_PORT:-9222}"
LOG_PATH="${OZON_CHROME_LOG:-/tmp/ozon-manual-chrome.log}"
XVFB_LOG_PATH="${OZON_XVFB_LOG:-/tmp/ozon-xvfb.log}"

mkdir -p "${PROFILE_PATH}" "${DOWNLOAD_PATH}"
rm -f "${PROFILE_PATH}/SingletonCookie" "${PROFILE_PATH}/SingletonLock" "${PROFILE_PATH}/SingletonSocket"

CHROME_BIN="${OZON_CHROME_BIN:-}"
if [[ -z "${CHROME_BIN}" ]]; then
  for candidate in \
    "$HOME/.cache/ms-playwright/chromium-1208/chrome-linux64/chrome" \
    "$(command -v google-chrome 2>/dev/null || true)" \
    "$(command -v google-chrome-stable 2>/dev/null || true)" \
    "$(command -v chromium 2>/dev/null || true)" \
    "$(command -v chromium-browser 2>/dev/null || true)"; do
    if [[ -n "${candidate}" && -x "${candidate}" ]]; then
      CHROME_BIN="${candidate}"
      break
    fi
  done
fi

if [[ -z "${CHROME_BIN}" ]]; then
  echo "No Chrome/Chromium binary found. Set OZON_CHROME_BIN explicitly." >&2
  exit 1
fi

if ! pgrep -af "Xvfb ${DISPLAY_NUM}" >/dev/null 2>&1; then
  Xvfb "${DISPLAY_NUM}" -screen 0 1440x900x24 -nolisten tcp >"${XVFB_LOG_PATH}" 2>&1 &
  sleep 1
fi

pkill -f "${CHROME_BIN}.*remote-debugging-port=${CDP_PORT}" >/dev/null 2>&1 || true

nohup env DISPLAY="${DISPLAY_NUM}" "${CHROME_BIN}" \
  --user-data-dir="${PROFILE_PATH}" \
  --remote-debugging-port="${CDP_PORT}" \
  --no-sandbox \
  --no-first-run \
  --no-default-browser-check \
  --password-store=basic \
  about:blank \
  >"${LOG_PATH}" 2>&1 </dev/null &

sleep 2

echo "display=${DISPLAY_NUM}"
echo "cdp_port=${CDP_PORT}"
echo "profile_path=${PROFILE_PATH}"
echo "download_path=${DOWNLOAD_PATH}"
echo "chrome_bin=${CHROME_BIN}"
echo "next_attach=python3 -m src.ozon_browser --cdp ${CDP_PORT} --session ozon login"
