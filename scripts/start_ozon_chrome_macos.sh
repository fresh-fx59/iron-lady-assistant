#!/usr/bin/env bash
set -euo pipefail

STATE_ROOT="${XDG_STATE_HOME:-$HOME/.local/state}/iron-lady-assistant/ozon-browser"
PROFILE_PATH="${OZON_PROFILE_PATH:-${STATE_ROOT}/manual-chrome-profile}"
DOWNLOAD_PATH="${OZON_DOWNLOAD_PATH:-${STATE_ROOT}/manual-chrome-downloads}"
CDP_PORT="${OZON_CDP_PORT:-9222}"
LOG_PATH="${OZON_CHROME_LOG:-/tmp/ozon-manual-chrome-macos.log}"

mkdir -p "${PROFILE_PATH}" "${DOWNLOAD_PATH}"
rm -f "${PROFILE_PATH}/SingletonCookie" "${PROFILE_PATH}/SingletonLock" "${PROFILE_PATH}/SingletonSocket"

CHROME_BIN="${OZON_CHROME_BIN:-}"
if [[ -z "${CHROME_BIN}" ]]; then
  for candidate in \
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
    "$HOME/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
    "/Applications/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing" \
    "$HOME/Applications/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing" \
    "/Applications/Chromium.app/Contents/MacOS/Chromium" \
    "$HOME/Applications/Chromium.app/Contents/MacOS/Chromium"; do
    if [[ -x "${candidate}" ]]; then
      CHROME_BIN="${candidate}"
      break
    fi
  done
fi

if [[ -z "${CHROME_BIN}" ]]; then
  echo "No Chrome/Chromium app found. Install Google Chrome or set OZON_CHROME_BIN explicitly." >&2
  exit 1
fi

pkill -f "${CHROME_BIN}.*remote-debugging-port=${CDP_PORT}" >/dev/null 2>&1 || true

nohup "${CHROME_BIN}" \
  --user-data-dir="${PROFILE_PATH}" \
  --remote-debugging-port="${CDP_PORT}" \
  --no-first-run \
  --no-default-browser-check \
  --password-store=basic \
  about:blank \
  >"${LOG_PATH}" 2>&1 </dev/null &

sleep 2

echo "cdp_port=${CDP_PORT}"
echo "profile_path=${PROFILE_PATH}"
echo "download_path=${DOWNLOAD_PATH}"
echo "chrome_bin=${CHROME_BIN}"
echo "log_path=${LOG_PATH}"
echo "next_attach=python3 -m src.ozon_browser --cdp ${CDP_PORT} --session ozon login"
