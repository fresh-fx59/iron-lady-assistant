#!/usr/bin/env bash
set -euo pipefail

DISPLAY_NUM="${OZON_DISPLAY:-:99}"
VNC_PORT="${OZON_VNC_PORT:-5901}"
PASSWD_FILE="${OZON_VNC_PASSWD_FILE:-$HOME/.vnc/passwd}"

if ! command -v x11vnc >/dev/null 2>&1; then
  echo "x11vnc is not installed. Install it first: sudo apt-get install -y x11vnc" >&2
  exit 1
fi

if [[ ! -f "${PASSWD_FILE}" ]]; then
  echo "VNC password file not found: ${PASSWD_FILE}" >&2
  echo "Create it first with: x11vnc -storepasswd" >&2
  exit 1
fi

existing="$(pgrep -af "x11vnc .* -display ${DISPLAY_NUM}" | head -n 1 || true)"
if [[ -n "${existing}" ]]; then
  if [[ "${existing}" =~ -rfbport[[:space:]]+([0-9]+) ]]; then
    echo "x11vnc already running for ${DISPLAY_NUM} on localhost:${BASH_REMATCH[1]}"
  else
    echo "x11vnc already running for ${DISPLAY_NUM}"
  fi
  exit 0
fi

port_is_free() {
  python3 - "$1" <<'PY'
import socket
import sys

port = int(sys.argv[1])
for family, host in ((socket.AF_INET, "127.0.0.1"), (socket.AF_INET6, "::1")):
    try:
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.bind((host, port))
        sock.close()
    except OSError:
        raise SystemExit(1)
raise SystemExit(0)
PY
}

if ! port_is_free "${VNC_PORT}"; then
  original_port="${VNC_PORT}"
  for candidate in 5902 5903 5904 5905 5906; do
    if port_is_free "${candidate}"; then
      VNC_PORT="${candidate}"
      break
    fi
  done
  if [[ "${VNC_PORT}" == "${original_port}" ]]; then
    echo "Requested VNC port ${original_port} is busy and no fallback port was available." >&2
    exit 1
  fi
  echo "Requested VNC port ${original_port} is busy; using localhost:${VNC_PORT} instead."
fi

exec x11vnc \
  -display "${DISPLAY_NUM}" \
  -localhost \
  -rfbauth "${PASSWD_FILE}" \
  -rfbport "${VNC_PORT}" \
  -forever \
  -shared
