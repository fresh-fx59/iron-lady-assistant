#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Create venv and install deps if missing
if [ ! -d "venv" ]; then
    python3 -m venv venv
    venv/bin/pip install --quiet -r requirements.txt
fi

source venv/bin/activate
exec python3 -m src.main
