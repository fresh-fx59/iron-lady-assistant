#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# One-time setup: build whisper.cpp and download the small model.
# Run with: bash setup_whisper.sh
# Requires: sudo (for apt install), internet access.
# ─────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WHISPER_DIR="$SCRIPT_DIR/whisper.cpp"
MODEL="small"

echo "=== Installing build dependencies ==="
sudo apt-get update -qq
sudo apt-get install -y -qq cmake g++ ffmpeg

if [ -d "$WHISPER_DIR" ]; then
    echo "=== Updating whisper.cpp ==="
    cd "$WHISPER_DIR"
    git pull --ff-only
else
    echo "=== Cloning whisper.cpp ==="
    git clone https://github.com/ggerganov/whisper.cpp "$WHISPER_DIR"
    cd "$WHISPER_DIR"
fi

echo "=== Building whisper.cpp ==="
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"

echo "=== Downloading ggml-$MODEL model ==="
./models/download-ggml-model.sh "$MODEL"

# Verify
BIN="$WHISPER_DIR/build/bin/whisper-cli"
MODEL_FILE="$WHISPER_DIR/models/ggml-$MODEL.bin"

if [ -x "$BIN" ] && [ -f "$MODEL_FILE" ]; then
    echo ""
    echo "=== Success ==="
    echo "  Binary: $BIN"
    echo "  Model:  $MODEL_FILE"
    echo ""
    echo "whisper.cpp is ready. The bot will auto-detect it."
else
    echo "ERROR: Build or model download failed."
    exit 1
fi
