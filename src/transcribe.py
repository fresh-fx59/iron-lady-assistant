"""Async voice transcription via whisper.cpp subprocess."""

import asyncio
import logging
import os
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

_REPO_DIR = Path(__file__).resolve().parent.parent
_DEFAULT_BIN = _REPO_DIR / "whisper.cpp" / "build" / "bin" / "whisper-cli"
_DEFAULT_MODEL = _REPO_DIR / "whisper.cpp" / "models" / "ggml-small.bin"

WHISPER_BIN: str = os.getenv("WHISPER_BIN", str(_DEFAULT_BIN))
WHISPER_MODEL: str = os.getenv("WHISPER_MODEL", str(_DEFAULT_MODEL))


def is_available() -> bool:
    """Check if whisper.cpp binary and model are present."""
    return os.path.isfile(WHISPER_BIN) and os.access(WHISPER_BIN, os.X_OK) and os.path.isfile(WHISPER_MODEL)


async def transcribe(audio_path: str) -> str:
    """Transcribe an audio file (any format ffmpeg supports) → text.

    Converts to 16 kHz mono WAV, runs whisper.cpp, returns the text.
    Raises RuntimeError on failure.
    """
    wav_path = tempfile.mktemp(suffix=".wav")
    try:
        # Convert to 16-bit 16 kHz mono WAV (whisper.cpp requirement)
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-i", audio_path,
            "-ar", "16000", "-ac", "1", "-f", "wav", wav_path,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"ffmpeg conversion failed: {stderr.decode()[-200:]}")

        # Run whisper.cpp
        proc = await asyncio.create_subprocess_exec(
            WHISPER_BIN, "-m", WHISPER_MODEL,
            "-f", wav_path,
            "-nt",          # no timestamps
            "-l", "auto",   # auto-detect language
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"whisper.cpp failed: {stderr.decode()[-200:]}")

        text = stdout.decode().strip()
        if not text:
            raise RuntimeError("whisper.cpp returned empty transcription")
        return text

    finally:
        if os.path.exists(wav_path):
            os.unlink(wav_path)
