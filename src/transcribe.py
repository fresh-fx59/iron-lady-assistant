"""Async voice transcription via whisper.cpp subprocess."""

import asyncio
import logging
import os
import re
import shutil
import tempfile
from time import monotonic
from pathlib import Path

logger = logging.getLogger(__name__)

_REPO_DIR = Path(__file__).resolve().parent.parent
_DEFAULT_BIN = _REPO_DIR / "whisper.cpp" / "build" / "bin" / "whisper-cli"
_DEFAULT_MODEL = _REPO_DIR / "whisper.cpp" / "models" / "ggml-small.bin"
_LOCAL_BIN = Path.home() / "local" / "bin"

WHISPER_BIN: str = os.getenv("WHISPER_BIN", str(_DEFAULT_BIN))
WHISPER_MODEL: str = os.getenv("WHISPER_MODEL", str(_DEFAULT_MODEL))

# Resolve ffmpeg: system PATH first, then ~/local/bin fallback
FFMPEG_BIN: str = shutil.which("ffmpeg") or str(_LOCAL_BIN / "ffmpeg")

_TIMING_RE = re.compile(
    r"whisper_print_timings:\s+(?P<label>.+?)\s*=\s*(?P<value>\d+(?:\.\d+)?)\s+ms"
)


def is_available() -> bool:
    """Check if whisper.cpp binary, model, and ffmpeg are present."""
    return (
        os.path.isfile(WHISPER_BIN) and os.access(WHISPER_BIN, os.X_OK)
        and os.path.isfile(WHISPER_MODEL)
        and os.path.isfile(FFMPEG_BIN) and os.access(FFMPEG_BIN, os.X_OK)
    )


def _parse_whisper_timings(stderr_text: str) -> dict[str, float]:
    timings: dict[str, float] = {}
    for match in _TIMING_RE.finditer(stderr_text):
        label = match.group("label").strip().replace(" ", "_")
        timings[label] = float(match.group("value"))
    return timings


async def transcribe(audio_path: str) -> str:
    """Transcribe an audio file (any format ffmpeg supports) → text.

    Converts to 16 kHz mono WAV, runs whisper.cpp, returns the text.
    Raises RuntimeError on failure.
    """
    wav_path = tempfile.mktemp(suffix=".wav")
    try:
        # Convert to 16-bit 16 kHz mono WAV (whisper.cpp requirement)
        ffmpeg_started_at = monotonic()
        proc = await asyncio.create_subprocess_exec(
            FFMPEG_BIN, "-y", "-i", audio_path,
            "-ar", "16000", "-ac", "1", "-f", "wav", wav_path,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        ffmpeg_elapsed_ms = (monotonic() - ffmpeg_started_at) * 1000
        if proc.returncode != 0:
            raise RuntimeError(f"ffmpeg conversion failed: {stderr.decode()[-200:]}")

        # Run whisper.cpp
        whisper_started_at = monotonic()
        proc = await asyncio.create_subprocess_exec(
            WHISPER_BIN, "-m", WHISPER_MODEL,
            "-f", wav_path,
            "-nt",          # no timestamps
            "-l", "auto",   # auto-detect language
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        whisper_elapsed_ms = (monotonic() - whisper_started_at) * 1000
        if proc.returncode != 0:
            raise RuntimeError(f"whisper.cpp failed: {stderr.decode()[-200:]}")

        whisper_stderr = stderr.decode(errors="replace")
        whisper_timings = _parse_whisper_timings(whisper_stderr)
        logger.info(
            "Voice transcription timings: source=%s ffmpeg_ms=%.1f whisper_wall_ms=%.1f "
            "whisper_load_ms=%s whisper_mel_ms=%s whisper_encode_ms=%s "
            "whisper_decode_ms=%s whisper_batchd_ms=%s whisper_total_ms=%s",
            os.path.basename(audio_path),
            ffmpeg_elapsed_ms,
            whisper_elapsed_ms,
            whisper_timings.get("load_time"),
            whisper_timings.get("mel_time"),
            whisper_timings.get("encode_time"),
            whisper_timings.get("decode_time"),
            whisper_timings.get("batchd_time"),
            whisper_timings.get("total_time"),
        )

        text = stdout.decode().strip()
        if not text:
            raise RuntimeError("whisper.cpp returned empty transcription")
        return text

    finally:
        if os.path.exists(wav_path):
            os.unlink(wav_path)
