"""Async OCR extraction from images using local Tesseract."""

import asyncio
import os
import shutil
from pathlib import Path

_LOCAL_BIN = Path.home() / "local" / "bin"
TESSERACT_BIN: str = os.getenv("TESSERACT_BIN", "") or (
    shutil.which("tesseract") or str(_LOCAL_BIN / "tesseract")
)
OCR_LANG: str = os.getenv("OCR_LANG", "eng")


def is_available() -> bool:
    """Check whether Tesseract OCR binary is available."""
    return os.path.isfile(TESSERACT_BIN) and os.access(TESSERACT_BIN, os.X_OK)


async def extract_text(image_path: str) -> str:
    """Extract text from image file using Tesseract."""
    proc = await asyncio.create_subprocess_exec(
        TESSERACT_BIN,
        image_path,
        "stdout",
        "-l",
        OCR_LANG,
        "--oem",
        "1",
        "--psm",
        "6",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"tesseract failed: {stderr.decode(errors='ignore')[-200:]}")

    return stdout.decode(errors="ignore").strip()
