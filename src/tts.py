"""Local text-to-speech helpers for Telegram voice bubbles."""

import asyncio
import difflib
import logging
import os
import re
import shutil
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

_LOCAL_BIN = Path.home() / "local" / "bin"

TTS_BIN: str = os.getenv(
    "LOCAL_TTS_BIN",
    shutil.which("espeak") or shutil.which("espeak-ng") or str(_LOCAL_BIN / "espeak"),
)
TTS_VOICE: str = os.getenv("LOCAL_TTS_VOICE", "auto")
TTS_VOICE_CYRILLIC: str = os.getenv("LOCAL_TTS_VOICE_CYRILLIC", "ru")
TTS_VOICE_LATIN: str = os.getenv("LOCAL_TTS_VOICE_LATIN", "en")
TTS_SPEED: str = os.getenv("LOCAL_TTS_SPEED_WPM", "220")
TTS_SPEED_CYRILLIC: str = os.getenv("LOCAL_TTS_SPEED_WPM_CYRILLIC", "170")
TTS_SPEED_LATIN: str = os.getenv("LOCAL_TTS_SPEED_WPM_LATIN", TTS_SPEED)
TTS_MAX_CHARS: int = int(os.getenv("LOCAL_TTS_MAX_CHARS", "1200"))
FFMPEG_BIN: str = shutil.which("ffmpeg") or str(_LOCAL_BIN / "ffmpeg")
TTS_ENGINE: str = os.getenv("LOCAL_TTS_ENGINE", "auto").strip().lower()

SHERPA_RUNTIME_DIR: str = os.getenv(
    "SHERPA_ONNX_RUNTIME_DIR",
    str(Path.home() / "local" / "sherpa-onnx-tts" / "runtime"),
)
SHERPA_MODEL_DIR: str = os.getenv(
    "SHERPA_ONNX_MODEL_DIR",
    str(Path.home() / "local" / "sherpa-onnx-tts" / "models" / "vits-piper-ru_RU-ruslan-medium"),
)
SHERPA_MODEL_FILE: str = os.getenv(
    "SHERPA_ONNX_MODEL_FILE",
    str(Path(SHERPA_MODEL_DIR) / "ru_RU-ruslan-medium.onnx"),
)
SHERPA_TOKENS_FILE: str = os.getenv(
    "SHERPA_ONNX_TOKENS_FILE",
    str(Path(SHERPA_MODEL_DIR) / "tokens.txt"),
)
SHERPA_DATA_DIR: str = os.getenv(
    "SHERPA_ONNX_DATA_DIR",
    str(Path(SHERPA_MODEL_DIR) / "espeak-ng-data"),
)
SHERPA_BIN: str = os.getenv(
    "SHERPA_ONNX_TTS_BIN",
    str(Path(SHERPA_RUNTIME_DIR) / "bin" / "sherpa-onnx-offline-tts"),
)
SHERPA_LIB_DIR: str = os.getenv(
    "SHERPA_ONNX_LIB_DIR",
    str(Path(SHERPA_RUNTIME_DIR) / "lib"),
)
TTS_VERIFY_INTELLIGIBILITY: bool = (
    os.getenv("LOCAL_TTS_VERIFY_INTELLIGIBILITY", "1").strip().lower() not in {"0", "false", "no"}
)
TTS_MIN_INTELLIGIBILITY_SCORE: float = max(
    0.0,
    min(1.0, float(os.getenv("LOCAL_TTS_MIN_INTELLIGIBILITY_SCORE", "0.55"))),
)
TTS_VERIFY_MAX_CHARS: int = int(os.getenv("LOCAL_TTS_VERIFY_MAX_CHARS", "260"))
TTS_OPUS_BITRATE: str = os.getenv("LOCAL_TTS_OPUS_BITRATE", "48k").strip() or "48k"
TTS_FFMPEG_AF: str = os.getenv(
    "LOCAL_TTS_FFMPEG_AF",
    "highpass=f=120,lowpass=f=7600,loudnorm=I=-16:TP=-1.5:LRA=11",
).strip()

_CODE_BLOCK_RE = re.compile(r"```.*?```", re.DOTALL)
_INLINE_CODE_RE = re.compile(r"`[^`]+`")
_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
_URL_RE = re.compile(r"https?://\S+")
_MARKDOWN_DECOR_RE = re.compile(r"[*_~#>]+")
_CYRILLIC_RE = re.compile(r"[А-Яа-яЁё]")
_LATIN_RE = re.compile(r"[A-Za-z]")


def is_available() -> bool:
    return (
        os.path.isfile(TTS_BIN)
        and os.access(TTS_BIN, os.X_OK)
        and os.path.isfile(FFMPEG_BIN)
        and os.access(FFMPEG_BIN, os.X_OK)
    )


def _sherpa_available() -> bool:
    required_files = (
        SHERPA_BIN,
        SHERPA_MODEL_FILE,
        SHERPA_TOKENS_FILE,
        SHERPA_DATA_DIR,
    )
    return all(os.path.exists(path) for path in required_files)


def _prepare_spoken_text(text: str) -> str:
    spoken_text = (text or "").strip()
    if not spoken_text:
        return ""

    # Remove fragments that sound like gibberish when read verbatim by TTS.
    spoken_text = _CODE_BLOCK_RE.sub(" ", spoken_text)
    spoken_text = _INLINE_CODE_RE.sub(" ", spoken_text)
    spoken_text = _LINK_RE.sub(r"\1", spoken_text)
    spoken_text = _URL_RE.sub(" ссылка ", spoken_text)
    spoken_text = _MARKDOWN_DECOR_RE.sub(" ", spoken_text)
    spoken_text = re.sub(r"\s+", " ", spoken_text).strip()
    if not spoken_text:
        return ""

    if len(spoken_text) > TTS_MAX_CHARS:
        trimmed = spoken_text[:TTS_MAX_CHARS]
        boundary = max(trimmed.rfind("."), trimmed.rfind("!"), trimmed.rfind("?"), trimmed.rfind(" "))
        spoken_text = (trimmed[:boundary] if boundary > 100 else trimmed).rstrip()
    return spoken_text


def _select_voice(spoken_text: str) -> str:
    manual = (TTS_VOICE or "").strip()
    if manual and manual.lower() != "auto":
        return manual
    cyr = len(_CYRILLIC_RE.findall(spoken_text))
    lat = len(_LATIN_RE.findall(spoken_text))
    return TTS_VOICE_CYRILLIC if cyr > lat else TTS_VOICE_LATIN


def _select_speed(spoken_text: str) -> str:
    cyr = len(_CYRILLIC_RE.findall(spoken_text))
    lat = len(_LATIN_RE.findall(spoken_text))
    return TTS_SPEED_CYRILLIC if cyr > lat else TTS_SPEED_LATIN


async def _run_tts_to_wav(
    spoken_text: str,
    wav_path: Path,
    voice: str,
    speed: str,
) -> tuple[int, str]:
    tts_proc = await asyncio.create_subprocess_exec(
        TTS_BIN,
        "--stdin",
        "-v",
        voice,
        "-s",
        speed,
        "-w",
        str(wav_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, tts_stderr = await tts_proc.communicate(spoken_text.encode("utf-8"))
    return tts_proc.returncode, tts_stderr.decode(errors="ignore")


async def _run_sherpa_to_wav(spoken_text: str, wav_path: Path) -> tuple[int, str]:
    env = os.environ.copy()
    if SHERPA_LIB_DIR and os.path.isdir(SHERPA_LIB_DIR):
        env["LD_LIBRARY_PATH"] = (
            f"{SHERPA_LIB_DIR}:{env.get('LD_LIBRARY_PATH', '')}"
            if env.get("LD_LIBRARY_PATH")
            else SHERPA_LIB_DIR
        )
    proc = await asyncio.create_subprocess_exec(
        SHERPA_BIN,
        f"--vits-model={SHERPA_MODEL_FILE}",
        f"--vits-tokens={SHERPA_TOKENS_FILE}",
        f"--vits-data-dir={SHERPA_DATA_DIR}",
        f"--output-filename={wav_path}",
        spoken_text,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    _, stderr = await proc.communicate()
    return proc.returncode, stderr.decode(errors="ignore")


def _is_cyrillic_dominant(text: str) -> bool:
    cyr = len(_CYRILLIC_RE.findall(text))
    lat = len(_LATIN_RE.findall(text))
    return cyr > lat


def _normalize_for_match(text: str) -> str:
    text = (text or "").lower()
    text = re.sub(r"[^a-zа-яё0-9\s]", " ", text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip()


def _intelligibility_score(expected_text: str, actual_text: str) -> float:
    expected = _normalize_for_match(expected_text)
    actual = _normalize_for_match(actual_text)
    if not expected:
        return 1.0
    if not actual:
        return 0.0
    return difflib.SequenceMatcher(None, expected, actual).ratio()


async def _verify_intelligibility(ogg_path: Path, expected_text: str) -> tuple[bool, str]:
    if not TTS_VERIFY_INTELLIGIBILITY:
        return True, "disabled"
    if len(expected_text) > TTS_VERIFY_MAX_CHARS:
        return True, "skipped: long text"

    try:
        from . import transcribe
    except Exception:
        return True, "skipped: transcribe unavailable"

    if not transcribe.is_available():
        return True, "skipped: transcribe unavailable"

    try:
        recognized = await transcribe.transcribe(str(ogg_path))
    except Exception as exc:
        return False, f"verification transcribe failed: {exc}"

    score = _intelligibility_score(expected_text, recognized)
    if score >= TTS_MIN_INTELLIGIBILITY_SCORE:
        return True, f"score={score:.2f}"
    return False, f"low intelligibility score={score:.2f}"


async def _convert_wav_to_ogg(wav_path: Path, ogg_path: Path) -> tuple[int, str]:
    ffmpeg_args = [
        FFMPEG_BIN,
        "-y",
        "-i",
        str(wav_path),
        "-c:a",
        "libopus",
        "-b:a",
        TTS_OPUS_BITRATE,
        "-vbr",
        "on",
        "-compression_level",
        "10",
        "-application",
        "voip",
        "-ar",
        "48000",
        "-ac",
        "1",
    ]
    if TTS_FFMPEG_AF:
        ffmpeg_args.extend(["-af", TTS_FFMPEG_AF])
    ffmpeg_args.append(str(ogg_path))

    ffmpeg_proc = await asyncio.create_subprocess_exec(
        *ffmpeg_args,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, ffmpeg_stderr = await ffmpeg_proc.communicate()
    return ffmpeg_proc.returncode, ffmpeg_stderr.decode(errors="ignore")


async def synthesize_voice(text: str) -> str:
    """Synthesize text to OGG/Opus suitable for Telegram sendVoice."""
    spoken_text = _prepare_spoken_text(text)
    if not spoken_text:
        raise RuntimeError("Cannot synthesize empty text")

    tmp_dir = Path(tempfile.mkdtemp(prefix="ila_tts_"))
    wav_path = tmp_dir / "speech.wav"
    ogg_path = tmp_dir / "speech.ogg"

    try:
        use_sherpa = (
            TTS_ENGINE == "sherpa"
            or (
                TTS_ENGINE == "auto"
                and _is_cyrillic_dominant(spoken_text)
                and _sherpa_available()
            )
        )

        selected_voice = _select_voice(spoken_text)
        selected_speed = _select_speed(spoken_text)
        attempts: list[tuple[str, str, str]] = []
        seen: set[tuple[str, str, str]] = set()

        def add_attempt(engine: str, voice: str = "", speed: str = "") -> None:
            key = (engine, voice, speed)
            if key not in seen:
                seen.add(key)
                attempts.append(key)

        if use_sherpa:
            add_attempt("sherpa")
        add_attempt("espeak", selected_voice, selected_speed)
        try:
            slower_speed = str(max(120, int(selected_speed) - 20))
        except ValueError:
            slower_speed = "150"
        add_attempt("espeak", selected_voice, slower_speed)
        if selected_voice != TTS_VOICE_LATIN:
            add_attempt("espeak", TTS_VOICE_LATIN, TTS_SPEED_LATIN)

        errors: list[str] = []
        for engine, voice, speed in attempts:
            cleanup_file(str(wav_path))
            cleanup_file(str(ogg_path))

            if engine == "sherpa":
                code, stderr_text = await _run_sherpa_to_wav(spoken_text, wav_path)
            else:
                code, stderr_text = await _run_tts_to_wav(spoken_text, wav_path, voice, speed)

            if code != 0:
                errors.append(f"{engine} failed: {stderr_text[-120:]}")
                continue

            ff_code, ff_stderr = await _convert_wav_to_ogg(wav_path, ogg_path)
            if ff_code != 0:
                errors.append(f"ffmpeg failed: {ff_stderr[-120:]}")
                continue
            if not ogg_path.exists():
                errors.append("ogg output missing")
                continue

            ok, detail = await _verify_intelligibility(ogg_path, spoken_text)
            if ok:
                cleanup_file(str(wav_path))
                return str(ogg_path)
            errors.append(f"{engine} rejected: {detail}")

        raise RuntimeError(f"TTS synthesis failed after retries: {' | '.join(errors)[-450:]}")
    except Exception:
        cleanup_file(str(ogg_path))
        cleanup_file(str(wav_path))
        try:
            tmp_dir.rmdir()
        except OSError:
            pass
        raise


def cleanup_file(path: str) -> None:
    try:
        Path(path).unlink(missing_ok=True)
    except Exception:
        logger.debug("Failed to cleanup temporary TTS file: %s", path)
