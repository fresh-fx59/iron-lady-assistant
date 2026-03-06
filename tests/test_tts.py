from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from src import tts


def test_prepare_spoken_text_strips_code_and_urls() -> None:
    text = "Привет! ```python\nprint('x')\n``` подробнее на https://example.com и `inline`."
    prepared = tts._prepare_spoken_text(text)
    assert "print('x')" not in prepared
    assert "https://example.com" not in prepared
    assert "inline" not in prepared
    assert "Привет" in prepared


def test_select_voice_prefers_cyrillic(monkeypatch) -> None:
    monkeypatch.setattr(tts, "TTS_VOICE", "auto")
    monkeypatch.setattr(tts, "TTS_VOICE_CYRILLIC", "ru")
    monkeypatch.setattr(tts, "TTS_VOICE_LATIN", "en")
    assert tts._select_voice("Это тест русского текста") == "ru"


def test_select_voice_prefers_latin(monkeypatch) -> None:
    monkeypatch.setattr(tts, "TTS_VOICE", "auto")
    monkeypatch.setattr(tts, "TTS_VOICE_CYRILLIC", "ru")
    monkeypatch.setattr(tts, "TTS_VOICE_LATIN", "en")
    assert tts._select_voice("This is a test sentence in English") == "en"


def test_select_speed_prefers_cyrillic(monkeypatch) -> None:
    monkeypatch.setattr(tts, "TTS_SPEED_CYRILLIC", "170")
    monkeypatch.setattr(tts, "TTS_SPEED_LATIN", "220")
    assert tts._select_speed("Это тест русского текста") == "170"


def test_select_speed_prefers_latin(monkeypatch) -> None:
    monkeypatch.setattr(tts, "TTS_SPEED_CYRILLIC", "170")
    monkeypatch.setattr(tts, "TTS_SPEED_LATIN", "220")
    assert tts._select_speed("This is a test sentence in English") == "220"


def test_is_cyrillic_dominant() -> None:
    assert tts._is_cyrillic_dominant("Привет как дела")
    assert not tts._is_cyrillic_dominant("hello world")


@pytest.mark.asyncio
async def test_synthesize_voice_prefers_sherpa_for_cyrillic(monkeypatch):
    monkeypatch.setattr(tts, "_prepare_spoken_text", lambda _text: "Привет, Алекс")
    monkeypatch.setattr(tts, "TTS_ENGINE", "auto")
    monkeypatch.setattr(tts, "_sherpa_available", lambda: True)
    monkeypatch.setattr(tts, "_run_sherpa_to_wav", AsyncMock(return_value=(0, "")))
    monkeypatch.setattr(tts, "_run_tts_to_wav", AsyncMock(return_value=(0, "")))

    class _OkProc:
        returncode = 0
        async def communicate(self):
            return b"", b""

    async def _fake_create_subprocess_exec(*args, **kwargs):
        Path(args[-1]).write_bytes(b"ogg")
        return _OkProc()

    monkeypatch.setattr(
        tts.asyncio,
        "create_subprocess_exec",
        AsyncMock(side_effect=_fake_create_subprocess_exec),
    )

    out = await tts.synthesize_voice("ignored")

    assert out.endswith(".ogg")
    tts._run_sherpa_to_wav.assert_awaited_once()
    tts._run_tts_to_wav.assert_not_awaited()
