from unittest.mock import AsyncMock, patch

import pytest

from src.bot import (
    _extract_media_directives,
    _maybe_add_local_tts_media,
    _sanitize_voice_capability_text,
    _send_media_refs,
    _voice_reply_language_hint,
    _wants_voice_reply,
)


def test_extract_media_directives_with_audio_as_voice_tag():
    text = """
Here is your update
[[audio_as_voice]]
MEDIA:/tmp/reply.wav
"""
    clean_text, media_refs, audio_as_voice = _extract_media_directives(text)
    assert clean_text == "Here is your update"
    assert media_refs == ["/tmp/reply.wav"]
    assert audio_as_voice is True


def test_extract_media_directives_plain_local_media_path(tmp_path):
    voice_path = tmp_path / "speech.ogg"
    voice_path.write_bytes(b"ogg")
    text = f"{voice_path}\n"
    clean_text, media_refs, audio_as_voice = _extract_media_directives(text)
    assert clean_text == ""
    assert media_refs == [str(voice_path)]
    assert audio_as_voice is False


def test_wants_voice_reply_detects_russian_prompt():
    assert _wants_voice_reply("пришли мне войс")


def test_wants_voice_reply_detects_english_prompt():
    assert _wants_voice_reply("please send voice reply")


def test_wants_voice_reply_ignores_regular_text():
    assert not _wants_voice_reply("send text answer")


def test_voice_reply_language_hint_prefers_russian_for_cyrillic_prompt():
    hint = _voice_reply_language_hint("Расскажи коротко про Иран")
    assert hint == "\n\nОтвечай по-русски."


def test_voice_reply_language_hint_prefers_english_for_latin_prompt():
    hint = _voice_reply_language_hint("Give me a short update")
    assert hint == "\n\nReply in English."


def test_sanitize_voice_capability_text_rewrites_interface_limitation_for_voice_request():
    text = (
        "Не могу физически отправить voice-note из этого интерфейса напрямую. "
        "Пришли текст."
    )
    sanitized = _sanitize_voice_capability_text(text, request_voice_reply=True)
    assert sanitized == "Пришли текст для озвучки, и я отправлю его голосовой заметкой в Telegram."


def test_sanitize_voice_capability_text_keeps_regular_response():
    text = "Готово. Сейчас отправляю голосовое сообщение."
    sanitized = _sanitize_voice_capability_text(text, request_voice_reply=True)
    assert sanitized == text


@pytest.mark.asyncio
async def test_send_media_refs_prefers_voice_for_ogg():
    message = AsyncMock()
    message.chat.id = 123

    await _send_media_refs(message, ["https://example.com/reply.ogg"], audio_as_voice=False)

    message.answer_voice.assert_called_once()
    message.answer_audio.assert_not_called()
    message.answer_document.assert_not_called()


@pytest.mark.asyncio
async def test_send_media_refs_audio_as_voice_for_wav():
    message = AsyncMock()
    message.chat.id = 123

    await _send_media_refs(message, ["https://example.com/reply.wav"], audio_as_voice=True)

    message.answer_voice.assert_called_once()
    message.answer_audio.assert_not_called()


@pytest.mark.asyncio
async def test_send_media_refs_regular_attachment_for_non_media():
    message = AsyncMock()
    message.chat.id = 123

    await _send_media_refs(message, ["https://example.com/file.zip"], audio_as_voice=False)

    message.answer_document.assert_called_once()
    message.answer_voice.assert_not_called()
    message.answer_audio.assert_not_called()


@pytest.mark.asyncio
async def test_send_media_refs_falls_back_to_audio_when_voice_fails():
    message = AsyncMock()
    message.chat.id = 123
    message.answer_voice.side_effect = RuntimeError("voice failed")

    await _send_media_refs(message, ["https://example.com/reply.ogg"], audio_as_voice=False)

    message.answer_voice.assert_called_once()
    message.answer_audio.assert_called_once()


@pytest.mark.asyncio
async def test_maybe_add_local_tts_media_for_voice_request():
    with (
        patch("src.bot.tts.is_available", return_value=True),
        patch("src.bot.tts.synthesize_voice", new=AsyncMock(return_value="/tmp/ila_tts/speech.ogg")),
    ):
        media_refs, generated = await _maybe_add_local_tts_media(
            clean_text="Voice response text",
            media_refs=["https://example.com/plot.png"],
            request_voice_reply=True,
        )

    assert generated == "/tmp/ila_tts/speech.ogg"
    assert media_refs == ["/tmp/ila_tts/speech.ogg", "https://example.com/plot.png"]


@pytest.mark.asyncio
async def test_maybe_add_local_tts_media_noop_when_tts_unavailable():
    with patch("src.bot.tts.is_available", return_value=False):
        media_refs, generated = await _maybe_add_local_tts_media(
            clean_text="Voice response text",
            media_refs=["https://example.com/plot.png"],
            request_voice_reply=True,
        )

    assert generated is None
    assert media_refs == ["https://example.com/plot.png"]
