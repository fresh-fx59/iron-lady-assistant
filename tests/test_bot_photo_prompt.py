import pytest

from src import bot


class _MessageStub:
    def __init__(self, text=None, caption=None, reply_to_message=None):
        self.text = text
        self.caption = caption
        self.reply_to_message = reply_to_message


@pytest.mark.asyncio
async def test_compose_incoming_prompt_without_photo_uses_caption(monkeypatch):
    message = _MessageStub(text=None, caption="look at this")

    async def _fake_download(_m):
        return None

    def _fail_ocr(_path):
        raise AssertionError("OCR should not run for non-image messages")

    monkeypatch.setattr(bot, "_download_photo_attachment", _fake_download)
    monkeypatch.setattr(bot, "extract_ocr_text", _fail_ocr)

    prompt = await bot._compose_incoming_prompt(message)

    assert prompt == "look at this"


@pytest.mark.asyncio
async def test_compose_incoming_prompt_with_photo_appends_local_path(monkeypatch, tmp_path):
    message = _MessageStub(text="what is on the image?", caption=None)

    # _attachment_blocks_for_message now skips images whose path does not exist on disk.
    image_path = tmp_path / "test.jpg"
    image_path.write_bytes(b"\xff\xd8\xff\xe0")  # minimal JPEG magic bytes

    async def _fake_download(_m):
        return str(image_path)

    def _fake_ocr(_path):
        return "Total: 1234"

    monkeypatch.setattr(bot, "_download_photo_attachment", _fake_download)
    monkeypatch.setattr(bot, "extract_ocr_text", _fake_ocr)

    prompt = await bot._compose_incoming_prompt(message)

    assert "what is on the image?" in prompt
    assert "User attached an image." in prompt
    assert f"Local image path: {image_path}" in prompt
    assert "Local OCR text (best-effort; low-quality images may include misreads):" in prompt
    assert "Total: 1234" in prompt


@pytest.mark.asyncio
async def test_compose_incoming_prompt_with_document_appends_preview(monkeypatch):
    message = _MessageStub(text="Summarize this file")

    async def _fake_download_photo(_m):
        return None

    async def _fake_attachment_blocks(_m, relation="current"):
        if relation != "current":
            return []
        return [
            "User attached a file.\n"
            "Filename: notes.txt\n"
            "Local file path: /tmp/incoming/notes.txt\n"
            "Extracted text preview (best-effort):\n"
            "alpha beta gamma"
        ]

    monkeypatch.setattr(bot, "_download_photo_attachment", _fake_download_photo)
    monkeypatch.setattr(bot, "_attachment_blocks_for_message", _fake_attachment_blocks)

    prompt = await bot._compose_incoming_prompt(message)

    assert "Summarize this file" in prompt
    assert "User attached a file." in prompt
    assert "Local file path: /tmp/incoming/notes.txt" in prompt
    assert "alpha beta gamma" in prompt


@pytest.mark.asyncio
async def test_compose_incoming_prompt_with_audio_attachment_uses_saved_file_block(monkeypatch):
    message = _MessageStub(text="Transcribe or inspect this")

    async def _fake_attachment_blocks(_m, relation="current"):
        if relation != "current":
            return []
        return [
            "User attached audio file.\n"
            "Filename: song.mp3\n"
            "Local file path: /tmp/incoming/song.mp3\n"
            "MIME type: audio/mpeg\n"
            "No local text preview extracted. Read the local file path directly when needed."
        ]

    monkeypatch.setattr(bot, "_attachment_blocks_for_message", _fake_attachment_blocks)

    prompt = await bot._compose_incoming_prompt(message)

    assert "Transcribe or inspect this" in prompt
    assert "User attached audio file." in prompt
    assert "Local file path: /tmp/incoming/song.mp3" in prompt


@pytest.mark.asyncio
async def test_download_generic_attachment_supports_audio_when_not_sent_as_document(monkeypatch, tmp_path):
    target_dir = tmp_path / "incoming"
    monkeypatch.setattr(bot, "_INCOMING_MEDIA_DIR", target_dir)

    class _AudioStub:
        file_id = "audio-file-id"
        file_name = None
        mime_type = "audio/mpeg"
        file_size = 321
        performer = "Artist"
        title = "Track"

    class _BotStub:
        async def get_file(self, _file_id):
            return type("TelegramFile", (), {"file_path": "telegram/audio/track.mp3"})()

        async def download_file(self, _file_path, destination):
            destination = bot.Path(destination)
            destination.write_bytes(b"ID3")

    message = type("MessageStub", (), {})()
    message.audio = _AudioStub()
    message.document = None
    message.video = None
    message.animation = None
    message.video_note = None
    message.bot = _BotStub()
    message.chat = type("ChatStub", (), {"id": 123})()
    message.message_id = 456

    monkeypatch.setattr(bot, "_text_preview_from_file", lambda path, mime_type=None: "")

    info = await bot._download_generic_attachment(message)

    assert info is not None
    assert info["relation_label"] == "audio file"
    assert info["mime_type"] == "audio/mpeg"
    assert info["size_bytes"] == 321
    assert info["file_name"] == "track.mp3"
    assert bot.Path(str(info["path"])).exists()


@pytest.mark.asyncio
async def test_compose_incoming_prompt_text_reply_includes_replied_attachment(monkeypatch):
    replied_message = _MessageStub(text=None, caption="file")
    message = _MessageStub(text="What are key points?", reply_to_message=replied_message)

    async def _fake_attachment_blocks(msg, relation="current"):
        if relation == "current":
            return []
        if msg is replied_message:
            return [
                "User referenced an earlier file in this message.\n"
                "User attached a file.\n"
                "Filename: report.md\n"
                "Local file path: /tmp/incoming/report.md\n"
                "Extracted text preview (best-effort):\n"
                "Project status is green."
            ]
        return []

    monkeypatch.setattr(bot, "_attachment_blocks_for_message", _fake_attachment_blocks)

    prompt = await bot._compose_incoming_prompt(message)

    assert "What are key points?" in prompt
    assert "User referenced an earlier file in this message." in prompt
    assert "Local file path: /tmp/incoming/report.md" in prompt
