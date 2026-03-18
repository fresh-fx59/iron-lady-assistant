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
async def test_compose_incoming_prompt_with_photo_appends_local_path(monkeypatch):
    message = _MessageStub(text="what is on the image?", caption=None)

    async def _fake_download(_m):
        return "/tmp/incoming/test.jpg"

    def _fake_ocr(_path):
        return "Total: 1234"

    monkeypatch.setattr(bot, "_download_photo_attachment", _fake_download)
    monkeypatch.setattr(bot, "extract_ocr_text", _fake_ocr)

    prompt = await bot._compose_incoming_prompt(message)

    assert "what is on the image?" in prompt
    assert "User attached an image." in prompt
    assert "Local image path: /tmp/incoming/test.jpg" in prompt
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
