"""tests/test_aggregator_image.py — Feature A2: gpt-image-2 English infographic.

The image generator is ported verbatim from dzen-autopilot's imagegen (stdlib
urllib on the HTTP path, injectable transport) so tests NEVER touch the network
or the broker. On top of the generic generator sit two aggregator-specific
helpers: an English-only infographic prompt builder (gpt-image-2 garbles
Cyrillic — operator decision) and the compose-then-generate wrapper.
"""
from __future__ import annotations

import base64
import urllib.error

import pytest

from src.telegram_aggregator_image import (
    ImageGenError,
    build_image_prompt,
    generate_digest_image,
    generate_image,
)

# A real 1x1 RGBA PNG, base64-encoded. Decoding yields valid PNG bytes starting
# with the PNG signature — no network needed.
PNG_B64 = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAF"
    "AAH/iZk9HQAAAABJRU5ErkJggg=="
)
PNG_BYTES = base64.b64decode(PNG_B64)
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


def _key_file(tmp_path, key: str = "  sk-test-key  \n"):
    kf = tmp_path / "key.txt"
    kf.write_text(key, encoding="utf-8")
    return kf


# --- Step 10: ported generator (b64_json happy path) ------------------------


def test_b64_json_writes_png(tmp_path):
    out = tmp_path / "img.png"

    def fake_transport(url, headers, body):
        return {"data": [{"b64_json": PNG_B64}]}

    result = generate_image(
        "an editorial infographic",
        out,
        key_file=_key_file(tmp_path),
        transport=fake_transport,
    )
    assert result == out
    assert out.read_bytes() == PNG_BYTES
    assert out.read_bytes()[:8] == PNG_SIGNATURE


def test_request_shape_and_key_are_correct(tmp_path):
    captured = {}

    def fake_transport(url, headers, body):
        captured["url"] = url
        captured["headers"] = headers
        captured["body"] = body
        return {"data": [{"b64_json": PNG_B64}]}

    generate_image(
        "prompt text",
        tmp_path / "img.png",
        key_file=_key_file(tmp_path, key="  my-secret-key\n"),
        base_url="http://127.0.0.1:8317/v1",
        model="gpt-image-2",
        size="1536x1024",
        transport=fake_transport,
    )
    assert captured["url"].endswith("/images/generations")
    assert captured["headers"]["Authorization"] == "Bearer my-secret-key"  # stripped
    assert captured["body"] == {
        "model": "gpt-image-2",
        "prompt": "prompt text",
        "n": 1,
        "size": "1536x1024",
    }


def test_url_variant_downloads_image(tmp_path):
    src = tmp_path / "source.png"
    src.write_bytes(PNG_BYTES)
    file_url = src.as_uri()
    out = tmp_path / "img.png"

    generate_image(
        "p",
        out,
        key_file=_key_file(tmp_path),
        transport=lambda url, headers, body: {"data": [{"url": file_url}]},
    )
    assert out.read_bytes() == PNG_BYTES


def test_http_error_raises(tmp_path):
    def fake_transport(url, headers, body):
        raise urllib.error.HTTPError(url, 500, "Internal Server Error", {}, None)

    with pytest.raises(ImageGenError):
        generate_image("p", tmp_path / "img.png", key_file=_key_file(tmp_path), transport=fake_transport)


def test_missing_data_array_raises(tmp_path):
    with pytest.raises(ImageGenError):
        generate_image(
            "p",
            tmp_path / "img.png",
            key_file=_key_file(tmp_path),
            transport=lambda url, headers, body: {"data": []},
        )


def test_item_without_b64_or_url_raises(tmp_path):
    with pytest.raises(ImageGenError):
        generate_image(
            "p",
            tmp_path / "img.png",
            key_file=_key_file(tmp_path),
            transport=lambda url, headers, body: {"data": [{"revised_prompt": "x"}]},
        )


def test_bad_base64_raises(tmp_path):
    with pytest.raises(ImageGenError):
        generate_image(
            "p",
            tmp_path / "img.png",
            key_file=_key_file(tmp_path),
            transport=lambda url, headers, body: {"data": [{"b64_json": "!!!not base64!!!"}]},
        )


def test_empty_key_file_raises(tmp_path):
    with pytest.raises(ImageGenError):
        generate_image(
            "p",
            tmp_path / "img.png",
            key_file=_key_file(tmp_path, key="   \n"),
            transport=lambda url, headers, body: {"data": [{"b64_json": PNG_B64}]},
        )


def test_no_image_written_on_error(tmp_path):
    out = tmp_path / "img.png"

    def fake_transport(url, headers, body):
        raise urllib.error.HTTPError(url, 502, "Bad Gateway", {}, None)

    with pytest.raises(ImageGenError):
        generate_image("p", out, key_file=_key_file(tmp_path), transport=fake_transport)
    assert not out.exists()


# --- Step 11: English infographic prompt builder ----------------------------


def test_build_image_prompt_english_and_dated():
    prompt = build_image_prompt(["Новая модель GPT", "Релиз ИИ-агента"], date_label="24.07.2026")
    assert prompt.strip()
    assert "24.07.2026" in prompt  # the date is baked in
    assert "English" in prompt  # explicit English-only instruction
    assert "Cyrillic" in prompt  # explicit no-Cyrillic phrase
    assert "infographic" in prompt.lower()  # themed as a digest infographic


def test_build_image_prompt_encodes_style_01_minimalist():
    """RETUNE: operator picked style 01 "clean minimalist" — near-white card, ONE
    electric-blue accent, a thin circuit-line motif, and thumbnail legibility."""
    prompt = build_image_prompt(["Новая модель GPT"], date_label="24.07.2026")
    low = prompt.lower()
    assert "minimalist" in low  # clean minimalist aesthetic
    assert "near-white" in low or "off-white" in low  # near-white/off-white background
    assert "electric-blue" in low  # a single electric-blue accent
    assert "one accent color" in low or "single" in low  # exactly ONE accent, not a gradient
    assert "circuit" in low  # thin circuit-line / connective-node motif
    assert "thumbnail" in low and "legibility" in low  # small-thumbnail legibility
    # invariants preserved through the retune
    assert '"AI DAILY DIGEST"' in prompt
    assert "24.07.2026" in prompt
    assert "English" in prompt and "Cyrillic" in prompt


def test_build_image_prompt_handles_empty_headlines():
    prompt = build_image_prompt([], date_label="01.01.2026")
    assert prompt.strip()
    assert "01.01.2026" in prompt
    assert "English" in prompt


# --- Step 12: compose prompt + generation -----------------------------------


def test_generate_digest_image_uses_built_prompt(tmp_path):
    captured = {}

    def fake_transport(url, headers, body):
        captured["prompt"] = body["prompt"]
        return {"data": [{"b64_json": PNG_B64}]}

    out = tmp_path / "digest.png"
    result = generate_digest_image(
        ["h1"],
        out,
        key_file=_key_file(tmp_path),
        date_label="24.07.2026",
        transport=fake_transport,
    )
    assert result == out
    assert out.read_bytes() == PNG_BYTES
    assert captured["prompt"] == build_image_prompt(["h1"], date_label="24.07.2026")
