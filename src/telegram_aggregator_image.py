"""src/telegram_aggregator_image.py — gpt-image-2 English infographic (A2).

The daily public digest gets ONE hero infographic. The HTTP path is ported
verbatim from dzen-autopilot's ``imagegen`` (stdlib ``urllib.request``,
injectable ``transport`` so tests never touch the network). Two aggregator-
specific helpers sit on top:

- :func:`build_image_prompt` — a deterministic **English-only** infographic
  prompt. The rendered in-image text is a fixed English title + date because
  gpt-image-2 garbles Cyrillic (operator decision 2026-07); RU headlines steer
  composition/theme only, never appear as text.
- :func:`generate_digest_image` — compose the prompt from the day's headlines,
  then generate.

Wire contract (proven): ``POST {base_url}/images/generations`` with a
``Bearer <key>`` header and JSON body ``{model, prompt, n: 1, size}`` returns
``{"data": [{"b64_json": "<base64 PNG>"}]}`` (some deployments return
``{"data": [{"url": "..."}]}`` — both handled). Key at
``/run/secrets/cliproxyapi_api_key`` (subscription-covered).
"""

from __future__ import annotations

import base64
import json
import urllib.error
import urllib.request
from pathlib import Path
from typing import Callable

DEFAULT_BASE_URL = "http://127.0.0.1:8317/v1"
DEFAULT_MODEL = "gpt-image-2"
DEFAULT_SIZE = "1536x1024"  # landscape hero for a single-message digest
DEFAULT_TIMEOUT = 180

# A transport takes (url, headers, json_body) and returns the parsed JSON dict.
Transport = Callable[[str, dict[str, str], dict], dict]


class ImageGenError(Exception):
    """Any HTTP / response-format / decode failure on the image path."""


def _default_transport(timeout: int) -> Transport:
    """Build a urllib-backed transport that POSTs JSON and returns parsed JSON."""

    def _transport(url: str, headers: dict[str, str], json_body: dict) -> dict:
        data = json.dumps(json_body).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as exc:
            detail = ""
            try:
                detail = exc.read().decode("utf-8", "replace")[:300]
            except Exception:  # noqa: BLE001 — best-effort detail only
                pass
            raise ImageGenError(
                f"image API returned HTTP {exc.code}" + (f": {detail}" if detail else "")
            ) from exc
        except urllib.error.URLError as exc:
            raise ImageGenError(f"image API unreachable: {exc.reason}") from exc
        try:
            return json.loads(raw)
        except ValueError as exc:
            raise ImageGenError(f"image API returned non-JSON body: {exc}") from exc

    return _transport


def _download(url: str, timeout: int) -> bytes:
    """Fetch raw bytes from a URL (supports ``file://`` for offline tests)."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.read()
    except (urllib.error.URLError, OSError, ValueError) as exc:
        raise ImageGenError(f"failed to download image url {url!r}: {exc}") from exc


def _decode_image(item: dict, timeout: int) -> bytes:
    """Turn one ``data[0]`` entry into PNG bytes (b64_json preferred, else url)."""
    b64 = item.get("b64_json")
    if b64:
        try:
            return base64.b64decode(b64, validate=True)
        except (ValueError, TypeError) as exc:
            raise ImageGenError(f"could not base64-decode 'b64_json': {exc}") from exc
    url = item.get("url")
    if url:
        return _download(url, timeout)
    raise ImageGenError("image API response item has neither 'b64_json' nor 'url'")


def generate_image(
    prompt: str,
    out_path: Path,
    *,
    key_file: Path,
    base_url: str = DEFAULT_BASE_URL,
    model: str = DEFAULT_MODEL,
    size: str = DEFAULT_SIZE,
    timeout: int = DEFAULT_TIMEOUT,
    transport: Transport | None = None,
) -> Path:
    """Generate an image and write it to ``out_path``; return ``out_path``.

    Reads the API key from ``key_file`` (whitespace-stripped), POSTs
    ``{model, prompt, n: 1, size}`` to ``{base_url}/images/generations`` with a
    ``Bearer`` auth header, decodes ``data[0]`` (``b64_json``, or a downloaded
    ``url``) into PNG bytes, and writes them. ``transport`` is injectable for
    tests. Raises :class:`ImageGenError` on any HTTP, response-shape, or decode
    failure (and never writes a partial file on error).
    """
    key_file = Path(key_file)
    try:
        key = key_file.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ImageGenError(f"could not read key file {key_file}: {exc}") from exc
    if not key:
        raise ImageGenError(f"key file {key_file} is empty")

    url = f"{base_url.rstrip('/')}/images/generations"
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    body = {"model": model, "prompt": prompt, "n": 1, "size": size}

    call = transport if transport is not None else _default_transport(timeout)
    try:
        payload = call(url, headers, body)
    except ImageGenError:
        raise
    except Exception as exc:  # noqa: BLE001 — any transport failure is a gen error
        raise ImageGenError(f"image request failed: {exc}") from exc

    if not isinstance(payload, dict):
        raise ImageGenError(f"unexpected image API response type: {type(payload).__name__}")
    data = payload.get("data")
    if not isinstance(data, list) or not data:
        raise ImageGenError(f"image API response has no 'data' array: {payload!r}"[:300])

    image_bytes = _decode_image(data[0], timeout)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(image_bytes)
    return out_path


def build_image_prompt(headlines: list[str], *, date_label: str) -> str:
    """Deterministic English infographic prompt — STYLE 01 "clean minimalist".

    The operator picked style 01 from the sample gallery: a near-white editorial
    card, ONE electric-blue accent (not a multi-color gradient), a thin
    circuit-line / connective-node motif used sparingly, generous negative
    space, a flat modern look — and, above all, MAXIMUM legibility as a small
    Telegram thumbnail (large, high-contrast header text).

    The RU ``headlines`` steer composition/theme only — the *rendered* in-image
    text is a fixed English title + ``date_label``, because gpt-image-2 garbles
    Cyrillic (operator decision). The prompt therefore carries an explicit
    English-only / no-Cyrillic instruction.
    """
    topics = "; ".join(h.strip() for h in headlines if h and h.strip())
    theme = f" Themes to evoke visually (do NOT render this text): {topics}." if topics else ""
    return (
        "Create a clean, minimalist, flat editorial infographic — a hero banner "
        "for a daily AI-news digest (style 01, clean minimalist). Use a "
        "near-white, off-white card background with generous negative space and "
        "a flat, modern editorial look. Use exactly ONE accent color — a single "
        "electric-blue — for all highlights; NO multi-color gradient, no rainbow "
        "palette. Add a thin, delicate circuit-line and connective-node motif in "
        "that same electric-blue, used sparingly near the edges as a light "
        "accent, never crowding the center. "
        f'Prominent header text: "AI DAILY DIGEST" and the date "{date_label}", '
        "set in a large, bold, high-contrast typeface against the near-white "
        "background. Legibility is the TOP priority: the header must stay crisp "
        "and easy to read even as a SMALL Telegram thumbnail. "
        "All visible text MUST be in English only — absolutely no Cyrillic and no "
        "other alphabet, spell every word correctly."
        f"{theme}"
    )


def generate_digest_image(
    headlines: list[str],
    out_path: Path,
    *,
    key_file: Path,
    date_label: str,
    base_url: str = DEFAULT_BASE_URL,
    model: str = DEFAULT_MODEL,
    size: str = DEFAULT_SIZE,
    timeout: int = DEFAULT_TIMEOUT,
    transport: Transport | None = None,
) -> Path:
    """Compose the English infographic prompt from ``headlines`` and generate."""
    prompt = build_image_prompt(headlines, date_label=date_label)
    return generate_image(
        prompt,
        out_path,
        key_file=key_file,
        base_url=base_url,
        model=model,
        size=size,
        timeout=timeout,
        transport=transport,
    )
