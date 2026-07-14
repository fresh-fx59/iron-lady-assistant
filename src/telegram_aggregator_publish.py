"""src/telegram_aggregator_publish.py — deterministic rendering + Bot API publish.

Rendering, splitting, link emission, footer, and the actual send are ALL code —
the model never touches the wire format (deliver-critical-values-by-code rule).
Publisher design (injectable Transport, 2-phase ledger, dry-run) mirrors
dzen-autopilot's post.py, adapted for multi-message digests.
"""
from __future__ import annotations

import html
import json
import logging
import os
import sqlite3
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from .telegram_aggregator_gates import Story

logger = logging.getLogger(__name__)

_MESSAGE_CAP = 4000  # under the 4096 Bot API ceiling; split at story boundaries


def _render_story(index: int, story: Story) -> str:
    links = " · ".join(
        f'<a href="{html.escape(link, quote=True)}">{html.escape(_link_label(link))}</a>'
        for link in story.source_links
    )
    return (
        f"{index}. <b>{html.escape(story.headline)}</b>\n"
        f"{html.escape(story.summary)}\n"
        f"Источники: {links}"
    )


def _link_label(link: str) -> str:
    path = urllib.parse.urlparse(link).path.strip("/")
    return "@" + path.split("/")[0] if path else link


def render_messages(stories: list[Story], *, date_label: str, footer: str) -> list[str]:
    header = f"📰 <b>AI-дайджест — {html.escape(date_label)}</b>"
    blocks = [_render_story(i + 1, s) for i, s in enumerate(stories)]
    footer_block = html.escape(footer)

    messages: list[str] = []
    current = header
    for block in blocks:
        candidate = f"{current}\n\n{block}"
        if len(candidate) > _MESSAGE_CAP and current != header:
            messages.append(current)
            current = block
        else:
            current = candidate
    # attach footer to the last message, splitting once more if it would overflow
    with_footer = f"{current}\n\n{footer_block}"
    if len(with_footer) > _MESSAGE_CAP:
        messages.append(current)
        messages.append(footer_block)
    else:
        messages.append(with_footer)
    return messages
