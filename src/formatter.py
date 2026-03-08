import re

MAX_MESSAGE_LENGTH = 4096


def _escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _format_text_segment(text: str) -> str:
    """Format markdown in plain-text segments (outside fenced code blocks)."""
    if not text:
        return text

    escaped = _escape_html(text)

    # Protect inline code before other markdown substitutions.
    code_tokens: list[str] = []

    def _inline_code(match: re.Match[str]) -> str:
        code_tokens.append(f"<code>{match.group(1)}</code>")
        return f"\u0000CODE{len(code_tokens) - 1}\u0000"

    escaped = re.sub(r"`([^`\n]+?)`", _inline_code, escaped)

    # Headings: #, ##, ### (convert to bold with emoji prefix)
    def _heading_replace(match: re.Match[str]) -> str:
        level = len(match.group(1))
        heading_text = match.group(2).strip()
        if level == 1:
            return f"\n<b>📍 {heading_text}</b>\n"
        if level == 2:
            return f"\n<b>▫️ {heading_text}</b>"
        return f"\n<b>• {heading_text}</b>"

    escaped = re.sub(r"(?m)^(#{1,3})\s+(.+)$", _heading_replace, escaped)

    # Emphasis and strikethrough (including multiline content).
    escaped = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", escaped, flags=re.DOTALL)
    escaped = re.sub(r"__(.+?)__", r"<b>\1</b>", escaped, flags=re.DOTALL)
    escaped = re.sub(r"~~(.+?)~~", r"<s>\1</s>", escaped, flags=re.DOTALL)
    escaped = re.sub(r"(?<!\w)\*([^\*]+?)\*(?!\w)", r"<i>\1</i>", escaped, flags=re.DOTALL)
    escaped = re.sub(r"(?<!\w)_([^_]+?)_(?!\w)", r"<i>\1</i>", escaped, flags=re.DOTALL)

    # Restore inline code tokens.
    for i, token in enumerate(code_tokens):
        escaped = escaped.replace(f"\u0000CODE{i}\u0000", token)

    return escaped


def markdown_to_html(text: str) -> str:
    """Convert Claude's markdown output to Telegram-compatible HTML."""
    lines = text.split("\n")
    parts: list[str] = []
    buffer: list[str] = []
    in_code_block = False
    code_lang = ""
    code_lines: list[str] = []

    for line in lines:
        if re.match(r"^```", line):
            if not in_code_block:
                if buffer:
                    parts.append(_format_text_segment("\n".join(buffer)))
                    buffer = []
                in_code_block = True
                code_lang = line[3:].strip()
                code_lines = []
            else:
                code_content = _escape_html("\n".join(code_lines))
                if code_lang:
                    parts.append(
                        f'<pre><code class="language-{_escape_html(code_lang)}">'
                        f"{code_content}</code></pre>"
                    )
                else:
                    parts.append(f"<pre><code>{code_content}</code></pre>")
                in_code_block = False
                code_lang = ""
                code_lines = []
            continue

        if in_code_block:
            code_lines.append(line)
        else:
            buffer.append(line)

    if in_code_block:
        code_content = _escape_html("\n".join(code_lines))
        parts.append(f"<pre><code>{code_content}</code></pre>")
    elif buffer:
        parts.append(_format_text_segment("\n".join(buffer)))

    return "\n".join(parts)


def split_message(text: str) -> list[str]:
    """Split text into chunks that fit Telegram's message limit."""
    if text == "":
        return [""]
    if len(text) <= MAX_MESSAGE_LENGTH:
        return [text]

    def _pack_units(units: list[str], separator: str) -> list[str]:
        chunks: list[str] = []
        current = ""
        for unit in units:
            if not unit:
                continue
            candidate = unit if not current else f"{current}{separator}{unit}"
            if len(candidate) <= MAX_MESSAGE_LENGTH:
                current = candidate
                continue
            if current:
                chunks.append(current)
                current = ""
            if len(unit) <= MAX_MESSAGE_LENGTH:
                current = unit
                continue
            oversized_parts = _split_oversized(unit)
            if oversized_parts:
                chunks.extend(oversized_parts[:-1])
                current = oversized_parts[-1]
        if current:
            chunks.append(current)
        return chunks

    def _split_oversized(chunk: str) -> list[str]:
        if len(chunk) <= MAX_MESSAGE_LENGTH:
            return [chunk]

        if "\n\n" in chunk:
            return _pack_units(chunk.split("\n\n"), "\n\n")
        if "\n" in chunk:
            return _pack_units(chunk.split("\n"), "\n")
        if " " in chunk:
            return _pack_units(chunk.split(" "), " ")

        return [
            chunk[index:index + MAX_MESSAGE_LENGTH]
            for index in range(0, len(chunk), MAX_MESSAGE_LENGTH)
        ]

    chunks = _split_oversized(text)
    return [chunk for chunk in chunks if chunk] or [""]


def strip_html(text: str) -> str:
    """Remove HTML tags for plain-text fallback."""
    return re.sub(r"<[^>]+>", "", text)
