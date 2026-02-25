import re

MAX_MESSAGE_LENGTH = 4096


def _escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def markdown_to_html(text: str) -> str:
    """Convert Claude's markdown output to Telegram-compatible HTML."""
    lines = text.split("\n")
    result: list[str] = []
    in_code_block = False
    code_lang = ""
    code_lines: list[str] = []

    for line in lines:
        # Fenced code block toggle
        if re.match(r"^```", line):
            if not in_code_block:
                in_code_block = True
                code_lang = line[3:].strip()
                code_lines = []
            else:
                code_content = _escape_html("\n".join(code_lines))
                if code_lang:
                    result.append(
                        f'<pre><code class="language-{_escape_html(code_lang)}">'
                        f"{code_content}</code></pre>"
                    )
                else:
                    result.append(f"<pre><code>{code_content}</code></pre>")
                in_code_block = False
                code_lang = ""
            continue

        if in_code_block:
            code_lines.append(line)
            continue

        # Normal line: escape HTML first, then apply formatting
        line = _escape_html(line)

        # Bold: **text** or __text__
        line = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", line)
        line = re.sub(r"__(.+?)__", r"<b>\1</b>", line)

        # Italic: *text* or _text_ (but not inside words with underscores)
        line = re.sub(r"(?<!\w)\*([^\*]+?)\*(?!\w)", r"<i>\1</i>", line)
        line = re.sub(r"(?<!\w)_([^_]+?)_(?!\w)", r"<i>\1</i>", line)

        # Strikethrough: ~~text~~
        line = re.sub(r"~~(.+?)~~", r"<s>\1</s>", line)

        # Inline code: `text`
        line = re.sub(r"`([^`]+?)`", lambda m: f"<code>{m.group(1)}</code>", line)

        result.append(line)

    # Handle unclosed code block
    if in_code_block:
        code_content = _escape_html("\n".join(code_lines))
        result.append(f"<pre><code>{code_content}</code></pre>")

    return "\n".join(result)


def split_message(text: str) -> list[str]:
    """Split text into chunks that fit Telegram's message limit."""
    if not text or not text.strip():
        return []

    if len(text) <= MAX_MESSAGE_LENGTH:
        return [text]

    chunks: list[str] = []
    while text:
        if len(text) <= MAX_MESSAGE_LENGTH:
            chunks.append(text)
            break

        # Try to split at paragraph boundary
        split_at = text.rfind("\n\n", 0, MAX_MESSAGE_LENGTH)
        if split_at == -1:
            # Try line boundary
            split_at = text.rfind("\n", 0, MAX_MESSAGE_LENGTH)
        if split_at == -1:
            # Try space boundary
            split_at = text.rfind(" ", 0, MAX_MESSAGE_LENGTH)
        if split_at == -1:
            # Hard split
            split_at = MAX_MESSAGE_LENGTH

        chunk = text[:split_at]
        if chunk.strip():
            chunks.append(chunk)
        text = text[split_at:].lstrip("\n")

    return chunks


def strip_html(text: str) -> str:
    """Remove HTML tags for plain-text fallback."""
    return re.sub(r"<[^>]+>", "", text)
