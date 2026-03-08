"""Integration tests for markdown formatter contract.

These tests define the expected behavior of markdown-to-HTML conversion
and message splitting. These are observable behaviors that must be
preserved during language rewrite.
"""

import pytest

from src.formatter import markdown_to_html, split_message, strip_html


# ── Contract 1: Basic markdown formatting ─────────────────────────
class TestBasicMarkdownFormatting:
    """Basic markdown elements should convert to correct HTML."""

    def test_plain_text_unchanged(self):
        """Plain text with no markdown should pass through."""
        result = markdown_to_html("Hello world")
        assert result == "Hello world"

    def test_bold_double_asterisk(self):
        """**text** should become <b>text</b>."""
        result = markdown_to_html("**bold**")
        assert result == "<b>bold</b>"

    def test_bold_double_underscore(self):
        """__text__ should become <b>text</b>."""
        result = markdown_to_html("__bold__")
        assert result == "<b>bold</b>"

    def test_italic_single_asterisk(self):
        """*text* should become <i>text</i>."""
        result = markdown_to_html("*italic*")
        assert result == "<i>italic</i>"

    def test_italic_single_underscore(self):
        """_text_ should become <i>text</i>."""
        result = markdown_to_html("_italic_")
        assert result == "<i>italic</i>"

    def test_strikethrough(self):
        """~~text~~ should become <s>text</s>."""
        result = markdown_to_html("~~strikethrough~~")
        assert result == "<s>strikethrough</s>"

    def test_inline_code(self):
        """`code` should become <code>code</code>."""
        result = markdown_to_html("`code`")
        assert result == "<code>code</code>"

    def test_multiple_formatting_on_same_line(self):
        """Multiple markdown elements should work on the same line."""
        result = markdown_to_html("**bold** and *italic* and `code`")
        assert result == "<b>bold</b> and <i>italic</i> and <code>code</code>"


# ── Contract 2: HTML escaping ─────────────────────────────────────
class TestHtmlEscaping:
    """Special characters should be escaped to prevent XSS."""

    def test_escaped_less_than(self):
        """< should become &lt;."""
        result = markdown_to_html("x < y")
        assert result == "x &lt; y"

    def test_escaped_greater_than(self):
        """> should become &gt;."""
        result = markdown_to_html("x > y")
        assert result == "x &gt; y"

    def test_escaped_ampersand(self):
        """& should become &amp;."""
        result = markdown_to_html("A & B")
        assert result == "A &amp; B"

    def test_formatting_with_special_chars(self):
        """Markdown should work alongside escaped HTML."""
        result = markdown_to_html("Sum = a + b < 100 and > 0")
        assert result == "Sum = a + b &lt; 100 and &gt; 0"

    def test_inline_code_preserves_angle_brackets_escaped(self):
        """Angle brackets in code blocks should be escaped."""
        result = markdown_to_html("`x < y`")
        assert result == "<code>x &lt; y</code>"


# ── Contract 3: Fenced code blocks ───────────────────────────────────
class TestFencedCodeBlocks:
    """Fenced code blocks with ``` should convert to <pre><code>."""

    def test_simple_code_block(self):
        """```code``` should become <pre><code>code</code></pre>."""
        result = markdown_to_html("```\ncode\n```")
        assert result == "<pre><code>code</code></pre>"

    def test_code_block_with_language(self):
        """```python should include the language class."""
        result = markdown_to_html("```python\nprint('hello')\n```")
        assert "language-python" in result
        assert "<pre><code class=\"language-python\">" in result

    def test_code_block_escapes_html(self):
        """Content in code blocks should be HTML-escaped."""
        result = markdown_to_html("```\n<div>test</div>\n```")
        assert "&lt;div&gt;test&lt;/div&gt;" in result

    def test_code_block_with_special_chars(self):
        """Special characters in code blocks should be escaped."""
        result = markdown_to_html("```\nvar x = a < b && c > d;\n```")
        assert "var x = a &lt; b &amp;&amp; c &gt; d;" in result

    def test_unclosed_code_block(self):
        """Unclosed ``` should still produce valid output."""
        result = markdown_to_html("```\nunclosed code")
        assert "<pre><code>unclosed code</code></pre>" in result

    def test_code_block_content_preserved(self):
        """Code block content should be preserved exactly (except for HTML)."""
        result = markdown_to_html("```\n  indented\n  code\n```")
        assert "  indented\n  code" in result


# ── Contract 4: Multiline formatting ─────────────────────────────
class TestMultilineFormatting:
    """Formatting across multiple lines should work correctly."""

    def test_bold_multiline(self):
        """Bold across multiple lines should work."""
        result = markdown_to_html("**bold\ntext**")
        assert result == "<b>bold\ntext</b>"

    def test_multiple_paragraphs(self):
        """Multiple lines should be preserved."""
        result = markdown_to_html("Line 1\nLine 2\nLine 3")
        assert result == "Line 1\nLine 2\nLine 3"

    def test_formatting_preserves_line_breaks(self):
        """Line breaks should be preserved around formatting."""
        result = markdown_to_html("Before\n**bold**\nAfter")
        assert result == "Before\n<b>bold</b>\nAfter"


# ── Contract 5: Message splitting ─────────────────────────────────
class TestMessageSplitting:
    """Long messages should be split intelligently at boundaries."""

    def test_short_message_unchanged(self):
        """Messages under limit should not be split."""
        result = split_message("Hello")
        assert result == ["Hello"]

    def test_exactly_limit_returns_one_message(self):
        """Message exactly at limit should not be split."""
        text = "a" * 4096
        result = split_message(text)
        assert result == [text]

    def test_split_at_paragraph_boundary(self):
        """Should keep paragraphs together when the full text fits."""
        paragraph1 = "a" * 2000
        paragraph2 = "b" * 2000
        text = f"{paragraph1}\n\n{paragraph2}"

        result = split_message(text)

        assert result == [text]

    def test_split_at_line_boundary(self):
        """Should keep line-broken text together when it still fits."""
        text = ""
        for i in range(4):
            text += "a" * 1000 + "\n"

        result = split_message(text)

        assert result == [text]

    def test_split_at_paragraph_boundary_when_over_limit(self):
        """Should prefer paragraph boundaries once splitting is required."""
        paragraph1 = "a" * 2500
        paragraph2 = "b" * 2500
        text = f"{paragraph1}\n\n{paragraph2}"

        result = split_message(text)

        assert len(result) == 2
        assert result[0] == paragraph1
        assert result[1] == paragraph2

    def test_split_at_space_boundary(self):
        """Should fall back to space if no line break."""
        # Continuous text that exceeds limit
        text = "word " * 1200  # Each "word " is 5 chars

        result = split_message(text)

        for chunk in result:
            assert len(chunk) <= 4096
            assert not chunk.endswith(" ")  # Should strip leading/trailing space

    def test_hard_split_if_no_boundaries(self):
        """Should hard split if no whitespace exists."""
        text = "a" * 10000

        result = split_message(text)

        assert len(result) == 3
        assert len(result[0]) == 4096
        assert len(result[1]) == 4096
        assert len(result[2]) == 10000 - 8192

    def test_split_strips_leading_newline(self):
        """Split chunks should not start with separator fragments."""
        text = ("a" * 4000) + "\n\n" + ("b" * 4000)

        result = split_message(text)

        assert not result[1].startswith("\n")


# ── Contract 6: HTML stripping for fallback ─────────────────────────
class TestHtmlStripping:
    """HTML stripping for plain-text fallback mode."""

    def test_strip_simple_tags(self):
        """Simple tags should be removed."""
        result = strip_html("<b>bold</b>")
        assert result == "bold"

    def test_strip_multiple_tags(self):
        """Multiple tags should be removed."""
        result = strip_html("<b>bold</b> and <i>italic</i>")
        assert result == "bold and italic"

    def test_strip_nested_tags(self):
        """Nested tags should be removed."""
        result = strip_html("<div><b>nested</b></div>")
        assert result == "nested"

    def test_strip_code_blocks(self):
        """Code blocks should be stripped."""
        result = strip_html("<pre><code>code</code></pre>")
        assert result == "code"

    def test_strip_preserves_text_between_tags(self):
        """Text between tags should be preserved."""
        result = strip_html("Before <b>bold</b> after")
        assert result == "Before bold after"


# ── Contract 7: Edge cases ────────────────────────────────────────
class TestEdgeCases:
    """Edge cases and special scenarios."""

    def test_empty_string(self):
        """Empty string should be handled."""
        assert markdown_to_html("") == ""
        assert split_message("") == [""]

    def test_only_newlines(self):
        """Only newlines should be preserved."""
        result = markdown_to_html("\n\n\n")
        assert result == "\n\n\n"

    def test_nested_asterisks_should_format_correctly(self):
        """Nested * should format inner ones correctly."""
        result = markdown_to_html("**bold *and italic* still bold**")
        assert "<b>bold <i>and italic</i> still bold</b>" == result

    def test_underscore_in_word_not_italic(self):
        """Underscore within words should not trigger italic."""
        result = markdown_to_html("variable_name")
        assert result == "variable_name"

    def test_asterisk_in_word_not_italic(self):
        """Asterisk within words should not trigger italic."""
        result = markdown_to_html("2*3=6")
        assert result == "2*3=6"

    def test_adjacent_formatting(self):
        """Adjacent formatting should work."""
        result = markdown_to_html("**bold***italic*")
        assert "<b>bold</b><i>italic</i>" == result
