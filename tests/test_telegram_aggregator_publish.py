"""tests/test_telegram_aggregator_publish.py"""
from __future__ import annotations

from src.telegram_aggregator_gates import Story
from src.telegram_aggregator_publish import render_messages

FOOTER = "🤖 Дайджест: отбор автоматический, курирование вручную."


def _story(i, summary_len=100):
    return Story(
        headline=f"Сюжет {i} <важный>",
        summary=("х" * summary_len) + " & конец.",
        source_links=[f"https://t.me/chan/{i}"],
    )


def test_render_single_message_structure():
    msgs = render_messages([_story(1), _story(2)], date_label="14.07", footer=FOOTER)
    assert len(msgs) == 1
    text = msgs[0]
    assert text.startswith("📰 <b>AI-дайджест — 14.07</b>")
    assert "<b>Сюжет 1 &lt;важный&gt;</b>" in text          # escaped headline
    assert '<a href="https://t.me/chan/1">' in text
    assert text.rstrip().endswith(FOOTER)
    assert "&amp; конец." in text                            # escaped summary


def test_render_splits_at_story_boundary():
    stories = [_story(i, summary_len=390) for i in range(12)]
    msgs = render_messages(stories, date_label="14.07", footer=FOOTER)
    assert len(msgs) >= 2
    assert all(len(m) <= 4000 for m in msgs)
    assert msgs[-1].rstrip().endswith(FOOTER)
    assert FOOTER not in "".join(msgs[:-1])
    joined = "".join(msgs)
    for i in range(12):
        assert f"Сюжет {i}" in joined                        # nothing lost in the split
