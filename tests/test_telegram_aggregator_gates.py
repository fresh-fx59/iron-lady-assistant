"""tests/test_telegram_aggregator_gates.py"""
from __future__ import annotations

import json

import pytest

from src.telegram_aggregator_gates import Story, parse_draft, run_gates

LINK_A = "https://t.me/chan_a/10"
LINK_B = "https://t.me/chan_b/20"
KNOWN = {LINK_A, LINK_B}
SOURCE_TEXTS = [
    "сегодня вышла новая модель которая обгоняет всех конкурентов на всех бенчмарках сразу и это очень важно для рынка",
]


def _story(**kw):
    base = dict(
        headline="Новая модель обошла конкурентов",
        summary="Коротко своими словами о релизе и почему это важно рынку.",
        source_links=[LINK_A],
    )
    base.update(kw)
    return base


def _raw(stories):
    return json.dumps({"stories": stories}, ensure_ascii=False)


def test_parse_draft_strict_json_and_fence():
    stories = parse_draft(_raw([_story()]))
    assert stories == [Story(**_story())]
    fenced = "```json\n" + _raw([_story()]) + "\n```"
    assert parse_draft(fenced) == stories
    with pytest.raises(ValueError):
        parse_draft("тут вообще не JSON")
    with pytest.raises(ValueError):
        parse_draft(json.dumps({"stories": [{"headline": "x"}]}))  # missing keys


def test_gates_pass_minimal():
    stories = [Story(**_story()), Story(**_story(source_links=[LINK_B])),
               Story(**_story(headline="Третий сюжет дня"))]
    result = run_gates(stories, known_links=KNOWN, source_texts=SOURCE_TEXTS)
    assert result.ok and len(result.stories) == 3


def test_gate_drops_unknown_or_foreign_links():
    bad_unknown = Story(**_story(source_links=["https://t.me/chan_a/999"]))
    bad_domain = Story(**_story(source_links=["https://example.com/x"]))
    good = [Story(**_story()), Story(**_story()), Story(**_story())]
    result = run_gates(good + [bad_unknown, bad_domain], known_links=KNOWN, source_texts=[])
    assert len(result.stories) == 3
    assert any("unknown link" in e for e in result.errors)
    assert any("non-t.me" in e for e in result.errors)


def test_gate_drops_verbatim_overlap():
    verbatim = Story(**_story(
        summary="сегодня вышла новая модель которая обгоняет всех конкурентов на всех бенчмарках сразу и это"
    ))
    good = [Story(**_story()), Story(**_story()), Story(**_story())]
    result = run_gates(good + [verbatim], known_links=KNOWN, source_texts=SOURCE_TEXTS)
    assert len(result.stories) == 3
    assert any("verbatim" in e for e in result.errors)


def test_gate_drops_verbatim_overlap_with_zero_width_spaces():
    # Same 15-word verbatim run as test_gate_drops_verbatim_overlap, but with
    # zero-width spaces (U+200B) spliced inside two of the words. \w doesn't
    # match ZWSP, so without stripping Cf chars first these split into extra
    # "words" and the 12-gram sliding window never lines up with the source —
    # the copy sails through undetected. Regression for that bypass.
    verbatim = Story(**_story(
        summary=(
            "сегодня вышла но​вая модель кото​рая обгоняет всех "
            "конкурентов на всех бенчмарках сразу и это"
        )
    ))
    good = [Story(**_story()), Story(**_story()), Story(**_story())]
    result = run_gates(good + [verbatim], known_links=KNOWN, source_texts=SOURCE_TEXTS)
    assert len(result.stories) == 3
    assert any("verbatim" in e for e in result.errors)


def test_gate_fails_when_too_few_survive():
    result = run_gates([Story(**_story())], known_links=KNOWN, source_texts=[])
    assert not result.ok


@pytest.mark.parametrize("raw", ["[1,2,3]", "42", "null", '"just a string"'])
def test_parse_draft_rejects_non_object_top_level(raw):
    with pytest.raises(ValueError):
        parse_draft(raw)


def test_parse_draft_rejects_null_source_links():
    raw = json.dumps(
        {"stories": [{"headline": "x", "summary": "y", "source_links": None}]}
    )
    with pytest.raises(ValueError):
        parse_draft(raw)


def test_parse_draft_rejects_bare_string_source_links():
    raw = json.dumps(
        {
            "stories": [
                {
                    "headline": "x",
                    "summary": "y",
                    "source_links": "https://t.me/a/1",
                }
            ]
        }
    )
    with pytest.raises(ValueError):
        parse_draft(raw)


def test_parse_draft_rejects_int_source_links():
    raw = json.dumps(
        {"stories": [{"headline": "x", "summary": "y", "source_links": 7}]}
    )
    with pytest.raises(ValueError):
        parse_draft(raw)


def test_gate_headline_length_boundary():
    ok_story = Story(**_story(headline="Р" * 120))
    too_long = Story(**_story(headline="Р" * 121))
    good = [Story(**_story()), Story(**_story())]
    result = run_gates(
        good + [ok_story, too_long], known_links=KNOWN, source_texts=[]
    )
    assert ok_story in result.stories
    assert too_long not in result.stories
    assert any("too long" in e for e in result.errors)


def test_gate_source_links_count_boundary():
    eight_links = [LINK_A] * 8
    nine_links = [LINK_A] * 9
    known_many = {LINK_A}
    ok_story = Story(**_story(source_links=eight_links))
    too_many = Story(**_story(source_links=nine_links))
    good = [Story(**_story(source_links=[LINK_A])), Story(**_story(source_links=[LINK_A]))]
    result = run_gates(
        good + [ok_story, too_many], known_links=known_many, source_texts=[]
    )
    assert ok_story in result.stories
    assert too_many not in result.stories
    assert any("1-8 source links" in e for e in result.errors)


def test_gate_url_backstop_drops_only_when_all_links_published():
    """The url backstop drops ONLY when EVERY source link already shipped. A
    genuinely-new story that re-shares one still-in-window url but ALSO cites a
    fresh one is kept (FIX 2: any() -> all())."""
    # links [already-published, fresh] -> KEPT (partially new)
    partial = Story(**_story(headline="Частично новый сюжет", source_links=[LINK_A, LINK_B]))
    # links all already-published -> DROPPED
    all_pub = Story(**_story(headline="Полностью старый сюжет", source_links=[LINK_A]))
    good = [Story(**_story(source_links=[LINK_B])), Story(**_story(source_links=[LINK_B]))]
    result = run_gates(
        good + [partial, all_pub],
        known_links=KNOWN,
        source_texts=[],
        blocked_links={LINK_A},
    )
    assert partial in result.stories
    assert all_pub not in result.stories
    assert any("already published" in e for e in result.errors)


def test_gate_drop_message_interpolates_window_days():
    """FIX 3: the drop message reports the configured window, not a hardcoded 7."""
    dropped = Story(**_story(headline="Старый сюжет", source_links=[LINK_A]))
    good = [Story(**_story(source_links=[LINK_B])) for _ in range(3)]
    result = run_gates(
        good + [dropped],
        known_links=KNOWN,
        source_texts=[],
        blocked_links={LINK_A},
        window_days=14,
    )
    assert any("last 14 days" in e for e in result.errors)
    # default keeps "7"
    result7 = run_gates(
        good + [dropped], known_links=KNOWN, source_texts=[], blocked_links={LINK_A}
    )
    assert any("last 7 days" in e for e in result7.errors)
