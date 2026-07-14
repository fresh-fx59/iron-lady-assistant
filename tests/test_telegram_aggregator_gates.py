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


def test_gate_fails_when_too_few_survive():
    result = run_gates([Story(**_story())], known_links=KNOWN, source_texts=[])
    assert not result.ok
