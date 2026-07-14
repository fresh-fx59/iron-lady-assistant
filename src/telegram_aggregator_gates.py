"""src/telegram_aggregator_gates.py — deterministic gates for the LLM digest draft.

The model only PROPOSES stories; everything here DISPOSES: strict schema, link
allow-listing against actually-collected links (anti-hallucination), and the
copyright no-verbatim rule (summaries must be own words — >=12 consecutive
shared words with any source text kill the story).
"""
from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass

_T_ME = re.compile(r"^https://t\.me/[A-Za-z][A-Za-z0-9_]{3,31}/\d+$")
_OVERLAP_WORDS = 12


@dataclass(frozen=True)
class Story:
    headline: str
    summary: str
    source_links: list[str]

    def __post_init__(self) -> None:  # lists are unhashable; freeze content shape only
        object.__setattr__(self, "source_links", list(self.source_links))

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, Story)
            and self.headline == other.headline
            and self.summary == other.summary
            and self.source_links == other.source_links
        )


# @dataclass(frozen=True) would otherwise auto-generate a __hash__ that tries to
# hash the list field (TypeError at call time). Assigning here — *after* the
# class body — is required: setting __hash__ = None inside the class body gets
# silently clobbered by the dataclass decorator, which treats a user-defined
# __eq__ + __hash__ = None as "no explicit hash" and regenerates one anyway.
Story.__hash__ = None


@dataclass(frozen=True)
class GateResult:
    ok: bool
    stories: list[Story]
    errors: list[str]


def parse_draft(raw: str) -> list[Story]:
    text = raw.strip()
    fence = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.DOTALL)
    if fence:
        text = fence.group(1)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"draft is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("draft JSON is not an object")
    stories_raw = payload.get("stories")
    if not isinstance(stories_raw, list):
        raise ValueError("draft JSON has no 'stories' list")
    stories: list[Story] = []
    for i, item in enumerate(stories_raw):
        if not isinstance(item, dict):
            raise ValueError(f"story {i} is not an object")
        links = item.get("source_links")
        # A bare string is iterable too (would silently split into one-char
        # "links" instead of raising) — reject anything that isn't a real list.
        if not isinstance(links, list):
            raise ValueError(f"story {i} source_links is not a list")
        try:
            stories.append(
                Story(
                    headline=str(item["headline"]).strip(),
                    summary=str(item["summary"]).strip(),
                    source_links=[str(x).strip() for x in links],
                )
            )
        except KeyError as exc:
            raise ValueError(f"story {i} missing key {exc}") from exc
        except TypeError as exc:
            raise ValueError(f"story {i} has invalid field type: {exc}") from exc
    return stories


def _norm_words(text: str) -> list[str]:
    norm = unicodedata.normalize("NFKC", text).lower()
    # Strip every Unicode "format" char (category Cf) BEFORE tokenizing — this
    # covers zero-width space U+200B, zero-width non-joiner/joiner U+200C/
    # U+200D, and soft hyphen U+00AD. \w doesn't match any of them, so left in
    # place they silently split a copied word in two ("но​вая" ->
    # "но","вая"), which shifts every downstream 12-word window and lets a
    # verbatim-copied passage sail past the no-verbatim gate untouched.
    norm = "".join(ch for ch in norm if unicodedata.category(ch) != "Cf")
    return re.findall(r"\w+", norm)


def _has_verbatim_overlap(story_text: str, source_texts: list[str]) -> bool:
    words = _norm_words(story_text)
    if len(words) < _OVERLAP_WORDS:
        # Fail-safe on purpose: short texts use the whole string as one needle,
        # which is stricter than the documented >=12-word rule (any overlap at
        # all kills a <12-word story, not just a 12-word run).
        needles = {" ".join(words)} if words else set()
    else:
        needles = {
            " ".join(words[i : i + _OVERLAP_WORDS])
            for i in range(len(words) - _OVERLAP_WORDS + 1)
        }
    for source in source_texts:
        haystack = " ".join(_norm_words(source))
        for needle in needles:
            if needle and needle in haystack:
                return True
    return False


def run_gates(
    stories: list[Story],
    *,
    known_links: set[str],
    source_texts: list[str],
    min_stories: int = 3,
    max_stories: int = 12,
) -> GateResult:
    surviving: list[Story] = []
    errors: list[str] = []
    for story in stories:
        label = story.headline[:40] or "<empty>"
        if not story.headline or len(story.headline) > 120:
            errors.append(f"drop '{label}': headline empty/too long")
            continue
        if not story.summary or len(story.summary) > 400:
            errors.append(f"drop '{label}': summary empty/too long")
            continue
        if not story.source_links or len(story.source_links) > 8:
            errors.append(f"drop '{label}': needs 1-8 source links")
            continue
        if any(not _T_ME.match(link) for link in story.source_links):
            errors.append(f"drop '{label}': non-t.me or malformed link")
            continue
        if any(link not in known_links for link in story.source_links):
            errors.append(f"drop '{label}': unknown link (not in collected window)")
            continue
        if _has_verbatim_overlap(f"{story.headline} {story.summary}", source_texts):
            errors.append(f"drop '{label}': verbatim overlap with a source")
            continue
        surviving.append(story)

    if len(surviving) < min_stories:
        errors.append(f"only {len(surviving)} stories survived (< {min_stories})")
        return GateResult(ok=False, stories=surviving, errors=errors)
    return GateResult(ok=True, stories=surviving[:max_stories], errors=errors)
