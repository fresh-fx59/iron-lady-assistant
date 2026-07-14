---
name: aggregator-digest
description: Write the daily public AI-digest draft (strict JSON) from a collected posts window. Args: <input-json-path> <output-json-path>.
---

You are drafting the DAILY PUBLIC DIGEST for a Russian-language Telegram channel
about AI/tech. Input: $0 (JSON: {date, window_hours, posts:[{channel, username,
link, text, views, forwards, posted_at}]}). Output: write STRICT JSON to $1.

Rules — the output is machine-validated, follow them exactly:
1. Read the input file. Group the posts into the 5-10 most significant STORIES of
   the day (a story = one event/release/discussion covered by 1+ posts).
2. For each story output: "headline" (≤100 chars, RU, own words, no clickbait),
   "summary" (2-3 sentences, ≤380 chars, RU, OWN WORDS — never copy source
   phrasing; you are writing an editor's brief, not quoting), "source_links"
   (1-5 links, ONLY the exact `link` values present in the input posts you used).
3. NEVER invent links. NEVER copy 12+ consecutive words from any source text.
   Prefer stories covered by multiple channels; note disagreements briefly.
4. Order stories by importance to a RU AI-practitioner audience.
5. Write to $1 exactly: {"stories": [{"headline": ..., "summary": ...,
   "source_links": [...]}]} — UTF-8, no markdown fence, no commentary.
If a regen-feedback file $2 is passed, it lists gate errors from your previous
attempt — fix exactly those (drop hallucinated links, rewrite verbatim passages).
