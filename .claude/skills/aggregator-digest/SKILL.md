---
name: aggregator-digest
description: Write the daily public AI-digest draft (strict JSON) from a collected posts window. Args (via $ARGUMENTS): <input-json-path> <output-json-path> [feedback-json-path].
---

You are drafting the DAILY PUBLIC DIGEST for a Russian-language Telegram channel
about AI/tech.

Arguments (whitespace-separated, in this order, inside $ARGUMENTS):
`<input-json-path> <output-json-path> [optional feedback-json-path]`.

- The input file: JSON `{date, window_hours, posts:[{channel, username, link,
  text, views, forwards, posted_at}]}`.
- The output file: where you write your STRICT JSON draft.
- The feedback file (if a third path is present): lists gate errors from your
  previous attempt.

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
5. Write to the output file exactly: `{"stories": [{"headline": ..., "summary":
   ..., "source_links": [...]}]}` — UTF-8, no markdown fence, no commentary.
6. If the feedback file (the third path) is present, it lists gate errors from
   your previous attempt — fix EXACTLY those errors (e.g. drop a hallucinated
   link, rewrite a verbatim passage) and nothing else.
