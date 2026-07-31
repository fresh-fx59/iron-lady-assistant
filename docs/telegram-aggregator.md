# Telegram Aggregator — Public Daily Digest

A self-contained pipeline that builds one **public** AI/tech digest per day for a
Telegram channel: it collects recent posts from a list of source channels, has an
LLM draft a short digest, runs the draft through **deterministic gates**, and
publishes the result to a channel through a Bot API bot.

The design rule throughout is *"the model proposes, the code disposes"*: the LLM
only picks and phrases stories; every fact that reaches the wire — which links are
allowed, whether text is too close to a source, how messages are split, and the
publish itself — is decided by plain code, not the model.

This is the **generic** guide. All channel names, IDs, hosts, tokens, and paths
below are placeholders — fill in your own.

---

## What it does

```
sources.txt ──► collect ──► render-input ──► draft (LLM) ──► gate ──► approve ──► publish
              (read proxy)    (input.json)   (/skill)      (code)   (ledger)   (Bot API)
```

1. **Collect** — resolve each source `@username` to a channel the reader account
   has already joined, then pull new messages incrementally into a local SQLite
   store (its own DB file, role `aggregator`).
2. **Render input** — select candidate posts (deduped by normalized text, ordered
   by views, must have a real `t.me` link and enough text) into a `*-input.json`.
3. **Draft** — a token-less `claude -p` call runs the `/aggregator-digest` skill,
   which writes a **strict JSON** draft: the 3–6 most important stories of the day,
   each with a headline, a summary in the reader's own words, and source links.
4. **Gate** — deterministic validation of the draft (schema, link-exists,
   no-verbatim), then a **one-message trim**, then the digest is written to the
   publish ledger.
5. **Approve** — the gated digest is marked `approved` (either automatically in the
   hands-off daily flow, or by an operator command).
6. **Publish** — a two-phase, crash-safe publish sends the approved digest to the
   channel through a Bot API bot, as **one post = image + full text**.

---

## One post = image + full text

A digest day with a hero image ships as a **single** `sendRichMessage` post
(**Bot API 10.2**, 2026-07-14): the PNG is uploaded in the same multipart request
and placed as a media block inside a rich-HTML document that carries the whole
digest — the rich-message text ceiling is **32768** chars, not the 1024 a photo
caption allows.

Before this, an image day posted **twice**: `sendPhoto` with a short caption, then
the digest as separate `sendMessage`s — because a real digest measures 1292–3097
chars and never fit the 1024-char caption. `render_rich_html` rebuilds the
one-document form from `render_messages`' output (same wording, links and
escaping — the text is the *approved* text, verbatim), wrapping each block in
`<p>` and the footer in `<footer>`.

Degrade rungs, tried **only** when the previous one proved nothing reached
Telegram (see the ledger section): `rich` → `photo + text` (the old plan, also
used when a digest would exceed the 32768 cap) → `text-only`. No image at all ⇒
text-only, unchanged.

---

## The deterministic gates

The LLM's draft is untrusted input. `run_gates` (in
`src/telegram_aggregator_gates.py`) drops any story that fails these checks and
requires a minimum number of survivors:

- **Schema** — headline 1–120 chars, summary 1–400 chars, 1–8 source links.
- **Link shape** — every link must match `https://t.me/<username>/<id>` exactly.
- **Link-exists (anti-hallucination)** — every link must be one that was *actually
  collected in the current window*. A link the model invented is not in the
  allow-list, so the story is dropped.
- **No-verbatim (copyright)** — the summary must be in the reader's own words. If
  any run of **12+ consecutive words** in the headline+summary also appears in a
  source post's text, the story is dropped. Comparison is Unicode-normalized
  (NFKC) with zero-width/format characters stripped first, so invisible characters
  can't be used to slip a copied passage past the check.

After gating, `telegram_aggregator_tool.py` applies the **one-message rule**: the
whole digest must fit a single Telegram message (rendered under a 4000-char cap,
below the Bot API 4096 ceiling). Stories arrive importance-ordered, so the tool
trims from the *tail* until the digest fits one message. The number trimmed is
reported as `trimmed_to_fit`, so a too-long draft loses its least-important stories
loudly, never silently.

---

## Configuration

### Sources list

A plain text file, one source per line:

```
# comments and blank lines are ignored
@some_ai_channel
t.me/another_channel
bare_username
```

`t.me/+invite` links are skipped (those are not resolvable usernames). The reader
account must have **joined** each source channel before collection can resolve it.

Channels with **several handles** (Telegram's multi-username feature) keep the
legacy `Channel.username` empty and list every handle in `Channel.usernames`. The
proxy reads the legacy attribute first and falls back to the first *active*
`usernames` entry, so such a channel resolves from its configured handle and its
posts get real `t.me` links. Without that fallback a fully public, already-joined
channel silently resolves to nothing on every run *and* its messages come back with
`link: None` (which the draft input drops) — invisible in both directions.

### Discussion-chat sources (optional, default OFF)

A **second, separate** allowlist file — `chat_sources.txt` beside `sources.txt`
(override with `AGGREGATOR_CHAT_SOURCES_PATH`) — lets named *discussion chats* feed
the digest. It is deliberately a different file: the broadcast list stays clean, and
a chat is only ever ingested because a human wrote it down. There is no "all joined
chats" mode.

The file is the flag: **no file, or a file that parses to nothing ⇒ the chat lane
never runs**, so deploying this code cannot change the digest by itself.

Accepted line forms, and what each maps to:

| line | maps to |
|------|---------|
| `@hyper_llm`, `t.me/hyper_llm`, `hyper_llm` | a **public chat handle**, resolved against the `linked_chat_username` of a joined channel |
| `-1001788662720` | the `-100`-marked id a Telegram client shows |
| `1788662720`, `id:1788662720` | the bare internal (positive) entity id |
| `t.me/c/1788662720/12`, `t.me/c/1788662720/8/12` | the internal deep link of a handle-less chat (the 3-segment form is what a **forum** group copies) |

Anything else on a line is **reported, never dropped**: this file is the ON switch
for the whole lane, so a fat-fingered handle (`@hyper-llm`) or an invite link
(`t.me/+hash`, not a resolvable identity) comes back in `unresolved` as
`{"entry": …, "form": "invalid", "reason": "malformed-line"}`. Two more reasons live
there: `no-linked-chat-with-this-handle` (no joined channel exposes it — the paced
join loop may not have reached it) and `broadcast-channel-id` (the id names a
broadcast channel we already track; reading it as a chat would double-ingest its
posts under a second `peer_key` with `views = NULL`). **Every allowlist line ends up
either in `chats[]` — tagged `resolved_via: username | id-linked | id-unlinked` — or
in `unresolved[]` with a reason.**

The id forms exist because a linked discussion group usually has **no public
username** (33 of 55 linked chats in the live dialogs). A genuinely handle-less chat
is **corpus-only**: the proxy cannot build a `t.me/<handle>/<id>` link for it, the
draft input drops link-less rows, and the draft gates only accept that link shape —
so its messages are ingested and visible to later analysis but can never be cited
in a published digest. `citable` in the report answers exactly that, and it is
derived from **the link the proxy actually built from the chat entity** — `true` /
`false` once messages were read, `null` when nothing was read this run. It is
deliberately *not* derived from the parent channel's `linked_chat_username`: that is
a second, disagreeing derivation which is `None` for every id-form entry, so it
reported `citable: false` for public chats whose rows carried a publishable link.
**Prefer chats that have a handle.**

**What gets in — an input gate, in this order.** All of it is plain code; the
ingest path never calls an LLM.

1. **The allowlist** (above).
2. **A deterministic quality gate** (`chat_admission_verdict`), rejecting:
   `service` (join/leave/pin), `via-bot` (inline-bot output), `nested-reply` (a
   reply to another *comment* — in a linked group every top-level comment carries
   `reply_to` pointing at the auto-forwarded parent post, and only a reply-to-a-reply
   sets a *different* `reply_to_top_id`, so dropping all replies would make a linked
   chat contribute nothing), `too-short` (under `CHAT_MIN_CHARS`, 200 — 2.5× the
   channel floor: a message must be a paragraph for the drafter to summarise and for
   the no-verbatim gate to have real source text), `no-carrier` (news arrives in a
   chat as a **link** or as a **forward**; a long opinion with neither is the chatter
   this lane must not import).
3. **Dedup against the corpus — EXACT IDENTITY ONLY.** An ingest reject is
   irreversible (the watermark has already moved past the message, so nothing ever
   offers it again), which rules out any fuzzy key here. The three ways:
   `forward-of-tracked-channel` (the parent-post echo, and any forward from a
   channel already in the corpus — we hold the original, with its view count),
   `duplicate-text` (`_identity_key`: NFKC + lower + collapsed whitespace over the
   **whole** text) and `echo-of-corpus-post` (the quoted `t.me/<channel>/<id>`, the
   dominant echo shape in live data — a manual repost that wraps the source link).
   *Not* `_dedup_key`'s 120-char prefix: any chat with a boilerplate header longer
   than that (a promo line, an "источник:" preamble) collapses onto ONE key, so the
   first message would be admitted and every genuinely new story after it rejected,
   forever. Fuzzy similarity therefore stays in `build_draft_input`, where a
   collision only costs a slot in today's draft and the row remains re-selectable.
   Within a single run the same identity is applied *before* the cap, so two
   messages of one story from one chat merge into their best-ranked member
   (`duplicate-in-run`) instead of burning two cap slots and publishing twice.
4. **A hard per-chat cap per collect run** (`CHAT_MAX_PER_RUN`, 2), applied *after*
   the gate, keeping the batch's best candidates (carries a link → longer → newer).
   This is the load-bearing bound. Measured: the news corpus takes ~70 messages/day
   across 53 channels (median channel ~1.3/day) while chat candidates run 28–205
   messages/day each and the busiest joined chats reach 536/day. The gate admits ~2%
   of a talk chat but **74%** of a feed-mirror chat that mostly forwards — so the
   gate alone does not bound a chat. `collect` runs 5×/day, so cap 2 ⇒ **≤10
   messages/day from any one chat**: ~14% of the channel baseline, ~7× a median
   channel, a 95% cut of the loudest chat's raw rate.

Chat rows carry `views = NULL`, so `build_draft_input`'s views-DESC ordering ranks
them **last**. That is ordering, not a bound: `--max-posts` (150) is well above the
~70 posts/day channel corpus, so there is no slot pressure and **every stored chat
row does enter the draft input**, ranked after the channels. The bound is the one at
ingest: `CHAT_MAX_PER_RUN` (2) × 5 collect runs/day ⇒ **≤10 rows/day per chat**. Each
post in the draft input is labelled `origin: "channel" | "chat"` — derived from
`digest_sources.kind`, so marking origin needed no new column and no migration.

Reads are **recency-first** from a watermark that advances past every message
*seen*, not just the ones admitted — a cap must never turn into a stalled cursor
that re-reads the same window forever. A burst larger than `--chat-read-limit`
(150) therefore skips its oldest excess on purpose: the digest only looks at the
last 24h. "Seen" means **every message the proxy iterated**, which is why the read
goes through `read_messages_page` (`{messages, seen, max_seen_id}`) instead of the
list form: the proxy drops text-less messages (stickers, uncaptioned media,
join/pin service events), so a cursor built from what came *back* stalls forever the
moment `--chat-read-limit` of them sit above the watermark — no error, no alert, just
`seen: 0` on every run. `max_seen_id` is counted before that filter. The field is
additive on the HTTP envelope, so existing callers (the lead pipeline) are unaffected.

**The proxy has a SECOND allowlist.** `TELEGRAM_PROXY_ALLOWED_CHAT_IDS` (config) is
enforced in `_authorize_entity`: when it is non-empty, reading any chat outside it
answers **403**, no matter what `chat_sources.txt` says. Both lists must contain a
chat for it to be ingested. A read failure — 403, FloodWait, not-in-dialogs — is
isolated per chat and now always produces a **named report entry** with
`error: "<Type>: <message>"` alongside `failed_sources`, instead of a bare count with
an empty `chats[]`. The `digest_sources` row is written only *after* a successful
read, so a permanently-403 chat no longer gets a row rewritten every run and the
table keeps meaning "chats we actually read".

**Preview before enabling** — the dry-run reads and gates exactly like a real run
but writes nothing (no source row, no message, no watermark) and prints the admitted
text per chat:

```bash
python -m src.telegram_aggregator_tool chats-dry-run \
  [--chat-read-limit 150] [--chat-max-per-run 2]
```

### Environment variables

| Variable | Purpose |
|----------|---------|
| `AGGREGATOR_STATE_DIR` | Directory holding the aggregator DB, drafts, ledger, and logs. Point it at a writable path you control. |
| `AGGREGATOR_SOURCES_PATH` | Path to the sources file (defaults to `<state_dir>/sources.txt`). |
| `AGGREGATOR_CHAT_SOURCES_PATH` | Path to the optional discussion-chat allowlist (defaults to `<state_dir>/chat_sources.txt`). Absent/empty ⇒ the chat lane is OFF. |
| `TELEGRAM_PROXY_BASE_URL` | Base URL of the read proxy (e.g. `http://127.0.0.1:8787`). |
| `TELEGRAM_PROXY_API_KEY` | Bearer token for the read proxy. |
| `TELEGRAM_AGGREGATOR_BOT_TOKEN` | Bot API token of the **poster** bot (must be a channel admin). |
| `TELEGRAM_AGGREGATOR_CHANNEL` | Target channel to publish to — `@your_channel` or a numeric `-100…` id. |
| `AGGREGATOR_ALERT_BOT_TOKEN` | Bot API token of a separate **alert** bot for failure pings. |
| `AGGREGATOR_OPERATOR_CHAT_ID` | Chat id the alert bot sends failure notices to. |

**`*_FILE` indirection for secrets.** For `TELEGRAM_PROXY_API_KEY`,
`TELEGRAM_AGGREGATOR_BOT_TOKEN`, and `AGGREGATOR_ALERT_BOT_TOKEN`, you may instead
set a `<NAME>_FILE=/path/to/secret` variable; the pipeline reads the file's
contents into the variable at startup (an already-set value always wins, and a
missing file is a silent no-op). Prefer this over putting tokens directly in the
environment.

### The two bots and the reader account

- **Reader account** — a normal Telegram *user* account, exposed through the
  read-only proxy (see the "Telegram Channel Daily Digest" section of the main
  `README.md` for how to set up the proxy and its encrypted session). It only ever
  *reads*; it must already be a member of every source channel.
- **Poster bot** — a Bot API bot added as an **admin** of the target channel (with
  post permission). Its token is `TELEGRAM_AGGREGATOR_BOT_TOKEN`. This is what
  actually publishes the digest.
- **Alert bot** — a separate Bot API bot used only to DM the operator when the
  pipeline fails. Success is deliberately silent — the published post is the
  success signal — so this bot only ever fires on problems.

---

## The two-phase publish ledger

Publishing is crash-safe. Each day's digest is a row in a SQLite ledger
(`DigestLedger`) that moves through explicit states:

```
pending ──approve──► approved ──begin_send──► sending ──► posted
                                                  │
                                                  └──► (left 'sending' on crash) ──► BLOCKED
```

- `upsert_draft` writes the rendered messages as **`pending`**.
- `approve` moves the newest pending digest to **`approved`**.
- `publish_next` takes the oldest approved digest, flips it to **`sending`**, sends
  the planned ops (one rich post on an image day; otherwise each message paced ~1s
  apart, honoring one `retry_after` on a 429), records the running `sent_count`,
  and finally marks it **`posted`**.
- **Blocks on crash.** If a send dies mid-way, the row is deliberately left in
  `sending` (not auto-reverted, not auto-failed). On the next run, any stuck
  `sending` row makes `publish_next` return **`blocked`** and refuse to publish
  anything — because some messages of that digest may already be live in the public
  channel, and blindly retrying would double-post. A human inspects `sent_count`
  and clears the row before publishing resumes.
- **Degrade only on proof.** A media send that *proves* nothing reached Telegram
  (unreadable file, DNS/connection error, `sendRichMessage` 404 = method absent)
  drops to the next plan, because the digest demonstrably has not gone out. Every
  **ambiguous** failure — any HTTP status Telegram answered with, a read timeout —
  stops the chain and leaves the row `sending`: re-sending could double-post.
- **Dry-run** prints the plan (for a rich post: chat, image path, char count, and
  the rich HTML) and reverts the row to `approved` without sending, so you can
  preview safely.

---

## Operating it

All stages run through the one CLI, each printing a single JSON line:

```bash
# Inspect the ledger (last 14 days: date, status, updated_at)
python -m src.telegram_aggregator_tool status

# Approve the pending digest (or a specific day)
python -m src.telegram_aggregator_tool approve [--date YYYY-MM-DD]

# Publish the next approved digest (preview first with --dry-run)
python -m src.telegram_aggregator_tool publish --dry-run
python -m src.telegram_aggregator_tool publish
```

Manual pipeline stepping (normally the daily runner does this for you):

```bash
python -m src.telegram_aggregator_tool collect        # channels, + chats if allowlisted
python -m src.telegram_aggregator_tool chats-dry-run  # preview the chat lane, writes nothing
python -m src.telegram_aggregator_tool render-input --out "$STATE/drafts/$DATE-input.json"
# ...LLM writes $STATE/drafts/$DATE-draft.json via the /aggregator-digest skill...
python -m src.telegram_aggregator_tool gate \
  --input "$STATE/drafts/$DATE-input.json" \
  --draft "$STATE/drafts/$DATE-draft.json" \
  --date "$DATE" --auto-approve
```

### Daily automation

`scripts/aggregator_draft_runner.sh` chains **collect → render-input → draft →
gate (`--auto-approve`)** for a hands-off daily draft, with a single automatic
regeneration if the first draft fails the gates (the gate errors are fed back to
the model as a feedback file). It takes a lock so two runs can't overlap, logs each
stage, and pings the alert bot only on failure. A **separate** scheduled
`publish` job runs later to post the approved digest — keeping drafting and
publishing on independent schedules so a slow draft never blocks the publish
window.

The draft step calls `claude -p` against the local `/aggregator-digest` skill
without an API token (it uses the interactive session's auth), so drafting costs
nothing beyond the existing subscription.

---

## Files

| Path | Role |
|------|------|
| `src/telegram_aggregator.py` | Collect + build draft input from the read proxy. |
| `src/telegram_aggregator_gates.py` | Deterministic draft gates (schema, link-exists, no-verbatim). |
| `src/telegram_aggregator_publish.py` | Rendering, message splitting, Bot API transport, the 2-phase ledger. |
| `src/telegram_aggregator_tool.py` | The CLI (`collect`, `render-input`, `gate`, `approve`, `publish`, `status`). |
| `scripts/aggregator_draft_runner.sh` | Daily draft runner (collect → draft → gate → auto-approve). |
| `.claude/skills/aggregator-digest/SKILL.md` | The LLM drafting instructions. |
