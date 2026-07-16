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
   channel through a Bot API bot.

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

### Environment variables

| Variable | Purpose |
|----------|---------|
| `AGGREGATOR_STATE_DIR` | Directory holding the aggregator DB, drafts, ledger, and logs. Point it at a writable path you control. |
| `AGGREGATOR_SOURCES_PATH` | Path to the sources file (defaults to `<state_dir>/sources.txt`). |
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
  each message (paced ~1s apart, honoring one `retry_after` on a 429), records the
  running `sent_count`, and finally marks it **`posted`**.
- **Blocks on crash.** If a send dies mid-way, the row is deliberately left in
  `sending` (not auto-reverted, not auto-failed). On the next run, any stuck
  `sending` row makes `publish_next` return **`blocked`** and refuse to publish
  anything — because some messages of that digest may already be live in the public
  channel, and blindly retrying would double-post. A human inspects `sent_count`
  and clears the row before publishing resumes.
- **Dry-run** prints the messages and reverts the row to `approved` without
  sending, so you can preview safely.

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
python -m src.telegram_aggregator_tool collect
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
