#!/usr/bin/env bash
# aggregator_draft_runner.sh — daily digest draft via token-less claude -p.
# Invocation contract (same as drift_triage_runner.sh — must never regress):
#  - plain `claude -p`, NEVER --bare (--bare bills the API instead of Max OAuth);
#  - env -u both token vars so nothing shadows the OAuth session;
#  - --dangerously-skip-permissions (headless; output is gated by code anyway);
#  - cwd = this repo so the /aggregator-digest skill resolves;
#  - 9>&- so no claude descendant inherits the run lock.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${AGGREGATOR_STATE_DIR:-/home/claude-developer/telegram-aggregator}"
# Export so the "$PY" - <<EOF heredocs below see the SAME state dir via their
# own resolve_paths() call — never rely on the bash default and the python
# default staying in sync by coincidence.
export AGGREGATOR_STATE_DIR="$STATE_DIR"
LOG_DIR="$STATE_DIR/logs"
PY="$REPO_DIR/venv/bin/python"
TODAY="$(date -u +%F)"
mkdir -p "$LOG_DIR"

exec 9>"$STATE_DIR/draft-runner.lock"
flock -n 9 || { echo "another draft run is active; exiting"; exit 0; }

git -C "$REPO_DIR" pull --ff-only 2>>"$LOG_DIR/$TODAY-runner.log" || true

cd "$REPO_DIR"
INPUT="$STATE_DIR/drafts/$TODAY-input.json"
DRAFT="$STATE_DIR/drafts/$TODAY-draft.json"
FEEDBACK="$STATE_DIR/drafts/$TODAY-gate-errors.json"

# collect failure must not kill the draft run — stale-but-present data still
# makes a digest.
"$PY" -m src.telegram_aggregator_tool collect >>"$LOG_DIR/$TODAY-runner.log" 2>&1 || true

# render-input and every run_draft call below must NOT kill the script
# silently under set -e — a bare failing command outside an if/&&/|| context
# still trips -e and the run just vanishes with no log line and no operator
# ping (that was the "no digest today, no one told me" gap). Toggle -e off
# around the call so the real exit code survives into $rc (negating via
# `if ! cmd` loses the original code), log it, notify, then exit 1 ourselves.
notify_stage_failure() {
  local stage="$1" rc="$2"
  echo "stage '$stage' failed (rc=$rc)" >>"$LOG_DIR/$TODAY-runner.log"
  "$PY" - <<'EOF'
from src.telegram_aggregator_publish import notify_operator
notify_operator("❌ Дайджест: сбой на этапе render-input/draft — см. логи")
EOF
}

set +e
"$PY" -m src.telegram_aggregator_tool render-input --out "$INPUT" >>"$LOG_DIR/$TODAY-runner.log" 2>&1
rc=$?
set -e
[ "$rc" -eq 0 ] || { notify_stage_failure "render-input" "$rc"; exit 1; }

run_draft() {
  local extra="${1:-}"
  env -u ANTHROPIC_API_KEY -u CLAUDE_CODE_OAUTH_TOKEN \
    claude -p "/aggregator-digest $INPUT $DRAFT $extra" \
    --model claude-sonnet-5 \
    --output-format json \
    --dangerously-skip-permissions \
    >"$LOG_DIR/$TODAY-claude.json" \
    2>"$LOG_DIR/$TODAY-claude-stderr.log" 9>&-
}

# set -euo pipefail + `tee` inside a function used in an `if !` condition is
# fine: pipefail makes the pipeline's exit status the rightmost non-zero exit
# (the python gate command), not tee's — so a gate-failed/parse-error/
# input-error exit from the CLI still trips `if ! gate`.
gate() {
  "$PY" -m src.telegram_aggregator_tool gate \
    --draft "$DRAFT" --input "$INPUT" --date "$TODAY" \
    | tee "$STATE_DIR/drafts/$TODAY-gate.json"
}

set +e
run_draft
rc=$?
set -e
[ "$rc" -eq 0 ] || { notify_stage_failure "draft" "$rc"; exit 1; }

if ! gate; then
  cp "$STATE_DIR/drafts/$TODAY-gate.json" "$FEEDBACK" 2>/dev/null || true
  echo "gate failed; one regen with feedback" >>"$LOG_DIR/$TODAY-runner.log"
  set +e
  run_draft "$FEEDBACK"
  rc=$?
  set -e
  [ "$rc" -eq 0 ] || { notify_stage_failure "draft-regen" "$rc"; exit 1; }
  gate || { "$PY" - <<'EOF'
from src.telegram_aggregator_publish import notify_operator
notify_operator("❌ Дайджест: черновик не прошёл гейты после regen — сегодня без выпуска. См. логи draft-runner.")
EOF
  exit 1; }
fi

"$PY" - "$TODAY" <<'EOF'
import json, sys
from src.telegram_aggregator import resolve_paths
from src.telegram_aggregator_publish import DigestLedger, notify_operator
today = sys.argv[1]
paths = resolve_paths()
# Best-effort preview: an unreadable/malformed gate or draft file must not
# crash the runner — fall back to a plain "draft ready, see state dir" ping
# instead (notify_operator itself never raises either way).
try:
    gate = json.loads((paths.drafts_dir / f"{today}-gate.json").read_text())
    preview = "\n".join(
        f"• {s['headline']}" for s in json.loads((paths.drafts_dir / f"{today}-draft.json").read_text())["stories"]
    )
    notify_operator(
        f"📰 Черновик дайджеста {today} готов: {gate['stories']} сюжетов, {gate['messages']} сообщ.\n{preview}\n\n"
        f"Одобрить: aggregator approve  (затем publish в 07:47 UTC)"
    )
except Exception:
    notify_operator(
        f"📰 Черновик дайджеста {today} готов — см. {paths.drafts_dir}.\n"
        f"Одобрить: aggregator approve  (затем publish в 07:47 UTC)"
    )
EOF
