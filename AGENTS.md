# Shared agent guidance

Claude Code and Codex use the same repository instructions and skills.

- Repository skills live in `.claude/skills/`. Codex reaches that exact tree through
  `.agents/skills`; never copy a skill into an agent home.
- The scheduled digest entry point is `scripts/aggregator_draft_runner.sh`. Keep its
  prompt on stdin and use the shared `HEADLESS_AGENT_RUNNER` contract.
- Every model attempt writes a unique, previously absent artifact. Promote it to the
  canonical draft only after exit zero and a fresh non-empty-file check.
- Keep provider, model, effort, sandbox, working directory, allowed state directory,
  binary paths, timeout, and required artifact explicit at the caller boundary.
- Never put prompts, source posts, or tokens in argv or routine logs.
- Preserve the existing Telegram alert, gate, retry, approval, and publish contracts.
- Run focused checks with `TELEGRAM_BOT_TOKEN=test .venv/bin/python -m unittest
  tests/test_aggregator_draft_runner.py -v` before broader tests.
