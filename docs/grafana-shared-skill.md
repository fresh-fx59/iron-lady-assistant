# Grafana Shared Skill

This repository stores the canonical Grafana skill/tool used by local Codex instances.

## Clarified Scope

- Canonical source of truth is this repository path (`tools/shared-skills/grafana`).
- All local Codex instances should reference this same path via `skills/grafana` symlink.
- Secret tokens are never stored in git; only local env/secret files hold credentials.
- Current implementation is provider-agnostic at CLI level (plain Python + Grafana HTTP API).
- Agent-specific adapters (Codex/Claude/OpenAI-compatible tool wrappers) should call the same CLI backend to avoid divergence.
- Existing legacy path `/home/claude-developer/.shared-codex-skills/grafana` is superseded by the repo-backed location.
- Running sessions may require restart/new session to pick up skill metadata updates.

## Location

- Skill root: `tools/shared-skills/grafana/`
- Skill entry: `tools/shared-skills/grafana/SKILL.md`
- Helper script: `tools/shared-skills/grafana/scripts/grafana_api.py`

## Runtime Credentials

Credentials stay outside the repository:

- `~/.config/grafana/env`
  - `GRAFANA_URL`
  - `GRAFANA_TOKEN`

## Local Instance Links

Each Codex home links `skills/grafana` to this repo path:

- `/home/claude-developer/.codex/skills/grafana`
- `/home/claude-developer/.codex2/.codex/skills/grafana`
- `/home/claude-developer/codex-secondary/.codex/skills/grafana`

## Verification

Run:

```bash
python3 tools/shared-skills/grafana/scripts/grafana_api.py me
python3 tools/shared-skills/grafana/scripts/grafana_api.py search --query "monitoring"
```
