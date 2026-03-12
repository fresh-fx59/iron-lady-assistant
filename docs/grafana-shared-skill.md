# Grafana Shared Skill

This repository stores the canonical Grafana skill/tool used by local Codex instances.

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
