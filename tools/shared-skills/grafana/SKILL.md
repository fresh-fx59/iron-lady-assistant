---
name: grafana
description: Use Grafana API workflows for low-token dashboard operations (search, summarize, create/update/delete) via a local helper script and ~/.config/grafana/env credentials.
---

# Grafana Skill

Use this skill when the task involves Grafana dashboards, service accounts, folders, or alert resources and you want to minimize token-heavy JSON exchanges.

## Quick Start

1. Ensure credentials are available in `~/.config/grafana/env`:
   - `GRAFANA_URL`
   - `GRAFANA_TOKEN`
2. Use `scripts/grafana_api.py` for all API calls and summaries.
3. Return concise summaries by default; only print full JSON when explicitly requested.

## Commands

- `python3 scripts/grafana_api.py me`
  - Validate token and show active principal.
- `python3 scripts/grafana_api.py search --query "<text>"`
  - Search dashboards/folders.
- `python3 scripts/grafana_api.py dashboard-summary --uid "<uid>"`
  - Print compact dashboard metadata + panel list.
- `python3 scripts/grafana_api.py dashboard-get --uid "<uid>" --out /tmp/db.json`
  - Fetch full dashboard JSON to file (no token-heavy chat output).
- `python3 scripts/grafana_api.py dashboard-upsert --file /tmp/db.json`
  - Create/update a dashboard from JSON payload.
- `python3 scripts/grafana_api.py dashboard-delete --uid "<uid>"`
  - Delete dashboard by UID.

## Output Discipline

- Prefer compact tabular/text summaries.
- When large JSON is needed, write to a file path and reference that path instead of inlining.
- For dashboard review tasks:
  1. `search`
  2. `dashboard-summary`
  3. `dashboard-get` only for selected UIDs

## Notes

- This skill intentionally focuses on the stable Dashboard HTTP API paths:
  - `/api/user`
  - `/api/search`
  - `/api/dashboards/uid/:uid`
  - `/api/dashboards/db`
- If an endpoint is forbidden (`403`), report missing permission and continue with allowed operations.
