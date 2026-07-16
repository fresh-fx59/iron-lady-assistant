# Deployment

This app runs on **your NixOS host (`<your-host>`)** as NixOS-managed systemd units. It is **NOT**
deployed via a `deploy.sh` / GitHub Action (those were removed 2026-07-14 — they targeted a
pre-migration host and a dead path `/home/claude-developer/claude-code-as-assistant`, and
following them would have deployed nothing).

## How it actually deploys
- **Units, env, secrets** — defined in the `personal-os` vault at
  `projects/active/contabo-nixos-migration-after-incident/nixos/modules/services/iron-lady.nix`,
  applied with `nixos-rebuild switch` (lock-serialized). Parser accounts are per-instance
  `telegram-proxy-<name>` units reading per-account sops blobs.
- **App source (`src/`)** — deployed **out-of-band**: copy `src/` to `/var/lib/iron-lady/src`,
  then `systemctl restart telegram-proxy-giedi telegram-scheduler iron-lady-bot gmail-gateway`.
- **Drift detection** — `iron-lady-src-drift.timer` on the box compares the deployed `src/`
  to this repo's `main` daily (via `gh api`) and fails loudly on divergence.

## Full runbooks (in the `personal-os` vault)
- `references/contabo-nixos-deploy-operations.md` — rebuild/deploy ops + the flock rule
- `references/telegram-parser-enrollment-runbook.md` — enroll/re-enroll a parser account
- `references/telegram-parser-disaster-recovery.md` — rebuild-from-scratch + backups
