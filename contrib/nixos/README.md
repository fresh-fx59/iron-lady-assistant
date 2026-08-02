# NixOS

`iron-lady.nix.example` is a sanitized copy of a working module. It shows the
unit topology: the bot, the scheduler, the gmail gateway, and one instanced
proxy per parser account.

Replace before use: `your-host`, `myaccount`, `/path/to/your/vault`, every
`yourservice_*` sops key name, and the `000000000` chat/user IDs.

Not on NixOS? You do not need this file. Run `bash setup.sh` — it installs
system packages, creates a venv, writes `.env`, and renders the units in
`../systemd/`.
