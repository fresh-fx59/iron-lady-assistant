# Server Setup Guide

Reproducible setup for Alex's server. Follow this if migrating to a new server.

## Server Info

- **IP**: 31.220.78.216
- **OS**: Ubuntu 24.04.4 LTS (Noble Numbat)
- **Kernel**: 6.8.0-101-generic
- **User**: `claude-developer` (UID 1000, groups: sudo, docker)
- **Docker**: 29.2.1
- **Python**: 3.12.3
- **Node**: v20.20.0

## Directory Layout

```
/home/claude-developer/
├── claude-code-as-assistant/   # Telegram bot (git repo)
│   ├── src/                    # Bot source code
│   ├── memory/                 # Bot memory — gitignored, must be restored
│   │   ├── user_profile.yaml   # User profile + semantic facts (YAML)
│   │   └── episodes.db         # Episodic memory (SQLite FTS5)
│   ├── tools/                  # Custom tool definitions (YAML)
│   ├── .deploy/                # Deploy state (gitignored)
│   ├── .env                    # Bot secrets (gitignored)
│   ├── run.sh                  # Entrypoint with crash loop protection
│   ├── deploy.sh               # Protected deploy script
│   ├── work-dir/               # Claude Code subprocess working directory
│   └── venv/                   # Python virtualenv (auto-created by run.sh)
├── .claude/projects/.../memory/  # Claude Code session memory (not in git)
│   ├── MEMORY.md               # Auto-loaded into Claude Code context
│   └── *.md                    # Topic-specific notes
├── syncthing/                  # Syncthing Docker setup
│   ├── docker-compose.yml
│   └── config/                 # Syncthing config (auto-generated)
├── obsidian-vault/             # Obsidian notes (synced via Syncthing)
├── traefik/                    # Reverse proxy
│   ├── docker-compose.yml
│   ├── certs/                  # TLS certificates
│   └── dynamic/                # Traefik routing config
├── monitoring/                 # Prometheus + Grafana
├── openclaw/                   # OpenClaw agent platform
└── crossposting-telegram-to-max-saas/  # Crossposter service
```

## Memory Systems

Two independent memory systems — both are **not in git** and must be restored on a new server.

### 1. Bot Memory (`memory/` — gitignored)

Used by the Telegram bot. Injected as XML context before each Claude Code subprocess call.

| Layer | File | Description |
|-------|------|-------------|
| Core + Semantic | `memory/user_profile.yaml` | User profile, preferences, facts with confidence scores |
| Episodic | `memory/episodes.db` | SQLite FTS5 — conversation summaries, keyword-searchable |

**Restore**: The bot creates these automatically on first run. `user_profile.yaml` starts empty and accumulates facts over time. Episodes build up from `/new` session reflections.

**Backup strategy**: Back up `memory/` directory periodically. On a new server, copy it back before starting the bot.

### 2. Claude Code Session Memory (`~/.claude/projects/.../memory/`)

Used by Claude Code interactive sessions (like this one). Auto-loaded into context.

| File | Description |
|------|-------------|
| `MEMORY.md` | Always loaded — project notes, env info, investigation state |
| `*.md` | Topic files linked from MEMORY.md |

**Restore**: Recreate manually or let it accumulate. Operational notes, not critical data.

**Path**: `~/.claude/projects/-home-claude-developer-claude-code-as-assistant/memory/`

### Key difference

- **Bot memory** = what the bot knows about the user (injected into every Telegram conversation)
- **Session memory** = what Claude Code knows about the project (loaded into interactive sessions)

## 1. Telegram Bot Setup

### Clone and configure

```bash
cd /home/claude-developer
git clone https://github.com/fresh-fx59/claude-code-as-assistant.git
cd claude-code-as-assistant
cp .env.example .env
# Edit .env: set TELEGRAM_BOT_TOKEN, ALLOWED_USER_IDS=314102923, DEFAULT_MODEL=haiku
```

### Create work directory

```bash
mkdir -p work-dir
```

### Restore bot memory (if available)

```bash
# Copy from backup
cp -r /path/to/backup/memory ./memory/
# Or let the bot create fresh ones:
mkdir -p memory
```

### Systemd service

```bash
sudo cp telegram-bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now telegram-bot.service
```

Service file location: `/etc/systemd/system/telegram-bot.service`

Key settings:
- `User=claude-developer`
- `WorkingDirectory=/home/claude-developer/claude-code-as-assistant`
- `ExecStart=run.sh` (auto-creates venv, installs deps)
- `Restart=always`, `RestartSec=5`
- `StartLimitBurst=5`, `StartLimitIntervalSec=600`
- `NoNewPrivileges=true`, `ProtectSystem=strict`

### GitHub Actions deploy

Required secrets in GitHub repo (`fresh-fx59/claude-code-as-assistant`):
- `SERVER_HOST` — server IP
- `SERVER_USER` — `claude-developer`
- `SSH_PRIVATE_KEY` — deploy SSH key
- `SSH_PORT` — (optional, default 22)

Generate deploy key:
```bash
ssh-keygen -t ed25519 -f ~/.ssh/deploy_key -N ""
cat ~/.ssh/deploy_key.pub >> ~/.ssh/authorized_keys
# Paste private key into GitHub secret SSH_PRIVATE_KEY
```

## 2. Traefik (Reverse Proxy)

```bash
mkdir -p /home/claude-developer/traefik/{certs,dynamic}
cd /home/claude-developer/traefik
```

`docker-compose.yml`:
```yaml
services:
  traefik:
    image: traefik:v3.3
    container_name: traefik
    command:
      - --api.insecure=true
      - --providers.file.directory=/config
      - --providers.file.watch=true
      - --entrypoints.web.address=:80
      - --entrypoints.websecure.address=:443
      - --entrypoints.web.http.redirections.entrypoint.to=websecure
      - --entrypoints.web.http.redirections.entrypoint.scheme=https
    ports:
      - "80:80"
      - "443:443"
      - "8080:8080"
    volumes:
      - ./certs:/certs:ro
      - ./dynamic:/config:ro
    networks:
      - traefik-public
    restart: unless-stopped

networks:
  traefik-public:
    name: traefik-public
    driver: bridge
```

```bash
docker compose up -d
```

## 3. Syncthing (Obsidian Vault Sync)

### Create directories

```bash
mkdir -p /home/claude-developer/syncthing
mkdir -p /home/claude-developer/obsidian-vault
```

### Docker Compose

`/home/claude-developer/syncthing/docker-compose.yml`:
```yaml
services:
  syncthing:
    image: syncthing/syncthing:latest
    container_name: syncthing
    hostname: vmi-syncthing
    environment:
      - PUID=1000
      - PGID=1000
    volumes:
      - ./config:/var/syncthing/config
      - /home/claude-developer/obsidian-vault:/var/syncthing/data
    ports:
      - "127.0.0.1:8384:8384"   # Web UI — localhost only (SSH tunnel)
      - "22000:22000/tcp"        # BEP sync protocol
      - "22000:22000/udp"        # BEP QUIC
      - "21027:21027/udp"        # Discovery
    restart: unless-stopped
```

### Start

```bash
cd /home/claude-developer/syncthing
docker compose up -d

# If config/ dir has wrong ownership (root instead of 1000):
docker run --rm -v /home/claude-developer/syncthing/config:/config alpine chown -R 1000:1000 /config
docker compose restart
```

### Pair with devices

Access web UI via SSH tunnel:
```bash
ssh -L 8384:localhost:8384 claude-developer@31.220.78.216
# Then open http://localhost:8384 in browser
```

- Add device IDs from phone/PC
- Share Obsidian vault folder, mapped to `/var/syncthing/data` in container
- Vault accessible at `/home/claude-developer/obsidian-vault/` on the host

## 4. Monitoring

Docker containers running:
- `cadvisor` — container metrics
- `node_exporter` — host metrics
- Bot exposes Prometheus metrics on port 9101

## 5. Other Services

- **LiteLLM proxies** — multiple instances for provider fallback (ports 4000-4002)
- **OpenClaw** — agent platform (gateway container)
- **Crossposter** — Telegram-to-MAX crossposting (backend + frontend + postgres)

## 6. Cloudflare Free Geo Steering (Implemented 2026-02-27)

Target zone: `aiengineerhelper.com`

Goal:
- Keep Cloudflare in front
- Route `RU` traffic to monitoring origin (`45.151.30.146`)
- Route non-`RU` traffic to primary origin (`31.220.78.216`)

### Implementation

- Product: Cloudflare Workers on Free plan (instead of paid Load Balancer geo steering)
- Worker script: `geo-origin-steering`
- Script source in repo: `tools/cloudflare/geo-origin-steering.js`
- Worker routes:
  - `aiengineerhelper.com/*`
  - `crossposter.aiengineerhelper.com/*`
- Internal DNS records used by `resolveOverride`:
  - `cf-origin-main.aiengineerhelper.com -> 31.220.78.216` (proxied)
  - `cf-origin-ru.aiengineerhelper.com -> 45.151.30.146` (proxied)
- Response debug headers from Worker:
  - `X-Origin-Selected: main|ru|main-fallback|ru-fallback`
  - `X-CF-Country: <country code>`

### Monitoring server path

`45.151.30.146` currently handles:
- `:80` HTTP reverse proxy to `31.220.78.216:80` for:
  - `aiengineerhelper.com`
  - `crossposter.aiengineerhelper.com`
- `:443` TLS passthrough (`nginx stream`) to `31.220.78.216:443` based on SNI for the same hostnames

### TLS notes

- Both origins present Cloudflare Origin CA cert with SAN:
  - `aiengineerhelper.com`
  - `*.aiengineerhelper.com`
- Cloudflare SSL mode should be `Full (strict)`.
- API token used on 2026-02-27 did not have zone SSL settings permission (`error 9109`), so SSL mode must be verified/set in dashboard or with a broader token.

### Verification commands

```bash
# Edge result should include worker headers
curl -sSI https://aiengineerhelper.com | grep -iE 'x-origin-selected|x-cf-country|^HTTP'
curl -sSI https://crossposter.aiengineerhelper.com | grep -iE 'x-origin-selected|x-cf-country|^HTTP'

# Confirm monitoring origin ports are open
nc -zv 45.151.30.146 80 443

# Direct origin checks (Origin CA is not publicly trusted; use -k for local probe)
curl -k -sSI --resolve aiengineerhelper.com:443:31.220.78.216 https://aiengineerhelper.com | head
curl -k -sSI --resolve aiengineerhelper.com:443:45.151.30.146 https://aiengineerhelper.com | head
```

## Ports Summary

| Port | Service | Exposure |
|------|---------|----------|
| 80 | Traefik HTTP | Public |
| 443 | Traefik HTTPS | Public |
| 8080 | Traefik dashboard | Public (consider restricting) |
| 8384 | Syncthing Web UI | Localhost only |
| 9101 | Bot Prometheus metrics | Local |
| 22000 | Syncthing BEP sync | Public |
| 21027 | Syncthing discovery | Public |
