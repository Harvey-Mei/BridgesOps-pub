<div align="center">

<img src="frontend/public/icon.svg" width="72" alt="BridgeOps">

# BridgeOps

**Multi-channel operations console and smart API gateway for NewAPI / Sub2API relay providers**

English | [简体中文](README.zh.md)

</div>

BridgeOps brings the balances, spending, rate multipliers, announcements, API keys and client routing of many upstream relay accounts into one admin panel. Clients connect to a single OpenAI/Claude-compatible gateway, and BridgeOps routes each request to the most suitable upstream by **actual cash cost, latency and availability**.

> The admin UI is currently in Simplified Chinese.

![Overview](docs/images/02-overview.png)

## Features

- **Unified channel management** — NewAPI and Sub2API sites (password, token or cookie login) plus direct API-key upstreams. Automatic login refresh; balance, spending, group-rate, model and announcement sync; optional per-channel network proxies.
- **Real cost accounting** — when top-ups are not 1:1, a weighted funding factor converts provider credit into the cash you actually paid. Balances, spending, request cost and routing scores all use the normalized value.
- **Smart routing gateway** — one endpoint for OpenAI (Chat Completions / Responses / Images) and Anthropic Messages. Scores candidates by price, latency, cache reuse and model capability; fails over before useful output starts; **never replays a request the upstream already accepted**, so there are no duplicate charges or tool calls. Session affinity, candidate groups, fixed routes and model mapping are supported.
- **Managed upstream keys** — creates, recovers and cleans up one managed key per upstream group with a full audit trail; manually created keys are never deleted.
- **Usage records and cost analytics** — tokens, cache read/write, first-byte latency, route attempts, frozen unit prices and cost source for every request; filtering, sorting, CSV export and reconciliation.
- **Monitoring and alerts** — low balance, rate changes, subscription quotas and provider announcements via Telegram, Webhook, Email, WeCom, DingTalk, Feishu and ServerChan3.
- **One-click client import** — CCSwitch (Codex CLI / Claude Code), Cherry Studio, Chatbox, Continue and more.
- **Extras** — Pelican Gallery (local model sanity check), Codex / Claude radar dashboards, captcha-provider balance management.

## Screenshots

| Channels | Channel monitor |
| --- | --- |
| ![Channels](docs/images/03-channels.png) | ![Channel monitor](docs/images/04-channel-monitor.png) |
| **Gateway keys** | **Usage records** |
| ![Gateway keys](docs/images/05-gateway-keys.png) | ![Usage records](docs/images/07-usage.png) |
| **Costs** | **Routing parameters** |
| ![Costs](docs/images/08-costs.png) | ![Routing parameters](docs/images/11-gateway-config.png) |

## Quick start

Requires Docker Engine and Docker Compose v2.

```bash
mkdir bridgeops && cd bridgeops
curl -fsSLO https://raw.githubusercontent.com/Harvey-Mei/BridgesOps-pub/main/docker-compose.yml
curl -fsSL  https://raw.githubusercontent.com/Harvey-Mei/BridgesOps-pub/main/.env.example -o .env

# Fill in the required secrets (or edit .env by hand)
sed -i "s/^POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=$(openssl rand -hex 16)/" .env
sed -i "s/^APP_SECRET=.*/APP_SECRET=$(openssl rand -hex 32)/" .env
sed -i "s/^ADMIN_PASSWORD=.*/ADMIN_PASSWORD=change-this-password/" .env

docker compose up -d
```

Open `http://your-server:8080` and sign in as `admin` with your `ADMIN_PASSWORD`.

> ⚠️ `APP_SECRET` encrypts every stored upstream credential. **Back it up** — if it changes or is lost, saved credentials can no longer be decrypted. Put an HTTPS reverse proxy in front of any public deployment.

### First steps

1. **渠道管理 (Channels) → 添加渠道 (Add channel)**: pick the site type (NewAPI / Sub2API), enter the site URL and credentials, then run the login test.
2. Wait for the background sync of balance, rates and models (usually under a minute). A managed key is created for each upstream group.
3. If top-ups are not 1:1, record the cash actually paid with **充值 (Top-up)** so costs reflect real money.
4. **密钥管理 (Keys) → 创建 Key (Create key)**: choose a model family and routing mode, then copy the gateway key.
5. Point your client at `http://your-server:8080/v1` (OpenAI-compatible) or `http://your-server:8080` (Claude Code), or use **导入客户端 (Import to client)**.

The full guide (Chinese) is in **[docs/usage.md](docs/usage.md)**.

## Deployment

| Method | Notes |
| --- | --- |
| Docker Compose | Recommended. Image `ghcr.io/harvey-mei/bridgesops-pub`, amd64 / arm64 |
| Binary + systemd | `./scripts/build-deployment-artifact.sh` builds a static binary with the UI embedded; `packaging/server/` has systemd, Redis and Caddy examples |

- Database: PostgreSQL 16+ only. Redis is optional and only serves a 30-second analytics cache.
- Upgrade: `docker compose pull app && docker compose up -d`. Migrations run automatically at startup with checksum verification. Pin `IMAGE_TAG=vX.Y.Z` in production.
- Backup: see [docs/usage.md](docs/usage.md#backup-and-upgrade) and [docs/recovery.md](docs/recovery.md).

## Documentation

| Document | Topic |
| --- | --- |
| [Usage guide](docs/usage.md) (zh) | Deployment, channels, keys, clients, operations |
| [Local development](docs/local-development.md) | Dev environment, tests, Docker verification |
| [PostgreSQL contract](docs/postgresql-migration.md) / [Recovery](docs/recovery.md) | Database and backups |
| [Network proxies](docs/network-proxies.md) (zh) | Per-channel egress |
| [Client import](docs/client-import.md) (zh) | Supported clients and limits |
| [Cost-aware routing](docs/cost-aware-routing.md) (zh) / [Failure confirmation](docs/gateway-failure-confirmation.md) (zh) | Routing and billing rules |
| [Request memory admission](docs/gateway-request-memory-admission.md) | Gateway request-body limits |

## Development

```bash
pnpm --dir frontend install --frozen-lockfile
./scripts/local-dev.sh          # backend on 127.0.0.1:8418, Vite on 127.0.0.1:3010
./scripts/local-test.sh         # full checks (needs a PostgreSQL test database)
./scripts/docker-test.sh        # isolated Docker acceptance stack on port 18418
```

Requirements: Go 1.23+, Node.js 20+, pnpm 10.4 (`corepack enable`), PostgreSQL 16+. See [docs/local-development.md](docs/local-development.md).

## Security

- Upstream passwords, tokens, cookies and API keys are encrypted with AES-GCM using `APP_SECRET`; stored secrets are never returned by the API.
- The gateway never forwards admin cookies, upstream `Set-Cookie`, provider implementation headers or internal routing headers to clients; errors are sanitized.
- Session affinity stores only a SHA-256 hash of the session identifier.
- Never commit `.env`, `config.yaml`, databases or real credentials.

## License

BridgeOps is distributed under a custom **source-available, non-commercial license**. It is not an OSI-approved open-source license.

- Personal, educational, research, evaluation and internal non-commercial use is permitted.
- Commercial use, paid hosting, paid API/SaaS offerings, commercial redistribution and commercial bundling require written permission.
- Redistributed, publicly hosted or modified versions must keep a visible BridgeOps attribution with the repository link, license and third-party notices.

See [LICENSE](LICENSE) and [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md). Third-party dependencies remain under their own licenses.

## Acknowledgements

BridgeOps builds on ideas and interface patterns from [upstream-hub](https://github.com/worryzyy/upstream-hub), UpstreamOps and Sub2API. Icons from [Lobe Icons](https://github.com/lobehub/lobe-icons).
