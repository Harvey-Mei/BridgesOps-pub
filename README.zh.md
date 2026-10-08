<div align="center">

<img src="frontend/public/icon.svg" width="72" alt="BridgeOps">

# BridgeOps

**面向 NewAPI / Sub2API 中转站的多渠道运营后台与智能 API 网关**

[English](README.md) | 简体中文

</div>

BridgeOps 把多个上游中转站账号的余额、消费、倍率、公告、Key 和客户端路由集中到一个后台里管理。客户端只需接入一个 OpenAI / Claude 兼容网关地址，BridgeOps 会按**真实现金成本、延迟和可用性**自动选择最合适的上游，在保证可用性的前提下尽量降低调用成本。

![总览](docs/images/02-overview.png)

## 功能亮点

- **多渠道统一管理**：支持 NewAPI、Sub2API 站点（账号密码 / Token / Cookie 登录）和直连 API Key；自动登录续期、同步余额、消费、分组倍率、模型和公告；可按渠道绑定独立网络代理。
- **真实成本核算**：充值不是 1:1 时，按加权资金系数把站内额度换算为真实现金成本，余额、消费、请求费用和路由评分都使用换算后的数值。
- **智能路由网关**：一个入口兼容 OpenAI（Chat Completions / Responses / Images）与 Anthropic Messages 协议。按价格、延迟、缓存命中和模型能力打分；有效输出前自动故障切换；**已被上游接受的请求绝不重放**，避免重复扣费和重复工具调用；支持会话亲和、候选分组、固定路由与模型映射。
- **受管 Key 生命周期**：为每个上游分组自动创建、恢复、清理受管 Key，并保留完整审计；不会误删手工 Key。
- **使用记录与费用统计**：每次请求的 Token、缓存读写、首字延迟、路由过程、冻结单价与费用来源一目了然；支持筛选、排序、导出 CSV 和对账。
- **监控与告警**：低余额、倍率变动、订阅额度、上游公告，支持 Telegram、Webhook、邮件、企业微信、钉钉、飞书、Server 酱³。
- **客户端一键导入**：CCSwitch（Codex CLI / Claude Code）、Cherry Studio、Chatbox、Continue 等。
- **辅助工具**：鹈鹕画廊（本地模型体感检测）、Codex / Claude 智商雷达、打码平台余额管理。

## 截图

| 渠道管理 | 渠道监控 |
| --- | --- |
| ![渠道管理](docs/images/03-channels.png) | ![渠道监控](docs/images/04-channel-monitor.png) |
| **密钥管理** | **使用记录** |
| ![密钥管理](docs/images/05-gateway-keys.png) | ![使用记录](docs/images/07-usage.png) |
| **费用统计** | **参数配置** |
| ![费用统计](docs/images/08-costs.png) | ![参数配置](docs/images/11-gateway-config.png) |

更多截图见 [使用说明](docs/usage.md)。

## 快速开始

需要 Docker Engine 与 Docker Compose v2。

```bash
mkdir bridgeops && cd bridgeops
curl -fsSLO https://raw.githubusercontent.com/Harvey-Mei/BridgesOps-pub/main/docker-compose.yml
curl -fsSL  https://raw.githubusercontent.com/Harvey-Mei/BridgesOps-pub/main/.env.example -o .env

# 生成随机密钥并填入 .env（也可以手动编辑）
sed -i "s/^POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=$(openssl rand -hex 16)/" .env
sed -i "s/^APP_SECRET=.*/APP_SECRET=$(openssl rand -hex 32)/" .env
sed -i "s/^ADMIN_PASSWORD=.*/ADMIN_PASSWORD=请改成你的管理员密码/" .env

docker compose up -d
```

打开 `http://服务器IP:8080`，用 `admin` 和你设置的 `ADMIN_PASSWORD` 登录。

> ⚠️ `APP_SECRET` 用于加密所有上游凭据，**请务必备份**，修改或丢失后已保存的凭据无法解密。公网使用请在前面加 HTTPS 反向代理（见 [使用说明 · 配置 HTTPS](docs/usage.md#12-配置-https建议)）。

### 第一次使用

1. **渠道管理 → 添加渠道**：选择站点类型（NewAPI / Sub2API）、填写站点地址和登录凭据，点击「测试登录」。
2. 等待后台同步余额、倍率和模型（通常一分钟内），BridgeOps 会为每个上游分组自动创建受管 Key。
3. 如果充值不是 1:1，在渠道上「充值」录入实付金额，让成本按真实现金计算。
4. **密钥管理 → 创建 Key**：选择模型家族和路由方式，复制生成的网关 Key。
5. 在客户端中使用：OpenAI 兼容客户端 Base URL 填 `http://服务器:8080/v1`，Claude Code 填 `http://服务器:8080`；或直接点「导入客户端」。

> 💡 「上游分组」「候选分组」「Key 分组」是三个不同的概念，第一次配置前建议先看 [使用说明 · 三种「分组」一览](docs/usage.md#51-三种分组一览)。

完整说明见 **[使用说明](docs/usage.md)**。

## 部署方式

| 方式 | 说明 |
| --- | --- |
| Docker Compose | 推荐。镜像 `ghcr.io/harvey-mei/bridgesops-pub`，支持 amd64 / arm64 |
| 二进制 + systemd | `./scripts/build-deployment-artifact.sh` 构建内嵌前端的静态二进制，`packaging/server/` 提供 systemd、Redis、Caddy 示例 |

- 数据库：仅支持 PostgreSQL 16+；Redis 可选，仅作 30 秒统计缓存。
- 升级：`docker compose pull app && docker compose up -d`，数据库迁移启动时自动执行。生产环境建议在 `.env` 中固定 `IMAGE_TAG=vX.Y.Z`。
- 备份：见 [使用说明 · 备份与升级](docs/usage.md#backup-and-upgrade) 与 [recovery.md](docs/recovery.md)。

## 文档

| 文档 | 内容 |
| --- | --- |
| [使用说明](docs/usage.md) | 部署、渠道、Key、客户端接入、运维全流程 |
| [本地开发](docs/local-development.md) | 开发环境、测试、Docker 验收 |
| [PostgreSQL 部署约束](docs/postgresql-migration.md) / [备份恢复](docs/recovery.md) | 数据库与备份 |
| [网络代理](docs/network-proxies.md) | 渠道独立出口 |
| [客户端导入](docs/client-import.md) | 各客户端支持方式与限制 |
| [成本路由](docs/cost-aware-routing.md) / [故障三次确认](docs/gateway-failure-confirmation.md) | 路由与计费规则 |
| [上游分组自动归组](docs/automatic-upstream-group-assignment.md) | 新增分组的归组规则 |
| [生图能力](docs/image-operation-capabilities.md) | 付费生图的派发与恢复边界 |
| [请求内存准入](docs/gateway-request-memory-admission.md) | 网关请求体内存限制 |
| [鹈鹕画廊](docs/model-integrity-check.md) | 本地模型检测 |
| [IP 定位](docs/client-ip-geolocation.md) | 管理员查看客户端 IP 归属 |
| [前端设计规范](docs/frontend-design-system.md) | 贡献 UI 时参考 |

## 本地开发

```bash
pnpm --dir frontend install --frozen-lockfile
./scripts/local-dev.sh          # 后端 127.0.0.1:8418 + Vite 127.0.0.1:3010
./scripts/local-test.sh         # 完整检查（需要 PostgreSQL 测试库）
./scripts/docker-test.sh        # 隔离的 Docker 验收环境，端口 18418
```

依赖：Go 1.23+、Node.js 20+、pnpm 10.4（`corepack enable`）、PostgreSQL 16+。详见 [docs/local-development.md](docs/local-development.md)。

## 数据安全

- 上游密码、Token、Cookie、API Key 均以 `APP_SECRET` 派生的 AES-GCM 加密保存，接口不会回显已保存的密钥。
- 网关不会把管理员 Cookie、上游 `Set-Cookie`、上游实现细节或内部路由头透传给客户端；错误信息经过脱敏。
- 会话亲和只保存会话标识的 SHA-256 哈希。
- 请勿提交 `.env`、`config.yaml`、数据库文件或任何真实凭据。

## 许可证

BridgeOps 使用自定义的 **源码可用、非商业许可证**（不是 OSI 认可的开源许可证）：

- 允许个人、教育、研究、评估和组织内部的非商业使用；
- 未经书面许可，禁止商业使用、付费托管、付费 API / SaaS、商业再分发或商业捆绑；
- 再分发、公开部署或修改版本必须在软件和文档中保留可见的 BridgeOps 署名、仓库链接、许可证和第三方声明。

详见 [LICENSE](LICENSE) 与 [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)。第三方依赖与引用代码仍遵循其各自许可证。

## 致谢

BridgeOps 参考了以下项目的运营思路、接口形态与界面设计：[upstream-hub](https://github.com/worryzyy/upstream-hub)、UpstreamOps、Sub2API。图标来自 [Lobe Icons](https://github.com/lobehub/lobe-icons)。
