# 客户端导入与配置约束

密钥管理页的“导入客户端”入口按客户端类型组织配置，但客户端品牌、下游协议和上游协议必须分开判断。当前页面只能为 BridgeOps 已经对外提供的协议生成配置；尚未实现协议转换的组合必须禁用，不能生成看似可用的配置。

## 当前支持方式

| 客户端 | 接入方式 | BridgeOps 行为 |
| --- | --- | --- |
| CCSwitch / Codex CLI | `ccswitch://` 官方深链 | 导入 Responses 配置和真实模型；HTTPS/loopback 地址启用累计 API 费用查询和 30 分钟自动刷新 |
| CCSwitch / Claude Code | `ccswitch://` 官方深链 | 导入 Anthropic Messages 配置、真实模型和三类 Claude 模型别名；HTTPS/loopback 地址启用用量刷新 |
| Cherry Studio | 设置页参数 | 官方深链强制把 Key 放入 URI，因此 BridgeOps 改为显示准确的手动填写参数 |
| Chatbox | `chatbox://provider/import` | 使用不含 Key 的官方 custom-provider payload；先复制 Key，再由用户在确认页粘贴 |
| LobeChat、Cline、Roo Code、Kilo Code | 设置页参数 | 明确显示需要填写的设置位置、协议、Base URL、Key 和模型 |
| Continue | 配置文件 | 明确显示 `~/.continue/config.yaml` 及可复制内容 |
| Codex / Claude 原生文件 | 配置文件 | 明确显示 `~/.codex/config.toml`、`~/.codex/auth.json` 或 `~/.claude/settings.json` |

Gemini CLI 原生 `/v1beta` 协议尚未实现，因此不能把 OpenAI 兼容配置标成 Gemini CLI 原生可用，也不能从旧“配置”入口生成 `GOOGLE_GEMINI_BASE_URL` / `GEMINI_API_KEY` 环境变量。界面的“协议不兼容”表示当前 Key 与客户端协议不能直接互通，不表示后台正在转换。完整多协议适配尚在规划中；实现前必须先补协议矩阵、流式转换和回归测试，再开放对应导入项。

## 模型来源

智能路由的客户端默认模型只能来自当前下游 Key 的可路由候选：候选必须未被当前 Key 禁用、未处于冷却且未被后端标记为不可用。暂停分组探测只停止后台延迟观测，不阻止真实智能流量，也不阻止候选分组成员关系。导出的默认模型必须来自这些候选同步得到的 `model_ids`。固定路由导入只从已配置目标的已同步模型中选择，不应用智能冷却、候选禁用、探测暂停或缓存协议能力过滤；真实转发仍会在渠道、分组、本地 Key 或实际协议目标缺失时明确失败。不得根据 Key 名称、分组名、渠道名或客户端品牌猜模型，也不得写死一个通用默认模型。

默认模型按当前 Key 的模型家族筛选，并从真实同步型号中选择旗舰：GPT 按数字版本从新到旧，同版本优先 Pro / Codex；Claude 优先 Opus，再按版本选择；Gemini 优先最新 Pro。同版本优先稳定型号。忽略 `auto-review` / `auto` 等别名、mini / nano / Flash / Haiku 等轻量型号及图片、音频、嵌入等专用型号。不受模型列表首项顺序影响，不写死某个最新版本，也不修改 Key 的模型映射或路由设置；其它家族未识别到旗舰时保留实际同步的普通文本型号。Codex 的 `model` 与 `review_model` 使用同一选择，Claude 原生设置通过 `ANTHROPIC_MODEL` 写入所选模型（[官方模型配置](https://code.claude.com/docs/en/model-config)）。

如果没有可靠模型：

- 文件或表单配置可以继续生成，但必须提示用户在客户端选择真实可用模型；如果客户端配置格式本身要求模型（例如 Continue），则在没有可靠模型时保持复制入口禁用并提示先同步，不得写入占位模型。
- CCSwitch 一键导入必须禁用。CCSwitch 自身会在缺少 Codex 模型时回退到内置默认值，可能生成不属于当前 Key 的配置。
- 智能路由 Key 没有可路由候选时，密钥列表中的客户端导入与旧配置入口都必须禁用，不能生成指向故障路由的配置。固定路由不能因为“没有智能可路由候选”而禁用；它只按已配置单目标是否仍有结构有效的本地 Key、目标和可靠模型决定导入入口。

## CCSwitch 用量与测试语义

BridgeOps 通过下游 Key 的 `GET /v1/usage` 提供累计请求、Token 和费用。导入的 CCSwitch 用量脚本只展示当前总费用及统计摘要，不提供或伪造上游账户剩余额度。总费用包含已对账、上游直报和仍待对账的估算项；摘要会分别标明已对账与估算金额，不能只取 `actual_cost` 而漏掉尚未对账的消费。

CCSwitch 的自定义用量脚本只允许 HTTPS，或本机 `localhost` / loopback HTTP。通过局域网、ZeroTier IP 等普通 HTTP 地址打开 BridgeOps 时不注入用量脚本，避免生成必然失败的配置；生产 HTTPS 地址不受影响。

CCSwitch 的供应商“测试”是对 Base URL 的轻量可达性检查：收到任意 HTTP 响应即算可达，不验证 Key、模型或鉴权。真实模型请求测试仍由 BridgeOps 密钥管理页的“api测试”执行并写入使用记录。

## 安全与维护

- Chatbox 深链不得携带 Key；Cherry Studio 使用手动填写，避免 Key 进入其强制带凭据的 URI。
- CCSwitch 官方 provider 深链要求携带 Key，只能在可信本机使用；BridgeOps 不把生成的深链写入服务端日志或公开页面。
- 不得把包含 Key 的导入链接改成 HTTPS 跳转链接，避免进入浏览器历史、代理日志或分析系统。
- 文件型配置必须显示准确的目标文件；设置型配置必须显示准确的填写位置，不能只显示一段无上下文 JSON。管理员打开“导入客户端”后，生成内容直接带入当前下游 Key，避免再次手工替换；这些内容不得写入服务端日志、普通用户页面或普通 HTTPS 跳转 URL。
- 新增客户端前必须核对其当前官方 schema 或源码，并为深链 payload、协议类型和模型字段增加回归测试。
