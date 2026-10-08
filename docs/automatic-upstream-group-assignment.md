# 上游新增分组自动归组设计

状态：已实现。本文同时作为实现和评审约束，不能用渠道名、上游分组名、Key 名称或候选分组名称推断归属。

## 目标与边界

上游同步成功并取得模型事实后，BridgeOps 可以把新增的 `(channel_id, group_name)` 自动加入一个候选分组。结果必须可解释、可预览、可撤销并可审计。无法唯一判断时保留在“上游分组变更”待处理列表，不做猜测。

自动归组只处理新增关系，不因为价格、余额、延迟或临时故障自动移除既有成员。这些字段变化频繁，用于路由评分和健康判断，不适合作为归组身份。

## 三层职责

### 上游分组事实

事实来自同步和探测结果，不由名称推断：

- 身份：稳定 `upstream_group_id`；外部定位键为 `channel_id + canonical_group_name`
- 渠道类型：`newapi` 或 `sub2api`
- 可用网关协议：`codex`、`claude`、`gemini`
- 已同步模型家族集合
- 已同步模型 ID 集合
- 已确认的模型支持关系和上下文容量
- 管理员维护或上游结构化返回的服务档位，例如 `standard`、`pro`、`max`
- 事实同步时间、同步错误和完整性状态

新增 `gateway_upstream_groups` 作为稳定分组身份和自动归组唯一读取面；上游 Key 只是它的凭据子资源，
Key 删除或重建不能删除分组身份。该表按 `(channel_id, canonical_group_name)` 唯一保存：

| 字段 | 语义 |
| --- | --- |
| `id` / `channel_id` | 稳定分组 ID 和所属渠道 |
| `raw_group_names` / `canonical_group_name` | 上游显示变体集合和唯一规范值 |
| `present` / `presence_generation` | 当前是否存在和本次出现代次 |
| `discovered_at` / `removed_at` | 当前代次的出现和移除时间 |
| `model_family_codes` / `model_ids` | 已分类家族和上游原始模型 ID 集合 |
| `unclassified_model_ids` / `ambiguous_model_ids` | 未分类或命中多个家族的模型 ID |
| `gateway_apps` / `gateway_app_sources` | 协议集合及每项来自目录兼容性还是实际探测 |
| `channel_type` | 渠道类型 |
| `service_tier` / `service_tier_source` | 服务档位及其人工或上游结构化来源 |
| `capability_profile` | 按模型保存已经可靠探测的上下文及能力事实 |
| `classification_version` | 生成家族分类的目录版本 |
| `facts_complete` / `incomplete_reasons` | 是否足以自动判定及缺失原因 |
| `facts_version` / `source_revision` | 当前代次事实版本和来源内容摘要 |
| `models_synced_at` / `facts_built_at` | 本代模型同步时间和事实生成时间 |

`canonical_group_name` 只能由现有后端统一函数生成：Go 仅校验 UTF-8、首尾空白和长度，规范值必须由
PostgreSQL `lower(btrim(raw))` 生成。迁移回填和运行时读写使用同一个数据库算法，不在 Go 中另做
Unicode 大小写转换、兼容折叠或名称语义转换；长度限制与现有分组字段一致。
所有 identity、Key、倍率、成员和 change 读写必须使用相同函数，延续现有大小写不敏感语义。
`vip` / `VIP` 属于同一稳定 identity，raw 变体全部保存供显示和审计；同次同步的大小写变体若事实等价，
按稳定排序选择显示值，若倍率、Key 或模型事实冲突则标记 `identity_conflict` 并停止自动归组，不能
取其中一条覆盖另一条。

现有 `channels`、`gateway_upstream_keys` 和 `gateway_model_capabilities` 是输入来源，但 evaluator 绝不
回退读取它们。所有模型、能力和存在性写入口收口到 group repository：在 PostgreSQL 事务中锁定
`gateway_upstream_groups`，更新来源行，按稳定排序和去重规则重建事实，递增 `facts_version`。
启动和定时 reconciliation 用来源 revision 修复双写中断，修复完成前保持 `facts_complete=false`。
规则若使用协议维度，必须声明接受 `catalog_compatible` 还是要求 `probed`；`probed` 是更强证据，既满足
`probed` 要求，也满足较弱的 `catalog_compatible` 要求，反向不成立。证据达不到要求时是事实不足。

服务档位与模型能力是两个独立维度。两个上游组可以支持完全相同的模型，但因 `pro`、`max` 等服务等级
和价格不同而必须进入不同候选组。`service_tier` 只能由管理员明确维护，或读取上游明确返回的结构化
字段；禁止从渠道名、上游分组名、Key 名或实时倍率推断。实时输入、输出和缓存价格只能在规则预览中
辅助核对，不能成为自动身份匹配条件。规则要求档位而事实未设置时必须返回 `service_tier_missing` 并
保持待处理，不能回退到普通档位规则。

删除分组时必须在同一事务中标记 `present=false`、清除完整状态、撤销本代全部 live membership claims、
把该分组所有旧 generation 的 pending added change 标记 `superseded` 并写原因审计，再写 removed change。
同名分组重现时递增 `presence_generation`，先保持事实不完整；只有带本代 generation 的成功模型同步
才能完成事实并触发判定。事实、能力、added change 和审计均绑定 generation，旧代事实永不复用。
真实网关请求对模型支持、上下文容量等异步返回的能力证据，也必须携带候选选中时捕获的 generation；
同一约束适用于成功触碰、路由故障计数/冷却、401/403 Key 失效和模型同步结果。分组在请求飞行期间
被删除或重建时，迟到响应不得写入新代或已删除分组，也不得清除新代的故障状态、让新 Key 失效，或把
旧请求的探测结果计入新代。状态触碰缓存的键必须包含 generation，旧代缓存不能抑制新代第一次成功回写。
受管 Key 在仍存在的同一分组内删除和重建不会改变 generation。

每次由 `present=false` 变回 `present=true` 都必须为新 generation 创建一条新的 added change，即使上一代
最后一条 change 也是 added / ignored。旧代 assignment override 不得终结或归组这条新 change；必须由
本代新的人工操作或本代完整事实上的自动规则重新决定，不能跳过新事件或复用上一代行。

#### 统一采集与信号边界

所有上游分组事实必须由统一采集器获取、规范化并写入上述稳定事实快照。受管 Key 协调器、模型探测器、
自动归组、人工归组和管理端投影都只能消费这一读取面，禁止各模块自行请求上游分组后维护另一套当前状态。
上游支持推送时，推送事件只触发统一采集器刷新；不支持推送时，由定时轮询触发同一个采集入口。服务启动
恢复、人工刷新和用户写操作前的强制刷新也必须复用该入口，不能实现各自不同的发现和规范化逻辑。

事件只表示“对应渠道的事实可能已经变化”，用于失效已有投影和排入刷新任务；事件载荷及其历史状态不能
直接参与归组、Key 维护或模型探测决策。所有消费者收到信号后必须重新读取统一采集器持久化的最新快照及其
完整性状态，并以快照绑定的 channel identity revision、`presence_generation` 和事实版本作决定。重复、乱序或迟到事件
必须幂等；它们不能把旧状态重新写成当前状态，也不能让下游把某次事件结果永久锁死。

受管 Key 是维持上游分组可管理性的控制面资源。低余额、账户余额暂不可用或余额采集失败不能阻止受管 Key
的盘点、创建、恢复、更新和删除，也不能让分组管理失去结构凭据。低余额同样不能停止模型能力检测；延迟方面
不对健康分组做周期探测，真实请求只被动记录延迟，只有仍处于当前精确 generation 冷却阶段的分组才执行恢复探测。
恢复探测返回 `402` 时只更新统一余额事实，不得把分组或 Key 标成故障、写入组级冷却或中止同轮其它恢复目标。
余额只在最终路由调度门禁生效，余额恢复后自动重新允许调度。

### 候选分组规则

规则属于候选分组，因为候选分组才是上游成员关系的目标。新增 `gateway_candidate_group_auto_rules`：

| 字段 | 语义 |
| --- | --- |
| `id` / `candidate_group_id` | 规则和目标候选分组 |
| `enabled` | 是否参与自动判定 |
| `priority` | 显式优先级，数值越大越优先 |
| `model_families` | 可接受的模型家族集合 |
| `family_match_mode` | `single_any`、`exact_set` 或 `contains_all` |
| `gateway_apps` | 可接受的网关协议集合 |
| `app_match_mode` | `exact_set` 或 `contains_all` |
| `channel_types` | 可接受的渠道类型集合 |
| `service_tiers` | 可接受的结构化服务档位集合 |
| `model_id_conditions` | 模型 ID 精确或前缀条件，不允许任意正则 |
| `capability_conditions` | 仅使用已经可靠探测并持久化的能力条件 |
| `rule_version` | 乐观并发和审计使用的递增版本 |
| `created_at` / `updated_at` | 审计时间 |

同一条规则的不同维度按 AND 匹配。同一候选分组允许多条规则，表示 OR。空维度表示不限制，
但不允许保存所有匹配维度都为空的规则。集合维度不能只写含糊的 ANY：

- `single_any` 是家族规则的安全默认值：上游事实必须只有一个家族，且该家族在规则允许集合内。
- `exact_set` 要求事实集合与规则集合完全一致，适合明确的混合模型池。
- `contains_all` 要求事实至少包含规则列出的全部值，必须由管理员显式选择，预览中显示额外家族或协议。
- `service_tiers` 与模型家族按 AND 匹配，用于把相同模型集合的 `standard`、`pro`、`max` 分到不同目标；
  规则要求档位但事实为空时是事实不足，不得根据名称或价格猜测。
- 模型 ID 条件逐条声明 `exact` 或 `prefix`；`condition_quantifier` 表示每条条件都要命中还是任一命中，
  `fact_coverage` 表示是否要求全部事实模型都被条件覆盖。匹配前只做首尾空白清理，不做大小写或
  名称语义猜测。
- 能力条件只支持结构化字段，例如 `min_context_window_tokens`、`min_safe_input_tokens` 和已经落库的
  布尔能力。每个条件必须带模型 selector：使用模型 ID 条件的命中集合，或显式 family / exact /
  prefix selector；同时声明 `any_model` / `all_models`、证据来源和最大 freshness。只有当前 generation、
  `supported=true`、无错误且覆盖 selector 目标集合的能力记录可用；覆盖不足或过期是
  `facts_incomplete`，不是“不匹配”。

每条可自动执行的规则必须至少有一个模型身份维度：家族条件，或 `fact_coverage=all` 的模型 ID 条件。
协议、渠道类型和通用能力只能收窄身份规则，不能单独触发归组。所有事实模型必须恰好分类到一个
稳定家族；`unclassified_model_ids` 或 `ambiguous_model_ids` 任一非空时，第一阶段一律
`facts_incomplete`，`other` 不能作为可自动匹配家族。`contains_all` 也不能绕过未知模型门禁。

新增单行 `gateway_candidate_group_ruleset_state` 保存全局 `ruleset_version`。任何规则新增、删除、
启停、优先级或条件修改都在同一事务中递增它，不能仅靠逐规则版本判断规则集未变化。
更新已有规则必须同时提交非零 `rule_version` 和当前 `ruleset_version`；缺少任一版本都拒绝写入，
版本不匹配返回冲突并要求重新加载，不能把零值当成绕过逐规则 CAS 的兼容模式。

`gateway_candidate_groups` 增加 `membership_version`。任何人工、规则、override 或清理流程引起的成员
claim / 物化成员增删都在同一事务中递增它；预览 token 绑定所有目标组的该版本，执行期逐行锁定，
并发成员变化必须返回 409。

`model_mapping_family` 只表示雷达映射用途，不能复用为自动归组条件。自动归组需要支持一个候选分组的多家族、多协议规则。

### 模型家族与 Key 分组属性

模型家族使用后端权威、带版本的代码级元数据目录，至少包含：

- 稳定家族代码和显示名称
- 历史别名到稳定代码的映射
- 兼容的网关协议
- 模型 ID 分类器
- 官方图标标识和品牌颜色

分类器只读取上游返回的模型 ID。渠道名、上游分组名和 Key 名不进入分类器。目录不允许在管理端
随意编辑，避免同一事实在不同时间被静默改类；`classification_version` 使用目录规范内容的稳定哈希，
每次目录变更后重算待处理项。前端从同一目录生成或读取展示元数据，不能再维护一份独立分类器。

Key 分组是下游 Key 的组织边界，不直接匹配上游。新增 `gateway_key_group_route_policies`：

| 字段 | 语义 |
| --- | --- |
| `key_group_id` | 所属 Key 分组 |
| `gateway_app` / `model_family` | 结构化协议与稳定家族代码 |
| `default_candidate_group_id` | 默认候选分组 |
| `target_mode` | `candidate_group` 或显式 `all_candidates` |
| `enabled` / `policy_version` | 启用状态与并发版本 |
| `created_at` / `updated_at` | 审计时间 |

以 `(key_group_id, gateway_app, model_family)` 建唯一约束。它只用于：

- 新建 Key 或把 Key 移入分组时提供默认候选分组；
- 校验 Key 的协议和模型家族是否符合目录契约；
- Key 已有明确候选分组时不静默覆盖。

这样一个 Key 分组可以分别声明 Claude、Codex、Gemini 的默认候选池，又不会参与上游归组竞争。
创建 Key 时未显式选择候选分组则自动填入策略默认值；移动已有 Key 时仅在其候选分组为空时自动
填入，否则只展示差异并要求人工确认。`candidate_group_id=0` 不能隐式表示“没找到策略所以使用全部”；
若管理员没有显式选择且不存在唯一启用策略，保存必须失败并保持待确认。确实需要全部候选时，
必须由策略显式选择 `all_candidates`。策略 app / family 使用规范代码并验证兼容性；创建或移动 Key
与读取 `policy_version` 在同一事务中完成。候选分组外键使用 RESTRICT，Key 分组删除时策略 CASCADE。
策略表增加 CHECK：`candidate_group` 模式下目标外键必须非空，`all_candidates` 模式下目标外键必须为空。

下游 Key 同时持久化 `candidate_target_mode`（`candidate_group` / `all_candidates` / `unset`）、
`candidate_target_source`（`manual` / `key_group_policy` / `legacy`）和应用时的 `route_policy_id/version`。
`candidate_group_id=0` 不再独自承担语义；只有 target mode 为 `all_candidates` 时才表示显式全部候选，
`unset` 必须继续待确认。这样后续移动或编辑能区分用户明确选择与缺省值，不能覆盖显式 all。
策略版本快照只在本次默认值应用落库时作为一次性 CAS 校验，不是 Key 的持续约束。策略后续修改或
删除不能改写已经初始化的 Key，也不能阻止 Key 改名、启停等无关编辑；只有管理员再次应用策略时才
读取并校验新版本。

Key 分组策略是默认选择，不承诺候选分组内每个成员都符合 app / family。自动规则约束自动加入，
手工成员仍可能不同，实际请求继续由现有路由逐项过滤。若以后需要强契约，应另建 candidate-group
route contract 并在所有成员写入口校验，不能从自动规则反向推导。

候选分组本身不重复保存一套“唯一模型家族”字段。它通过多条自动规则表达可接收的上游事实，
通过 Key 分组策略表达下游默认入口；这两种属性用途不同，不能合并成一个名称或单值标签。

自动归组和人工指派使用不同门禁。自动规则必须等待当前 `presence_generation` 的模型同步成功，
并且只读取这一代完整、无冲突的结构化事实；未同步或同步失败时只能保持 `awaiting_facts`。
管理员从当前 generation 的待处理变更显式选择候选分组时，可以在模型事实未完成、后台探测关闭或
同步失败时人工加入，结果以 `manual_override_incomplete_facts` 持久化并审计。人工路径仍必须校验
稳定分组仍 present、change 与 identity 属于同一 generation、目标候选分组存在且成员所有权事务一致；
它不能借“人工”绕过 generation 变化、目标删除、Key 缺失或其它结构完整性门禁。

## 判定顺序

1. 对新增变更和稳定分组加 PostgreSQL 行锁，确认 change 仍 pending、分组 present，且 change、事实和
   能力属于同一 `presence_generation`。
2. 查找同时匹配当前 identity 和 `presence_generation` 的持久人工例外。`ignore` 和 `assign` 只在创建它们
   的本代内可应用；
   `assign` 不要求本代模型同步成功，但必须明确记录它绕过了哪些尚未完成的自动分类或能力事实。
3. 若存在 assign override，锁定全部目标并执行现有候选成员安全校验后整体应用；任一目标缺失则整体
   conflict，不能部分加入。绕过不完整事实时审计动作标记 `manual_override_incomplete_facts`。
4. 没有 assign override 时，检查本规则实际需要的事实维度是否完整，同时执行全局未分类、歧义和
   identity conflict 门禁；这些门禁只阻止自动规则，不让人工处理成为不可达状态。
5. 读取同一 `ruleset_version` 下的全部启用规则，把每条命中的具体维度和事实写入判定结果。
6. 每个目标候选分组只保留最高优先级命中，再比较不同目标。可能命中但事实不足的规则，仅在没有
   完整命中，或其优先级不低于当前最高完整命中时阻塞判定；低优先级事实不足不能阻塞已经确定的
   高优先级结果，但仍保留在预览明细中。
7. 没有命中时标记 `unmatched`，保持待处理。
8. 最高优先级只对应一个目标时自动加入，并标记 `assigned`。
9. 最高优先级对应多个目标时标记 `conflict`，保持待处理。规则更具体不构成隐式胜出条件，必须由显式优先级或人工选择解决。

自动归组使用数据库事务和候选成员唯一约束保证幂等。Redis 不参与规则、例外、判定或审计的权威状态。

`gateway_upstream_group_changes` 增加 `upstream_group_id` 和 `presence_generation`。自动归组新增的
assignment 字段和约束只适用于 `kind='added'`：另加不会改变待处理可见性的 `assignment_state`、
`assignment_reason_code`、`suggested_candidate_group_ids`、`evaluated_at`、`evaluation_token` 和
`facts_digest`。pending 时 assignment state 只能是 `awaiting_facts`、`unmatched`、`conflict` 或
`ready`；added 终态只能与 change status 的 assigned / ignored / superseded 对应。建议目标 ID 按升序
去重，摘要使用版本化 canonical JSON 的 SHA-256。成员 claim / 物化成员写入、change 终态和不可变
审计必须同事务提交或回滚。

现有 `kind='removed'` 和 managed-key removal 生命周期保持不变：普通 removed 可以直接
`acknowledged`；受管 Key 删除仍使用 `processing -> acknowledged|ignored` 和 `removal_claim_key`。
这些行的 assignment 字段为 NULL，不受 added 的 CHECK 约束。第一阶段不复用或破坏现有删除审计状态机。

### 成员来源与撤销

新增 `gateway_candidate_group_membership_claims`，唯一键为
`(candidate_group_id, upstream_group_id, presence_generation, source_kind, source_id)`：

- `source_kind` 为 `manual`、`auto_rule` 或 `override`，`source_id` 指向规则、override 或人工操作审计；
- 物化的 `gateway_candidate_group_members` 只要至少有一个有效 claim 就保留；
- 自动执行遇到已有人工成员时只增加自己的 claim，不假装创建了该成员；
- 唯一目标被选中时，只为实际促成本次决策的规则保存 claim：规则目标是唯一全局最高目标，且规则
  优先级等于本次全局最高优先级；同目标同优先级并列规则可分别持有 claim，低优先级命中只写审计；
- 撤销规则或 override 默认只阻止未来应用。管理员明确按完整成员列表排除某个 current-generation 成员时，
  保存 generation-bound negative override，并撤销该成员在本代的全部 live claims，避免 UI 报告成功后规则
  claim 又把成员留在 storage；底层按来源撤销仍只删除指定来源的 claims；
- 删除最后一个 claim 才删除物化成员。人工确认自动成员时新增 manual claim，之后撤销自动 claim 不会误删；
- 保存或仅重命名候选分组时按稳定 `upstream_group_id` 识别既有成员；完整成员列表中的既有自动成员
  不得因此新增 manual claim 或持久 assign override，只有真正新增的稳定身份才视为人工加入；
- 每次 claim 和物化成员变化都递增候选组 `membership_version` 并写审计。

这样“可撤销”有稳定所有权：普通规则、override 和人工来源撤销只处理自己持有的理由；管理员在候选
分组完整列表中明确排除成员则是独立的本代负向决定，会覆盖本代其它来源。分组上游删除属于实体不存在
清理，会撤销该实体当前 generation 的全部 live claims 并删除物化成员；历史审计和旧代 override 保留，
但不能为重现后的新 generation 生成 claim。

## 本代人工例外

新增 `gateway_upstream_group_assignment_overrides`，按 `upstream_group_id` 唯一保存：

- `presence_generation`: 创建人工决定时的分组代次；`0` 是失效的 legacy 标记
- `action`: `assign` 或 `ignore`
- `reason`、`created_by`、`created_at`、`updated_at`

assign 目标使用子表 `gateway_upstream_group_assignment_override_targets` 保存：同时保留不可变
`candidate_group_id_snapshot` 和可空候选分组外键，目标删除时外键 `ON DELETE SET NULL`。这样既能锁定
全部现存目标，又能保留“原目标已经删除”的冲突证据，禁止只应用仍存在的部分目标。override 有独立
递增 `override_version`。

上游删除分组时仍按现有逻辑清除候选成员，但保留例外和审计。相同渠道的相同上游分组重新出现时，
旧代 ignore 和 assign 例外都不重放；新 added change 必须等待本代人工确认或本代自动事实。管理员在
新代再次人工处理时可以把同一 override 行显式重绑到当前 generation，并递增 `override_version`。目标
候选分组已经删除时不得猜测改投其它分组；旧目标快照只保留审计和冲突说明用途。

## 预览与审计

规则编辑器必须先提供只读预览，展示当前上游分组的以下结果：

- 唯一命中目标、规则和逐项命中原因
- 同优先级冲突目标
- 缺少的事实
- 未命中原因
- 人工例外将覆盖的规则结果

保存规则不自动重写历史成员。用户需要明确执行“应用到当前待处理项”，才能批量处理既有变更。
开启“同步后自动执行”只影响开关开启后发生的真实同步和事实更新：这些事件完成重算后可以自动应用
新的 ready 项。开关不会立即执行开启前已经存在的 ready 项；启动恢复扫描也只重算，不执行。既有
ready 项仍须由管理员显式点击“应用就绪项”。

规则草案预览与已保存规则的判定预览是两个接口：草案预览只返回差异，不能直接执行；保存规则使用
ruleset CAS。已保存规则预览由服务器签发 opaque `evaluation_token`，绑定 change status / kind /
generation、facts version / digest / classification version、全局 ruleset version、override version、
全部目标候选分组存在性及 `membership_version`。执行事务重新锁定并计算这些输入；任一变化返回
HTTP 409 并要求重新预览，不能把旧预览结果套到新同步事实上。
批量执行必须在每一项提交前单独重新预览并取得最新 token；前一项可能递增共同目标候选组的
`membership_version`，不能复用批次开始时为后续项签发的旧 token。管理员批量应用时还必须确认刷新后
的状态、目标和判定理由与已核对结果一致，否则停止执行并要求重新核对。

用户执行“加入候选分组”时，服务端必须在成员写入前按 `channel_id` 对所选变更去重，每个渠道只调用一次
统一采集入口强制刷新上游事实。刷新成功后必须重新读取该渠道的当前变更和事实快照，再生成或校验本次执行
输入，不能继续使用列表加载时的历史投影。某渠道刷新失败时，该渠道的全部所选加入操作 fail closed 并返回
同一条可追溯原因；其它渠道仍独立刷新和执行，不能因一个渠道失败而阻断整个批次。忽略操作不产生成员关系，
不需要为了执行而访问上游。

写事务仍是最终并发边界：它必须锁定当前渠道、稳定分组和目标候选分组，复核稳定分组仍 present、change
与当前 `presence_generation` 一致，并且当前 generation 仍有结构可用的受管 Key。任一复核失败都返回冲突
或结构不可用，禁止通过强制刷新、人工 override 或批处理绕过，也禁止用刷新前的结果部分提交。后续若把
刷新结果拆成可跨请求执行的预览 token，该 token 还必须绑定 channel identity revision 和目标候选分组的
`membership_version`，并在写事务内做 CAS；当前刷新与写入必须保持在同一个服务端请求中。

新增不可变审计表 `gateway_upstream_group_assignment_audits`，至少保存变更 ID、稳定分组 ID、raw / canonical
名称、presence generation、classification / facts / ruleset / override 版本、动作、触发来源、命中规则、
目标、事实快照、解释、操作者和时间。触发来源区分 `sync`、`rule_recheck`、`manual_assign`、
`manual_ignore` 和 `override_reapply`。

## 同步与恢复

自动判定发生在模型同步成功并持久化模型家族和模型 ID 之后，不能在只发现倍率分组时提前判定。为处理进程崩溃，后台任务在以下时机幂等重算待处理项：

- 渠道模型同步成功后；
- 自动规则保存或启用后；
- 人工点击重新判定时；
- 服务启动后立即执行的恢复扫描；
- 服务存活期间每 5 分钟执行一次的周期恢复扫描。

首次模型同步失败也必须幂等创建本代 `pending added / awaiting_facts` 变更；后续重试成功只能补全同一条
变更，不能重复创建。同步失败只更新事实错误和待处理原因，不应把候选加入冷却，也不能把待处理项误标成已忽略。
恢复扫描必须先通过统一采集入口取得并持久化当前上游事实快照，再用该快照核对 `present` 和 generation，
然后处理 pending；不能只相信旧 facts 行，也不能在扫描器内另写一套上游发现逻辑。
扫描依次修复 presence、facts 和持久化 preview，单实例内同一时刻只允许一轮运行。扫描只重算不执行，
即使已经开启自动执行，也不能把开关开启前遗留的 pending 在后台静默应用。服务关闭时必须取消正在执行的
数据库查询并等待扫描任务退出。

## 上线迁移

PostgreSQL migration 和后台回填分两阶段执行，未完成回填前自动归组总开关保持关闭：

1. 从当前倍率快照、upstream key、候选成员和 change 中按现有 lower canonical 建立稳定 upstream group
   身份；同一规范键的 raw 变体事实等价时合并并保留变体，事实冲突时标记 `identity_conflict`。
2. 当前仍存在的分组初始化 `presence_generation=1`，但只有模型来源和事实摘要核对成功后才设置
   `facts_complete`；已消失实体保留 tombstone。
3. 当前仍 present 的既有候选成员回填一个本代 `manual` claim，并写入专用 migration audit；所有既有
   候选成员都回填持久 assign override 和原候选分组目标快照。后续迁移把这些无法证明操作代次的 legacy
   override 标记为 `presence_generation=0` 并 fail-closed；已消失成员不保留活跃 claim 或物化成员，同名
   分组重现后必须重新人工确认或按本代事实自动归组。
4. 既有 Key 的 `candidate_group_id>0` 回填为 `candidate_group/legacy`；值为 0 的既有 Key 为保持当前路由
   行为回填为 `all_candidates/legacy` 并在管理端提示确认。新建或编辑不能再生成含糊 legacy 状态。
5. 旧 pending added 只在能绑定当前 present generation 时迁入；已删除、身份冲突或属于旧代的行标记
   `superseded` 并写迁移审计。removed / managed-key removal 行原样保留现有状态。
6. 能力记录仅在能绑定稳定分组、当前 generation 和明确模型 ID 时进入 capability profile；其余记录
   保留原审计用途但不能满足自动规则。
7. 回填完成后先运行只读预览并核对计数、冲突和 claims，再由管理员显式开启自动执行。

`000013_gateway_automatic_group_assignment.sql` 是只向前执行的不可变迁移。生产库一旦记录其 checksum，
后续版本不得编辑该文件；任何修正都必须新建更高版本迁移，否则启动时 checksum 校验会拒绝继续。
该迁移没有降级脚本：回滚必须停服并恢复应用迁移前的 PostgreSQL 全库备份。迁移后的
`gateway_candidate_group_members.upstream_group_id` 为必填字段，旧版二进制不兼容新 schema，不能仅
通过回退程序版本完成回滚。

## 第一阶段验收

- 名称完全不同但事实相同的两个上游分组得到相同匹配结果。
- 名称看似相同但协议或模型家族不同的分组不能误匹配。
- 支持相同模型集合的 `pro` 和 `max` 分组按结构化服务档位进入不同候选组；未设置档位时保持待处理。
- 修改服务档位会递增事实版本并使旧预览 token 返回 409；实时价格变化不改变服务档位身份。
- 唯一最高优先级自动加入；同优先级跨目标保持冲突。
- 模型事实缺失时保持待处理，同步补齐后可自动重算。
- 人工加入和忽略在同一 generation 内跨重启有效；删除并重新出现后必须按新代重新确认。
- 规则预览与实际执行使用同一判定函数，结果一致。
- 每次自动或人工决定都能从审计记录还原当时事实和原因。
- 单家族安全默认规则不能接收混合家族分组；只有显式 `exact_set` / `contains_all` 规则可以处理混合池。
- Key 分组按协议和家族填入默认候选分组，但不会覆盖已有人工候选分组。
- 分类目录版本变化后，待处理项使用新版本重算，历史审计仍保留旧版本和事实摘要。
- 删除后事实立即不可用；同名重现完成本代模型同步前不能使用旧代事实，Key 重建则保持同一代。
- 同次同步的规范名变体事实等价时稳定合并，事实冲突时进入 `identity_conflict`，不静默覆盖。
- 预览后新增同优先级规则或修改 override 会使旧 token 执行返回 409。
- 未分类、命中多个家族、能力覆盖不全或能力证据过期都不能自动归组。
- 无 Key 分组策略且未显式选候选池时拒绝保存，不静默扩大到全部候选。
- 删除分组会 supersede 旧代 pending added；同名重现始终生成新代 added，旧代 ignore override 不应用。
- 自动、override 和人工 claim 可分别撤销，最后一个 claim 消失前不删除候选成员。
- 未同步、同步失败或包含未知模型的 current-generation 分组可由显式人工 assign 处理，并审计其绕过的
  不完整事实；相同输入在自动规则路径中仍保持 `awaiting_facts`。
- Key 上可区分显式全部候选、具体候选分组和未选择，后续编辑不覆盖明确选择。
- 推送事件和轮询都只触发同一采集入口；决策只读取最新持久化快照，不直接消费事件载荷或历史投影。
- 批量人工加入按渠道各刷新一次；单渠道刷新失败只拒绝该渠道，不能阻断其它渠道。
- 低余额或余额暂不可用时受管 Key 和模型能力检测仍继续；延迟恢复探测只处理当前代次的冷却分组，`402` 只更新余额事实，余额只影响最终路由调度。
- 写入前 channel identity revision、presence generation 或当前可用 Key 任一变化都会拒绝旧操作。
