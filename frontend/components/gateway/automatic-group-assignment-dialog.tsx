import { useMemo, useState, type ReactNode } from "react"
import { Check, GitBranch, Loader2, Pencil, Play, Plus, RefreshCw, Search, Trash2, TriangleAlert, X } from "lucide-react"
import { toast } from "sonner"
import { apiFetch } from "@/lib/api"
import type {
  GatewayAutomaticAssignmentConfig,
  GatewayAutomaticAssignmentPreview,
  GatewayAutomaticAssignmentRule,
  GatewayCapabilityCondition,
  GatewayCandidateGroup,
  GatewayModelIDCondition,
  GatewayRouteCandidate,
} from "@/lib/api-types"
import {
  automaticAssignmentOutcomeChanged,
  automaticAssignmentReasonLabel,
  capabilityConditionDraftError,
  capabilityConditionFromDraft,
  capabilityConditionToDraft,
  mergeServiceTierEdits,
  parseSelectorValues,
  selectableAutomaticModelFamilies,
  selectorTextForTypeChange,
  type GatewayCapabilityConditionDraft,
} from "@/lib/automatic-group-assignment"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"

const gatewayApps = ["codex", "claude", "gemini"]
const channelTypes = ["newapi", "sub2api"]
const appSources = ["catalog_compatible", "probed"]

type RuleDraft = {
  id?: number
  ruleVersion?: number
  candidateGroupID: number
  enabled: boolean
  priority: number
  modelFamilies: string[]
  familyMatchMode: GatewayAutomaticAssignmentRule["family_match_mode"]
  gatewayApps: string[]
  appMatchMode: GatewayAutomaticAssignmentRule["app_match_mode"]
  acceptedAppSources: string[]
  channelTypes: string[]
  serviceTiers: string
  modelIDConditions: GatewayModelIDCondition[]
  modelConditionQuantifier: GatewayAutomaticAssignmentRule["model_condition_quantifier"]
  modelFactCoverage: GatewayAutomaticAssignmentRule["model_fact_coverage"]
  capabilityConditions: GatewayCapabilityConditionDraft[]
}

export function AutomaticGroupAssignmentDialog({ groups, candidates, onChanged, disabled = false }: {
  disabled?: boolean
  groups: GatewayCandidateGroup[]
  candidates: GatewayRouteCandidate[]
  onChanged: () => void
}) {
  const [open, setOpen] = useState(false)
  const [tab, setTab] = useState("rules")
  const [config, setConfig] = useState<GatewayAutomaticAssignmentConfig | null>(null)
  const [previews, setPreviews] = useState<GatewayAutomaticAssignmentPreview[]>([])
  const [draftPreviews, setDraftPreviews] = useState<GatewayAutomaticAssignmentPreview[] | null>(null)
  const [draft, setDraft] = useState<RuleDraft>(() => emptyDraft(groups[0]?.id ?? 0))
  const [tierEdits, setTierEdits] = useState<Record<number, string>>({})
  const [search, setSearch] = useState("")
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)

  const candidateByIdentity = useMemo(() => {
    const map = new Map<string, GatewayRouteCandidate>()
    for (const candidate of candidates) map.set(identityKey(candidate.channel_id, candidate.group_name), candidate)
    return map
  }, [candidates])
  const normalizedSearch = search.trim().toLowerCase()
  const visibleUpstreamGroups = useMemo(() => (config?.upstream_groups ?? []).filter((group) => {
    if (!normalizedSearch) return true
    const candidate = candidateByIdentity.get(identityKey(group.channel_id, group.canonical_group_name))
    return [candidate?.channel_name, group.canonical_group_name, group.model_family_codes.join(" "), group.service_tier].join(" ").toLowerCase().includes(normalizedSearch)
  }), [candidateByIdentity, config?.upstream_groups, normalizedSearch])

  async function load() {
    setLoading(true)
    const previousGroups = config?.upstream_groups ?? []
    try {
      const [nextConfig, nextPreviews] = await Promise.all([
        apiFetch<GatewayAutomaticAssignmentConfig>("/gateway/automatic-assignment"),
        apiFetch<GatewayAutomaticAssignmentPreview[]>("/gateway/automatic-assignment/preview", { method: "POST", body: JSON.stringify({}) }),
      ])
      setConfig(nextConfig)
      setPreviews(nextPreviews)
      setDraftPreviews(null)
      setTierEdits((current) => mergeServiceTierEdits(previousGroups, nextConfig.upstream_groups, current))
      setDraft((current) => current.candidateGroupID ? current : emptyDraft(groups[0]?.id ?? 0))
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "加载自动归组配置失败")
    } finally {
      setLoading(false)
    }
  }

  function handleOpen(next: boolean) {
    setOpen(next)
    if (next) void load()
  }

  function editRule(rule: GatewayAutomaticAssignmentRule) {
    setDraft({
      id: rule.id,
      ruleVersion: rule.rule_version,
      candidateGroupID: rule.candidate_group_id,
      enabled: rule.enabled,
      priority: rule.priority,
      modelFamilies: rule.model_families ?? [],
      familyMatchMode: rule.family_match_mode,
      gatewayApps: rule.gateway_apps ?? [],
      appMatchMode: rule.app_match_mode,
      acceptedAppSources: rule.accepted_app_sources ?? [],
      channelTypes: rule.channel_types ?? [],
      serviceTiers: (rule.service_tiers ?? []).join(", "),
      modelIDConditions: (rule.model_id_conditions ?? []).map((condition) => ({ ...condition })),
      modelConditionQuantifier: rule.model_condition_quantifier,
      modelFactCoverage: rule.model_fact_coverage,
      capabilityConditions: (rule.capability_conditions ?? []).map(capabilityConditionToDraft),
    })
    setDraftPreviews(null)
  }

  function updateDraft(nextDraft: RuleDraft) {
    setDraft(nextDraft)
    setDraftPreviews(null)
  }

  function draftValidationError() {
    if (!draft.candidateGroupID) return "请选择目标候选分组"
    const modelConditions = draft.modelIDConditions.filter((condition) => condition.value.trim())
    if (draft.modelFamilies.length === 0 && (modelConditions.length === 0 || draft.modelFactCoverage !== "all")) {
      return "规则需要模型家族，或覆盖全部模型事实的模型 ID 条件"
    }
    const invalidTier = csvValues(draft.serviceTiers).find((tier) => !/^[a-z0-9_-]+$/.test(tier))
    if (invalidTier) return `服务档位“${invalidTier}”只能使用小写字母、数字、下划线或连字符`
    for (const condition of draft.capabilityConditions) {
      const error = capabilityConditionDraftError(condition, modelFamilies)
      if (error) return error
    }
    return ""
  }

  function rulePayload() {
    return {
      id: draft.id ?? 0,
      candidate_group_id: draft.candidateGroupID,
      enabled: draft.enabled,
      priority: draft.priority,
      model_families: draft.modelFamilies.filter((family) => modelFamilies.includes(family)),
      family_match_mode: draft.familyMatchMode,
      gateway_apps: draft.gatewayApps,
      app_match_mode: draft.appMatchMode,
      accepted_app_sources: draft.acceptedAppSources,
      channel_types: draft.channelTypes,
      service_tiers: csvValues(draft.serviceTiers),
      model_id_conditions: draft.modelIDConditions.map((condition) => ({ ...condition, value: condition.value.trim() })).filter((condition) => condition.value),
      model_condition_quantifier: draft.modelConditionQuantifier,
      model_fact_coverage: draft.modelFactCoverage,
      capability_conditions: draft.capabilityConditions.map((draftCondition) => ({
        ...capabilityConditionFromDraft(draftCondition, modelFamilies),
        max_age_hours: Number(draftCondition.max_age_hours),
        min_context_window_tokens: Number(draftCondition.min_context_window_tokens ?? 0),
        min_safe_input_tokens: Number(draftCondition.min_safe_input_tokens ?? 0),
      })),
      rule_version: draft.ruleVersion ?? 0,
    }
  }

  async function previewDraft() {
    const validationError = draftValidationError()
    if (validationError) {
      toast.error(validationError)
      return
    }
    setSaving(true)
    try {
      const result = await apiFetch<GatewayAutomaticAssignmentPreview[]>("/gateway/automatic-assignment/preview-draft", {
        method: "POST",
        body: JSON.stringify({ rule: rulePayload() }),
      })
      setDraftPreviews(result)
      toast.success(`草案已评估 ${result.length} 个待处理分组`)
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "预览规则失败")
    } finally {
      setSaving(false)
    }
  }

  async function saveRule() {
    if (!config) return
    const validationError = draftValidationError()
    if (validationError) {
      toast.error(validationError)
      return
    }
    setSaving(true)
    try {
      await apiFetch(draft.id ? `/gateway/automatic-assignment/rules/${draft.id}` : "/gateway/automatic-assignment/rules", {
        method: draft.id ? "PATCH" : "POST",
        body: JSON.stringify({ ruleset_version: config.state.ruleset_version, rule: rulePayload() }),
      })
      toast.success(draft.id ? "自动归组规则已更新" : "自动归组规则已创建")
      setDraft(emptyDraft(groups[0]?.id ?? 0))
      setDraftPreviews(null)
      await load()
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "保存规则失败")
    } finally {
      setSaving(false)
    }
  }

  async function deleteRule(rule: GatewayAutomaticAssignmentRule) {
    if (!config || !window.confirm("确定删除这条自动归组规则？")) return
    const revokeApplied = window.confirm("是否同时撤销仅由该规则持有的已应用关系？\n\n确定：撤销该规则的成员来源；取消：只删除规则，保留现有成员。")
    setSaving(true)
    try {
      await apiFetch(`/gateway/automatic-assignment/rules/${rule.id}?ruleset_version=${config.state.ruleset_version}&revoke_applied=${revokeApplied ? 1 : 0}`, { method: "DELETE" })
      if (draft.id === rule.id) setDraft(emptyDraft(groups[0]?.id ?? 0))
      await load()
      toast.success("规则已删除")
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "删除规则失败")
    } finally {
      setSaving(false)
    }
  }

  async function updateState(backfillComplete: boolean, autoExecuteEnabled: boolean) {
    if (!config) return
    setSaving(true)
    try {
      await apiFetch("/gateway/automatic-assignment/state", {
        method: "PATCH",
        body: JSON.stringify({ ruleset_version: config.state.ruleset_version, backfill_complete: backfillComplete, auto_execute_enabled: autoExecuteEnabled }),
      })
      await load()
      toast.success(autoExecuteEnabled ? "后续同步将自动执行唯一判定" : "自动归组状态已更新")
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "更新自动执行状态失败")
    } finally {
      setSaving(false)
    }
  }

  function changeAutoExecute(checked: boolean) {
    if (checked && !window.confirm("开启后，仅后续模型同步产生的唯一判定会自动加入候选组。当前待处理项仍需在正式预览中人工应用。")) return
    void updateState(config?.state.backfill_complete ?? false, checked)
  }

  async function saveTier(id: number) {
    setSaving(true)
    try {
      await apiFetch(`/gateway/upstream-groups/${id}/service-tier`, {
        method: "PATCH",
        body: JSON.stringify({ service_tier: tierEdits[id] ?? "", source: "manual" }),
      })
      await load()
      toast.success("服务档位已保存")
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "保存服务档位失败")
    } finally {
      setSaving(false)
    }
  }

  async function applyReady() {
    setSaving(true)
    try {
      const ready = previews.filter((item) => item.state === "ready" && item.evaluation_token)
      let applied = 0
      for (const item of ready) {
        const refreshed = await apiFetch<GatewayAutomaticAssignmentPreview[]>("/gateway/automatic-assignment/preview", {
          method: "POST",
          body: JSON.stringify({ change_ids: [item.change_id] }),
        })
        const current = refreshed[0]
        if (!current?.evaluation_token || current.state !== "ready" || automaticAssignmentOutcomeChanged(item, current)) {
          throw new Error(`分组 ${item.group_name} 的判定结果已变化`)
        }
        await apiFetch("/gateway/automatic-assignment/execute", {
          method: "POST",
          body: JSON.stringify({ change_id: current.change_id, evaluation_token: current.evaluation_token }),
        })
        applied++
      }
      toast.success(`已应用 ${applied} 个唯一判定，冲突和事实不足项保持待处理`)
      onChanged()
      await load()
    } catch (error) {
      toast.error(error instanceof Error ? `${error.message}；预览已刷新，请重新核对后再应用` : "预览已失效，请重新核对后再应用")
      await load()
    } finally {
      setSaving(false)
    }
  }

  const shownPreviews = draftPreviews ?? previews
  const modelFamilies = selectableAutomaticModelFamilies(config?.model_catalog)

  return (
    <Dialog open={open} onOpenChange={handleOpen}>
      <DialogTrigger asChild>
        <Button type="button" size="sm" variant="outline" disabled={disabled}><GitBranch className="size-3.5" />{"归组规则"}</Button>
      </DialogTrigger>
      <DialogContent className="grid h-[calc(100dvh-1rem)] w-[calc(100vw-1rem)] max-w-none grid-rows-[auto_minmax(0,1fr)_auto] gap-0 overflow-hidden p-0 sm:h-[min(860px,calc(100dvh-3rem))] sm:w-[min(1320px,calc(100vw-3rem))] sm:max-w-none">
        <DialogHeader className="border-b border-border px-5 py-4 pr-12">
          <div className="flex flex-wrap items-center gap-3">
            <DialogTitle>{"上游分组自动归组"}</DialogTitle>
            {config ? <Badge variant="outline">{`规则集 v${config.state.ruleset_version}`}</Badge> : null}
            {loading ? <Loader2 className="size-4 animate-spin text-muted-foreground" /> : null}
          </div>
        </DialogHeader>
        <Tabs value={tab} onValueChange={setTab} className="min-h-0 gap-0 overflow-hidden">
          <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border bg-muted/15 px-5 py-2.5">
            <TabsList><TabsTrigger value="rules">{"规则"}</TabsTrigger><TabsTrigger value="facts">{"上游事实"}</TabsTrigger><TabsTrigger value="preview">{"判定预览"}</TabsTrigger><TabsTrigger value="audits">{"审计"}</TabsTrigger></TabsList>
            {config ? (
              <div className="flex flex-wrap items-center gap-4 text-xs">
                <label className="flex items-center gap-2"><Switch checked={config.state.backfill_complete} disabled={saving || config.state.auto_execute_enabled} onCheckedChange={(checked) => void updateState(checked, false)} /><span>{"已确认初始化"}</span></label>
                <label className="flex items-center gap-2" title="只影响后续模型同步；当前待处理项仍需在正式预览中人工应用"><Switch checked={config.state.auto_execute_enabled} disabled={saving || !config.state.backfill_complete} onCheckedChange={changeAutoExecute} /><span>{"后续同步自动执行"}</span></label>
              </div>
            ) : null}
          </div>

          <TabsContent value="rules" className="min-h-0 overflow-y-auto">
            <div className="grid min-h-full lg:grid-cols-[18rem_minmax(0,1fr)]">
              <aside className="border-b border-border bg-muted/10 p-4 lg:border-b-0 lg:border-r">
                <Button className="w-full" onClick={() => updateDraft(emptyDraft(groups[0]?.id ?? 0))}><Plus className="size-4" />{"新建规则"}</Button>
                <div className="mt-3 space-y-2">
                  {(config?.rules ?? []).map((rule) => (
                    <div key={rule.id} className={`border px-3 py-2.5 ${draft.id === rule.id ? "border-brand bg-background" : "border-border bg-background/70"}`}>
                      <button className="w-full text-left" onClick={() => editRule(rule)}>
                        <div className="flex items-center justify-between gap-2"><span className="truncate text-sm font-semibold">{candidateGroupName(groups, rule.candidate_group_id)}</span><Badge variant="outline">{rule.priority}</Badge></div>
                        <div className="mt-1 truncate text-xs text-muted-foreground">{[rule.model_families.join(" / "), rule.service_tiers.join(" / ")].filter(Boolean).join(" · ") || "未配置身份"}</div>
                      </button>
                      <div className="mt-2 flex justify-end gap-1"><Button size="icon-sm" variant="ghost" onClick={() => editRule(rule)} title="编辑规则"><Pencil className="size-3.5" /></Button><Button size="icon-sm" variant="ghost" onClick={() => void deleteRule(rule)} title="删除规则"><Trash2 className="size-3.5" /></Button></div>
                    </div>
                  ))}
                </div>
              </aside>
              <section className="min-w-0 p-5">
                <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
                  <Field label="目标候选分组"><Select value={draft.candidateGroupID ? String(draft.candidateGroupID) : ""} onValueChange={(value) => updateDraft({ ...draft, candidateGroupID: Number(value) })}><SelectTrigger><SelectValue placeholder="选择分组" /></SelectTrigger><SelectContent>{groups.map((group) => <SelectItem key={group.id} value={String(group.id)}>{group.name}</SelectItem>)}</SelectContent></Select></Field>
                  <Field label="优先级"><Input type="number" value={draft.priority} onChange={(event) => updateDraft({ ...draft, priority: Number(event.target.value) })} /></Field>
                  <Field label="家族匹配"><Select value={draft.familyMatchMode} onValueChange={(value) => updateDraft({ ...draft, familyMatchMode: value as RuleDraft["familyMatchMode"] })}><SelectTrigger><SelectValue /></SelectTrigger><SelectContent><SelectItem value="single_any">{"唯一家族命中"}</SelectItem><SelectItem value="exact_set">{"家族集合完全一致"}</SelectItem><SelectItem value="contains_all">{"包含全部家族"}</SelectItem></SelectContent></Select></Field>
                  <label className="flex h-16 items-end gap-2 pb-2 text-sm"><Switch checked={draft.enabled} onCheckedChange={(enabled) => updateDraft({ ...draft, enabled })} /><span>{"启用规则"}</span></label>
                </div>
                <ChoiceSet label="模型家族" values={modelFamilies} selected={draft.modelFamilies} onChange={(modelFamilies) => updateDraft({ ...draft, modelFamilies })} />
                <details key={draft.id ?? "new-rule"} open={Boolean(draft.id)} className="mt-5 rounded-lg border border-border p-4">
                  <summary className="cursor-pointer text-sm font-medium focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">高级匹配条件<span className="ml-2 font-normal text-muted-foreground">档位、协议、模型与能力</span></summary>
                <div className="mt-5 grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
                  <Field label="服务档位"><Input value={draft.serviceTiers} onChange={(event) => updateDraft({ ...draft, serviceTiers: event.target.value })} placeholder="standard, pro, max" /></Field>
                  <Field label="协议匹配"><Select value={draft.appMatchMode} onValueChange={(value) => updateDraft({ ...draft, appMatchMode: value as RuleDraft["appMatchMode"] })}><SelectTrigger><SelectValue /></SelectTrigger><SelectContent><SelectItem value="exact_set">{"协议集合完全一致"}</SelectItem><SelectItem value="contains_all">{"包含全部所选协议"}</SelectItem></SelectContent></Select></Field>
                  <Field label="模型条件量词"><Select value={draft.modelConditionQuantifier} onValueChange={(value) => updateDraft({ ...draft, modelConditionQuantifier: value as RuleDraft["modelConditionQuantifier"] })}><SelectTrigger><SelectValue /></SelectTrigger><SelectContent><SelectItem value="any">{"任一条件"}</SelectItem><SelectItem value="all">{"全部条件"}</SelectItem></SelectContent></Select></Field>
                  <Field label="模型事实范围"><Select value={draft.modelFactCoverage} onValueChange={(value) => updateDraft({ ...draft, modelFactCoverage: value as RuleDraft["modelFactCoverage"] })}><SelectTrigger><SelectValue /></SelectTrigger><SelectContent><SelectItem value="matched">{"已匹配家族"}</SelectItem><SelectItem value="all">{"全部模型"}</SelectItem></SelectContent></Select></Field>
                </div>
                <ConditionSection title="模型 ID 条件" onAdd={() => updateDraft({ ...draft, modelIDConditions: [...draft.modelIDConditions, { match: "prefix", value: "" }] })}>
                  {draft.modelIDConditions.map((condition, index) => <div key={index} className="grid gap-2 sm:grid-cols-[8rem_minmax(0,1fr)_2rem]"><Select value={condition.match} onValueChange={(value) => updateDraft({ ...draft, modelIDConditions: draft.modelIDConditions.map((item, current) => current === index ? { ...item, match: value as GatewayModelIDCondition["match"] } : item) })}><SelectTrigger><SelectValue /></SelectTrigger><SelectContent><SelectItem value="prefix">{"前缀"}</SelectItem><SelectItem value="exact">{"精确"}</SelectItem></SelectContent></Select><Input value={condition.value} onChange={(event) => updateDraft({ ...draft, modelIDConditions: draft.modelIDConditions.map((item, current) => current === index ? { ...item, value: event.target.value } : item) })} placeholder="gpt-5" /><Button type="button" size="icon" variant="ghost" onClick={() => updateDraft({ ...draft, modelIDConditions: draft.modelIDConditions.filter((_, current) => current !== index) })} title="删除模型条件"><X className="size-4" /></Button></div>)}
                </ConditionSection>
                <ConditionSection title="能力条件" onAdd={() => updateDraft({ ...draft, capabilityConditions: [...draft.capabilityConditions, emptyCapabilityCondition(draft.modelFamilies)] })}>
                  {draft.capabilityConditions.map((condition, index) => <div key={index} className="grid gap-2 border border-border p-3 sm:grid-cols-2 xl:grid-cols-4">
						<Field label="选择范围"><Select value={condition.selector_type} onValueChange={(value) => { const selectorType = value as GatewayCapabilityCondition["selector_type"]; updateCapabilityDraft(draft, updateDraft, index, { selector_type: selectorType, selectorValuesText: selectorTextForTypeChange(condition.selectorValuesText, selectorType, modelFamilies) }) }}><SelectTrigger><SelectValue /></SelectTrigger><SelectContent><SelectItem value="family">{"指定家族"}</SelectItem><SelectItem value="exact">{"精确模型 ID"}</SelectItem><SelectItem value="prefix">{"模型 ID 前缀"}</SelectItem><SelectItem value="matched_model_ids">{"规则命中的模型"}</SelectItem></SelectContent></Select></Field>
						{condition.selector_type === "family" ? <FieldGroup label="选择值"><FamilySelector values={modelFamilies} selected={parseSelectorValues(condition.selectorValuesText)} onChange={(values) => updateCapabilityDraft(draft, updateDraft, index, { selectorValuesText: values.join(", ") })} /></FieldGroup> : <Field label="选择值"><Input value={condition.selectorValuesText} disabled={condition.selector_type === "matched_model_ids"} onChange={(event) => updateCapabilityDraft(draft, updateDraft, index, { selectorValuesText: event.target.value })} placeholder="Model-ID, vendor-prefix" /></Field>}
                    <Field label="模型量词"><Select value={condition.quantifier} onValueChange={(value) => updateCapabilityDraft(draft, updateDraft, index, { quantifier: value as GatewayCapabilityCondition["quantifier"] })}><SelectTrigger><SelectValue /></SelectTrigger><SelectContent><SelectItem value="all_models">{"全部模型"}</SelectItem><SelectItem value="any_model">{"任一模型"}</SelectItem></SelectContent></Select></Field>
                    <Field label="证据有效小时"><Input type="number" min={1} value={condition.max_age_hours} onChange={(event) => updateCapabilityDraft(draft, updateDraft, index, { max_age_hours: Number(event.target.value) })} /></Field>
                    <Field label="最小上下文 tokens"><Input type="number" min={0} value={condition.min_context_window_tokens ?? 0} onChange={(event) => updateCapabilityDraft(draft, updateDraft, index, { min_context_window_tokens: Number(event.target.value) })} /></Field>
                    <Field label="最小安全输入 tokens"><Input type="number" min={0} value={condition.min_safe_input_tokens ?? 0} onChange={(event) => updateCapabilityDraft(draft, updateDraft, index, { min_safe_input_tokens: Number(event.target.value) })} /></Field>
                    <div className="flex items-end justify-end sm:col-span-2"><Button type="button" variant="outline" onClick={() => updateDraft({ ...draft, capabilityConditions: draft.capabilityConditions.filter((_, current) => current !== index) })}><Trash2 className="size-4" />{"删除条件"}</Button></div>
                  </div>)}
                </ConditionSection>
                <ChoiceSet label="网关协议" values={gatewayApps} selected={draft.gatewayApps} onChange={(gatewayApps) => updateDraft({ ...draft, gatewayApps })} />
                <ChoiceSet label="协议证据来源" values={appSources} selected={draft.acceptedAppSources} onChange={(acceptedAppSources) => updateDraft({ ...draft, acceptedAppSources })} />
                <ChoiceSet label="渠道类型" values={channelTypes} selected={draft.channelTypes} onChange={(channelTypes) => updateDraft({ ...draft, channelTypes })} />
                </details>
                <div className="mt-5 flex flex-wrap justify-end gap-2"><Button variant="outline" disabled={saving} onClick={() => void previewDraft()}><Search className="size-4" />{"预览草案"}</Button><Button disabled={saving || !draft.candidateGroupID} onClick={() => void saveRule()}>{saving ? <Loader2 className="size-4 animate-spin" /> : <Check className="size-4" />}{draft.id ? "保存规则" : "创建规则"}</Button></div>
                {draftPreviews ? <PreviewSummary previews={draftPreviews} baseline={previews} mode="draft" groups={groups} candidates={candidates} detailed /> : null}
              </section>
            </div>
          </TabsContent>

          <TabsContent value="facts" className="min-h-0 overflow-y-auto p-5">
            <div className="relative mb-3 max-w-md"><Search className="pointer-events-none absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" /><Input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="搜索渠道、上游组、模型或档位" className="pl-8" /></div>
            <div className="min-w-[960px] border border-border">
              <div className="grid grid-cols-[minmax(11rem,1.2fr)_minmax(9rem,1fr)_8rem_10rem_8rem_14rem] border-b border-border bg-muted/30 px-3 py-2 text-xs font-medium text-muted-foreground"><span>{"渠道 / 上游组"}</span><span>{"模型家族"}</span><span>{"协议"}</span><span>{"当前倍率"}</span><span>{"事实状态"}</span><span>{"服务档位"}</span></div>
              {visibleUpstreamGroups.map((group) => {
                const candidate = candidateByIdentity.get(identityKey(group.channel_id, group.canonical_group_name))
                const tier = tierEdits[group.id] ?? ""
                const changed = tier !== (group.service_tier ?? "")
                return <div key={group.id} className="grid grid-cols-[minmax(11rem,1.2fr)_minmax(9rem,1fr)_8rem_10rem_8rem_14rem] items-center border-b border-border px-3 py-2.5 text-sm last:border-b-0">
                  <span className="min-w-0"><span className="block truncate font-medium">{candidate?.channel_name || `渠道 #${group.channel_id}`}</span><span className="block truncate text-xs text-muted-foreground">{group.canonical_group_name}</span></span>
                  <span className="truncate">{group.model_family_codes.join(" / ") || "-"}</span><span>{group.gateway_apps.join(" / ") || "-"}</span>
                  <span className="text-xs tabular-nums">{candidate ? `输入 ${formatRatio(candidate.ratio)} · 输出 ${formatRatio(candidate.completion_ratio)}` : "-"}</span>
                  <Badge variant={group.facts_complete ? "outline" : "secondary"} className="w-fit">{group.facts_complete ? "完整" : "待同步"}</Badge>
                  <span className="flex gap-2"><Input value={tier} onChange={(event) => setTierEdits({ ...tierEdits, [group.id]: event.target.value.toLowerCase() })} placeholder="standard / pro / max" className="h-8" /><Button size="icon-sm" variant={changed ? "default" : "outline"} disabled={!changed || saving} onClick={() => void saveTier(group.id)} title="保存服务档位"><Check className="size-3.5" /></Button></span>
                </div>
              })}
            </div>
          </TabsContent>

          <TabsContent value="preview" className="min-h-0 overflow-y-auto p-5"><PreviewSummary previews={shownPreviews} baseline={draftPreviews ? previews : undefined} mode={draftPreviews ? "draft" : "formal"} groups={groups} candidates={candidates} detailed /></TabsContent>
          <TabsContent value="audits" className="min-h-0 overflow-y-auto p-5">
            <div className="space-y-2">{(config?.audits ?? []).map((audit) => {
              const facts = audit.facts_snapshot ?? {}
              const explanation = audit.explanation ?? {}
              return <details key={audit.id} className="border border-border bg-background px-3 py-2.5">
                <summary className="grid cursor-pointer list-none gap-2 sm:grid-cols-[minmax(0,1fr)_10rem_12rem]">
                  <span className="min-w-0"><span className="block truncate text-sm font-medium">{audit.raw_group_name}</span><span className="block truncate text-xs text-muted-foreground">{`${assignmentActionLabel(audit.action)} · generation ${audit.presence_generation}`}</span></span>
                  <span className="text-xs text-muted-foreground">{audit.target_candidate_group_ids?.map((id) => candidateGroupName(groups, id)).join(" / ") || "无目标"}</span>
                  <span className="text-right text-xs text-muted-foreground">{new Date(audit.created_at).toLocaleString()}</span>
                </summary>
                <div className="mt-3 grid gap-3 border-t border-border pt-3 text-xs sm:grid-cols-2">
                  <AuditFacts label="判定事实" values={[`模型家族：${arrayText(facts.model_families)}`, `模型：${arrayText(facts.model_ids)}`, `协议：${arrayText(facts.gateway_apps)}`, `服务档位：${String(facts.service_tier || "未设置")}`]} />
                  <AuditFacts label="判定说明" values={[`状态：${String(explanation.state || "-")}`, `原因：${automaticAssignmentReasonLabel(String(explanation.reason_code || explanation.reason || "-"))}`, `缺少事实：${reasonArrayText(explanation.missing_facts)}`, `命中规则：${arrayText(audit.matched_rule_ids)}`, `触发来源：${audit.trigger_source} · 操作者：${audit.actor}`]} />
                  <AuditFacts label="版本依据" values={[`分类：${audit.classification_version || "-"}`, `事实：v${audit.facts_version ?? 0}`, `规则集：v${audit.ruleset_version ?? 0}`, `例外：v${audit.override_version ?? 0}`]} />
                  <AuditFacts label="来源" values={[`来源引用：${audit.source_ref ?? "-"}`, `稳定分组：#${audit.upstream_group_id}`, `规范名称：${audit.canonical_group_name}`]} />
                </div>
              </details>
            })}{!loading && (config?.audits?.length ?? 0) === 0 ? <p className="py-16 text-center text-sm text-muted-foreground">{"暂无自动归组审计"}</p> : null}</div>
          </TabsContent>
        </Tabs>
        <DialogFooter className="border-t border-border px-5 py-3">
          <Button variant="outline" onClick={() => void load()} disabled={loading || saving}><RefreshCw className={`size-4 ${loading ? "animate-spin" : ""}`} />{"重新判定"}</Button>
          <Button onClick={() => void applyReady()} disabled={saving || tab !== "preview" || draftPreviews !== null || previews.every((item) => item.state !== "ready")} title={draftPreviews !== null ? "草案预览不能执行，请保存规则并重新核对正式预览" : tab !== "preview" ? "请先在判定预览中核对结果" : undefined}><Play className="size-4" />{"应用已核对结果"}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function Field({ label, children }: { label: string; children: ReactNode }) { return <label className="space-y-1.5"><span className="text-xs font-medium text-muted-foreground">{label}</span>{children}</label> }
function FieldGroup({ label, children }: { label: string; children: ReactNode }) { return <fieldset className="space-y-1.5"><legend className="text-xs font-medium text-muted-foreground">{label}</legend>{children}</fieldset> }

function ConditionSection({ title, onAdd, children }: { title: string; onAdd: () => void; children: ReactNode }) {
  return <section className="mt-5 space-y-2"><div className="flex items-center justify-between gap-3"><h3 className="text-xs font-medium text-muted-foreground">{title}</h3><Button type="button" size="sm" variant="outline" onClick={onAdd}><Plus className="size-3.5" />{"添加条件"}</Button></div>{children}</section>
}

function ChoiceSet({ label, values, selected, onChange }: { label: string; values: string[]; selected: string[]; onChange: (values: string[]) => void }) {
  return <fieldset className="mt-5"><legend className="mb-2 text-xs font-medium text-muted-foreground">{label}</legend><div className="flex flex-wrap gap-x-4 gap-y-2">{values.map((value) => <label key={value} className="flex cursor-pointer items-center gap-2 text-sm"><Checkbox checked={selected.includes(value)} onCheckedChange={(checked) => onChange(checked ? [...selected, value] : selected.filter((item) => item !== value))} /><span>{value}</span></label>)}</div></fieldset>
}

function FamilySelector({ values, selected, onChange }: { values: string[]; selected: string[]; onChange: (values: string[]) => void }) {
  return <div className="flex min-h-9 flex-wrap items-center gap-x-3 gap-y-2 rounded-md border border-input px-3 py-2">{values.map((value) => <label key={value} className="flex cursor-pointer items-center gap-1.5 text-xs"><Checkbox checked={selected.includes(value)} onCheckedChange={(checked) => onChange(checked ? [...selected, value] : selected.filter((item) => item !== value))} /><span>{value}</span></label>)}</div>
}

function PreviewSummary({ previews, baseline, mode = "formal", groups, candidates, detailed = false }: {
  previews: GatewayAutomaticAssignmentPreview[]
  baseline?: GatewayAutomaticAssignmentPreview[]
  mode?: "formal" | "draft"
  groups: GatewayCandidateGroup[]
  candidates: GatewayRouteCandidate[]
  detailed?: boolean
}) {
  const counts = previews.reduce<Record<string, number>>((result, item) => ({ ...result, [item.state]: (result[item.state] ?? 0) + 1 }), {})
  const baselineByChangeID = new Map((baseline ?? []).map((item) => [item.change_id, item]))
  const changedCount = mode === "draft" ? previews.filter((item) => automaticAssignmentOutcomeChanged(item, baselineByChangeID.get(item.change_id))).length : 0
  return <div className="mt-5 border-t border-border pt-4">
    <div className="flex flex-wrap gap-2">
      <Badge variant={mode === "draft" ? "secondary" : "outline"}>{mode === "draft" ? "草案预览 · 不可执行" : "正式预览 · 可执行"}</Badge>
      {mode === "draft" ? <Badge variant="outline">{`最终结果变化 ${changedCount}`}</Badge> : null}
      <Badge variant="outline">{`可应用 ${counts.ready ?? 0}`}</Badge><Badge variant="outline">{`冲突 ${counts.conflict ?? 0}`}</Badge><Badge variant="outline">{`事实不足 ${counts.awaiting_facts ?? 0}`}</Badge><Badge variant="outline">{`未命中 ${counts.unmatched ?? 0}`}</Badge>{counts.superseded ? <Badge variant="outline">{`已失效 ${counts.superseded}`}</Badge> : null}
    </div>
    {detailed ? <div className="mt-3 space-y-2">{previews.map((item) => {
      const candidate = candidates.find((current) => current.channel_id === item.channel_id && identityKey(current.channel_id, current.group_name) === identityKey(item.channel_id, item.group_name))
      const changed = mode === "draft" && automaticAssignmentOutcomeChanged(item, baselineByChangeID.get(item.change_id))
      const summary = item.state === "ready"
        ? item.suggested_candidate_group_ids.map((id) => candidateGroupName(groups, id)).join(" / ")
        : reasonArrayText([...item.missing_facts, item.reason_code].filter(Boolean))
      return <details key={item.change_id} className="border border-border bg-background px-3 py-2.5"><summary className="grid cursor-pointer list-none gap-2 sm:grid-cols-[minmax(0,1fr)_8rem_minmax(12rem,1fr)]"><span className="min-w-0"><span className="flex min-w-0 items-center gap-2"><span className="truncate text-sm font-medium">{item.group_name}</span>{changed ? <Badge variant="secondary" className="shrink-0">{"有变化"}</Badge> : null}</span><span className="block truncate text-xs text-muted-foreground">{`${item.model_family_codes.join(" / ") || "-"} · ${item.service_tier || "未设置档位"}`}</span></span><AssignmentState state={item.state} /><span className="text-xs text-muted-foreground">{summary || "-"}</span></summary><div className="mt-3 grid gap-3 border-t border-border pt-3 text-xs sm:grid-cols-2"><AuditFacts label="判定事实" values={[`模型：${arrayText(item.model_ids)}`, `协议：${arrayText(item.gateway_apps)}`, `渠道类型：${item.channel_type || "-"}`, `服务档位：${item.service_tier || "未设置"} · ${serviceTierSourceLabel(item.service_tier_source)}`]} /><AuditFacts label="价格核对（不参与规则判定）" values={[candidate ? `输入 ${formatRatio(candidate.ratio)} · 输出 ${formatRatio(candidate.completion_ratio)}` : "当前无倍率快照"]} /><AuditFacts label="最终判定" values={[`原因：${automaticAssignmentReasonLabel(item.reason_code || "-")}`, `缺少事实：${reasonArrayText(item.missing_facts)}`, `目标：${item.suggested_candidate_group_ids.map((id) => candidateGroupName(groups, id)).join(" / ") || "-"}`]} />{(item.rule_matches ?? []).map((match) => <AuditFacts key={`${item.change_id}-${match.rule_id}`} label={`规则 #${match.rule_id || "草案"} · 优先级 ${match.priority} · ${match.matched ? "命中" : match.facts_incomplete ? "事实不足" : "未命中"}`} values={[`目标：${candidateGroupName(groups, match.candidate_group_id)}`, `判定：${match.reasons.map(automaticAssignmentReasonLabel).join(" · ") || "-"}`, `缺少：${match.missing_facts.map(automaticAssignmentReasonLabel).join(" · ") || "-"}`]} />)}</div></details>
    })}</div> : null}
  </div>
}

function AssignmentState({ state }: { state: GatewayAutomaticAssignmentPreview["state"] }) {
  if (state === "ready") return <Badge className="w-fit bg-success text-success-foreground">{"可应用"}</Badge>
  if (state === "conflict") return <Badge className="w-fit bg-warning text-warning-foreground"><TriangleAlert className="size-3" />{"冲突"}</Badge>
  if (state === "awaiting_facts") return <Badge variant="secondary" className="w-fit">{"事实不足"}</Badge>
  if (state === "superseded") return <Badge variant="secondary" className="w-fit">{"已失效"}</Badge>
  return <Badge variant="outline" className="w-fit">{"未命中"}</Badge>
}

function AuditFacts({ label, values }: { label: string; values: string[] }) { return <div><div className="mb-1 font-medium text-foreground">{label}</div>{values.map((value) => <div key={value} className="mt-1 break-words text-muted-foreground">{value}</div>)}</div> }
function arrayText(value: unknown) { return Array.isArray(value) && value.length > 0 ? value.join(" / ") : "-" }
function reasonArrayText(value: unknown) { return Array.isArray(value) && value.length > 0 ? value.map((item) => automaticAssignmentReasonLabel(String(item))).join(" / ") : "-" }
function assignmentActionLabel(action: string) { return ({ auto_assigned: "规则自动加入", override_assigned: "人工例外加入", manual_override_incomplete_facts: "人工确认加入（事实不完整）", manual_assign: "人工加入", manual_ignore: "人工忽略", override_ignore: "例外自动忽略", migration_manual_claim: "迁移保留" } as Record<string, string>)[action] ?? action }

function emptyDraft(candidateGroupID: number): RuleDraft { return { candidateGroupID, enabled: true, priority: 100, modelFamilies: [], familyMatchMode: "single_any", gatewayApps: [], appMatchMode: "exact_set", acceptedAppSources: ["catalog_compatible"], channelTypes: [], serviceTiers: "", modelIDConditions: [], modelConditionQuantifier: "any", modelFactCoverage: "matched", capabilityConditions: [] } }
function emptyCapabilityCondition(families: string[]): GatewayCapabilityConditionDraft { return { selector_type: "family", selectorValuesText: families.join(", "), quantifier: "all_models", evidence_source: "probed", max_age_hours: 24, min_context_window_tokens: 128_000, min_safe_input_tokens: 0 } }
function updateCapabilityDraft(draft: RuleDraft, setDraft: (draft: RuleDraft) => void, index: number, update: Partial<GatewayCapabilityConditionDraft>) { setDraft({ ...draft, capabilityConditions: draft.capabilityConditions.map((condition, current) => current === index ? { ...condition, ...update } : condition) }) }
function candidateGroupName(groups: GatewayCandidateGroup[], id: number) { return groups.find((group) => group.id === id)?.name ?? `候选组 #${id}` }
function csvValues(value: string) { return Array.from(new Set(value.split(",").map((item) => item.trim().toLowerCase()).filter(Boolean))).sort() }
function identityKey(channelID: number, groupName: string) { return `${channelID}::${String(groupName ?? "").trim().toLowerCase()}` }
function formatRatio(value: number | null | undefined) { return Number.isFinite(value) ? `${Number(value).toFixed(2)}x` : "-" }
function serviceTierSourceLabel(source: string | undefined) { return source === "provider" ? "上游提供" : source === "manual" ? "人工维护" : "来源未标注" }
