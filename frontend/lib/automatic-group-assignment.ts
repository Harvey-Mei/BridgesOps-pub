import type {
  GatewayAutomaticAssignmentPreview,
  GatewayCapabilityCondition,
  GatewayModelFamilyCatalog,
  GatewayUpstreamGroup,
} from "./api-types"

export type GatewayCapabilityConditionDraft = Omit<GatewayCapabilityCondition, "selector_values"> & {
  selectorValuesText: string
}

export function capabilityConditionToDraft(condition: GatewayCapabilityCondition): GatewayCapabilityConditionDraft {
  const { selector_values: selectorValues, ...rest } = condition
  return {
    ...rest,
    selectorValuesText: selectorValues.join(", "),
  }
}

export function capabilityConditionFromDraft(condition: GatewayCapabilityConditionDraft, selectableFamilies: string[]): GatewayCapabilityCondition {
  const { selectorValuesText, ...rest } = condition
  const values = parseSelectorValues(selectorValuesText)
  return {
    ...rest,
    selector_values: condition.selector_type === "matched_model_ids"
      ? []
      : condition.selector_type === "family"
        ? values.filter((value) => selectableFamilies.includes(value))
        : values,
  }
}

export function selectorTextForTypeChange(
  currentText: string,
  nextType: GatewayCapabilityCondition["selector_type"],
  selectableFamilies: string[],
) {
  if (nextType === "matched_model_ids") return ""
  if (nextType === "family") {
    return parseSelectorValues(currentText).filter((value) => selectableFamilies.includes(value)).join(", ")
  }
  return currentText
}

export function parseSelectorValues(value: string) {
  return Array.from(new Set(value.split(",").map((item) => item.trim()).filter(Boolean))).sort()
}

export function selectableAutomaticModelFamilies(catalog: GatewayModelFamilyCatalog | null | undefined) {
  return catalog?.families.filter((family) => family.auto_assignable && family.code !== "other").map((family) => family.code) ?? []
}

export function capabilityConditionDraftError(condition: GatewayCapabilityConditionDraft, selectableFamilies: string[]) {
  const normalized = capabilityConditionFromDraft(condition, selectableFamilies)
  if (normalized.selector_type !== "matched_model_ids" && normalized.selector_values.length === 0) {
    return "能力条件需要至少一个模型选择值"
  }
  if (!Number.isFinite(condition.max_age_hours) || condition.max_age_hours <= 0) {
    return "证据有效小时必须大于 0"
  }
  const contextWindow = Number(condition.min_context_window_tokens ?? 0)
  const safeInput = Number(condition.min_safe_input_tokens ?? 0)
  if (!Number.isFinite(contextWindow) || !Number.isFinite(safeInput) || contextWindow < 0 || safeInput < 0) {
    return "能力阈值不能为负数"
  }
  if (contextWindow === 0 && safeInput === 0) {
    return "最小上下文或最小安全输入至少填写一项"
  }
  return ""
}

export function mergeServiceTierEdits(
  previousGroups: GatewayUpstreamGroup[],
  nextGroups: GatewayUpstreamGroup[],
  currentEdits: Record<number, string>,
) {
  const previousByID = new Map(previousGroups.map((group) => [group.id, group]))
  return Object.fromEntries(nextGroups.map((group) => {
    const previous = previousByID.get(group.id)
    const currentEdit = currentEdits[group.id]
    const previousPersisted = previous?.service_tier ?? ""
    const isDirty = previous != null && currentEdit != null && currentEdit !== previousPersisted
    return [group.id, isDirty ? currentEdit : group.service_tier ?? ""]
  }))
}

export function automaticAssignmentOutcomeChanged(
  draft: GatewayAutomaticAssignmentPreview,
  formal: GatewayAutomaticAssignmentPreview | undefined,
) {
  if (!formal) return true
  return JSON.stringify(assignmentOutcome(draft)) !== JSON.stringify(assignmentOutcome(formal))
}

/** Recheck each approved outcome; never replace it with a newly suggested target. */
export async function applyApprovedAssignments(
  approved: GatewayAutomaticAssignmentPreview[],
  refresh: (id: number) => Promise<GatewayAutomaticAssignmentPreview | undefined>,
  execute: (item: GatewayAutomaticAssignmentPreview) => Promise<void>,
) {
  const applied: number[] = []
  const issues: Array<{ id: number; reason: string; kind: "changed" | "unconfirmed" | "unexecuted" }> = []
  for (let index = 0; index < approved.length; index++) {
    const item = approved[index]
    try {
      const current = await refresh(item.change_id)
      if (!current || current.change_id !== item.change_id || current.upstream_group_id !== item.upstream_group_id || current.presence_generation !== item.presence_generation || current.state !== "ready" || !current.evaluation_token || automaticAssignmentOutcomeChanged(item, current)) {
        issues.push({ id: item.change_id, kind: "changed", reason: "判定已变化，请重新预览" })
        continue
      }
      await execute(current)
      applied.push(item.change_id)
    } catch (error) {
      issues.push({ id: item.change_id, kind: "unconfirmed", reason: error instanceof Error ? error.message : "处理结果未确认，请刷新后核对" })
      for (const remaining of approved.slice(index + 1)) issues.push({ id: remaining.change_id, kind: "unexecuted", reason: "本批次已暂停，尚未执行" })
      break
    }
  }
  return { applied, issues }
}

function assignmentOutcome(preview: GatewayAutomaticAssignmentPreview) {
  return {
    state: preview.state,
    reason: preview.reason_code,
    missing: [...preview.missing_facts].sort(),
    targets: [...preview.suggested_candidate_group_ids].sort((left, right) => left - right),
  }
}

const automaticAssignmentReasonLabels: Record<string, string> = {
  ambiguous_models: "模型同时命中多个家族",
  capability_evidence_missing_or_stale: "能力证据缺失或已过期",
  capability_matched: "能力阈值命中",
  capability_selector_empty: "能力条件没有可检查的模型",
  capability_threshold_not_matched: "能力阈值未命中",
  channel_type_matched: "渠道类型命中",
  channel_type_not_matched: "渠道类型未命中",
  equal_priority_targets: "同优先级规则指向不同候选组",
  facts_incomplete: "上游事实不完整",
  gateway_app_evidence_missing: "网关协议缺少所需证据",
  gateway_app_matched: "网关协议命中",
  gateway_app_not_matched: "网关协议未命中",
  group_generation_changed: "上游分组代次已变化",
  group_not_present: "上游分组当前不存在",
  identity_conflict: "同一上游分组的来源事实冲突",
  manual_assign_override: "使用人工指定候选组",
  manual_ignore_override: "使用人工忽略例外",
  manual_override_incomplete_facts: "人工指定并明确绕过不完整事实",
  migration_backfill_pending: "迁移事实仍在回填",
  model_family_contains_all: "包含规则要求的全部模型家族",
  model_family_matched: "模型家族命中",
  model_family_missing: "缺少规则要求的模型家族",
  model_family_not_matched: "模型家族未命中",
  model_family_set_exact: "模型家族集合完全一致",
  model_family_set_not_exact: "模型家族集合不一致",
  model_id_matched: "模型 ID 命中",
  model_id_not_matched: "模型 ID 未命中",
  model_ids_missing: "未同步到模型 ID",
  models_not_synced_for_generation: "本代上游分组尚未成功同步模型",
  models_sync_error: "模型同步失败",
  no_rule_matched: "没有规则命中",
  override_target_deleted: "人工例外的目标候选组已删除",
  override_waiting_for_current_generation_facts: "人工例外正在等待本代模型同步",
  service_tier_matched: "服务档位命中",
  service_tier_missing: "未维护服务档位",
  service_tier_not_matched: "服务档位未命中",
  unclassified_models: "存在未分类模型",
  unique_highest_priority_match: "唯一最高优先级规则命中",
}

export function automaticAssignmentReasonLabel(reason: string) {
  return automaticAssignmentReasonLabels[reason] ?? reason
}
