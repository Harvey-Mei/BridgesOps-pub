import assert from "node:assert/strict"
import test from "node:test"

import catalogDocument from "../../modelcatalogdata/catalog.json"
import {
  applyApprovedAssignments,
  automaticAssignmentOutcomeChanged,
  automaticAssignmentReasonLabel,
  capabilityConditionDraftError,
  capabilityConditionFromDraft,
  capabilityConditionToDraft,
  mergeServiceTierEdits,
  parseSelectorValues,
  selectableAutomaticModelFamilies,
  selectorTextForTypeChange,
} from "./automatic-group-assignment"
import type { GatewayAutomaticAssignmentPreview, GatewayCapabilityCondition, GatewayModelFamilyCatalog, GatewayUpstreamGroup } from "./api-types"

test("exact and prefix capability selectors round-trip without changing spelling", () => {
  for (const selectorType of ["exact", "prefix"] as const) {
    const condition: GatewayCapabilityCondition = {
      selector_type: selectorType,
      selector_values: ["Vendor/Model-X", "gpt-5."],
      quantifier: "all_models",
      evidence_source: "probed",
      max_age_hours: 24,
      min_context_window_tokens: 128_000,
    }

    assert.deepEqual(capabilityConditionFromDraft(capabilityConditionToDraft(condition), ["codex", "claude"]), condition)
  }
})

test("selector text remains editable until payload normalization", () => {
  const editingText = "Model-A, vendor/Model-B, "
  assert.equal(capabilityConditionToDraft({
    selector_type: "exact",
    selector_values: ["Model-A"],
    quantifier: "any_model",
    evidence_source: "probed",
    max_age_hours: 24,
  }).selectorValuesText, "Model-A")
  assert.equal(selectorTextForTypeChange(editingText, "prefix", ["codex"]), editingText)
  assert.equal(selectorTextForTypeChange(editingText, "exact", ["codex"]), editingText)
  assert.deepEqual(parseSelectorValues(editingText), ["Model-A", "vendor/Model-B"])
})

test("the shared catalog never offers the unclassified family for automatic assignment", () => {
  const catalog = { version: catalogDocument.revision, families: catalogDocument.families } as GatewayModelFamilyCatalog
  const selectable = selectableAutomaticModelFamilies(catalog)

  assert.equal(catalog.families.find((family) => family.code === "other")?.auto_assignable, false)
  assert.equal(selectable.includes("other"), false)
  assert.ok(selectable.includes("codex"))
  assert.ok(selectable.includes("claude"))
  assert.equal(selectorTextForTypeChange("other, codex, Vendor/Model-X", "family", selectable), "codex")
  assert.deepEqual(capabilityConditionFromDraft({
    selector_type: "family",
    selectorValuesText: "other, codex",
    quantifier: "all_models",
    evidence_source: "probed",
    max_age_hours: 24,
  }, selectable).selector_values, ["codex"])
})

test("capability condition defaults and invalid thresholds get readable validation", () => {
  const valid = {
    selector_type: "family" as const,
    selectorValuesText: "codex",
    quantifier: "all_models" as const,
    evidence_source: "probed" as const,
    max_age_hours: 24,
    min_context_window_tokens: 128_000,
    min_safe_input_tokens: 0,
  }
  assert.equal(capabilityConditionDraftError(valid, ["codex"]), "")
  assert.equal(capabilityConditionDraftError({ ...valid, min_context_window_tokens: 0 }, ["codex"]), "最小上下文或最小安全输入至少填写一项")
  assert.equal(capabilityConditionDraftError({ ...valid, selectorValuesText: "other" }, ["codex"]), "能力条件需要至少一个模型选择值")
})

test("refreshing facts preserves only unsaved service tier edits", () => {
  const group = (id: number, tier: string): GatewayUpstreamGroup => ({
    id,
    channel_id: 1,
    canonical_group_name: `group-${id}`,
    raw_group_names: [],
    present: true,
    presence_generation: 1,
    model_family_codes: ["codex"],
    model_ids: ["gpt-5"],
    unclassified_model_ids: [],
    ambiguous_model_ids: [],
    gateway_apps: ["codex"],
    gateway_app_sources: { codex: "catalog_compatible" },
    channel_type: "newapi",
    service_tier: tier,
    service_tier_source: "manual",
    facts_complete: true,
    incomplete_reasons: [],
    facts_version: 1,
  })

  assert.deepEqual(mergeServiceTierEdits(
    [group(1, "standard"), group(2, "standard")],
    [group(1, "pro"), group(2, "standard")],
    { 1: "standard", 2: "max" },
  ), { 1: "pro", 2: "max" })
})

test("draft preview differences compare final outcomes and reasons are localized", () => {
  const preview = (overrides: Partial<GatewayAutomaticAssignmentPreview> = {}): GatewayAutomaticAssignmentPreview => ({
    change_id: 1,
    upstream_group_id: 1,
    channel_id: 1,
    group_name: "pro",
    presence_generation: 1,
    model_family_codes: ["codex"],
    model_ids: ["gpt-5"],
    gateway_apps: ["codex"],
    channel_type: "newapi",
    state: "ready",
    reason_code: "unique_highest_priority_match",
    missing_facts: [],
    suggested_candidate_group_ids: [3],
    matched_rule_ids: [1],
    rule_matches: [],
    evaluated_at: "2026-07-22T00:00:00Z",
    ...overrides,
  })

  assert.equal(automaticAssignmentOutcomeChanged(preview(), preview({ matched_rule_ids: [99] })), false)
  assert.equal(automaticAssignmentOutcomeChanged(preview({ suggested_candidate_group_ids: [4] }), preview()), true)
  assert.equal(automaticAssignmentReasonLabel("service_tier_missing"), "未维护服务档位")
  assert.equal(automaticAssignmentReasonLabel("unknown_future_reason"), "unknown_future_reason")
})

function approvedAssignment(id: number, target = 10): GatewayAutomaticAssignmentPreview {
  return { change_id: id, upstream_group_id: id, channel_id: 1, group_name: `group-${id}`, presence_generation: 1,
    model_family_codes: ["codex"], model_ids: ["gpt-5"], gateway_apps: ["codex"], channel_type: "newapi",
    state: "ready", reason_code: "unique_highest_priority_match", missing_facts: [], suggested_candidate_group_ids: [target],
    matched_rule_ids: [1], rule_matches: [], evaluation_token: "approved", evaluated_at: "2026-09-18T00:00:00Z" }
}

test("batch assignment preserves individual targets and executes fresh evidence only", async () => {
  const approved = [approvedAssignment(1, 10), approvedAssignment(2, 20)]
  const executed: GatewayAutomaticAssignmentPreview[] = []
  const result = await applyApprovedAssignments(approved, async id => ({ ...approved[id - 1], evaluation_token: `fresh-${id}` }), async item => { executed.push(item) })
  assert.deepEqual(result, { applied: [1, 2], issues: [] })
  assert.deepEqual(executed.map(item => [item.change_id, item.suggested_candidate_group_ids, item.evaluation_token]), [[1, [10], "fresh-1"], [2, [20], "fresh-2"]])
})

test("batch assignment skips changed targets, identities and non-ready outcomes", async () => {
  for (const patch of [{ suggested_candidate_group_ids: [99] }, { presence_generation: 2 }, { upstream_group_id: 999 }, { state: "conflict" as const }, { evaluation_token: undefined }]) {
    let executed = false
    const result = await applyApprovedAssignments([approvedAssignment(1)], async () => ({ ...approvedAssignment(1), ...patch }), async () => { executed = true })
    assert.equal(executed, false)
    assert.deepEqual(result.applied, [])
    assert.equal(result.issues.length, 1)
  }
})

test("batch assignment stops after uncertain failure and retains unexecuted work", async () => {
  const requested: number[] = []
  const result = await applyApprovedAssignments([1, 2, 3].map(id => approvedAssignment(id)), async id => { requested.push(id); return approvedAssignment(id) }, async item => { if (item.change_id === 2) throw new Error("connection lost") })
  assert.deepEqual(requested, [1, 2])
  assert.deepEqual(result.applied, [1])
  assert.deepEqual(result.issues.map(item => item.id), [2, 3])
})
