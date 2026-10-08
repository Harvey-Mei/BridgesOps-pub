import assert from "node:assert/strict"
import test from "node:test"
import {
  buildModelMappingPatch,
  candidateGroupSelectionChanged,
  modelMappingDraftEqual,
  modelMappingDraftFromKey,
  modelMappingDraftIsDirty,
  modelMappingDraftIsValid,
  modelMappingCandidateGroupCompatible,
  modelMappingDraftCandidateGroupCompatible,
  modelMappingStateFromKey,
  normalizeModelMappingStrategy,
  setModelMappingEnabled,
} from "./model-mapping-editor"

test("normalizes unknown mapping strategies to request-model mode", () => {
  assert.equal(normalizeModelMappingStrategy("BALANCED"), "balanced")
  assert.equal(normalizeModelMappingStrategy("unknown"), "off")
  assert.equal(normalizeModelMappingStrategy(null), "off")
})

test("manual mapping requires both a model and reasoning effort", () => {
  const draft = modelMappingDraftFromKey({
    model_mapping_strategy: "manual",
    model_mapping_model: " gpt-5.4 ",
    model_mapping_reasoning_effort: " high ",
  })
  assert.equal(modelMappingDraftIsValid(draft), true)
  assert.equal(modelMappingDraftIsValid({ ...draft, model: "" }), false)
  assert.equal(modelMappingDraftIsValid({ ...draft, reasoningEffort: "" }), false)
})

test("enabling mapping chooses an editable default without mutating an active draft", () => {
  const off = modelMappingDraftFromKey({ model_mapping_strategy: "off" })
  assert.deepEqual(setModelMappingEnabled(off, true, "codex"), {
    strategy: "balanced",
    model: "",
    reasoningEffort: "",
  })
  assert.deepEqual(setModelMappingEnabled(off, true, "claude"), {
    strategy: "manual",
    model: "",
    reasoningEffort: "",
  })
  const active = { strategy: "cost" as const, model: "", reasoningEffort: "low" }
  assert.equal(setModelMappingEnabled(active, true, "codex"), active)
  assert.deepEqual(setModelMappingEnabled(active, false, "codex"), {
    strategy: "off",
    model: "",
    reasoningEffort: "",
  })
})

test("draft equality ignores surrounding whitespace", () => {
  assert.equal(
    modelMappingDraftEqual(
      { strategy: "manual", model: "gpt-5.4", reasoningEffort: "high" },
      { strategy: "manual", model: " gpt-5.4 ", reasoningEffort: " high " },
    ),
    true,
  )
})

test("invalid persisted strategies stay visible as repairable state", () => {
  const persisted = modelMappingStateFromKey({
    model_mapping_strategy: "legacy-auto",
    model_mapping_model: "gpt-5.4",
    model_mapping_reasoning_effort: "high",
  })

  assert.equal(persisted.invalidStrategy, "legacy-auto")
  assert.equal(persisted.draft.strategy, "off")
  assert.equal(modelMappingDraftIsDirty(persisted.draft, persisted), true)
  assert.deepEqual(buildModelMappingPatch(persisted.draft), {
    model_mapping_strategy: "off",
    model_mapping_model: "",
    model_mapping_reasoning_effort: "",
  })
})

test("mapping PATCH payload keeps only fields valid for the selected strategy", () => {
  assert.deepEqual(buildModelMappingPatch({ strategy: "manual", model: " gpt-5.4 ", reasoningEffort: " high " }), {
    model_mapping_strategy: "manual",
    model_mapping_model: "gpt-5.4",
    model_mapping_reasoning_effort: "high",
  })
  assert.deepEqual(buildModelMappingPatch({ strategy: "balanced", model: "stale-model", reasoningEffort: " low " }), {
    model_mapping_strategy: "balanced",
    model_mapping_model: "",
    model_mapping_reasoning_effort: "low",
  })
})

test("disabled mapping allows all candidates", () => {
  assert.equal(modelMappingCandidateGroupCompatible(
    { app: "codex", model_family: "codex", model_mapping_strategy: "off" },
    "__all__",
    [],
  ), true)
})

test("enabled mapping requires a matching candidate group family", () => {
  const key = { app: "codex", model_family: "codex", model_mapping_strategy: "balanced" }
  const groups = [
    { id: 1, model_mapping_family: "codex" },
    { id: 2, model_mapping_family: "claude" },
  ]

  assert.equal(modelMappingCandidateGroupCompatible(key, "__all__", groups), false)
  assert.equal(modelMappingCandidateGroupCompatible(key, 2, groups), false)
  assert.equal(modelMappingCandidateGroupCompatible(key, "1", groups), true)
})

test("invalid persisted mapping strategy blocks candidate group changes until repaired", () => {
  assert.equal(modelMappingCandidateGroupCompatible(
    { app: "codex", model_mapping_strategy: "legacy-auto" },
    1,
    [{ id: 1, model_mapping_family: "codex" }],
  ), false)
})

test("manual mapping requires its fixed model in the target group members", () => {
  const key = {
    app: "codex",
    model_family: "codex",
    model_mapping_strategy: "manual",
    model_mapping_model: "gpt-5.4",
  }
  const groups = [{
    id: 1,
    model_mapping_family: "codex",
    members: [{ channel_id: 7, group_name: "premium" }],
  }]

  assert.equal(modelMappingCandidateGroupCompatible(key, 1, groups, [{
    channel_id: 7,
    group_name: "premium",
    model_ids: ["gpt-5.3"],
  }]), false)
  assert.equal(modelMappingCandidateGroupCompatible(key, 1, groups, [{
    channel_id: 7,
    group_name: "premium",
    model_ids: ["gpt-5.3", "GPT-5.4"],
  }]), true)
})

test("manual mapping draft fails closed without synchronized target models", () => {
  const key = { app: "codex", model_family: "codex" }
  const draft = { strategy: "manual" as const, model: "gpt-5.4", reasoningEffort: "high" }
  const groups = [{
    id: 1,
    model_mapping_family: "codex",
    members: [{ channel_id: 7, group_name: "premium" }],
  }]

  assert.equal(modelMappingDraftCandidateGroupCompatible(key, draft, 1, groups, []), false)
  assert.equal(modelMappingDraftCandidateGroupCompatible(key, draft, 1, groups, [{
    channel_id: 8,
    group_name: "other",
    model_ids: ["gpt-5.4"],
  }]), false)
})

test("candidate group capability checks only run for an actual target change", () => {
  assert.equal(candidateGroupSelectionChanged(1, "1"), false)
  assert.equal(candidateGroupSelectionChanged(0, "__all__"), false)
  assert.equal(candidateGroupSelectionChanged(undefined, 0), false)
  assert.equal(candidateGroupSelectionChanged(1, "2"), true)
})
