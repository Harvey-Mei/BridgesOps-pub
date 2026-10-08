import assert from "node:assert/strict"
import test from "node:test"

import {
  candidatePreviewModelStorageKey,
  readCandidatePreviewModelPreference,
  resolveCandidatePreviewModelPreference,
  writeCandidatePreviewModelPreference,
  type CandidatePreviewPreferenceStorage,
} from "./candidate-preview-preference"

function memoryStorage(): CandidatePreviewPreferenceStorage {
  const values = new Map<string, string>()
  return {
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => { values.set(key, value) },
    removeItem: (key) => { values.delete(key) },
  }
}

test("candidate preview model preference persists independently for each downstream key", () => {
  const storage = memoryStorage()

  assert.equal(writeCandidatePreviewModelPreference(41, "requested", "  gpt-5.4  ", storage), true)
  assert.equal(writeCandidatePreviewModelPreference(42, "requested", "claude-sonnet-4-6", storage), true)
  assert.equal(readCandidatePreviewModelPreference(41, "requested", storage), "gpt-5.4")
  assert.equal(readCandidatePreviewModelPreference(42, "requested", storage), "claude-sonnet-4-6")
  assert.notEqual(candidatePreviewModelStorageKey(41, "requested"), candidatePreviewModelStorageKey(42, "requested"))
})

test("requested and mapped preview models are persisted as separate contexts", () => {
  const storage = memoryStorage()

  assert.equal(writeCandidatePreviewModelPreference(41, "requested", "gpt-5.4", storage), true)
  assert.equal(writeCandidatePreviewModelPreference(41, "mapped", "gpt-5.4-codex", storage), true)
  assert.equal(readCandidatePreviewModelPreference(41, "requested", storage), "gpt-5.4")
  assert.equal(readCandidatePreviewModelPreference(41, "mapped", storage), "gpt-5.4-codex")
})

test("invalid keys and empty models are not persisted", () => {
  const storage = memoryStorage()

  assert.equal(writeCandidatePreviewModelPreference(0, "requested", "gpt-5.4", storage), false)
  assert.equal(writeCandidatePreviewModelPreference(41, "requested", "   ", storage), false)
  assert.equal(readCandidatePreviewModelPreference(0, "requested", storage), "")
  assert.equal(readCandidatePreviewModelPreference(41, "requested", storage), "")
})

test("unavailable browser storage does not block candidate management", () => {
  const unavailableStorage: CandidatePreviewPreferenceStorage = {
    getItem: () => { throw new Error("storage unavailable") },
    setItem: () => { throw new Error("storage unavailable") },
    removeItem: () => { throw new Error("storage unavailable") },
  }

  assert.equal(readCandidatePreviewModelPreference(41, "requested", unavailableStorage), "")
  assert.equal(writeCandidatePreviewModelPreference(41, "requested", "gpt-5.4", unavailableStorage), false)
  assert.equal(readCandidatePreviewModelPreference(41, "requested", null), "")
})

test("a valid saved model wins before current mapping and request fallbacks", () => {
  const storage = memoryStorage()
  writeCandidatePreviewModelPreference(41, "mapped", "GPT-5.4-CODEX", storage)

  assert.deepEqual(resolveCandidatePreviewModelPreference({
    downstreamKeyID: 41,
    validModels: ["gpt-5.4", "gpt-5.4-codex"],
    preferenceContext: "mapped",
    mappedModel: "gpt-5.4",
    recentRequestModel: "gpt-5.4",
    storage,
  }), {
    model: "gpt-5.4-codex",
    context: "mapped",
    source: "saved",
  })
})

test("an unavailable saved model falls back for initialization without erasing the preference", () => {
  const storage = memoryStorage()
  writeCandidatePreviewModelPreference(41, "mapped", "retired-model", storage)

  assert.deepEqual(resolveCandidatePreviewModelPreference({
    downstreamKeyID: 41,
    validModels: ["gpt-5.4", "gpt-5.4-mini"],
    preferenceContext: "mapped",
    mappedModel: "gpt-5.4",
    recentRequestModel: "gpt-5.4-mini",
    storage,
  }), {
    model: "gpt-5.4",
    context: "mapped",
    source: "mapping",
  })
  assert.equal(readCandidatePreviewModelPreference(41, "mapped", storage), "retired-model")
})

test("an empty model inventory waits for synchronization without deleting the saved value", () => {
  const storage = memoryStorage()
  writeCandidatePreviewModelPreference(41, "mapped", "gpt-5.4", storage)

  assert.deepEqual(resolveCandidatePreviewModelPreference({
    downstreamKeyID: 41,
    validModels: [],
    preferenceContext: "mapped",
    mappedModel: "gpt-5.4",
    storage,
  }), {
    model: "",
    context: "mapped",
    source: "none",
  })
  assert.equal(readCandidatePreviewModelPreference(41, "mapped", storage), "gpt-5.4")
})

test("restore priority continues from an invalid mapping to recent request and available default", () => {
  const recent = resolveCandidatePreviewModelPreference({
    downstreamKeyID: 41,
    validModels: ["gpt-5.4", "gpt-5.4-mini"],
    preferenceContext: "mapped",
    mappedModel: "retired-model",
    recentRequestModel: "gpt-5.4-mini",
    storage: memoryStorage(),
  })
  assert.deepEqual(recent, {
    model: "gpt-5.4-mini",
    context: "mapped",
    source: "recent_request",
  })

  const availableDefault = resolveCandidatePreviewModelPreference({
    downstreamKeyID: 41,
    validModels: ["gpt-5.4", "gpt-5.4-mini"],
    preferenceContext: "mapped",
    mappedModel: "retired-model",
    recentRequestModel: "also-retired",
    defaultContext: "mapped",
    storage: memoryStorage(),
  })
  assert.deepEqual(availableDefault, {
    model: "gpt-5.4",
    context: "mapped",
    source: "available_default",
  })
})

test("temporary inventory gaps preserve a manual choice for recovery", () => {
  const storage = memoryStorage()
  writeCandidatePreviewModelPreference(41, "mapped", "gpt-5.4-codex", storage)
  resolveCandidatePreviewModelPreference({ downstreamKeyID: 41, preferenceContext: "mapped", validModels: ["gpt-5.4"], mappedModel: "gpt-5.4", storage })
  assert.equal(readCandidatePreviewModelPreference(41, "mapped", storage), "gpt-5.4-codex")
  assert.deepEqual(resolveCandidatePreviewModelPreference({ downstreamKeyID: 41, preferenceContext: "mapped", validModels: ["gpt-5.4", "gpt-5.4-codex"], mappedModel: "gpt-5.4", storage }), { model: "gpt-5.4-codex", context: "mapped", source: "saved" })
})
