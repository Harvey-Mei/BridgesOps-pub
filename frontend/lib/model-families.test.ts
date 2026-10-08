import assert from "node:assert/strict"
import test from "node:test"

import { modelFamilyFromModelName, modelFamilyLabel, normalizeModelFamily } from "./model-families"

test("model family aliases normalize to stable family codes", () => {
  const cases = [
    ["gpt", "codex"],
    ["OpenAI", "codex"],
    ["anthropic", "claude"],
    ["xai", "grok"],
    ["meta", "llama"],
    ["zhipu", "glm"],
    ["chatglm", "glm"],
    ["baidu", "ernie"],
  ] as const

  for (const [input, expected] of cases) {
    assert.equal(normalizeModelFamily(input), expected, input)
  }
})

test("model names classify into every supported family", () => {
  const cases = [
    ["gpt-5.4", "codex"],
    ["claude-sonnet-4-6", "claude"],
    ["gemini-2.5-pro", "gemini"],
    ["grok-4", "grok"],
    ["deepseek-v3.2", "deepseek"],
    ["qwen3-coder", "qwen"],
    ["mistral-large", "mistral"],
    ["meta-llama-4", "llama"],
    ["chatglm-4", "glm"],
    ["minimax-m2", "minimax"],
    ["moonshot-v1", "moonshot"],
    ["kimi-k2", "kimi"],
    ["yi-large", "yi"],
    ["ernie-4.5", "ernie"],
  ] as const

  for (const [model, expected] of cases) {
    assert.equal(modelFamilyFromModelName(model), expected, model)
  }
})

test("OpenAI reasoning model tokens require a complete token", () => {
  for (const model of ["o1", "provider/o3-mini", "vendor.o4-high"]) {
    assert.equal(modelFamilyFromModelName(model), "codex", model)
  }
  for (const model of ["o10", "foo3bar", "provider-o40-mini"]) {
    assert.equal(modelFamilyFromModelName(model), "other", model)
  }
})

test("unknown and empty values use a stable neutral presentation", () => {
  assert.equal(modelFamilyFromModelName(""), "other")
  assert.equal(modelFamilyFromModelName("vendor-model"), "other")
  assert.equal(modelFamilyLabel("other"), "其他")
  assert.equal(modelFamilyLabel("custom-family"), "custom-family")
  assert.equal(modelFamilyLabel(null), "其他")
})
