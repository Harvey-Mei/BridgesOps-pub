import assert from "node:assert/strict"
import test from "node:test"

import { officialReasoningEffortLabel } from "./reasoning-effort"

test("reasoning effort uses the official English values", () => {
  for (const effort of ["low", "medium", "high", "xhigh"] as const) {
    assert.equal(officialReasoningEffortLabel(effort), effort)
    assert.equal(officialReasoningEffortLabel(effort.toUpperCase()), effort)
  }
})

test("legacy extra-high spellings normalize to xhigh without translating values", () => {
  for (const effort of ["x-high", "x_high", "extra-high", "extra_high"]) {
    assert.equal(officialReasoningEffortLabel(effort), "xhigh")
  }
  assert.equal(officialReasoningEffortLabel("minimal"), "minimal")
  assert.equal(officialReasoningEffortLabel("max"), "max")
  assert.equal(officialReasoningEffortLabel(null), "")
})
