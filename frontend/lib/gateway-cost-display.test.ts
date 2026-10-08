import test from "node:test"
import assert from "node:assert/strict"
import { gatewayCostDisplayAmount, gatewayCostDisplaySource, gatewayCostRatioEstimate, gatewayFrozenRatios } from "./gateway-cost-display"

test("frozen multiplier display distinguishes explicit free output from missing history", () => {
  assert.deepEqual(gatewayFrozenRatios({ group_ratio: 2, completion_ratio: 0, raw_completion_ratio: 0 }), { input: 2, output: 2 })
  assert.deepEqual(gatewayFrozenRatios({ group_ratio: 2, completion_ratio: 0, price_fact_id: 1, price_fact_schema_version: 1 }), { input: 2, output: 2 })
  assert.deepEqual(gatewayFrozenRatios({ group_ratio: 2, completion_ratio: 0, price_fact_id: 1, price_fact_schema_version: 2 }), { input: 2, output: 0 })
  assert.deepEqual(gatewayFrozenRatios({ group_ratio: 2 }), { input: 2, output: 2 })
  assert.equal(gatewayFrozenRatios({ group_ratio: 0 }), null)
  assert.deepEqual(gatewayCostRatioEstimate({ group_ratio: 2, completion_ratio: 0, raw_completion_ratio: 0, prompt_tokens: 100, completion_tokens: 100 }), { input: 0.0004, output: 0.0004, total: 0.0008 })
})

test("confirmed historical bills including zero take precedence over ratio estimates", () => {
  for (const cost of [0, 0.37]) {
    for (const cost_status of ["reported", "reconciled"]) {
      const record = { cost, cost_status, group_ratio: 2, prompt_tokens: 100_000, completion_tokens: 1_000 }
      assert.equal(gatewayCostDisplayAmount(record), cost)
      assert.equal(gatewayCostDisplaySource(record), "legacy")
    }
  }
})

test("new bill fields retain precedence and do not resurrect legacy amounts", () => {
  const record = { cost: 999, cost_status: "reconciled", upstream_cost: 0, calculated_cost: 0.2, group_ratio: 2, prompt_tokens: 100_000 }
  assert.equal(gatewayCostDisplayAmount(record), 0)
  assert.equal(gatewayCostDisplaySource(record), "upstream")
  assert.equal(gatewayCostDisplayAmount({ ...record, price_fact_id: 1, upstream_cost: null, calculated_cost: null, display_cost: null }), null)
})
