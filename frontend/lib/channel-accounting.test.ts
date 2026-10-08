import assert from "node:assert/strict"
import test from "node:test"
import {
	channelCostBasisOverrideIdempotencyKey,
	channelCostUnitRebaseIdempotencyKey,
	costBasisOverrideIntent,
  costUnitRebaseIntent,
  providerFactorForMultiplier,
  manualFundingDraft,
} from "./channel-accounting"

test("funding preview follows balance inclusion and never projects historical entries from today's balance", () => {
  const state = { raw_balance: 100, funding_factor: 0.5 }
  assert.deepEqual(manualFundingDraft(state, "10", "20", true, false).preview, {beforeRaw:80,afterRaw:100,afterActual:50,factor:0.5})
  assert.equal(manualFundingDraft(state, "0", "20", false, false).preview?.factor, 50 / 120)
  assert.equal(manualFundingDraft(state, "10", "200", true, false).valid, false)
  assert.deepEqual(manualFundingDraft(state, "10", "200", true, true), {valid:true,error:"",preview:null})
  assert.equal(manualFundingDraft(state, "", "20", true, false).valid, false)
  assert.equal(manualFundingDraft(state, "-1", "20", true, true).valid, false)
  assert.equal(manualFundingDraft(state, "10", "0", true, true).valid, false)
})
import type { ChannelCostBasisOverrideReadiness, ChannelCostUnitRebaseReadiness } from "./api-types"

const readiness: ChannelCostUnitRebaseReadiness = {
  supported: true,
  eligible: true,
  blockers: [],
  expected_version: 18,
  expected_cost_unit_epoch: 7,
  multiplier_mode: "divide",
  previous_multiplier: 10,
  previous_provider_factor: 0.1,
}

const overrideReadiness: ChannelCostBasisOverrideReadiness = {
	eligible: true,
	blockers: [],
	expected_version: 18,
	expected_identity_revision: 3,
	expected_cost_unit_epoch: 7,
	current_funding_factor: 0.1,
	pending_funding_candidates: 0,
}

test("manual cost basis override intent contains only fenced administrator input", () => {
	const result = costBasisOverrideIntent(overrideReadiness, "0.25", "  corrected finance ticket  ")
	assert.equal(result.error, null)
	assert.deepEqual(result.intent, {
		expected_version: 18,
		expected_identity_revision: 3,
		expected_cost_unit_epoch: 7,
		funding_factor: 0.25,
		note: "corrected finance ticket",
	})
	assert.equal("raw_balance" in (result.intent as unknown as Record<string, unknown>), false)
})

test("manual cost basis override accepts same-factor reassertion for accounting recovery", () => {
	const result = costBasisOverrideIntent(overrideReadiness, "0.1", "verified unchanged basis")
	assert.equal(result.error, null)
	assert.equal(result.intent?.funding_factor, 0.1)
	assert.equal(costBasisOverrideIntent(overrideReadiness, "0", "free provider basis").error, null)
})

test("manual cost basis override validation rejects missing reason and stale fences", () => {
	assert.match(costBasisOverrideIntent(overrideReadiness, "", "verified").error ?? "", /有效/)
	assert.match(costBasisOverrideIntent(overrideReadiness, "   ", "verified").error ?? "", /有效/)
	assert.match(costBasisOverrideIntent(overrideReadiness, "0.2", " ").error ?? "", /原因/)
	assert.match(costBasisOverrideIntent({ ...overrideReadiness, eligible: false }, "0.2", "verified").error ?? "", /不允许/)
})

test("cost-unit rebase intent contains administrator input but no monetary samples", () => {
  const result = costUnitRebaseIntent(readiness, "1", "  upstream unit changed  ")
  assert.equal(result.error, null)
  assert.deepEqual(result.intent, {
    expected_version: 18,
    previous_multiplier: 10,
    current_multiplier: 1,
    note: "upstream unit changed",
  })
  assert.equal("raw_balance" in (result.intent as unknown as Record<string, unknown>), false)
  assert.equal("raw_total_cost" in (result.intent as unknown as Record<string, unknown>), false)
})

test("cost-unit rebase validation rejects stale or incomplete administrator intent", () => {
  assert.match(costUnitRebaseIntent(readiness, "10", "verified").error ?? "", /必须与旧倍率不同/)
  assert.match(costUnitRebaseIntent(readiness, "0", "verified").error ?? "", /有效/)
  assert.match(costUnitRebaseIntent(readiness, "1", " ").error ?? "", /核对说明/)
  assert.match(costUnitRebaseIntent({ ...readiness, eligible: false }, "1", "verified").error ?? "", /尚不允许/)
})

test("multiplier preview follows the server divide and multiply modes", () => {
  assert.equal(providerFactorForMultiplier(10, "divide"), 0.1)
  assert.equal(providerFactorForMultiplier(0.1, "multiply"), 0.1)
  assert.equal(providerFactorForMultiplier(0, "divide"), null)
})

test("cost-unit rebase idempotency keys are scoped and API-safe", () => {
  const key = channelCostUnitRebaseIdempotencyKey(41, 18, () => "00000000-0000-4000-8000-000000000000")
  assert.equal(key, "cost-unit-rebase:41:18:00000000-0000-4000-8000-000000000000")
  assert.match(key, /^[A-Za-z0-9._:-]{1,128}$/)
})

test("manual cost basis override idempotency keys are scoped and API-safe", () => {
	const key = channelCostBasisOverrideIdempotencyKey(41, 18, () => "00000000-0000-4000-8000-000000000000")
	assert.equal(key, "cost-basis-override:41:18:00000000-0000-4000-8000-000000000000")
	assert.match(key, /^[A-Za-z0-9._:-]{1,128}$/)
})
