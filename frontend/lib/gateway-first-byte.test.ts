import assert from "node:assert/strict"
import test from "node:test"
import {
  gatewayFirstByteTone,
  gatewayManualCooldownIdempotencyKey,
  gatewayRequestCanBeManuallyCooled,
} from "./gateway-first-byte"

test("first-byte latency uses stable health tiers", () => {
  assert.equal(gatewayFirstByteTone(0), "missing")
  assert.equal(gatewayFirstByteTone(10_000), "fast")
  assert.equal(gatewayFirstByteTone(10_001), "moderate")
  assert.equal(gatewayFirstByteTone(30_000), "moderate")
  assert.equal(gatewayFirstByteTone(30_001), "slow")
})

test("manual cooldown is offered only for a slow log with a stored route", () => {
  assert.equal(gatewayRequestCanBeManuallyCooled({ channel_id: 3, group_name: "pool", first_byte_ms: 30_001, route_reason: "smart_primary" }), true)
  assert.equal(gatewayRequestCanBeManuallyCooled({ channel_id: 3, group_name: "pool", first_byte_ms: 30_000, route_reason: "smart_primary" }), false)
  assert.equal(gatewayRequestCanBeManuallyCooled({ channel_id: 0, group_name: "pool", first_byte_ms: 31_000, route_reason: "smart_primary" }), false)
  assert.equal(gatewayRequestCanBeManuallyCooled({ channel_id: 3, group_name: " ", first_byte_ms: 31_000, route_reason: "smart_primary" }), false)
})

test("manual cooldown is hidden for internal gateway records", () => {
  for (const routeReason of ["", "manual", " MANUAL ", "latency_probe", "api_test", "model_check", "future_route_reason", " API_TEST "]) {
    assert.equal(gatewayRequestCanBeManuallyCooled({
      channel_id: 3,
      group_name: "pool",
      first_byte_ms: 31_000,
      route_reason: routeReason,
    }), false)
  }
})

test("manual cooldown accepts only known smart-routing outcomes", () => {
  for (const routeReason of ["smart_primary", " fallback ", "session_affinity", "model_filtered"]) {
    assert.equal(gatewayRequestCanBeManuallyCooled({
      channel_id: 3,
      group_name: "pool",
      first_byte_ms: 31_000,
      route_reason: routeReason,
    }), true)
  }
})

test("manual cooldown idempotency keys are scoped to the request log", () => {
  assert.equal(
    gatewayManualCooldownIdempotencyKey(42, () => "00000000-0000-4000-8000-000000000000"),
    "request-log-42-00000000-0000-4000-8000-000000000000",
  )
})
