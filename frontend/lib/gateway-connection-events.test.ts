import assert from "node:assert/strict"
import test from "node:test"
import {
  gatewayConnectionEventDetail,
  gatewayConnectionEventLabel,
  gatewayRequestConnectionEvent,
} from "./gateway-connection-events"

const base = {
  connection_event: undefined,
  route_reason: "smart_primary",
  status_code: 200,
  upstream_accepted: false,
  terminal_failure_code: "",
  error: "",
}

test("prefers the explicit connection event from the API", () => {
  assert.equal(gatewayRequestConnectionEvent({
    ...base,
    connection_event: "dispatch_recovery",
    route_reason: "client_canceled",
    status_code: 499,
  }), "dispatch_recovery")
})

test("classifies legacy client disconnect rows", () => {
  assert.equal(gatewayRequestConnectionEvent({ ...base, route_reason: "client_canceled", status_code: 499 }), "client_disconnect")
  assert.equal(gatewayRequestConnectionEvent({ ...base, status_code: 499 }), "client_disconnect")
  assert.equal(gatewayRequestConnectionEvent({ ...base, error: "context canceled" }), "client_disconnect")
})

test("classifies accepted upstream interruptions without inventing one for pre-acceptance failures", () => {
  assert.equal(gatewayRequestConnectionEvent({
    ...base,
    status_code: 502,
    upstream_accepted: true,
    terminal_failure_code: "upstream_5xx",
  }), "upstream_interrupted")
  assert.equal(gatewayRequestConnectionEvent({
    ...base,
    status_code: 502,
    upstream_accepted: true,
    error: "stream disconnected before completion",
  }), "upstream_interrupted")
  assert.equal(gatewayRequestConnectionEvent({
    ...base,
    status_code: 502,
    terminal_failure_code: "retry_prompt_after_exhaustion",
  }), null)
})

test("classifies dispatch recovery and ignores ordinary successes", () => {
  assert.equal(gatewayRequestConnectionEvent({ ...base, route_reason: "dispatch_recovery", status_code: 502 }), "dispatch_recovery")
  assert.equal(gatewayRequestConnectionEvent(base), null)
})

test("keeps fixed labels and explanatory details stable", () => {
  assert.equal(gatewayConnectionEventLabel("client_disconnect"), "客户端断开 / 可能重连")
  assert.equal(gatewayConnectionEventLabel("upstream_interrupted"), "上游断流")
  assert.match(gatewayConnectionEventDetail("upstream_interrupted"), /未重放/)
})
