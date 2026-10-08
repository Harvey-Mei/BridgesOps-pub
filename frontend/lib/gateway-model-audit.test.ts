import test from "node:test"
import assert from "node:assert/strict"
import { gatewayModelAudit } from "./gateway-model-audit"

test("response audit compares dispatched model, never client alias", () => {
  assert.equal(gatewayModelAudit({ model_name: "gpt-example", requested_model_name: "auto", upstream_response_model: "gpt-example", upstream_model_mismatch: true }).differs, false)
  assert.equal(gatewayModelAudit({ model_name: "gpt-example", upstream_response_model: "gpt-example-2026-09-01" }).differs, true)
  assert.equal(gatewayModelAudit({ model_name: "gpt-example", upstream_response_model: "gpt-example-mini" }).differs, true)
  assert.equal(gatewayModelAudit({ model_name: "gpt-example" }).differs, false)
  assert.equal(gatewayModelAudit({ model_name: "", upstream_response_model: "gpt-example" }).differs, false)
  assert.equal(gatewayModelAudit({ model_name: " gpt-example ", upstream_response_model: "gpt-example " }).differs, false)
  assert.equal(gatewayModelAudit({ model_name: "gpt-example", upstream_response_model: "GPT-example" }).differs, true)
})
