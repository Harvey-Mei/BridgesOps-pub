import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

const panel = readFileSync(new URL("../components/gateway/model-prices-panel.tsx", import.meta.url), "utf8")

test("long-context cache prices remain optional in the model price form", () => {
  assert.match(panel, /long_context_input_unit_price_usd[\s\S]*?required:\s*true/)
  assert.match(panel, /long_context_output_unit_price_usd[\s\S]*?required:\s*true/)
  assert.match(panel, /long_context_cache_read_unit_price_usd[\s\S]*?required:\s*false/)
  assert.match(panel, /long_context_cache_write_unit_price_usd[\s\S]*?required:\s*false/)
  assert.match(panel, /longContextPriceFields\.map\(\(\{ field, label, required \}\)/)
  assert.match(panel, /errorField=\{errorField\} required=\{required\} onChange=\{updateDraft\}/)
})

test("long-context tier label matches the strict greater-than pricing boundary", () => {
  assert.match(panel, /title=\{`超过 \$\{price\.long_context_threshold_tokens\.toLocaleString/)
  assert.match(panel, /\{`> \$\{compactTokenCount\(price\.long_context_threshold_tokens\)\}`\}/)
  assert.doesNotMatch(panel, /\{`>= \$\{compactTokenCount\(price\.long_context_threshold_tokens\)\}`\}/)
})
