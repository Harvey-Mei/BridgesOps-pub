import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

import { createServer } from "vite"

test("channel list cache survives route remounts without delaying other API refreshes", async () => {
  const server = await createServer({
    appType: "custom",
    logLevel: "silent",
    server: { middlewareMode: true },
  })
  try {
    const { apiCacheTTL } = await server.ssrLoadModule("/lib/queries.ts")
    assert.equal(apiCacheTTL("/channels"), 30_000)
    assert.equal(apiCacheTTL("/channels?page=1&page_size=9"), 800)
    assert.equal(apiCacheTTL("/dashboard/summary"), 800)
  } finally {
    await server.close()
  }
})

test("request query and CSV reject the same DST boundary instead of omitting it", async () => {
  const server = await createServer({
    appType: "custom",
    logLevel: "silent",
    server: { middlewareMode: true },
  })
  try {
    const {
      gatewayRequestLogExportURL,
      gatewayRequestLogQuery,
    } = await server.ssrLoadModule("/lib/queries.ts")
    const filters = {
      start: "2024-11-03T01:30",
      end: "2024-11-03T03:30",
    }

    const result = gatewayRequestLogQuery(1, 20, filters, true, "America/New_York")
    assert.equal(result.query, "")
    assert.equal(result.error?.code, "ambiguous_local_time")
    assert.throws(
      () => gatewayRequestLogExportURL(filters, "America/New_York"),
      (error) => error?.code === result.error?.code && error?.message === result.error?.message,
    )
  } finally {
    await server.close()
  }
})

test("valid request query keeps both UTC boundaries", async () => {
  const server = await createServer({
    appType: "custom",
    logLevel: "silent",
    server: { middlewareMode: true },
  })
  try {
    const { gatewayRequestLogQuery } = await server.ssrLoadModule("/lib/queries.ts")
    const result = gatewayRequestLogQuery(1, 20, {
      start: "2024-07-01T12:30",
      end: "2024-07-01T13:30",
    }, true, "America/New_York")

    assert.equal(result.error, null)
    const query = new URLSearchParams(result.query)
    assert.equal(query.get("start"), "2024-07-01T16:30:00.000Z")
    assert.equal(query.get("end"), "2024-07-01T17:30:00.000Z")
  } finally {
    await server.close()
  }
})

test("request query preserves the exact upstream request ID", async () => {
  const server = await createServer({
    appType: "custom",
    logLevel: "silent",
    server: { middlewareMode: true },
  })
  try {
    const { gatewayRequestLogQuery } = await server.ssrLoadModule("/lib/queries.ts")
    for (const upstreamRequestID of [
      "  req-123\t",
      " \t ",
      "a".repeat(128),
      "b".repeat(512),
      "c".repeat(513),
    ]) {
      const result = gatewayRequestLogQuery(1, 20, { upstreamRequestID }, true, "UTC")

      assert.equal(result.error, null)
      assert.equal(new URLSearchParams(result.query).get("upstream_request_id"), upstreamRequestID)
    }
  } finally {
    await server.close()
  }
})

test("usage filter wiring does not trim or truncate the upstream request ID", () => {
  const gatewayPage = readFileSync(new URL("../app/gateway-page.tsx", import.meta.url), "utf8")
  const usageView = readFileSync(new URL("../components/gateway/gateway-usage-view.tsx", import.meta.url), "utf8")
  const requestIDInputStart = usageView.indexOf("value={draftFilters.upstreamRequestID}")
  const requestIDInputEnd = usageView.indexOf("/>", requestIDInputStart)

  assert.doesNotMatch(gatewayPage, /upstreamRequestID\.trim\(\)/)
  assert.doesNotMatch(usageView, /upstreamRequestID\.trim\(\)/)
  assert.ok(requestIDInputStart >= 0 && requestIDInputEnd > requestIDInputStart)
  assert.doesNotMatch(usageView.slice(requestIDInputStart, requestIDInputEnd), /maxLength=/)
})

test("dashboard multiplier query keeps the overview UTC range but never forwards model or current-price filters", async () => {
  const server = await createServer({
    appType: "custom",
    logLevel: "silent",
    server: { middlewareMode: true },
  })
  try {
    const { dashboardMultiplierTrendQuery } = await server.ssrLoadModule("/lib/queries.ts")
    const result = dashboardMultiplierTrendQuery({
      start: "2024-07-01T12:30",
      end: "2024-07-01T13:30",
      modelName: "gpt-5.4",
      downstreamKeyID: "9",
      routeReason: "fallback",
    }, "America/New_York")

    assert.equal(result.error, null)
    const query = new URLSearchParams(result.query)
    assert.equal(query.get("start"), "2024-07-01T16:30:00.000Z")
    assert.equal(query.get("end"), "2024-07-01T17:30:00.000Z")
    assert.equal(query.get("model"), null)
    assert.equal(query.get("downstream_key_id"), null)
    assert.equal(query.get("route_reason"), null)
  } finally {
    await server.close()
  }
})

test("model unit-price reconciliation key keeps token kinds independent", async () => {
  const server = await createServer({
    appType: "custom",
    logLevel: "silent",
    server: { middlewareMode: true },
  })
  try {
    const { gatewayRequestLogModelUnitPriceKey } = await server.ssrLoadModule("/lib/queries.ts")
    const common = {
      bucket: "2026-07-23T08:00:00Z",
      model_family: "gpt",
      model_name: "gpt-5.4",
      total_tokens: 100,
      total_cost: 0.1,
      cost_per_million_tokens: 1000,
    }

    assert.notEqual(
      gatewayRequestLogModelUnitPriceKey({ ...common, token_type: "input" }),
      gatewayRequestLogModelUnitPriceKey({ ...common, token_type: "output" }),
    )
  } finally {
    await server.close()
  }
})
