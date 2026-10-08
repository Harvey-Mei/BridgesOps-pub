import assert from "node:assert/strict"
import test from "node:test"

const payloadModulePath = "./claude-radar-payload.ts"
const { normalizeClaudeRadarPayload } = await import(payloadModulePath)

test("normalizes the minimal Claude radar payload", () => {
  const payload = normalizeClaudeRadarPayload({
    updated_at: "2026-07-19T00:00:00Z",
    labels: ["7.19am"],
    ignored: "external metadata",
    iq: {
      models: [{ key: "claude-sonnet", name: null, score: 91, iq: [91, null], cost: [0.08] }],
    },
  })

  assert.deepEqual(payload, {
    updated_at: "2026-07-19T00:00:00Z",
    labels: ["7.19am"],
    iq: {
      updated_at: undefined,
      models: [{
        key: "claude-sonnet",
        name: "claude-sonnet",
        score: 91,
        iq: [91, null],
        cost: [0.08],
      }],
    },
  })
})

test("normalizes nullable collections to the existing empty state", () => {
  assert.deepEqual(normalizeClaudeRadarPayload({ labels: null, iq: { models: null } }), {
    updated_at: undefined,
    labels: [],
    iq: { updated_at: undefined, models: [] },
  })
  assert.equal(normalizeClaudeRadarPayload(null), null)
})

test("rejects malformed collection and model field types", () => {
  const invalidPayloads = [
    { labels: "7.19am", iq: { models: [] } },
    { labels: ["7.19am", 19], iq: { models: [] } },
    { labels: [], iq: { models: "upstream error" } },
    { labels: [], iq: { models: [null] } },
    { labels: [], iq: { models: [{ key: "claude-sonnet", cost: ["0.08"] }] } },
    { labels: [], iq: { models: [{ key: "duplicate" }, { key: "duplicate" }] } },
  ]

  for (const payload of invalidPayloads) {
    assert.equal(normalizeClaudeRadarPayload(payload), null)
  }
})
