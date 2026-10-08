import assert from "node:assert/strict"
import test from "node:test"

const modulePath = "./client-import-data.ts"
const {
  buildCCSwitchUsageScript,
  buildChatboxClientImport,
  buildContinueClientConfig,
  buildNativeClientConfigFiles,
  ccswitchUsageBaseURL,
  clientImportDefaultModel,
  clientImportFixedDefaultModel,
} = await import(modulePath)

function decodeBase64URL(value: string) {
  const normalized = value.replace(/-/g, "+").replace(/_/g, "/")
  const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, "=")
  return Buffer.from(padded, "base64").toString("utf8")
}

test("CCSwitch usage script reports consumed cost without inventing a remaining quota", () => {
  const script = buildCCSwitchUsageScript()
  assert.match(script, /actual_cost/)
  assert.match(script, /used: cost/)
  assert.match(script, /BridgeOps API 使用量/)
  assert.doesNotMatch(script, /\n\s+remaining:/)
  assert.doesNotMatch(script, /\n\s+total:/)
})

test("CCSwitch usage scripts are enabled only for HTTPS or loopback HTTP origins", () => {
  assert.equal(ccswitchUsageBaseURL("https://bridge.example"), "https://bridge.example")
  assert.equal(ccswitchUsageBaseURL("http://127.0.0.1:18418"), "http://127.0.0.1:18418")
  assert.equal(ccswitchUsageBaseURL("http://localhost:18418"), "http://localhost:18418")
  assert.equal(ccswitchUsageBaseURL("http://192.168.1.10:18418"), "")
})

test("Chatbox import uses the current OpenAI Responses provider contract", () => {
  const result = buildChatboxClientImport({
    providerName: "BridgeOps-Dev-GPT-Codex",
    app: "codex",
    modelName: "gpt-5.4",
    baseURL: "https://bridge.example/v1",
    originURL: "https://bridge.example",
  })
  const url = new URL(result.url)
  assert.equal(url.protocol, "chatbox:")
  assert.equal(url.hostname, "provider")
  assert.equal(url.pathname, "/import")
  const payload = JSON.parse(decodeBase64URL(url.searchParams.get("config") ?? ""))
  assert.equal(payload.type, "openai-responses")
  assert.equal(payload.settings.apiHost, "https://bridge.example/v1")
  assert.equal(payload.settings.apiKey, undefined)
  assert.doesNotMatch(result.url, /sk-test/)
})

test("Chatbox import uses model objects and the Anthropic origin endpoint", () => {
  const result = buildChatboxClientImport({
    providerName: "BridgeOps-Dev-CC-Claude",
    app: "claude",
    modelName: "claude-sonnet-4-6",
    baseURL: "https://bridge.example/v1",
    originURL: "https://bridge.example",
  })
  const url = new URL(result.url)
  const payload = JSON.parse(decodeBase64URL(url.searchParams.get("config") ?? ""))
  assert.equal(payload.type, "anthropic")
  assert.equal(payload.settings.apiHost, "https://bridge.example")
  assert.deepEqual(payload.settings.models, [{ modelId: "claude-sonnet-4-6", nickname: "claude-sonnet-4-6" }])
})

test("official imports omit models when BridgeOps has no reliable model capability", () => {
  const result = buildChatboxClientImport({
    providerName: "BridgeOps-Dev-GPT-Codex",
    app: "codex",
    baseURL: "https://bridge.example/v1",
    originURL: "https://bridge.example",
  })
  const url = new URL(result.url)
  const payload = JSON.parse(decodeBase64URL(url.searchParams.get("config") ?? ""))
  assert.equal(payload.settings.models, undefined)
})

test("client import default models skip candidates that cannot currently route", () => {
  const candidates = [
    { model_ids: ["disabled-model"], disabled_for_key: true, cooling_down: false },
    { model_ids: ["cooling-model"], disabled_for_key: false, cooling_down: true },
    { model_ids: ["legacy-model"], disabled_for_key: false, cooling_down: false },
    { model_ids: ["unavailable-model"], disabled_for_key: false, cooling_down: false, routing_available: false },
    { model_ids: ["blocked-model"], disabled_for_key: false, cooling_down: false, availability_reasons: ["upstream_key_unavailable"] },
    { model_ids: ["ready-model"], disabled_for_key: false, cooling_down: false, routing_available: true },
  ]

  assert.equal(clientImportDefaultModel(undefined, candidates[0], candidates), "legacy-model")
  assert.equal(clientImportDefaultModel("disabled-model", null, candidates), "legacy-model")
  assert.equal(clientImportDefaultModel("ready-model", null, candidates), "ready-model")
})

test("client import ignores legacy probe pause availability reasons", () => {
  const candidate = {
    model_ids: ["legacy-model"],
    disabled_for_key: false,
    cooling_down: false,
    availability_reasons: ["probe_disabled"],
  }
  assert.equal(clientImportDefaultModel(undefined, candidate, [candidate]), "legacy-model")
})

test("client import omits a default model when every candidate is unavailable", () => {
  const candidates = [
    { model_ids: ["disabled-model"], disabled_for_key: true, cooling_down: false },
    { model_ids: ["cooling-model"], disabled_for_key: false, cooling_down: true },
  ]
  assert.equal(clientImportDefaultModel("disabled-model", candidates[0], candidates), "")
})

test("fixed imports require synchronized model evidence without smart availability filtering", () => {
  const candidate = {
    model_ids: ["fixed-model"],
    disabled_for_key: true,
    cooling_down: true,
    routing_available: false,
    availability_reasons: ["routing_cooldown"],
  }

  assert.equal(clientImportFixedDefaultModel("mapped-model", candidate, [candidate]), "fixed-model")
  assert.equal(clientImportFixedDefaultModel("mapped-model", { ...candidate, model_ids: [] }, [candidate]), "")
})

test("client imports select the newest supported full GPT across eligible candidates", () => {
  const first = { model_ids: ["auto-review", "gpt-5.4-mini", "gpt-5.3-codex"] }
  const next = { model_ids: ["gpt-5.4", "gpt-5.2-pro", "gpt-5.4-nano"] }
  const blocked = { model_ids: ["gpt-9"], disabled_for_key: true }
  assert.equal(clientImportDefaultModel("gpt-5.3-codex", first, [first, next, blocked], "codex"), "gpt-5.4")
  assert.equal(clientImportDefaultModel(undefined, next, [next, first], "codex"), "gpt-5.4")
  const numeric = { model_ids: ["gpt-5.9", "gpt-5.10", "gpt-5.10-pro"] }
  assert.equal(clientImportDefaultModel(undefined, numeric, [numeric], "codex"), "gpt-5.10-pro")
})

test("client imports prefer Opus and Pro while keeping model families separate", () => {
  const mixed = { model_ids: ["claude-haiku-4-5", "claude-sonnet-4-6", "claude-opus-4-5", "claude-opus-4-6", "gemini-3.1-flash", "gemini-2.5-pro", "gemini-3.1-pro-preview", "gpt-5.4"] }
  assert.equal(clientImportDefaultModel(undefined, mixed, [mixed], "claude"), "claude-opus-4-6")
  assert.equal(clientImportDefaultModel(undefined, mixed, [mixed], "gemini"), "gemini-3.1-pro-preview")
  assert.equal(clientImportDefaultModel(undefined, mixed, [mixed], "codex"), "gpt-5.4")
})

test("client imports leave aliases and lightweight-only inventories without a default", () => {
  const candidate = { model_ids: ["auto-review", "auto", "gpt-5.4-mini", "gemini-3-flash", "claude-haiku-4-5", "text-embedding-3-large", "gpt-image-1"] }
  assert.equal(clientImportDefaultModel("auto-review", candidate, [candidate]), "")
})

test("fixed imports select a flagship only from the configured target", () => {
  const fixed = { model_ids: ["auto-review", "gpt-5.3-codex", "gpt-5.4"], disabled_for_key: true, cooling_down: true }
  const other = { model_ids: ["gpt-9"] }
  assert.equal(clientImportFixedDefaultModel("gpt-5.3-codex", fixed, [fixed, other], "codex"), "gpt-5.4")
})

test("client imports prefer stable same-generation models and retain exact synchronized IDs", () => {
  const candidate = { model_ids: ["gemini-3.1-pro-preview", "gemini-3.1-pro", "claude-opus-4-6-20260205", "openai/gpt-5.4"] }
  assert.equal(clientImportDefaultModel(undefined, candidate, [candidate], "gemini"), "gemini-3.1-pro")
  assert.equal(clientImportDefaultModel(undefined, candidate, [candidate], "claude"), "claude-opus-4-6-20260205")
  assert.equal(clientImportDefaultModel(undefined, candidate, [candidate], "codex"), "openai/gpt-5.4")
})

test("native exports consistently use the selected default including code review", () => {
  const codex = buildNativeClientConfigFiles("test-key", "codex", "https://bridge.example/v1", "https://bridge.example", "gpt-5.4")
  assert.match(codex[0].content, /model = "gpt-5\.4"/)
  assert.match(codex[0].content, /review_model = "gpt-5\.4"/)
  const claude = buildNativeClientConfigFiles("test-key", "claude", "", "https://bridge.example", "claude-opus-4-6")
  assert.equal(JSON.parse(claude[0].content).env.ANTHROPIC_MODEL, "claude-opus-4-6")
})

test("fixed client imports use the selected route model despite smart routing state", () => {
  const candidate = {
    model_ids: ["fixed-model"],
    disabled_for_key: true,
    cooling_down: true,
    routing_available: false,
  }

  assert.equal(clientImportFixedDefaultModel(undefined, candidate, [candidate]), "fixed-model")
})

test("native Gemini config stays unavailable until BridgeOps implements v1beta", () => {
  const files = buildNativeClientConfigFiles(
    "test-key",
    "gemini",
    "https://bridge.example/v1",
    "https://bridge.example",
    "gemini-2.5-pro",
  )
  assert.deepEqual(files, [])
  assert.doesNotMatch(JSON.stringify(files), /GOOGLE_GEMINI_BASE_URL|GEMINI_API_KEY/)
})

test("Continue config requires a routable model and includes the current downstream key", () => {
  const input = {
    apiKey: "test-key",
    name: "BridgeOps · test",
    app: "codex" as const,
    model: "",
    endpoint: "https://bridge.example/v1",
  }
  assert.equal(buildContinueClientConfig(input), null)

  const file = buildContinueClientConfig({ ...input, model: "gpt-5.4" })
  assert.ok(file)
  assert.match(file.content, /model: "gpt-5\.4"/)
  assert.match(file.content, /apiKey: "test-key"/)
  assert.doesNotMatch(file.content, /secrets\.BRIDGEOPS_API_KEY/)
})
