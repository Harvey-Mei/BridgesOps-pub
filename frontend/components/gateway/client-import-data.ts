import type { GatewayApp, GatewayRouteCandidate } from "@/lib/api-types"
import { modelFamilyFromModelName, normalizeModelFamily } from "../../lib/model-families"

export type ClientImportRouteCandidate = Pick<
  GatewayRouteCandidate,
  "model_ids" | "disabled_for_key" | "cooling_down" | "routing_available" | "availability_reasons"
>

export type NativeClientConfigFile = {
  id: string
  label: string
  path: string
  content: string
}

export type OfficialClientImportInput = {
  providerName: string
  app: GatewayApp
  modelName?: string
  baseURL: string
  originURL: string
}

export type OfficialClientImport = {
  url: string
  payload: string
}

export type ContinueClientConfigInput = {
  apiKey: string
  name: string
  app: GatewayApp
  model: string
  endpoint: string
}

/** Encode UTF-8 text using standard Base64. */
export function base64Encode(value: string) {
  const bytes = new TextEncoder().encode(value)
  let binary = ""
  bytes.forEach((byte) => {
    binary += String.fromCharCode(byte)
  })
  return btoa(binary)
}

export function base64UrlEncode(value: string) {
  return base64Encode(value).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "")
}

/**
 * CCSwitch usage scripts return consumed usage, not an upstream account balance.
 * Keep total/remaining out of the result so the client does not render a false
 * quota. The detailed counters remain available in the extra line.
 */
export function buildCCSwitchUsageScript() {
  return `({
  request: {
    url: "{{baseUrl}}/v1/usage",
    method: "GET",
    headers: {
      "Authorization": "Bearer {{apiKey}}",
      "Accept": "application/json"
    }
  },
  extractor: function(response) {
    function numberValue(value) {
      var parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : 0;
    }
    var requests = numberValue(response.requests);
    var tokens = numberValue(response.total_tokens);
    var totalCost = numberValue(response.total_cost);
    var actualCost = numberValue(response.actual_cost);
    var estimatedCost = numberValue(response.estimated_cost);
    var cost = totalCost;
    var source = estimatedCost > 0 ? "当前费用（含估算）" : actualCost > 0 ? "已对账费用" : "暂无费用";
    return {
      isValid: response.is_active !== false,
      invalidMessage: response.is_active === false ? "BridgeOps Key 已停用" : undefined,
      planName: "BridgeOps API 使用量",
      used: cost,
      unit: "USD",
      extra: requests + " requests · " + tokens + " tokens · " + source + " · 已对账 " + actualCost + " USD · 估算 " + estimatedCost + " USD"
    };
  }
})`
}

export function clientImportModelForApp(modelName: string | undefined, app: GatewayApp) {
  const model = modelName?.trim() ?? ""
  if (!model) return ""
  if (app === "claude") return model
  return model
}

export function clientImportCandidateRoutable(candidate: ClientImportRouteCandidate) {
  const routeAvailabilityReasons = (candidate.availability_reasons ?? [])
    .map((reason) => String(reason ?? "").trim())
    .filter((reason) => reason !== "" && reason !== "probe_disabled")
  const unavailableWithoutFlag = candidate.routing_available == null
    && routeAvailabilityReasons.length > 0
  return !candidate.disabled_for_key
    && !candidate.cooling_down
    && candidate.routing_available !== false
    && !unavailableWithoutFlag
}

export function clientImportDefaultModel(
  mappedModel: string | undefined,
  preferredCandidate: ClientImportRouteCandidate | null | undefined,
  candidates: readonly ClientImportRouteCandidate[],
  modelFamily?: string,
) {
  const routableCandidates = candidates.filter(clientImportCandidateRoutable)
  const orderedCandidates = preferredCandidate && routableCandidates.includes(preferredCandidate)
    ? [preferredCandidate, ...routableCandidates.filter((candidate) => candidate !== preferredCandidate)]
    : routableCandidates
  return selectClientImportModel(orderedCandidates.flatMap(candidateModelIDs), mappedModel, modelFamily)
}

export function clientImportFixedDefaultModel(
  mappedModel: string | undefined,
  preferredCandidate: ClientImportRouteCandidate | null | undefined,
  candidates: readonly ClientImportRouteCandidate[],
  modelFamily?: string,
) {
  // A fixed Key can only use its selected target, even when smart health is stale.
  const models = preferredCandidate ? candidateModelIDs(preferredCandidate) : candidates.flatMap(candidateModelIDs)
  return selectClientImportModel(models, mappedModel, modelFamily)
}

function selectClientImportModel(modelIDs: string[], mappedModel?: string, modelFamily?: string) {
  const family = normalizeModelFamily(modelFamily)
  const models = Array.from(new Set(modelIDs)).filter(model => {
    const name = model.toLowerCase().split("/").at(-1) ?? ""
    if (/^(?:auto(?:-review)?|default|latest|router)$/.test(name)) return false
    if (/(?:^|[-_.])(?:mini|nano|flash|haiku|lite|fast|spark|embedding|rerank|image|audio|tts|transcribe|realtime|moderation|search)(?:[-_.]|$)/.test(name)) return false
    return !family || modelFamilyFromModelName(model) === family
  })
  // Prefer recognized flagship families by numeric generation, never API ordering.
  const ranked = models.map(model => ({ model, rank: clientModelRank(model) })).filter(item => item.rank.length > 0)
  ranked.sort((left, right) => {
    for (let index = 0; index < Math.max(left.rank.length, right.rank.length); index++) {
      const difference = (right.rank[index] ?? 0) - (left.rank[index] ?? 0)
      if (difference) return difference
    }
    return left.model.localeCompare(right.model, "en", { numeric: true })
  })
  if (ranked.length) return ranked[0].model
  const mapped = mappedModel?.trim() ?? ""
  return models.includes(mapped) ? mapped : models[0] ?? ""
}

function clientModelRank(model: string): number[] {
  const name = model.toLowerCase().split("/").at(-1) ?? ""
  const stable = /(?:^|-)(?:preview|experimental|exp)(?:-|$)/.test(name) ? 0 : 1
  const base = name.replace(/-\d{4}-?\d{2}-?\d{2}$/, "").replace(/-(?:latest|preview|experimental|exp)(?:-\d{2}-\d{2})?$/, "")
  const gpt = /^gpt-(\d+)(?:[.-](\d+))?(?:[.-](\d+))?(?:-(pro|codex)(?:-(max))?)?$/.exec(base)
  if (gpt) return [3, Number(gpt[1]), Number(gpt[2] ?? 0), Number(gpt[3] ?? 0), gpt[4] === "pro" ? 3 : gpt[5] ? 2 : gpt[4] === "codex" ? 1 : 0, stable]
  if (base === "gpt-4o") return [3, 4, 0, 0, 0, stable]
  const claude = /^claude-(opus|sonnet)-(\d+)(?:[.-](\d+))?$/.exec(base)
  if (claude) return [claude[1] === "opus" ? 3 : 2, Number(claude[2]), Number(claude[3] ?? 0), 0, 0, stable]
  const legacyClaude = /^claude-(\d+)(?:[.-](\d+))?-(opus|sonnet)$/.exec(base)
  if (legacyClaude) return [legacyClaude[3] === "opus" ? 3 : 2, Number(legacyClaude[1]), Number(legacyClaude[2] ?? 0), 0, 0, stable]
  const gemini = /^gemini-(\d+)(?:[.-](\d+))?-pro$/.exec(base)
  if (gemini) return [3, Number(gemini[1]), Number(gemini[2] ?? 0), 0, 0, stable]
  const grok = /^grok-(\d+)(?:[.-](\d+))?(?:-(heavy))?$/.exec(base)
  if (grok) return [3, Number(grok[1]), Number(grok[2] ?? 0), 0, grok[3] ? 1 : 0, stable]
  return []
}

export function buildNativeClientConfigFiles(
  apiKey: string,
  app: GatewayApp,
  baseURL: string,
  originURL: string,
  modelName?: string,
): NativeClientConfigFile[] {
  const model = clientImportModelForApp(modelName, app)
  const apiKeyValue = apiKey.trim()
  if (app === "claude") {
    return [
      {
        id: "claude-settings",
        label: "Claude settings.json",
        path: "~/.claude/settings.json",
        content: JSON.stringify(
          {
            env: {
              ANTHROPIC_BASE_URL: originURL,
              ANTHROPIC_AUTH_TOKEN: apiKeyValue,
              ...(model ? { ANTHROPIC_MODEL: model } : {}),
              CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: "1",
              CLAUDE_CODE_ATTRIBUTION_HEADER: "0",
            },
          },
          null,
          2,
        ),
      },
    ]
  }
  if (app === "gemini") return []
  const modelLines = model
    ? [`model = ${tomlString(model)}`, `review_model = ${tomlString(model)}`]
    : []
  return [
    {
      id: "codex-config",
      label: "Codex config.toml",
      path: "~/.codex/config.toml",
      content: `model_provider = "OpenAI"
${modelLines.length > 0 ? `${modelLines.join("\n")}\n` : ""}
model_reasoning_effort = "xhigh"
disable_response_storage = true
network_access = "enabled"
windows_wsl_setup_acknowledged = true

[model_providers.OpenAI]
name = "OpenAI"
base_url = ${tomlString(baseURL)}
wire_api = "responses"
requires_openai_auth = false

[features]
goals = true`,
    },
    {
      id: "codex-auth",
      label: "Codex auth.json",
      path: "~/.codex/auth.json",
      content: JSON.stringify({ OPENAI_API_KEY: apiKeyValue }, null, 2),
    },
  ]
}

export function buildContinueClientConfig(input: ContinueClientConfigInput): NativeClientConfigFile | null {
  const model = input.model.trim()
  if (!model) return null
  return {
    id: "continue-config",
    label: "Continue config.yaml",
    path: "~/.continue/config.yaml（Windows: %USERPROFILE%\\.continue\\config.yaml）",
    content: [
      `name: ${yamlString(input.name)}`,
      'version: "1.0.0"',
      "schema: v1",
      "models:",
      `  - name: ${yamlString(model)}`,
      `    provider: ${yamlString(input.app === "claude" ? "anthropic" : "openai")}`,
      `    model: ${yamlString(model)}`,
      `    apiBase: ${yamlString(input.endpoint)}`,
      `    apiKey: ${yamlString(input.apiKey.trim())}`,
    ].join("\n"),
  }
}

export function ccswitchUsageBaseURL(originURL: string) {
  try {
    const url = new URL(originURL)
    const hostname = url.hostname.toLowerCase()
    const loopback = hostname === "localhost" || hostname === "127.0.0.1" || hostname === "[::1]" || hostname === "::1"
    return url.protocol === "https:" || (url.protocol === "http:" && loopback)
      ? url.origin
      : ""
  } catch {
    return ""
  }
}

export function buildChatboxClientImport(input: OfficialClientImportInput): OfficialClientImport {
  const endpoint = input.app === "claude" ? input.originURL : input.baseURL
  const model = clientImportModelForApp(input.modelName, input.app)
  const id = `bridgeops-${input.app}-${stableProviderSuffix(input.providerName)}`
  const payload = JSON.stringify({
    isCustom: true,
    id,
    name: input.providerName,
    type: input.app === "claude" ? "anthropic" : input.app === "codex" ? "openai-responses" : "openai",
    settings: {
      apiHost: endpoint,
      ...(model ? { models: [{ modelId: model, nickname: model }] } : {}),
    },
  }, null, 2)
  const params = new URLSearchParams({ config: base64UrlEncode(payload) })
  return { url: `chatbox://provider/import?${params.toString()}`, payload }
}

function candidateModelIDs(candidate: ClientImportRouteCandidate) {
  return (candidate.model_ids ?? []).map((value) => value.trim()).filter(Boolean)
}

function tomlString(value: string) {
  return JSON.stringify(value)
}

function yamlString(value: string) {
  return JSON.stringify(value)
}

function stableProviderSuffix(value: string) {
  let hash = 2166136261
  for (const character of value.trim()) {
    hash ^= character.codePointAt(0) ?? 0
    hash = Math.imul(hash, 16777619)
  }
  return (hash >>> 0).toString(36)
}
