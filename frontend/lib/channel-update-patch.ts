import type {
  Channel,
  CredentialMode,
  DirectCredential,
  DirectCredentialInput,
  GatewayApp,
} from "@/lib/api-types"

export function directModelsForForm(raw?: string): string {
  if (!raw) return ""
  try {
    const parsed = JSON.parse(raw)
    if (Array.isArray(parsed)) {
      return parsed.filter((item): item is string => typeof item === "string").join("\n")
    }
  } catch {
    // Preserve an invalid stored value so the administrator can repair it.
  }
  return raw
}

export function directModelsPayload(raw: string): string {
  const models = Array.from(
    new Set(
      raw
        .split(/\r?\n/)
        .map((item) => item.trim())
        .filter(Boolean),
    ),
  )
  return JSON.stringify(models)
}

export function directModelsList(raw: string): string[] {
  return JSON.parse(directModelsPayload(raw)) as string[]
}

export function directModelsChanged(persisted: string | undefined, nextFormValue: string): boolean {
  return directModelsPayload(directModelsForForm(persisted)) !== directModelsPayload(nextFormValue)
}

export interface DirectCredentialDraft {
  id?: number
  group_name: string
  base_url: string
  api_key: string
  key_configured: boolean
  models: string
  ratio: string
  completion_ratio: string
  enabled: boolean
}

export function initializeDirectCredentialDrafts<T extends DirectCredentialDraft>(
  drafts: T[],
  siteURL: string,
): T[] {
  return drafts.map((item, index) => {
    if (
      index !== 0 ||
      item.id != null ||
      item.group_name.trim() ||
      item.api_key.trim() ||
      item.models.trim()
    ) {
      return item
    }
    return {
      ...item,
      group_name: "direct",
      base_url: item.base_url.trim() ? item.base_url : siteURL.trim(),
    }
  })
}

export function buildDirectCredentialPayload(
  drafts: DirectCredentialDraft[],
): DirectCredentialInput[] {
  return drafts.map((item) => {
    const apiKey = item.api_key.trim()
    return {
      ...(item.id != null ? { id: item.id } : {}),
      group_name: item.group_name.trim(),
      base_url: item.base_url.trim(),
      ...(apiKey ? { api_key: apiKey } : {}),
      models: directModelsPayload(item.models),
      ratio: Number(item.ratio),
      completion_ratio: Number(item.completion_ratio),
      enabled: item.enabled,
    }
  })
}

export type DirectCredentialValidationIssue = {
  message: string
  index: number
  field: "direct-group-name" | "direct-base-url" | "direct-api-key" | "direct-models" | "direct-input-ratio" | "direct-output-ratio"
}

export function directCredentialValidationIssue(drafts: DirectCredentialDraft[]): DirectCredentialValidationIssue | null {
  if (drafts.length === 0) return { message: "直连 API 模式至少需要一个分组", index: 0, field: "direct-group-name" }
  const names = new Set<string>()
  for (const [index, item] of drafts.entries()) {
    const issue = (message: string, field: DirectCredentialValidationIssue["field"]): DirectCredentialValidationIssue => ({ message, index, field })
    const groupNumber = index + 1
    const groupName = item.group_name.trim()
    const canonicalName = groupName.toLowerCase()
    if (!groupName) return issue(`直连分组 ${groupNumber} 的名称不能为空`, "direct-group-name")
    if (groupName.length > 128) return issue(`直连分组 ${groupNumber} 的名称不能超过 128 个字符`, "direct-group-name")
    if (names.has(canonicalName)) return issue(`直连分组 ${groupNumber} 的名称不能重复`, "direct-group-name")
    names.add(canonicalName)
    if (!item.base_url.trim()) return issue(`直连分组 ${groupNumber} 请填写 Base URL`, "direct-base-url")
    if (!item.api_key.trim() && (item.id == null || !item.key_configured)) return issue(`直连分组 ${groupNumber} 必须填写 API Key`, "direct-api-key")
    if (directModelsList(item.models).length === 0) return issue(`直连分组 ${groupNumber} 必须至少选择一个模型`, "direct-models")
    const ratio = Number(item.ratio)
    const completionRatio = Number(item.completion_ratio)
    if (!Number.isFinite(ratio) || ratio <= 0) return issue(`直连分组 ${groupNumber} 的倍率必须是大于 0 的数值`, "direct-input-ratio")
    if (!Number.isFinite(completionRatio) || completionRatio <= 0) return issue(`直连分组 ${groupNumber} 的倍率必须是大于 0 的数值`, "direct-output-ratio")
  }
  return null
}

export function directCredentialValidationError(drafts: DirectCredentialDraft[]): string | null {
  return directCredentialValidationIssue(drafts)?.message ?? null
}

export function directCredentialsChanged(
  persisted: DirectCredential[] | undefined,
  drafts: DirectCredentialDraft[],
): boolean {
  if (!persisted || persisted.length !== drafts.length) return true

  return drafts.some((draft, index) => {
    const current = persisted[index]
    if (!current || draft.api_key.trim()) return true
    return (
      draft.id !== current.id ||
      draft.group_name.trim() !== current.group_name ||
      draft.base_url.trim() !== current.base_url ||
      directModelsChanged(current.models, draft.models) ||
      Number(draft.ratio) !== current.ratio ||
      Number(draft.completion_ratio) !== current.completion_ratio ||
      draft.enabled !== current.enabled
    )
  })
}

export interface ChannelEditableValues {
  name: string
  gateway_app: GatewayApp
  site_url: string
  username: string
  sort_order: number
  credential_mode: CredentialMode
  login_extra_params: string
  balance_threshold: number
  monitor_enabled: boolean
  turnstile_enabled: boolean
  ignore_announcements: boolean
  subscription_enabled: boolean
  network_proxy_id: number
  captcha_config_id: number | null
}

export function channelEditableValues(channel: Channel): ChannelEditableValues {
  return {
    name: channel.name,
    gateway_app: channel.gateway_app,
    site_url: channel.site_url,
    username: channel.username,
    sort_order: channel.sort_order,
    credential_mode: channel.credential_mode,
    login_extra_params: channel.login_extra_params,
    balance_threshold: channel.balance_threshold,
    monitor_enabled: channel.monitor_enabled,
    turnstile_enabled: channel.turnstile_enabled,
    ignore_announcements: channel.ignore_announcements,
    subscription_enabled: channel.subscription_enabled,
    network_proxy_id: channel.network_proxy_id ?? 0,
    captcha_config_id: channel.captcha_config_id ?? null,
  }
}

// buildChannelUpdatePatch sends only values changed from the dialog's opening
// snapshot. expected_admin_revision is still included so a second writer can
// reject the save even when an older client accidentally resubmits full data.
export function buildChannelUpdatePatch(
  initial: ChannelEditableValues,
  next: ChannelEditableValues,
  expectedAdminRevision: number,
): Record<string, unknown> {
  const patch: Record<string, unknown> = {
    expected_admin_revision: expectedAdminRevision,
  }
  for (const key of Object.keys(initial) as Array<keyof ChannelEditableValues>) {
    if (!Object.is(initial[key], next[key])) {
      patch[key] = next[key]
    }
  }
  return patch
}
