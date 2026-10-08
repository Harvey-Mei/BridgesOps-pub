"use client"

import { HelpHint } from "@/components/ui/help-hint"

import { useEffect, useRef, useState, type FormEvent } from "react"
import { ExternalLink, HelpCircle, LogIn, Plus, RefreshCw, Trash2 } from "lucide-react"
import {
  Dialog,
  DialogContent,
  DialogBody,
  DialogError,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Button } from "@/components/ui/button"
import { Textarea } from "@/components/ui/textarea"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import { Checkbox } from "@/components/ui/checkbox"
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover"
import type { Channel, ChannelType, CredentialMode, GatewayApp } from "@/lib/api-types"
import { apiFetch } from "@/lib/api"
import {
  buildChannelUpdatePatch,
  buildDirectCredentialPayload,
  channelEditableValues,
  directCredentialsChanged,
  directCredentialValidationIssue,
  directModelsForForm,
  directModelsList,
  initializeDirectCredentialDrafts,
  type ChannelEditableValues,
} from "@/lib/channel-update-patch"
import { useTriggerRefresh } from "@/lib/refresh-context"
import { useCaptchaConfigs, useNetworkProxies } from "@/lib/queries"
import { cn } from "@/lib/utils"
import { NetworkProxySelector } from "@/components/network-proxy/network-proxy-selector"

interface ChannelFormDialogProps {
  open: boolean
  onOpenChange: (v: boolean) => void
  /** 编辑模式时传入；为空表示新增 */
  channel?: Channel | null
}

/**
 * FormState 是表单的所有可编辑字段。
 *
 * password 字段在 token 模式下不展示；
 * token 字段（cookie / user_id / access_token）在 password 模式下不展示。
 * 这些状态对应保留在内存里，方便用户来回切换不丢失输入。
 */
interface FormState {
  name: string
  type: ChannelType
  gateway_app: GatewayApp
  site_url: string
  username: string
  sort_order: string
  password: string
  login_extra_params: string

  credential_mode: CredentialMode
  // NewAPI token 模式
  // newapi_token_kind 决定用 Cookie 还是系统访问令牌走 NewAPI 鉴权
  newapi_token_kind: "cookie" | "access_token"
  newapi_cookie: string
  newapi_access_token: string
  newapi_user_id: string
  // Sub2API token 模式
  sub2api_access_token: string
  sub2api_refresh_token: string
  direct_credentials: DirectCredentialForm[]

  balance_threshold: string
  monitor_enabled: boolean
  turnstile_enabled: boolean
  ignore_announcements: boolean
  subscription_enabled: boolean
  network_proxy_id: number
  captcha_config_id: string // "" 表示不绑定
}

interface DirectCredentialForm {
  form_key: string
  id?: number
  group_name: string
  base_url: string
  api_key: string
  key_configured: boolean
  models: string
  model_options: string[]
  model_search: string
  ratio: string
  completion_ratio: string
  enabled: boolean
}

let nextDirectCredentialFormKey = 1

function newDirectCredentialForm(
  values: Partial<DirectCredentialForm> = {},
): DirectCredentialForm {
  const models = values.models ?? ""
  return {
    form_key: values.form_key ?? `direct-credential-${nextDirectCredentialFormKey++}`,
    id: values.id,
    group_name: values.group_name ?? "",
    base_url: values.base_url ?? "",
    api_key: values.api_key ?? "",
    key_configured: values.key_configured ?? false,
    models,
    model_options: values.model_options ?? directModelsList(models),
    model_search: values.model_search ?? "",
    ratio: values.ratio ?? "1",
    completion_ratio: values.completion_ratio ?? "1",
    enabled: values.enabled ?? true,
  }
}

function directCredentialForms(channel?: Channel | null): DirectCredentialForm[] {
  const configured = channel?.direct_credentials ?? []
  if (configured.length > 0) {
    return configured.map((item) => ({
      ...newDirectCredentialForm(),
      id: item.id,
      group_name: item.group_name,
      base_url: item.base_url || channel?.site_url || "",
      api_key: "",
      key_configured: item.key_configured,
      models: directModelsForForm(item.models),
      model_options: directModelsList(directModelsForForm(item.models)),
      ratio: String(item.ratio ?? 1),
      completion_ratio: String(item.completion_ratio ?? item.ratio ?? 1),
      enabled: item.enabled ?? true,
    }))
  }
  if (channel?.credential_mode === "direct") {
    return [newDirectCredentialForm({
      group_name: "direct",
      base_url: channel.site_url || "",
      api_key: "",
      key_configured: false,
      models: directModelsForForm(channel.direct_models),
      model_options: directModelsList(directModelsForForm(channel.direct_models)),
    })]
  }
  return [newDirectCredentialForm({ base_url: channel?.site_url ?? "" })]
}

function initialState(c?: Channel | null): FormState {
  return {
    name: c?.name ?? "",
    type: c?.type ?? "sub2api",
    gateway_app: c?.gateway_app ?? "codex",
    site_url: c?.site_url ?? "",
    username: c?.username ?? "",
    sort_order: c?.sort_order != null ? String(c.sort_order) : "1",
    password: "",
    login_extra_params: c?.login_extra_params ?? "",
    credential_mode: c?.credential_mode ?? "password",
    newapi_token_kind: "cookie",
    newapi_cookie: "",
    newapi_access_token: "",
    newapi_user_id: c?.type === "newapi" ? (c.user_id ?? "") : "",
    sub2api_access_token: "",
    sub2api_refresh_token: "",
    direct_credentials: directCredentialForms(c),
    balance_threshold: c?.balance_threshold != null ? String(c.balance_threshold) : "0.1",
    monitor_enabled: c?.monitor_enabled ?? true,
    turnstile_enabled: c?.turnstile_enabled ?? false,
    ignore_announcements: c?.ignore_announcements ?? false,
    subscription_enabled: c?.subscription_enabled ?? false,
    network_proxy_id: c?.network_proxy_id ?? 0,
    captcha_config_id: c?.captcha_config_id != null ? String(c.captcha_config_id) : "",
  }
}

/**
 * buildTokenCredential 把当前表单里的 token 字段序列化成后端期望的 JSON 字符串。
 * 字段命名与 channel/service.go 里的 NewAPITokenCredential / Sub2APITokenCredential 对齐。
 */
function buildTokenCredential(form: FormState): string {
  if (form.type === "newapi") {
    if (form.newapi_token_kind === "access_token") {
      return JSON.stringify({
        access_token: form.newapi_access_token.trim(),
        user_id: form.newapi_user_id.trim(),
      })
    }
    return JSON.stringify({
      cookie: form.newapi_cookie.trim(),
      user_id: form.newapi_user_id.trim(),
    })
  }
  const refreshToken = form.sub2api_refresh_token.trim()
  return JSON.stringify({
    access_token: form.sub2api_access_token.trim(),
    ...(refreshToken ? { refresh_token: refreshToken } : {}),
  })
}

export function ChannelFormDialog({ open, onOpenChange, channel }: ChannelFormDialogProps) {
  const [form, setForm] = useState<FormState>(() => initialState(channel))
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [manualTakeover, setManualTakeover] = useState(false)
  const [loadingDirectModelKeys, setLoadingDirectModelKeys] = useState<Set<string>>(
    () => new Set(),
  )
  const directModelRequests = useRef(new Map<string, AbortController>())
  const refresh = useTriggerRefresh()
  const captchas = useCaptchaConfigs(open)
  const networkProxies = useNetworkProxies(open)

  // 打开 / 切换目标渠道时重置表单。
  useEffect(() => {
    const requests = directModelRequests.current
    for (const controller of requests.values()) controller.abort()
    requests.clear()
    if (open) {
      const next = initialState(channel)
      setForm(next)
      setError(null)
      setManualTakeover(false)
      setLoadingDirectModelKeys(new Set())
    }
    return () => {
      for (const controller of requests.values()) controller.abort()
      requests.clear()
    }
  }, [open, channel])

  const isEdit = !!channel
  const isTokenMode = form.credential_mode === "token"
  const isDirectMode = form.credential_mode === "direct"
  const loadingDirectModels = loadingDirectModelKeys.size > 0
  const supportsSubscriptionUsage =
    !isDirectMode && (form.type === "sub2api" || form.type === "newapi")
  // 编辑模式下，若 credential_mode 没变，token / password 都可以留空表示不修改。
  const modeChanged = isEdit && form.credential_mode !== (channel?.credential_mode ?? "password")
  const existingNewAPIUserID = isEdit && channel?.type === "newapi" ? (channel.user_id ?? "").trim() : ""
  function updateDirectCredential(formKey: string, patch: Partial<DirectCredentialForm>) {
    setForm((current) => ({
      ...current,
      direct_credentials: current.direct_credentials.map((item) =>
        item.form_key === formKey ? { ...item, ...patch } : item,
      ),
    }))
  }

  function updateDirectCredentialModels(formKey: string, models: string) {
    setForm((current) => ({
      ...current,
      direct_credentials: current.direct_credentials.map((item) =>
        item.form_key === formKey
          ? {
              ...item,
              models,
              model_options: Array.from(new Set([
                ...item.model_options,
                ...directModelsList(models),
              ])),
            }
          : item,
      ),
    }))
  }

  function setSelectedDirectModels(formKey: string, models: string[]) {
    updateDirectCredential(formKey, {
      models: Array.from(new Set(models.map((model) => model.trim()).filter(Boolean))).join("\n"),
    })
  }

  function addDirectCredential() {
    setForm((current) => ({
      ...current,
      direct_credentials: [
        ...current.direct_credentials,
        newDirectCredentialForm({
          base_url: current.direct_credentials[0]?.base_url || current.site_url,
        }),
      ],
    }))
  }

  function removeDirectCredential(formKey: string) {
    directModelRequests.current.get(formKey)?.abort()
    directModelRequests.current.delete(formKey)
    setLoadingDirectModelKeys((current) => {
      const next = new Set(current)
      next.delete(formKey)
      return next
    })
    setForm((current) => ({
      ...current,
      direct_credentials: current.direct_credentials.filter((item) => item.form_key !== formKey),
    }))
  }

  async function loadCredentialModels(item: DirectCredentialForm, index: number) {
    if (!item.base_url.trim()) {
      setError(`分组 ${index + 1} 请填写 Base URL`)
      return
    }
    const persisted = channel?.direct_credentials?.find((current) => current.id === item.id)
    const canUseSavedKey = Boolean(
      isEdit &&
      channel?.credential_mode === "direct" &&
      !modeChanged &&
      item.id != null &&
      item.key_configured &&
      persisted,
    )
    if (!item.api_key.trim() && !canUseSavedKey) {
      setError(`分组 ${index + 1} 请填写 API Key`)
      return
    }
    const savedTargetChanged = canUseSavedKey && (
      item.base_url.trim() !== persisted!.base_url.trim() ||
      Number(form.network_proxy_id || 0) !== Number(channel?.network_proxy_id ?? 0)
    )
    if (!item.api_key.trim() && savedTargetChanged) {
      setError(`分组 ${index + 1} 修改 Base URL 或网络代理后，需要重新填写 API Key 才能读取模型`)
      return
    }

    directModelRequests.current.get(item.form_key)?.abort()
    const controller = new AbortController()
    directModelRequests.current.set(item.form_key, controller)
    setLoadingDirectModelKeys((current) => new Set(current).add(item.form_key))
    setError(null)
    try {
      const models = item.api_key.trim()
        ? await apiFetch<string[]>("/channels/direct-models", {
            method: "POST",
            signal: controller.signal,
            body: JSON.stringify({
              base_url: item.base_url.trim(),
              api_key: item.api_key.trim(),
              network_proxy_id: form.network_proxy_id || null,
            }),
          })
        : await apiFetch<string[]>(
            `/channels/${channel!.id}/direct-credentials/${item.id}/models`,
            { method: "POST", signal: controller.signal, body: JSON.stringify({}) },
          )
      if (directModelRequests.current.get(item.form_key) !== controller) return
      const normalizedModels = Array.from(
        new Set(models.map((model) => model.trim()).filter(Boolean)),
      )
      setForm((current) => ({
        ...current,
        direct_credentials: current.direct_credentials.map((currentItem) => {
          if (currentItem.form_key !== item.form_key) return currentItem
          const selected = directModelsList(currentItem.models)
          return {
            ...currentItem,
            models: selected.length > 0 ? currentItem.models : normalizedModels.join("\n"),
            model_options: Array.from(new Set([...normalizedModels, ...selected])),
          }
        }),
      }))
    } catch (e) {
      if ((e as Error).name === "AbortError") return
      setError((e as Error).message || "读取模型失败")
    } finally {
      if (directModelRequests.current.get(item.form_key) === controller) {
        directModelRequests.current.delete(item.form_key)
        setLoadingDirectModelKeys((current) => {
          const next = new Set(current)
          next.delete(item.form_key)
          return next
        })
      }
    }
  }
  async function handleSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault()
    if (submitting) return
    const formElement = e.currentTarget
    let invalidFieldID = ""
    function invalid(message: string, fieldID: string): never {
      invalidFieldID = fieldID
      throw new Error(message)
    }
    setError(null)
    setSubmitting(true)
    try {
      const threshold = Number(form.balance_threshold)
      if (!Number.isFinite(threshold) || threshold < 0.1) {
        invalid("余额阈值不能低于 0.1", "threshold")
      }
      const sortOrder = Number(form.sort_order)
      if (!Number.isInteger(sortOrder)) {
        invalid("排序必须是整数", "sort-order")
      }
      const loginExtraParams = isTokenMode || isDirectMode ? "" : form.login_extra_params.trim()
      if (loginExtraParams) {
        let parsed: unknown
        try {
          parsed = JSON.parse(loginExtraParams)
        } catch {
          invalid("附加表单参数 JSON 格式错误", "login-extra-params")
        }
        if (!parsed || Array.isArray(parsed) || typeof parsed !== "object") {
          invalid("附加表单参数必须是 JSON 对象", "login-extra-params")
        }
      }

      const directCredentials = isDirectMode
        ? buildDirectCredentialPayload(form.direct_credentials)
        : []
      const firstDirectCredential = form.direct_credentials[0]
      if (isDirectMode) {
        const issue = directCredentialValidationIssue(form.direct_credentials)
        if (issue) invalid(issue.message, `${issue.field}-${form.direct_credentials[issue.index]?.form_key}`)
      }

      // token 模式：用户填的字段对应不同 connector 的 token JSON
      let tokenCredential = ""
      if (isTokenMode) {
        if (form.type === "newapi") {
          const credField =
            form.newapi_token_kind === "access_token"
              ? form.newapi_access_token.trim()
              : form.newapi_cookie.trim()
          const userID = form.newapi_user_id.trim()
          const userIDChanged = isEdit ? userID !== existingNewAPIUserID : userID !== ""
          if (!isEdit || modeChanged || credField || userIDChanged) {
            if (!credField) {
              throw new Error(
                form.newapi_token_kind === "access_token"
                  ? "NewAPI token 模式必须填写系统访问令牌"
                  : "NewAPI token 模式必须填写 Cookie",
              )
            }
            if (!userID) invalid("NewAPI token 模式必须填写 User ID", "newapi-user-id")
          }
        } else {
          if (!isEdit || modeChanged || form.sub2api_access_token || form.sub2api_refresh_token) {
            if (!form.sub2api_access_token.trim())
              invalid("Sub2API token 模式必须填写 Access Token", "sub2api-token")
          }
        }
        // 只在用户填写了字段、或者首次创建、或者切换模式时下发 token_credential
        const newapiCredFilled =
          form.newapi_token_kind === "access_token"
            ? form.newapi_access_token
            : form.newapi_cookie
        const newapiUserIDChanged =
          form.type === "newapi" && form.newapi_user_id.trim() !== existingNewAPIUserID
        if (
          !isEdit ||
          modeChanged ||
          newapiCredFilled ||
          newapiUserIDChanged ||
          form.sub2api_access_token ||
          form.sub2api_refresh_token
        ) {
          tokenCredential = buildTokenCredential(form)
        }
      }

      // 打码 provider 只在 password 模式 + 启用 Turnstile 时生效
      const useCaptcha = !isTokenMode && !isDirectMode && form.turnstile_enabled
      const captchaConfigID =
        useCaptcha && form.captcha_config_id ? Number(form.captcha_config_id) : null
      if (useCaptcha && captchaConfigID == null) {
        invalid("启用 Turnstile 时必须选择一个打码 provider", "captcha-config")
      }

      // password 模式下的密码校验
      if (!isTokenMode) {
        if (!isDirectMode) {
          if (!isEdit && !form.password) invalid("新建时必须填写密码", "password")
          if (modeChanged && !form.password) invalid("切换到账号密码模式时必须填写密码", "password")
        }
      }

      if (isEdit) {
        const nextValues: ChannelEditableValues = {
          name: form.name,
          gateway_app: form.gateway_app,
          site_url: isDirectMode && firstDirectCredential ? firstDirectCredential.base_url.trim() : form.site_url,
          username: form.username,
          sort_order: sortOrder,
          credential_mode: form.credential_mode,
          login_extra_params: loginExtraParams,
          balance_threshold: threshold,
          monitor_enabled: form.monitor_enabled,
          turnstile_enabled: !isTokenMode && !isDirectMode && form.turnstile_enabled,
          ignore_announcements: form.ignore_announcements,
          subscription_enabled: supportsSubscriptionUsage && form.subscription_enabled,
          network_proxy_id: form.network_proxy_id,
          captcha_config_id: captchaConfigID,
        }
        const body = buildChannelUpdatePatch(
          channelEditableValues(channel!),
          nextValues,
          channel!.admin_revision,
        )
        if (!isTokenMode && !isDirectMode && form.password) body.password = form.password
        if (isTokenMode && tokenCredential) body.token_credential = tokenCredential
        if (
          isDirectMode &&
          (modeChanged || directCredentialsChanged(channel?.direct_credentials, form.direct_credentials))
        ) {
          body.direct_credentials = directCredentials
        }
        if (Object.keys(body).length > 1) {
          await apiFetch(`/channels/${channel!.id}`, {
            method: "PUT",
            body: JSON.stringify(body),
          })
        }
      } else {
        await apiFetch(`/channels`, {
          method: "POST",
          body: JSON.stringify({
            name: form.name,
            type: form.type,
            gateway_app: form.gateway_app,
            site_url: isDirectMode && firstDirectCredential ? firstDirectCredential.base_url.trim() : form.site_url,
            username: form.username,
            sort_order: sortOrder,
            credential_mode: form.credential_mode,
            login_extra_params: loginExtraParams,
            password: isTokenMode || isDirectMode ? "" : form.password,
            token_credential: isTokenMode ? tokenCredential : "",
            ...(isDirectMode ? { direct_credentials: directCredentials } : {}),
            balance_threshold: threshold,
            monitor_enabled: form.monitor_enabled,
            turnstile_enabled: !isTokenMode && !isDirectMode && form.turnstile_enabled,
            ignore_announcements: form.ignore_announcements,
            subscription_enabled: supportsSubscriptionUsage && form.subscription_enabled,
            network_proxy_id: form.network_proxy_id,
            captcha_config_id: captchaConfigID,
          }),
        })
      }
      onOpenChange(false)
      refresh()
    } catch (e) {
      const err = e as Error
      setError(err.message || "保存失败")
      if (invalidFieldID) window.requestAnimationFrame(() => {
        const field = formElement.querySelector<HTMLElement>(`[id="${invalidFieldID}"]`)
        const details = field?.closest("details")
        if (details) details.open = true
        field?.focus()
        field?.scrollIntoView({ block: "nearest" })
      })
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange} busy={submitting}>
      <DialogContent formLayout className="sm:w-[min(896px,calc(100vw-3rem))] sm:max-w-none">
        <DialogHeader>
          <DialogTitle>{isEdit ? `${channel.name} · 编辑渠道` : "新增渠道"}</DialogTitle>
          <DialogDescription>
            {isDirectMode
              ? "配置 OpenAI 兼容 Base URL、API Key 和允许路由的模型。"
              : isEdit
                ? "修改凭据后会清空已缓存的登录会话。"
                : "添加上游账号，开启监控后将按计划自动登录。"}
          </DialogDescription>
        </DialogHeader>

        <form onSubmit={handleSubmit} onInvalidCapture={event => { const details = (event.target as HTMLElement).closest("details"); if (details) details.open = true }}>
          <DialogBody className="space-y-4">
          <div className="grid gap-4 sm:grid-cols-2">
          <div className="grid grid-cols-1 gap-3 sm:col-span-2 sm:grid-cols-[1fr_120px]">
            <div className="space-y-1.5">
              <Label htmlFor="name">渠道名</Label>
              <Input
                id="name"
                value={form.name}
                onChange={(e) => setForm({ ...form, name: e.target.value })}
                required
                disabled={submitting}
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="sort-order">排序</Label>
              <Input
                id="sort-order"
                type="number"
                step="1"
                value={form.sort_order}
                onChange={(e) => setForm({ ...form, sort_order: e.target.value })}
                disabled={submitting}
              />
            </div>
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="type" className="h-5">类型</Label>
            <Select
              value={form.type}
              onValueChange={(v) => setForm({ ...form, type: v as ChannelType })}
              disabled={isEdit || submitting}
            >
              <SelectTrigger id="type" className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="sub2api">Sub2API</SelectItem>
                <SelectItem value="newapi">NewAPI</SelectItem>
              </SelectContent>
            </Select>
            {isEdit ? (
              <p className="text-[11px] text-muted-foreground">类型创建后不可修改</p>
            ) : null}
          </div>

          <div className="space-y-1.5">
            <div className="flex items-center gap-1"><Label htmlFor="gateway-app">网关协议</Label><HelpHint label="网关协议">OpenAI 兼容协议支持 GPT、Grok、DeepSeek 等模型，同步后按实际模型区分。</HelpHint></div>
            <Select
              value={form.gateway_app}
              onValueChange={(v) => setForm({ ...form, gateway_app: v as GatewayApp })}
              disabled={submitting}
            >
              <SelectTrigger id="gateway-app" className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="codex">OpenAI 兼容（GPT / Grok / DeepSeek）</SelectItem>
                <SelectItem value="claude">Claude</SelectItem>
                <SelectItem value="gemini">Gemini</SelectItem>
              </SelectContent>
            </Select>

          </div>

          {!isDirectMode ? (
            <div className="space-y-1.5 sm:col-span-2">
              <div className="flex items-center gap-1"><Label htmlFor="site_url">站点地址</Label><HelpHint label="站点地址">填写站点主页；前后端分域时会自动识别 API 地址。</HelpHint></div>
              <Input
                id="site_url"
                placeholder="https://example.com"
                value={form.site_url}
                onChange={(e) => setForm({ ...form, site_url: e.target.value })}
                required
                disabled={submitting || loadingDirectModels}
              />

            </div>
          ) : null}

          </div>

          {/* 凭据类型 toggle */}
          <div className="space-y-1.5">
            <Label>凭据类型</Label>
            <div className="grid grid-cols-1 gap-2 rounded-lg border border-border p-1 sm:grid-cols-3">
              <button
                type="button"
                disabled={submitting || loadingDirectModels}
                onClick={() => setForm({ ...form, credential_mode: "password" })}
                className={cn(
                  "rounded-md px-3 py-1.5 text-xs font-medium transition-colors",
                  !isTokenMode && !isDirectMode
                    ? "bg-accent text-accent-foreground"
                    : "text-muted-foreground hover:bg-muted",
                )}
              >
                账号密码
              </button>
              <button
                type="button"
                disabled={submitting || loadingDirectModels}
                onClick={() => setForm({ ...form, credential_mode: "token" })}
                className={cn(
                  "rounded-md px-3 py-1.5 text-xs font-medium transition-colors",
                  isTokenMode
                    ? "bg-accent text-accent-foreground"
                    : "text-muted-foreground hover:bg-muted",
                )}
              >
                Token (跳过登录)
              </button>
              <button
                type="button"
                disabled={submitting || loadingDirectModels}
                onClick={() => setForm((current) => ({
                  ...current,
                  credential_mode: "direct",
                  direct_credentials: initializeDirectCredentialDrafts(
                    current.direct_credentials,
                    current.site_url,
                  ),
                }))}
                className={cn(
                  "rounded-md px-3 py-1.5 text-xs font-medium transition-colors",
                  isDirectMode
                    ? "bg-accent text-accent-foreground"
                    : "text-muted-foreground hover:bg-muted",
                )}
              >
                直连 API
              </button>
            </div>
            <p className="text-[11px] text-muted-foreground">
              {isDirectMode
                ? "直接调用兼容 OpenAI API 的 Base URL；不会尝试登录或访问上游管理后台。"
                : isTokenMode
                ? form.type === "sub2api"
                  ? "粘贴浏览器里已登录后的 Token；保存 refresh token 后会自动续期，refresh token 失效时需重新接管。"
                  : "粘贴浏览器里已登录后的 Token / Cookie；凭据失效时需要手动重新粘贴。"
                : "提供账号密码，系统自动登录并续期。可能需要配打码 provider。"}
            </p>
            {form.type === "sub2api" && !isTokenMode && !isDirectMode ? (
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="w-full justify-center"
                disabled={submitting || loadingDirectModels}
                onClick={() => {
                  setForm({ ...form, credential_mode: "token" })
                  setManualTakeover(true)
                }}
              >
                <LogIn className="size-3.5" />
                Turnstile 免费方案：手动登录接管
              </Button>
            ) : null}
          </div>

          {/* —— direct API 模式字段 —— */}
          {isDirectMode ? (
            <>
              <div className="flex items-center justify-between">
                <Label>直连分组配置</Label>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={addDirectCredential}
                  disabled={submitting}
                >
                  <Plus className="size-3.5" /> 添加分组
                </Button>
              </div>
              <div className="space-y-3">
                {form.direct_credentials.map((item, index) => {
                  const selectedModels = directModelsList(item.models)
                  const selectedModelSet = new Set(selectedModels)
                  const query = item.model_search.trim().toLowerCase()
                  const visibleModels = item.model_options.filter(
                    (model) => !query || model.toLowerCase().includes(query),
                  )
                  const loadingModels = loadingDirectModelKeys.has(item.form_key)
                  const fieldID = (name: string) => `${name}-${item.form_key}`
                  return (
                    <section
                      key={item.form_key}
                      aria-labelledby={fieldID("direct-group-title")}
                      className="space-y-3 rounded-md border border-border p-3"
                    >
                      <div className="flex items-center justify-between gap-3">
                        <h3 id={fieldID("direct-group-title")} className="text-sm font-medium">
                          分组 {index + 1}
                        </h3>
                        <Button
                          type="button"
                          variant="ghost"
                          size="icon"
                          aria-label={`删除直连分组 ${index + 1}`}
                          title="删除分组"
                          onClick={() => removeDirectCredential(item.form_key)}
                          disabled={submitting || loadingModels || form.direct_credentials.length <= 1}
                        >
                          <Trash2 className="size-4" />
                        </Button>
                      </div>

                      <div className="grid grid-cols-1 gap-3 md:grid-cols-[minmax(0,0.8fr)_minmax(0,1.4fr)]">
                        <div className="space-y-1.5">
                          <Label htmlFor={fieldID("direct-group-name")}>分组名称</Label>
                          <Input
                            id={fieldID("direct-group-name")}
                            value={item.group_name}
                            onChange={(e) => updateDirectCredential(item.form_key, { group_name: e.target.value })}
                            disabled={submitting || loadingModels}
                          />
                        </div>
                        <div className="space-y-1.5">
                          <Label htmlFor={fieldID("direct-base-url")}>Base URL</Label>
                          <Input
                            id={fieldID("direct-base-url")}
                            type="url"
                            placeholder="https://api.example.com"
                            value={item.base_url}
                            onChange={(e) => updateDirectCredential(item.form_key, { base_url: e.target.value })}
                            disabled={submitting || loadingModels}
                          />
                        </div>
                      </div>

                      <div className="grid grid-cols-1 gap-3 md:grid-cols-[minmax(0,1fr)_9rem_9rem]">
                        <div className="space-y-1.5">
                          <Label htmlFor={fieldID("direct-api-key")}>API Key</Label>
                          <Input
                            id={fieldID("direct-api-key")}
                            type="password"
                            placeholder={item.key_configured ? "留空则保留已保存的 Key" : "填写 API Key"}
                            value={item.api_key}
                            onChange={(e) => updateDirectCredential(item.form_key, { api_key: e.target.value })}
                            autoComplete="new-password"
                            data-1p-ignore="true"
                            data-lpignore="true"
                            disabled={submitting || loadingModels}
                          />
                          {item.key_configured ? (
                            <p className="text-[11px] text-muted-foreground">已保存，留空则保留</p>
                          ) : null}
                        </div>
                        <div className="space-y-1.5">
                          <Label htmlFor={fieldID("direct-input-ratio")}>输入倍率</Label>
                          <Input
                            id={fieldID("direct-input-ratio")}
                            type="number"
                            min="0.000001"
                            step="any"
                            value={item.ratio}
                            onChange={(e) => updateDirectCredential(item.form_key, { ratio: e.target.value })}
                            disabled={submitting}
                          />
                        </div>
                        <div className="space-y-1.5">
                          <Label htmlFor={fieldID("direct-output-ratio")}>输出倍率</Label>
                          <Input
                            id={fieldID("direct-output-ratio")}
                            type="number"
                            min="0.000001"
                            step="any"
                            value={item.completion_ratio}
                            onChange={(e) => updateDirectCredential(item.form_key, { completion_ratio: e.target.value })}
                            disabled={submitting}
                          />
                        </div>
                      </div>

                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <div className="flex items-center gap-2">
                          <Switch
                            id={fieldID("direct-enabled")}
                            checked={item.enabled}
                            onCheckedChange={(enabled) => updateDirectCredential(item.form_key, { enabled })}
                            disabled={submitting}
                          />
                          <Label htmlFor={fieldID("direct-enabled")} className="text-xs font-normal">
                            参与路由
                          </Label>
                        </div>
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          onClick={() => loadCredentialModels(item, index)}
                          disabled={submitting || loadingModels}
                        >
                          <RefreshCw className={cn("size-3.5", loadingModels && "animate-spin")} />
                          读取模型
                        </Button>
                      </div>

                      {item.model_options.length > 0 ? (
                        <div className="space-y-2 border-t border-border pt-3">
                          <div className="flex flex-wrap items-center gap-2">
                            <Input
                              value={item.model_search}
                              onChange={(e) => updateDirectCredential(item.form_key, { model_search: e.target.value })}
                              placeholder="筛选模型 ID"
                              aria-label={`筛选直连分组 ${index + 1} 的模型`}
                              className="h-8 min-w-44 flex-1 text-xs"
                              disabled={submitting}
                            />
                            <Button
                              type="button"
                              variant="ghost"
                              size="sm"
                              onClick={() => setSelectedDirectModels(item.form_key, [...selectedModels, ...visibleModels])}
                              disabled={submitting || visibleModels.length === 0}
                            >
                              全选
                            </Button>
                            <Button
                              type="button"
                              variant="ghost"
                              size="sm"
                              onClick={() => {
                                const visibleSet = new Set(visibleModels)
                                setSelectedDirectModels(
                                  item.form_key,
                                  selectedModels.filter((model) => !visibleSet.has(model)),
                                )
                              }}
                              disabled={submitting || visibleModels.length === 0 || selectedModels.length === 0}
                            >
                              清空可见
                            </Button>
                          </div>
                          <div className="max-h-36 space-y-1 overflow-y-auto pr-1">
                            {visibleModels.map((model) => (
                              <label
                                key={model}
                                className="flex min-w-0 cursor-pointer items-center gap-2 rounded px-1 py-1 text-xs hover:bg-muted"
                              >
                                <Checkbox
                                  checked={selectedModelSet.has(model)}
                                  onCheckedChange={(checked) => setSelectedDirectModels(
                                    item.form_key,
                                    checked === true
                                      ? [...selectedModels, model]
                                      : selectedModels.filter((selected) => selected !== model),
                                  )}
                                  disabled={submitting}
                                />
                                <span className="truncate font-mono" title={model}>{model}</span>
                              </label>
                            ))}
                            {visibleModels.length === 0 ? (
                              <p className="px-1 py-2 text-xs text-muted-foreground">没有匹配模型</p>
                            ) : null}
                          </div>
                          <p className="text-[11px] text-muted-foreground">
                            已选 {selectedModels.length} / {item.model_options.length}
                          </p>
                        </div>
                      ) : null}

                      <div className="space-y-1.5">
                        <Label htmlFor={fieldID("direct-models")}>模型 ID</Label>
                        <Textarea
                          id={fieldID("direct-models")}
                          placeholder={"每行一个模型 ID，例如：\ngpt-4o\nclaude-3-7-sonnet"}
                          value={item.models}
                          onChange={(e) => updateDirectCredentialModels(item.form_key, e.target.value)}
                          rows={3}
                          className="field-sizing-fixed text-xs font-mono"
                          disabled={submitting}
                        />
                      </div>
                    </section>
                  )
                })}
              </div>
            </>
          ) : !isTokenMode ? (
            <>
              <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                <div className="space-y-1.5">
                  <Label htmlFor="username">账号 / 邮箱</Label>
                  <Input
                    id="username"
                    value={form.username}
                    onChange={(e) => setForm({ ...form, username: e.target.value })}
                    required
                    disabled={submitting}
                  />
                </div>
                <div className="space-y-1.5">
                  <Label htmlFor="password">
                    {isEdit ? "新密码 (留空不变)" : "密码"}
                  </Label>
                  <Input
                    id="password"
                    type="password"
                    value={form.password}
                    onChange={(e) => setForm({ ...form, password: e.target.value })}
                    required={!isEdit || modeChanged}
                    disabled={submitting}
                  />
                </div>
              </div>
            </>
          ) : null}

          {/* —— token 模式字段 —— */}
          {isTokenMode ? (
            <>
              <div className="space-y-1.5">
                <Label htmlFor="username-display">备注（可选）</Label>
                <Input
                  id="username-display"
                  placeholder="如：worry@example.com"
                  value={form.username}
                  onChange={(e) => setForm({ ...form, username: e.target.value })}
                  disabled={submitting}
                />
                <p className="text-[11px] text-muted-foreground">
                  仅作展示，不参与鉴权
                </p>
              </div>

              {form.type === "newapi" ? (
                <>
                  <div className="space-y-1.5">
                    <Label>鉴权方式</Label>
                    <div className="grid grid-cols-1 gap-2 rounded-lg border border-border p-1 sm:grid-cols-2">
                      <button
                        type="button"
                        disabled={submitting}
                        onClick={() => setForm({ ...form, newapi_token_kind: "cookie" })}
                        className={cn(
                          "rounded-md px-3 py-1.5 text-xs font-medium transition-colors",
                          form.newapi_token_kind === "cookie"
                            ? "bg-accent text-accent-foreground"
                            : "text-muted-foreground hover:bg-muted",
                        )}
                      >
                        Cookie (session)
                      </button>
                      <button
                        type="button"
                        disabled={submitting}
                        onClick={() => setForm({ ...form, newapi_token_kind: "access_token" })}
                        className={cn(
                          "rounded-md px-3 py-1.5 text-xs font-medium transition-colors",
                          form.newapi_token_kind === "access_token"
                            ? "bg-accent text-accent-foreground"
                            : "text-muted-foreground hover:bg-muted",
                        )}
                      >
                        系统访问令牌
                      </button>
                    </div>
                    <p className="text-[11px] text-muted-foreground">
                      {form.newapi_token_kind === "access_token"
                        ? "对应 NewAPI 个人设置页生成的「系统访问令牌」(user.access_token)，请求走 Authorization 头。"
                        : "浏览器登录后的整段 Cookie，典型形如 session=MTc4...; ..."}
                    </p>
                  </div>

                  {form.newapi_token_kind === "cookie" ? (
                    <div className="space-y-1.5">
                      <div className="flex items-center justify-between">
                        <Label htmlFor="newapi-cookie">Cookie</Label>
                        <NewAPITokenHelp kind="cookie" />
                      </div>
                      <Textarea
                        id="newapi-cookie"
                        placeholder={
                          isEdit
                            ? "留空 = 不修改；填写则覆盖原 token"
                            : "粘贴整段 Cookie 字符串，例：session=...; ..."
                        }
                        value={form.newapi_cookie}
                        onChange={(e) => setForm({ ...form, newapi_cookie: e.target.value })}
                        rows={3}
                        className="field-sizing-fixed text-xs font-mono break-all"
                        disabled={submitting}
                      />
                    </div>
                  ) : (
                    <div className="space-y-1.5">
                      <div className="flex items-center justify-between">
                        <Label htmlFor="newapi-access-token">系统访问令牌</Label>
                        <NewAPITokenHelp kind="access_token" />
                      </div>
                      <Textarea
                        id="newapi-access-token"
                        placeholder={
                          isEdit
                            ? "留空 = 不修改；填写则覆盖原令牌"
                            : "粘贴 NewAPI 生成的 32 位系统访问令牌"
                        }
                        value={form.newapi_access_token}
                        onChange={(e) =>
                          setForm({ ...form, newapi_access_token: e.target.value })
                        }
                        rows={3}
                        className="field-sizing-fixed text-xs font-mono break-all"
                        disabled={submitting}
                      />
                    </div>
                  )}

                  <div className="space-y-1.5">
                    <Label htmlFor="newapi-user-id">User ID</Label>
                    <Input
                      id="newapi-user-id"
                      placeholder={
                        isEdit
                          ? "留空 = 不修改；NewAPI 个人设置页可见"
                          : "整数，NewAPI 个人设置页可见"
                      }
                      value={form.newapi_user_id}
                      onChange={(e) => setForm({ ...form, newapi_user_id: e.target.value })}
                      disabled={submitting}
                    />
                  </div>
                </>
              ) : null}

              {form.type === "sub2api" ? (
                <div className="space-y-3">
                  {manualTakeover ? (
                    <Sub2APIManualTakeover
                      siteURL={form.site_url}
                      onImport={(accessToken, refreshToken) =>
                        setForm({
                          ...form,
                          sub2api_access_token: accessToken,
                          sub2api_refresh_token: refreshToken,
                        })
                      }
                    />
                  ) : (
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      className="w-full justify-center"
                      onClick={() => setManualTakeover(true)}
                    >
                      <LogIn className="size-3.5" />
                      手动登录接管
                    </Button>
                  )}
                  <div className="space-y-1.5">
                    <div className="flex items-center justify-between">
                      <Label htmlFor="sub2api-token">Access Token</Label>
                      <Sub2APITokenHelp />
                    </div>
                    <Textarea
                      id="sub2api-token"
                      placeholder={
                        isEdit
                          ? "留空 = 不修改；填写则覆盖原 token"
                          : "粘贴 access_token"
                      }
                      value={form.sub2api_access_token}
                      onChange={(e) =>
                        setForm({ ...form, sub2api_access_token: e.target.value })
                      }
                      rows={3}
                      className="field-sizing-fixed text-xs font-mono break-all"
                      disabled={submitting}
                    />
                  </div>
                  <div className="space-y-1.5">
                    <Label htmlFor="sub2api-refresh-token">Refresh Token</Label>
                    <Textarea
                      id="sub2api-refresh-token"
                      placeholder={
                        isEdit
                          ? "留空 = 不修改；如填写需同时填写 Access Token"
                          : "粘贴 refresh_token（建议填写，用于后续续期）"
                      }
                      value={form.sub2api_refresh_token}
                      onChange={(e) =>
                        setForm({ ...form, sub2api_refresh_token: e.target.value })
                      }
                      rows={3}
                      className="field-sizing-fixed text-xs font-mono break-all"
                      disabled={submitting}
                    />
                  </div>
                </div>
              ) : null}
            </>
          ) : null}

          <details className="rounded-lg border border-border p-3">
            <summary className="cursor-pointer text-sm font-medium focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">监控与高级设置</summary>
            <div className="mt-4 space-y-3">
          {!isTokenMode && !isDirectMode ? (
            <div className="space-y-1.5">
              <Label htmlFor="login-extra-params">附加表单参数</Label>
              <Textarea
                id="login-extra-params"
                placeholder='{"device_id":"xxx"}'
                value={form.login_extra_params}
                onChange={(e) => setForm({ ...form, login_extra_params: e.target.value })}
                rows={3}
                className="field-sizing-fixed text-xs font-mono"
                disabled={submitting}
              />
              <p className="text-[11px] text-muted-foreground">
                用于非标准 Sub2API / NewAPI 魔改版增加的登录参数字段。
              </p>
            </div>
          ) : null}

          {!isDirectMode ? (
            <div className="space-y-1.5">
              <Label htmlFor="threshold" title="低于阈值后告警并暂停该渠道全部分组参与路由；余额恢复后自动启用">余额阈值</Label>
              <Input
                id="threshold"
                type="number"
                step="0.01"
                min="0.1"
                value={form.balance_threshold}
                onChange={(e) => setForm({ ...form, balance_threshold: e.target.value })}
                disabled={submitting}
              />
            </div>
          ) : null}

          <div className="flex items-center justify-between rounded-lg border border-border px-3 py-2">
            <div>
              <p className="text-sm font-medium">启用监控</p>
              <p className="text-xs text-muted-foreground">关闭后调度器不会扫描此渠道</p>
            </div>
            <Switch
              checked={form.monitor_enabled}
              onCheckedChange={(v) => setForm({ ...form, monitor_enabled: v })}
              disabled={submitting}
            />
          </div>

          {!isDirectMode ? (
            <div className="flex items-center justify-between rounded-lg border border-border px-3 py-2">
              <div>
                <p className="text-sm font-medium">忽略公告</p>
                <p className="text-xs text-muted-foreground">开启后不会拉取该渠道公告</p>
              </div>
              <Switch
                checked={form.ignore_announcements}
                onCheckedChange={(v) => setForm({ ...form, ignore_announcements: v })}
                disabled={submitting}
              />
            </div>
          ) : null}

          {supportsSubscriptionUsage ? (
            <div className="flex items-center justify-between rounded-lg border border-border px-3 py-2">
              <div>
                <p className="text-sm font-medium">{form.type === "newapi" ? "启用订阅用量" : "启用订阅管理"}</p>
                <p className="text-xs text-muted-foreground">{form.type === "newapi" ? "同步已购订阅的额度、重置时间与到期状态" : "开启后充值弹窗显示订阅购买和用量"}</p>
              </div>
              <Switch
                checked={form.subscription_enabled}
                onCheckedChange={(v) => setForm({ ...form, subscription_enabled: v })}
                disabled={submitting}
              />
            </div>
          ) : null}

          <NetworkProxySelector
            value={form.network_proxy_id}
            proxies={networkProxies.data ?? []}
            loading={networkProxies.loading}
            onValueChange={(networkProxyID) => setForm({ ...form, network_proxy_id: networkProxyID })}
            disabled={submitting || loadingDirectModels}
          />

          {/* Turnstile / 打码：token 模式下整段不展示 */}
          {!isTokenMode && !isDirectMode ? (
            <>
              <div className="flex items-center justify-between rounded-lg border border-border px-3 py-2">
                <div>
                  <p className="text-sm font-medium">Turnstile 人机校验</p>
                  <p className="text-xs text-muted-foreground">站点开启 Cloudflare Turnstile 时打开</p>
                </div>
                <Switch
                  checked={form.turnstile_enabled}
                  onCheckedChange={(v) => setForm({ ...form, turnstile_enabled: v })}
                  disabled={submitting}
                />
              </div>

              {form.turnstile_enabled ? (
                <div className="space-y-1.5">
                  <Label htmlFor="captcha-config">打码 provider</Label>
                  <Select
                    value={form.captcha_config_id}
                    onValueChange={(v) => setForm({ ...form, captcha_config_id: v })}
                    disabled={submitting}
                  >
                    <SelectTrigger id="captcha-config" className="w-full">
                      <SelectValue
                        placeholder={
                          captchas.data && captchas.data.length > 0
                            ? "选择 provider"
                            : "先到底部 [验证码服务] 卡片新增"
                        }
                      />
                    </SelectTrigger>
                    <SelectContent>
                      {(captchas.data ?? [])
                        .filter((c) => c.enabled)
                        .map((c) => (
                          <SelectItem key={c.id} value={String(c.id)}>
                            {c.name}
                          </SelectItem>
                        ))}
                    </SelectContent>
                  </Select>
                  <p className="text-[11px] text-muted-foreground">
                    {"siteKey 会自动从上游公开接口拉取，无需在此填写。"}
                  </p>
                </div>
              ) : null}
            </>
          ) : null}

            </div>
          </details>



          </DialogBody>
          {error ? <DialogError>{error}</DialogError> : null}
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)} disabled={submitting}>
              取消
            </Button>
            <Button type="submit" disabled={submitting || loadingDirectModels}>
              {submitting ? "保存中…" : "保存"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}

function NewAPITokenHelp({ kind = "cookie" }: { kind?: "cookie" | "access_token" }) {
  return (
    <Popover>
      <PopoverTrigger asChild>
        <button
          type="button"
          className="inline-flex items-center gap-1 text-[11px] text-muted-foreground hover:text-foreground"
        >
          <HelpCircle className="size-3" />
          如何获取？
        </button>
      </PopoverTrigger>
      <PopoverContent className="w-80 text-xs" align="end">
        {kind === "access_token" ? (
          <>
            <p className="font-medium text-foreground">获取系统访问令牌</p>
            <ol className="mt-1 ml-4 list-decimal space-y-0.5 text-muted-foreground">
              <li>在浏览器登录 NewAPI 站点，进入「个人设置」页</li>
              <li>找到「生成的系统访问令牌」区块</li>
              <li>点击生成并复制这串 32 位令牌（即 user.access_token）</li>
            </ol>
            <p className="mt-2 text-muted-foreground">
              connector 用 <span className="font-mono text-foreground">Authorization</span> 头携带此令牌，不再依赖 Cookie 续期。
            </p>
          </>
        ) : (
          <>
            <p className="font-medium text-foreground">获取 Cookie</p>
            <ol className="mt-1 ml-4 list-decimal space-y-0.5 text-muted-foreground">
              <li>在浏览器登录 NewAPI 站点</li>
              <li>按 F12 打开 DevTools，切到 Application / 存储 标签</li>
              <li>左侧 Cookies 选中站点域名</li>
              <li>复制 <span className="font-mono text-foreground">session</span> 字段值，格式：<span className="font-mono">session=xxxxx</span></li>
            </ol>
          </>
        )}
        <p className="mt-2 font-medium text-foreground">获取 User ID</p>
        <p className="mt-1 text-muted-foreground">
          登录 NewAPI 后到「个人设置」页查看用户 ID，或到 Application / Local Storage 里复制 <span className="font-mono">uid</span>。
        </p>
      </PopoverContent>
    </Popover>
  )
}

function Sub2APITokenHelp() {
  return (
    <Popover>
      <PopoverTrigger asChild>
        <button
          type="button"
          className="inline-flex items-center gap-1 text-[11px] text-muted-foreground hover:text-foreground"
        >
          <HelpCircle className="size-3" />
          如何获取？
        </button>
      </PopoverTrigger>
      <PopoverContent className="w-96 text-xs" align="end">
        <p className="font-medium text-foreground">获取 Access Token / Refresh Token</p>
        <ol className="mt-1 ml-4 list-decimal space-y-0.5 text-muted-foreground">
          <li>在浏览器登录 Sub2API 站点</li>
          <li>按 F12 打开 DevTools，切到 Application / 存储 标签</li>
          <li>左侧 Local Storage 选中站点域名</li>
          <li>复制 <span className="font-mono text-foreground">auth_token</span> 或 <span className="font-mono text-foreground">access_token</span></li>
          <li>同时复制 <span className="font-mono text-foreground">refresh_token</span>（如果站点已启用刷新令牌）</li>
        </ol>
        <p className="mt-2 font-medium text-foreground">登录后特征</p>
        <p className="mt-1 text-muted-foreground">
          标准 Sub2API 登录接口是 <span className="font-mono">/api/v1/auth/login</span>，成功后返回 <span className="font-mono">access_token</span>、<span className="font-mono">refresh_token</span> 和 <span className="font-mono">expires_in</span>，前端会把 access token 保存为 <span className="font-mono">auth_token</span>。
        </p>
        <p className="mt-2 text-[11px] text-muted-foreground">
          也可以在 Network 标签里找任意已登录接口的 <span className="font-mono">Authorization</span> 头，去掉 <span className="font-mono">Bearer </span> 前缀。
        </p>
      </PopoverContent>
    </Popover>
  )
}

function Sub2APIManualTakeover({
  siteURL,
  onImport,
}: {
  siteURL: string
  onImport: (accessToken: string, refreshToken: string) => void
}) {
  const [credentialJSON, setCredentialJSON] = useState("")
  const [parseError, setParseError] = useState<string | null>(null)
  const loginURL = `${siteURL.trim().replace(/\/$/, "")}/login`

  function importCredential() {
    setParseError(null)
    try {
      const parsed = JSON.parse(credentialJSON) as Record<string, unknown>
      const accessToken = String(parsed.access_token ?? parsed.auth_token ?? "").trim()
      const refreshToken = String(parsed.refresh_token ?? "").trim()
      if (!accessToken) throw new Error("JSON 中没有 access_token 或 auth_token")
      onImport(accessToken, refreshToken)
      setCredentialJSON("")
    } catch (error) {
      setParseError(error instanceof Error ? error.message : "凭据 JSON 格式错误")
    }
  }

  return (
    <div className="space-y-2 rounded-md border border-border bg-muted/40 p-3">
      <div className="flex items-center justify-between gap-3">
        <div>
          <p className="text-xs font-medium text-foreground">手动通过一次 Turnstile</p>
          <p className="mt-0.5 text-[11px] text-muted-foreground">不会绕过验证码；保存后 BridgeOps 会使用 refresh token 自动续期。</p>
        </div>
        {siteURL.trim() ? (
          <Button asChild type="button" variant="outline" size="sm" className="shrink-0">
            <a href={loginURL} target="_blank" rel="noreferrer">
              打开登录页 <ExternalLink className="size-3" />
            </a>
          </Button>
        ) : null}
      </div>
      <ol className="ml-4 list-decimal space-y-0.5 text-[11px] text-muted-foreground">
        <li>在上游网页完成登录和 Turnstile</li>
        <li>打开 DevTools Console，运行下面的命令</li>
        <li>把复制出的 JSON 粘贴到输入框并导入</li>
      </ol>
      <code className="block select-all overflow-x-auto rounded bg-background px-2 py-1.5 text-[10px] text-foreground">
        {'copy(JSON.stringify({access_token:localStorage.getItem("auth_token")||localStorage.getItem("access_token"),refresh_token:localStorage.getItem("refresh_token")}))'}
      </code>
      <div className="flex gap-2">
        <Textarea
          aria-label="登录凭据 JSON"
          placeholder='粘贴 {"access_token":"...","refresh_token":"..."}'
          value={credentialJSON}
          onChange={(event) => setCredentialJSON(event.target.value)}
          rows={2}
          className="field-sizing-fixed min-w-0 flex-1 text-xs font-mono"
        />
        <Button type="button" variant="secondary" size="sm" className="self-end" onClick={importCredential} disabled={!credentialJSON.trim()}>
          导入
        </Button>
      </div>
      {parseError ? <p className="text-[11px] text-destructive">{parseError}</p> : null}
    </div>
  )
}
