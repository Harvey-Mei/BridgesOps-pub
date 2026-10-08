"use client"

import { useEffect, useState, type FormEvent } from "react"
import { Copy, Loader2, Pencil, Plus, Search } from "lucide-react"
import { toast } from "sonner"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { Badge } from "@/components/ui/badge"
import { Switch } from "@/components/ui/switch"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { apiFetch } from "@/lib/api"
import { channelTypeLabel, dateTime, dateTimeInputValue, decimal, formatRatio, zonedDateTimeToISOString } from "@/lib/format"
import type {
  Channel,
  ChannelAPIKey,
  ChannelAPIKeyGroup,
  ChannelAPIKeyPage,
  ChannelAPIKeyReveal,
  ChannelAPIKeyStatus,
} from "@/lib/api-types"
import { cn } from "@/lib/utils"

interface ChannelAPIKeysDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  channel: Channel | null
}

type Mode = "list" | "create" | "edit"

interface KeyForm {
  name: string
  custom_key: string
  status: "active" | "disabled"
  group: string
  group_id: string
  remain_quota: string
  quota: string
  unlimited_quota: boolean
  expired_time: string
  expires_in_days: string
  expires_at: string
  model_limits_enabled: boolean
  model_limits: string
  allow_ips: string
  ip_whitelist: string
  ip_blacklist: string
  cross_group_retry: boolean
  rate_limit_5h: string
  rate_limit_1d: string
  rate_limit_7d: string
  reset_quota: boolean
  reset_rate_limit_usage: boolean
}

const PAGE_SIZE = 10

const emptyForm: KeyForm = {
  name: "",
  custom_key: "",
  status: "active",
  group: "",
  group_id: "",
  remain_quota: "",
  quota: "",
  unlimited_quota: false,
  expired_time: "-1",
  expires_in_days: "",
  expires_at: "",
  model_limits_enabled: false,
  model_limits: "",
  allow_ips: "",
  ip_whitelist: "",
  ip_blacklist: "",
  cross_group_retry: false,
  rate_limit_5h: "",
  rate_limit_1d: "",
  rate_limit_7d: "",
  reset_quota: false,
  reset_rate_limit_usage: false,
}

function formFromKey(key: ChannelAPIKey, channel: Channel): KeyForm {
  const base = { ...emptyForm }
  base.name = key.name ?? ""
  base.status = key.status === "disabled" ? "disabled" : "active"
  if (channel.type === "newapi") {
    base.group = key.group ?? ""
    base.remain_quota = key.quota ? String(key.quota) : "0"
    base.unlimited_quota = !!key.unlimited_quota
    base.expired_time = key.expired_time != null ? String(key.expired_time) : "-1"
    base.model_limits_enabled = !!key.model_limits_enabled
    base.model_limits = key.model_limits ?? ""
    base.allow_ips = key.allow_ips ?? ""
    base.cross_group_retry = !!key.cross_group_retry
  } else {
    base.group_id = key.group_id != null ? String(key.group_id) : ""
    base.quota = key.quota ? String(key.quota) : "0"
    base.expires_at = key.expires_at ? toDateTimeLocal(key.expires_at) : ""
    base.ip_whitelist = (key.ip_whitelist ?? []).join("\n")
    base.ip_blacklist = (key.ip_blacklist ?? []).join("\n")
    base.rate_limit_5h = key.rate_limit_5h ? String(key.rate_limit_5h) : "0"
    base.rate_limit_1d = key.rate_limit_1d ? String(key.rate_limit_1d) : "0"
    base.rate_limit_7d = key.rate_limit_7d ? String(key.rate_limit_7d) : "0"
  }
  return base
}

function groupDisplayName(key: ChannelAPIKey) {
  return key.group_name || key.group || (key.group_id != null ? `#${key.group_id}` : "—")
}

function statusLabel(status: string) {
  switch (status) {
    case "active":
      return "启用"
    case "disabled":
      return "停用"
    case "expired":
      return "已过期"
    case "quota_exhausted":
      return "额度耗尽"
    default:
      return status || "未知"
  }
}

function statusClass(status: string) {
  switch (status) {
    case "active":
      return "bg-success/10 text-success border-success/20"
    case "disabled":
      return "bg-muted text-muted-foreground border-border"
    case "expired":
    case "quota_exhausted":
      return "bg-warning/10 text-warning border-warning/20"
    default:
      return "bg-muted text-muted-foreground border-border"
  }
}

function splitLines(value: string): string[] {
  return value
    .split(/\r?\n|,/)
    .map((item) => item.trim())
    .filter(Boolean)
}

function optionalInt(value: string): number | undefined {
  const trimmed = value.trim()
  if (!trimmed) return undefined
  const n = Number(trimmed)
  if (!Number.isFinite(n)) throw new Error("请输入有效整数")
  return Math.trunc(n)
}

function optionalFloat(value: string): number | undefined {
  const trimmed = value.trim()
  if (!trimmed) return undefined
  const n = Number(trimmed)
  if (!Number.isFinite(n)) throw new Error("请输入有效数字")
  return n
}

function toDateTimeLocal(iso: string): string {
  const d = new Date(iso)
  return Number.isNaN(d.getTime()) ? "" : dateTimeInputValue(d)
}

function localDateTimeToISO(value: string): string | undefined {
  if (!value.trim()) return undefined
  const iso = zonedDateTimeToISOString(value)
  if (!iso) throw new Error("过期时间在当前时区中不存在或格式无效")
  return iso
}

function maskKey(key: string) {
  if (!key) return "—"
  if (key.length <= 12) return key
  return `${key.slice(0, 8)}…${key.slice(-6)}`
}

async function copyText(text: string, label = "已复制") {
  const writeClipboard = navigator.clipboard?.writeText?.bind(navigator.clipboard)
  if (writeClipboard) {
    try {
      await writeClipboard(text)
      toast.success(label)
      return
    } catch {
      // 线上非安全上下文或权限受限时走下面的 textarea 兜底。
    }
  }

  const textarea = document.createElement("textarea")
  textarea.value = text
  textarea.setAttribute("readonly", "")
  textarea.style.position = "fixed"
  textarea.style.left = "-9999px"
  textarea.style.top = "0"
  document.body.appendChild(textarea)
  textarea.select()
  textarea.setSelectionRange(0, text.length)
  const copied = document.execCommand("copy")
  document.body.removeChild(textarea)
  if (!copied) throw new Error("复制失败")
  toast.success(label)
}

export function ChannelAPIKeysDialog({
  open,
  onOpenChange,
  channel,
}: ChannelAPIKeysDialogProps) {
  const [mode, setMode] = useState<Mode>("list")
  const [editing, setEditing] = useState<ChannelAPIKey | null>(null)
  const [form, setForm] = useState<KeyForm>(emptyForm)
  const [page, setPage] = useState(1)
  const [reloadTick, setReloadTick] = useState(0)
  const [search, setSearch] = useState("")
  const [status, setStatus] = useState<ChannelAPIKeyStatus | "all">("all")
  const [data, setData] = useState<ChannelAPIKeyPage | null>(null)
  const [groups, setGroups] = useState<ChannelAPIKeyGroup[]>([])
  const [groupsLoading, setGroupsLoading] = useState(false)
  const [loading, setLoading] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [revealingID, setRevealingID] = useState<number | null>(null)
  const [revealedKeys, setRevealedKeys] = useState<Record<number, string>>({})

  const items = data?.items ?? []
  const totalPages = Math.max(1, data?.pages ?? 1)
  const isNewAPI = channel?.type === "newapi"
  const groupByName = new Map(groups.map((g) => [g.name, g]))
  const groupByID = new Map(groups.filter((g) => g.id != null).map((g) => [String(g.id), g]))

  useEffect(() => {
    if (!open) return
    setMode("list")
    setEditing(null)
    setForm(emptyForm)
    setPage(1)
    setSearch("")
    setStatus("all")
    setError(null)
    setRevealingID(null)
    setRevealedKeys({})
  }, [open, channel?.id])

  useEffect(() => {
    if (!open || !channel) return
    let cancelled = false
    setGroupsLoading(true)
    apiFetch<ChannelAPIKeyGroup[]>(`/channels/${channel.id}/api-keys/groups`)
      .then((res) => {
        if (cancelled) return
        setGroups(Array.isArray(res) ? res : [])
      })
      .catch((e) => {
        if (cancelled) return
        const err = e as Error
        toast.error(err.message || "加载分组失败")
        setGroups([])
      })
      .finally(() => {
        if (!cancelled) setGroupsLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [open, channel])

  useEffect(() => {
    if (!open || !channel || mode !== "list") return
    let cancelled = false
    const params = new URLSearchParams({
      page: String(page),
      page_size: String(PAGE_SIZE),
    })
    if (search.trim()) params.set("search", search.trim())
    if (status !== "all") params.set("status", status)
    setLoading(true)
    setError(null)
    apiFetch<ChannelAPIKeyPage>(`/channels/${channel.id}/api-keys?${params.toString()}`)
      .then((res) => {
        if (cancelled) return
        setData({
          items: Array.isArray(res?.items) ? res.items : [],
          total: res?.total ?? 0,
          page: res?.page ?? page,
          page_size: res?.page_size ?? PAGE_SIZE,
          pages: res?.pages ?? 1,
        })
      })
      .catch((e) => {
        if (cancelled) return
        const err = e as Error
        setError(err.message || "加载密钥失败")
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [open, channel, mode, page, search, status, reloadTick])

  function reload() {
    setReloadTick((tick) => tick + 1)
  }

  function openCreate() {
    setEditing(null)
    setForm(() => {
      const next = { ...emptyForm }
      const first = groups[0]
      if (first) {
        if (channel?.type === "newapi") next.group = first.name
        else if (first.id != null) next.group_id = String(first.id)
      }
      return next
    })
    setError(null)
    setMode("create")
  }

  function openEdit(key: ChannelAPIKey) {
    if (!channel) return
    setEditing(key)
    setForm(formFromKey(key, channel))
    setError(null)
    setMode("edit")
  }

  function buildPayload() {
    if (!channel) return {}
    if (!form.name.trim()) throw new Error("密钥名称不能为空")
    if (channel.type === "newapi") {
      const payload: Record<string, unknown> = {
        name: form.name.trim(),
        group: form.group.trim(),
        remain_quota: optionalInt(form.remain_quota) ?? 0,
        unlimited_quota: form.unlimited_quota,
        expired_time: optionalInt(form.expired_time) ?? -1,
        model_limits_enabled: form.model_limits_enabled,
        model_limits: form.model_limits.trim(),
        allow_ips: form.allow_ips.trim(),
        cross_group_retry: form.cross_group_retry,
      }
      if (mode === "create" && form.custom_key.trim()) payload.custom_key = form.custom_key.trim()
      if (mode === "edit") payload.status = form.status
      return payload
    }
    const payload: Record<string, unknown> = {
      name: form.name.trim(),
      group_id: optionalInt(form.group_id),
      quota: optionalFloat(form.quota) ?? 0,
      ip_whitelist: splitLines(form.ip_whitelist),
      ip_blacklist: splitLines(form.ip_blacklist),
      rate_limit_5h: optionalFloat(form.rate_limit_5h) ?? 0,
      rate_limit_1d: optionalFloat(form.rate_limit_1d) ?? 0,
      rate_limit_7d: optionalFloat(form.rate_limit_7d) ?? 0,
    }
    if (mode === "create") {
      if (form.custom_key.trim()) payload.custom_key = form.custom_key.trim()
      const days = optionalInt(form.expires_in_days)
      if (days != null) payload.expires_in_days = days
    } else {
      payload.status = form.status
      payload.expires_at = localDateTimeToISO(form.expires_at) ?? ""
      payload.reset_quota = form.reset_quota
      payload.reset_rate_limit_usage = form.reset_rate_limit_usage
    }
    return payload
  }

  async function handleSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault()
    if (!channel) return
    setSubmitting(true)
    setError(null)
    try {
      const payload = buildPayload()
      if (mode === "create") {
        const created = await apiFetch<ChannelAPIKey>(`/channels/${channel.id}/api-keys`, {
          method: "POST",
          body: JSON.stringify(payload),
        })
        if (created?.key) {
          if (created.id) {
            setRevealedKeys((prev) => ({ ...prev, [created.id]: created.key }))
          }
          void copyText(created.key, "密钥已创建并复制")
        }
      } else if (editing) {
        await apiFetch<ChannelAPIKey>(`/channels/${channel.id}/api-keys/${editing.id}`, {
          method: "PUT",
          body: JSON.stringify(payload),
        })
        toast.success("密钥已更新")
      }
      setMode("list")
      setEditing(null)
      setForm(emptyForm)
      setPage(1)
      reload()
    } catch (e) {
      const err = e as Error
      setError(err.message || "保存密钥失败")
    } finally {
      setSubmitting(false)
    }
  }

  async function revealKey(key: ChannelAPIKey) {
    if (!channel) throw new Error("渠道不存在")
    const cached = revealedKeys[key.id]
    if (cached) return cached
    setRevealingID(key.id)
    try {
      const res = await apiFetch<ChannelAPIKeyReveal>(`/channels/${channel.id}/api-keys/${key.id}/reveal`, {
        method: "POST",
      })
      if (!res?.key) throw new Error("上游未返回完整密钥")
      setRevealedKeys((prev) => ({ ...prev, [key.id]: res.key }))
      return res.key
    } finally {
      setRevealingID((current) => (current === key.id ? null : current))
    }
  }

  async function revealAndShow(key: ChannelAPIKey) {
    try {
      await revealKey(key)
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "获取完整密钥失败")
    }
  }

  async function revealAndCopy(key: ChannelAPIKey) {
    try {
      const fullKey = await revealKey(key)
      await copyText(fullKey)
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "获取完整密钥失败")
    }
  }

  const description = channel
    ? `${channel.name} · ${channelTypeLabel(channel.type)}`
    : "管理上游 API 密钥。"

  return (
    <>
      <Dialog open={open} onOpenChange={onOpenChange}>
        <DialogContent className="w-[min(96vw,90rem)] max-w-[min(96vw,90rem)] sm:max-w-[min(96vw,90rem)]">
          <DialogHeader>
            <DialogTitle>API 密钥管理</DialogTitle>
            <DialogDescription>{description}</DialogDescription>
          </DialogHeader>

          {mode === "list" ? (
            <div className="space-y-4">
              <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
                  <div className="relative w-full sm:max-w-sm sm:flex-1">
                    <Search className="pointer-events-none absolute left-2.5 top-2.5 size-4 text-muted-foreground" />
                    <Input
                      value={search}
                      onChange={(e) => {
                        setSearch(e.target.value)
                        setPage(1)
                      }}
                      placeholder="搜索名称或密钥"
                      className="pl-8"
                    />
                  </div>
                  <Select
                    value={status}
                    onValueChange={(value) => {
                      setStatus(value as ChannelAPIKeyStatus | "all")
                      setPage(1)
                    }}
                  >
                    <SelectTrigger className="w-full sm:w-28">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">全部状态</SelectItem>
                      <SelectItem value="active">启用</SelectItem>
                      <SelectItem value="disabled">停用</SelectItem>
                      <SelectItem value="expired">已过期</SelectItem>
                      <SelectItem value="quota_exhausted">额度耗尽</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <Button type="button" size="sm" className="w-full gap-1.5 sm:w-auto" onClick={openCreate}>
                  <Plus className="size-4" />
                  新建密钥
                </Button>
              </div>

              <p className="text-xs leading-5 text-muted-foreground">
                为避免绕过受管密钥的归属与分组代际校验，此处不提供直接删除；受管密钥仅能通过“上游分组变更”流程安全移除。
              </p>

              <div className="overflow-hidden rounded-lg border border-border">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="min-w-28">名称</TableHead>
                      <TableHead className="min-w-40">密钥</TableHead>
                      <TableHead>状态</TableHead>
                          <TableHead className="min-w-52">分组</TableHead>
                      <TableHead className="min-w-24">额度</TableHead>
                      <TableHead className="min-w-32">过期</TableHead>
                      <TableHead className="min-w-20 text-right">操作</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {loading ? (
                      <TableRow>
                        <TableCell colSpan={7} className="h-28 text-center text-muted-foreground">
                          <Loader2 className="mx-auto mb-2 size-4 animate-spin" />
                          加载中…
                        </TableCell>
                      </TableRow>
                    ) : items.length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={7} className="h-28 text-center text-muted-foreground">
                          暂无密钥
                        </TableCell>
                      </TableRow>
                    ) : (
                      items.map((item) => {
                        const displayKey = revealedKeys[item.id] || maskKey(item.key)
                        const isRevealing = revealingID === item.id
                        return (
                        <TableRow key={item.id}>
                          <TableCell className="font-medium">{item.name || "未命名"}</TableCell>
                          <TableCell className="min-w-40">
                            <Input
                              readOnly
                              value={isRevealing ? "加载中…" : displayKey}
                              className="h-8 w-40 cursor-pointer truncate font-mono text-xs sm:w-48"
                              title="点击显示完整密钥"
                              disabled={isRevealing}
                              onClick={() => void revealAndShow(item)}
                            />
                          </TableCell>
                          <TableCell>
                            <Badge variant="outline" className={cn(statusClass(item.status))}>
                              {statusLabel(item.status)}
                            </Badge>
                          </TableCell>
                          <TableCell>
                            <div className="max-w-64 whitespace-normal">
                              <p className="break-words text-xs font-medium">{groupDisplayName(item)}</p>
                              <p className="break-words text-[11px] leading-4 text-muted-foreground">
                                {item.group_description || "无描述"}
                              </p>
                              {item.group_ratio > 0 ? (
                                <p className="text-[11px] text-muted-foreground">倍率 {formatRatio(item.group_ratio)}</p>
                              ) : null}
                            </div>
                          </TableCell>
                          <TableCell>
                            {isNewAPI
                              ? item.unlimited_quota
                                ? "无限"
                                : decimal(item.quota, 0)
                              : `${decimal(item.quota_used)}/${item.quota > 0 ? decimal(item.quota) : "无限"}`}
                          </TableCell>
                          <TableCell>
                            {isNewAPI
                              ? item.expired_time === -1 || !item.expired_time
                                ? "永不过期"
                                : dateTime(new Date(item.expired_time * 1000).toISOString())
                              : item.expires_at
                                ? dateTime(item.expires_at)
                                : "永不过期"}
                          </TableCell>
                          <TableCell>
                            <div className="flex justify-end gap-1">
                              <Button
                                type="button"
                                variant="ghost"
                                size="icon-sm"
                                title="复制完整密钥"
                                disabled={isRevealing}
                                onClick={() => void revealAndCopy(item)}
                              >
                                {isRevealing ? (
                                  <Loader2 className="size-4 animate-spin" />
                                ) : (
                                  <Copy className="size-4" />
                                )}
                              </Button>
                              <Button
                                type="button"
                                variant="ghost"
                                size="icon-sm"
                                title="编辑"
                                onClick={() => openEdit(item)}
                              >
                                <Pencil className="size-4" />
                              </Button>
                            </div>
                          </TableCell>
                        </TableRow>
                        )
                      })
                    )}
                  </TableBody>
                </Table>
              </div>

              {error ? <p className="text-sm text-destructive">{error}</p> : null}

              <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                <p className="text-xs text-muted-foreground">
                  共 {data?.total ?? 0} 条，第 {data?.page ?? page}/{totalPages} 页
                </p>
                <div className="grid grid-cols-2 gap-2 sm:flex">
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    disabled={page <= 1 || loading}
                    onClick={() => setPage((p) => Math.max(1, p - 1))}
                  >
                    上一页
                  </Button>
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    disabled={page >= totalPages || loading}
                    onClick={() => setPage((p) => p + 1)}
                  >
                    下一页
                  </Button>
                </div>
              </div>
            </div>
          ) : (
            <form onSubmit={handleSubmit} className="space-y-4">
              <div className="grid gap-3 sm:grid-cols-2">
                <Field label="名称">
                  <Input
                    value={form.name}
                    onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
                    disabled={submitting}
                    required
                  />
                </Field>
                {mode === "create" ? (
                  <Field label="自定义 Key">
                    <Input
                      value={form.custom_key}
                      onChange={(e) => setForm((f) => ({ ...f, custom_key: e.target.value }))}
                      placeholder="留空则由上游生成"
                      disabled={submitting}
                    />
                  </Field>
                ) : (
                  <Field label="状态">
                    <Select
                      value={form.status}
                      onValueChange={(value) => setForm((f) => ({ ...f, status: value as "active" | "disabled" }))}
                    >
                      <SelectTrigger className="w-full">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="active">启用</SelectItem>
                        <SelectItem value="disabled">停用</SelectItem>
                      </SelectContent>
                    </Select>
                  </Field>
                )}
              </div>

              {isNewAPI ? (
                <NewAPIFields form={form} setForm={setForm} disabled={submitting} groups={groups} groupsLoading={groupsLoading} selectedGroup={groupByName.get(form.group)} />
              ) : (
                <Sub2APIFields form={form} setForm={setForm} disabled={submitting} mode={mode} groups={groups} groupsLoading={groupsLoading} selectedGroup={groupByID.get(form.group_id)} />
              )}

              {error ? (
                <p className="text-sm text-destructive" role="alert">
                  {error}
                </p>
              ) : null}

              <DialogFooter>
                <Button
                  type="button"
                  variant="outline"
                  onClick={() => {
                    setMode("list")
                    setEditing(null)
                    setError(null)
                  }}
                  disabled={submitting}
                >
                  返回
                </Button>
                <Button type="submit" disabled={submitting}>
                  {submitting ? "保存中…" : mode === "create" ? "创建" : "保存"}
                </Button>
              </DialogFooter>
            </form>
          )}
        </DialogContent>
      </Dialog>
    </>
  )
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="space-y-1.5">
      <Label>{label}</Label>
      {children}
    </div>
  )
}

function InlineSwitch({
  label,
  checked,
  disabled,
  onCheckedChange,
}: {
  label: string
  checked: boolean
  disabled?: boolean
  onCheckedChange: (checked: boolean) => void
}) {
  return (
    <label className="flex items-center justify-between rounded-lg border border-border px-3 py-2 text-sm">
      <span>{label}</span>
      <Switch checked={checked} onCheckedChange={onCheckedChange} disabled={disabled} />
    </label>
  )
}

function NewAPIFields({
  form,
  setForm,
  disabled,
  groups,
  groupsLoading,
  selectedGroup,
}: {
  form: KeyForm
  setForm: React.Dispatch<React.SetStateAction<KeyForm>>
  disabled: boolean
  groups: ChannelAPIKeyGroup[]
  groupsLoading: boolean
  selectedGroup?: ChannelAPIKeyGroup
}) {
  return (
    <div className="space-y-4">
      <div className="grid gap-3 sm:grid-cols-3">
        <Field label="分组">
          <Select
            value={form.group}
            onValueChange={(value) => setForm((f) => ({ ...f, group: value }))}
            disabled={disabled || groupsLoading}
          >
            <SelectTrigger className="w-full">
              {selectedGroup ? (
                <SelectValue>{selectedGroup.name} · {formatRatio(selectedGroup.ratio)}</SelectValue>
              ) : (
                <SelectValue placeholder={groupsLoading ? "加载中…" : "选择分组"} />
              )}
            </SelectTrigger>
            <SelectContent className="max-h-64">
              {groups.map((group) => (
                <SelectItem key={group.name} value={group.name}>
                  <span className="flex flex-col items-start">
                    <span>{group.name} · {formatRatio(group.ratio)}</span>
                    <span className="max-w-96 whitespace-normal break-words text-[11px] text-muted-foreground">
                      {group.description || "无描述"}
                    </span>
                  </span>
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <GroupHint group={selectedGroup} />
        </Field>
        <Field label="剩余额度">
          <Input value={form.remain_quota} onChange={(e) => setForm((f) => ({ ...f, remain_quota: e.target.value }))} disabled={disabled} inputMode="numeric" />
        </Field>
        <Field label="过期时间戳">
          <Input value={form.expired_time} onChange={(e) => setForm((f) => ({ ...f, expired_time: e.target.value }))} disabled={disabled} inputMode="numeric" />
        </Field>
      </div>
      <div className="grid gap-3 sm:grid-cols-3">
        <InlineSwitch label="无限额度" checked={form.unlimited_quota} disabled={disabled} onCheckedChange={(v) => setForm((f) => ({ ...f, unlimited_quota: v }))} />
        <InlineSwitch label="启用模型限制" checked={form.model_limits_enabled} disabled={disabled} onCheckedChange={(v) => setForm((f) => ({ ...f, model_limits_enabled: v }))} />
        <InlineSwitch label="跨分组重试" checked={form.cross_group_retry} disabled={disabled} onCheckedChange={(v) => setForm((f) => ({ ...f, cross_group_retry: v }))} />
      </div>
      <div className="grid gap-3 sm:grid-cols-2">
        <Field label="模型限制">
          <Textarea value={form.model_limits} onChange={(e) => setForm((f) => ({ ...f, model_limits: e.target.value }))} placeholder="逗号分隔" disabled={disabled} />
        </Field>
        <Field label="允许 IP">
          <Textarea value={form.allow_ips} onChange={(e) => setForm((f) => ({ ...f, allow_ips: e.target.value }))} placeholder="每行一个 IP" disabled={disabled} />
        </Field>
      </div>
    </div>
  )
}

function Sub2APIFields({
  form,
  setForm,
  disabled,
  mode,
  groups,
  groupsLoading,
  selectedGroup,
}: {
  form: KeyForm
  setForm: React.Dispatch<React.SetStateAction<KeyForm>>
  disabled: boolean
  mode: Mode
  groups: ChannelAPIKeyGroup[]
  groupsLoading: boolean
  selectedGroup?: ChannelAPIKeyGroup
}) {
  return (
    <div className="space-y-4">
      <div className="grid gap-3 sm:grid-cols-4">
        <Field label="分组 ID">
          <Select
            value={form.group_id}
            onValueChange={(value) => setForm((f) => ({ ...f, group_id: value }))}
            disabled={disabled || groupsLoading}
          >
            <SelectTrigger className="w-full">
              {selectedGroup ? (
                <SelectValue>{selectedGroup.name} · {formatRatio(selectedGroup.ratio)}</SelectValue>
              ) : (
                <SelectValue placeholder={groupsLoading ? "加载中…" : "选择分组"} />
              )}
            </SelectTrigger>
            <SelectContent className="max-h-64">
              {groups
                .filter((group) => group.id != null)
                .map((group) => (
                  <SelectItem key={group.id} value={String(group.id)}>
                    <span className="flex flex-col items-start">
                      <span>{group.name} · {formatRatio(group.ratio)}</span>
                      <span className="max-w-96 whitespace-normal break-words text-[11px] text-muted-foreground">
                        {group.description || "无描述"}
                      </span>
                    </span>
                  </SelectItem>
                ))}
            </SelectContent>
          </Select>
          <GroupHint group={selectedGroup} />
        </Field>
        <Field label="额度 USD">
          <Input value={form.quota} onChange={(e) => setForm((f) => ({ ...f, quota: e.target.value }))} disabled={disabled} inputMode="decimal" />
        </Field>
        {mode === "create" ? (
          <Field label="过期天数">
            <Input value={form.expires_in_days} onChange={(e) => setForm((f) => ({ ...f, expires_in_days: e.target.value }))} placeholder="留空永不过期" disabled={disabled} inputMode="numeric" />
          </Field>
        ) : (
          <Field label="过期时间">
            <Input type="datetime-local" value={form.expires_at} onChange={(e) => setForm((f) => ({ ...f, expires_at: e.target.value }))} disabled={disabled} />
          </Field>
        )}
        <Field label="5h 限额">
          <Input value={form.rate_limit_5h} onChange={(e) => setForm((f) => ({ ...f, rate_limit_5h: e.target.value }))} disabled={disabled} inputMode="decimal" />
        </Field>
      </div>
      <div className="grid gap-3 sm:grid-cols-2">
        <Field label="IP 白名单">
          <Textarea value={form.ip_whitelist} onChange={(e) => setForm((f) => ({ ...f, ip_whitelist: e.target.value }))} placeholder="每行一个 IP 或 CIDR" disabled={disabled} />
        </Field>
        <Field label="IP 黑名单">
          <Textarea value={form.ip_blacklist} onChange={(e) => setForm((f) => ({ ...f, ip_blacklist: e.target.value }))} placeholder="每行一个 IP 或 CIDR" disabled={disabled} />
        </Field>
      </div>
      <div className="grid gap-3 sm:grid-cols-4">
        <Field label="1d 限额">
          <Input value={form.rate_limit_1d} onChange={(e) => setForm((f) => ({ ...f, rate_limit_1d: e.target.value }))} disabled={disabled} inputMode="decimal" />
        </Field>
        <Field label="7d 限额">
          <Input value={form.rate_limit_7d} onChange={(e) => setForm((f) => ({ ...f, rate_limit_7d: e.target.value }))} disabled={disabled} inputMode="decimal" />
        </Field>
        {mode === "edit" ? (
          <>
            <InlineSwitch label="重置配额" checked={form.reset_quota} disabled={disabled} onCheckedChange={(v) => setForm((f) => ({ ...f, reset_quota: v }))} />
            <InlineSwitch label="重置限额用量" checked={form.reset_rate_limit_usage} disabled={disabled} onCheckedChange={(v) => setForm((f) => ({ ...f, reset_rate_limit_usage: v }))} />
          </>
        ) : null}
      </div>
    </div>
  )
}

function GroupHint({ group }: { group?: ChannelAPIKeyGroup }) {
  if (!group) return null
  return (
    <p className="whitespace-normal break-words text-[11px] leading-4 text-muted-foreground">
      {group.description || "无描述"} · 倍率 {formatRatio(group.ratio)}
    </p>
  )
}
