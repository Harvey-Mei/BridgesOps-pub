"use client"

import { useEffect, useState, type FormEvent } from "react"
import { toast } from "sonner"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
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
import { Switch } from "@/components/ui/switch"
import { Textarea } from "@/components/ui/textarea"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import { apiFetch } from "@/lib/api"
import type {
  NetworkProxyImportFormat,
  NetworkProxyImportInput,
  NetworkProxyImportResult,
} from "@/lib/api-types"
import { cn } from "@/lib/utils"

type ImportDraft = {
  format: NetworkProxyImportFormat
  content: string
  subscriptionURL: string
  cleanIP: boolean
  enabled: boolean
}

const formatOptions: Array<{ value: NetworkProxyImportFormat; label: string; hint?: string }> = [
  { value: "auto", label: "自动" },
  { value: "uri", label: "代理地址列表" },
  {
    value: "clash",
    label: "Clash/Mihomo",
    hint: "仅导入标准 HTTP、HTTPS、SOCKS5 出口；其他节点请先由外部代理引擎转换为标准出口。",
  },
  {
    value: "3x-ui",
    label: "3x-ui",
    hint: "仅导入 HTTP、HTTPS、SOCKS5 出口；VLESS、VMess、Trojan 等节点请先由 Mihomo、Clash、Xray 或 sing-box 转换。",
  },
]

function initialDraft(): ImportDraft {
  return {
    format: "auto",
    content: "",
    subscriptionURL: "",
    cleanIP: false,
    enabled: true,
  }
}

export function NetworkProxyImportDialog({
  open,
  onOpenChange,
  onSaved,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  onSaved: () => void
}) {
  const [draft, setDraft] = useState<ImportDraft>(initialDraft)
  const [result, setResult] = useState<NetworkProxyImportResult | null>(null)
  const [importing, setImporting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (open) {
      setDraft(initialDraft())
      setResult(null)
      setError(null)
    }
  }, [open])

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    const content = draft.content.trim()
    const subscriptionURL = draft.subscriptionURL.trim()
    if (!content && !subscriptionURL) {
      setError("请填写导入内容或订阅 URL")
      return
    }
    if (content && subscriptionURL) {
      setError("导入内容与订阅 URL 只能选择一项")
      return
    }

    setError(null)
    setImporting(true)
    const body: NetworkProxyImportInput = {
      format: draft.format,
      clean_ip: draft.cleanIP,
      enabled: draft.enabled,
      ...(subscriptionURL ? { subscription_url: subscriptionURL } : { content }),
    }
    try {
      const nextResult = await apiFetch<NetworkProxyImportResult>("/network-proxies/import", {
        method: "POST",
        body: JSON.stringify(body),
      })
      setResult(nextResult)
      toast.success("已导入 " + String(nextResult.created.length) + " 个代理")
      onSaved()
    } catch (cause) {
      const message = cause instanceof Error ? cause.message.trim() : ""
      setError(message && message.length <= 120 && !/:\/\//.test(message) ? message : "导入失败，请检查填写内容")
    } finally {
      setImporting(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange} busy={importing}>
      <DialogContent formLayout className="sm:max-w-3xl 2xl:max-w-4xl">
        <DialogHeader>
          <DialogTitle>导入网络代理</DialogTitle>
          <DialogDescription className="sr-only">选择配置格式并导入代理地址或订阅 URL</DialogDescription>
        </DialogHeader>
        <form onSubmit={submit} >
          <DialogBody className="space-y-4">
          <div className="space-y-1.5">
            <Label>格式</Label>
            <div className="grid grid-cols-2 gap-1 rounded-lg border border-border p-1 sm:grid-cols-4">
              {formatOptions.map((option) => (
                <Tooltip key={option.value}>
                  <TooltipTrigger asChild>
                    <button
                      type="button"
                      disabled={importing}
                      onClick={() => setDraft({ ...draft, format: option.value })}
                      className={cn(
                        "rounded-md px-2 py-1.5 text-xs font-medium transition-colors",
                        draft.format === option.value ? "bg-foreground text-background" : "text-muted-foreground hover:bg-muted",
                      )}
                    >
                      {option.label}
                    </button>
                  </TooltipTrigger>
                  {option.hint ? <TooltipContent className="max-w-xs leading-5">{option.hint}</TooltipContent> : null}
                </Tooltip>
              ))}
            </div>
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="network-proxy-subscription-url">订阅 URL</Label>
            <Input
              id="network-proxy-subscription-url"
              type="url"
              inputMode="url"
              autoComplete="off"
              value={draft.subscriptionURL}
              onChange={(event) => setDraft({ ...draft, subscriptionURL: event.target.value, content: event.target.value.trim() ? "" : draft.content })}
              disabled={importing || Boolean(draft.content.trim())}
            />
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="network-proxy-import-content">导入内容</Label>
            <Textarea
              id="network-proxy-import-content"
              rows={9}
              value={draft.content}
              onChange={(event) => setDraft({ ...draft, content: event.target.value, subscriptionURL: event.target.value.trim() ? "" : draft.subscriptionURL })}
              disabled={importing || Boolean(draft.subscriptionURL.trim())}
              className="field-sizing-fixed font-mono text-xs"
            />
          </div>

          <div className="grid gap-3 sm:grid-cols-2">
            <div className="flex items-center justify-between rounded-lg border border-border px-3 py-2">
              <Label htmlFor="network-proxy-import-clean-ip" className="text-sm font-medium">标记为纯净 IP</Label>
              <Switch
                id="network-proxy-import-clean-ip"
                checked={draft.cleanIP}
                onCheckedChange={(cleanIP) => setDraft({ ...draft, cleanIP })}
                disabled={importing}
              />
            </div>
            <div className="flex items-center justify-between rounded-lg border border-border px-3 py-2">
              <Label htmlFor="network-proxy-import-enabled" className="text-sm font-medium">导入后启用</Label>
              <Switch
                id="network-proxy-import-enabled"
                checked={draft.enabled}
                onCheckedChange={(enabled) => setDraft({ ...draft, enabled })}
                disabled={importing}
              />
            </div>
          </div>



          {result ? (
            <section className="rounded-lg border border-border bg-muted/20">
              <div className="flex min-h-11 items-center justify-between gap-3 border-b border-border px-3 py-2">
                <span className="text-sm font-medium">导入结果</span>
                <div className="flex items-center gap-2">
                  <Badge variant="outline" className="border-success/25 bg-success/5 text-success">新增 {result.created.length}</Badge>
                  {result.skipped.length > 0 ? <Badge variant="outline" className="border-warning/25 bg-warning/5 text-warning">跳过 {result.skipped.length}</Badge> : null}
                </div>
              </div>
              {result.skipped.length > 0 ? (
                <div className="max-h-44 divide-y divide-border overflow-y-auto">
                  {result.skipped.map((issue, index) => (
                    <div key={String(index)} className="flex items-start justify-between gap-3 px-3 py-2 text-xs">
                      <span className="shrink-0 font-medium">{skippedEntryLabel(issue.entry, index)}</span>
                      <span className="min-w-0 max-w-[28rem] truncate text-right text-muted-foreground">{safeIssueReason(issue.reason)}</span>
                    </div>
                  ))}
                </div>
              ) : <div className="px-3 py-3 text-sm text-muted-foreground">没有跳过条目</div>}
            </section>
          ) : null}

          </DialogBody>
          {error ? <DialogError>{error}</DialogError> : null}
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)} disabled={importing}>{result ? "关闭" : "取消"}</Button>
            <Button type="submit" disabled={importing}>{importing ? "导入中" : "开始导入"}</Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}

function skippedEntryLabel(entry: string, index: number) {
  const scheme = entry.trim().match(/^([a-z][a-z0-9+.-]*):\/\//i)
  return scheme ? scheme[1].toUpperCase() + " · 跳过项 " + String(index + 1) : "跳过项 " + String(index + 1)
}

function safeIssueReason(value: string) {
  const compact = value
    .replace(/[a-z][a-z0-9+.-]*:\/\/\S+/gi, "链接已隐藏")
    .replace(/\s+/g, " ")
    .trim()
  return compact.length > 180 ? compact.slice(0, 180) + "…" : compact || "服务端未提供原因"
}
