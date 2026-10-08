"use client"

import { useState } from "react"
import {
  LockKeyhole,
  Network,
  Pencil,
  Play,
  Plus,
  RefreshCw,
  Trash2,
  Upload,
} from "lucide-react"
import { toast } from "sonner"
import { NetworkProxyErrorBadge, NetworkProxyErrorDialog } from "@/components/network-proxy/network-proxy-error-dialog"
import { NetworkProxyFormDialog } from "@/components/network-proxy/network-proxy-form-dialog"
import { NetworkProxyImportDialog } from "@/components/network-proxy/network-proxy-import-dialog"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { useConfirm } from "@/components/ui/confirm-dialog"
import { PageHeader } from "@/components/ui/page-header"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { apiFetch } from "@/lib/api"
import type { NetworkProxy, NetworkProxyTestResult } from "@/lib/api-types"
import { relativeTime } from "@/lib/format"
import { useNetworkProxies } from "@/lib/queries"
import { cn } from "@/lib/utils"

export default function NetworkProxyPage() {
  const proxies = useNetworkProxies()
  const { confirm, dialog } = useConfirm()
  const [formOpen, setFormOpen] = useState(false)
  const [importOpen, setImportOpen] = useState(false)
  const [editingProxy, setEditingProxy] = useState<NetworkProxy | null>(null)
  const [errorProxy, setErrorProxy] = useState<NetworkProxy | null>(null)
  const [testingIDs, setTestingIDs] = useState<Set<number>>(() => new Set())

  function openCreate() {
    setEditingProxy(null)
    setFormOpen(true)
  }

  function openEdit(proxy: NetworkProxy) {
    setEditingProxy(proxy)
    setFormOpen(true)
  }

  function closeForm(open: boolean) {
    setFormOpen(open)
    if (!open) setEditingProxy(null)
  }

  async function testProxy(proxy: NetworkProxy) {
    setTestingIDs((current) => new Set(current).add(proxy.id))
    try {
      const result = await apiFetch<NetworkProxyTestResult>("/network-proxies/" + String(proxy.id) + "/test", { method: "POST" })
      if (result.ok) {
        const details = ["测试成功"]
        if (Number.isFinite(result.latency_ms)) details.push(formatLatency(result.latency_ms))
        if (result.ip) details.push(result.ip)
        toast.success(details.join(" · "))
      } else {
        toast.error("测试失败，请查看异常详情")
      }
    } catch {
      toast.error("测试失败，请查看异常详情")
    } finally {
      setTestingIDs((current) => {
        const next = new Set(current)
        next.delete(proxy.id)
        return next
      })
      proxies.refetch()
    }
  }

  async function deleteProxy(proxy: NetworkProxy) {
    const accepted = await confirm({
      title: "删除网络代理",
      description: "确定删除“" + proxy.name + "”吗？",
      confirmLabel: "删除",
      destructive: true,
    })
    if (!accepted) return
    try {
      await apiFetch("/network-proxies/" + String(proxy.id), { method: "DELETE" })
      toast.success("代理已删除")
      proxies.refetch()
    } catch (cause) {
      toast.error(cause instanceof Error ? cause.message : "删除失败")
    }
  }

  const items = proxies.data ?? []

  return (
    <div className="space-y-4">
      <PageHeader
        icon={Network}
        title="网络代理"
        description="管理出口、测试连通性与渠道绑定"
        actions={
          <>
            <Button type="button" variant="outline" size="sm" onClick={() => setImportOpen(true)}>
              <Upload className="size-3.5" />
              导入
            </Button>
            <Button type="button" size="sm" onClick={openCreate}>
              <Plus className="size-3.5" />
              新建代理
            </Button>
            <Button
              type="button"
              variant="outline"
              size="icon-sm"
              onClick={proxies.refetch}
              disabled={proxies.refreshing}
              aria-label="刷新网络代理"
              title="刷新网络代理"
            >
              <RefreshCw className={cn("size-3.5", proxies.refreshing && "animate-spin")} />
            </Button>
          </>
        }
      />

      <section className="overflow-hidden rounded-lg border border-border bg-background">
        <div className="flex min-h-12 items-center justify-between gap-3 border-b border-border bg-muted/20 px-4 py-3">
          <div className="flex min-w-0 items-center gap-2">
            <h2 className="truncate text-lg font-semibold tracking-tight">代理列表</h2>
            <Badge variant="outline">{items.length}</Badge>
          </div>
        </div>

        {proxies.loading && !proxies.data ? (
          <EmptyState text="正在读取网络代理" />
        ) : proxies.error ? (
          <EmptyState text="读取网络代理失败" action={<Button size="sm" variant="outline" onClick={proxies.refetch} disabled={proxies.refreshing}>重试</Button>} />
        ) : items.length === 0 ? (
          <EmptyState text="暂无网络代理" />
        ) : (
          <div className="overflow-x-auto">
          <Table className="min-w-[960px] table-fixed">
            <TableHeader>
              <TableRow>
                <TableHead className="w-[17%] text-left">名称</TableHead>
                <TableHead className="w-[18%] text-left">地址</TableHead>
                <TableHead className="w-[9%] text-center">来源</TableHead>
                <TableHead className="w-[12%] text-center">出口标记</TableHead>
                <TableHead className="w-[9%] text-center">状态</TableHead>
                <TableHead className="w-[8%] text-center">绑定渠道</TableHead>
                <TableHead className="w-[15%] text-center">最近测试</TableHead>
                <TableHead className="w-[12%] text-center">操作</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {items.map((proxy) => {
                const testing = testingIDs.has(proxy.id)
                return (
                  <TableRow key={proxy.id}>
                    <TableCell className="align-middle">
                      <div className="truncate font-semibold" title={proxy.name}>{proxy.name}</div>
                      {proxy.auth_configured ? (
                        <div className="mt-1 flex items-center gap-1 text-xs text-muted-foreground">
                          <LockKeyhole className="size-3 shrink-0" />
                          <span>已配置认证</span>
                        </div>
                      ) : null}
                    </TableCell>
                    <TableCell className="align-middle">
                      <div className="truncate font-medium tabular-nums" title={proxy.host + ":" + String(proxy.port)}>{proxy.host + ":" + String(proxy.port)}</div>
                      <div className="mt-1">
                        <Badge variant="outline" className="border-brand/20 bg-brand/5 text-brand">{proxy.protocol.toUpperCase()}</Badge>
                      </div>
                    </TableCell>
                    <TableCell className="text-center align-middle">
                      <Badge variant="outline" className="border-border bg-muted/30 text-muted-foreground">{sourceLabel(proxy.source)}</Badge>
                    </TableCell>
                    <TableCell className="text-center align-middle">
                      <Badge
                        variant="outline"
                        className={proxy.clean_ip ? "border-brand/20 bg-brand/5 text-brand" : "border-border bg-muted/30 text-muted-foreground"}
                      >
                        {proxy.clean_ip ? "纯净 IP" : "普通出口"}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-center align-middle">
                      <Badge
                        variant="outline"
                        className={proxy.enabled ? "border-success/25 bg-success/5 text-success" : "border-border bg-muted/30 text-muted-foreground"}
                      >
                        {proxy.enabled ? "已启用" : "已停用"}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-center align-middle tabular-nums">{proxy.bound_channel_count}</TableCell>
                    <TableCell className="text-center align-middle">
                      {proxy.last_error ? (
                        <NetworkProxyErrorBadge proxy={proxy} onClick={() => setErrorProxy(proxy)} />
                      ) : proxy.last_tested_at ? (
                        <div className="min-w-0 text-xs">
                          <div className="font-medium tabular-nums">{proxy.last_latency_ms == null ? "完成" : formatLatency(proxy.last_latency_ms)}</div>
                          <div className="mt-1 truncate text-muted-foreground" title={proxy.last_egress_ip ?? undefined}>{proxy.last_egress_ip || relativeTime(proxy.last_tested_at)}</div>
                        </div>
                      ) : <span className="text-xs text-muted-foreground">未测试</span>}
                    </TableCell>
                    <TableCell className="text-center align-middle">
                      <div className="flex items-center justify-center gap-1.5">
                        <Button
                          type="button"
                          size="icon-sm"
                          variant="outline"
                          onClick={() => void testProxy(proxy)}
                          disabled={testing}
                          aria-label={"测试 " + proxy.name}
                          title="测试"
                        >
                          {testing ? <RefreshCw className="size-3.5 animate-spin" /> : <Play className="size-3.5" />}
                        </Button>
                        <Button type="button" size="icon-sm" variant="outline" onClick={() => openEdit(proxy)} aria-label={"编辑 " + proxy.name} title="编辑">
                          <Pencil className="size-3.5" />
                        </Button>
                        <Button type="button" size="icon-sm" variant="outline" onClick={() => void deleteProxy(proxy)} aria-label={"删除 " + proxy.name} title="删除">
                          <Trash2 className="size-3.5" />
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                )
              })}
            </TableBody>
          </Table>
          </div>
        )}
      </section>

      <NetworkProxyFormDialog open={formOpen} proxy={editingProxy} onOpenChange={closeForm} onSaved={proxies.refetch} />
      <NetworkProxyImportDialog open={importOpen} onOpenChange={setImportOpen} onSaved={proxies.refetch} />
      <NetworkProxyErrorDialog proxy={errorProxy} onOpenChange={(open) => { if (!open) setErrorProxy(null) }} />
      {dialog}
    </div>
  )
}

function EmptyState({ text, action }: { text: string; action?: React.ReactNode }) {
  return (
    <div className="flex min-h-44 flex-col items-center justify-center gap-3 p-6 text-center text-sm text-muted-foreground">
      <Network className="size-5" />
      <span>{text}</span>
      {action}
    </div>
  )
}

function sourceLabel(source: string) {
  switch (source.trim().toLowerCase()) {
    case "uri":
      return "地址列表"
    case "clash":
    case "mihomo":
      return "Clash"
    case "3x-ui":
      return "3x-ui"
    case "legacy-system":
      return "已迁移"
    case "manual":
      return "手动"
    default:
      return "导入"
  }
}

function formatLatency(value: number) {
  if (!Number.isFinite(value) || value < 0) return "—"
  if (value < 1000) return String(Math.round(value)) + " ms"
  return (Math.round(value / 10) / 100).toFixed(2) + " s"
}
