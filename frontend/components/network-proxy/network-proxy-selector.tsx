"use client"

import { useMemo, useState } from "react"
import { ChevronDown, Network } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Label } from "@/components/ui/label"
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover"
import type { NetworkProxy } from "@/lib/api-types"
import { cn } from "@/lib/utils"

export function NetworkProxySelector({
  value,
  proxies,
  loading = false,
  disabled = false,
  onValueChange,
}: {
  value: number
  proxies: NetworkProxy[]
  loading?: boolean
  disabled?: boolean
  onValueChange: (value: number) => void
}) {
  const [open, setOpen] = useState(false)
  const selected = useMemo(() => proxies.find((proxy) => proxy.id === value) ?? null, [proxies, value])
  const enabled = useMemo(() => proxies.filter((proxy) => proxy.enabled), [proxies])
  const unavailable = value > 0 && !loading && (selected == null || !selected.enabled)
  const label = value === 0
    ? "直连"
    : loading && selected == null
      ? "正在读取代理"
      : selected
        ? selected.name
        : "已绑定代理 #" + String(value)

  function choose(nextValue: number) {
    onValueChange(nextValue)
    setOpen(false)
  }

  return (
    <div className="min-w-0 space-y-1.5">
      <Label htmlFor="network-proxy-selector">网络代理</Label>
      <Popover open={open} onOpenChange={setOpen}>
        <PopoverTrigger asChild>
          <Button
            id="network-proxy-selector"
            type="button"
            variant="outline"
            role="combobox"
            aria-expanded={open}
            aria-label="选择网络代理"
            disabled={disabled}
            className={cn(
              "h-9 w-full min-w-0 justify-between px-3 text-left font-normal",
              unavailable && "border-danger/30 text-danger",
            )}
          >
            <span className="flex min-w-0 items-center gap-2">
              <Network className="size-3.5 shrink-0" />
              <span className="truncate">{label}</span>
              {unavailable ? <Badge variant="outline" className="border-danger/25 bg-danger/5 text-danger">不可用</Badge> : null}
            </span>
            <ChevronDown className="size-3.5 shrink-0 text-muted-foreground" />
          </Button>
        </PopoverTrigger>
        <PopoverContent align="start" className="w-[min(22rem,calc(100vw-2rem))] p-2">
          <div className="max-h-72 space-y-1 overflow-y-auto">
            <ProxyChoice active={value === 0} onClick={() => choose(0)}>
              <span className="font-medium">直连</span>
              <span className="text-xs text-muted-foreground">不使用网络代理</span>
            </ProxyChoice>
            {enabled.map((proxy) => (
              <ProxyChoice key={proxy.id} active={value === proxy.id} onClick={() => choose(proxy.id)}>
                <span className="min-w-0 truncate font-medium">{proxy.name}</span>
                <span className="flex shrink-0 items-center gap-1.5">
                  <Badge variant="outline" className="border-brand/20 bg-brand/5 text-brand">{proxy.protocol.toUpperCase()}</Badge>
                  <span className="max-w-32 truncate text-xs text-muted-foreground">{proxy.host + ":" + String(proxy.port)}</span>
                </span>
              </ProxyChoice>
            ))}
            {enabled.length === 0 ? <div className="px-2 py-2 text-xs text-muted-foreground">暂无已启用代理</div> : null}
            {unavailable ? (
              <div className="mt-2 border-t border-border pt-2">
                <div className="rounded-md border border-danger/20 bg-danger/5 px-2 py-2 text-xs text-danger">
                  <span className="block truncate">当前绑定：{selected?.name ?? "代理 #" + String(value)} · 不可用</span>
                </div>
              </div>
            ) : null}
          </div>
        </PopoverContent>
      </Popover>
    </div>
  )
}

function ProxyChoice({
  active,
  children,
  onClick,
}: {
  active: boolean
  children: React.ReactNode
  onClick: () => void
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        "flex w-full min-w-0 items-center justify-between gap-3 rounded-md px-2 py-2 text-left text-sm transition-colors hover:bg-muted",
        active && "bg-brand/10 text-brand",
      )}
    >
      {children}
    </button>
  )
}
