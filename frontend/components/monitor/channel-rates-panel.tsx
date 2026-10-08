"use client"

import { useRef, useState } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { useChannels, useChannelRates } from "@/lib/queries"
import { channelTypeLabel, formatRatio, relativeTime } from "@/lib/format"
import { cn } from "@/lib/utils"
import type { Channel } from "@/lib/api-types"
import { ChannelErrorBadge, ChannelErrorDialog } from "@/components/monitor/channel-error-dialog"

/**
 * 按倍率给 chip 上色：
 *   <= 0.8 折扣 → 绿
 *   0.8 ~ 1.2 正常 → 默认
 *   > 1.2 加价 → 橙
 *   > 2     高溢价 → 红
 */
function ratioTone(r: number): string {
  if (r <= 0.8) return "bg-success/10 text-success ring-success/20"
  if (r > 2) return "bg-danger/10 text-danger ring-danger/20"
  if (r > 1.2) return "bg-warning/10 text-warning ring-warning/20"
  return "bg-muted text-foreground ring-border"
}

function ChannelRateRow({ channel, onErrorClick }: { channel: Channel; onErrorClick: (channel: Channel, trigger: HTMLButtonElement) => void }) {
  const { data, loading } = useChannelRates(channel.id)
  const rates = [...(data ?? [])].sort((a, b) => a.ratio - b.ratio)
  const latest = rates[0]?.last_seen_at

  return (
    <div className="flex flex-col gap-2 rounded-lg border border-border p-3 sm:flex-row sm:items-start sm:gap-4">
      {/* 左：渠道名 + 类型 */}
      <div className="flex shrink-0 items-center gap-2 sm:w-44 sm:flex-col sm:items-start">
        <div className="flex items-center gap-2">
          <span className="text-sm font-semibold text-foreground">{channel.name}</span>
          <span
            className={cn(
              "inline-flex items-center rounded px-1.5 py-0.5 text-[10px] font-medium ring-1 ring-inset",
              channel.type === "newapi"
                ? "bg-brand/10 text-brand ring-brand/20"
                : "bg-foreground/5 text-foreground ring-border",
            )}
          >
            {channelTypeLabel(channel.type)}
          </span>
        </div>
        <span className="text-[11px] text-muted-foreground">
          {rates.length > 0
            ? `${rates.length} 个分组 · ${relativeTime(latest)}`
            : "暂无数据"}
        </span>
      </div>

      {/* 右：倍率 chip 流 */}
      <div className="min-w-0 flex-1">
        {loading ? (
          <p className="text-xs text-muted-foreground">{"加载中…"}</p>
        ) : rates.length === 0 ? (
          channel.last_error ? (
            <ChannelErrorBadge channel={channel} onClick={(trigger) => onErrorClick(channel, trigger)} />
          ) : (
            <p className="text-xs text-muted-foreground">{"尚未采集"}</p>
          )
        ) : (
          <div className="flex flex-wrap gap-1.5">
            {rates.map((r) => {
              const updated = `最近更新：${relativeTime(r.last_seen_at)}`
              const chip = (
                <span
                  key={r.id}
                  className={cn(
                    "inline-flex cursor-default items-center gap-1.5 rounded-md px-2 py-0.5 text-xs ring-1 ring-inset transition-colors",
                    "hover:bg-muted/60",
                    ratioTone(r.ratio),
                  )}
                >
                  <span className="font-medium">{r.model_name}</span>
                  <span className="rounded bg-primary/10 px-1 font-semibold tabular-nums text-primary ring-1 ring-inset ring-primary/15">
                    {formatRatio(r.ratio)}
                  </span>
                </span>
              )
              return (
                <Tooltip key={r.id} delayDuration={150}>
                  <TooltipTrigger asChild>{chip}</TooltipTrigger>
                  <TooltipContent side="top" className="max-w-xs text-xs">
                    <p className="font-medium">{r.model_name}</p>
                    {r.description ? (
                      <p className="mt-0.5 text-muted-foreground">{r.description}</p>
                    ) : (
                      <p className="mt-0.5 text-muted-foreground italic">(无描述)</p>
                    )}
                    <p className="mt-0.5 text-muted-foreground">{updated}</p>
                  </TooltipContent>
                </Tooltip>
              )
            })}
          </div>
        )}
      </div>
    </div>
  )
}

export function ChannelRatesPanel() {
  const { data: channels, loading } = useChannels()
  const list = channels ?? []
  const [errorChannel, setErrorChannel] = useState<Channel | null>(null)
  const errorTriggerRef = useRef<HTMLButtonElement | null>(null)

  return (
    <>
      <Card className="border border-border shadow-none">
        <CardHeader className="flex flex-row items-baseline justify-between pb-2">
          <CardTitle className="text-lg font-semibold tracking-tight text-foreground">{"分组倍率"}</CardTitle>
          <span className="text-xs text-muted-foreground">
            {"每个渠道的当前可用分组及倍率（绿=折扣 · 红=溢价）"}
          </span>
        </CardHeader>
        <CardContent className="space-y-2">
          {loading ? (
            <p className="rounded-lg border border-dashed border-border px-4 py-6 text-center text-xs text-muted-foreground">
              {"加载中…"}
            </p>
          ) : list.length === 0 ? (
            <p className="rounded-lg border border-dashed border-border px-4 py-6 text-center text-xs text-muted-foreground">
              {"还没有渠道"}
            </p>
          ) : (
            list.map((c) => (
              <ChannelRateRow
                key={c.id}
                channel={c}
                onErrorClick={(channel, trigger) => {
                  errorTriggerRef.current = trigger
                  setErrorChannel(channel)
                }}
              />
            ))
          )}
        </CardContent>
      </Card>

      <ChannelErrorDialog
        channel={errorChannel}
        onOpenChange={(open) => {
          if (!open) setErrorChannel(null)
        }}
        restoreFocus={() => {
          const trigger = errorTriggerRef.current
          errorTriggerRef.current = null
          if (!trigger?.isConnected) return false
          trigger.focus()
          return document.activeElement === trigger
        }}
      />
    </>
  )
}
