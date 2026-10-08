"use client"

import { AlertTriangle, Copy, WalletCards } from "lucide-react"
import { toast } from "sonner"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import { relativeTime } from "@/lib/format"
import type { Channel } from "@/lib/api-types"

type ChannelErrorSource = Pick<
  Channel,
  | "name"
  | "last_error"
  | "balance_error"
  | "balance_error_at"
  | "upstream_account_balance_insufficient"
  | "upstream_account_balance_insufficient_at"
>

export function channelBalanceError(channel: Pick<Channel, "balance_error">) {
  const message = channel.balance_error?.trim()
  return message || null
}

export function hasUpstreamAccountBalanceInsufficient(
  channel: Pick<Channel, "upstream_account_balance_insufficient" | "upstream_account_balance_insufficient_at">,
) {
  return channel.upstream_account_balance_insufficient === true && Boolean(channel.upstream_account_balance_insufficient_at)
}

async function writeClipboard(text: string) {
  const clipboardWrite = navigator.clipboard?.writeText?.bind(navigator.clipboard)
  if (clipboardWrite) {
    try {
      await clipboardWrite(text)
      return
    } catch {
      // HTTP or denied clipboard access falls back to the hidden textarea below.
    }
  }

  const textarea = document.createElement("textarea")
  const previousFocus = document.activeElement
  textarea.value = text
  textarea.setAttribute("readonly", "")
  textarea.style.position = "fixed"
  textarea.style.opacity = "0"
  textarea.style.left = "0"
  textarea.style.top = "0"
  const dialogs = document.querySelectorAll('[data-slot="dialog-content"][data-state="open"]')
  const container = dialogs[dialogs.length - 1] ?? document.body
  container.appendChild(textarea)
  try {
    textarea.focus()
    textarea.select()
    textarea.setSelectionRange(0, text.length)
    if (!document.execCommand("copy")) throw new Error("copy failed")
  } finally {
    textarea.remove()
    if (previousFocus instanceof HTMLElement && previousFocus.isConnected) previousFocus.focus({ preventScroll: true })
  }
}

export function ChannelErrorBadge({
  channel,
  onClick,
}: {
  channel: ChannelErrorSource
  onClick: (trigger: HTMLButtonElement) => void
}) {
  return (
    <button
      type="button"
      className="inline-flex h-6 items-center justify-center gap-1 rounded-md border border-danger/25 bg-danger/5 px-2 text-[11px] font-medium leading-none text-danger transition-colors hover:bg-danger/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-danger/30"
      aria-haspopup="dialog"
      aria-label={`查看 ${channel.name} 的异常详情`}
      title={`查看 ${channel.name} 的异常详情`}
      onClick={(event) => onClick(event.currentTarget)}
    >
      <AlertTriangle className="size-3 shrink-0" />
      <span>{"异常"}</span>
    </button>
  )
}

export function ChannelBalanceErrorBadge({
  channel,
  onClick,
}: {
  channel: ChannelErrorSource
  onClick: (trigger: HTMLButtonElement) => void
}) {
  const observedAt = channel.balance_error_at
  if (!channelBalanceError(channel)) return null
  return (
    <button
      type="button"
      className="inline-flex h-6 max-w-full items-center justify-center gap-1 rounded-md border border-warning/30 bg-warning/5 px-2 text-[11px] font-medium leading-none text-warning transition-colors hover:bg-warning/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-warning/30"
      aria-haspopup="dialog"
      aria-label={`查看 ${channel.name} 的余额采集错误${observedAt ? `，${relativeTime(observedAt)}` : ""}`}
      title={observedAt ? `余额采集错误 · ${relativeTime(observedAt)}` : "余额采集错误"}
      onClick={(event) => onClick(event.currentTarget)}
    >
      <AlertTriangle className="size-3 shrink-0" />
      <span className="truncate">{"余额采集错误"}</span>
    </button>
  )
}

export function UpstreamAccountBalanceInsufficientBadge({ channel }: { channel: ChannelErrorSource }) {
  if (!hasUpstreamAccountBalanceInsufficient(channel)) return null
  const observedAt = channel.upstream_account_balance_insufficient_at
  return (
    <Tooltip delayDuration={150}>
      <TooltipTrigger asChild>
        <span
          tabIndex={0}
          className="inline-flex h-6 max-w-full cursor-help items-center justify-center gap-1 rounded-md border border-danger/25 bg-danger/5 px-2 text-[11px] font-medium leading-none text-danger ring-1 ring-inset ring-danger/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1"
          aria-label={`${channel.name} 上游账户余额不足${observedAt ? `，${relativeTime(observedAt)}` : ""}`}
        >
          <WalletCards className="size-3 shrink-0" />
          <span className="truncate">{"上游账户余额不足"}</span>
        </span>
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-80 space-y-1 text-xs">
        <p className="font-medium">{"上游账户余额不足"}</p>
        <p>{"上游结构化响应明确表示账户余额不足；这不是本地余额阈值判断，也不会把余额快照改成 0。"}</p>
        <p className="text-muted-foreground">{observedAt ? `最近观测：${relativeTime(observedAt)}` : "观测时间未知"}</p>
      </TooltipContent>
    </Tooltip>
  )
}

export function ChannelErrorDialog({
  channel,
  onOpenChange,
  restoreFocus,
}: {
  channel: ChannelErrorSource | null
  onOpenChange: (open: boolean) => void
  restoreFocus?: () => boolean
}) {
  const error = typeof channel?.last_error === "string" ? channel.last_error.trim() : ""
  const balanceError = channel ? channelBalanceError(channel) ?? "" : ""
  const balanceObservedAt = channel?.balance_error_at
  const balanceInsufficient = channel ? hasUpstreamAccountBalanceInsufficient(channel) : false
  const balanceInsufficientAt = channel?.upstream_account_balance_insufficient_at
  const copyText = [
    error ? `渠道异常\n${error}` : "",
    balanceError ? `余额采集错误${balanceObservedAt ? `（${balanceObservedAt}）` : ""}\n${balanceError}` : "",
    balanceInsufficient ? `上游账户余额不足${balanceInsufficientAt ? `（${balanceInsufficientAt}）` : ""}` : "",
  ].filter(Boolean).join("\n\n")

  async function copyError() {
    if (!copyText) return
    try {
      await writeClipboard(copyText)
      toast.success("异常详情已复制")
    } catch {
      toast.error("复制失败")
    }
  }

  return (
    <Dialog open={channel != null} onOpenChange={onOpenChange}>
      <DialogContent
        className="w-[min(96vw,48rem)] max-w-none sm:max-w-2xl"
        onCloseAutoFocus={(event) => {
          event.preventDefault()
          window.requestAnimationFrame(() => {
            if (restoreFocus?.()) return
            document.querySelector<HTMLElement>('[data-slot="page-header-trigger"]')?.focus({ preventScroll: true })
          })
        }}
      >
        <DialogHeader className="pr-8">
          <DialogTitle className="flex min-w-0 items-center gap-2 text-base">
            <span className="flex size-8 shrink-0 items-center justify-center rounded-md bg-danger/10 text-danger ring-1 ring-inset ring-danger/20">
              <AlertTriangle className="size-4" />
            </span>
            <span className="min-w-0 truncate">
              {channel ? `${channel.name} · 异常详情` : "异常详情"}
            </span>
          </DialogTitle>
          <DialogDescription className="sr-only">
            {channel ? `查看 ${channel.name} 的完整异常信息` : "查看完整异常信息"}
          </DialogDescription>
        </DialogHeader>

        <div className="max-h-[min(60vh,30rem)] space-y-3 overflow-auto">
          {error ? (
            <section className="rounded-lg border border-danger/20 bg-danger/5 p-3">
              <h3 className="mb-2 text-xs font-semibold text-danger">{"渠道异常"}</h3>
              <pre className="whitespace-pre-wrap break-words font-mono text-xs leading-5 text-foreground">{error}</pre>
            </section>
          ) : null}
          {balanceError ? (
            <section className="rounded-lg border border-warning/25 bg-warning/5 p-3">
              <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
                <h3 className="text-xs font-semibold text-warning">{"余额采集错误"}</h3>
                <span className="text-[11px] text-muted-foreground">{balanceObservedAt ? relativeTime(balanceObservedAt) : "时间未知"}</span>
              </div>
              <pre className="whitespace-pre-wrap break-words font-mono text-xs leading-5 text-foreground">{balanceError}</pre>
            </section>
          ) : null}
          {balanceInsufficient ? (
            <section className="rounded-lg border border-danger/20 bg-danger/5 p-3 text-xs leading-5">
              <h3 className="font-semibold text-danger">{"上游账户余额不足"}</h3>
              <p className="mt-1 text-foreground">{"上游结构化响应明确表示账户余额不足。"}</p>
              <p className="mt-1 text-muted-foreground">{balanceInsufficientAt ? `最近观测：${relativeTime(balanceInsufficientAt)}` : "观测时间未知"}</p>
            </section>
          ) : null}
          {!error && !balanceError && !balanceInsufficient ? (
            <div className="rounded-lg border border-border bg-muted/20 p-3 text-xs text-muted-foreground">{"暂无异常内容"}</div>
          ) : null}
        </div>

        <DialogFooter>
          <Button type="button" variant="outline" size="sm" onClick={() => void copyError()} disabled={!copyText}>
            <Copy className="size-3.5" />
            {"一键复制"}
          </Button>
          <Button type="button" size="sm" onClick={() => onOpenChange(false)}>
            {"关闭"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
