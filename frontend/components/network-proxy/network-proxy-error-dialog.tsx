"use client"

import { AlertTriangle, Copy } from "lucide-react"
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
import type { NetworkProxy } from "@/lib/api-types"
import { dateTime } from "@/lib/format"

type NetworkProxyErrorSource = Pick<NetworkProxy, "name" | "last_error" | "last_tested_at" | "last_egress_ip">

export function NetworkProxyErrorBadge({
  proxy,
  onClick,
}: {
  proxy: NetworkProxyErrorSource
  onClick: () => void
}) {
  return (
    <button
      type="button"
      className="inline-flex h-6 items-center justify-center gap-1 rounded-md border border-danger/25 bg-danger/5 px-2 text-[11px] font-medium leading-none text-danger transition-colors hover:bg-danger/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-danger/30"
      aria-haspopup="dialog"
      aria-label={"查看 " + proxy.name + " 的异常详情"}
      title={"查看 " + proxy.name + " 的异常详情"}
      onClick={onClick}
    >
      <AlertTriangle className="size-3 shrink-0" />
      <span>异常</span>
    </button>
  )
}

export function NetworkProxyErrorDialog({
  proxy,
  onOpenChange,
}: {
  proxy: NetworkProxyErrorSource | null
  onOpenChange: (open: boolean) => void
}) {
  const error = typeof proxy?.last_error === "string" ? proxy.last_error.trim() : ""
  const detail = [
    proxy ? "代理：" + proxy.name : "",
    proxy?.last_tested_at ? "最近测试：" + dateTime(proxy.last_tested_at) : "",
    proxy?.last_egress_ip ? "出口 IP：" + proxy.last_egress_ip : "",
    "",
    "错误：",
    error || "暂无异常内容",
  ].join("\n")

  async function copyDetail() {
    try {
      await writeClipboard(detail)
      toast.success("异常详情已复制")
    } catch {
      toast.error("复制失败")
    }
  }

  return (
    <Dialog open={proxy != null} onOpenChange={onOpenChange}>
      <DialogContent className="w-[min(96vw,48rem)] max-w-none sm:max-w-2xl">
        <DialogHeader className="pr-8">
          <DialogTitle className="flex min-w-0 items-center gap-2 text-base">
            <span className="flex size-8 shrink-0 items-center justify-center rounded-md bg-danger/10 text-danger ring-1 ring-inset ring-danger/20">
              <AlertTriangle className="size-4" />
            </span>
            <span className="min-w-0 truncate">{proxy ? proxy.name + " · 异常详情" : "异常详情"}</span>
          </DialogTitle>
          <DialogDescription className="sr-only">查看完整异常信息</DialogDescription>
        </DialogHeader>
        <div className="max-h-[min(60vh,30rem)] overflow-auto rounded-lg border border-danger/20 bg-danger/5 p-3">
          <pre className="whitespace-pre-wrap break-words font-mono text-xs leading-5 text-foreground">{detail}</pre>
        </div>
        <DialogFooter>
          <Button type="button" variant="outline" size="sm" onClick={() => void copyDetail()}>
            <Copy className="size-3.5" />
            一键复制
          </Button>
          <Button type="button" size="sm" onClick={() => onOpenChange(false)}>关闭</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
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
  textarea.value = text
  textarea.setAttribute("readonly", "")
  textarea.style.position = "fixed"
  textarea.style.left = "-9999px"
  textarea.style.top = "0"
  document.body.appendChild(textarea)
  try {
    textarea.select()
    textarea.setSelectionRange(0, text.length)
    if (!document.execCommand("copy")) throw new Error("copy failed")
  } finally {
    document.body.removeChild(textarea)
  }
}
