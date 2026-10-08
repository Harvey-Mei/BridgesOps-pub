import { useEffect, useState } from "react"
import { ChevronDown, ChevronUp, Loader2 } from "lucide-react"
import type { ModelCheckManualOption } from "@/lib/api-types"
import type { ModelCheckProgress } from "@/lib/model-check-stream"

const labels: Record<string, string> = { preparing: "核对检测目标", probe_started: "准备请求", request_sent: "请求已发起", response_headers: "收到响应头", response: "收到上游正文", probe_result: "本地校验", probe_error: "请求失败", audit_error: "记录保存失败", saving: "保存检测记录" }

export function ModelCheckLogPanel({ selection, entries, running, open, onOpenChange, startedAt, error }: {
  selection: ModelCheckManualOption; entries: ModelCheckProgress[]; running: boolean; open: boolean; onOpenChange: (open: boolean) => void; startedAt: number; error: string
}) {
  const [now, setNow] = useState(startedAt)
  useEffect(() => { if (!running) return; const timer = window.setInterval(() => setNow(Date.now()), 1000); return () => window.clearInterval(timer) }, [running, startedAt])
  return <section className="overflow-hidden rounded-xl border bg-card">
    <button type="button" className="flex w-full items-center justify-between gap-4 px-5 py-3 text-left" aria-expanded={open} aria-controls="pelican-live-log" onClick={() => onOpenChange(!open)}>
      <span className="flex min-w-0 items-center gap-3"><span className="shrink-0 font-medium">检测日志</span><span className="truncate text-sm text-muted-foreground">{selection.channel_name} / {selection.group_name} / {selection.model_name}</span></span>
      <span className="flex shrink-0 items-center gap-2 text-sm">{running ? <><Loader2 className="size-4 animate-spin" />进行中 · {Math.max(0, Math.floor((now - startedAt) / 1000))} 秒</> : error ? "未完整完成" : "已完成"}{open ? <ChevronUp className="size-4" /> : <ChevronDown className="size-4" />}</span>
    </button>
    <div id="pelican-live-log" hidden={!open} className="max-h-80 space-y-3 overflow-auto border-t bg-muted/15 px-5 py-4 text-sm">
      {entries.map((entry, index) => <div key={index} className="min-w-0 space-y-1.5">
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1"><time className="font-mono text-xs text-muted-foreground">{new Date(entry.at).toLocaleTimeString()}</time><span className="font-medium">{entry.kind === "pelican_svg" ? "鹈鹕绘制 · " : entry.kind === "candy_shapes" ? "糖果题 · " : ""}{labels[entry.type] || entry.type}</span>{entry.path ? <code>POST {entry.path}</code> : null}{entry.status_code ? <span>HTTP {entry.status_code}</span> : null}{entry.total_tokens ? <span>{entry.total_tokens} Token</span> : null}</div>
        {entry.message ? <p className="text-muted-foreground [overflow-wrap:anywhere]">{entry.message}</p> : null}
        {entry.request_body || entry.type === "response" && entry.response_text ? <details open><summary className="cursor-pointer text-xs text-brand">{entry.request_body ? "请求参数" : "上游返回正文"}</summary><pre className="mt-2 max-h-48 overflow-auto whitespace-pre-wrap break-all rounded-lg border bg-background p-3 font-mono text-xs leading-5">{entry.request_body || entry.response_text}</pre></details> : null}
      </div>)}
      {running ? <p className="flex items-center gap-2 text-muted-foreground"><Loader2 className="size-3.5 animate-spin" />等待下一步响应，折叠日志不会停止检测。</p> : null}
      {error ? <p role="alert" className="text-destructive [overflow-wrap:anywhere]">{error}</p> : null}
    </div>
  </section>
}
