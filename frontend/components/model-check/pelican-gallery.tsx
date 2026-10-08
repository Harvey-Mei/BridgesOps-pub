import { useMemo, useState } from "react"
import { Bike, Copy, Pause, Play, RefreshCw } from "lucide-react"
import { toast } from "sonner"
import type { ModelCheckRun } from "@/lib/api-types"
import { compactTokenCount, dateTime } from "@/lib/format"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog"

function evidence(run: ModelCheckRun, kind: string) {
  return run.evidence.find(item => item.kind === kind)
}

function PelicanImage({ svg, paused, className }: { svg: string; paused: boolean; className: string }) {
  const source = useMemo(() => {
    if (!paused) return svg
    const doc = new DOMParser().parseFromString(svg, "image/svg+xml")
    doc.querySelectorAll("animate, animateTransform, animateMotion").forEach(node => node.remove())
    doc.querySelectorAll("*").forEach(node => {
      (node as SVGElement).style?.setProperty("animation-play-state", "paused", "important")
    })
    return new XMLSerializer().serializeToString(doc)
  }, [svg, paused])
  // Image context disables script and document access. Never insert model
  // output with innerHTML or a same-origin document/frame.
  return <img src={`data:image/svg+xml;charset=utf-8,${encodeURIComponent(source)}`} alt="本轮模型生成的鹈鹕骑行动画" className={className} loading="lazy" />
}

function CheckBadge({ item, label }: { item: Record<string, unknown> | undefined; label: string }) {
  const text = !item ? "未完成" : item.error ? "请求失败" : item.passed ? "通过" : "未通过"
  return <Badge variant="outline" className={item?.passed ? "border-success/25 bg-success/5 text-success" : "border-warning/25 bg-warning/5 text-warning"}>{label} · {text}</Badge>
}

export function PelicanGallery({ runs, loading, error, refreshing, onRefresh }: {
  runs: ModelCheckRun[]; loading: boolean; error: string | null; refreshing: boolean; onRefresh: () => void
}) {
  const [selected, setSelected] = useState<ModelCheckRun | null>(null)
  const [paused, setPaused] = useState(false)
  return <section className="overflow-hidden rounded-2xl border border-border bg-card">
    <div className="flex items-center justify-between gap-4 border-b px-5 py-4">
      <div className="flex items-baseline gap-3"><h2 className="text-lg font-semibold">鹈鹕骑行画廊</h2><span className="text-sm text-muted-foreground">全站最近 12 次 · 点击放大对比</span></div>
      <div className="flex gap-2"><Button size="sm" variant="ghost" onClick={() => setPaused(value => !value)}>{paused ? <Play /> : <Pause />}{paused ? "播放动画" : "暂停动画"}</Button><Button size="sm" variant="outline" onClick={onRefresh} disabled={refreshing}><RefreshCw className={refreshing ? "animate-spin" : ""} />刷新</Button></div>
    </div>
    {error ? <div role="alert" className="px-5 py-3 text-sm text-destructive">画廊读取失败，请刷新重试</div> : null}
    {!runs.length ? <div className="flex min-h-80 flex-col items-center justify-center gap-3 text-muted-foreground"><Bike className="size-12 text-brand/60" /><p>{loading ? "正在读取检测结果…" : "还没有骑行作品"}</p><p className="text-sm">在上方选择渠道、分组和模型，点击开始检测即可生成。</p></div> : <div className="grid auto-rows-fr grid-cols-2 gap-4 p-4 xl:grid-cols-5">
      {runs.slice(0, 12).map(run => {
        const pelican = evidence(run, "pelican_svg")
        const svg = typeof pelican?.artifact_svg === "string" ? pelican.artifact_svg : ""
        return <button key={run.id} type="button" onClick={() => setSelected(run)} aria-label={`查看 ${run.channel_name} ${run.model_name} 检测结果`} className="group flex flex-col overflow-hidden rounded-xl border text-left transition-colors hover:border-brand/50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
          <div className="flex flex-1 items-center justify-center overflow-hidden border-b bg-slate-50">
            {svg ? <PelicanImage svg={svg} paused={paused} className="block h-auto w-full" /> : <div className="flex min-h-[clamp(120px,14vh,200px)] items-center justify-center gap-2 text-sm text-muted-foreground"><Bike className="size-6" />{run.probe_version === "pelican-v1" ? "动画未完成" : "历史检测，无动画"}</div>}
          </div>
          <div className="w-full shrink-0 space-y-2 px-3 py-2.5"><div className="flex min-w-0 items-baseline justify-between gap-2"><span className="truncate text-sm font-medium" title={`${run.channel_name} / ${run.group_name}`}>{run.channel_name} <span className="text-muted-foreground">/ {run.group_name}</span></span><time dateTime={run.finished_at} className="shrink-0 text-xs text-muted-foreground" title={`记录 #${run.id}`}>{dateTime(run.finished_at)}</time></div><div className="flex min-w-0 items-center gap-1.5"><span className="min-w-0 flex-1 truncate text-sm text-muted-foreground" title={run.model_name}>{run.model_name}</span><CheckBadge item={evidence(run, "candy_shapes")} label="糖果" /><CheckBadge item={pelican} label="SVG" /></div></div>
        </button>
      })}
    </div>}
    <PelicanRunDetail run={selected} open={selected != null} onOpenChange={open => { if (!open) setSelected(null) }} />
  </section>
}

export function PelicanRunDetail({ run, open, onOpenChange }: { run: ModelCheckRun | null; open: boolean; onOpenChange: (open: boolean) => void }) {
  const pelican = run ? evidence(run, "pelican_svg") : undefined
  const candy = run ? evidence(run, "candy_shapes") : undefined
  const svg = typeof pelican?.artifact_svg === "string" ? pelican.artifact_svg : ""
  const [paused, setPaused] = useState(false)
  async function copy(text: string) {
    try {
      if (navigator.clipboard?.writeText && window.isSecureContext) {
        await navigator.clipboard.writeText(text)
      } else {
        const input = document.createElement("textarea")
        input.value = text
        input.style.cssText = "position:fixed;opacity:0;left:0;top:0"
        // Keep the temporary input inside the active dialog's focus trap.
        const dialog = Array.from(document.querySelectorAll('[data-slot="dialog-content"]')).at(-1)
        const previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null
        ;(dialog ?? document.body).append(input)
        try { input.select(); if (!document.execCommand("copy")) throw new Error("copy failed") } finally { input.remove(); previousFocus?.focus() }
      }
      toast.success("已复制")
    } catch { toast.error("复制失败，请展开题目与回答后手动复制") }
  }
  return <Dialog open={open} onOpenChange={onOpenChange}><DialogContent className="max-h-[calc(100vh-2rem)] overflow-y-auto sm:max-w-[min(1400px,94vw)]">
    <DialogHeader><DialogTitle>{run ? `${run.channel_name} / ${run.group_name} · ${run.model_name}` : "检测详情"}</DialogTitle></DialogHeader>
    {run ? <div className="grid min-w-0 gap-5 lg:grid-cols-[minmax(0,1fr)_320px]">
      <div className="min-w-0"><div className="flex h-[min(62vh,620px)] items-center justify-center rounded-xl border bg-slate-50">{svg ? <PelicanImage svg={svg} paused={paused} className="size-full object-contain" /> : <span className="text-sm text-muted-foreground">本轮没有可预览的 SVG</span>}</div><div className="mt-3 flex items-center justify-between gap-3"><p className="text-sm text-muted-foreground">本地校验不判断画面质量，请核对鹈鹕形体、构图与踩踏动作。</p>{svg ? <Button size="sm" variant="outline" onClick={() => setPaused(value => !value)}>{paused ? <Play /> : <Pause />}{paused ? "播放" : "暂停"}</Button> : null}</div></div>
      <div className="min-w-0 space-y-5"><div className="flex flex-wrap gap-2"><CheckBadge item={candy} label="糖果题" /><CheckBadge item={pelican} label="SVG 校验" /></div>
        <dl className="grid grid-cols-[80px_1fr] gap-x-3 gap-y-2 text-sm"><dt className="text-muted-foreground">渠道</dt><dd className="break-words">{run.channel_name}</dd><dt className="text-muted-foreground">分组</dt><dd className="break-words">{run.group_name}</dd><dt className="text-muted-foreground">请求模型</dt><dd className="break-all">{run.model_name}</dd><dt className="text-muted-foreground">协议</dt><dd>{run.app === "claude" ? "Messages" : "Chat Completions"}</dd><dt className="text-muted-foreground">场景</dt><dd>{typeof pelican?.scene === "string" ? pelican.scene : "—"}</dd><dt className="text-muted-foreground">时间</dt><dd>{dateTime(run.finished_at)}</dd><dt className="text-muted-foreground">Token</dt><dd>{compactTokenCount(run.total_tokens)}</dd><dt className="text-muted-foreground">请求</dt><dd>{run.request_count} 次</dd></dl>
        <div className="max-h-64 overflow-y-auto rounded-lg border p-3 text-sm [overflow-wrap:anywhere]"><p className="mb-2 font-medium">校验结果</p><p>{typeof candy?.detail === "string" ? candy.detail : "糖果题未完成"}</p><p className="mt-2">{typeof pelican?.detail === "string" ? pelican.detail : "SVG 校验未完成"}</p>{run.reasons.map(reason => <p key={reason} className="mt-2 text-warning">{reason}</p>)}{run.error ? <p className="mt-2 text-warning">{run.error}</p> : null}</div>
        <div className="flex flex-wrap gap-2">{svg ? <Button variant="outline" size="sm" onClick={() => void copy(svg)}><Copy />复制 SVG</Button> : null}<Button variant="outline" size="sm" onClick={() => void copy(JSON.stringify(run, null, 2))}><Copy />复制完整记录</Button></div>
        <details className="rounded-lg border p-3"><summary className="cursor-pointer text-sm font-medium">题目与回答</summary><div className="mt-3 max-h-72 space-y-4 overflow-auto">{run.evidence.map((item, index) => <div key={index} className="space-y-2 text-sm"><p className="font-medium">{item.kind === "pelican_svg" ? "鹈鹕动画" : item.kind === "candy_shapes" ? "糖果题" : "历史题目"}</p><pre className="whitespace-pre-wrap break-words font-sans">{String(item.prompt || "")}</pre><pre className="whitespace-pre-wrap break-words rounded bg-muted p-2 text-xs">{String(item.response || item.error || "无回答")}</pre>{item.kind === "candy_shapes" ? <p className="break-all">期望：{String(item.expected || "")}</p> : null}</div>)}</div></details>
      </div>
    </div> : null}
  </DialogContent></Dialog>
}
