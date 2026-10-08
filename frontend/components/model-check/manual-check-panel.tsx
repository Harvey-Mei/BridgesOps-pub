import { useMemo, useRef, useState } from "react"
import { Play, RefreshCw } from "lucide-react"
import type { ModelCheckManualOption } from "@/lib/api-types"
import { useModelCheckManualModels } from "@/lib/queries"
import { Button } from "@/components/ui/button"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"

const storageKey = "bridgeops.pelican.manual-selection.v1"
type Selection = Partial<ModelCheckManualOption>

function restoreSelection(): Selection {
  try {
    const value = JSON.parse(window.localStorage.getItem(storageKey) || "null")
    if (!value || !Number.isSafeInteger(value.channel_id) || value.channel_id <= 0) return {}
    return {
      channel_id: value.channel_id,
      identity_revision: Number.isSafeInteger(value.identity_revision) ? value.identity_revision : undefined,
      channel_name: typeof value.channel_name === "string" ? value.channel_name.slice(0, 128) : undefined,
      group_name: typeof value.group_name === "string" ? value.group_name.slice(0, 128) : undefined,
      model_name: typeof value.model_name === "string" ? value.model_name.slice(0, 256) : undefined,
      app: ["codex", "claude", "gemini"].includes(value.app) ? value.app : undefined,
    }
  } catch { return {} }
}

function modelChoiceKey(option: Selection) { return JSON.stringify([option.model_name, option.app, option.identity_revision]) }

export function ManualCheckPanel({ options, loading, refreshing, error, requests, budget, onRefresh, onRun }: {
  options: ModelCheckManualOption[]; loading: boolean; refreshing: boolean; error: string | null
  requests: number; budget: number; onRefresh: () => void; onRun: (option: ModelCheckManualOption) => Promise<void>
}) {
  const [selection, setSelection] = useState<Selection>(restoreSelection)
  const [running, setRunning] = useState(false)
  const inFlight = useRef(false)
  const channels = useMemo(() => Array.from(new Map(options.map(option => [option.channel_id, option])).values()).sort((a, b) => a.channel_name.localeCompare(b.channel_name)), [options])
  const channelOptions = options.filter(option => option.channel_id === selection.channel_id)
  const groups = Array.from(new Set(channelOptions.map(option => option.group_name))).sort()
  const group = channelOptions.find(option => option.group_name === selection.group_name)
  const upstreamModels = useModelCheckManualModels(group)
  const models = [...(upstreamModels.data ?? [])].sort((a, b) => a.model_name.localeCompare(b.model_name) || a.app.localeCompare(b.app))
  const selected = models.find(option => option.model_name === selection.model_name && option.app === selection.app && option.identity_revision === selection.identity_revision)
  const modelsBusy = upstreamModels.loading || upstreamModels.refreshing
  const inventoryError = error || upstreamModels.error
  const unavailable = !loading && !refreshing && !modelsBusy && !inventoryError && !!selection.model_name && !selected

  function remember(next: Selection) {
    setSelection(next)
    try { window.localStorage.setItem(storageKey, JSON.stringify(next)) } catch { /* Selection remains usable when browser storage is restricted. */ }
  }
  async function run() {
    if (inFlight.current || !selected || inventoryError || loading || refreshing || modelsBusy) return
    inFlight.current = true
    setRunning(true)
    try { await onRun(selected) } finally { inFlight.current = false; setRunning(false) }
  }

  return <section className="rounded-2xl border border-border bg-card px-5 py-4" aria-label="手动检测">
    <div className="mb-3 flex items-center justify-between gap-4"><h2 className="font-semibold">选择检测目标</h2><div className="flex items-center gap-3 text-sm text-muted-foreground"><span>每轮 2 次请求 · 24 小时已用 {requests} / {budget}</span><Button variant="ghost" size="icon-sm" onClick={() => { onRefresh(); upstreamModels.refetch() }} disabled={running || refreshing || modelsBusy} aria-label="刷新可选渠道和模型"><RefreshCw className={refreshing ? "animate-spin" : ""} /></Button></div></div>
    <div className="grid grid-cols-[minmax(160px,1fr)_minmax(160px,1fr)_minmax(240px,1.5fr)_auto] items-end gap-4">
      <label className="grid min-w-0 gap-2 text-sm font-medium">渠道<Select value={selection.channel_id ? String(selection.channel_id) : ""} onValueChange={id => { const option = channels.find(item => item.channel_id === Number(id)); if (option) remember({ channel_id: option.channel_id, channel_name: option.channel_name, identity_revision: option.identity_revision }) }} disabled={running || loading || !!error}><SelectTrigger className="w-full" aria-label="检测渠道"><SelectValue placeholder={loading ? "读取渠道中…" : "请选择渠道"}>{selection.channel_id ? channels.find(item => item.channel_id === selection.channel_id)?.channel_name ?? selection.channel_name ?? `渠道 #${selection.channel_id}` : undefined}</SelectValue></SelectTrigger><SelectContent>{channels.map(option => <SelectItem key={option.channel_id} value={String(option.channel_id)}>{option.channel_name}</SelectItem>)}</SelectContent></Select></label>
      <label className="grid min-w-0 gap-2 text-sm font-medium">分组<Select value={selection.group_name ?? ""} onValueChange={group => { const option = channelOptions.find(item => item.group_name === group); if (option) remember({ channel_id: option.channel_id, channel_name: option.channel_name, identity_revision: option.identity_revision, group_name: group }) }} disabled={running || !channelOptions.length || !!error}><SelectTrigger className="w-full" aria-label="检测分组"><SelectValue placeholder="请选择分组">{selection.group_name}</SelectValue></SelectTrigger><SelectContent>{groups.map(group => <SelectItem key={group} value={group}>{group}</SelectItem>)}</SelectContent></Select></label>
      <label className="grid min-w-0 gap-2 text-sm font-medium">模型<Select value={selection.model_name ? modelChoiceKey(selection) : ""} onValueChange={value => { const option = models.find(item => modelChoiceKey(item) === value); if (option) remember(option) }} disabled={running || modelsBusy || !models.length || !!inventoryError}><SelectTrigger className="w-full" aria-label="检测模型"><SelectValue placeholder={modelsBusy ? "读取上游模型中…" : "请选择模型"}>{selection.model_name}</SelectValue></SelectTrigger><SelectContent>{models.map(option => <SelectItem key={modelChoiceKey(option)} value={modelChoiceKey(option)}>{option.model_name}<span className="ml-2 text-muted-foreground">{option.app === "claude" ? "Messages" : "Chat Completions"}</span></SelectItem>)}</SelectContent></Select></label>
      <Button onClick={() => void run()} disabled={running || !selected || loading || refreshing || modelsBusy || !!inventoryError}><Play className={running ? "animate-pulse" : ""} />{running ? "检测中…" : "开始检测"}</Button>
    </div>
    {inventoryError ? <p role="alert" className="mt-3 text-sm text-destructive">{upstreamModels.error || "可选目标读取失败，请刷新重试。"}</p> : unavailable ? <p role="status" className="mt-3 text-sm text-warning">上次选择的目标已不可用或渠道配置已更新。选择已保留，请重新选择后检测。</p> : group && !modelsBusy && !models.length ? <p className="mt-3 text-sm text-muted-foreground">该分组的上游未返回支持的模型。</p> : !loading && !options.length ? <p className="mt-3 text-sm text-muted-foreground">暂无已同步的可选模型。请先同步渠道模型，并将分组加入候选范围。</p> : null}
  </section>
}
