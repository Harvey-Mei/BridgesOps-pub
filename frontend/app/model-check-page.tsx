import { useEffect, useMemo, useRef, useState } from "react"
import { useSearchParams } from "react-router-dom"
import {
  Clock3,
  History,
  RefreshCw,
  Search,
  Settings2,
  Bike,
  ShieldQuestion,
} from "lucide-react"
import { toast } from "sonner"
import { apiFetch } from "@/lib/api"
import type {
  ModelCheckConfig,
  ModelCheckRun,
  ModelCheckRunStatus,
  ModelCheckTarget,
  ModelCheckTargetStatus,
  ModelCheckManualOption,
} from "@/lib/api-types"
import { compactTokenCount, dateTime, decimal, relativeTime } from "@/lib/format"
import { useModelCheckOverview, useModelCheckRuns, useModelCheckLatestRuns, useModelCheckManualOptions } from "@/lib/queries"
import { PelicanGallery, PelicanRunDetail } from "@/components/model-check/pelican-gallery"
import { cn } from "@/lib/utils"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PageHeader } from "@/components/ui/page-header"
import { ModelCheckLogPanel } from "@/components/model-check/model-check-log-panel"
import { streamModelCheck, type ModelCheckProgress } from "@/lib/model-check-stream"
import { ManualCheckPanel } from "@/components/model-check/manual-check-panel"
import { Switch } from "@/components/ui/switch"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"

type TargetFilter = "all" | "attention" | "healthy" | "pending" | "disabled"

const targetFilters: Array<{ value: TargetFilter; label: string }> = [
  { value: "all", label: "全部" },
  { value: "attention", label: "需要关注" },
  { value: "healthy", label: "健康" },
  { value: "pending", label: "待检测" },
  { value: "disabled", label: "已停用" },
]

export default function ModelCheckPage() {
  const overview = useModelCheckOverview()
  const latest = useModelCheckLatestRuns()
  const manualOptions = useModelCheckManualOptions()
  const [logSelection, setLogSelection] = useState<ModelCheckManualOption | null>(null)
  const [logEntries, setLogEntries] = useState<ModelCheckProgress[]>([])
  const [logOpen, setLogOpen] = useState(true)
  const [logRunning, setLogRunning] = useState(false)
  const [logStartedAt, setLogStartedAt] = useState(0)
  const [logError, setLogError] = useState("")
  const runController = useRef<AbortController | null>(null)
  useEffect(() => () => runController.current?.abort(), [])
  const [searchParams, setSearchParams] = useSearchParams()
  const [search, setSearch] = useState("")
  const [filter, setFilter] = useState<TargetFilter>("all")
  const [settingsOpen, setSettingsOpen] = useState(false)
  const [selectedTarget, setSelectedTarget] = useState<ModelCheckTarget | null>(null)
  const [busyTargets, setBusyTargets] = useState<Set<number>>(() => new Set())
  const targets = useMemo(() => overview.data?.targets ?? [], [overview.data?.targets])
  const linkedChannelID = Number(searchParams.get("channel")) > 0 ? Number(searchParams.get("channel")) : null
  const linkedIssuesOnly = searchParams.get("status") === "issue"
  const linkedChannelName = linkedChannelID == null ? "" : targets.find((target) => target.channel_id === linkedChannelID)?.channel_name ?? `渠道 #${linkedChannelID}`
  const normalizedSearch = search.trim().toLowerCase()
  const filteredTargets = useMemo(() => targets.filter((target) => {
    if (linkedChannelID != null && target.channel_id !== linkedChannelID) return false
    if (linkedIssuesOnly && target.status !== "suspect" && target.status !== "unavailable") return false
    const matchesSearch = !normalizedSearch || [target.channel_name, target.group_name, target.model_name, target.app]
      .join(" ").toLowerCase().includes(normalizedSearch)
    if (!matchesSearch) return false
    if (filter === "disabled") return !target.enabled
    if (!target.enabled) return filter === "all"
    if (filter === "attention") return target.status === "suspect" || target.status === "unavailable"
    if (filter === "healthy") return target.status === "healthy"
    if (filter === "pending") return target.status === "pending"
    return true
  }), [filter, linkedChannelID, linkedIssuesOnly, normalizedSearch, targets])

  function setTargetBusy(id: number, busy: boolean) {
    setBusyTargets((current) => {
      const next = new Set(current)
      if (busy) next.add(id)
      else next.delete(id)
      return next
    })
  }

  async function toggleTarget(target: ModelCheckTarget, enabled: boolean) {
    setTargetBusy(target.id, true)
    try {
      await apiFetch(`/model-checks/targets/${target.id}`, {
        method: "PATCH",
        body: JSON.stringify({ enabled }),
      })
      toast.success(enabled ? "已启用该检测目标" : "已停用该检测目标")
      overview.refetch()
    } catch (cause) {
      toast.error(cause instanceof Error ? cause.message : "更新检测目标失败")
    } finally {
      setTargetBusy(target.id, false)
    }
  }

  async function runManual(option: ModelCheckManualOption) {
    const controller = new AbortController()
    runController.current = controller
    setLogSelection(option); setLogEntries([]); setLogError(""); setLogOpen(true); setLogRunning(true); setLogStartedAt(Date.now())
    try {
      const run = await streamModelCheck(option, event => setLogEntries(current => [...current, event].slice(-40)), controller.signal)
      toast.success(run.status === "passed" ? "本地校验通过，动画已加入画廊" : run.status === "anomaly" ? "检测完成，请查看未通过的项目" : "检测未完整完成，请查看记录")
      overview.refetch()
      latest.refetch()
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : "手动检测失败"
      setLogError(message)
      toast.error(message)
      manualOptions.refetch()
    } finally {
      setLogRunning(false)
      if (runController.current === controller) runController.current = null
    }
  }

  const summary = overview.data?.summary
  const budget = overview.data?.config.requestBudget24h ?? 0

  return (
    <div className="space-y-4">
      <PageHeader
        icon={Bike}
        title="鹈鹕画廊"
        description="鹈鹕骑行 + 随机糖果题 · 本地校验，画面由你判断"
        actions={<>
          <Badge variant="outline" className={overview.data?.config.enabled ? "border-success/25 bg-success/5 text-success" : "border-warning/25 bg-warning/5 text-warning"}>
            {overview.data?.config.enabled ? "定时检测已启用" : "定时检测已关闭"}
          </Badge>
          <Button variant="outline" size="icon-sm" onClick={() => setSettingsOpen(true)} aria-label="配置鹈鹕检测" title="配置鹈鹕检测">
            <Settings2 className="size-3.5" />
          </Button>
          <Button variant="outline" size="icon-sm" onClick={() => { overview.refetch(); manualOptions.refetch(); latest.refetch() }} disabled={overview.refreshing || manualOptions.refreshing} aria-label="刷新检测目标" title="刷新检测目标">
            <RefreshCw className={cn("size-3.5", overview.refreshing && "animate-spin")} />
          </Button>
        </>}
      />

      {overview.error ? (
        <div className="flex items-center justify-between gap-3 rounded-lg border border-danger/25 bg-danger/5 px-4 py-3 text-sm text-danger">
          <span className="min-w-0 truncate">{`检测数据读取失败：${overview.error}`}</span>
          <Button size="sm" variant="outline" onClick={overview.refetch} disabled={overview.refreshing}>{"重试"}</Button>
        </div>
      ) : null}

      <ManualCheckPanel options={manualOptions.data ?? []} loading={manualOptions.loading} refreshing={manualOptions.refreshing} error={manualOptions.error} requests={summary?.requests_24h ?? 0} budget={budget} onRefresh={manualOptions.refetch} onRun={runManual} />

      {logSelection ? <ModelCheckLogPanel selection={logSelection} entries={logEntries} running={logRunning} open={logOpen} onOpenChange={setLogOpen} startedAt={logStartedAt} error={logError} /> : null}

      <PelicanGallery runs={latest.data ?? []} loading={latest.loading} error={latest.error} refreshing={latest.refreshing} onRefresh={latest.refetch} />

      <section className="overflow-hidden rounded-lg border border-border bg-background">
        <div className="flex min-h-12 flex-col gap-3 border-b border-border bg-muted/20 px-4 py-3 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex items-center gap-2">
            <h2 className="text-lg font-semibold tracking-tight">{"定时检测目标"}</h2>
            <Badge variant="outline">{`${filteredTargets.length}/${targets.length}`}</Badge>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            {linkedChannelID != null ? <Button variant="outline" size="sm" className="h-8 border-brand/20 bg-brand/5 text-brand" onClick={() => setSearchParams({})} title="清除渠道告警范围">{`${linkedChannelName}${linkedIssuesOnly ? " · 问题目标" : ""} ×`}</Button> : null}
            <div className="relative min-w-52 flex-1 lg:w-72 lg:flex-none">
              <Search className="pointer-events-none absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground" />
              <Input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="搜索渠道、分组或模型" className="h-8 pl-8 text-xs" />
            </div>
            <div className="flex flex-wrap items-center gap-1" aria-label="检测目标筛选">
              {targetFilters.map((item) => (
                <Button key={item.value} size="sm" variant={filter === item.value ? "default" : "outline"} className="h-8" onClick={() => setFilter(item.value)}>
                  {item.label}
                </Button>
              ))}
            </div>
          </div>
        </div>
        {overview.loading && !overview.data ? (
          <EmptyState icon={Clock3} text="正在读取检测目标" />
        ) : filteredTargets.length === 0 ? (
          <EmptyState icon={ShieldQuestion} text={targets.length === 0 ? "暂无符合条件的检测目标" : "没有匹配当前筛选的目标"} />
        ) : (
          <div className="overflow-x-auto">
            <Table className="min-w-[1040px] table-fixed">
              <TableHeader>
                <TableRow>
                  <TableHead className="w-[20%] text-left">{"渠道 / 分组"}</TableHead>
                  <TableHead className="w-[21%] text-left">{"检测模型"}</TableHead>
                  <TableHead className="w-[10%] text-center">{"协议"}</TableHead>
                  <TableHead className="w-[11%] text-center">{"状态"}</TableHead>
                  <TableHead className="w-[13%] text-center">{"分数 / 基线"}</TableHead>
                  <TableHead className="w-[11%] text-center">{"最近检测"}</TableHead>
                  <TableHead className="w-36 text-center">{"操作"}</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filteredTargets.map((target) => {
                  const busy = busyTargets.has(target.id)
                  return (
                    <TableRow key={target.id}>
                      <TableCell className="align-middle">
                        <div className="truncate font-semibold" title={target.channel_name}>{target.channel_name}</div>
                        <div className="mt-1 truncate text-xs text-muted-foreground" title={target.group_name}>{target.group_name}</div>
                      </TableCell>
                      <TableCell className="align-middle">
                        <div className="truncate font-medium" title={target.model_name}>{target.model_name}</div>
                        {target.last_actual_model && target.last_actual_model !== target.model_name ? <div className="mt-1 truncate text-xs text-muted-foreground" title={target.last_actual_model}>{`实际 ${target.last_actual_model}`}</div> : null}
                      </TableCell>
                      <TableCell className="text-center align-middle"><Badge variant="outline">{appLabel(target.app)}</Badge></TableCell>
                      <TableCell className="text-center align-middle"><TargetStatusBadge target={target} /></TableCell>
                      <TableCell className="text-center align-middle tabular-nums">
                        <div className="font-semibold">{target.last_score == null ? "—" : decimal(target.last_score, 1)}</div>
                        <div className="mt-1 text-xs text-muted-foreground">{target.baseline_score == null ? "基线 —" : `基线 ${decimal(target.baseline_score, 1)}`}</div>
                      </TableCell>
                      <TableCell className="text-center align-middle text-xs" title={target.last_checked_at ?? undefined}>{target.last_checked_at ? relativeTime(target.last_checked_at) : "未检测"}</TableCell>
                      <TableCell className="text-center align-middle">
                        <div className="flex items-center justify-center gap-1.5">
                          <Switch checked={target.enabled} onCheckedChange={(enabled) => void toggleTarget(target, enabled)} disabled={busy} aria-label={`${target.enabled ? "停用" : "启用"}${target.channel_name} ${target.group_name} ${target.model_name}`} />
                          <Button size="icon-sm" variant="outline" onClick={() => setSelectedTarget(target)} aria-label="查看检测历史" title="查看检测历史"><History className="size-3.5" /></Button>
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

      <ModelCheckSettingsDialog open={settingsOpen} onOpenChange={setSettingsOpen} config={overview.data?.config ?? null} onSaved={overview.refetch} />
      <TargetHistoryDialog target={selectedTarget} open={selectedTarget != null} onOpenChange={(open) => { if (!open) setSelectedTarget(null) }} />
    </div>
  )
}

function TargetStatusBadge({ target }: { target: ModelCheckTarget }) {
  if (!target.enabled) return <Badge variant="outline" className="border-border bg-muted/40 text-muted-foreground">{"已停用"}</Badge>
  const labels: Record<ModelCheckTargetStatus, string> = { pending: "待检测", healthy: "正常", suspect: "异常", unavailable: "不可用" }
  const classes: Record<ModelCheckTargetStatus, string> = {
    pending: "border-warning/25 bg-warning/5 text-warning",
    healthy: "border-success/25 bg-success/5 text-success",
    suspect: "border-danger/25 bg-danger/5 text-danger",
    unavailable: "border-warning/25 bg-warning/5 text-warning",
  }
  return <Badge variant="outline" className={classes[target.status]}>{labels[target.status]}</Badge>
}

function RunStatusBadge({ status }: { status: ModelCheckRunStatus }) {
  const label = status === "passed" ? "校验通过" : status === "anomaly" ? "校验未通过" : status === "deferred" ? "已延后" : "请求失败"
  const className = status === "passed" ? "border-success/25 bg-success/5 text-success" : status === "anomaly" ? "border-danger/25 bg-danger/5 text-danger" : "border-warning/25 bg-warning/5 text-warning"
  return <Badge variant="outline" className={className}>{label}</Badge>
}

function ModelCheckSettingsDialog({ open, onOpenChange, config, onSaved }: { open: boolean; onOpenChange: (open: boolean) => void; config: ModelCheckConfig | null; onSaved: () => void }) {
  const [draft, setDraft] = useState<ModelCheckConfig | null>(config)
  const [saving, setSaving] = useState(false)
  useEffect(() => { if (open) setDraft(config) }, [config, open])

  async function save() {
    if (!draft) return
    setSaving(true)
    try {
      await apiFetch("/model-checks/config", { method: "PATCH", body: JSON.stringify({ config: draft }) })
      toast.success("鹈鹕检测配置已保存")
      onOpenChange(false)
      onSaved()
    } catch (cause) {
      toast.error(cause instanceof Error ? cause.message : "保存检测配置失败")
    } finally {
      setSaving(false)
    }
  }

  const fields: Array<{ key: Exclude<keyof ModelCheckConfig, "enabled">; label: string; min: number; max: number }> = [
    { key: "runsPerDay", label: "每日轮次", min: 1, max: 12 },
    { key: "concurrency", label: "并发目标", min: 1, max: 4 },
    { key: "maxTargetsPerRun", label: "单轮目标", min: 1, max: 100 },
    { key: "requestBudget24h", label: "24 小时请求预算", min: 2, max: 2000 },
    { key: "timeoutSeconds", label: "糖果题超时（秒）", min: 10, max: 180 },
    { key: "pelicanTimeoutSeconds", label: "鹈鹕绘制超时（秒）", min: 60, max: 1800 },
    { key: "alertFailures", label: "连续告警次数", min: 1, max: 3 },
  ]

  return (
    <Dialog open={open} onOpenChange={onOpenChange} busy={saving}>
      <DialogContent className="sm:max-w-2xl">
        <DialogHeader><DialogTitle>{"鹈鹕画廊 · 运行配置"}</DialogTitle><DialogDescription className="sr-only">{"配置检测频率、并发、预算和告警阈值"}</DialogDescription></DialogHeader>
        {draft ? <div className="space-y-4">
          <p className="text-sm text-muted-foreground">每轮 2 次请求：糖果题与鹈鹕动画。全站仅保留最近 12 份完整结果，不调用视觉评分模型。</p>
          <div className="flex items-center justify-between rounded-lg border border-brand/15 bg-brand/5 px-4 py-3">
            <Label htmlFor="model-check-enabled" className="font-medium">{"启用定时检测"}</Label>
            <Switch id="model-check-enabled" checked={draft.enabled} onCheckedChange={(enabled) => setDraft({ ...draft, enabled })} disabled={saving} />
          </div>
          <div className="grid gap-3 sm:grid-cols-2">
            {fields.map((field) => <label key={field.key} className="space-y-1.5"><Label htmlFor={`model-check-${field.key}`}>{field.label}</Label><Input id={`model-check-${field.key}`} type="number" min={field.min} max={field.max} value={draft[field.key]} onChange={(event) => setDraft({ ...draft, [field.key]: Number(event.target.value) })} disabled={saving} /></label>)}
          </div>
        </div> : <EmptyState icon={Clock3} text="配置尚未加载" />}
        <DialogFooter><Button variant="outline" onClick={() => onOpenChange(false)} disabled={saving}>{"取消"}</Button><Button onClick={() => void save()} disabled={!draft || saving}>{saving ? "保存中" : "保存配置"}</Button></DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function TargetHistoryDialog({ target, open, onOpenChange }: { target: ModelCheckTarget | null; open: boolean; onOpenChange: (open: boolean) => void }) {
  const [page, setPage] = useState(1)
  const [selectedRun, setSelectedRun] = useState<ModelCheckRun | null>(null)
  const runs = useModelCheckRuns(target?.id ?? null, page, 20, open && target != null)
  useEffect(() => { setPage(1); setSelectedRun(null) }, [target?.id])
  const pages = Math.max(1, Math.ceil((runs.data?.total ?? 0) / (runs.data?.page_size ?? 20)))
  return <>
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-5xl">
        <DialogHeader><DialogTitle>{`${target?.channel_name ?? "检测目标"} / ${target?.group_name ?? "—"} / ${target?.model_name ?? "—"} · 检测历史`}</DialogTitle><DialogDescription className="sr-only">{"查看该检测目标的历史结果"}</DialogDescription></DialogHeader>
        {runs.error ? <div className="rounded-lg border border-danger/25 bg-danger/5 px-3 py-2 text-sm text-danger">{runs.error}</div> : null}
        <div className="max-h-[62vh] overflow-auto rounded-lg border">
          <Table className="min-w-[760px] table-fixed">
            <TableHeader><TableRow><TableHead className="w-[16%] text-center">{"状态"}</TableHead><TableHead className="w-[18%] text-center">{"分数"}</TableHead><TableHead className="w-[18%] text-center">{"Token"}</TableHead><TableHead className="w-[18%] text-center">{"耗时"}</TableHead><TableHead className="w-[20%] text-center">{"完成时间"}</TableHead><TableHead className="w-[10%] text-center">{"详情"}</TableHead></TableRow></TableHeader>
            <TableBody>{(runs.data?.items ?? []).map((run) => <TableRow key={run.id}><TableCell className="text-center align-middle"><RunStatusBadge status={run.status} /></TableCell><TableCell className="text-center align-middle font-semibold tabular-nums">{decimal(run.score, 1)}</TableCell><TableCell className="text-center align-middle tabular-nums" title={String(run.total_tokens)}>{compactTokenCount(run.total_tokens)}</TableCell><TableCell className="text-center align-middle tabular-nums">{formatDuration(run.latency_ms)}</TableCell><TableCell className="text-center align-middle text-xs" title={run.finished_at}>{dateTime(run.finished_at)}</TableCell><TableCell className="text-center align-middle"><Button size="icon-sm" variant="outline" onClick={() => setSelectedRun(run)} aria-label="查看检测详情" title="查看检测详情"><History className="size-3.5" /></Button></TableCell></TableRow>)}</TableBody>
          </Table>
          {!runs.loading && (runs.data?.items.length ?? 0) === 0 ? <EmptyState icon={History} text="暂无检测历史" /> : null}
        </div>
        <DialogFooter className="items-center sm:justify-between"><span className="text-xs text-muted-foreground">{`第 ${page}/${pages} 页 · ${runs.data?.total ?? 0} 条`}</span><div className="flex gap-2"><Button variant="outline" size="sm" onClick={() => setPage((value) => Math.max(1, value - 1))} disabled={page <= 1 || runs.refreshing}>{"上一页"}</Button><Button variant="outline" size="sm" onClick={() => setPage((value) => Math.min(pages, value + 1))} disabled={page >= pages || runs.refreshing}>{"下一页"}</Button></div></DialogFooter>
      </DialogContent>
    </Dialog>
    <RunDetailDialog run={selectedRun} open={selectedRun != null} onOpenChange={(next) => { if (!next) setSelectedRun(null) }} />
  </>
}

function RunDetailDialog(props: { run: ModelCheckRun | null; open: boolean; onOpenChange: (open: boolean) => void }) {
  return <PelicanRunDetail {...props} />
}

function EmptyState({ icon: Icon, text }: { icon: typeof Clock3; text: string }) {
  return <div className="flex min-h-40 flex-col items-center justify-center gap-2 p-6 text-center text-sm text-muted-foreground"><Icon className="size-5" /><span>{text}</span></div>
}

function appLabel(app: string) {
  if (app === "claude") return "Claude"
  if (app === "gemini") return "Gemini"
  return "Codex"
}

function formatDuration(ms: number) {
  if (!Number.isFinite(ms) || ms <= 0) return "—"
  if (ms < 1000) return `${Math.round(ms)} ms`
  return `${decimal(ms / 1000, 2)} s`
}
