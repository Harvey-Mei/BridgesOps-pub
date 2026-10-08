import { useState } from "react"
import { CheckCircle2, RefreshCw } from "lucide-react"
import { toast } from "sonner"
import { apiFetch } from "@/lib/api"
import type { GatewayAutomaticAssignmentPreview, GatewayCandidateGroup, GatewayUpstreamGroupChange } from "@/lib/api-types"
import { applyApprovedAssignments, automaticAssignmentReasonLabel } from "@/lib/automatic-group-assignment"
import { Button } from "@/components/ui/button"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { HelpHint } from "@/components/ui/help-hint"

export function AssignmentQuickAction({ changes, groups, scopeLabel, disabled, onLockChange, onFinished }: {
  changes: GatewayUpstreamGroupChange[]
  groups: GatewayCandidateGroup[]
  scopeLabel: string
  disabled: boolean
  onLockChange: (locked: boolean) => void
  onFinished: (applied: number[]) => Promise<void>
}) {
  const [open, setOpen] = useState(false)
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [plan, setPlan] = useState<GatewayAutomaticAssignmentPreview[]>([])
  const [frozenChanges, setFrozenChanges] = useState<GatewayUpstreamGroupChange[]>([])
  const [frozenLabel, setFrozenLabel] = useState("")
  const [page, setPage] = useState(0)
  const [scope, setScope] = useState<GatewayUpstreamGroupChange[]>([])
  const [result, setResult] = useState<Awaited<ReturnType<typeof applyApprovedAssignments>> | null>(null)
  const groupNames = new Map(groups.map(group => [group.id, group.name]))
  const ready = plan.filter(item => item.state === "ready" && item.evaluation_token && item.suggested_candidate_group_ids.length > 0 && item.suggested_candidate_group_ids.every(id => groupNames.has(id)))

  async function loadPage(targets: GatewayUpstreamGroupChange[], nextPage: number) {
    const pageTargets = targets.slice(nextPage * 100, (nextPage + 1) * 100)
    if (!pageTargets.length) return false
    setLoading(true)
    try {
      const ids = new Set(pageTargets.map(item => item.id))
      const previews = await apiFetch<GatewayAutomaticAssignmentPreview[]>("/gateway/automatic-assignment/preview", { method: "POST", body: JSON.stringify({ change_ids: [...ids] }) })
      setScope(pageTargets)
      setPage(nextPage)
      setPlan(previews.filter(item => ids.has(item.change_id)))
      setResult(null)
      return true
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "预览失败")
      return false
    } finally { setLoading(false) }
  }

  async function prepare() {
    const targets = changes.filter(item => item.kind === "added" && item.status === "pending" && item.assignment_actionable === true)
    if (!targets.length) { toast.info("当前范围没有可处理项，请检查同步状态或选择其他变更"); return }
    onLockChange(true)
    setFrozenChanges(targets)
    setFrozenLabel(scopeLabel)
    if (await loadPage(targets, 0)) setOpen(true)
    else onLockChange(false)
  }

  async function apply() {
    setSaving(true)
    try {
      const outcome = await applyApprovedAssignments(ready, async id => {
        const previews = await apiFetch<GatewayAutomaticAssignmentPreview[]>("/gateway/automatic-assignment/preview", { method: "POST", body: JSON.stringify({ change_ids: [id] }) })
        return previews.find(item => item.change_id === id)
      }, async item => {
        await apiFetch("/gateway/automatic-assignment/execute", { method: "POST", body: JSON.stringify({ change_id: item.change_id, evaluation_token: item.evaluation_token }) })
      })
      setResult(outcome)
      await onFinished(outcome.applied)
    } catch (error) { toast.error(error instanceof Error ? error.message : "刷新结果失败，请重新核对") }
    finally { setSaving(false) }
  }

  return <>
    <Button size="sm" disabled={disabled || loading || changes.length === 0} onClick={() => void prepare()}>
      {loading ? <RefreshCw className="size-4 animate-spin" /> : <CheckCircle2 className="size-4" />}按规则处理
    </Button>
    <Dialog open={open} onOpenChange={next => { if (!saving && !loading) { setOpen(next); onLockChange(next) } }}>
      <DialogContent className="sm:max-w-3xl" onEscapeKeyDown={event => { if (saving || loading) event.preventDefault() }} onPointerDownOutside={event => { if (saving || loading) event.preventDefault() }}>
        <DialogHeader>
          <DialogTitle>{result ? "处理结果" : "确认自动归组"}</DialogTitle>
          <DialogDescription>{result ? `已完成 ${result.applied.length} 项，其余 ${scope.length - result.applied.length} 项请按下方结果核对。` : `${frozenLabel} · 共 ${frozenChanges.length} 项，本批 ${scope.length} 项，${ready.length} 项可按规则处理。`}</DialogDescription>
        </DialogHeader>
        <div className="max-h-[55vh] overflow-auto rounded-lg border border-border">
          <div className="grid grid-cols-[minmax(0,1fr)_minmax(0,1fr)_8rem] gap-3 border-b bg-muted px-4 py-2 text-sm font-medium"><span>渠道 / 分组</span><span>目标候选分组</span><span>结果</span></div>
          {scope.map(change => {
            const preview = plan.find(item => item.change_id === change.id)
            const issue = result?.issues.find(item => item.id === change.id)
            const applied = result?.applied.includes(change.id)
            const canApply = ready.some(item => item.change_id === change.id)
            return <div key={change.id} className="grid grid-cols-[minmax(0,1fr)_minmax(0,1fr)_8rem] items-center gap-3 border-b last:border-b-0 px-4 py-3 text-sm">
              <span className="truncate" title={`${change.channel_name || change.channel_id} / ${change.group_name}`}>{change.channel_name || change.channel_id} / {change.group_name}</span>
              <span className="truncate" title={preview?.suggested_candidate_group_ids.map(id => groupNames.get(id) || `#${id}`).join("、")}>{preview?.suggested_candidate_group_ids.map(id => groupNames.get(id) || `#${id}`).join("、") || "—"}</span>
              <span className="flex items-center gap-1">{applied ? "已完成" : issue ? (issue.kind === "unconfirmed" ? "结果未确认" : issue.kind === "unexecuted" ? "未执行" : "需重新核对") : canApply ? "待应用" : "待处理"}{!applied && <HelpHint label={`${change.group_name}处理原因`}>{issue?.reason || (preview ? automaticAssignmentReasonLabel(preview.reason_code) : "未返回判定，请刷新后核对")}</HelpHint>}</span>
            </div>
          })}
        </div>
        <DialogFooter>
          {frozenChanges.length > 100 && <div className="mr-auto flex items-center gap-2">
            <Button variant="outline" disabled={saving || loading || page === 0} onClick={() => void loadPage(frozenChanges, page - 1)}>上一批</Button>
            <span className="text-sm text-muted-foreground">{page + 1} / {Math.ceil(frozenChanges.length / 100)}</span>
            <Button variant="outline" disabled={saving || loading || (page + 1) * 100 >= frozenChanges.length} onClick={() => void loadPage(frozenChanges, page + 1)}>下一批</Button>
          </div>}
          <Button variant="outline" disabled={saving || loading} onClick={() => { setOpen(false); onLockChange(false) }}>{result ? "关闭" : "取消"}</Button>
          {!result && <Button disabled={saving || loading || ready.length === 0} onClick={() => void apply()}>{saving ? "正在逐项核验…" : `确认处理 ${ready.length} 项`}</Button>}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  </>
}
