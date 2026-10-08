import { useEffect, useMemo, useRef, useState, type FormEvent } from "react"
import { Loader2, RotateCcw, ShieldAlert } from "lucide-react"
import { toast } from "sonner"
import { apiFetch } from "@/lib/api"
import type { Channel, ChannelCostBasisOverrideResult, ChannelCostProfile } from "@/lib/api-types"
import {
  channelCostBasisOverrideBlockerLabel,
  channelCostBasisOverrideIdempotencyKey,
  costBasisOverrideIntent,
} from "@/lib/channel-accounting"
import { decimal } from "@/lib/format"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { useConfirm } from "@/components/ui/confirm-dialog"

interface Props {
  channel: Channel
  profile: ChannelCostProfile
  disabled: boolean
  onApplied: () => Promise<void>
  onConflict?: () => void
  onSubmittingChange?: (busy: boolean) => void
}

interface PendingIdempotencyKey {
  fingerprint: string
  value: string
}

export function ChannelCostBasisOverridePanel({ channel, profile, disabled, onApplied, onConflict, onSubmittingChange }: Props) {
  const readiness = profile.cost_basis_override
  const [factor, setFactor] = useState("")
  const [note, setNote] = useState("")
  const [submitting, setSubmitting] = useState(false)
  const submittingRef = useRef(false)
  const idempotencyKeyRef = useRef<PendingIdempotencyKey | null>(null)
  const { confirm, dialog: confirmDialog } = useConfirm()

  useEffect(() => {
    setFactor("")
    setNote("")
    setSubmitting(false)
    idempotencyKeyRef.current = null
  }, [channel.id, readiness.expected_version, readiness.expected_cost_unit_epoch])

  const validation = useMemo(
    () => costBasisOverrideIntent(readiness, factor, note),
    [factor, note, readiness],
  )
  const normalizedFactor = factor.trim()
  const nextFactor = Number(normalizedFactor)
  const projectedBalance = normalizedFactor && Number.isFinite(nextFactor) && nextFactor >= 0
    ? profile.state.raw_balance * nextFactor
    : null

  async function submit(event: FormEvent) {
    event.preventDefault()
    if (disabled || submittingRef.current) return
    if (!validation.intent) {
      toast.error(validation.error ?? "当前账务状态尚不允许手动覆盖")
      return
    }
    const intent = validation.intent
    const pendingCandidateNotice = readiness.pending_funding_candidates > 0
      ? ` 另有 ${readiness.pending_funding_candidates} 条待确认充值将同时归档。`
      : ""
    submittingRef.current = true
    onSubmittingChange?.(true)
    const approved = await confirm({
      title: `覆盖 ${channel.name} 的成本系数？`,
      description: `成本系数将从 ${decimal(readiness.current_funding_factor, 8)} 改为 ${decimal(intent.funding_factor, 8)}，不会新增充值记录；历史请求费用和上游原始分组倍率不会被修改。${pendingCandidateNotice}`,
      confirmLabel: "确认覆盖",
      destructive: true,
    })
    if (!approved) { submittingRef.current = false; onSubmittingChange?.(false); return }

    const fingerprint = JSON.stringify(intent)
    if (idempotencyKeyRef.current?.fingerprint !== fingerprint) {
      idempotencyKeyRef.current = {
        fingerprint,
        value: channelCostBasisOverrideIdempotencyKey(channel.id, intent.expected_version),
      }
    }
    setSubmitting(true)
    onSubmittingChange?.(true)
    try {
      const result = await apiFetch<ChannelCostBasisOverrideResult>(`/channels/${channel.id}/cost-basis-override`, {
        method: "POST",
        headers: { "Idempotency-Key": idempotencyKeyRef.current.value },
        body: JSON.stringify(intent),
      })
      await onApplied()
      const archivedNotice = result.audit.superseded_candidate_count > 0
        ? `，已归档 ${result.audit.superseded_candidate_count} 条待确认充值`
        : ""
      toast.success(result.idempotent_replay
        ? "手动覆盖结果已确认，无重复写入"
        : `成本系数已覆盖，账务代次 ${result.audit.applied_cost_unit_epoch}${archivedNotice}`)
    } catch (cause) {
      if ((cause as { status?: number })?.status === 409) onConflict?.()
      toast.error(cause instanceof Error ? cause.message : "手动校准成本系数失败")
    } finally {
      submittingRef.current = false
      setSubmitting(false)
      onSubmittingChange?.(false)
    }
  }

  return (
    <>
      <section className="overflow-hidden rounded-md border border-border">
        <div className="flex min-h-12 items-center justify-between gap-3 border-b bg-muted/20 px-4 py-3">
          <div className="flex min-w-0 items-center gap-2">
            <RotateCcw className="size-4 shrink-0 text-primary" />
            <h3 className="truncate text-sm font-semibold">{"手动校准成本系数"}</h3>
          </div>
          <span className="shrink-0 text-[11px] text-muted-foreground">{`版本 ${readiness.expected_version} · 代次 ${readiness.expected_cost_unit_epoch}`}</span>
        </div>

        {!readiness.eligible ? (
          <div className="space-y-2 px-4 py-3">
            <p className="text-xs font-medium text-foreground">{"当前不能执行手动覆盖"}</p>
            <div className="flex flex-wrap gap-1.5">
              {readiness.blockers.map((blocker) => (
                <span key={blocker} className="inline-flex min-h-6 items-center rounded border border-warning/25 bg-warning/5 px-2 text-[11px] text-warning">
                  {channelCostBasisOverrideBlockerLabel(blocker)}
                </span>
              ))}
            </div>
          </div>
        ) : (
          <form onSubmit={submit} className="space-y-4 p-4">
            <div className="grid items-end gap-4 sm:grid-cols-[14rem_minmax(0,1fr)]">
              <label className="grid gap-2"><Label htmlFor={`cost-basis-next-${channel.id}`}>新成本系数</Label><Input id={`cost-basis-next-${channel.id}`} type="number" min="0" step="any" inputMode="decimal" value={factor} onChange={event => setFactor(event.target.value)} disabled={disabled || submitting} autoComplete="off" required /></label>
              <p className="py-2 text-sm text-muted-foreground">{projectedBalance == null ? "输入新系数，预览校准后的资金余额。" : `校准后折合资金余额 ${decimal(projectedBalance, 6)}`}</p>
            </div>
            <label className="block space-y-1.5">
              <Label htmlFor={`cost-basis-note-${channel.id}`}>{"校准原因"}</Label>
              <Textarea
                id={`cost-basis-note-${channel.id}`}
                value={note}
                onChange={(event) => setNote(event.target.value)}
                rows={2}
                maxLength={1024}
                placeholder="核对依据、工单编号或人工修正原因"
                disabled={disabled || submitting}
                required
              />
            </label>

            <div className="flex items-center justify-between gap-4 border-t pt-3">
              <p className="text-sm text-muted-foreground">{readiness.pending_funding_candidates > 0 ? `本次校准将归档 ${readiness.pending_funding_candidates} 条待确认充值；若这些是真实充值，请先完成入账。` : "仅影响当前余额折算和后续新增费用。"}</p>
              <Button type="submit" variant="destructive" className="shrink-0" disabled={disabled || submitting || !validation.intent}>{submitting ? <Loader2 className="animate-spin" /> : <ShieldAlert />}{submitting ? "保存中…" : "校准成本系数"}</Button>
            </div>
          </form>
        )}
      </section>
      {confirmDialog}
    </>
  )
}
