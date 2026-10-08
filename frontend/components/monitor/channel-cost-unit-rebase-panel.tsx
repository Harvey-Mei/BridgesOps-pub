import { useEffect, useMemo, useRef, useState, type FormEvent } from "react"
import { AlertTriangle, ArrowLeftRight, Loader2, ShieldCheck } from "lucide-react"
import { toast } from "sonner"
import { apiFetch } from "@/lib/api"
import type {
  Channel,
  ChannelCostProfile,
  ChannelCostUnitRebaseResult,
} from "@/lib/api-types"
import {
  channelCostUnitRebaseBlockerLabel,
  channelCostUnitRebaseIdempotencyKey,
  costUnitRebaseIntent,
  providerFactorForMultiplier,
} from "@/lib/channel-accounting"
import { decimal } from "@/lib/format"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { useConfirm } from "@/components/ui/confirm-dialog"

interface ChannelCostUnitRebasePanelProps {
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

export function ChannelCostUnitRebasePanel({
  channel,
  profile,
  disabled,
  onApplied,
  onConflict,
  onSubmittingChange,
}: ChannelCostUnitRebasePanelProps) {
  const readiness = profile.cost_unit_rebase
  const reviewReason = channel.accounting_review_reason
    ?? (profile.state.status === "needs_review" ? "accounting_review" : "")
  const [currentMultiplier, setCurrentMultiplier] = useState("")
  const [note, setNote] = useState("")
  const [acknowledged, setAcknowledged] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const submittingRef = useRef(false)
  const idempotencyKeyRef = useRef<PendingIdempotencyKey | null>(null)
  const { confirm, dialog: confirmDialog } = useConfirm()

  useEffect(() => {
    setCurrentMultiplier("")
    setNote("")
    setAcknowledged(false)
    setSubmitting(false)
    idempotencyKeyRef.current = null
  }, [channel.id, readiness.expected_version])

  const validation = useMemo(
    () => costUnitRebaseIntent(readiness, currentMultiplier, note),
    [currentMultiplier, note, readiness],
  )
  const currentProviderFactor = providerFactorForMultiplier(
    Number(currentMultiplier),
    readiness.multiplier_mode,
  )
  const projectedFundingFactor = currentProviderFactor != null
    && readiness.previous_provider_factor != null
    && readiness.previous_provider_factor > 0
    ? profile.state.funding_factor * currentProviderFactor / readiness.previous_provider_factor
    : null

  async function submit(event: FormEvent) {
    event.preventDefault()
    if (disabled || submittingRef.current) return
    if (!validation.intent) {
      toast.error(validation.error ?? "当前账务证据尚不允许执行重基线")
      return
    }
    if (!acknowledged) {
      toast.error("请先确认已核对当前上游倍率")
      return
    }
    const intent = validation.intent
    submittingRef.current = true
    onSubmittingChange?.(true)
    const approved = await confirm({
      title: `重基线 ${channel.name} 的账务口径？`,
      description: `旧倍率 ${decimal(intent.previous_multiplier, 8)}，当前倍率 ${decimal(intent.current_multiplier, 8)}，账务版本 ${intent.expected_version}。服务端将重新采集余额、成本和全部分组倍率后原子切换；证据不一致时不会写入。`,
      confirmLabel: "执行重基线",
      destructive: true,
    })
    if (!approved) { submittingRef.current = false; onSubmittingChange?.(false); return }

    const fingerprint = JSON.stringify(intent)
    if (idempotencyKeyRef.current?.fingerprint !== fingerprint) {
      idempotencyKeyRef.current = {
        fingerprint,
        value: channelCostUnitRebaseIdempotencyKey(channel.id, intent.expected_version),
      }
    }
    setSubmitting(true)
    onSubmittingChange?.(true)
    try {
      const result = await apiFetch<ChannelCostUnitRebaseResult>(`/channels/${channel.id}/cost-unit-rebase`, {
        method: "POST",
        headers: { "Idempotency-Key": idempotencyKeyRef.current.value },
        body: JSON.stringify(intent),
      })
      await onApplied()
      setAcknowledged(false)
      toast.success(result.idempotent_replay ? "重基线结果已确认，无重复写入" : `账务口径已切换到代次 ${result.audit.applied_cost_unit_epoch}`)
    } catch (cause) {
      if ((cause as { status?: number })?.status === 409) onConflict?.()
      toast.error(cause instanceof Error ? cause.message : "账务重基线失败")
    } finally {
      submittingRef.current = false
      setSubmitting(false)
      onSubmittingChange?.(false)
    }
  }

  if (profile.state.status === "error") {
    return (
      <Alert variant="destructive" className="rounded-md border-danger/30 bg-danger/5">
        <AlertTriangle />
        <AlertTitle>{"账务状态异常"}</AlertTitle>
        <AlertDescription>{"当前金额投影不可用，请先刷新状态并核对账务证据。"}</AlertDescription>
      </Alert>
    )
  }
  if (profile.state.status !== "needs_review") return null

  if (reviewReason === "funding_basis_missing") {
    return (
      <Alert className="rounded-md border-warning/30 bg-warning/5 text-warning">
        <AlertTriangle />
        <AlertTitle>{"充值成本待校准"}</AlertTitle>
        <AlertDescription className="text-warning/90">{"缺少可靠的充值成本依据；记录最近一次充值后再核对金额投影。"}</AlertDescription>
      </Alert>
    )
  }

  return (
    <>
      <Alert className="rounded-md border-warning/30 bg-warning/5 text-warning">
        <AlertTriangle />
        <AlertTitle>{"账务口径待复核"}</AlertTitle>
        <AlertDescription className="text-warning/90">
          {reviewReason === "provider_factor_changed"
            ? "上游倍率与已确认口径不一致，金额账本和智能价格已暂停更新。"
            : "账务证据不一致，金额账本和智能价格已暂停更新。"}
        </AlertDescription>
      </Alert>

      <section className="overflow-hidden rounded-md border border-border">
        <div className="flex min-h-12 items-center justify-between gap-3 border-b bg-muted/20 px-4 py-3">
          <div className="flex min-w-0 items-center gap-2">
            <ArrowLeftRight className="size-4 shrink-0 text-primary" />
            <h3 className="truncate text-sm font-semibold">{"受控账务重基线"}</h3>
          </div>
          <span className="shrink-0 text-[11px] text-muted-foreground">{`版本 ${readiness.expected_version} · 代次 ${readiness.expected_cost_unit_epoch}`}</span>
        </div>

        {!readiness.eligible ? (
          <div className="space-y-2 px-4 py-3">
            <p className="text-xs font-medium text-foreground">{"当前不能执行自动重基线"}</p>
            <div className="flex flex-wrap gap-1.5">
              {readiness.blockers.map((blocker) => (
                <span key={blocker} className="inline-flex min-h-6 items-center rounded border border-warning/25 bg-warning/5 px-2 text-[11px] text-warning">
                  {channelCostUnitRebaseBlockerLabel(blocker)}
                </span>
              ))}
            </div>
          </div>
        ) : (
          <form onSubmit={submit} className="space-y-4 p-4">
            <div className="grid gap-3 sm:grid-cols-2">
              <label className="space-y-1.5">
                <Label htmlFor={`cost-unit-previous-${channel.id}`}>{"已确认旧倍率"}</Label>
                <Input
                  id={`cost-unit-previous-${channel.id}`}
                  value={readiness.previous_multiplier == null ? "" : decimal(readiness.previous_multiplier, 8)}
                  readOnly
                  disabled
                  className="font-mono tabular-nums"
                />
              </label>
              <label className="space-y-1.5">
                <Label htmlFor={`cost-unit-current-${channel.id}`}>{"当前上游倍率"}</Label>
                <Input
                  id={`cost-unit-current-${channel.id}`}
                  type="number"
                  min="0.00000001"
                  step="any"
                  inputMode="decimal"
                  value={currentMultiplier}
                  onChange={(event) => setCurrentMultiplier(event.target.value)}
                  disabled={disabled || submitting}
                  autoComplete="off"
                  required
                />
              </label>
            </div>

            <div className="grid gap-px overflow-hidden rounded-md border bg-border sm:grid-cols-3">
              <RebaseMetric label="换算模式" value={readiness.multiplier_mode === "divide" ? "除法" : "乘法"} />
              <RebaseMetric label="当前上游系数" value={currentProviderFactor == null ? "待输入" : `×${decimal(currentProviderFactor, 8)}`} />
              <RebaseMetric label="预计资金系数" value={projectedFundingFactor == null ? "待输入" : `×${decimal(projectedFundingFactor, 8)}`} />
            </div>

            <label className="block space-y-1.5">
              <Label htmlFor={`cost-unit-note-${channel.id}`}>{"核对说明"}</Label>
              <Textarea
                id={`cost-unit-note-${channel.id}`}
                value={note}
                onChange={(event) => setNote(event.target.value)}
                rows={2}
                maxLength={1024}
                placeholder="上游变更记录、核对时间或工单编号"
                disabled={disabled || submitting}
                required
              />
            </label>

            <div className="flex flex-col gap-3 border-t pt-4 sm:flex-row sm:items-center sm:justify-between">
              <label htmlFor={`cost-unit-ack-${channel.id}`} className="flex min-w-0 cursor-pointer items-start gap-2 text-xs text-muted-foreground">
                <Checkbox
                  id={`cost-unit-ack-${channel.id}`}
                  checked={acknowledged}
                  onCheckedChange={(checked) => setAcknowledged(checked === true)}
                  disabled={disabled || submitting}
                />
                <span>{"已在上游后台核对当前倍率，并确认渠道账号未发生切换"}</span>
              </label>
              <Button type="submit" className="shrink-0" disabled={disabled || submitting || !acknowledged || !validation.intent}>
                {submitting ? <Loader2 className="animate-spin" /> : <ShieldCheck />}
                {submitting ? "核验并切换中" : "核验并执行重基线"}
              </Button>
            </div>
          </form>
        )}
      </section>
      {confirmDialog}
    </>
  )
}

function RebaseMetric({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0 bg-background px-3 py-2.5 text-center">
      <div className="text-[11px] text-muted-foreground">{label}</div>
      <div className="mt-1 truncate font-mono text-xs font-semibold tabular-nums" title={value}>{value}</div>
    </div>
  )
}
