import { HelpHint } from "@/components/ui/help-hint"
import { useEffect, useRef, useState, type ReactNode } from "react"
import { toast } from "sonner"
import { BellRing, CalendarClock, Coins, RefreshCw, Send, SlidersHorizontal, WalletCards } from "lucide-react"
import { NotificationStatus } from "@/components/monitor/bottom-panels"
import { NumberDraftInput } from "@/components/number-draft-input"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Switch } from "@/components/ui/switch"
import { apiFetch } from "@/lib/api"
import { useTriggerRefresh } from "@/lib/refresh-context"
import type { ApplyConfigResult, SystemNotificationsConfig } from "@/lib/api-types"
import { useSystemConfig } from "@/lib/queries"
import { PageHeader } from "@/components/ui/page-header"

export default function NotificationsPage() {
  const config = useSystemConfig()
  const refresh = useTriggerRefresh()
  const [policy, setPolicy] = useState<SystemNotificationsConfig | null>(null)
  const [saving, setSaving] = useState(false)
  const [applying, setApplying] = useState(false)
  const [pendingApply, setPendingApply] = useState(false)
  const [dirtyNumbers, setDirtyNumbers] = useState<Set<string>>(() => new Set())
  const [policyRevision, setPolicyRevision] = useState(0)
  const dirtyNumbersRef = useRef(dirtyNumbers)
  const currentPolicyRef = useRef<SystemNotificationsConfig | null>(policy)
  const serverPolicySnapshot = useRef<string | null>(null)
  const savedPolicySnapshot = useRef<string | null>(null)
  const savedPolicyValue = useRef<SystemNotificationsConfig | null>(null)
  const editGeneration = useRef(0)
  const acceptNextConfig = useRef(false)
  dirtyNumbersRef.current = dirtyNumbers
  currentPolicyRef.current = policy

  useEffect(() => {
    const next = config.data?.config.notifications
    if (!next) return
    const nextSnapshot = JSON.stringify(next)
    if (acceptNextConfig.current) {
      acceptNextConfig.current = false
      serverPolicySnapshot.current = nextSnapshot
      const current = currentPolicyRef.current
      const continuedEditing = dirtyNumbersRef.current.size > 0
        || Boolean(current && savedPolicySnapshot.current && JSON.stringify(current) !== savedPolicySnapshot.current)
      if (!continuedEditing) {
        setPolicy(next)
        setDirtyNumbers(new Set())
        setPolicyRevision((revision) => revision + 1)
      }
      savedPolicySnapshot.current = null
      savedPolicyValue.current = null
      return
    }
    setPolicy((current) => {
      const dirty = Boolean(
        dirtyNumbersRef.current.size > 0
        || (current && serverPolicySnapshot.current && JSON.stringify(current) !== serverPolicySnapshot.current),
      )
      if (dirty || (current && JSON.stringify(current) === nextSnapshot)) return current
      return next
    })
    serverPolicySnapshot.current = nextSnapshot
  }, [config.data])

  const dirty = Boolean(
    dirtyNumbers.size > 0
    || (policy && serverPolicySnapshot.current && JSON.stringify(policy) !== serverPolicySnapshot.current),
  )

  function updateDraftState(key: string, isDirty: boolean) {
    editGeneration.current += 1
    setDirtyNumbers((current) => {
      const next = new Set(current)
      if (isDirty) next.add(key)
      else next.delete(key)
      if (next.size === current.size && Array.from(next).every((item) => current.has(item))) return current
      return next
    })
  }

  function updatePolicy<K extends keyof SystemNotificationsConfig>(key: K, value: SystemNotificationsConfig[K]) {
    editGeneration.current += 1
    setPolicy((current) => current ? { ...current, [key]: value } : current)
  }

  async function savePolicy() {
    const base = config.data?.config
    if (!base || !policy) return
    const shouldSave = dirty
    if (!shouldSave && !pendingApply) return
    const policyToSave = policy
    const generationAtSave = editGeneration.current
    setSaving(true)
    let saved = false
    try {
      if (shouldSave) {
        await apiFetch("/settings/config/notifications", {
          method: "PATCH",
          body: JSON.stringify({ ...base, notifications: policyToSave }),
        })
        const savedSnapshot = JSON.stringify(policyToSave)
        serverPolicySnapshot.current = savedSnapshot
        savedPolicySnapshot.current = savedSnapshot
        savedPolicyValue.current = policyToSave
        if (editGeneration.current === generationAtSave) setDirtyNumbers(new Set())
        setPendingApply(true)
        saved = true
      }
      setApplying(true)
      const result = await apiFetch<ApplyConfigResult>("/settings/apply", { method: "POST" })
      toast.success(result.message)
      setPendingApply(false)
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "保存并应用失败")
    } finally {
      if (saved) {
        acceptNextConfig.current = true
        config.refetch()
        refresh()
      }
      setSaving(false)
      setApplying(false)
    }
  }

  function discardPolicyChanges() {
    const current = savedPolicyValue.current ?? config.data?.config.notifications
    if (!current) return
    serverPolicySnapshot.current = JSON.stringify(current)
    setPolicy(current)
    setDirtyNumbers(new Set())
    setPolicyRevision((revision) => revision + 1)
    editGeneration.current += 1
  }

  return (
    <section className="space-y-4">
      <PageHeader icon={BellRing} title="通知设置" description="管理通知渠道、告警阈值和发送重试策略" />
      <div className="sticky top-0 z-30 flex flex-wrap items-center justify-between gap-3 rounded-lg border border-border bg-background/95 px-3 py-2.5 shadow-sm backdrop-blur">
        <span className="text-sm font-medium">告警规则与发送策略</span>
        <div className="flex min-w-0 flex-wrap items-center justify-end gap-2">
          <Badge role="status" aria-live="polite" variant={dirty ? "secondary" : "outline"} className={pendingApply && !dirty ? "border-amber-300 bg-amber-50 text-amber-700" : undefined}>
            {applying ? "正在应用" : saving ? "正在保存" : dirty ? "有未保存更改" : pendingApply ? "应用失败待重试" : "已应用"}
          </Badge>
          <Button type="button" size="sm" variant="outline" onClick={dirty ? discardPolicyChanges : config.refetch} disabled={saving || applying || config.loading || config.refreshing}>
            <RefreshCw className={config.loading || config.refreshing ? "size-3.5 animate-spin" : "size-3.5"} />
            {dirty ? "放弃更改" : "刷新策略"}
          </Button>
          <Button type="button" size="sm" onClick={() => void savePolicy()} disabled={!policy || (!dirty && !pendingApply) || saving || applying}>
            <SlidersHorizontal className="size-3.5" />
            {applying ? "应用中" : pendingApply && !dirty ? "重新应用" : "保存并应用"}
          </Button>
        </div>
      </div>

      {config.error ? <p className="rounded-lg border border-destructive/20 bg-destructive/5 px-4 py-3 text-sm text-destructive">{config.error}</p> : null}

      <div className="grid items-start gap-6 2xl:grid-cols-[minmax(0,1.1fr)_minmax(0,1fr)]">
      <div id="notification-channels" className="scroll-mt-16">
        <NotificationStatus />
      </div>

      {policy ? (
        <div key={policyRevision} className="overflow-hidden rounded-2xl border border-border bg-card">
          <PolicySection id="notification-rates" icon={<Coins className="size-4 text-primary" />} title="倍率变化" description="控制倍率变动事件的合并方式和最低提醒幅度。">
            <div className="notification-policy-grid grid gap-px bg-card min-[1800px]:grid-cols-2">
              <PolicySwitch label="合并倍率变化" description="同一次扫描中的多条倍率变化合并为一条通知。" checked={policy.batchRateChanges} onCheckedChange={(value) => updatePolicy("batchRateChanges", value)} />
              <PolicyNumberField label="最小涨跌幅" description="低于该百分比的倍率变化不发送通知。" suffix="%">
                <NumberDraftInput draftKey="minChangePct" value={policy.minChangePct} min={0} integer={false} onDraftDirtyChange={updateDraftState} onValueChange={(value) => updatePolicy("minChangePct", value)} />
              </PolicyNumberField>
            </div>
          </PolicySection>

          <PolicySection id="notification-balance" icon={<WalletCards className="size-4 text-primary" />} title="余额告警" description="抑制同一渠道在余额持续不足时产生的重复提醒。">
            <div className="notification-policy-grid grid gap-px bg-card min-[1800px]:grid-cols-2">
              <PolicyNumberField label="余额不足冷却" description="同一渠道两次余额不足告警之间的最短间隔。" suffix="分钟">
                <NumberDraftInput draftKey="balanceLowCooldownMinutes" value={policy.balanceLowCooldownMinutes} min={0} onDraftDirtyChange={updateDraftState} onValueChange={(value) => updatePolicy("balanceLowCooldownMinutes", value)} />
              </PolicyNumberField>

            </div>
          </PolicySection>

          <PolicySection id="notification-subscriptions" icon={<CalendarClock className="size-4 text-primary" />} title="订阅告警" description="分别设置日、周、月剩余额度与到期时间阈值，填 0 表示关闭该项提醒。">
            <div className="notification-policy-grid grid gap-px bg-card min-[1800px]:grid-cols-2">
              <PolicyNumberField label="每日剩余阈值" description="每日剩余额度低于该值时提醒。" suffix="%"><NumberDraftInput draftKey="subscriptionDailyRemainingThresholdPct" value={policy.subscriptionDailyRemainingThresholdPct} min={0} integer={false} onDraftDirtyChange={updateDraftState} onValueChange={(value) => updatePolicy("subscriptionDailyRemainingThresholdPct", value)} /></PolicyNumberField>
              <PolicyNumberField label="每周剩余阈值" description="每周剩余额度低于该值时提醒。" suffix="%"><NumberDraftInput draftKey="subscriptionWeeklyRemainingThresholdPct" value={policy.subscriptionWeeklyRemainingThresholdPct} min={0} integer={false} onDraftDirtyChange={updateDraftState} onValueChange={(value) => updatePolicy("subscriptionWeeklyRemainingThresholdPct", value)} /></PolicyNumberField>
              <PolicyNumberField label="每月剩余阈值" description="每月剩余额度低于该值时提醒。" suffix="%"><NumberDraftInput draftKey="subscriptionMonthlyRemainingThresholdPct" value={policy.subscriptionMonthlyRemainingThresholdPct} min={0} integer={false} onDraftDirtyChange={updateDraftState} onValueChange={(value) => updatePolicy("subscriptionMonthlyRemainingThresholdPct", value)} /></PolicyNumberField>
              <PolicyNumberField label="到期提醒阈值" description="订阅剩余时间低于该值时提醒。" suffix="小时"><NumberDraftInput draftKey="subscriptionExpiryThresholdHours" value={policy.subscriptionExpiryThresholdHours} min={0} onDraftDirtyChange={updateDraftState} onValueChange={(value) => updatePolicy("subscriptionExpiryThresholdHours", value)} /></PolicyNumberField>
              <PolicyNumberField label="订阅提醒冷却" description="同一渠道同一类订阅提醒的最短间隔。" suffix="分钟"><NumberDraftInput draftKey="subscriptionAlertCooldownMinutes" value={policy.subscriptionAlertCooldownMinutes} min={0} onDraftDirtyChange={updateDraftState} onValueChange={(value) => updatePolicy("subscriptionAlertCooldownMinutes", value)} /></PolicyNumberField>
            </div>
          </PolicySection>

          <PolicySection id="notification-delivery" icon={<Send className="size-4 text-primary" />} title="发送与重试" description="控制单次通知送达失败后的最大尝试次数。">
            <div className="notification-policy-grid grid gap-px bg-card min-[1800px]:grid-cols-2">
              <PolicyNumberField label="最大尝试次数" description="包含首次发送；建议保留至少一次尝试。" suffix="次"><NumberDraftInput draftKey="sendMaxAttempts" value={policy.sendMaxAttempts} min={1} onDraftDirtyChange={updateDraftState} onValueChange={(value) => updatePolicy("sendMaxAttempts", value)} /></PolicyNumberField>

            </div>
          </PolicySection>
        </div>
      ) : (
        <p className="rounded-lg border border-border px-4 py-6 text-sm text-muted-foreground">{"加载通知策略中"}</p>
      )}
      </div>
    </section>
  )
}

function PolicySection({ id, icon, title, description, children }: { id: string; icon: ReactNode; title: string; description: string; children: ReactNode }) {
  return (
    <section id={id} className="scroll-mt-16 overflow-hidden border-b border-border last:border-b-0">
      <div className="flex items-center gap-3 border-b border-border bg-card px-5 py-4" title={description}>
        <span className="flex shrink-0 items-center justify-center">{icon}</span>
        <h2 className="min-w-0 text-lg font-semibold tracking-tight text-foreground">{title}</h2>
      </div>
      {children}
    </section>
  )
}

function PolicySwitch({ label, description, checked, onCheckedChange }: { label: string; description: string; checked: boolean; onCheckedChange: (value: boolean) => void }) {
  return (
    <div className="flex min-h-16 items-center justify-between gap-4 bg-card px-4 py-3">
      <div className="space-y-1">
        <div className="flex items-center gap-1.5"><span className="text-sm font-medium text-foreground">{label}</span><HelpHint label={label}>{description}</HelpHint></div>
      </div>
      <Switch aria-label={label} checked={checked} onCheckedChange={onCheckedChange} />
    </div>
  )
}

function PolicyNumberField({ label, description, suffix, children }: { label: string; description: string; suffix: string; children: ReactNode }) {
  return (
    <div className="grid min-h-16 grid-cols-[minmax(0,1fr)_9rem] items-center gap-4 bg-card px-4 py-3">
      <span className="min-w-0">
        <span className="flex items-center gap-1.5 text-sm font-medium text-foreground">{label}<HelpHint label={label}>{description}</HelpHint></span>
      </span>
      <label className="flex items-center gap-2">
        <span className="sr-only">{label}</span>
        <span className="min-w-0 flex-1">{children}</span>
        <span className="w-8 shrink-0 text-[11px] text-muted-foreground">{suffix}</span>
      </label>
    </div>
  )
}
