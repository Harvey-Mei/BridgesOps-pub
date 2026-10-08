import { useCallback, useEffect, useMemo, useRef, useState, type FormEvent, type ReactNode } from "react"
import { AlertTriangle, Calculator, RefreshCw } from "lucide-react"
import { toast } from "sonner"
import { apiFetch } from "@/lib/api"
import type { Channel, ChannelCostProfile, ChannelFundingCandidate } from "@/lib/api-types"
import { dateTime, decimal, getDisplayTimeZone, money, zonedDateTimeToISOString } from "@/lib/format"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Dialog, DialogBody, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Textarea } from "@/components/ui/textarea"
import { useTriggerRefresh } from "@/lib/refresh-context"
import { ChannelCostUnitRebasePanel } from "@/components/monitor/channel-cost-unit-rebase-panel"
import { ChannelCostBasisOverridePanel } from "@/components/monitor/channel-cost-basis-override-panel"
import { manualFundingDraft } from "@/lib/channel-accounting"

export function ChannelCostDialog({ channel, open, onOpenChange }: { channel: Channel | null; open: boolean; onOpenChange: (open: boolean) => void }) {
  return open && channel ? <ChannelCostDialogSession key={channel.id} channel={channel} open onOpenChange={onOpenChange} /> : null
}

function ChannelCostDialogSession({ channel, open, onOpenChange }: { channel: Channel; open: boolean; onOpenChange: (open: boolean) => void }) {
  const [profile, setProfile] = useState<ChannelCostProfile | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)
  const [childSaving, setChildSaving] = useState(false)
  const busy = saving || childSaving
  const busyRef = useRef(false)
  const loadingRef = useRef(false)
  const [tab, setTab] = useState("funding")
  const [historyPage, setHistoryPage] = useState(1)
  const [unconfirmedFunding, setUnconfirmedFunding] = useState(false)
  const loadGeneration = useRef(0)
  const activeChannel = useRef<number | null>(null)
  activeChannel.current = open ? channel?.id ?? null : null
  const [paid, setPaid] = useState("")
  const [credited, setCredited] = useState("")
  const [balanceIncludesCredit, setBalanceIncludesCredit] = useState(true)
  const [note, setNote] = useState("")
  const [effectiveAt, setEffectiveAt] = useState("")
  const [candidatePaid, setCandidatePaid] = useState<Record<number, string>>({})
  const refresh = useTriggerRefresh()

  const loadProfile = useCallback(async (channelID: number | undefined, page = 1) => {
    if (!channelID || activeChannel.current !== channelID) return false
    const generation = ++loadGeneration.current
    setLoading(true)
    loadingRef.current = true
    setError(null)
    try {
      const next = await apiFetch<ChannelCostProfile>(`/channels/${channelID}/cost-profile?page_size=20&page=${page}`)
      if (generation !== loadGeneration.current || activeChannel.current !== channelID) return false
      setProfile(next)
      setHistoryPage(page)
      return true
    } catch (cause) {
      if (generation === loadGeneration.current && activeChannel.current === channelID) setError(cause instanceof Error ? cause.message : "读取充值成本失败")
      return false
    } finally {
      if (generation === loadGeneration.current) { setLoading(false); loadingRef.current = false }
    }
  }, [])

  useEffect(() => {
    activeChannel.current = open ? channel?.id ?? null : null
    loadGeneration.current += 1
    setProfile(null)
    if (!open || !channel?.id) return
    setPaid("")
    setCredited("")
    setNote("")
    setEffectiveAt("")
    setBalanceIncludesCredit(true)
    setCandidatePaid({})
    setTab("funding")
    setHistoryPage(1)
    setUnconfirmedFunding(false)
    void loadProfile(channel.id)
    return () => { loadGeneration.current += 1; activeChannel.current = null }
  }, [channel?.id, loadProfile, open])

  const preview = useMemo(() => {
    return manualFundingDraft(profile?.state ?? null, paid, credited, balanceIncludesCredit, Boolean(effectiveAt))
  }, [balanceIncludesCredit, credited, paid, profile, effectiveAt])
  const blocked = busy || loading || Boolean(error)
  const pendingCount = Math.max(profile?.pending_total ?? 0, profile?.pending.length ?? 0)

  function mutationFailed(cause: unknown, manual: boolean) {
    const message = cause instanceof Error ? cause.message : "保存失败"
    const status = (cause as { status?: number })?.status
    if (!status || status >= 500) {
      setError("提交结果尚未确认，请重新加载并核对充值历史，避免重复入账。")
      if (manual) { setUnconfirmedFunding(true); setTab("history") }
    } else if (status === 409) {
      setError(`${message}；请重新加载最新状态。`)
    }
    toast.error(message)
  }

  async function afterChildApplied() {
    const loaded = await loadProfile(channel?.id)
    refresh()
    if (!loaded) throw new Error("操作已保存，但最新状态读取失败；请重新加载后继续，勿重复提交。")
  }

  function childBusy(value: boolean) { busyRef.current = value; setChildSaving(value) }
  function childConflict() { setError("成本状态已更新，请重新加载后核对，再继续校准。") }

  async function submitFunding(event: FormEvent) {
    event.preventDefault()
    if (busyRef.current || loadingRef.current || blocked || unconfirmedFunding || pendingCount > 0 || !channel || !profile || profile.state.channel_id !== channel.id || !preview.valid) return
    let effectiveISO: string | undefined
    try {
      effectiveISO = effectiveAt ? zonedDateTimeToISOString(effectiveAt, getDisplayTimeZone()) : undefined
      if (effectiveISO && Date.parse(effectiveISO) > Date.now()) { toast.error("实际充值时间不能晚于当前时间"); return }
    } catch { toast.error("实际充值时间无效，请重新选择"); return }
    busyRef.current = true
    const channelID = channel.id
    setSaving(true)
    try {
      const next = await apiFetch<ChannelCostProfile>(`/channels/${channelID}/funding-events`, {
        method: "POST",
        body: JSON.stringify({
          paid_amount: Number(paid), credited_amount: Number(credited),
          balance_includes_credit: balanceIncludesCredit,
          expected_version: profile.state.version, note: note.trim(),
          effective_at: effectiveISO,
        }),
      })
      if (activeChannel.current !== channelID) return
      loadGeneration.current += 1
      setProfile(next)
      setHistoryPage(1)
      setPaid("")
      setCredited("")
      setNote("")
      setEffectiveAt("")
      refresh()
      toast.success("充值成本已计入当前余额")
    } catch (cause) {
      if (activeChannel.current === channelID) mutationFailed(cause, true)
    } finally {
      setSaving(false)
      busyRef.current = false
    }
  }

  async function confirmCandidate(candidate: ChannelFundingCandidate) {
    if (busyRef.current || loadingRef.current || blocked || !channel || !profile || profile.state.channel_id !== channel.id || candidate.id !== profile.pending[0]?.id) return
    const paidText = String(candidatePaid[candidate.id] ?? candidate.paid_amount ?? "").trim()
    if (!paidText) {
      toast.error("请输入本次实际付款金额")
      return
    }
    const paidValue = Number(paidText)
    if (!Number.isFinite(paidValue) || paidValue < 0) {
      toast.error("请输入本次实际付款金额")
      return
    }
    busyRef.current = true
    const channelID = channel.id
    setSaving(true)
    try {
      const next = await apiFetch<ChannelCostProfile>(`/channels/${channelID}/funding-candidates/${candidate.id}/confirm`, {
        method: "POST",
        body: JSON.stringify({ paid_amount: paidValue, expected_version: profile.state.version, note: candidate.note ?? "" }),
      })
      if (activeChannel.current !== channelID) return
      loadGeneration.current += 1
      setProfile(next)
      setHistoryPage(1)
      setCandidatePaid(current => { const next = {...current}; delete next[candidate.id]; return next })
      refresh()
      toast.success(next.candidate_resolution === "superseded" ? "该旧提醒已归档，未重复计入充值" : "待确认充值已入账")
    } catch (cause) {
      if (activeChannel.current === channelID) mutationFailed(cause, false)
    } finally {
      setSaving(false)
      busyRef.current = false
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange} busy={busy}>
      <DialogContent formLayout className="h-[calc(100dvh-2rem)] sm:h-[min(780px,calc(100dvh-3rem))] sm:max-w-5xl">
        <DialogHeader>
          <DialogTitle>{`${channel?.name ?? "渠道"} · 充值成本`}</DialogTitle>
          <DialogDescription>记录真实充值，按剩余余额加权计算成本系数。</DialogDescription>
        </DialogHeader>
        <DialogBody className="flex-1 space-y-4">
          {error ? <div role="alert" className="flex items-center justify-between gap-3 rounded-lg border border-destructive/20 bg-destructive/5 px-3 py-2 text-sm"><span className="text-destructive [overflow-wrap:anywhere]">{error}</span><Button size="sm" variant="outline" disabled={loading || busy} onClick={() => void loadProfile(channel?.id, historyPage)}>{loading ? "读取中…" : "重新加载"}</Button></div> : null}
          {loading && !profile ? <p className="py-8 text-center text-sm text-muted-foreground">读取成本状态中…</p> : null}
          {profile && profile.state.channel_id === channel?.id ? <>
            <div className="grid grid-cols-3 gap-px overflow-hidden rounded-lg border bg-border">
              <CostMetric label="上游余额" value={decimal(profile.state.raw_balance, 4)} hint={profile.state.balance_sampled_at ? dateTime(profile.state.balance_sampled_at) : "尚未采集"} />
              <CostMetric label="折合资金余额" value={money(profile.state.actual_inventory, { precise: true })} hint="上游余额 × 成本系数" />
              <CostMetric label="成本系数" value={`× ${decimal(profile.state.funding_factor, 6)}`} hint={costSourceLabel(profile.state.source)} />
            </div>
            {profile.state.status !== "ready" ? <div className="flex items-center justify-between gap-3 rounded-lg border border-warning/25 bg-warning/5 px-3 py-2 text-sm"><span className="text-warning">{profile.state.status === "error" ? "账务状态异常，请先核对成本依据。" : "成本口径待复核；记录充值不会自动解除该状态。"}</span><Button variant="outline" size="sm" disabled={busy} onClick={() => setTab("adjust")}>查看校准</Button></div> : null}
            <Tabs value={tab} onValueChange={value => { if (!busyRef.current) setTab(value) }} className="space-y-3">
            <div className="flex items-center justify-between gap-3">
                <TabsList>
                  <TabsTrigger value="funding" disabled={busy}>充值处理{pendingCount > 0 ? ` · ${pendingCount}` : ""}</TabsTrigger>
                  <TabsTrigger value="adjust" disabled={busy}>成本校准</TabsTrigger>
                  <TabsTrigger value="history" disabled={busy}>充值历史</TabsTrigger>
                  <TabsTrigger value="rates" disabled={busy}>分组倍率</TabsTrigger>
                </TabsList>
              <Button size="icon-sm" variant="outline" disabled={loading || busy} onClick={() => void loadProfile(channel?.id, historyPage)} aria-label="刷新充值成本"><RefreshCw className={loading ? "size-4 animate-spin" : "size-4"} /></Button>
            </div>
            {unconfirmedFunding ? <div className="flex items-center justify-between gap-3 rounded-lg border border-warning/25 bg-warning/5 p-3 text-sm"><span>上次提交结果未确认。请先核对历史，确认未入账后再重新填写。</span><Button size="sm" variant="outline" disabled={blocked} onClick={() => { setUnconfirmedFunding(false); setPaid(""); setCredited(""); setNote(""); setEffectiveAt(""); setTab("funding") }}>重新填写</Button></div> : null}
            <TabsContent value="funding" forceMount className="space-y-3 data-[state=inactive]:hidden">
              {pendingCount > 0 ? <section className="overflow-hidden rounded-lg border">
                <SectionTitle icon={<AlertTriangle className="size-4 text-warning" />} title={`待确认充值 · ${pendingCount}`} description="按实际发生时间从早到晚处理，确认后自动计算成本系数。" />
                <p className="border-b bg-muted/10 px-4 py-2 text-sm text-muted-foreground">先确认最早一条，只需填写实际付款；处理完后才能手工新增充值。</p>
                <div className="max-h-72 divide-y overflow-y-auto">{profile.pending.map((item, index) => <div key={item.id} className="grid grid-cols-[minmax(0,1fr)_10rem_6rem] items-center gap-3 px-4 py-3">
                  <div className="min-w-0"><div className="flex flex-wrap items-center gap-2 text-sm font-medium"><span>{fundingSourceLabel(item.source)}</span><Badge variant="outline">{`到账 ${decimal(item.credited_amount, 4)}`}</Badge></div><p className="mt-1 text-xs text-muted-foreground" title={`充值前余额 ${decimal(item.raw_balance_before, 4)} · ${item.external_id}`}>{dateTime(item.occurred_at || item.detected_at)}</p></div>
                  <Input aria-label={`充值 #${item.id} 实际付款`} type="number" min="0" step="any" value={candidatePaid[item.id] ?? item.paid_amount ?? ""} onChange={event => setCandidatePaid(current => ({...current, [item.id]: event.target.value}))} placeholder="实际付款" disabled={blocked} />
                  <Button size="sm" disabled={blocked || index !== 0} onClick={() => void confirmCandidate(item)}>{index === 0 ? saving ? "保存中…" : "确认入账" : "等待上一条"}</Button>
                </div>)}</div>
                {profile.pending.length === 0 ? <p className="p-4 text-sm text-muted-foreground">待确认列表暂不可用，请重新加载。</p> : profile.pending.length < pendingCount ? <p className="border-t px-4 py-2 text-xs text-muted-foreground">{`先处理最早 ${profile.pending.length} 条，后续记录随确认自动载入。`}</p> : null}
              </section> : <section className="overflow-hidden rounded-lg border">
                <SectionTitle icon={<Calculator className="size-4 text-primary" />} title="记录一笔充值" description="仅记录已发生的真实资金充值，不用于直接修改成本系数。" />
                <form onSubmit={submitFunding} className="space-y-4 p-4">
                  <div className="grid grid-cols-3 items-end gap-4">
                    <label className="grid gap-2"><Label htmlFor="funding-paid">实际付款</Label><Input id="funding-paid" type="number" min="0" step="any" value={paid} onChange={event => setPaid(event.target.value)} required disabled={blocked || unconfirmedFunding} /></label>
                    <label className="grid gap-2"><Label htmlFor="funding-credit">上游到账</Label><Input id="funding-credit" type="number" min="0" step="any" value={credited} onChange={event => setCredited(event.target.value)} required disabled={blocked || unconfirmedFunding} /></label>
                    <label className="grid gap-2 text-sm font-medium">余额状态<Select value={balanceIncludesCredit ? "included" : "not_included"} onValueChange={value => setBalanceIncludesCredit(value === "included")} disabled={blocked || unconfirmedFunding}><SelectTrigger aria-label="到账是否已计入余额" className="w-full"><SelectValue /></SelectTrigger><SelectContent><SelectItem value="included">当前余额已包含到账</SelectItem><SelectItem value="not_included">到账尚未计入当前余额</SelectItem></SelectContent></Select></label>
                  </div>
                  <details className="rounded-lg border bg-muted/10"><summary className="cursor-pointer px-3 py-2 text-sm">补充信息 · 备注 / 历史补录</summary><div className="grid grid-cols-[minmax(0,1fr)_16rem] gap-4 border-t p-3"><label className="grid gap-2"><Label htmlFor="funding-note">备注</Label><Textarea id="funding-note" rows={2} maxLength={1024} value={note} onChange={event => setNote(event.target.value)} disabled={blocked || unconfirmedFunding} /></label><label className="grid content-start gap-2"><Label htmlFor="funding-effective-at">实际充值时间</Label><Input id="funding-effective-at" type="datetime-local" value={effectiveAt} onChange={event => setEffectiveAt(event.target.value)} disabled={blocked || unconfirmedFunding} /><span className="text-xs text-muted-foreground">留空按当前时间，历史补录依赖当时快照。</span></label></div></details>
                  <div className="flex items-center justify-between gap-4 border-t pt-3"><div className="min-w-0 text-sm text-muted-foreground">{preview.error ? <span role="alert" className="text-destructive">{preview.error}</span> : effectiveAt ? "历史补录由服务端按当时快照核算，不用当前余额预估。" : preview.preview ? `新系数 ×${decimal(preview.preview.factor, 6)} · 折合余额 ${money(preview.preview.afterActual, {precise:true})}` : "填写实际付款和到账金额，预览新成本系数。"}</div><Button type="submit" disabled={blocked || unconfirmedFunding || !preview.valid}>{saving ? "保存中…" : "确认记录充值"}</Button></div>
                </form>
              </section>}
            </TabsContent>
            <TabsContent value="adjust" forceMount className="space-y-3 data-[state=inactive]:hidden">
              <p className="text-sm text-muted-foreground">仅校准折算口径，不新增充值。若有待确认充值，手动覆盖会将其归档，提交前请核对影响范围。</p>
              <ChannelCostUnitRebasePanel channel={channel} profile={profile} disabled={blocked} onSubmittingChange={childBusy} onApplied={afterChildApplied} onConflict={childConflict} />
              <ChannelCostBasisOverridePanel channel={channel} profile={profile} disabled={blocked} onSubmittingChange={childBusy} onApplied={afterChildApplied} onConflict={childConflict} />
            </TabsContent>
            <TabsContent value="rates" forceMount className="overflow-hidden rounded-lg border data-[state=inactive]:hidden">
              <div className="grid grid-cols-3 border-b bg-muted/20 px-4 py-2 text-sm font-medium"><span>分组</span><span className="text-right">上游倍率</span><span className="text-right">实际倍率</span></div>
              <div className="max-h-80 divide-y overflow-y-auto">{profile.rates.map(rate => <div key={rate.group_name} className="grid grid-cols-3 px-4 py-2 text-sm"><span className="truncate" title={rate.group_name}>{rate.group_name}</span><span className="text-right tabular-nums">{decimal(rate.raw_ratio, 4)}</span><span className="text-right font-medium tabular-nums">{decimal(rate.effective_ratio, 6)}</span></div>)}</div>
              {!profile.rates.length ? <p className="p-6 text-center text-sm text-muted-foreground">暂无分组倍率。</p> : null}
            </TabsContent>
            <TabsContent value="history" forceMount className="overflow-hidden rounded-lg border data-[state=inactive]:hidden">
              <SectionTitle title="充值历史" description="已确认记录不可修改，校准追加独立审计，不伪造充值。" />
              <div className="max-h-80 divide-y overflow-y-auto">{profile.events.map(item => <div key={item.id} className="grid grid-cols-[8rem_minmax(0,1fr)_auto] items-center gap-3 px-4 py-2.5 text-sm"><span className="text-xs text-muted-foreground">{dateTime(item.effective_at)}</span><span className="min-w-0"><span>{`付款 ${money(item.paid_amount, {precise:true})} · 到账 ${decimal(item.credited_amount, 4)}`}</span>{item.note ? <span className="mt-0.5 block break-words text-xs text-muted-foreground">{item.note}</span> : null}</span><span className="tabular-nums" title={`×${item.funding_factor_before} → ×${item.funding_factor_after}`}>{`×${decimal(item.funding_factor_before, 6)} → ×${decimal(item.funding_factor_after, 6)}`}</span></div>)}</div>
              {!profile.events.length ? <p className="p-6 text-center text-sm text-muted-foreground">暂无已确认充值。</p> : null}
              <div className="flex items-center justify-between border-t px-4 py-2 text-xs text-muted-foreground"><span>{`第 ${historyPage} 页 · 共 ${profile.total} 条`}</span><div className="flex gap-2"><Button size="sm" variant="outline" disabled={blocked || historyPage <= 1} onClick={() => void loadProfile(channel.id, historyPage - 1)}>上一页</Button><Button size="sm" variant="outline" disabled={blocked || historyPage * 20 >= profile.total} onClick={() => void loadProfile(channel.id, historyPage + 1)}>下一页</Button></div></div>
            </TabsContent>
            </Tabs>
          </> : null}
        </DialogBody>
      </DialogContent>
    </Dialog>
  )
}

function SectionTitle({ icon, title, description }: { icon?: ReactNode; title: string; description: string }) {
  return <div className="flex items-center gap-2 border-b bg-muted/20 px-4 py-3" title={description}>{icon}<h3 className="text-sm font-semibold">{title}</h3></div>
}

function CostMetric({ label, value, hint, warning = false }: { label: string; value: string; hint: string; warning?: boolean }) {
  return <div className="min-w-0 bg-background px-3 py-3"><div className="text-[11px] text-muted-foreground">{label}</div><div className={warning ? "mt-1 text-center text-sm font-semibold text-amber-600" : "mt-1 text-center text-sm font-semibold"}>{value}</div><div className="mt-1 truncate text-center text-[11px] text-muted-foreground" title={hint}>{hint}</div></div>
}

function costSourceLabel(source: string) {
  if (source.startsWith("confirmed_")) return "已确认充值"
  if (source === "manual_recharge") return "手工充值"
  if (source === "advertised") return "上游建议"
  if (source === "manual_override") return "手动覆盖"
  if (source.startsWith("legacy")) return "旧配置迁移"
  return source || "默认 1:1"
}

function fundingSourceLabel(source: string) {
  if (source === "balance_increase") return "检测到余额上涨"
  if (source === "redeem_code") return "兑换码到账"
  if (source === "newapi_topup") return "NewAPI 充值订单"
  if (source === "sub2api_payment") return "Sub2API 充值订单"
  return "待确认充值"
}
