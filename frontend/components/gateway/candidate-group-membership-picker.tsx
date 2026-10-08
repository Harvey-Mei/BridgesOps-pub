import { useEffect, useMemo, useState, type ReactNode } from "react"
import { Check, ChevronDown, ChevronRight, ChevronsUpDown, Minus, Plus, Power, Search } from "lucide-react"
import type { GatewayRouteCandidate } from "@/lib/api-types"
import {
  gatewayRoutingEnableBlockedReason,
  gatewayRoutingEnableEligible,
  type GatewayRoutingEnableCandidate,
} from "@/lib/gateway-routing-enable"
import { candidateAvailabilityLabel } from "@/lib/route-candidate-availability"
import {
  buildCandidateGroupMembershipRows,
  candidateGroupMemberKey,
  candidateGroupMembershipBulkTargets,
  candidateGroupMembershipChannelOptions,
  candidateGroupMembershipFilterAll,
  candidateGroupMembershipProtocolOptions,
  candidateGroupMembershipRowState,
  filterCandidateGroupMembershipRows,
  groupCandidateGroupMembershipRows,
  type CandidateGroupMembershipBulkOperation,
  type CandidateGroupMembershipCandidate,
  type CandidateGroupMembershipFilters,
  type CandidateGroupMembershipRow,
  type CandidateGroupMembershipStatusFilter,
} from "@/lib/candidate-group-membership"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command"
import { Input } from "@/components/ui/input"
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"

type PendingBulkAction = {
  operation: CandidateGroupMembershipBulkOperation
  rows: CandidateGroupMembershipRow[]
}

type CandidateGroupMembershipPickerProps = {
  candidates: GatewayRouteCandidate[]
  existingMembers: { channel_id: number; group_name: string }[]
  selected: ReadonlySet<string>
  loading: boolean
  disabled: boolean
  resetKey: number
  onSelectionChange: (next: Set<string>) => void
  onEnableRoutingGroup: (candidate: GatewayRoutingEnableCandidate) => void
  isRoutingGroupEnableSaving: (candidate: GatewayRoutingEnableCandidate) => boolean
}

const emptyFilters: CandidateGroupMembershipFilters = {
  search: "",
  status: candidateGroupMembershipFilterAll,
  protocol: candidateGroupMembershipFilterAll,
  channelID: candidateGroupMembershipFilterAll,
}

export function CandidateGroupMembershipPicker({
  candidates,
  existingMembers,
  selected,
  loading,
  disabled,
  resetKey,
  onSelectionChange,
  onEnableRoutingGroup,
  isRoutingGroupEnableSaving,
}: CandidateGroupMembershipPickerProps) {
  const [filters, setFilters] = useState<CandidateGroupMembershipFilters>(emptyFilters)
  const [expandedChannelIDs, setExpandedChannelIDs] = useState<Set<number>>(() => new Set())
  const [pendingBulkAction, setPendingBulkAction] = useState<PendingBulkAction | null>(null)
  const membershipCandidates = useMemo<CandidateGroupMembershipCandidate[]>(() => {
    const candidateKeys = new Set(candidates.map((candidate) => candidateGroupMemberKey(candidate.channel_id, candidate.group_name)))
    const channelFacts = new Map<number, { channelName: string; channelType: string }>()
    for (const candidate of candidates) {
      if (!channelFacts.has(candidate.channel_id)) channelFacts.set(candidate.channel_id, {
        channelName: candidate.channel_name,
        channelType: candidate.channel_type,
      })
    }
    const missingMembers = existingMembers
      .filter((member) => !candidateKeys.has(candidateGroupMemberKey(member.channel_id, member.group_name)))
      .map((member) => {
        const channel = channelFacts.get(member.channel_id)
        return {
          channel_id: member.channel_id,
          channel_name: channel?.channelName || `渠道 ${member.channel_id}`,
          channel_type: channel?.channelType || "",
          group_name: member.group_name,
          gateway_app: "",
          routing_available: false,
          membership_snapshot_missing: true,
        }
      })
    return [...candidates, ...missingMembers]
  }, [candidates, existingMembers])
  const rows = useMemo(() => buildCandidateGroupMembershipRows(membershipCandidates), [membershipCandidates])
  const visibleRows = useMemo(() => filterCandidateGroupMembershipRows(rows, filters), [filters, rows])
  const visibleChannels = useMemo(() => groupCandidateGroupMembershipRows(visibleRows), [visibleRows])
  const protocols = useMemo(() => candidateGroupMembershipProtocolOptions(rows), [rows])
  const channels = useMemo(() => candidateGroupMembershipChannelOptions(rows), [rows])
  const addTargets = useMemo(() => candidateGroupMembershipBulkTargets(visibleRows, selected, "add"), [selected, visibleRows])
  const removeTargets = useMemo(() => candidateGroupMembershipBulkTargets(visibleRows, selected, "remove"), [selected, visibleRows])
  const visibleSelectedCount = useMemo(() => visibleRows.filter((row) => selected.has(row.key)).length, [selected, visibleRows])

  useEffect(() => {
    setFilters(emptyFilters)
    setExpandedChannelIDs(new Set())
    setPendingBulkAction(null)
  }, [resetKey])

  useEffect(() => {
    setFilters((current) => {
      const protocolValid = current.protocol === candidateGroupMembershipFilterAll || protocols.includes(current.protocol)
      const channelValid = current.channelID === candidateGroupMembershipFilterAll || channels.some((channel) => channel.value === current.channelID)
      if (protocolValid && channelValid) return current
      return {
        ...current,
        protocol: protocolValid ? current.protocol : candidateGroupMembershipFilterAll,
        channelID: channelValid ? current.channelID : candidateGroupMembershipFilterAll,
      }
    })
  }, [channels, protocols])

  function updateFilter<K extends keyof CandidateGroupMembershipFilters>(key: K, value: CandidateGroupMembershipFilters[K]) {
    setFilters((current) => ({ ...current, [key]: value }))
  }

  function toggleRow(row: CandidateGroupMembershipRow, checked: boolean) {
    const next = new Set(selected)
    if (checked) next.add(row.key)
    else next.delete(row.key)
    onSelectionChange(next)
  }

  function toggleChannel(channelID: number) {
    setExpandedChannelIDs((current) => {
      const next = new Set(current)
      if (next.has(channelID)) next.delete(channelID)
      else next.add(channelID)
      return next
    })
  }

  function requestBulkAction(operation: CandidateGroupMembershipBulkOperation) {
    const targets = operation === "add" ? addTargets : removeTargets
    if (targets.length === 0) return
    setPendingBulkAction({ operation, rows: targets })
  }

  function confirmBulkAction() {
    if (!pendingBulkAction) return
    const next = new Set(selected)
    for (const row of pendingBulkAction.rows) {
      if (pendingBulkAction.operation === "add") next.add(row.key)
      else next.delete(row.key)
    }
    onSelectionChange(next)
    setPendingBulkAction(null)
  }

  return (
    <>
      <section className="mt-4 flex min-h-0 flex-1 flex-col overflow-hidden rounded-md border border-border" aria-label="候选渠道选择">
        <div className="space-y-3 border-b border-border bg-muted/15 p-3">
          <div className="grid gap-2 md:grid-cols-[minmax(12rem,1fr)_10rem_10rem]">
            <label className="relative block">
              <span className="sr-only">搜索候选</span>
              <Search className="pointer-events-none absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" aria-hidden="true" />
              <Input value={filters.search} onChange={(event) => updateFilter("search", event.target.value)} placeholder="搜索渠道、上游分组或协议" aria-label="搜索候选" className="bg-background pl-8" disabled={disabled} />
            </label>
            <FilterSelect label="筛选状态" value={filters.status} disabled={disabled} onValueChange={(value) => updateFilter("status", value as CandidateGroupMembershipStatusFilter)}>
              <SelectItem value={candidateGroupMembershipFilterAll}>{"全部状态"}</SelectItem>
              <SelectItem value="routing_available">{"智能可路由"}</SelectItem>
              <SelectItem value="cooling_down">{"冷却中"}</SelectItem>
              <SelectItem value="routing_unavailable">{"智能受限"}</SelectItem>
            </FilterSelect>
            <FilterSelect label="筛选协议" value={filters.protocol} disabled={disabled} onValueChange={(value) => updateFilter("protocol", value)}>
              <SelectItem value={candidateGroupMembershipFilterAll}>{"全部协议"}</SelectItem>
              {protocols.map((protocol) => <SelectItem key={protocol} value={protocol}>{protocol}</SelectItem>)}
            </FilterSelect>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <ChannelFilter value={filters.channelID} channels={channels} disabled={disabled} onValueChange={(value) => updateFilter("channelID", value)} />
            <Badge variant="secondary" className="h-9 px-3 tabular-nums">{`已选 ${selected.size}`}</Badge>
            <span className="text-xs text-muted-foreground tabular-nums">{`当前筛选 ${visibleRows.length} · 已选 ${visibleSelectedCount}`}</span>
            <div className="ml-auto flex flex-wrap items-center gap-2">
              <Button type="button" size="sm" variant="outline" disabled={disabled || removeTargets.length === 0} onClick={() => requestBulkAction("remove")} aria-label={`从当前筛选结果移除 ${removeTargets.length} 个候选`}>
                <Minus className="size-3.5" aria-hidden="true" />
                {`移除筛选结果 ${removeTargets.length}`}
              </Button>
              <Button type="button" size="sm" disabled={disabled || addTargets.length === 0} onClick={() => requestBulkAction("add")} aria-label={`将当前筛选结果加入 ${addTargets.length} 个候选`}>
                <Plus className="size-3.5" aria-hidden="true" />
                {`加入筛选结果 ${addTargets.length}`}
              </Button>
            </div>
          </div>
        </div>

        <p className="sr-only" aria-live="polite">{`当前筛选显示 ${visibleRows.length} 个候选，已选择 ${visibleSelectedCount} 个。`}</p>
        <div className="min-h-0 flex-1 overflow-y-auto" role="list" aria-label="按渠道分组的候选渠道" aria-busy={loading}>
          {visibleChannels.map((channel) => {
            const collapsed = !expandedChannelIDs.has(channel.channelID)
            const channelSelectedCount = channel.rows.filter((row) => selected.has(row.key)).length
            const panelID = `candidate-membership-channel-${channel.channelID}`
            return (
              <section key={channel.channelID} className="border-b border-border last:border-b-0" role="listitem" aria-labelledby={`${panelID}-title`}>
                <div className="flex min-w-0 items-center gap-2 bg-muted/20 px-3 py-2.5">
                  <button type="button" className="flex min-w-0 flex-1 items-center gap-2 text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring" onClick={() => toggleChannel(channel.channelID)} aria-expanded={!collapsed} aria-controls={panelID} aria-label={`${collapsed ? "展开" : "折叠"}渠道 ${channel.channelName}`}>
                    {collapsed ? <ChevronRight className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" /> : <ChevronDown className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />}
                    <span id={`${panelID}-title`} className="truncate text-sm font-semibold" title={channel.channelName}>{channel.channelName}</span>
                    {channel.channelType ? <Badge variant="outline" className="h-5 shrink-0 px-1.5 text-[10px]">{channel.channelType}</Badge> : null}
                  </button>
                  <Badge variant="secondary" className="h-6 shrink-0 px-2 text-[11px] tabular-nums">{channelSelectedCount > 0 ? `已选 ${channelSelectedCount} / ${channel.rows.length}` : `${channel.rows.length} 个分组`}</Badge>
                </div>
                <div id={panelID} hidden={collapsed}>
                  {!collapsed ? channel.rows.map((row) => <CandidateMembershipRow key={row.key} row={row} protocol={filters.protocol} checked={selected.has(row.key)} disabled={disabled} onCheckedChange={(checked) => toggleRow(row, checked)} onEnableRoutingGroup={onEnableRoutingGroup} isRoutingGroupEnableSaving={isRoutingGroupEnableSaving} />) : null}
                </div>
              </section>
            )
          })}
          {visibleRows.length === 0 ? <div className="flex min-h-48 items-center justify-center px-4 text-center text-sm text-muted-foreground">{loading ? "正在加载候选渠道" : "没有匹配的候选渠道"}</div> : null}
        </div>
      </section>

      <AlertDialog open={pendingBulkAction !== null} onOpenChange={(next) => { if (!next) setPendingBulkAction(null) }}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{pendingBulkAction?.operation === "add" ? "确认加入筛选结果" : "确认移除筛选结果"}</AlertDialogTitle>
            <AlertDialogDescription>
              {pendingBulkAction?.operation === "add"
                ? `将把 ${pendingBulkAction.rows.length} 个候选加入当前编辑选择，保存分组后生效。`
                : `将从当前编辑选择移除 ${pendingBulkAction?.rows.length ?? 0} 个候选，保存分组后生效。`}
            </AlertDialogDescription>
          </AlertDialogHeader>
          {pendingBulkAction ? <BulkPreview rows={pendingBulkAction.rows} /> : null}
          <AlertDialogFooter>
            <AlertDialogCancel>{"取消"}</AlertDialogCancel>
            <AlertDialogAction className={pendingBulkAction?.operation === "remove" ? "bg-destructive text-white hover:bg-destructive/90" : undefined} onClick={confirmBulkAction}>
              {pendingBulkAction?.operation === "add" ? "确认加入" : "确认移除"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  )
}

function FilterSelect({ label, value, disabled, onValueChange, children }: {
  label: string
  value: string
  disabled: boolean
  onValueChange: (value: string) => void
  children: ReactNode
}) {
  return (
    <div className="min-w-0">
      <Select value={value} onValueChange={onValueChange} disabled={disabled}>
        <SelectTrigger aria-label={label} className="w-full bg-background"><SelectValue /></SelectTrigger>
        <SelectContent>{children}</SelectContent>
      </Select>
    </div>
  )
}

function ChannelFilter({ value, channels, disabled, onValueChange }: {
  value: string
  channels: ReturnType<typeof candidateGroupMembershipChannelOptions>
  disabled: boolean
  onValueChange: (value: string) => void
}) {
  const [open, setOpen] = useState(false)
  const selected = channels.find((channel) => channel.value === value)
  const label = selected ? selected.label : "全部渠道"

  function choose(next: string) {
    onValueChange(next)
    setOpen(false)
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button type="button" variant="outline" role="combobox" aria-expanded={open} aria-label="筛选渠道" disabled={disabled} className="h-9 min-w-48 max-w-full justify-between bg-background text-left font-normal">
          <span className="truncate">{label}</span>
          <ChevronsUpDown className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
        </Button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-[min(24rem,calc(100vw-2rem))] p-0">
        <Command>
          <CommandInput placeholder="搜索渠道" aria-label="搜索渠道筛选项" />
          <CommandList>
            <CommandEmpty>{"没有匹配的渠道"}</CommandEmpty>
            <CommandGroup heading="渠道">
              <CommandItem value="全部渠道" onSelect={() => choose(candidateGroupMembershipFilterAll)}>
                <Check className={value === candidateGroupMembershipFilterAll ? "size-3.5" : "invisible size-3.5"} aria-hidden="true" />
                <span>{"全部渠道"}</span>
              </CommandItem>
              {channels.map((channel) => (
                <CommandItem key={channel.value} value={`${channel.label} ${channel.channelType} ${channel.value}`} onSelect={() => choose(channel.value)}>
                  <Check className={value === channel.value ? "size-3.5" : "invisible size-3.5"} aria-hidden="true" />
                  <span className="min-w-0 flex-1 truncate">{channel.label}</span>
                  {channel.channelType ? <span className="shrink-0 text-xs text-muted-foreground">{channel.channelType}</span> : null}
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  )
}

function CandidateMembershipRow({ row, protocol, checked, disabled, onCheckedChange, onEnableRoutingGroup, isRoutingGroupEnableSaving }: {
  row: CandidateGroupMembershipRow
  protocol: string
  checked: boolean
  disabled: boolean
  onCheckedChange: (checked: boolean) => void
  onEnableRoutingGroup: (candidate: GatewayRoutingEnableCandidate) => void
  isRoutingGroupEnableSaving: (candidate: GatewayRoutingEnableCandidate) => boolean
}) {
  const state = candidateGroupMembershipRowState(row, protocol)
  const checkboxID = `candidate-membership-${encodeURIComponent(row.key)}`
  const visibleApps = protocol === candidateGroupMembershipFilterAll ? row.gatewayApps : row.gatewayApps.filter((app) => app === protocol)
  const protocolCandidates = protocol === candidateGroupMembershipFilterAll ? row.candidates : row.candidates.filter((candidate) => candidate.gateway_app === protocol)
  const routingEnableCandidate = protocolCandidates.find((candidate) => gatewayRoutingEnableEligible(candidate)) ?? protocolCandidates.find((candidate) => candidate.cooling_down === true)
  const routingEnableAllowed = routingEnableCandidate ? gatewayRoutingEnableEligible(routingEnableCandidate) : false
  const routingEnableSaving = routingEnableCandidate ? isRoutingGroupEnableSaving(routingEnableCandidate) : false
  const routingEnableDisabled = disabled || routingEnableSaving || !routingEnableAllowed
  const routingEnableTooltip = disabled
    ? "候选分组正在加载或保存"
    : routingEnableSaving
      ? "正在启用当前分组"
      : routingEnableAllowed
        ? "清除当前代次的路由健康阻断和自适应失败链"
        : gatewayRoutingEnableBlockedReason(routingEnableCandidate ?? {})
  return (
    <div className={`grid grid-cols-[2.25rem_minmax(0,1fr)] items-center gap-x-2 gap-y-2 border-t border-border px-3 py-3 text-sm hover:bg-muted/25 sm:grid-cols-[2.25rem_minmax(0,1fr)_12rem_16rem] ${disabled ? "opacity-70" : ""}`}>
      <span className="flex items-center justify-center"><Checkbox id={checkboxID} checked={checked} disabled={disabled} onCheckedChange={(next) => onCheckedChange(next === true)} aria-label={`${checked ? "取消选择" : "选择"} ${row.channelName} ${row.groupName}`} /></span>
      <label htmlFor={checkboxID} className={`min-w-0 ${disabled ? "cursor-not-allowed" : "cursor-pointer"}`}>
        <span className="block truncate font-medium" title={row.groupName}>{row.groupName}</span>
      </label>
      <span className="flex min-w-0 flex-wrap items-center gap-1" aria-label={visibleApps.length > 0 ? `协议：${visibleApps.join("、")}` : "协议：当前不可见"}>
        {visibleApps.length > 0 ? visibleApps.map((app) => <Badge key={app} variant="outline" className="h-5 px-1.5 text-[10px]">{app}</Badge>) : <Badge variant="outline" className="h-5 px-1.5 text-[10px]">{"-"}</Badge>}
      </span>
      <span className="flex min-w-0 flex-wrap items-center gap-1">
        <CandidateMembershipStatus state={state} />
        {routingEnableCandidate?.cooling_down ? (
          <Tooltip delayDuration={150}>
            <TooltipTrigger asChild>
              <span
                className="inline-flex rounded-md focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                tabIndex={routingEnableDisabled ? 0 : undefined}
                role={routingEnableDisabled ? "button" : undefined}
                aria-disabled={routingEnableDisabled ? true : undefined}
                aria-label={routingEnableDisabled ? `一键启用 ${row.channelName} / ${row.groupName} 路由不可用：${routingEnableTooltip}` : undefined}
              >
                <Button
                  type="button"
                  size="sm"
                  variant="outline"
                  className="h-6 px-2 text-[11px]"
                  disabled={routingEnableDisabled}
                  aria-busy={routingEnableSaving}
                  onClick={() => onEnableRoutingGroup(routingEnableCandidate)}
                  aria-label={`一键启用 ${row.channelName} / ${row.groupName} 路由${routingEnableAllowed ? "" : `不可用：${routingEnableTooltip}`}`}
                >
                  <Power className={routingEnableSaving ? "size-3 animate-pulse" : "size-3"} aria-hidden="true" />
                  {routingEnableSaving ? "启用中" : "一键启用"}
                </Button>
              </span>
            </TooltipTrigger>
            <TooltipContent side="top" className="max-w-72 text-xs">{routingEnableTooltip}</TooltipContent>
          </Tooltip>
        ) : null}
      </span>
    </div>
  )
}

function CandidateMembershipStatus({ state }: { state: ReturnType<typeof candidateGroupMembershipRowState> }) {
  const unavailableTitle = state.availabilityReasons.length > 0 ? `智能路由门禁：${state.availabilityReasons.map(candidateAvailabilityLabel).join("、")}` : "智能路由当前不可用"
  const partiallyUnavailable = state.routingAvailable && state.routingUnavailable
  if (state.snapshotMissing) return <MembershipStatusBadge label="当前不可见" detail="该既有成员不在当前候选快照中；保持选择不会被前端静默移除，保存时若上游事实已失效会明确报错" className="border-warning/30 bg-warning/5 text-warning" />
  return (
    <>
      {state.coolingDown ? <MembershipStatusBadge label="故障隔离" detail="智能路由将持续隔离，连续三次探测成功后才恢复；固定路由不受此状态限制" className="border-warning/35 bg-warning/10 text-warning" /> : null}
      {partiallyUnavailable ? <MembershipStatusBadge label="部分受限" detail={unavailableTitle} className="border-warning/30 bg-warning/5 text-warning" /> : state.routingAvailable ? <MembershipStatusBadge label="智能可路由" detail="至少一个当前协议通过智能路由门禁" className="border-success/25 bg-success/5 text-success" /> : <MembershipStatusBadge label="智能受限" detail={unavailableTitle} className="border-warning/30 bg-warning/5 text-warning" />}
    </>
  )
}

function MembershipStatusBadge({ label, detail, variant = "outline", className, children }: {
  label: string
  detail: string
  variant?: "outline" | "secondary"
  className?: string
  children?: ReactNode
}) {
  return (
    <Tooltip delayDuration={150}>
      <TooltipTrigger asChild>
        <Badge variant={variant} tabIndex={0} aria-label={`${label}：${detail}`} className={`h-5 px-1.5 text-[10px] ${className ?? ""}`}>{children}{label}</Badge>
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-72 text-xs">{detail}</TooltipContent>
    </Tooltip>
  )
}

function BulkPreview({ rows }: { rows: CandidateGroupMembershipRow[] }) {
  const preview = rows.slice(0, 8)
  return (
    <div className="rounded-md border border-border bg-muted/15 p-3">
      <div className="mb-2 flex items-center justify-between gap-3 text-sm"><span className="text-muted-foreground">{"将变更"}</span><span className="font-semibold tabular-nums">{rows.length}</span></div>
      <ul className="max-h-44 space-y-1 overflow-y-auto text-xs" aria-label="批量操作预览">
        {preview.map((row) => <li key={row.key} className="truncate" title={`${row.channelName} / ${row.groupName}`}>{`${row.channelName} / ${row.groupName}`}</li>)}
        {rows.length > preview.length ? <li className="text-muted-foreground">{`另有 ${rows.length - preview.length} 个候选`}</li> : null}
      </ul>
    </div>
  )
}
