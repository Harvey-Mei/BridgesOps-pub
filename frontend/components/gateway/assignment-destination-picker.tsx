import { useRef, useState } from "react"
import { Check, ChevronDown, Search } from "lucide-react"
import type { GatewayCandidateGroup, GatewayUpstreamGroupChange } from "@/lib/api-types"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"

export function AssignmentDestinationPicker({ changes, groups, disabled, visibleSelectedCount, compact = false, onAssign }: {
  changes: GatewayUpstreamGroupChange[]
  groups: GatewayCandidateGroup[]
  disabled: boolean
  visibleSelectedCount?: number
  compact?: boolean
  onAssign: (targets: GatewayUpstreamGroupChange[], groupIDs: number[]) => Promise<void>
}) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState("")
  const [multiple, setMultiple] = useState(false)
  const [selected, setSelected] = useState<number[]>([])
  const [targets, setTargets] = useState<GatewayUpstreamGroupChange[]>([])
  const [hiddenCount, setHiddenCount] = useState(0)
  const submitting = useRef(false)
  const validSelected = selected.filter(id => groups.some(group => group.id === id))
  const visibleGroups = groups.filter(group => group.name.toLowerCase().includes(search.trim().toLowerCase()))

  function changeOpen(next: boolean) {
    if (disabled || submitting.current) return
    if (next) {
      setTargets([...changes])
      setHiddenCount(Math.max(0, changes.length - (visibleSelectedCount ?? changes.length)))
      setSearch("")
      setMultiple(false)
      setSelected([])
    }
    setOpen(next)
  }

  async function assign(groupIDs: number[]) {
    if (disabled || submitting.current || targets.length === 0 || groupIDs.length === 0) return
    submitting.current = true
    setOpen(false)
    try { await onAssign(targets, groupIDs) }
    finally { submitting.current = false }
  }

  return <Popover open={open} onOpenChange={changeOpen}>
    <PopoverTrigger asChild>
      <Button type="button" size="sm" variant={compact ? "outline" : "default"} disabled={disabled || changes.length === 0} className={compact ? "h-8 gap-1 px-2" : undefined} aria-label={compact ? `分配 ${changes[0]?.channel_name || changes[0]?.channel_id} / ${changes[0]?.group_name}` : "将已选变更加入分组"} onClick={event => event.stopPropagation()}>
        {compact ? "分配" : `加入分组 (${changes.length})`}<ChevronDown className="size-3.5" />
      </Button>
    </PopoverTrigger>
    <PopoverContent align="end" className="w-96 p-0" onClick={event => event.stopPropagation()}>
      <div className="space-y-2 border-b border-border p-3">
        <div className="text-sm font-semibold">{targets.length === 1 ? `${targets[0].channel_name || targets[0].channel_id} / ${targets[0].group_name}` : `将已选 ${targets.length} 项加入分组`}</div>
        {hiddenCount > 0 && <p className="text-sm text-warning">包含当前筛选外的 {hiddenCount} 项</p>}
        <p className="text-sm text-muted-foreground">{multiple ? "选择目标后保存人工归组，替换原人工目标。" : "点“加入”即保存人工归组，替换原人工目标。"}</p>
        <div className="relative"><Search className="pointer-events-none absolute left-2.5 top-2.5 size-4 text-muted-foreground" /><Input value={search} onChange={event => setSearch(event.target.value)} className="pl-8" placeholder="搜索目标分组" aria-label="搜索目标分组" /></div>
      </div>
      <div className="max-h-72 overflow-y-auto p-1" aria-label="目标候选分组">
        {visibleGroups.map(group => <div key={group.id} className="flex min-w-0 items-center gap-2 rounded-md px-2 py-1 hover:bg-accent/60">
          {multiple ? <label className="flex min-w-0 flex-1 cursor-pointer items-center gap-2 py-2">
            <Checkbox checked={validSelected.includes(group.id)} onCheckedChange={checked => setSelected(current => checked ? [...new Set([...current, group.id])] : current.filter(id => id !== group.id))} aria-label={`选择目标 ${group.name}`} />
            <span className="truncate text-sm" title={group.name}>{group.name}</span>
          </label> : <>
            <span className="min-w-0 flex-1 truncate text-sm" title={group.name}>{group.name}</span>
            <Button type="button" size="sm" variant="ghost" aria-label={`将 ${targets.length} 项加入 ${group.name}`} onClick={() => void assign([group.id])}>加入</Button>
          </>}
        </div>)}
        {visibleGroups.length === 0 && <p className="px-3 py-6 text-center text-sm text-muted-foreground">{groups.length ? "没有匹配分组" : "请先创建候选分组"}</p>}
      </div>
      <div className="space-y-2 border-t border-border p-3">
        {multiple && <p className="max-h-24 overflow-auto break-words text-sm" aria-live="polite">{validSelected.length ? `${targets.length} 项 → ${groups.filter(group => validSelected.includes(group.id)).map(group => group.name).join("、")}` : "请选择目标分组"}</p>}
        <div className="flex items-center justify-between gap-2">
          <Button type="button" variant="ghost" size="sm" onClick={() => { setMultiple(current => !current); setSelected([]) }}>{multiple ? "返回单个目标" : "选择多个分组"}</Button>
          {multiple && <Button type="button" size="sm" disabled={disabled || validSelected.length === 0} onClick={() => void assign(validSelected)}><Check className="size-3.5" />加入 {validSelected.length} 个分组</Button>}
        </div>
      </div>
    </PopoverContent>
  </Popover>
}
