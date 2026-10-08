import { useConfirm } from "@/components/ui/confirm-dialog"
import { useEffect, useMemo, useState } from "react"
import { Pencil, Plus, Trash2 } from "lucide-react"
import { toast } from "sonner"
import { apiFetch } from "@/lib/api"
import type { GatewayCandidateGroup, GatewayRouteCandidate } from "@/lib/api-types"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { AutomaticGroupAssignmentDialog } from "@/components/gateway/automatic-group-assignment-dialog"
import { CandidateGroupMembershipPicker } from "@/components/gateway/candidate-group-membership-picker"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  candidateGroupSelectionSupportsMappingFamily,
} from "@/lib/route-candidate-availability"
import { candidateGroupMemberKey, resolveCandidateGroupMembershipSelection } from "@/lib/candidate-group-membership"
import type { GatewayRoutingEnableCandidate } from "@/lib/gateway-routing-enable"

type CandidateGroupsDialogProps = {
  groups: GatewayCandidateGroup[]
  candidates: GatewayRouteCandidate[]
  loading: boolean
  onChanged: () => void
  onEnableRoutingGroup: (candidate: GatewayRoutingEnableCandidate) => void
  isRoutingGroupEnableSaving: (candidate: GatewayRoutingEnableCandidate) => boolean
}

type MemberInput = { channel_id: number; group_name: string; upstream_group_id?: number; presence_generation?: number }
type ModelMappingFamily = "" | "codex" | "claude"

export function CandidateGroupsDialog({ groups, candidates, loading, onChanged, onEnableRoutingGroup, isRoutingGroupEnableSaving }: CandidateGroupsDialogProps) {
  const { confirm, dialog: confirmation } = useConfirm()
  const [open, setOpen] = useState(false)
  const [editingID, setEditingID] = useState<number | null>(null)
  const [editingMembershipVersion, setEditingMembershipVersion] = useState(0)
  const [name, setName] = useState("")
  const [modelMappingFamily, setModelMappingFamily] = useState<ModelMappingFamily>("")
  const [selected, setSelected] = useState<Set<string>>(() => new Set())
  const [existingMembers, setExistingMembers] = useState<MemberInput[]>([])
  const [editorRevision, setEditorRevision] = useState(0)
  const [saving, setSaving] = useState(false)

  const uniqueCandidates = useMemo(() => {
    const seen = new Set<string>()
    return candidates.filter((candidate) => {
      const key = candidateGroupMemberKey(candidate.channel_id, candidate.group_name)
      if (seen.has(key)) return false
      seen.add(key)
      return true
    })
  }, [candidates])
  useEffect(() => {
    if (!open) resetEditor()
  }, [open])

  function resetEditor() {
    setEditingID(null)
    setEditingMembershipVersion(0)
    setName("")
    setModelMappingFamily("")
    setSelected(new Set())
    setExistingMembers([])
    setEditorRevision((current) => current + 1)
  }

  function editGroup(group: GatewayCandidateGroup) {
    const members = (group.members ?? []).map((member) => ({
      channel_id: member.channel_id,
      group_name: member.group_name,
      upstream_group_id: member.upstream_group_id,
      presence_generation: member.presence_generation,
    }))
    const memberKeys = new Set(members.map((member) => candidateGroupMemberKey(member.channel_id, member.group_name)))
    setEditingID(group.id)
    setEditingMembershipVersion(group.membership_version)
    setName(group.name)
    setModelMappingFamily(normalizeModelMappingFamily(group.model_mapping_family))
    setSelected(memberKeys)
    setExistingMembers(members)
    setEditorRevision((current) => current + 1)
  }

  function changeModelMappingFamily(value: string) {
    setModelMappingFamily(normalizeModelMappingFamily(value))
  }

  async function saveGroup() {
    if (saving) return
    if (loading) {
      toast.error("候选分组数据仍在加载，请稍后保存")
      return
    }
    const trimmed = name.trim()
    if (!trimmed) {
      toast.error("请输入分组名称")
      return
    }
    if (selected.size === 0) {
      toast.error("请至少选择一个候选渠道")
      return
    }
    if (editingID && editingMembershipVersion <= 0) {
      toast.error("候选分组版本已失效，请刷新后重试")
      return
    }
    const { members, unresolvedKeys } = resolveCandidateGroupMembershipSelection(existingMembers, uniqueCandidates, selected)
    if (unresolvedKeys.length > 0) {
      toast.error("候选数据已变化，请刷新后重试")
      return
    }
    const selectedCandidates = uniqueCandidates.filter((candidate) => selected.has(candidateGroupMemberKey(candidate.channel_id, candidate.group_name)))
    if (!candidateGroupSelectionSupportsMappingFamily(selectedCandidates, modelMappingFamily)) {
      // Membership is a routing inventory decision. A missing model snapshot
      // (or a paused probe) must not prevent saving it; mapping capability is
      // validated again when a mapped downstream key is enabled or changed.
      toast.warning(`当前候选尚未同步 ${modelMappingFamilyLabel(modelMappingFamily)} 雷达能力，保存后启用映射前仍需完成同步`)
    }
    setSaving(true)
    try {
      await apiFetch(editingID ? `/gateway/candidate-groups/${editingID}` : "/gateway/candidate-groups", {
        method: editingID ? "PATCH" : "POST",
        body: JSON.stringify({
          name: trimmed,
          model_mapping_family: modelMappingFamily,
          expected_membership_version: editingID ? editingMembershipVersion : undefined,
          members,
        }),
      })
      toast.success(editingID ? "候选分组已更新" : "候选分组已创建")
      resetEditor()
      onChanged()
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "保存候选分组失败")
    } finally {
      setSaving(false)
    }
  }

  async function deleteGroup(group: GatewayCandidateGroup) {
    if (!await confirm({ title: `删除候选分组“${group.name}”？`, description: "已分配给 Key 的分组需要先解除分配。", confirmLabel: "删除", destructive: true })) return
    setSaving(true)
    try {
      await apiFetch(`/gateway/candidate-groups/${group.id}`, { method: "DELETE" })
      if (editingID === group.id) resetEditor()
      toast.success("候选分组已删除")
      onChanged()
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "删除候选分组失败")
    } finally {
      setSaving(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={setOpen} busy={saving}>
      <DialogTrigger asChild>
        <Button type="button" size="sm" variant="outline">
          <Plus className="size-3.5" />
          {"候选分组"}
        </Button>
      </DialogTrigger>
      <DialogContent className="grid h-[calc(100dvh-1rem)] w-[calc(100vw-1rem)] max-w-none grid-rows-[auto_minmax(0,1fr)_auto] gap-0 overflow-hidden p-0 sm:h-[min(780px,calc(100dvh-3rem))] sm:w-[90vw] sm:max-w-none">
        <DialogHeader className="border-b border-border px-4 py-4 sm:px-6 sm:py-5">
          <div className="flex flex-wrap items-center justify-between gap-3"><DialogTitle>{"候选分组管理"}</DialogTitle><AutomaticGroupAssignmentDialog groups={groups} candidates={candidates} onChanged={onChanged} /></div>
        </DialogHeader>
        <div className="grid min-h-0 grid-rows-[minmax(220px,2fr)_minmax(360px,3fr)] overflow-y-auto lg:grid-cols-[280px_minmax(0,1fr)] lg:grid-rows-1 lg:overflow-hidden">
          <aside className="flex min-h-0 flex-col border-b border-border bg-muted/20 p-4 lg:border-b-0 lg:border-r">
            <Button type="button" className="w-full" onClick={resetEditor} disabled={saving}>
              <Plus className="size-4" />
              {"新建分组"}
            </Button>
            <div className="mt-4 min-h-0 flex-1 space-y-2 overflow-y-auto pr-1">
              {groups.map((group) => {
                return (
                  <div key={group.id} className={`rounded-md border p-3 ${editingID === group.id ? "border-primary/40 bg-muted" : "border-border bg-background"}`}>
                    <div className="flex items-start gap-1.5">
                      <button type="button" className="min-w-0 flex-1 text-left" disabled={saving} onClick={() => editGroup(group)}>
                        <div className="truncate text-sm font-medium">{group.name}</div>
                        <div className="mt-1 flex items-center gap-1.5 text-xs text-muted-foreground">
                          <span>{`${group.members?.length ?? 0} 个候选`}</span>
                          {modelMappingFamilyLabel(group.model_mapping_family) ? <Badge variant="outline" className="h-5 px-1.5 text-[10px]">{modelMappingFamilyLabel(group.model_mapping_family)}</Badge> : null}
                        </div>
                      </button>
                      <span className="size-5 shrink-0" aria-hidden="true" />
                    </div>
                    <div className="mt-2 flex justify-end gap-1">
                      <Button type="button" size="icon-sm" variant="ghost" disabled={saving} onClick={() => editGroup(group)} aria-label={`编辑 ${group.name}`}>
                        <Pencil className="size-3.5" />
                      </Button>
                      <Button type="button" size="icon-sm" variant="ghost" onClick={() => void deleteGroup(group)} disabled={saving} aria-label={`删除 ${group.name}`}>
                        <Trash2 className="size-3.5" />
                      </Button>
                    </div>
                  </div>
                )
              })}
              {!loading && groups.length === 0 ? <p className="py-8 text-center text-xs text-muted-foreground">{"暂无自定义分组"}</p> : null}
            </div>
          </aside>
          <div className="flex min-h-0 min-w-0 flex-col p-5">
            <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_11rem]">
              <label className="space-y-1.5">
                <span className="text-xs font-medium text-muted-foreground">{"分组名称"}</span>
                <Input value={name} onChange={(event) => setName(event.target.value)} placeholder="例如：GPT 低成本渠道" disabled={saving} />
              </label>
              <label className="space-y-1.5">
                <span className="text-xs font-medium text-muted-foreground">{"雷达属性"}</span>
                <Select value={modelMappingFamily || "__none__"} onValueChange={changeModelMappingFamily} disabled={saving}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="__none__">{"不使用雷达"}</SelectItem>
                    <SelectItem value="codex">{"GPT / Codex"}</SelectItem>
                    <SelectItem value="claude">{"Claude"}</SelectItem>
                  </SelectContent>
                </Select>
              </label>
            </div>
            <CandidateGroupMembershipPicker candidates={candidates} existingMembers={existingMembers} selected={selected} loading={loading} disabled={saving} resetKey={editorRevision} onSelectionChange={(next) => setSelected(next)} onEnableRoutingGroup={onEnableRoutingGroup} isRoutingGroupEnableSaving={isRoutingGroupEnableSaving} />
          </div>
        </div>
        <DialogFooter className="border-t border-border px-4 py-4 sm:px-6">
          <Button type="button" variant="outline" disabled={saving} onClick={() => setOpen(false)}>{"关闭"}</Button>
          <Button type="button" onClick={() => void saveGroup()} disabled={saving || loading}>{saving ? "保存中" : editingID ? "保存修改" : "创建分组"}</Button>
        </DialogFooter>
      </DialogContent>
      {confirmation}
    </Dialog>
  )
}

function normalizeModelMappingFamily(value: string | null | undefined): ModelMappingFamily {
  const family = String(value ?? "").trim().toLowerCase()
  return family === "codex" || family === "claude" ? family : ""
}

function modelMappingFamilyLabel(value: string | null | undefined) {
  const family = normalizeModelMappingFamily(value)
  if (family === "codex") return "GPT / Codex"
  if (family === "claude") return "Claude"
  return ""
}
