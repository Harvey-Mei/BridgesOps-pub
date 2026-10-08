import { useConfirm } from "@/components/ui/confirm-dialog"
import { useRef, useState } from "react"
import { FolderTree, Loader2, Pencil, Plus, Route, Save, Trash2, X } from "lucide-react"
import { toast } from "sonner"
import type { GatewayCandidateGroup, GatewayDownstreamKey, GatewayKeyGroup, GatewayKeyGroupRoutePolicy, GatewayModelFamilyCatalog } from "@/lib/api-types"
import { apiFetch } from "@/lib/api"
import { keyGroupRoutePolicyDraft } from "@/lib/gateway-key-route-policy"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"

export function GatewayKeyGroupsDialog({ groups, keys, candidateGroups, onChanged }: { groups: GatewayKeyGroup[]; keys: GatewayDownstreamKey[]; candidateGroups: GatewayCandidateGroup[]; onChanged: () => void }) {
	const { confirm, dialog: confirmation } = useConfirm()
  const [open, setOpen] = useState(false)
  const [name, setName] = useState("")
  const [editingID, setEditingID] = useState<number | null>(null)
  const [saving, setSaving] = useState(false)
  const [policies, setPolicies] = useState<GatewayKeyGroupRoutePolicy[]>([])
  const [policyKeyGroupID, setPolicyKeyGroupID] = useState(0)
  const [policyApp, setPolicyApp] = useState<"codex" | "claude" | "gemini">("codex")
  const [policyFamily, setPolicyFamily] = useState("codex")
  const [policyAllCandidates, setPolicyAllCandidates] = useState(false)
  const [policyCandidateGroupID, setPolicyCandidateGroupID] = useState(0)
  const [policyEnabled, setPolicyEnabled] = useState(true)
  const [modelCatalog, setModelCatalog] = useState<GatewayModelFamilyCatalog | null>(null)
  const [loadingPolicies, setLoadingPolicies] = useState(false)
  const [policyLoadError, setPolicyLoadError] = useState(false)
  const policyLoadGeneration = useRef(0)

	function resetDraft() {
		setName("")
		setEditingID(null)
	}

	function handleOpenChange(nextOpen: boolean) {
    if (saving) return
    if (!nextOpen) policyLoadGeneration.current += 1
		if (!nextOpen) resetDraft()
		setOpen(nextOpen)
		if (nextOpen) void loadPolicies()
	}

	type PolicySelection = { keyGroupID: number; app: typeof policyApp; family: string }

	function applyPolicySelection(nextPolicies: GatewayKeyGroupRoutePolicy[], selection: PolicySelection) {
		const family = selection.family.trim().toLowerCase()
		const nextDraft = keyGroupRoutePolicyDraft(nextPolicies, selection.keyGroupID, selection.app, family, candidateGroups[0]?.id ?? 0)
		setPolicyKeyGroupID(selection.keyGroupID)
		setPolicyApp(selection.app)
		setPolicyFamily(family)
		setPolicyAllCandidates(nextDraft.allCandidates)
		setPolicyCandidateGroupID(nextDraft.candidateGroupID)
		setPolicyEnabled(nextDraft.enabled)
	}

	function policyFamilies(catalog: GatewayModelFamilyCatalog | null, app: typeof policyApp) {
		return (catalog?.families ?? []).filter((family) => (family.compatible_apps ?? []).includes(app))
	}

	async function loadPolicies(preferred?: Partial<PolicySelection>) {
    const generation = ++policyLoadGeneration.current
		setLoadingPolicies(true)
		try {
			const [nextPolicies, nextCatalog] = await Promise.all([
				apiFetch<GatewayKeyGroupRoutePolicy[]>("/gateway/key-group-route-policies"),
				apiFetch<GatewayModelFamilyCatalog>("/gateway/model-catalog"),
			])
			if (generation !== policyLoadGeneration.current) return false
      setPolicyLoadError(false)
			setPolicies(nextPolicies)
			setModelCatalog(nextCatalog)
			const keyGroupID = groups.some((group) => group.id === preferred?.keyGroupID)
				? preferred?.keyGroupID ?? 0
				: groups.some((group) => group.id === policyKeyGroupID) ? policyKeyGroupID : groups[0]?.id ?? 0
			const app = preferred?.app ?? policyApp
			const families = policyFamilies(nextCatalog, app)
			const requestedFamily = (preferred?.family ?? policyFamily).trim().toLowerCase()
			const family = families.some((item) => item.code === requestedFamily) ? requestedFamily : families[0]?.code ?? app
			applyPolicySelection(nextPolicies, { keyGroupID, app, family })
      return true
		} catch {
      if (generation === policyLoadGeneration.current) setPolicyLoadError(true)
      return false
		} finally {
      if (generation === policyLoadGeneration.current) setLoadingPolicies(false)
		}
	}

	function selectPolicyKeyGroup(keyGroupID: number) {
		applyPolicySelection(policies, { keyGroupID, app: policyApp, family: policyFamily })
	}

	function selectPolicyApp(app: typeof policyApp) {
		const family = policyFamilies(modelCatalog, app)[0]?.code ?? app
		applyPolicySelection(policies, { keyGroupID: policyKeyGroupID, app, family })
	}

	function selectPolicyFamily(family: string) {
		applyPolicySelection(policies, { keyGroupID: policyKeyGroupID, app: policyApp, family })
	}

	function editPolicy(policy: GatewayKeyGroupRoutePolicy) {
		applyPolicySelection(policies, {
			keyGroupID: policy.key_group_id,
			app: policy.gateway_app as typeof policyApp,
			family: policy.model_family,
		})
	}

	async function savePolicy() {
    if (saving || loadingPolicies || policyLoadError) return
		if (!policyKeyGroupID || (!policyAllCandidates && !policyCandidateGroupID)) {
			toast.error("请选择 Key 分组和默认候选分组")
			return
		}
		const family = policyFamily.trim().toLowerCase()
		if (!family) {
			toast.error("请选择模型家族")
			return
		}
		const existing = keyGroupRoutePolicyDraft(policies, policyKeyGroupID, policyApp, family, candidateGroups[0]?.id ?? 0).existing
		const policy = {
			id: existing?.id ?? 0,
			key_group_id: policyKeyGroupID,
			gateway_app: policyApp,
			model_family: family,
			default_candidate_group_id: policyAllCandidates ? undefined : policyCandidateGroupID,
			target_mode: policyAllCandidates ? "all_candidates" : "candidate_group",
			enabled: policyEnabled,
			policy_version: existing?.policy_version ?? 0,
		}
		setSaving(true)
		try {
			await apiFetch(existing ? `/gateway/key-group-route-policies/${existing.id}` : "/gateway/key-group-route-policies", {
				method: existing ? "PATCH" : "POST",
				body: JSON.stringify({ expected_version: existing?.policy_version ?? 0, policy }),
			})
      const loaded = await loadPolicies({ keyGroupID: policyKeyGroupID, app: policyApp, family })
      if (loaded) toast.success("Key 分组默认路由已保存")
      else toast.warning("默认路由已保存，列表刷新失败，请重新加载")
		} catch (error) {
			toast.error(error instanceof Error ? error.message : "保存默认路由失败")
		} finally {
			setSaving(false)
		}
	}

	async function removePolicy(policy: GatewayKeyGroupRoutePolicy) {
		if (saving || loadingPolicies || policyLoadError) return
    if (!await confirm({ title: `删除 ${policy.gateway_app} / ${policy.model_family} 的默认路由？`, description: "已创建 Key 的路由不会改变。", confirmLabel: "删除", destructive: true })) return
		setSaving(true)
		try {
			await apiFetch(`/gateway/key-group-route-policies/${policy.id}?expected_version=${policy.policy_version}`, { method: "DELETE" })
      const loaded = await loadPolicies()
      if (loaded) toast.success("默认路由策略已删除")
      else toast.warning("策略已删除，列表刷新失败，请重新加载")
		} catch (error) {
			toast.error(error instanceof Error ? error.message : "删除默认路由失败")
		} finally {
			setSaving(false)
		}
	}

  async function save() {
    if (saving) return
    const trimmed = name.trim()
    if (!trimmed) {
      toast.error("请输入分组名称")
      return
    }
    setSaving(true)
    try {
      await apiFetch(editingID ? `/gateway/key-groups/${editingID}` : "/gateway/key-groups", {
        method: editingID ? "PATCH" : "POST",
        body: JSON.stringify({ name: trimmed }),
      })
		resetDraft()
      onChanged()
      toast.success(editingID ? "分组名称已更新" : "Key 分组已创建")
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "保存分组失败")
    } finally {
      setSaving(false)
    }
  }

  async function remove(group: GatewayKeyGroup) {
    if (keys.some((key) => key.key_group_id === group.id)) {
      toast.error("请先把组内 Key 移出该分组")
      return
    }
    setSaving(true)
    try {
      await apiFetch(`/gateway/key-groups/${group.id}`, { method: "DELETE" })
      onChanged()
      toast.success("Key 分组已删除")
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "删除分组失败")
    } finally {
      setSaving(false)
    }
  }

  const selectedPolicy = keyGroupRoutePolicyDraft(
    policies,
    policyKeyGroupID,
    policyApp,
    policyFamily,
    candidateGroups[0]?.id ?? 0,
  ).existing
  const compatiblePolicyFamilies = policyFamilies(modelCatalog, policyApp)

  return (
		<Dialog open={open} onOpenChange={handleOpenChange} busy={saving}>
      <DialogTrigger asChild>
        <Button type="button" size="sm" variant="outline"><FolderTree className="size-3.5" />{"Key 分组"}</Button>
      </DialogTrigger>
      <DialogContent className="w-[calc(100vw-1rem)] max-w-none sm:w-[min(980px,calc(100vw-3rem))] sm:max-w-none">
        <DialogHeader><DialogTitle>{"Key 分组管理"}</DialogTitle></DialogHeader>
        <div className="flex gap-2 border border-brand/15 bg-brand/5 p-3">
          <Input value={name} onChange={(event) => setName(event.target.value)} placeholder="例如 办公室" disabled={saving} onKeyDown={(event) => { if (event.key === "Enter") void save() }} />
          <Button type="button" onClick={() => void save()} disabled={saving} size="icon" title={editingID ? "保存名称" : "创建分组"} aria-label={editingID ? "保存名称" : "创建分组"}>{editingID ? <Save className="size-4" /> : <Plus className="size-4" />}</Button>
		  {editingID ? <Button type="button" variant="outline" size="icon" disabled={saving} onClick={resetDraft} title="取消编辑" aria-label="取消编辑"><X className="size-4" /></Button> : null}
        </div>
        <div className="grid min-h-0 gap-5 lg:grid-cols-[minmax(18rem,0.85fr)_minmax(0,1.4fr)]">
        <div className="max-h-[520px] space-y-2 overflow-y-auto">
          {groups.map((group) => {
            const count = keys.filter((key) => key.key_group_id === group.id).length
            return <div key={group.id} className="flex items-center gap-3 rounded-lg border border-border bg-background px-3 py-2.5">
              <span className="flex size-8 shrink-0 items-center justify-center rounded-md bg-muted text-foreground"><FolderTree className="size-4" /></span>
              <div className="min-w-0 flex-1"><div className="truncate text-sm font-semibold" title={group.name}>{group.name}</div><div className="text-[11px] text-muted-foreground">{`${count} 个 Key`}</div></div>
              <Button type="button" variant="ghost" size="icon-sm" disabled={saving} onClick={() => { setEditingID(group.id); setName(group.name) }} title="重命名" aria-label={`重命名 ${group.name}`}><Pencil className="size-3.5" /></Button>
              <Button type="button" variant="ghost" size="icon-sm" disabled={saving || count > 0} onClick={() => void remove(group)} title={count > 0 ? "组内仍有 Key" : "删除分组"} aria-label={`删除 ${group.name}`}><Trash2 className="size-3.5" /></Button>
            </div>
          })}
          {groups.length === 0 ? <div className="rounded-lg border border-dashed border-border py-10 text-center text-sm text-muted-foreground">{"暂无 Key 分组"}</div> : null}
        </div>
        <section className="min-w-0 border-t border-border pt-4 lg:border-l lg:border-t-0 lg:pl-5 lg:pt-0">
          <div className="flex flex-wrap items-center gap-2"><Route className="size-4 text-brand" /><h3 className="text-sm font-semibold">{"新建或移动 Key 的默认路由"}</h3>{selectedPolicy ? <Badge variant="outline">{`编辑 v${selectedPolicy.policy_version}`}</Badge> : <Badge variant="secondary">{"新策略"}</Badge>}{loadingPolicies ? <Loader2 className="size-4 animate-spin text-muted-foreground" /> : null}</div>
          {policyLoadError ? <div className="mt-3 flex items-center justify-between gap-2 rounded-lg border border-warning/30 px-3 py-2 text-sm" role="alert"><span>默认路由版本未加载，请先刷新再编辑。</span><Button size="sm" variant="outline" disabled={saving || loadingPolicies} onClick={() => void loadPolicies()}>重新加载</Button></div> : null}
          <div className="mt-3 grid gap-3 sm:grid-cols-2">
            <label className="space-y-1.5"><span className="text-xs text-muted-foreground">{"Key 分组"}</span><Select value={policyKeyGroupID ? String(policyKeyGroupID) : ""} onValueChange={(value) => selectPolicyKeyGroup(Number(value))} disabled={saving || loadingPolicies || policyLoadError}><SelectTrigger><SelectValue placeholder="选择 Key 分组" /></SelectTrigger><SelectContent>{groups.map((group) => <SelectItem key={group.id} value={String(group.id)}>{group.name}</SelectItem>)}</SelectContent></Select></label>
            <label className="space-y-1.5"><span className="text-xs text-muted-foreground">{"网关协议"}</span><Select value={policyApp} onValueChange={(value) => selectPolicyApp(value as typeof policyApp)} disabled={saving || loadingPolicies || policyLoadError}><SelectTrigger><SelectValue /></SelectTrigger><SelectContent><SelectItem value="codex">{"Codex"}</SelectItem><SelectItem value="claude">{"Claude"}</SelectItem><SelectItem value="gemini">{"Gemini"}</SelectItem></SelectContent></Select></label>
				<label className="space-y-1.5"><span className="text-xs text-muted-foreground">{"模型家族"}</span><Select value={policyFamily} onValueChange={selectPolicyFamily} disabled={saving || loadingPolicies || policyLoadError || compatiblePolicyFamilies.length === 0}><SelectTrigger><SelectValue /></SelectTrigger><SelectContent>{compatiblePolicyFamilies.map((family) => <SelectItem key={family.code} value={family.code}>{family.display_name}</SelectItem>)}</SelectContent></Select></label>
            <label className="flex items-end gap-2 pb-2 text-sm"><Switch checked={policyEnabled} onCheckedChange={setPolicyEnabled} disabled={saving || loadingPolicies || policyLoadError} /><span>{"启用策略"}</span></label>
            <label className="flex items-center gap-2 text-sm sm:col-span-2"><Switch checked={policyAllCandidates} onCheckedChange={setPolicyAllCandidates} disabled={saving || loadingPolicies || policyLoadError} /><span>{"显式使用全部候选"}</span></label>
            {!policyAllCandidates ? <label className="space-y-1.5 sm:col-span-2"><span className="text-xs text-muted-foreground">{"默认候选分组"}</span><Select value={policyCandidateGroupID ? String(policyCandidateGroupID) : ""} onValueChange={(value) => setPolicyCandidateGroupID(Number(value))} disabled={saving || loadingPolicies || policyLoadError}><SelectTrigger><SelectValue placeholder="选择候选分组" /></SelectTrigger><SelectContent>{candidateGroups.map((group) => <SelectItem key={group.id} value={String(group.id)}>{group.name}</SelectItem>)}</SelectContent></Select></label> : null}
          </div>
          <div className="mt-3 flex justify-end"><Button onClick={() => void savePolicy()} disabled={saving || loadingPolicies || policyLoadError || !policyKeyGroupID || !policyFamily}><Save className="size-4" />{selectedPolicy ? "更新默认路由" : "创建默认路由"}</Button></div>
          <div className="mt-4 border-t border-border pt-3">
            {policies.map((policy) => <div key={policy.id} className={`flex items-center gap-2 border-b border-border py-2 text-sm last:border-b-0 ${selectedPolicy?.id === policy.id ? "bg-muted/30" : ""}`}><button type="button" className="min-w-0 flex-1 text-left" onClick={() => editPolicy(policy)} disabled={saving || loadingPolicies || policyLoadError}><span className="block truncate font-medium">{`${groups.find((group) => group.id === policy.key_group_id)?.name ?? `#${policy.key_group_id}`} · ${policy.gateway_app} · ${policy.model_family}`}</span><span className="block truncate text-xs text-muted-foreground">{policy.target_mode === "all_candidates" ? "全部候选" : candidateGroups.find((group) => group.id === policy.default_candidate_group_id)?.name ?? `候选组 #${policy.default_candidate_group_id}`}</span></button><Badge variant={policy.enabled ? "outline" : "secondary"}>{policy.enabled ? `v${policy.policy_version}` : "已停用"}</Badge><Button size="icon-sm" variant="ghost" onClick={() => editPolicy(policy)} disabled={saving || loadingPolicies || policyLoadError} title="编辑默认路由"><Pencil className="size-3.5" /></Button><Button size="icon-sm" variant="ghost" onClick={() => void removePolicy(policy)} disabled={saving || loadingPolicies || policyLoadError} title="删除默认路由"><Trash2 className="size-3.5" /></Button></div>)}
            {policies.length === 0 ? <p className="py-8 text-center text-sm text-muted-foreground">{"暂无默认路由策略"}</p> : null}
          </div>
        </section>
        </div>
      </DialogContent>
      {confirmation}
    </Dialog>
  )
}
