import { useCallback, useRef, useState } from "react"
import { toast } from "sonner"
import { apiFetch } from "@/lib/api"
import { useGatewayCandidateGroups, useGatewayUpstreamGroupChangeReviewSummary, useGatewayUpstreamGroupChanges } from "@/lib/queries"
import type { GatewayCandidateGroup, GatewayUpstreamGroupChange, GatewayUpstreamGroupChangeReviewSummary } from "@/lib/api-types"
import { UpstreamGroupChangesWorkspace } from "@/components/gateway/upstream-group-changes-workspace"

export default function UpstreamGroupChangesPage() {
  const changes = useGatewayUpstreamGroupChanges()
  const candidateGroups = useGatewayCandidateGroups()
  const reviewSummary = useGatewayUpstreamGroupChangeReviewSummary()
  const [syncingChannelID, setSyncingChannelID] = useState<number | null>(null)
  const refreshRequestRef = useRef<Promise<void> | null>(null)

  const refreshWorkflow = useCallback(() => {
    if (refreshRequestRef.current) return refreshRequestRef.current
    const request = Promise.all([
      apiFetch<GatewayUpstreamGroupChange[]>("/gateway/upstream-group-changes"),
      apiFetch<GatewayCandidateGroup[]>("/gateway/candidate-groups"),
      apiFetch<GatewayUpstreamGroupChangeReviewSummary>("/gateway/upstream-group-changes/review-summary").catch(() => null),
    ])
      .then(([nextChanges, nextCandidateGroups, nextReviewSummary]) => {
        changes.setData(nextChanges)
        candidateGroups.setData(nextCandidateGroups)
        if (nextReviewSummary) reviewSummary.setData(nextReviewSummary)
      })
      .finally(() => {
        if (refreshRequestRef.current === request) refreshRequestRef.current = null
      })
    refreshRequestRef.current = request
    return request
  }, [candidateGroups, changes, reviewSummary])

  async function syncChannelModelFamilies(channelID: number) {
    setSyncingChannelID(channelID)
    try {
      const result = await apiFetch<{ items: Array<{ error?: string }>; total: number }>(`/gateway/model-families/sync?channel_id=${channelID}`, { method: "POST" })
      await refreshWorkflow()
      const failed = result.items.filter((item) => item.error).length
      toast.success(failed > 0 ? `已刷新模型列表，${failed} 个分组失败` : `已刷新模型列表，共 ${result.total} 个分组`)
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "刷新模型列表失败")
    } finally {
      setSyncingChannelID(null)
    }
  }

  const initialLoading = (changes.loading && changes.data === null) || (candidateGroups.loading && candidateGroups.data === null)
  const initialError = [
    changes.data === null ? changes.error : null,
    candidateGroups.data === null ? candidateGroups.error : null,
  ].filter((value): value is string => Boolean(value)).join("\n") || null

  return (
    <UpstreamGroupChangesWorkspace
      changes={changes.data ?? []}
      candidateGroups={candidateGroups.data ?? []}
      manualReviewCount={reviewSummary.data?.count ?? 0}
      manualReviewError={reviewSummary.error}
      initialLoading={initialLoading}
      initialError={initialError}
      syncingChannelID={syncingChannelID}
      onRefresh={refreshWorkflow}
      onSyncModels={syncChannelModelFamilies}
      onReconcileReserved={async () => {
        const result = await apiFetch<{ resolved: number; failures: number }>("/gateway/upstream-group-changes/reconcile-reserved", { method: "POST" })
        await refreshWorkflow()
        return result
      }}
      onChanged={() => {
        // resolveBatch awaits the shared onRefresh request immediately after
        // this callback. Keeping this callback side-effect free avoids a
        // duplicate global refresh racing the authoritative page refresh.
      }}
    />
  )
}
