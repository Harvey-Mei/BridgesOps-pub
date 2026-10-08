import type {
  ChannelDeletionKeyClaimStatus,
  ChannelDeletionReviewClaim,
  ChannelDeletionState,
} from "@/lib/api-types"

export interface ChannelDeletionStatusPresentation {
  label: string
  tone: "warning" | "danger" | "success" | "neutral"
}

export function channelDeletionStatusPresentation(status: ChannelDeletionState): ChannelDeletionStatusPresentation {
  switch (status) {
    case "manual_review_required":
      return { label: "自动清理中", tone: "warning" }
    case "ready_for_local_finalize":
      return { label: "删除收尾中", tone: "success" }
    case "finalized":
      return { label: "删除已完成", tone: "neutral" }
    default:
      return { label: "远端清理待处理", tone: "warning" }
  }
}

const claimStatusLabels: Record<ChannelDeletionKeyClaimStatus, string> = {
  pending_verification: "等待核验",
  processing: "正在核验",
  processing_unknown: "结果待确认",
  delete_confirmed: "远端删除已确认",
  initially_absent_confirmed: "远端不存在已确认",
  retained_adopted: "接管密钥已保留",
  retained_by_review: "按策略保留",
  manual_review_required: "等待自动处理",
}

export function channelDeletionClaimStatusLabel(status: ChannelDeletionKeyClaimStatus) {
  return claimStatusLabels[status]
}

export function channelDeletionOwnershipLabel(provenance: string) {
  switch (provenance) {
    case "created":
      return "由系统创建"
    case "provisioning_claim":
      return "创建中所有权"
    case "adopted":
      return "管理员接管"
    default:
      return "所有权未知"
  }
}

export function channelDeletionDispositionLabel(disposition: ChannelDeletionReviewClaim["disposition"]) {
  switch (disposition) {
    case "verify_managed_delete":
      return "核验后删除"
    case "retain_adopted":
      return "保留接管密钥"
    default:
      return "自动保留"
  }
}

export function channelDeletionClaimSubject(claim: ChannelDeletionReviewClaim) {
  const group = claim.canonical_group_name?.trim()
  const key = claim.upstream_key_name?.trim()
  if (group && key) return `${group} · ${key}`
  if (group) return group
  if (key) return key
  return `本地密钥 #${claim.local_key_id}`
}
