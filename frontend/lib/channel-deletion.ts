import type { Channel, ChannelDeletionAccepted } from "@/lib/api-types"

export function isChannelDeletionPending(channel: Pick<Channel, "deletion_pending" | "deletion_state">) {
  return channel.deletion_pending === true || (channel.deletion_state != null && channel.deletion_state !== "finalized")
}

export function channelDeletionStatusLabel(state: Channel["deletion_state"]) {
  switch (state) {
    case "manual_review_required":
      return "自动清理中"
    case "ready_for_local_finalize":
      return "待本地收尾"
    case "finalized":
      return "删除已完成"
    default:
      return "待远端清理"
  }
}

export function markChannelDeletionPending(channels: Channel[], accepted: ChannelDeletionAccepted) {
  return channels.map((channel) => channel.id === accepted.channel_id
    ? { ...channel, deletion_pending: true, deletion_state: accepted.status }
    : channel)
}
