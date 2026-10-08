"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import { AlertTriangle, Copy, Loader2, RefreshCw, ShieldCheck } from "lucide-react"
import { toast } from "sonner"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import { apiFetch } from "@/lib/api"
import type { Channel, ChannelDeletionReview, ChannelDeletionReviewClaim } from "@/lib/api-types"
import {
  channelDeletionClaimStatusLabel,
  channelDeletionClaimSubject,
  channelDeletionDispositionLabel,
  channelDeletionOwnershipLabel,
  channelDeletionStatusPresentation,
} from "@/lib/channel-deletion-review"
import { relativeTime } from "@/lib/format"
import { cn } from "@/lib/utils"

type ReviewChannel = Pick<Channel, "id" | "name">
function statusClass(tone: ReturnType<typeof channelDeletionStatusPresentation>["tone"]) {
  switch (tone) {
    case "danger":
      return "border-danger/25 bg-danger/5 text-danger"
    case "success":
      return "border-success/25 bg-success/5 text-success"
    case "neutral":
      return "border-border bg-muted/30 text-muted-foreground"
    default:
      return "border-warning/30 bg-warning/5 text-warning"
  }
}

function claimStatusClass(claim: ChannelDeletionReviewClaim) {
  if (claim.status === "manual_review_required" || claim.status === "processing_unknown" || claim.status === "pending_verification" || claim.status === "processing") {
    return "border-warning/30 bg-warning/5 text-warning"
  }
  return "border-success/25 bg-success/5 text-success"
}

function reviewCopyText(review: ChannelDeletionReview) {
  const lines = [
    `${review.channel_name} · ${review.status}`,
    `intent=${review.intent_id} deletion_epoch=${review.deletion_epoch} identity_revision=${review.channel_identity_revision}`,
  ]
  if (review.last_error) lines.push(`intent_reason=${review.last_error}`)
  for (const claim of review.claims) {
    lines.push(
      `${channelDeletionClaimSubject(claim)} · status=${claim.status} · disposition=${claim.disposition} · claim=${claim.id}/${claim.claim_epoch}`,
    )
    if (claim.observed_remote_id) lines.push(`observed_remote_id=${claim.observed_remote_id}`)
    if (claim.outcome_reason || claim.last_error) lines.push(`reason=${claim.outcome_reason || claim.last_error}`)
  }
  return lines.join("\n")
}

export function ChannelDeletionReviewBadge({
  channel,
  onClick,
}: {
  channel: Channel
  onClick: () => void
}) {
  if (!channel.deletion_pending && !channel.deletion_state) return null
  const presentation = channelDeletionStatusPresentation(channel.deletion_state ?? "pending_remote_cleanup")
  return (
    <button
      type="button"
      className={cn(
        "inline-flex h-6 max-w-full items-center justify-center gap-1 rounded-md border px-2 text-[10px] font-medium leading-none transition-colors hover:bg-muted/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
        statusClass(presentation.tone),
      )}
      aria-haspopup="dialog"
      aria-label={`查看 ${channel.name} 的删除状态：${presentation.label}`}
      title={`查看 ${channel.name} 的删除状态`}
      onClick={onClick}
    >
      {presentation.tone === "danger" ? <AlertTriangle className="size-3 shrink-0" /> : null}
      <span className="truncate">{presentation.label}</span>
    </button>
  )
}

export function ChannelDeletionReviewDialog({
  channel,
  onOpenChange,
  onChanged,
}: {
  channel: ReviewChannel | null
  onOpenChange: (open: boolean) => void
  onChanged: () => void
}) {
  const [review, setReview] = useState<ChannelDeletionReview | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const activeRequest = useRef<AbortController | null>(null)
  const requestGeneration = useRef(0)
  const loadedChannelID = useRef<number | null>(null)

  const channelID = channel?.id
  const loadReview = useCallback(async () => {
    if (!channelID) return null
    activeRequest.current?.abort()
    const controller = new AbortController()
    const generation = requestGeneration.current + 1
    activeRequest.current = controller
    requestGeneration.current = generation
    setLoading(true)
    setError(null)
    try {
      const next = await apiFetch<ChannelDeletionReview>(`/channels/${channelID}/deletion`, { signal: controller.signal })
      if (controller.signal.aborted || generation !== requestGeneration.current) return null
      setReview(next)
      return next
    } catch (cause) {
      if (controller.signal.aborted || generation !== requestGeneration.current) return null
      setError((cause as Error).message || "删除状态加载失败")
      return null
    } finally {
      if (generation === requestGeneration.current) {
        activeRequest.current = null
        setLoading(false)
      }
    }
  }, [channelID])

  useEffect(() => {
    if (!channelID) {
      activeRequest.current?.abort()
      activeRequest.current = null
      requestGeneration.current += 1
      loadedChannelID.current = null
      setReview(null)
      setError(null)
      return
    }
    const reviewStatus = review?.status
    if (reviewStatus === "finalized") return
    const channelChanged = loadedChannelID.current !== channelID
    loadedChannelID.current = channelID
    if (channelChanged) {
      setReview(null)
      setError(null)
    }
    let cancelled = false
    let timeout: number | undefined
    const poll = async () => {
      const next = await loadReview()
      if (!cancelled && next?.status !== "finalized") {
        timeout = window.setTimeout(() => void poll(), 5000)
      }
    }
    timeout = window.setTimeout(() => void poll(), channelChanged || !reviewStatus ? 0 : 5000)
    return () => {
      cancelled = true
      if (timeout !== undefined) window.clearTimeout(timeout)
    }
  }, [channelID, loadReview, review?.status])

  useEffect(() => () => {
    requestGeneration.current += 1
    activeRequest.current?.abort()
  }, [])

  useEffect(() => {
    if (review?.status === "finalized") onChanged()
  }, [onChanged, review?.status])

  async function copyReview() {
    if (!review) return
    try {
      await navigator.clipboard.writeText(reviewCopyText(review))
      toast.success("删除状态详情已复制")
    } catch {
      toast.error("复制失败")
    }
  }

  const status = review ? channelDeletionStatusPresentation(review.status) : null

  return (
    <Dialog
      open={channel != null}
      onOpenChange={onOpenChange}
    >
        <DialogContent className="w-[min(96vw,56rem)] max-w-none sm:max-w-4xl">
          <DialogHeader className="pr-8">
            <DialogTitle className="flex min-w-0 items-center gap-2 text-base">
              <span className="flex size-8 shrink-0 items-center justify-center rounded-md bg-warning/10 text-warning ring-1 ring-inset ring-warning/20">
                <ShieldCheck className="size-4" />
              </span>
              <span className="truncate">{`${channel?.name ?? "渠道"} · 删除状态`}</span>
            </DialogTitle>
            <DialogDescription>
              {"BridgeOps 会自动删除可证明由系统管理的远端密钥，并按策略保留其他密钥。"}
            </DialogDescription>
          </DialogHeader>

          {loading && !review ? (
            <div className="flex h-64 items-center justify-center text-sm text-muted-foreground">
              <Loader2 className="mr-2 size-4 animate-spin" />
              {"正在加载删除状态"}
            </div>
          ) : error && !review ? (
            <div className="flex h-64 flex-col items-center justify-center gap-3 text-center">
              <AlertTriangle className="size-6 text-danger" />
              <p className="max-w-lg break-words text-sm text-danger">{error}</p>
              <Button type="button" size="sm" variant="outline" onClick={() => void loadReview()}>
                <RefreshCw className="size-3.5" />
                {"重试"}
              </Button>
            </div>
          ) : review ? (
            <div className="space-y-3">
              <div className="grid gap-2 border-y border-border bg-muted/20 px-3 py-2.5 text-xs sm:grid-cols-[minmax(0,1fr)_auto] sm:items-center">
                <div className="min-w-0 space-y-1">
                  <div className="flex flex-wrap items-center gap-2">
                    {status ? <span className={cn("inline-flex h-6 items-center rounded-md border px-2 font-medium", statusClass(status.tone))}>{status.label}</span> : null}
                    <span className="text-muted-foreground">{`已尝试 ${review.attempt_count} 次 · ${relativeTime(review.updated_at)}`}</span>
                  </div>
                  <p className="truncate font-mono text-[11px] text-muted-foreground" title={`intent=${review.intent_id} · epoch=${review.deletion_epoch} · identity=${review.channel_identity_revision}`}>
                    {`intent ${review.intent_id} · epoch ${review.deletion_epoch} · identity ${review.channel_identity_revision}`}
                  </p>
                  {review.last_error ? <p className="whitespace-pre-wrap break-words text-danger">{review.last_error}</p> : null}
                </div>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Button type="button" size="icon-sm" variant="outline" aria-label="刷新删除状态" disabled={loading} onClick={() => void loadReview()}>
                      <RefreshCw className={cn("size-3.5", loading && "animate-spin")} />
                    </Button>
                  </TooltipTrigger>
                  <TooltipContent>{"刷新删除状态"}</TooltipContent>
                </Tooltip>
              </div>

              <ScrollArea className="h-[min(58vh,32rem)] rounded-md border border-border">
                {review.claims.length === 0 ? (
                  <div className="flex h-40 items-center justify-center text-sm text-muted-foreground">{"该渠道没有受管远端密钥"}</div>
                ) : (
                  <div className="divide-y divide-border">
                    {review.claims.map((claim) => {
                      const reason = claim.outcome_reason || claim.last_error
                      return (
                        <section key={claim.id} className="space-y-3 px-3 py-3">
                          <div className="flex min-w-0 flex-wrap items-start justify-between gap-2">
                            <div className="min-w-0">
                              <h3 className="truncate text-sm font-semibold" title={channelDeletionClaimSubject(claim)}>{channelDeletionClaimSubject(claim)}</h3>
                              <p className="mt-0.5 truncate font-mono text-[11px] text-muted-foreground" title={`claim=${claim.id}/${claim.claim_epoch} · local_key=${claim.local_key_id}`}>
                                {`claim ${claim.id}/${claim.claim_epoch} · local key ${claim.local_key_id}`}
                              </p>
                            </div>
                            <span className={cn("inline-flex h-6 shrink-0 items-center rounded-md border px-2 text-[11px] font-medium", claimStatusClass(claim))}>
                              {channelDeletionClaimStatusLabel(claim.status)}
                            </span>
                          </div>
                          <dl className="grid gap-x-5 gap-y-1 text-xs sm:grid-cols-4">
                            <div><dt className="text-muted-foreground">{"所有权来源"}</dt><dd className="truncate font-medium">{channelDeletionOwnershipLabel(claim.ownership_provenance)}</dd></div>
                            <div><dt className="text-muted-foreground">{"处理策略"}</dt><dd className="truncate font-medium">{channelDeletionDispositionLabel(claim.disposition)}</dd></div>
                            <div><dt className="text-muted-foreground">{"密钥代次"}</dt><dd className="truncate font-mono" title={claim.key_generation}>{claim.key_generation}</dd></div>
                            <div><dt className="text-muted-foreground">{"远端观测 ID"}</dt><dd className="truncate font-mono" title={claim.observed_remote_id ?? "未观测"}>{claim.observed_remote_id ?? "未观测"}</dd></div>
                          </dl>
                          {reason ? (
                            <div className="max-h-28 overflow-y-auto whitespace-pre-wrap break-words rounded-md bg-muted/30 px-2.5 py-2 text-xs text-foreground">
                              {reason}
                            </div>
                          ) : null}
                        </section>
                      )
                    })}
                  </div>
                )}
              </ScrollArea>
            </div>
          ) : null}

          <DialogFooter className="gap-2 sm:justify-between">
            <Button type="button" size="sm" variant="outline" disabled={!review} onClick={() => void copyReview()}>
              <Copy className="size-3.5" />
              {"复制详情"}
            </Button>
            <Button type="button" size="sm" onClick={() => onOpenChange(false)}>
              {"关闭"}
            </Button>
          </DialogFooter>
        </DialogContent>
    </Dialog>
  )
}
