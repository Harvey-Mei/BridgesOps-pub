import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

import type { ChannelDeletionReviewClaim } from "./api-types"
import {
  channelDeletionClaimSubject,
  channelDeletionStatusPresentation,
} from "./channel-deletion-review"

const claim: ChannelDeletionReviewClaim = {
  id: "9007199254740993",
  claim_epoch: "0",
  local_key_id: "9007199254740994",
  key_generation: "12",
  canonical_group_name: "primary",
  upstream_key_name: "BOG-managed",
  observed_remote_id: "9007199254740998",
  ownership_provenance: "unknown",
  provisioning: false,
  disposition: "manual_review_required",
  status: "manual_review_required",
  updated_at: "2026-07-28T00:00:00Z",
}

test("deletion states present manual-review backlog as automatic cleanup", () => {
  assert.deepEqual(channelDeletionStatusPresentation("pending_remote_cleanup"), { label: "远端清理待处理", tone: "warning" })
  assert.deepEqual(channelDeletionStatusPresentation("manual_review_required"), { label: "自动清理中", tone: "warning" })
  assert.deepEqual(channelDeletionStatusPresentation("ready_for_local_finalize"), { label: "删除收尾中", tone: "success" })
  assert.equal(channelDeletionClaimSubject(claim), "primary · BOG-managed")
})

test("channel table opens the automatic cleanup status UI without review actions", () => {
  const cards = readFileSync(new URL("../components/monitor/channel-cards.tsx", import.meta.url), "utf8")
  const dialog = readFileSync(new URL("../components/monitor/channel-deletion-review-dialog.tsx", import.meta.url), "utf8")

  assert.match(cards, /<ChannelDeletionReviewBadge channel=\{c\}/)
  assert.match(cards, /setDeletionReviewChannel\(c\)/)
  assert.match(cards, /apiFetch<ChannelDeletionAccepted>\(`\/channels\/\$\{c\.id\}`/)
  assert.match(dialog, /apiFetch<ChannelDeletionReview>\(`\/channels\/\$\{channelID\}\/deletion`/)
  assert.match(dialog, /BridgeOps 会自动删除可证明由系统管理的远端密钥，并按策略保留其他密钥/)
  assert.doesNotMatch(dialog, /重新核验并允许删除/)
  assert.doesNotMatch(dialog, /明确保留/)
  assert.match(dialog, /claim\.observed_remote_id/)
  assert.match(dialog, /window\.setTimeout/)
  assert.match(dialog, /requestGeneration/)
  assert.match(dialog, /loadedChannelID/)
  assert.match(dialog, /AbortController/)
  assert.doesNotMatch(dialog, /window\.setInterval/)
})
