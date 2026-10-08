import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

const gatewayPage = readFileSync(new URL("../app/gateway-page.tsx", import.meta.url), "utf8")
const gatewayKeysView = readFileSync(new URL("../components/gateway/gateway-keys-view.tsx", import.meta.url), "utf8")

test("candidate management treats probe inventory loading as unknown, not as an empty snapshot", () => {
  assert.match(gatewayPage, /candidateInventoryLoading=\{probeConfig\.loading\}/)
  assert.match(gatewayKeysView, /candidateInventoryLoading: boolean/)
  assert.match(gatewayKeysView, /const inventoryLoading = candidateGroupsLoading \|\| candidateInventoryLoading/)
  assert.match(gatewayKeysView, /keyData && !inventoryLoading\s*\? candidateManagementInventoryCandidates/)
  assert.match(gatewayKeysView, /候选库存加载中，暂不能修改候选状态/)
  assert.match(gatewayKeysView, /if \(inventoryLoading\) return/)
})

test("candidate management disables every route mutation while its inventory is loading", () => {
  assert.match(gatewayKeysView, /\(disabled\) => updateDisabledCandidate\(candidate, disabled\),\s*inventoryLoading,/)

  const start = gatewayPage.indexOf("function renderRouteCandidateItem")
  const end = gatewayPage.indexOf("\n  return (", start)
  const renderSource = gatewayPage.slice(start, end)
  assert.ok(start >= 0 && end > start)
  assert.match(renderSource, /disabled=\{interactionsDisabled \|\| routingEnableSaving \|\| !routingEnableAllowed\}/)
  assert.match(renderSource, /disabled=\{interactionsDisabled \|\| savingKeyIDs\.has\(key\.id\) \|\| !manualSelectable\}/)
  assert.match(renderSource, /disabled=\{interactionsDisabled \|\| saving \|\| anotherCandidateSaving \|\| savingKeyIDs\.has\(key\.id\) \|\| selected\}/)
  assert.match(renderSource, /if \(interactionsDisabled\) return/)
})

test("missing snapshots remain identity-only rows and never enter route actions", () => {
  const inventory = readFileSync(new URL("./candidate-management-inventory.ts", import.meta.url), "utf8")
  const missingStart = inventory.indexOf('kind: "missing_snapshot"')
  const missingEnd = inventory.indexOf("function normalizeGroupName", missingStart)
  const missingSource = inventory.slice(missingStart, missingEnd)

  assert.ok(missingStart >= 0 && missingEnd > missingStart)
  assert.match(missingSource, /channel_type: "unknown"/)
  assert.doesNotMatch(missingSource, /effective_price/)
  assert.match(gatewayKeysView, /candidateManagementInventoryCandidateIsCurrent\(candidate\) \? renderRouteCandidateItem/)
  assert.match(gatewayKeysView, /<Badge variant="outline" className="h-5 min-w-8 justify-center px-1 text-\[10px\]">\{"-"\}<\/Badge>/)
  assert.match(gatewayKeysView, /无法显示渠道类型、倍率或执行操作/)
})

test("candidate management keeps one flat global route rank across filters", () => {
  assert.doesNotMatch(gatewayKeysView, /groupCandidateGroupMembershipRows/)
  assert.doesNotMatch(gatewayKeysView, /expandedChannelIDs|toggleChannel|按渠道稳定分组/)

  const dialogStart = gatewayKeysView.indexOf("function CandidateManagementDialog")
  const dialogEnd = gatewayKeysView.indexOf("\nfunction KeyStat", dialogStart)
  const dialogSource = gatewayKeysView.slice(dialogStart, dialogEnd)
  assert.ok(dialogStart >= 0 && dialogEnd > dialogStart)
  assert.match(dialogSource, /candidateManagementRankedRows\(\s*inventoryRows,\s*previewCandidates,\s*groupCandidates,/)
  assert.match(dialogSource, /candidateManagementCategorizeRows\(filteredRows, membershipProtocol\)/)
  assert.match(dialogSource, /const activeRankedRows = useMemo\(\(\) => rankedRows\.filter\(\(\{ row \}\) => activeRowKeys\.has\(row\.key\)\)/)
  assert.match(dialogSource, /activeRankedRows\.map\(\(\{ row, candidate, rank \}\)/)

  const rankedStart = gatewayKeysView.indexOf("function candidateManagementRankedRows")
  const rankedEnd = gatewayKeysView.indexOf("\nfunction candidateManagementRowReason", rankedStart)
  const rankedSource = gatewayKeysView.slice(rankedStart, rankedEnd)
  assert.ok(rankedStart >= 0 && rankedEnd > rankedStart)
  assert.match(rankedSource, /candidateManagementRankedMemberKeys\(rowKeys, previewCandidates, originalInventoryCandidates\)/)
  assert.match(rankedSource, /rankByKey\.get\(row\.key\)/)
  assert.match(rankedSource, /if \(left\.rank > 0 && right\.rank > 0\) return left\.rank - right\.rank/)
  assert.match(rankedSource, /if \(leftCurrent !== rightCurrent\) return leftCurrent \? -1 : 1/)
})
