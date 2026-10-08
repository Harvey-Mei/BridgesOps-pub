import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

const dialogSource = readFileSync(new URL("../components/gateway/upstream-group-changes-workspace.tsx", import.meta.url), "utf8")
const destinationSource = readFileSync(new URL("../components/gateway/assignment-destination-picker.tsx", import.meta.url), "utf8")
const helperSource = readFileSync(new URL("./upstream-group-changes.ts", import.meta.url), "utf8")
const gatewayPageSource = readFileSync(new URL("../app/gateway-page.tsx", import.meta.url), "utf8")
const upstreamPageSource = readFileSync(new URL("../app/upstream-group-changes-page.tsx", import.meta.url), "utf8")
const queriesSource = readFileSync(new URL("./queries.ts", import.meta.url), "utf8")
const refreshContextSource = readFileSync(new URL("./refresh-context.tsx", import.meta.url), "utf8")

test("history view fetches exactly one requested server cursor page", () => {
	assert.doesNotMatch(dialogSource, /do\s*\{[\s\S]*?page\s*\+=\s*1[\s\S]*?\}\s*while\s*\(page\s*<=\s*pages\)/)
	assert.match(dialogSource, /apiFetch<GatewayUpstreamGroupChangeHistoryCursorPage>\(historyRequestPath, \{ signal: controller\.signal \}\)/)
	assert.match(dialogSource, /upstreamGroupChangeHistoryPath\(historyView, activeHistoryCursorState\.cursor, historyPageSize, filters\)/)
	assert.match(dialogSource, /activeHistoryResponse\?\.items \?\? \[\]/)
	assert.doesNotMatch(dialogSource, /Array\.from\(items\.values\(\)\)/)
	assert.match(dialogSource, /previousCursors/)
	assert.match(dialogSource, /next_cursor/)
})

test("history query pushes every active filter and cursor to the bounded API request", () => {
	assert.match(helperSource, /new URLSearchParams\(\{ page_size:/)
	assert.match(helperSource, /view === "review"/)
	assert.match(helperSource, /page_size:/)
	assert.match(helperSource, /params\.set\("cursor", normalizedCursor\)/)
	for (const parameter of ["search", "channel", "group", "source"]) {
		assert.match(helperSource, new RegExp(`params\\.set\\(\\"${parameter}\\"`))
	}
	assert.match(helperSource, /Math\.min\(100, Math\.max\(1,/)
	assert.match(dialogSource, /setHistoryCursorByView\(\(current\) => \(\{ \.\.\.current, \[view\]: initialUpstreamGroupChangeHistoryCursorState\(\) \}\)\)/)
	assert.match(dialogSource, /createUpstreamGroupChangeHistoryRequestGate\(\)/)
})

test("history responses remain bounded and expose server facet truncation", () => {
  assert.match(dialogSource, /const historyPageSize = 50/)
  assert.match(dialogSource, /activeHistoryResponse\?\.filter_options/)
  assert.match(dialogSource, /筛选项超过 500 个/)
})

test("pending workflow renders a flat queue with one channel sync plus fresh-data refresh", () => {
  assert.match(dialogSource, /groupUpstreamGroupChangesByChannelAndSource\(changes\)/)
  assert.match(dialogSource, /showSyncAction={channel\.sources\[0\]\?\.changes\[0\]\?\.id === change\.id}/)
  assert.match(dialogSource, /onSyncModels\(change\.channel_id\)/)
  assert.match(dialogSource, /upstreamGroupChangePendingFacetCounts/)
  assert.match(dialogSource, /role="table" aria-label="待处理上游分组变更"/)
  assert.match(dialogSource, /pendingBatchAction\.targets\.length \* pendingBatchAction\.candidateGroupIDs\.length/)
  assert.match(dialogSource, /不移除现有归属/)
  assert.match(dialogSource, /aria-label=\{`刷新\$\{refreshLabel\}`\}/)
  assert.match(destinationSource, /点“加入”即保存人工归组/);
  assert.match(destinationSource, /onAssign\(targets, groupIDs\)/);
  assert.match(dialogSource, /reconcileUpstreamGroupChangeSelection\(current, pendingAdded\)/)
  assert.match(upstreamPageSource, /onSyncModels=\{syncChannelModelFamilies\}/)
  assert.match(upstreamPageSource, /onRefresh=\{refreshWorkflow\}/)
  assert.match(upstreamPageSource, /apiFetch<GatewayUpstreamGroupChange\[\]>\("\/gateway\/upstream-group-changes"\)/)
})

test("pending workbench keeps rows dense and exposes isolated keyboard-accessible details", () => {
  assert.match(dialogSource, /grid min-h-14 grid-cols-/)
  assert.match(dialogSource, /<Popover>/)
  assert.match(dialogSource, /aria-label=\{`查看 \$\{channelName\} \/ \$\{change\.group_name\} 变更详情`\}/)
  assert.match(dialogSource, /onClick=\{\(event\) => event\.stopPropagation\(\)\}/)
  assert.match(dialogSource, /{"输入倍率"}/)
  assert.match(dialogSource, /{"输出倍率"}/)
  assert.match(dialogSource, /aria-label=\{`查看 \$\{label\} 的失败详情`\}/)
  assert.match(dialogSource, /copyBatchFailureReason\(reason\)/)
  assert.match(dialogSource, /{"渠道"}.*\{channelName\}/s)
  assert.match(dialogSource, /{"上游分组"}.*\{change\.group_name\}/s)
  assert.match(dialogSource, /role="toolbar" aria-label="已选待处理变更操作"/)
  const failurePanel = dialogSource.slice(dialogSource.indexOf("function BatchFailurePanel"), dialogSource.indexOf("function ChangeQueue"))
  assert.match(failurePanel, /<FailureDetailButton key=/)
  assert.match(dialogSource, /<DialogTitle>\{`\$\{label\} · 异常详情`\}<\/DialogTitle>/)
  assert.match(dialogSource, /<pre className=/)
})

test("blocked pending changes are not selectable and batch responses always refresh", () => {
  assert.match(helperSource, /assignment_actionable/)
  assert.match(helperSource, /assignment_blocked_reason/)
  assert.match(dialogSource, /upstreamGroupChangeIsActionable/)
  assert.match(dialogSource, /const actionable = upstreamGroupChangeIsActionable\(change\)/)
  assert.match(dialogSource, /disabled=\{selectionDisabled \|\| !actionable\}/)
  assert.match(dialogSource, /disabled=\{selectionDisabled \|\| syncingChannelID !== null\}/)
  assert.match(dialogSource, /const interactionLocked = batchSaving !== null \|\| pendingBatchAction !== null/)
  assert.match(dialogSource, /disabled=\{disabled\} \/>/)
  assert.doesNotMatch(dialogSource, /pendingRefreshInterval/)
  assert.match(dialogSource, /setPendingBatchAction\(null\)/)
  assert.match(dialogSource, /await onRefresh\(\)/)
})

test("upstream changes live on a dedicated route and use the shared refresh cache", () => {
  assert.match(upstreamPageSource, /useGatewayUpstreamGroupChanges\(\)/)
  assert.match(upstreamPageSource, /useGatewayCandidateGroups\(\)/)
  assert.match(upstreamPageSource, /useGatewayUpstreamGroupChangeReviewSummary\(\)/)
  assert.match(upstreamPageSource, /refreshRequestRef/)
  assert.match(upstreamPageSource, /if \(refreshRequestRef\.current\) return refreshRequestRef\.current/)
  assert.match(upstreamPageSource, /review-summary/)
  assert.match(upstreamPageSource, /initialLoading=\{initialLoading\}/)
  assert.match(upstreamPageSource, /initialError=\{initialError\}/)
  assert.match(upstreamPageSource, /changes\.data === null \? changes\.error : null/)
  assert.match(upstreamPageSource, /candidateGroups\.data === null \? candidateGroups\.error : null/)
  assert.match(refreshContextSource, /const POLL_INTERVAL_MS = 30_000/)
  assert.match(queriesSource, /useApi<GatewayUpstreamGroupChange\[\]>\(enabled \? "\/gateway\/upstream-group-changes" : null\)/)
  assert.match(queriesSource, /useApi<GatewayCandidateGroup\[\]>\(enabled \? "\/gateway\/candidate-groups" : null\)/)
  assert.match(queriesSource, /useApi<GatewayUpstreamGroupChangeReviewSummary>\(enabled \? "\/gateway\/upstream-group-changes\/review-summary" : null\)/)
  assert.match(gatewayPageSource, /to=\"\/gateway\/upstream-changes\"/)
})

test("manual-review removal recovery is confirmed, reasoned, refreshable, and keeps failures in details", () => {
  assert.match(helperSource, /upstreamGroupChangeRequiresManualRemovalReview/)
  assert.match(dialogSource, /\/gateway\/upstream-group-changes\/\$\{change\.id\}\/manual-review\/resume/)
  assert.match(dialogSource, /JSON\.stringify\(\{ resolution: reason \}\)/)
  assert.match(dialogSource, /placeholder="填写核对依据或工单编号"/)
  assert.match(dialogSource, /maxLength=\{64\}/)
  assert.match(dialogSource, /人工复核受管密钥删除/)
  assert.match(dialogSource, /manualReviewFailures\[change\.id\]/)
  assert.match(dialogSource, /reason=\{manualReviewFailure\}/)
  assert.match(dialogSource, /upstreamGroupChangeManualReviewReasonLabel\(pendingManualReviewChange\)/)
})

test("history failures have an explicit generation-gated retry", () => {
  assert.match(dialogSource, /function retryHistory\(\)/)
  assert.match(dialogSource, /setHistoryRefreshVersion\(\(current\) => current \+ 1\)/)
  assert.match(dialogSource, /<LoadFailureState label="处理记录加载失败"/)
  assert.match(dialogSource, /onRetry=\{retryHistory\}/)
})
