import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

const gatewayPage = readFileSync(new URL("../app/gateway-page.tsx", import.meta.url), "utf8")
const gatewayKeysView = readFileSync(new URL("../components/gateway/gateway-keys-view.tsx", import.meta.url), "utf8")

test("inline fixed-route action waits for and merges the mapped-target preview", () => {
  assert.match(gatewayPage, /const mappedTargetState = manualMappedTargetSelectionState\(candidate, mappedTargetPreview\?\.mappedTargetData\)/)
  assert.match(gatewayPage, /mappedTargetPreview\.mappedTargetData != null && mappedTargetState\.selectable/)
  assert.match(gatewayPage, /disabled=\{interactionsDisabled \|\| savingKeyIDs\.has\(key\.id\) \|\| !manualSelectable\}/)
})

test("fixed-route dialog merges the server-owned mapped-target selection gate", () => {
  assert.match(gatewayKeysView, /useGatewayManualMappedTargetPreview\(manualRoutePickerKey\?\.id/)
  assert.match(gatewayKeysView, /const mappedTargetState = manualMappedTargetSelectionState\(candidate, mappedTargetPreview\)/)
  assert.match(gatewayKeysView, /const selectable = mappedTargetState\.selectable/)
  assert.match(gatewayKeysView, /disabled=\{!selectable \|\| saving \|\| keyData == null\}/)
})

test("fixed-route dialog waits for both generic and mapped-target previews", () => {
  assert.match(gatewayKeysView, /manualRoutePickerPreview\.loading \|\| manualRoutePickerPreview\.refreshing \|\| manualMappedTargetPreview\.loading \|\| manualMappedTargetPreview\.refreshing/)
  assert.match(gatewayKeysView, /manualRoutePickerPreview\.error \|\| manualMappedTargetPreview\.error/)
})

test("current fixed-target summary consumes the same mapped-target gate", () => {
  assert.match(gatewayKeysView, /manualMappedTargetSelectionState\(selectedRoute, routePreview\.mappedTargetData\)/)
  assert.match(gatewayKeysView, /routePreview\.mappedTargetLoading/)
  assert.match(gatewayKeysView, /routePreview\.mappedTargetError/)
  assert.match(gatewayKeysView, /selectedRoute && !selectedRouteDisabled \? selectedRoute : null/)
})

test("per-key preview loader fetches mapped targets under the existing concurrency bound", () => {
  const queries = readFileSync(new URL("./queries.ts", import.meta.url), "utf8")
  assert.match(queries, /apiFetch<GatewayManualMappedTargetPreview>\(`\/gateway\/manual-mapped-target-preview\?downstream_key_id=\$\{key\.id\}`/)
  assert.match(queries, /const workerCount = Math\.min\(GATEWAY_ROUTE_PREVIEW_CONCURRENCY, previewKeys\.length\)/)
  assert.match(queries, /mappedTargetLoading: !key\.smartRouting/)
})

test("fixed-route dialog presents a missing rate snapshot as a non-blocking warning", () => {
  assert.match(gatewayKeysView, /const rateSnapshotMissing = candidateAvailabilityReasons\(candidate\)\.includes\("no_rate_snapshot"\)/)
  assert.match(gatewayKeysView, /rateSnapshotMissing \? "倍率 暂无" :/)
  assert.match(gatewayKeysView, /倍率快照缺失 · 固定直通/)
  assert.match(gatewayPage, /routeCandidateRateSnapshotMissing\(candidate\) \? "倍率 暂无"/)
})

test("generic candidate previews cannot opt into mapped-model blocking", () => {
  const availability = readFileSync(new URL("./route-candidate-availability.ts", import.meta.url), "utf8")
  const mappedStart = availability.indexOf("const manualRouteKnownMappedModelBlockingReasons")
  const mappedEnd = availability.indexOf("const availabilityLabels", mappedStart)
  const mappedReasons = availability.slice(mappedStart, mappedEnd)

  assert.ok(mappedStart >= 0 && mappedEnd > mappedStart)
  assert.match(mappedReasons, /"model_not_listed"/)
  assert.match(mappedReasons, /"model_not_supported"/)
  assert.doesNotMatch(gatewayPage, /manualRouteCandidateSelectable\(candidate\)/)
  assert.match(gatewayPage, /manualMappedTargetSelectionState\(candidate, mappedTargetPreview\?\.mappedTargetData\)/)
  assert.match(gatewayKeysView, /manualMappedTargetSelectionState\(candidate, mappedTargetPreview\)/)
})
