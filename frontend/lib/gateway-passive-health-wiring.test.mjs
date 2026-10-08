import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

const gatewayPage = readFileSync(new URL("../app/gateway-page.tsx", import.meta.url), "utf8")
const gatewayKeysView = readFileSync(new URL("../components/gateway/gateway-keys-view.tsx", import.meta.url), "utf8")
const candidateGroupPicker = readFileSync(new URL("../components/gateway/candidate-group-membership-picker.tsx", import.meta.url), "utf8")
const candidateGroupsDialog = readFileSync(new URL("../components/gateway/candidate-groups-dialog.tsx", import.meta.url), "utf8")
const clientImportData = readFileSync(new URL("../components/gateway/client-import-data.ts", import.meta.url), "utf8")
const candidateMembership = readFileSync(new URL("./candidate-group-membership.ts", import.meta.url), "utf8")
const candidateAvailability = readFileSync(new URL("./route-candidate-availability.ts", import.meta.url), "utf8")

test("gateway health UI exposes passive request signals and cooldown recovery only", () => {
  assert.match(gatewayPage, /title="上游健康状态"/)
  assert.match(gatewayPage, /延迟由真实请求被动记录；仅在路由冷却恢复阶段自动探测/)
  assert.match(gatewayPage, /真实请求样本.*尚未参与评分/)
  assert.match(gatewayPage, /\/gateway\/model-families\/sync/)
  assert.match(gatewayPage, /刷新全部模型/)
  assert.match(gatewayPage, /onClick=\{\(\) => void enableRoutingGroup\(routingEnableCandidate\)\}/)
})

test("gateway health UI does not expose legacy scheduled-probe controls", () => {
  for (const endpoint of ["/gateway/probe-channel", "/gateway/probe-group", "/gateway/latency-probe-model"]) {
    assert.doesNotMatch(gatewayPage, new RegExp(endpoint))
  }
  for (const field of [
    "latencyProbeEnabled",
    "latencyProbeIntervalSeconds",
    "latencyProbeMaxCandidates",
    "latencyProbeModelSyncMinutes",
    "latencyProbeTimeoutSeconds",
  ]) {
    assert.doesNotMatch(gatewayPage, new RegExp(field))
  }
  for (const label of ["探测周期", "上游探测模型", "分组探测", "单轮估算费用", "启用筛选结果", "暂停筛选结果"]) {
    assert.doesNotMatch(gatewayPage, new RegExp(label))
  }
  for (const source of [gatewayKeysView, candidateGroupPicker, candidateGroupsDialog]) {
    assert.doesNotMatch(source, /探测暂停|后台延迟探测|probe_paused/)
  }
  for (const source of [gatewayPage, gatewayKeysView, candidateGroupPicker, candidateGroupsDialog, clientImportData, candidateMembership, candidateAvailability]) {
    assert.doesNotMatch(source, /\.probe_disabled\b/)
  }
  assert.doesNotMatch(gatewayPage, /probe_disabled\s*:/)
  assert.doesNotMatch(gatewayPage, /(?:channel|group)\.probe_enabled\b/)
})
