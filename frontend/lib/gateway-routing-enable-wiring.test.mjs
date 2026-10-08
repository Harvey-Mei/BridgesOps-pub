import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

const gatewayPage = readFileSync(new URL("../app/gateway-page.tsx", import.meta.url), "utf8")
const keysView = readFileSync(new URL("../components/gateway/gateway-keys-view.tsx", import.meta.url), "utf8")
const candidateDialog = readFileSync(new URL("../components/gateway/candidate-groups-dialog.tsx", import.meta.url), "utf8")
const membershipPicker = readFileSync(new URL("../components/gateway/candidate-group-membership-picker.tsx", import.meta.url), "utf8")

test("candidate management forwards the one-click routing enable action", () => {
  assert.match(gatewayPage, /onEnableRoutingGroup=\{\(candidate\) => \{ void enableRoutingGroup\(candidate\) \}\}/)
  assert.match(gatewayPage, /isRoutingGroupEnableSaving=\{isRoutingGroupEnableSaving\}/)
  assert.match(keysView, /<CandidateGroupsDialog[\s\S]*onEnableRoutingGroup=\{onEnableRoutingGroup\}[\s\S]*isRoutingGroupEnableSaving=\{isRoutingGroupEnableSaving\}/)
  assert.match(candidateDialog, /<CandidateGroupMembershipPicker[\s\S]*onEnableRoutingGroup=\{onEnableRoutingGroup\}[\s\S]*isRoutingGroupEnableSaving=\{isRoutingGroupEnableSaving\}/)
})

test("only cooling candidate rows expose a guarded enable button", () => {
  assert.match(membershipPicker, /routingEnableCandidate\?\.cooling_down \? \(/)
  assert.match(membershipPicker, /gatewayRoutingEnableEligible\(routingEnableCandidate\)/)
  assert.match(membershipPicker, /disabled=\{routingEnableDisabled\}/)
  assert.match(membershipPicker, /onClick=\{\(\) => onEnableRoutingGroup\(routingEnableCandidate\)\}/)
  assert.match(membershipPicker, /\{routingEnableSaving \? "启用中" : "一键启用"\}/)
})
