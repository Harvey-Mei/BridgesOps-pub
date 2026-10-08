import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

const dialogSource = readFileSync(new URL("../components/gateway/candidate-groups-dialog.tsx", import.meta.url), "utf8")

test("candidate group save warns but does not block when radar facts are not synced", () => {
  const warningBranch = dialogSource.match(
    /if \(!candidateGroupSelectionSupportsMappingFamily\(selectedCandidates, modelMappingFamily\)\) \{[\s\S]*?^    \}/m,
  )?.[0]

  assert.ok(warningBranch, "candidate group mapping warning branch is missing")
  assert.match(warningBranch, /toast\.warning\(/)
  assert.doesNotMatch(warningBranch, /\breturn\b/)
  assert.match(dialogSource, /expected_membership_version: editingID \? editingMembershipVersion : undefined/)
  assert.match(dialogSource, /body: JSON\.stringify\(\{[\s\S]*?members,[\s\S]*?\}\)/)
})
