import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

const dialog = readFileSync(new URL("../components/monitor/channel-api-keys-dialog.tsx", import.meta.url), "utf8")

test("channel API key dialog does not expose unsafe direct deletion", () => {
  assert.doesNotMatch(dialog, /method:\s*["']DELETE["']/)
  assert.doesNotMatch(dialog, /Trash2/)
  assert.match(dialog, /不提供直接删除/)
  assert.match(dialog, /上游分组变更/)
})
