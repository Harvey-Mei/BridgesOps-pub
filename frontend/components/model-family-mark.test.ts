import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

test("model family mark renders only the icon while preserving an accessible label", () => {
  const source = readFileSync(new URL("./model-family-mark.tsx", import.meta.url), "utf8")
  const markSource = source.slice(source.indexOf("export function ModelFamilyMark"))

  assert.match(markSource, /role="img"/)
  assert.match(markSource, /aria-label=\{label\}/)
  assert.match(markSource, /<TooltipContent>\{label\}<\/TooltipContent>/)
  assert.doesNotMatch(markSource, /<span[^>]*>\s*\{label\}\s*<\/span>/)
})
