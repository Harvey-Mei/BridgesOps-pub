import assert from "node:assert/strict"
import test from "node:test"

const placementModulePath = "./radar-tooltip-placement.ts"
const {
  calculateRadarTooltipPosition,
  radarTooltipAnchor,
  radarTooltipAnchorIsVisible,
  resolveRadarTooltipPlacement,
} = await import(placementModulePath)

test("chooses a side from the visible viewport and preserves the center dead zone", () => {
  assert.equal(resolveRadarTooltipPlacement(270, 100, 400, "left"), "right")
  assert.equal(resolveRadarTooltipPlacement(330, 100, 400, "right"), "left")
  assert.equal(resolveRadarTooltipPlacement(300, 100, 400, "right"), "right")
  assert.equal(resolveRadarTooltipPlacement(300, 100, 400, "left"), "left")
})

test("converts a wide chart coordinate into its scrolled viewport coordinate", () => {
  const viewport = { left: 100, top: 50, width: 400, height: 320 }
  const anchor = radarTooltipAnchor({ x: 600, y: 120 }, viewport, 480)

  assert.deepEqual(anchor, { x: 220, y: 170 })
  assert.equal(radarTooltipAnchorIsVisible(anchor!, viewport), true)
  assert.equal(radarTooltipAnchorIsVisible({ x: 580, y: 170 }, viewport), false)
})

test("keeps the portal inside the browser viewport", () => {
  assert.deepEqual(calculateRadarTooltipPosition({
    anchor: { x: 790, y: 590 },
    placement: "right",
    tooltip: { width: 336, height: 200 },
    windowWidth: 800,
    windowHeight: 600,
  }), { left: 448, top: 384 })
})
