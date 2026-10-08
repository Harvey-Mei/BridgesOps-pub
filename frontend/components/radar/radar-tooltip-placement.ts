import { useCallback, useRef, useState, type MouseEvent } from "react"

export type RadarTooltipPlacement = "left" | "right"
export type RadarTooltipPointerHandler = (event: MouseEvent<HTMLDivElement>) => void
export type RadarTooltipCoordinate = { x?: number; y?: number }
export type RadarTooltipViewport = { left: number; top: number; width: number; height: number }
export type RadarTooltipSize = { width: number; height: number }

const tooltipRightBoundary = 0.44
const tooltipLeftBoundary = 0.56
const tooltipGap = 12
const windowMargin = 16

export function resolveRadarTooltipPlacement(
  clientX: number,
  viewportLeft: number,
  viewportWidth: number,
  current: RadarTooltipPlacement,
): RadarTooltipPlacement {
  if (!Number.isFinite(clientX) || !Number.isFinite(viewportLeft) || !Number.isFinite(viewportWidth) || viewportWidth <= 0) return current
  const position = (clientX - viewportLeft) / viewportWidth
  if (position <= tooltipRightBoundary) return "right"
  if (position >= tooltipLeftBoundary) return "left"
  return current
}

export function radarTooltipAnchor(
  coordinate: RadarTooltipCoordinate,
  viewport: RadarTooltipViewport,
  scrollLeft: number,
): { x: number; y: number } | null {
  if (!Number.isFinite(coordinate.x) || !Number.isFinite(coordinate.y)) return null
  return {
    x: viewport.left + (coordinate.x as number) - scrollLeft,
    y: viewport.top + (coordinate.y as number),
  }
}

export function radarTooltipAnchorIsVisible(anchor: { x: number; y: number }, viewport: RadarTooltipViewport): boolean {
  return anchor.x >= viewport.left
    && anchor.x <= viewport.left + viewport.width
    && anchor.y >= viewport.top
    && anchor.y <= viewport.top + viewport.height
}

export function calculateRadarTooltipPosition({
  anchor,
  placement,
  tooltip,
  windowWidth,
  windowHeight,
}: {
  anchor: { x: number; y: number }
  placement: RadarTooltipPlacement
  tooltip: RadarTooltipSize
  windowWidth: number
  windowHeight: number
}): { left: number; top: number } {
  const maxLeft = Math.max(windowMargin, windowWidth - windowMargin - tooltip.width)
  const desiredLeft = placement === "right"
    ? anchor.x + tooltipGap
    : anchor.x - tooltipGap - tooltip.width
  const left = Math.min(Math.max(desiredLeft, windowMargin), maxLeft)

  const maxTop = Math.max(windowMargin, windowHeight - windowMargin - tooltip.height)
  const desiredTop = anchor.y - tooltip.height / 2
  const top = Math.min(Math.max(desiredTop, windowMargin), maxTop)
  return { left, top }
}

export function useRadarTooltipPlacement(): {
  tooltipPlacement: RadarTooltipPlacement
  onChartMouseMove: RadarTooltipPointerHandler
} {
  const [tooltipPlacement, setTooltipPlacement] = useState<RadarTooltipPlacement>("right")
  const tooltipPlacementRef = useRef<RadarTooltipPlacement>("right")
  const onChartMouseMove = useCallback<RadarTooltipPointerHandler>((event) => {
    const bounds = event.currentTarget.getBoundingClientRect()
    const nextPlacement = resolveRadarTooltipPlacement(event.clientX, bounds.left, bounds.width, tooltipPlacementRef.current)
    if (nextPlacement === tooltipPlacementRef.current) return
    tooltipPlacementRef.current = nextPlacement
    setTooltipPlacement(nextPlacement)
  }, [])
  return { tooltipPlacement, onChartMouseMove }
}
