import { useLayoutEffect, useRef, useState, type ReactNode, type RefObject } from "react"
import { createPortal } from "react-dom"
import {
  calculateRadarTooltipPosition,
  radarTooltipAnchor,
  radarTooltipAnchorIsVisible,
  type RadarTooltipCoordinate,
  type RadarTooltipPlacement,
  type RadarTooltipSize,
} from "@/components/radar/radar-tooltip-placement"

const initialTooltipSize: RadarTooltipSize = { width: 336, height: 0 }

export function RadarTooltipPortal({
  active,
  coordinate,
  placement,
  viewportRef,
  children,
}: {
  active?: boolean
  coordinate?: RadarTooltipCoordinate
  placement: RadarTooltipPlacement
  viewportRef: RefObject<HTMLDivElement | null>
  children: ReactNode
}) {
  const contentRef = useRef<HTMLDivElement>(null)
  const [tooltipSize, setTooltipSize] = useState<RadarTooltipSize>(initialTooltipSize)

  useLayoutEffect(() => {
    if (!active || !contentRef.current) return
    const element = contentRef.current
    const measure = () => {
      const bounds = element.getBoundingClientRect()
      const next = { width: Math.ceil(bounds.width), height: Math.ceil(bounds.height) }
      setTooltipSize((current) => current.width === next.width && current.height === next.height ? current : next)
    }
    measure()
    if (typeof ResizeObserver === "undefined") return
    const observer = new ResizeObserver(measure)
    observer.observe(element)
    return () => observer.disconnect()
  }, [active])

  if (!active || !coordinate || typeof document === "undefined" || typeof window === "undefined") return null
  const viewportElement = viewportRef.current
  if (!viewportElement) return null

  const bounds = viewportElement.getBoundingClientRect()
  const viewport = { left: bounds.left, top: bounds.top, width: bounds.width, height: bounds.height }
  const anchor = radarTooltipAnchor(coordinate, viewport, viewportElement.scrollLeft)
  if (!anchor || !radarTooltipAnchorIsVisible(anchor, viewport)) return null

  const position = calculateRadarTooltipPosition({
    anchor,
    placement,
    tooltip: tooltipSize,
    windowWidth: window.innerWidth,
    windowHeight: window.innerHeight,
  })

  return createPortal(
    <div
      ref={contentRef}
      role="tooltip"
      className="pointer-events-none fixed z-[80] w-[min(21rem,calc(100vw-2rem))]"
      style={{
        left: position.left,
        top: position.top,
        visibility: tooltipSize.height > 0 ? "visible" : "hidden",
      }}
    >
      {children}
    </div>,
    document.body,
  )
}
