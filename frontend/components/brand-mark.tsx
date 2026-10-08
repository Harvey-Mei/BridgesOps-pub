"use client"

import { cn } from "@/lib/utils"
import { useTheme } from "next-themes"

export const BRAND_ICON_SRC = "/icon.svg?v=bridgeops-icon-8"
export const BRAND_ICON_DARK_SRC = "/icon-dark.svg?v=bridgeops-icon-8"

export function BrandMark({
  className,
  imgClassName,
  alt = "BridgeOps",
}: {
  className?: string
  imgClassName?: string
  alt?: string
}) {
  const { resolvedTheme } = useTheme()
  const iconSrc = resolvedTheme === "dark" ? BRAND_ICON_DARK_SRC : BRAND_ICON_SRC

  return (
    <span
      className={cn(
        "inline-flex shrink-0 items-center justify-center overflow-hidden rounded-[10px] bg-accent",
        className,
      )}
    >
      <img
        src={iconSrc}
        alt={alt}
        className={cn("size-full object-cover", imgClassName)}
        draggable={false}
      />
    </span>
  )
}
