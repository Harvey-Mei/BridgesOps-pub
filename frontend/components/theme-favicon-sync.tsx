"use client"

import { useEffect } from "react"
import { useTheme } from "next-themes"

const LIGHT_ICON = "/icon.svg?v=bridgeops-icon-8"
const DARK_ICON = "/icon-dark.svg?v=bridgeops-icon-8"

export function ThemeFaviconSync() {
  const { resolvedTheme } = useTheme()

  useEffect(() => {
    const dark = resolvedTheme === "dark"
    const icon = dark ? DARK_ICON : LIGHT_ICON
    document.querySelectorAll<HTMLLinkElement>("link[rel~='icon'], link[rel='shortcut icon'], link[rel='apple-touch-icon']").forEach((link) => {
      link.href = icon
    })
    const mask = document.querySelector<HTMLLinkElement>("link[rel='mask-icon']")
    if (mask) mask.setAttribute("color", dark ? "#b3cff0" : "#4774aa")
    const themeColor = document.querySelector<HTMLMetaElement>("meta[name='theme-color']")
    if (themeColor) themeColor.content = dark ? "#171e28" : "#f5f7fa"
  }, [resolvedTheme])

  return null
}
