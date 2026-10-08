export type GatewayUsageTab = "requests" | "group-cache"

const gatewayUsageTabRoutes: Record<GatewayUsageTab, string> = {
  requests: "gateway/usage",
  "group-cache": "gateway/usage/group-cache",
}

export function gatewayUsageTabRoute(tab: GatewayUsageTab) {
  return gatewayUsageTabRoutes[tab]
}

export function gatewayUsageTabPath(tab: GatewayUsageTab) {
  return `/${gatewayUsageTabRoute(tab)}`
}

export function gatewayUsageRouteBoundaryPath(pathname: string) {
  const normalizedPathname = pathname.length > 1 ? pathname.replace(/\/+$/, "") : pathname
  if (normalizedPathname === gatewayUsageTabPath("requests") || normalizedPathname === gatewayUsageTabPath("group-cache")) {
    return gatewayUsageTabPath("requests")
  }
  return pathname
}
