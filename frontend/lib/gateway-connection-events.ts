import type { GatewayRequestLog } from "@/lib/api-types"

export type GatewayConnectionEvent =
  | "client_disconnect"
  | "upstream_interrupted"
  | "dispatch_recovery"

const knownConnectionEvents = new Set<GatewayConnectionEvent>([
  "client_disconnect",
  "upstream_interrupted",
  "dispatch_recovery",
])

/**
 * Returns an observability-only connection event. New API values take
 * precedence; the fallback keeps older request rows useful until they age
 * out of the usage-log retention window.
 */
export function gatewayRequestConnectionEvent(log: Pick<GatewayRequestLog, "connection_event" | "route_reason" | "status_code" | "upstream_accepted" | "terminal_failure_code" | "error">): GatewayConnectionEvent | null {
  const explicit = String(log.connection_event ?? "").trim().toLowerCase() as GatewayConnectionEvent
  if (knownConnectionEvents.has(explicit)) return explicit

  const reason = String(log.route_reason ?? "").trim().toLowerCase()
  const failure = String(log.terminal_failure_code ?? "").trim().toLowerCase()
  const error = String(log.error ?? "").trim().toLowerCase()

  if (reason === "dispatch_recovery" || failure === "dispatch_recovery") return "dispatch_recovery"
  if (reason === "client_canceled" || log.status_code === 499 || error === "context canceled" || error === "客户端取消") {
    return "client_disconnect"
  }
  if (
    log.upstream_accepted === true &&
    log.status_code >= 500 &&
    (failure === "transient_after_acceptance" ||
      failure === "upstream_5xx" ||
      error.includes("stream read failed after upstream acceptance") ||
      error.includes("stream disconnected before completion"))
  ) {
    return "upstream_interrupted"
  }
  return null
}

export function gatewayConnectionEventLabel(event: GatewayConnectionEvent): string {
  switch (event) {
    case "client_disconnect":
      return "客户端断开 / 可能重连"
    case "upstream_interrupted":
      return "上游断流"
    case "dispatch_recovery":
      return "调度恢复"
  }
}

export function gatewayConnectionEventDetail(event: GatewayConnectionEvent): string {
  switch (event) {
    case "client_disconnect":
      return "下游连接被客户端关闭，可能是客户端重连，也可能是用户主动取消；不计为渠道故障。"
    case "upstream_interrupted":
      return "上游已受理请求后连接中断；为避免重复请求，BridgeOps 未重放该请求。"
    case "dispatch_recovery":
      return "服务中断后无法确认原请求完成状态，已写入调度恢复记录。"
  }
}

export function gatewayConnectionEventClassName(event: GatewayConnectionEvent): string {
  switch (event) {
    case "client_disconnect":
      return "border-border bg-muted/30 text-muted-foreground"
    case "upstream_interrupted":
      return "border-destructive/30 bg-destructive/5 text-destructive"
    case "dispatch_recovery":
      return "border-warning/40 bg-warning/5 text-warning"
  }
}
