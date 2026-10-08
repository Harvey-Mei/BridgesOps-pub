import { getToken, notifyUnauthorized } from "@/lib/api"
import type { ModelCheckManualOption, ModelCheckRun } from "@/lib/api-types"

export interface ModelCheckProgress {
  type: string
  kind?: string
  at: string
  path?: string
  request_body?: string
  response_text?: string
  status_code?: number
  message?: string
  passed?: boolean
  latency_ms?: number
  total_tokens?: number
}

export async function streamModelCheck(option: ModelCheckManualOption, onProgress: (event: ModelCheckProgress) => void, signal: AbortSignal): Promise<ModelCheckRun> {
  const token = getToken()
  const response = await fetch("/api/model-checks/run", { method: "POST", signal, headers: { "Content-Type": "application/json", Accept: "text/event-stream", ...(token ? { Authorization: `Bearer ${token}` } : {}) }, body: JSON.stringify(option) })
  if (response.status === 401) notifyUnauthorized()
  if (!response.ok) {
    const body = await response.json().catch(() => ({}))
    throw new Error(body.error || `检测请求失败（HTTP ${response.status}）`)
  }
  if (!response.body) throw new Error("检测日志连接不可用")
  const reader = response.body.getReader(), decoder = new TextDecoder()
  let buffer = "", received = 0
  try {
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      received += value.byteLength
      if (received > 4 * 1024 * 1024) throw new Error("检测日志超过容量上限")
      buffer += decoder.decode(value, { stream: true })
      buffer = buffer.replace(/\r\n/g, "\n")
      if (buffer.length > 1024 * 1024) throw new Error("检测日志单条内容过大")
      let boundary: number
      while ((boundary = buffer.indexOf("\n\n")) >= 0) {
        const block = buffer.slice(0, boundary); buffer = buffer.slice(boundary + 2)
        const lines = block.split("\n")
        const event = lines.find(line => line.startsWith("event:"))?.slice(6).trim()
        const data = lines.filter(line => line.startsWith("data:")).map(line => line.slice(5).trimStart()).join("\n")
        if (!data) continue
        const payload = JSON.parse(data)
        if (event === "progress") onProgress(payload as ModelCheckProgress)
        else if (event === "error") throw new Error(payload.message || "检测失败")
        else if (event === "complete") return payload as ModelCheckRun
      }
    }
    throw new Error("检测连接已中断，结果尚未确认；请刷新画廊查看，不会自动重发")
  } finally { await reader.cancel().catch(() => {}); reader.releaseLock() }
}
