"use client"

import { useEffect, useRef, useState } from "react"
import { apiFetch } from "@/lib/api"
import type {
  GatewayImageOperationTarget,
  GatewayImageOperationTargetInventoryPage,
} from "@/lib/api-types"
import { useRefreshTick } from "@/lib/refresh-context"

export const IMAGE_OPERATION_TARGET_PAGE_SIZE = 250
export const IMAGE_OPERATION_TARGET_MAXIMUM = 20_000
const IMAGE_OPERATION_TARGET_MAXIMUM_PAGES = 100

export type GatewayImageOperationTargetInventory = {
  items: GatewayImageOperationTarget[]
  snapshot: string
  capturedAt: string
}

export type GatewayImageOperationTargetInventoryState = {
  data: GatewayImageOperationTargetInventory | null
  loading: boolean
  refreshing: boolean
  error: string | null
  fresh: boolean
  refetch: () => void
}

type InventoryPageFetcher = (
  path: string,
  signal?: AbortSignal,
) => Promise<GatewayImageOperationTargetInventoryPage>

export async function loadGatewayImageOperationTargetInventory(
  fetchPage: InventoryPageFetcher = (path, signal) => apiFetch<GatewayImageOperationTargetInventoryPage>(path, { signal }),
  signal?: AbortSignal,
): Promise<GatewayImageOperationTargetInventory> {
  const items: GatewayImageOperationTarget[] = []
  const identities = new Set<string>()
  const cursors = new Set<string>()
  let cursor = ""
  let snapshot = ""
  let capturedAt = ""

  for (let pageNumber = 0; pageNumber < IMAGE_OPERATION_TARGET_MAXIMUM_PAGES; pageNumber += 1) {
    if (signal?.aborted) throw abortError()
    const query = new URLSearchParams({ page_size: String(IMAGE_OPERATION_TARGET_PAGE_SIZE) })
    if (cursor) query.set("cursor", cursor)
    const page = await fetchPage(`/gateway/image-operation-targets?${query.toString()}`, signal)
    if (!page || !Array.isArray(page.items) || page.items.length > IMAGE_OPERATION_TARGET_PAGE_SIZE) {
      throw new Error("生图目标清单分页响应无效")
    }
    const pageSnapshot = String(page.snapshot ?? "").trim()
    if (!pageSnapshot || (snapshot && snapshot !== pageSnapshot)) {
      throw new Error("生图目标清单在加载期间发生变化")
    }
    snapshot = pageSnapshot
    capturedAt = String(page.captured_at ?? "").trim()
    if (!capturedAt || Number.isNaN(Date.parse(capturedAt))) {
      throw new Error("生图目标清单缺少有效快照时间")
    }

    for (const item of page.items) {
      const identity = imageOperationTargetGenerationIdentity(item)
      if (!identity) throw new Error("生图目标清单包含无效代次身份")
      if (identities.has(identity)) throw new Error("生图目标清单包含重复目标")
      identities.add(identity)
      items.push(item)
      if (items.length > IMAGE_OPERATION_TARGET_MAXIMUM) {
        throw new Error("生图目标数量超过浏览器安全上限，请缩小后端清单范围")
      }
    }

    const nextCursor = String(page.next_cursor ?? "").trim()
    if (!nextCursor) {
      return { items: items.sort(compareImageOperationTargets), snapshot, capturedAt }
    }
    if (page.items.length === 0 || cursors.has(nextCursor)) {
      throw new Error("生图目标清单游标未向前推进")
    }
    cursors.add(nextCursor)
    cursor = nextCursor
  }
  throw new Error("生图目标清单分页超过安全上限")
}

export function useGatewayImageOperationTargetInventory(enabled = true): GatewayImageOperationTargetInventoryState {
  const [data, setData] = useState<GatewayImageOperationTargetInventory | null>(null)
  const [loading, setLoading] = useState(enabled)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [fresh, setFresh] = useState(false)
  const [bump, setBump] = useState(0)
  const refreshTick = useRefreshTick()
  const requestGeneration = useRef(0)

  useEffect(() => {
    const generation = ++requestGeneration.current
    if (!enabled) {
      setData(null)
      setLoading(false)
      setRefreshing(false)
      setError(null)
      setFresh(false)
      return
    }

    const controller = new AbortController()
    setError(null)
    setFresh(false)
    if (data) setRefreshing(true)
    else setLoading(true)
    void loadGatewayImageOperationTargetInventory(undefined, controller.signal)
      .then((inventory) => {
        if (generation !== requestGeneration.current) return
        setData(inventory)
        setFresh(true)
      })
      .catch((cause: unknown) => {
        if (generation !== requestGeneration.current || controller.signal.aborted) return
        setError(cause instanceof Error ? cause.message : "加载生图目标清单失败")
      })
      .finally(() => {
        if (generation !== requestGeneration.current) return
        setLoading(false)
        setRefreshing(false)
      })
    return () => controller.abort()
    // A completed inventory is deliberately not a dependency: refreshes keep
    // the previous snapshot read-only until the replacement is fully loaded.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [enabled, refreshTick, bump])

  return {
    data,
    loading,
    refreshing,
    error,
    fresh,
    refetch: () => {
      setFresh(false)
      setBump((current) => current + 1)
    },
  }
}

export function imageOperationTargetGenerationIdentity(item: GatewayImageOperationTarget) {
  const channelID = Number(item.channel_id)
  const upstreamGroupID = Number(item.upstream_group_id)
  const presenceGeneration = Number(item.presence_generation)
  const modelName = String(item.model_name ?? "").trim()
  if (!Number.isInteger(channelID) || channelID <= 0 || !Number.isInteger(upstreamGroupID) || upstreamGroupID <= 0 ||
    !Number.isInteger(presenceGeneration) || presenceGeneration <= 0 || !modelName) return ""
  return JSON.stringify([channelID, upstreamGroupID, presenceGeneration, modelName])
}

function compareImageOperationTargets(left: GatewayImageOperationTarget, right: GatewayImageOperationTarget) {
  return left.channel_name.localeCompare(right.channel_name, "zh-CN", { numeric: true, sensitivity: "base" })
    || left.channel_id - right.channel_id
    || left.group_name.localeCompare(right.group_name, "zh-CN", { numeric: true, sensitivity: "base" })
    || left.upstream_group_id - right.upstream_group_id
    || left.presence_generation - right.presence_generation
    || left.model_name.localeCompare(right.model_name, "en-US", { numeric: true, sensitivity: "base" })
}

function abortError() {
  return Object.assign(new Error("aborted"), { name: "AbortError" })
}
