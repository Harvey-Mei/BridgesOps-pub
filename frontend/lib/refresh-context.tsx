"use client"

import {
  createContext,
  useContext,
  useState,
  useCallback,
  useEffect,
  type ReactNode,
} from "react"

interface RefreshContextValue {
  tick: number
  refreshedAt: number
  bump: () => void
}

const RefreshContext = createContext<RefreshContextValue>({
  tick: 0,
  refreshedAt: 0,
  bump: () => {},
})

/** 全局后台轮询周期；后端 cron 是分钟级，这里 30s 已足够"显得活着"。 */
const POLL_INTERVAL_MS = 30_000

export function RefreshProvider({ children }: { children: ReactNode }) {
  const [tick, setTick] = useState(0)
  const [refreshedAt, setRefreshedAt] = useState(0)
  const bump = useCallback(() => {
    setRefreshedAt(Date.now())
    setTick((t) => t + 1)
  }, [])

  // 30 秒静默 polling。页面在后台标签时（document.hidden）不轮询，避免后台浪费请求。
  useEffect(() => {
    const id = setInterval(() => {
      if (typeof document !== "undefined" && document.hidden) return
      bump()
    }, POLL_INTERVAL_MS)
    return () => clearInterval(id)
  }, [bump])

  return (
    <RefreshContext.Provider value={{ tick, refreshedAt, bump }}>
      {children}
    </RefreshContext.Provider>
  )
}

/** useRefreshTick 在 tick 变化时让组件重新拉数据。 */
export function useRefreshTick() {
  return useContext(RefreshContext).tick
}

/** useRefreshTimestamp returns the instant associated with the latest global refresh. */
export function useRefreshTimestamp() {
  return useContext(RefreshContext).refreshedAt
}

/** useTriggerRefresh 返回手动 bump 的方法，比如点头部的"刷新"按钮。 */
export function useTriggerRefresh() {
  return useContext(RefreshContext).bump
}
