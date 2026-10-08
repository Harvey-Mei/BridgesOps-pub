"use client"

import { Component, useEffect, useState, type ErrorInfo, type ReactNode } from "react"
import { Outlet, useLocation } from "react-router-dom"
import { DockBar } from "@/components/monitor/dock-bar"
import {
  SidebarInset,
  SidebarProvider,
  SidebarTrigger,
} from "@/components/ui/sidebar"
import { SiteSidebar } from "@/components/site-sidebar"
import { useSystemConfig } from "@/lib/queries"
import { DEFAULT_DISPLAY_TIME_ZONE, getDisplayTimeZone, setDisplayTimeZone } from "@/lib/format"
import { gatewayUsageRouteBoundaryPath } from "@/lib/gateway-usage-tabs"
import { cn } from "@/lib/utils"

/**
 * AppShell 是所有路由共享的外壳：左侧导航 + 中间 Outlet（+ 可选底部 dock）。
 *
 * 当前 Dock 暂时隐藏 —— 单用户 / 少量数据下单页布局比拆页好。
 * 把 SHOW_DOCK 改成 true 即可恢复底部导航 + 路由跳转。
 */
const SHOW_DOCK = false

export function AppShell() {
  const location = useLocation()
  const settings = useSystemConfig()
  const [timeZone, setTimeZone] = useState(getDisplayTimeZone)

  useEffect(() => {
    if (!settings.data) return
    setTimeZone(setDisplayTimeZone(settings.data.effective_time_zone || DEFAULT_DISPLAY_TIME_ZONE))
  }, [settings.data])
  return (
    <SidebarProvider defaultOpen>
      <SiteSidebar />
      <SidebarInset>
        <div className="workspace-screen min-h-screen min-w-0 bg-background">
          <SidebarTrigger className="fixed left-3 top-3 z-40 size-9 rounded-[8px] border border-border bg-background/90 text-muted-foreground shadow-sm backdrop-blur hover:bg-muted hover:text-foreground md:hidden" />
          <div className={cn("min-w-0 space-y-4 pl-14 pr-4 py-4 sm:pl-14 sm:pr-5 sm:py-5 md:px-6 md:py-6 2xl:px-8 2xl:py-7", SHOW_DOCK && "pb-24")}>
            <RouteErrorBoundary
              key={`${gatewayUsageRouteBoundaryPath(location.pathname)}${location.search}:${timeZone}`}
              resetKey={`${location.pathname}${location.search}:${timeZone}`}
            >
              <div className="min-h-[calc(100vh-8rem)]">
                <Outlet />
              </div>
            </RouteErrorBoundary>
          </div>
          {SHOW_DOCK ? <DockBar /> : null}
        </div>
      </SidebarInset>
    </SidebarProvider>
  )
}

class RouteErrorBoundary extends Component<
  { children: ReactNode; resetKey: string },
  { error: unknown | null }
> {
  state: { error: unknown | null } = { error: null }

  static getDerivedStateFromError(error: unknown) {
    return { error }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error("Route render failed", error, info)
  }

  componentDidUpdate(previousProps: { children: ReactNode; resetKey: string }) {
    if (this.state.error && previousProps.resetKey !== this.props.resetKey) {
      this.setState({ error: null })
    }
  }

  render() {
    if (!this.state.error) return this.props.children
    const message = routeErrorMessage(this.state.error)
    return (
      <section className="rounded-lg border border-destructive/30 bg-destructive/5 p-6 text-sm">
        <div className="text-base font-semibold text-destructive">{"页面渲染失败"}</div>
        <p className="mt-2 text-muted-foreground">
          {"当前页面数据或前端资源异常，已阻止整页白屏。请刷新页面；如果仍失败，把下面的错误信息发给维护者。"}
        </p>
        <pre className="mt-4 max-h-56 overflow-auto rounded-lg border border-border bg-background p-3 text-xs text-muted-foreground">
          {message}
        </pre>
        <button
          type="button"
          className="mt-4 rounded-md bg-foreground px-3 py-2 text-xs font-medium text-background"
          onClick={() => window.location.reload()}
        >
          {"刷新页面"}
        </button>
      </section>
    )
  }
}

function routeErrorMessage(error: unknown) {
  if (error instanceof Error) return error.message
  return String(error || "未知错误")
}
