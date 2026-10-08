import { Component, lazy, StrictMode, Suspense, type ComponentType, type ErrorInfo, type ReactNode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import '@fontsource-variable/geist'
import '@fontsource-variable/geist-mono'
import { ThemeProvider } from '@/components/theme-provider'
import { AuthProvider } from '@/lib/auth-context'
import { RefreshProvider } from '@/lib/refresh-context'
import { AddChannelProvider } from '@/lib/add-channel-context'
import { AuthGate } from '@/components/auth/auth-gate'
import { AppShell } from '@/components/app-shell'
import { Toaster } from '@/components/ui/sonner'
import { ThemeFaviconSync } from '@/components/theme-favicon-sync'
import { gatewayUsageTabRoute } from '@/lib/gateway-usage-tabs'
import '@/app/globals.css'

const DashboardPage = lazyPage(() => import('@/app/page'))
const ActivityPage = lazyPage(() => import('@/app/activity-page'))
const ChannelMonitorPage = lazyPage(() => import('@/app/channel-monitor-page'))
const ChannelsPage = lazyPage(() => import('@/app/channels-page'))
const CaptchaPage = lazyPage(() => import('@/app/captcha-page'))
const GatewayPage = lazyPage(() => import('@/app/gateway-page'))
const UpstreamGroupChangesPage = lazyPage(() => import('@/app/upstream-group-changes-page'))
const InsightsPage = lazyPage(() => import('@/app/insights-page'))
const IntelligenceRadarPage = lazyPage(() => import('@/app/intelligence-radar-page'))
const ModelCheckPage = lazyPage(() => import('@/app/model-check-page'))
const NetworkProxyPage = lazyPage(() => import('@/app/network-proxy-page'))
const NotificationsPage = lazyPage(() => import('@/app/notifications-page'))
const SettingsPage = lazyPage(() => import('@/app/settings-page'))

const pageFallback = (
  <div className="flex min-h-80 items-center justify-center">
    <div className="h-8 w-8 animate-spin rounded-full border-2 border-muted border-t-primary" />
  </div>
)

function lazyPage<P extends object>(loader: () => Promise<{ default: ComponentType<P> }>) {
  return lazy(() =>
    loader().catch((error) => {
      if (shouldReloadForChunkError(error) && reloadOnceForChunkUpdate()) {
        return new Promise<{ default: ComponentType<P> }>(() => {})
      }
      throw error
    }),
  )
}

function shouldReloadForChunkError(error: unknown) {
  const message = error instanceof Error ? error.message : String(error ?? '')
  return /ChunkLoadError|Loading chunk|Failed to fetch dynamically imported module|Importing a module script failed|error loading dynamically imported module/i.test(message)
}

function reloadOnceForChunkUpdate() {
  if (typeof window === 'undefined') return false
  const key = 'bridgeops:chunk-reload-at'
  const now = Date.now()
  const last = Number(window.sessionStorage.getItem(key) ?? 0)
  if (Number.isFinite(last) && now - last < 30_000) return false
  window.sessionStorage.setItem(key, String(now))
  window.location.reload()
  return true
}

class AppErrorBoundary extends Component<
  { children: ReactNode },
  { error: unknown | null }
> {
  state: { error: unknown | null } = { error: null }

  static getDerivedStateFromError(error: unknown) {
    return { error }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('App render failed', error, info)
  }

  render() {
    if (!this.state.error) return this.props.children
    const message = this.state.error instanceof Error ? this.state.error.message : String(this.state.error ?? '未知错误')
    return (
      <div className="min-h-screen bg-background p-6 text-foreground">
        <section className="mx-auto mt-16 max-w-2xl rounded-2xl border border-destructive/30 bg-destructive/5 p-6 text-sm">
          <div className="text-base font-semibold text-destructive">页面加载失败</div>
          <p className="mt-2 text-muted-foreground">
            前端资源可能刚刚更新，或当前页面数据触发了渲染异常。请先刷新页面；如果仍失败，把下面的错误信息发给维护者。
          </p>
          <pre className="mt-4 max-h-56 overflow-auto rounded-lg border border-border bg-background p-3 text-xs text-muted-foreground">
            {message}
          </pre>
          <button
            type="button"
            className="mt-4 rounded-md bg-foreground px-3 py-2 text-xs font-medium text-background"
            onClick={() => window.location.reload()}
          >
            刷新页面
          </button>
        </section>
      </div>
    )
  }
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <ThemeProvider attribute="class" defaultTheme="light" enableSystem disableTransitionOnChange>
      <ThemeFaviconSync />
      <AppErrorBoundary>
        <AuthProvider>
          <AuthGate>
            <RefreshProvider>
              <BrowserRouter>
                <AddChannelProvider>
                  <Suspense fallback={pageFallback}>
                    <Routes>
                      <Route element={<AppShell />}>
                        <Route index element={<DashboardPage />} />
                        <Route path="activity" element={<ActivityPage />} />
                        <Route path="channel-monitor" element={<ChannelMonitorPage />} />
                        <Route path="channels" element={<ChannelsPage />} />
                        <Route path="insights" element={<InsightsPage />} />
                        <Route path="model-check" element={<ModelCheckPage />} />
                        <Route path="network-proxies" element={<NetworkProxyPage />} />
                        <Route path="radar" element={<Navigate to="/radar/codex" replace />} />
                        <Route path="radar/codex" element={<IntelligenceRadarPage tab="codex" />} />
                        <Route path="radar/claude" element={<IntelligenceRadarPage tab="claude" />} />
                        <Route path="codex-radar" element={<Navigate to="/radar/codex" replace />} />
                        <Route path="claude-radar" element={<Navigate to="/radar/claude" replace />} />
                        <Route path="gateway" element={<Navigate to="/gateway/keys" replace />} />
                        <Route path="gateway/keys" element={<GatewayPage view="keys" />} />
                        <Route path="gateway/upstream-changes" element={<UpstreamGroupChangesPage />} />
                        <Route path={gatewayUsageTabRoute("requests")} element={<GatewayPage view="usage" usageTab="requests" />} />
                        <Route path={gatewayUsageTabRoute("group-cache")} element={<GatewayPage view="usage" usageTab="group-cache" />} />
                        <Route path="gateway/costs" element={<GatewayPage view="costs" />} />
                        <Route path="gateway/config" element={<GatewayPage view="config" />} />
                        <Route path="captcha" element={<CaptchaPage />} />
                        <Route path="notifications" element={<NotificationsPage />} />
                        <Route path="settings" element={<SettingsPage />} />
                      </Route>
                    </Routes>
                  </Suspense>
                </AddChannelProvider>
              </BrowserRouter>
            </RefreshProvider>
            <Toaster richColors closeButton position="top-right" />
          </AuthGate>
        </AuthProvider>
      </AppErrorBoundary>
    </ThemeProvider>
  </StrictMode>,
)
