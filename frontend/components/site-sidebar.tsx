"use client"

import { useEffect, useState, type ComponentProps, type ComponentType } from "react"
import { NavLink, useLocation } from "react-router-dom"
import { useTheme } from "next-themes"
import {
  Activity,
  BarChart3,
  BellRing,
  CircleDollarSign,
  ExternalLink,
  Gauge,
  Github,
  KeyRound,
  LineChart,
  LayoutDashboard,
  LogOut,
  Moon,
  Network,
  PlugZap,
  RefreshCw,
  Radar,
  Settings2,
  ShieldCheck,
  Bike,
  Sun,
  WalletCards,
} from "lucide-react"
import { toast } from "sonner"
import { apiFetch } from "@/lib/api"
import { useAuth } from "@/lib/auth-context"
import { useTriggerRefresh } from "@/lib/refresh-context"
import { useAppVersion } from "@/lib/queries"
import type { AppVersion } from "@/lib/api-types"
import { BrandMark } from "@/components/brand-mark"
import { Button } from "@/components/ui/button"
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarSeparator,
  SidebarTrigger,
} from "@/components/ui/sidebar"
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { cn } from "@/lib/utils"

type NavItem = {
  to: string
  label: string
  description: string
  icon: ComponentType<{ className?: string }>
  exact?: boolean
}

const workspaceNav: NavItem[] = [
  { to: "/", label: "总览", description: "关键状态与待处理事项", icon: LayoutDashboard, exact: true },
  { to: "/channels", label: "渠道管理", description: "渠道、登录、充值、倍率", icon: WalletCards },
  { to: "/channel-monitor", label: "渠道监控", description: "健康、余额、倍率、告警", icon: LineChart },
  { to: "/model-check", label: "鹈鹕画廊", description: "选择渠道、分组与模型，查看检测作品", icon: Bike },
  { to: "/activity", label: "事件中心", description: "告警、公告、倍率变动", icon: Activity },
]

const gatewayNav: NavItem[] = [
  { to: "/gateway/keys", label: "密钥管理", description: "Key、候选渠道、路由", icon: KeyRound },
  { to: "/gateway/upstream-changes", label: "上游变更", description: "上游分组变更与收敛", icon: BellRing },
  { to: "/gateway/usage", label: "使用记录", description: "请求、Token、路由", icon: BarChart3 },
  { to: "/gateway/costs", label: "费用统计", description: "费用、趋势、排行", icon: CircleDollarSign },
]

const analysisNav: NavItem[] = [
  { to: "/insights", label: "趋势分析", description: "余额、消费、倍率", icon: Gauge },
  { to: "/radar", label: "智商雷达", description: "Codex、Claude 能力与费用", icon: Radar },
]

const opsNav: NavItem[] = [
  { to: "/gateway/config", label: "参数配置", description: "模型价格、评分与健康", icon: Gauge },
  { to: "/notifications", label: "通知渠道", description: "告警出口与发送策略", icon: BellRing },
  { to: "/captcha", label: "打码平台", description: "验证码服务与可用性", icon: ShieldCheck },
  { to: "/network-proxies", label: "网络代理", description: "出口、测试与渠道绑定", icon: Network },
  { to: "/settings", label: "系统设置", description: "鉴权、调度、代理与保留", icon: Settings2 },
]

function isActivePath(pathname: string, to: string, exact = false) {
  if (exact) return pathname === to
  if (to === "/") return pathname === "/"
  return pathname === to || pathname.startsWith(`${to}/`)
}

function SidebarNavButton({ item }: { item: NavItem }) {
  const { pathname } = useLocation()
  const active = isActivePath(pathname, item.to, item.exact)
  const Icon = item.icon

  return (
    <SidebarMenuItem>
      <SidebarMenuButton
        asChild
        isActive={active}
        tooltip={`${item.label} · ${item.description}`}
        className="h-9 gap-2.5 rounded-lg px-2.5 text-sidebar-foreground/80 hover:bg-sidebar-accent data-[active=true]:bg-sidebar-accent data-[active=true]:text-sidebar-accent-foreground data-[active=true]:hover:bg-sidebar-accent data-[active=true]:hover:text-sidebar-accent-foreground data-[active=true]:data-[state=open]:hover:bg-sidebar-accent data-[active=true]:data-[state=open]:hover:text-sidebar-accent-foreground group-data-[collapsible=icon]:size-10! group-data-[collapsible=icon]:p-1!"
      >
        <NavLink to={item.to} end={item.exact} aria-label={item.label}>
          <span
            className={cn(
              "flex size-7 shrink-0 items-center justify-center rounded-[8px] border border-transparent bg-transparent text-sidebar-foreground group-data-[collapsible=icon]:size-8 group-data-[collapsible=icon]:border-0 group-data-[collapsible=icon]:bg-transparent",
              active && "text-sidebar-accent-foreground",
            )}
          >
            <Icon className="size-4 shrink-0 group-data-[collapsible=icon]:size-5" />
          </span>
          <span className="min-w-0 group-data-[collapsible=icon]:hidden">
            <span className="block truncate text-sm font-medium">{item.label}</span>
          </span>
        </NavLink>
      </SidebarMenuButton>
    </SidebarMenuItem>
  )
}

export function SiteSidebar() {
  return (
    <Sidebar collapsible="icon" variant="sidebar">
      <SidebarHeader className="px-3 pt-4 group-data-[collapsible=icon]:items-center group-data-[collapsible=icon]:px-2">
        <NavLink
          to="/"
          aria-label="返回总览"
          className="flex h-12 items-center gap-3 rounded-md px-2 transition-colors hover:bg-sidebar-accent group-data-[collapsible=icon]:size-9 group-data-[collapsible=icon]:justify-center group-data-[collapsible=icon]:p-0"
        >
          <BrandMark className="size-9 rounded-[10px] group-data-[collapsible=icon]:size-8" />
          <div className="min-w-0 group-data-[collapsible=icon]:hidden">
            <div className="truncate text-base font-semibold tracking-tight text-foreground">BridgeOps</div>
            <div className="truncate text-[11px] text-muted-foreground">管理控制台</div>
          </div>
        </NavLink>
      </SidebarHeader>
      <SidebarSeparator className="mx-4" />
      <SidebarContent className="gap-2 px-2 py-2">
        <SidebarGroup className="p-0">
          <SidebarGroupLabel className="h-6 px-3 text-[10px] font-medium tracking-[0.04em] text-muted-foreground">
            工作区
          </SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu className="gap-0.5">
              {workspaceNav.map((item) => (
                <SidebarNavButton key={item.to} item={item} />
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        <SidebarGroup className="p-0">
          <SidebarGroupLabel className="h-6 px-3 text-[10px] font-medium tracking-[0.04em] text-muted-foreground">
            API 网关
          </SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu className="gap-0.5">
              {gatewayNav.map((item) => (
                <SidebarNavButton key={item.to} item={item} />
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        <SidebarGroup className="p-0">
          <SidebarGroupLabel className="h-6 px-3 text-[10px] font-medium tracking-[0.04em] text-muted-foreground">
            数据分析
          </SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu className="gap-0.5">
              {analysisNav.map((item) => (
                <SidebarNavButton key={item.to} item={item} />
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        <SidebarGroup className="p-0">
          <SidebarGroupLabel className="h-6 px-3 text-[10px] font-medium tracking-[0.04em] text-muted-foreground">
            运维配置
          </SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu className="gap-0.5">
              {opsNav.map((item) => (
                <SidebarNavButton key={item.to} item={item} />
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
      <SidebarFooter className="px-3 pb-4 group-data-[collapsible=icon]:px-2">
        <SidebarSystemActions />
      </SidebarFooter>
    </Sidebar>
  )
}

function SidebarSystemActions() {
  const { theme, setTheme } = useTheme()
  const { username, authDisabled, logout } = useAuth()
  const refresh = useTriggerRefresh()
  const appVersion = useAppVersion()
  const [mounted, setMounted] = useState(false)
  const [syncing, setSyncing] = useState(false)
  const [checkingVersion, setCheckingVersion] = useState(false)
  const version = appVersion.data?.version?.trim()
  const latestVersion = appVersion.data?.latest_version?.trim()
  const updateAvailable = Boolean(appVersion.data?.update_available && latestVersion)
  const updateURL = appVersion.data?.release_url?.trim() || appVersion.data?.repo_url?.trim() || "https://github.com/Harvey-Mei/BridgesOps-pub"

  useEffect(() => setMounted(true), [])

  function handleRefresh() {
    setSyncing(true)
    refresh()
    window.setTimeout(() => setSyncing(false), 800)
  }

  async function handleCheckVersion() {
    setCheckingVersion(true)
    try {
      const result = await apiFetch<AppVersion>("/version?force=1")
      appVersion.setData(result)
      if (result.update_error) {
        toast.error(result.update_error)
      } else if (result.update_available && result.latest_version) {
        toast.warning(`发现新版本 ${result.latest_version}`)
      } else {
        toast.success("当前已是最新版本")
      }
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "检测更新失败")
    } finally {
      setCheckingVersion(false)
    }
  }

  return (
    <div className="space-y-2 border-t border-sidebar-border pt-3 text-xs group-data-[collapsible=icon]:border-0 group-data-[collapsible=icon]:pt-0">
      <div className="flex items-center justify-between gap-2 group-data-[collapsible=icon]:justify-center">
        <div className="min-w-0 group-data-[collapsible=icon]:hidden">
          <div className="truncate font-medium text-foreground">{username || "BridgeOps"}</div>
          <div className="truncate text-[11px] text-muted-foreground">{version ? `v${version}` : authDisabled ? "本地模式" : "已登录"}</div>
        </div>
        <SidebarTrigger className="size-8 rounded-[8px] border border-sidebar-border bg-background/70 text-muted-foreground hover:bg-muted hover:text-foreground" />
      </div>

      <div className="grid grid-cols-5 gap-1.5 group-data-[collapsible=icon]:grid-cols-1">
        <SidebarToolButton
          label="刷新"
          tooltip="刷新视图"
          onClick={handleRefresh}
          disabled={syncing}
          icon={<RefreshCw className={cn("size-3.5", syncing && "animate-spin")} />}
        />
        <SidebarToolButton
          label={checkingVersion ? "检测中" : updateAvailable ? `更新 ${latestVersion}` : version ? `v${version}` : "版本"}
          tooltip={updateAvailable ? `发现新版本 ${latestVersion}` : "检测更新"}
          onClick={handleCheckVersion}
          disabled={checkingVersion}
          className={updateAvailable ? "text-emerald-700 dark:text-emerald-300" : undefined}
          icon={<PlugZap className="size-3.5" />}
        />
        <SidebarToolButton
          label="GitHub"
          tooltip="GitHub 仓库"
          asChild
          icon={<Github className="size-3.5" />}
        >
          <a href="https://github.com/Harvey-Mei/BridgesOps-pub" target="_blank" rel="noopener noreferrer">
            <Github className="size-3.5" />
            <span className="sr-only">
              {"GitHub"}
              <ExternalLink className="size-3" />
            </span>
          </a>
        </SidebarToolButton>
        <SidebarToolButton
          label={mounted && theme === "dark" ? "深色" : "浅色"}
          tooltip={mounted && theme === "dark" ? "切换浅色模式" : "切换深色模式"}
          onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
          icon={mounted && theme === "dark" ? <Moon className="size-3.5" /> : <Sun className="size-3.5" />}
        />
        {authDisabled ? null : (
          <SidebarToolButton
            label="退出"
            tooltip={username ? `${username} · 退出登录` : "退出登录"}
            onClick={logout}
            icon={<LogOut className="size-3.5" />}

          />
        )}
      </div>

      {updateAvailable && updateURL ? (
        <a
          href={updateURL}
          target="_blank"
          rel="noopener noreferrer"
          className="block truncate rounded-md border border-emerald-200 bg-emerald-50 px-2.5 py-2 text-[11px] font-medium text-emerald-700 underline-offset-2 hover:underline dark:border-emerald-900/60 dark:bg-emerald-950/35 dark:text-emerald-300 group-data-[collapsible=icon]:hidden"
        >
          {`打开发布页 ${latestVersion}`}
        </a>
      ) : null}
    </div>
  )
}

function SidebarToolButton({
  label,
  tooltip,
  icon,
  className,
  asChild,
  children,
  ...props
}: {
  label: string
  tooltip: string
  icon: import("react").ReactNode
  className?: string
  asChild?: boolean
  children?: import("react").ReactNode
} & ComponentProps<typeof Button>) {
  return (
    <Tooltip delayDuration={200}>
      <TooltipTrigger asChild>
        <Button
          type={asChild ? undefined : "button"}
          variant="outline"
          size="sm"
          asChild={asChild}
          className={cn(
            "h-8 justify-center gap-1.5 rounded-[8px] border-sidebar-border bg-background/70 px-2 text-xs text-muted-foreground hover:bg-muted hover:text-foreground group-data-[collapsible=icon]:size-8 group-data-[collapsible=icon]:justify-center group-data-[collapsible=icon]:px-0",
            className,
          )}
          aria-label={tooltip || label}
          {...props}
        >
          {asChild && children ? (
            children
          ) : (
            <>
              {icon}
              <span className="sr-only">{label}</span>
            </>
          )}
        </Button>
      </TooltipTrigger>
      <TooltipContent side="right" className="text-xs">
        {tooltip}
      </TooltipContent>
    </Tooltip>
  )
}
