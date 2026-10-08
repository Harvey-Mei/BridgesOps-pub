import { Radar } from "lucide-react"
import type { ComponentType } from "react"
import { NavLink } from "react-router-dom"
import ClaudeRadarPage from "@/app/claude-radar-page"
import CodexRadarPage from "@/app/codex-radar-page"
import { PageHeader } from "@/components/ui/page-header"
import { ClaudeIcon, OpenAIIcon } from "@/components/brand-icons"
import { cn } from "@/lib/utils"

type RadarTab = "codex" | "claude"

export default function IntelligenceRadarPage({ tab }: { tab: RadarTab }) {
  return (
    <div className="space-y-4">
      <PageHeader icon={Radar} title="智商雷达" description="对比 Codex 与 Claude 的模型能力、费用和耗时" actions={<nav className="flex w-full items-center rounded-lg border border-border bg-card p-1 shadow-sm sm:w-auto" aria-label="智商雷达数据源"><RadarTabLink to="/radar/codex" icon={OpenAIIcon} active={tab === "codex"}>Codex 雷达</RadarTabLink><RadarTabLink to="/radar/claude" icon={ClaudeIcon} active={tab === "claude"}>Claude 雷达</RadarTabLink></nav>} />
      {tab === "codex" ? <CodexRadarPage embedded /> : <ClaudeRadarPage embedded />}
    </div>
  )
}

function RadarTabLink({ to, icon: Icon, active, children }: { to: string; icon: ComponentType<{ className?: string }>; active: boolean; children: string }) {
  return <NavLink to={to} aria-current={active ? "page" : undefined} className={cn("inline-flex h-8 min-w-0 flex-1 items-center justify-center gap-1.5 whitespace-nowrap rounded-md px-2 text-sm font-medium transition-colors sm:flex-none sm:gap-2 sm:px-3", active ? "bg-primary text-primary-foreground shadow-sm" : "text-muted-foreground hover:bg-muted hover:text-foreground")}><Icon className="size-4 shrink-0" />{children}</NavLink>
}
