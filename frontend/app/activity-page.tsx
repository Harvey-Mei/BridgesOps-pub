import { useState } from "react"
import { Activity, Bell, Megaphone, RefreshCw, TrendingUp } from "lucide-react"
import { AlertFeed, UpstreamAnnouncements } from "@/components/monitor/bottom-panels"
import { MultiplierChanges } from "@/components/monitor/multiplier-changes"
import { Button } from "@/components/ui/button"
import { PageHeader } from "@/components/ui/page-header"
import { useTriggerRefresh } from "@/lib/refresh-context"
import { cn } from "@/lib/utils"
import type { LucideIcon } from "lucide-react"

type EventView = "all" | "alerts" | "announcements" | "rates"

const views: Array<{ value: EventView; label: string; icon: LucideIcon }> = [
  { value: "all", label: "全部事件", icon: Activity },
  { value: "alerts", label: "告警", icon: Bell },
  { value: "announcements", label: "公告", icon: Megaphone },
  { value: "rates", label: "倍率变动", icon: TrendingUp },
]

export default function ActivityPage() {
  const [view, setView] = useState<EventView>("all")
  const triggerRefresh = useTriggerRefresh()

  return (
    <section className="space-y-4">
      <PageHeader icon={Activity} title="事件中心" description="告警发送、上游公告与倍率变动" actions={<Button type="button" variant="outline" size="icon-sm" onClick={triggerRefresh} aria-label="刷新事件" title="刷新事件"><RefreshCw className="size-3.5" /></Button>} />

      <div className="flex items-center gap-1 border-b border-border pb-2" role="group" aria-label="事件类型">
        {views.map(({ value, label, icon: Icon }) => (
          <button key={value} type="button" aria-pressed={view === value} onClick={() => setView(value)} className={cn("inline-flex h-8 items-center gap-1.5 rounded-md px-3 text-xs transition-colors", view === value ? "bg-primary text-primary-foreground shadow-sm" : "text-muted-foreground hover:bg-muted hover:text-foreground")}>
            <Icon className="size-3.5" />{label}
          </button>
        ))}
      </div>

      <div className={cn("grid gap-4", view === "all" && "xl:grid-cols-2")}>
        <div className={cn(view !== "all" && view !== "alerts" && "hidden")}><AlertFeed /></div>
        <div className={cn(view !== "all" && view !== "announcements" && "hidden")}><UpstreamAnnouncements /></div>
        <div className={cn(view !== "all" && view !== "rates" && "hidden", view === "all" && "xl:col-span-2")}><MultiplierChanges /></div>
      </div>
    </section>
  )
}
