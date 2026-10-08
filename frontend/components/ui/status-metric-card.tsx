import type { ReactNode } from "react"
import type { LucideIcon } from "lucide-react"
import { cn } from "@/lib/utils"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"

export type StatusMetricTone = "brand" | "success" | "warning" | "danger" | "muted"

export function StatusMetricCard({
  label,
  value,
  hint,
  icon: Icon,
  tone = "muted",
  valueClassName,
  onClick,
}: {
  label: string
  value: ReactNode
  hint: ReactNode
  icon: LucideIcon
  tone?: StatusMetricTone
  valueClassName?: string
  onClick?: () => void
}) {
  const hintTitle = typeof hint === "string" ? hint : undefined
  const valueTitle = typeof value === "string" || typeof value === "number" ? String(value) : undefined
  const accessibleLabel = [label, valueTitle, hintTitle].filter(Boolean).join("，")

  const card = (
    <div
      data-slot="status-metric"
      className={cn("flex min-h-[112px] min-w-0 flex-col items-center justify-between gap-3 rounded-xl border border-border bg-card px-5 py-5 text-center outline-none focus-visible:ring-2 focus-visible:ring-ring", onClick && "cursor-pointer transition-colors hover:border-foreground/30")}
      title={hintTitle}
      aria-label={accessibleLabel}
      role={onClick ? "button" : undefined}
      tabIndex={0}
      onClick={onClick}
      onKeyDown={onClick ? (event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault()
          onClick()
        }
      } : undefined}
    >
      <div className="flex w-full min-w-0 items-center justify-center gap-2">
        <span aria-hidden="true" className={cn("flex size-6 shrink-0 items-center justify-center rounded-md ring-1 ring-inset", iconClass(tone))}>
          <Icon className="size-3.5" />
        </span>
        <div className="min-w-0 truncate text-sm font-medium text-muted-foreground" title={label}>{label}</div>
      </div>
      <div
        className={cn("w-full min-w-0 truncate text-center font-semibold leading-none tracking-tight tabular-nums", valueClass(tone), valueClassName, "text-[30px] 2xl:text-[34px]")}
        title={valueTitle}
      >
        {value}
      </div>
    </div>
  )

  return hint ? (
    <Tooltip>
      <TooltipTrigger asChild>{card}</TooltipTrigger>
      <TooltipContent side="top" className="max-w-xs text-xs">
        <div className="font-medium">{label}：{value}</div>
        <div className="mt-0.5 opacity-80">{hint}</div>
      </TooltipContent>
    </Tooltip>
  ) : card
}

function iconClass(tone: StatusMetricTone) {
  if (tone === "brand") return "bg-accent text-brand ring-brand/10"
  if (tone === "success") return "bg-success/10 text-success ring-success/15"
  if (tone === "warning") return "bg-warning/10 text-warning ring-warning/15"
  if (tone === "danger") return "bg-danger/10 text-danger ring-danger/15"
  return "bg-muted text-muted-foreground ring-border"
}

function valueClass(tone: StatusMetricTone) {
  if (tone === "brand") return "text-foreground"
  if (tone === "success") return "text-success"
  if (tone === "warning") return "text-warning"
  if (tone === "danger") return "text-danger"
  return "text-foreground"
}
