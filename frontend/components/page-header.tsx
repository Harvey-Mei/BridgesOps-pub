import type { LucideIcon } from "lucide-react"
import type { ReactNode } from "react"
import { cn } from "@/lib/utils"

/** Shared page heading that keeps hierarchy visible without another full-width card. */
export function PageHeader({
  icon: Icon,
  title,
  description,
  actions,
  meta,
  className,
}: {
  icon?: LucideIcon
  title: string
  description?: string
  actions?: ReactNode
  meta?: ReactNode
  className?: string
}) {
  return (
    <header className={cn("flex min-w-0 flex-col gap-2 pb-0 sm:flex-row sm:items-center sm:justify-between", className)}>
      <div className="flex min-w-0 items-start gap-2.5">
        {Icon ? (
          <span className="mt-0.5 flex size-8 shrink-0 items-center justify-center rounded-lg border border-primary/15 bg-primary/10 text-primary">
            <Icon className="size-3.5" />
          </span>
        ) : null}
        <div className="min-w-0">
          <h1 className="truncate text-lg font-semibold tracking-tight text-foreground" title={title}>{title}</h1>
          {description ? <p className="mt-0.5 max-w-3xl text-xs leading-4 text-muted-foreground">{description}</p> : null}
        </div>
      </div>
      {actions || meta ? <div className="flex shrink-0 flex-wrap items-center gap-2">{actions}{meta}</div> : null}
    </header>
  )
}
