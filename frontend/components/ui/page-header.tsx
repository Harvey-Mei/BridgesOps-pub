import type { ComponentType, ReactNode } from "react"

export function PageHeader({
  icon: Icon,
  title,
  description,
  actions,
}: {
  icon: ComponentType<{ className?: string }>
  title: string
  description: string
  actions?: ReactNode
}) {
  return (
    <header className="flex flex-col gap-3 px-1 sm:flex-row sm:items-center sm:justify-between">
      <div className="flex min-w-0 items-center gap-3.5">
        <Icon className="size-7 shrink-0 text-brand" />
        <div className="min-w-0">
          <h1 className="text-2xl font-semibold tracking-tight text-foreground 2xl:text-[28px]">{title}</h1>
          {description ? <p className="mt-1 text-sm text-muted-foreground">{description}</p> : null}
        </div>
      </div>
      {actions ? <div className="flex flex-wrap items-center gap-2">{actions}</div> : null}
    </header>
  )
}
