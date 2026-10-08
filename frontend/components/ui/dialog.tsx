import * as React from 'react'
import * as DialogPrimitive from '@radix-ui/react-dialog'
import { XIcon } from 'lucide-react'

import { cn } from '@/lib/utils'

const DialogBusyContext = React.createContext(false)

function Dialog({
  busy = false,
  onOpenChange,
  ...props
}: React.ComponentProps<typeof DialogPrimitive.Root> & { busy?: boolean }) {
  return <DialogBusyContext.Provider value={busy}>
    <DialogPrimitive.Root data-slot="dialog" {...props} onOpenChange={open => { if (!busy) onOpenChange?.(open) }} />
  </DialogBusyContext.Provider>
}

function DialogTrigger({
  ...props
}: React.ComponentProps<typeof DialogPrimitive.Trigger>) {
  return <DialogPrimitive.Trigger data-slot="dialog-trigger" {...props} />
}

function DialogPortal({
  ...props
}: React.ComponentProps<typeof DialogPrimitive.Portal>) {
  return <DialogPrimitive.Portal data-slot="dialog-portal" {...props} />
}

function DialogClose({
  disabled,
  ...props
}: React.ComponentProps<typeof DialogPrimitive.Close>) {
  const busy = React.useContext(DialogBusyContext)
  return <DialogPrimitive.Close data-slot="dialog-close" disabled={busy || disabled} {...props} />
}

function DialogOverlay({
  className,
  ...props
}: React.ComponentProps<typeof DialogPrimitive.Overlay>) {
  return (
    <DialogPrimitive.Overlay
      data-slot="dialog-overlay"
      className={cn(
        'data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 fixed inset-0 z-50 bg-black/50',
        className,
      )}
      {...props}
    />
  )
}

function DialogContent({
  className,
  children,
  showCloseButton = true,
  formLayout = false,
  onEscapeKeyDown,
  onInteractOutside,
  ...props
}: React.ComponentProps<typeof DialogPrimitive.Content> & {
  showCloseButton?: boolean
  formLayout?: boolean
}) {
  const busy = React.useContext(DialogBusyContext)
  return (
    <DialogPortal data-slot="dialog-portal">
      <DialogOverlay />
      <DialogPrimitive.Content
        data-slot="dialog-content"
        aria-busy={busy || undefined}
        onEscapeKeyDown={event => { if (busy) event.preventDefault(); onEscapeKeyDown?.(event) }}
        onInteractOutside={event => { if (busy) event.preventDefault(); onInteractOutside?.(event) }}
        className={cn(
          'bg-card data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95 fixed top-[50%] left-[50%] z-[51] grid max-h-[calc(100dvh-1rem)] w-full max-w-[calc(100%-1rem)] translate-x-[-50%] translate-y-[-50%] gap-4 overflow-y-auto rounded-2xl border p-4 shadow-lg duration-200 sm:max-h-[calc(100dvh-2rem)] sm:max-w-lg sm:p-6',
          formLayout && 'flex flex-col gap-0 overflow-hidden p-0 sm:p-0 [&>form]:flex [&>form]:min-h-0 [&>form]:flex-col [&>form]:overflow-hidden [&>[data-slot=dialog-header]]:shrink-0 [&>[data-slot=dialog-header]]:border-b [&>[data-slot=dialog-header]]:p-5 [&>[data-slot=dialog-header]]:pr-12 [&_[data-slot=dialog-footer]]:shrink-0 [&_[data-slot=dialog-footer]]:bg-card [&_[data-slot=dialog-footer]]:px-5 [&_[data-slot=dialog-footer]]:py-3',
          className,
        )}
        {...props}
      >
        {children}
        {showCloseButton && (
          <DialogPrimitive.Close
            data-slot="dialog-close"
            disabled={busy}
            className="ring-offset-background focus:ring-ring data-[state=open]:bg-accent data-[state=open]:text-muted-foreground absolute top-4 right-4 rounded-xs opacity-70 transition-opacity hover:opacity-100 focus:ring-2 focus:ring-offset-2 focus:outline-hidden disabled:pointer-events-none [&_svg]:pointer-events-none [&_svg]:shrink-0 [&_svg:not([class*='size-'])]:size-4"
          >
            <XIcon />
            <span className="sr-only">关闭窗口</span>
          </DialogPrimitive.Close>
        )}
      </DialogPrimitive.Content>
    </DialogPortal>
  )
}

function DialogBody({ className, ...props }: React.ComponentProps<'div'>) {
  return <div data-slot="dialog-body" className={cn('min-h-0 overflow-y-auto overscroll-contain p-5', className)} {...props} />
}

function DialogError({ className, ...props }: React.ComponentProps<'div'>) {
  return <div role="alert" className={cn('max-h-24 shrink-0 overflow-y-auto break-words border-t border-border px-5 py-3 text-sm text-destructive', className)} {...props} />
}

function DialogHeader({ className, ...props }: React.ComponentProps<'div'>) {
  return (
    <div
      data-slot="dialog-header"
      className={cn('flex flex-col gap-2 text-left pr-6', className)}
      {...props}
    />
  )
}

function DialogFooter({ className, ...props }: React.ComponentProps<'div'>) {
  return (
    <div
      data-slot="dialog-footer"
      className={cn(
        'flex flex-col-reverse gap-2 border-t border-border pt-4 sm:flex-row sm:justify-end',
        className,
      )}
      {...props}
    />
  )
}

function DialogTitle({
  className,
  ...props
}: React.ComponentProps<typeof DialogPrimitive.Title>) {
  return (
    <DialogPrimitive.Title
      data-slot="dialog-title"
      className={cn('text-lg leading-snug font-semibold break-words', className)}
      {...props}
    />
  )
}

function DialogDescription({
  className,
  ...props
}: React.ComponentProps<typeof DialogPrimitive.Description>) {
  return (
    <DialogPrimitive.Description
      data-slot="dialog-description"
      className={cn('text-muted-foreground text-sm', className)}
      {...props}
    />
  )
}

export {
  Dialog,
  DialogBody,
  DialogError,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogOverlay,
  DialogPortal,
  DialogTitle,
  DialogTrigger,
}
