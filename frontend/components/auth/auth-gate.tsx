"use client"

import { Loader2 } from "lucide-react"
import { useAuth } from "@/lib/auth-context"
import { LoginPage } from "@/components/auth/login-page"
import { AuthLayout } from "@/components/auth/auth-layout"
import type { ReactNode } from "react"

/**
 * AuthGate 把根渲染分成三态：
 *   loading       本地有 token 但还没验完 — 显示占位
 *   anonymous     未登录 — 显示登录页
 *   authenticated 已登录 — 显示业务内容
 */
export function AuthGate({ children }: { children: ReactNode }) {
  const { status } = useAuth()

  if (status === "loading") {
    return <AuthLoadingPage />
  }
  if (status === "anonymous") {
    return <LoginPage />
  }
  return <>{children}</>
}

function AuthLoadingPage() {
  return (
    <AuthLayout>
      <section className="auth-card text-center" role="status" aria-live="polite">
        <Loader2 className="mx-auto size-6 animate-spin text-[var(--auth-muted)]" aria-hidden="true" />
        <h1 className="mt-5 text-lg font-semibold">正在验证会话</h1>
        <p className="mt-2 text-sm text-[var(--auth-muted)]">请稍候…</p>
      </section>
    </AuthLayout>
  )
}
