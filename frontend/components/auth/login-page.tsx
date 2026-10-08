"use client"

import { useEffect, useState, type FormEvent } from "react"
import { ArrowRight, Eye, EyeOff, Loader2 } from "lucide-react"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Button } from "@/components/ui/button"
import { AuthLayout } from "@/components/auth/auth-layout"
import { useAuth } from "@/lib/auth-context"
import { useAppVersion } from "@/lib/queries"
import type { ApiError } from "@/lib/api"

const loginInputClassName = "h-[54px] rounded-xl border-[var(--auth-border)] bg-[var(--auth-control)] px-4 text-[var(--auth-foreground)] shadow-none placeholder:text-[var(--auth-muted)] focus-visible:border-[var(--auth-muted)] focus-visible:ring-[var(--auth-border)] dark:bg-[var(--auth-control)]"

export function LoginPage() {
  const { login } = useAuth()
  const appVersion = useAppVersion()
  const [username, setUsername] = useState("")
  const [password, setPassword] = useState("")
  const [showPassword, setShowPassword] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const appTitle = appVersion.data?.title?.trim() || "BridgeOps"

  useEffect(() => {
    document.title = appTitle
  }, [appTitle])

  async function handleSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault()
    setError(null)
    setSubmitting(true)
    try {
      await login(username.trim(), password)
    } catch (err) {
      const e = err as ApiError
      if (e.status === 401) {
        setError("账号或密码错误")
      } else {
        setError("暂时无法登录，请稍后重试")
      }
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <AuthLayout title={appTitle}>
      <section className="auth-card" aria-labelledby="login-title">
        <h1 id="login-title" className="sr-only">{`${appTitle} 登录`}</h1>
        <form onSubmit={handleSubmit} className="space-y-5" aria-busy={submitting}>
          <div className="space-y-2">
            <Label htmlFor="username" className="text-xs font-medium text-[var(--auth-muted)]">账号</Label>
            <Input
              id="username"
              name="username"
              autoComplete="username"
              autoCapitalize="none"
              placeholder="输入账号"
              className={loginInputClassName}
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              required
              disabled={submitting}
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="password" className="text-xs font-medium text-[var(--auth-muted)]">密码</Label>
            <div className="relative">
              <Input
                id="password"
                name="password"
                type={showPassword ? "text" : "password"}
                autoComplete="current-password"
                placeholder="输入密码"
                className={`${loginInputClassName} pr-12`}
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                disabled={submitting}
              />
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="absolute right-1.5 top-1/2 size-10 -translate-y-1/2 text-[var(--auth-muted)] hover:bg-[var(--auth-border)]/30 hover:text-[var(--auth-foreground)] dark:hover:bg-[var(--auth-border)]/30"
                onClick={() => setShowPassword((visible) => !visible)}
                aria-label={showPassword ? "隐藏密码" : "显示密码"}
                aria-pressed={showPassword}
                disabled={submitting}
              >
                {showPassword ? <EyeOff aria-hidden="true" /> : <Eye aria-hidden="true" />}
              </Button>
            </div>
          </div>
          {error ? (
            <p className="rounded-lg border border-danger/20 bg-danger/5 px-3 py-2.5 text-sm text-destructive dark:text-danger" role="alert">
              {error}
            </p>
          ) : null}
          <Button
            type="submit"
            className="h-[52px] w-full justify-between rounded-xl bg-[var(--auth-action)] px-4 font-semibold text-[var(--auth-action-foreground)] hover:bg-[var(--auth-action)] hover:brightness-95 disabled:opacity-40 has-[>svg]:px-4"
            disabled={submitting || !username.trim() || !password}
          >
            <span className="inline-flex items-center gap-2">
              {submitting ? <Loader2 className="size-4 animate-spin" aria-hidden="true" /> : null}
              {submitting ? "登录中…" : "登录"}
            </span>
            <ArrowRight className="size-4" aria-hidden="true" />
          </Button>
        </form>
      </section>
    </AuthLayout>
  )
}
