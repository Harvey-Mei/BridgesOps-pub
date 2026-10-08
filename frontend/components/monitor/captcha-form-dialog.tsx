"use client"

import { useEffect, useState, type FormEvent } from "react"
import {
  Dialog,
  DialogContent,
  DialogBody,
  DialogError,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Button } from "@/components/ui/button"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import type { CaptchaConfig, CaptchaProviderType } from "@/lib/api-types"
import { apiFetch } from "@/lib/api"
import { useTriggerRefresh } from "@/lib/refresh-context"

interface CaptchaFormDialogProps {
  open: boolean
  onOpenChange: (v: boolean) => void
  config?: CaptchaConfig | null
}

interface FormState {
  name: string
  type: CaptchaProviderType
  api_key: string
  endpoint: string
  enabled: boolean
  proxy_enabled: boolean
}

function initialState(c?: CaptchaConfig | null): FormState {
  return {
    name: c?.name ?? "",
    type: c?.type ?? "2captcha",
    api_key: "",
    endpoint: c?.endpoint ?? "",
    enabled: c?.enabled ?? true,
    proxy_enabled: c?.proxy_enabled ?? false,
  }
}

export function CaptchaFormDialog({ open, onOpenChange, config }: CaptchaFormDialogProps) {
  const [form, setForm] = useState<FormState>(() => initialState(config))
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const refresh = useTriggerRefresh()

  useEffect(() => {
    if (open) {
      setForm(initialState(config))
      setError(null)
    }
  }, [open, config])

  const isEdit = !!config

  async function handleSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault()
    setError(null)
    setSubmitting(true)
    try {
      const body: Record<string, unknown> = {
        name: form.name,
        type: form.type,
        endpoint: form.endpoint,
        enabled: form.enabled,
        proxy_enabled: form.proxy_enabled,
      }
      if (form.api_key) body.api_key = form.api_key
      if (isEdit) {
        if (!form.api_key && !config!.id) {
          // shouldn't happen, defensive
          throw new Error("缺少 API Key")
        }
        await apiFetch(`/captcha-configs/${config!.id}`, {
          method: "PUT",
          body: JSON.stringify(body),
        })
      } else {
        if (!form.api_key) {
          throw new Error("新建时必须填写 API Key")
        }
        await apiFetch(`/captcha-configs`, {
          method: "POST",
          body: JSON.stringify(body),
        })
      }
      onOpenChange(false)
      refresh()
    } catch (e) {
      const err = e as Error
      setError(err.message || "保存失败")
    } finally {
      setSubmitting(false)
    }
  }

  // 不同 provider 的提示文案
  const placeholders: Record<CaptchaProviderType, { key: string; endpoint?: string }> = {
    "2captcha": { key: "2Captcha clientKey", endpoint: "https://api.2captcha.com（留空走默认）" },
    capsolver: { key: "CapSolver clientKey", endpoint: "https://api.capsolver.com（留空走默认）" },
    anticaptcha: { key: "Anti-Captcha clientKey", endpoint: "https://api.anti-captcha.com（留空走默认）" },
    yescaptcha: { key: "YesCaptcha clientKey", endpoint: "https://api.yescaptcha.com（留空走默认）" },
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange} busy={submitting}>
      <DialogContent formLayout className="sm:max-w-xl">
        <DialogHeader>
          <DialogTitle>{isEdit ? `${config.name} · 验证码服务` : "新增验证码服务"}</DialogTitle>
          <DialogDescription>
            用于处理站点人机验证，siteKey 自动获取。
          </DialogDescription>
        </DialogHeader>

        <form onSubmit={handleSubmit}>
          <DialogBody className="space-y-4">
          <div className="grid gap-4 sm:grid-cols-2">
          <div className="space-y-1.5">
            <Label htmlFor="captcha-name">配置名</Label>
            <Input
              id="captcha-name"
              placeholder="例如：CapSolver-主账号"
              value={form.name}
              onChange={(e) => setForm({ ...form, name: e.target.value })}
              required
              disabled={submitting}
            />
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="captcha-type">平台</Label>
            <Select
              value={form.type}
              onValueChange={(v) => setForm({ ...form, type: v as CaptchaProviderType })}
              disabled={submitting}
            >
              <SelectTrigger id="captcha-type" className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="2captcha">2Captcha</SelectItem>
                <SelectItem value="capsolver">CapSolver</SelectItem>
                <SelectItem value="anticaptcha">AntiCaptcha</SelectItem>
                <SelectItem value="yescaptcha">YesCaptcha</SelectItem>
              </SelectContent>
            </Select>
          </div>

          </div>

          <div className="space-y-1.5">
            <Label htmlFor="captcha-key">
              API Key {isEdit ? <span className="text-xs text-muted-foreground">(留空则不变)</span> : null}
            </Label>
            <Input
              id="captcha-key"
              type="password"
              placeholder={placeholders[form.type].key}
              value={form.api_key}
              onChange={(e) => setForm({ ...form, api_key: e.target.value })}
              required={!isEdit}
              disabled={submitting}
            />
            <p className="text-[11px] text-muted-foreground">{"从打码平台后台获取，加密存储后不会回显。"}</p>
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="captcha-endpoint">Endpoint（可选）</Label>
            <Input
              id="captcha-endpoint"
              placeholder={placeholders[form.type].endpoint ?? ""}
              value={form.endpoint}
              onChange={(e) => setForm({ ...form, endpoint: e.target.value })}
              disabled={submitting}
            />
          </div>

          <div className="flex items-start justify-between gap-3 rounded-lg border border-border px-3 py-2">
            <div>
              <p className="text-sm font-medium">启用</p>
              <p className="text-xs text-muted-foreground">关闭后渠道无法使用此 provider 求解</p>
            </div>
            <Switch
              checked={form.enabled}
              onCheckedChange={(v) => setForm({ ...form, enabled: v })}
              disabled={submitting}
            />
          </div>

          <div className="flex items-start justify-between gap-3 rounded-lg border border-border px-3 py-2">
            <div>
              <p className="text-sm font-medium">启用代理 IP</p>
              <p className="text-xs text-muted-foreground">全局代理启用后，此打码平台请求走系统代理配置</p>
            </div>
            <Switch
              checked={form.proxy_enabled}
              onCheckedChange={(v) => setForm({ ...form, proxy_enabled: v })}
              disabled={submitting}
            />
          </div>



          </DialogBody>
          {error ? <DialogError>{error}</DialogError> : null}
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)} disabled={submitting}>
              取消
            </Button>
            <Button type="submit" disabled={submitting}>
              {submitting ? "保存中…" : "保存"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}
