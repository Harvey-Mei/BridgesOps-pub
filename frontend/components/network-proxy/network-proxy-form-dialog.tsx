"use client"

import { useEffect, useState, type FormEvent } from "react"
import { toast } from "sonner"
import { Button } from "@/components/ui/button"
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import { apiFetch } from "@/lib/api"
import type { NetworkProxy, NetworkProxyInput, NetworkProxyProtocol } from "@/lib/api-types"

type ProxyFormState = {
  name: string
  protocol: NetworkProxyProtocol
  host: string
  port: string
  username: string
  password: string
  clearCredentials: boolean
  cleanIP: boolean
  enabled: boolean
}

function initialState(proxy?: NetworkProxy | null): ProxyFormState {
  return {
    name: proxy?.name ?? "",
    protocol: proxy?.protocol ?? "http",
    host: proxy?.host ?? "",
    port: proxy?.port != null ? String(proxy.port) : "",
    username: "",
    password: "",
    clearCredentials: false,
    cleanIP: proxy?.clean_ip ?? false,
    enabled: proxy?.enabled ?? true,
  }
}

export function NetworkProxyFormDialog({
  open,
  proxy,
  onOpenChange,
  onSaved,
}: {
  open: boolean
  proxy: NetworkProxy | null
  onOpenChange: (open: boolean) => void
  onSaved: () => void
}) {
  const [form, setForm] = useState<ProxyFormState>(() => initialState(proxy))
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const isEdit = proxy != null

  useEffect(() => {
    if (open) {
      setForm(initialState(proxy))
      setError(null)
    }
  }, [open, proxy])

  async function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    const port = Number(form.port)
    if (!form.name.trim()) {
      setError("请填写代理名称")
      return
    }
    if (!form.host.trim()) {
      setError("请填写主机地址")
      return
    }
    if (!Number.isInteger(port) || port < 1 || port > 65535) {
      setError("端口必须在 1 到 65535 之间")
      return
    }

    setError(null)
    setSaving(true)
    const body: NetworkProxyInput = {
      name: form.name.trim(),
      protocol: form.protocol,
      host: form.host.trim(),
      port,
      username: form.username,
      password: form.password,
      clear_credentials: isEdit && form.clearCredentials,
      clean_ip: form.cleanIP,
      enabled: form.enabled,
    }
    try {
      await apiFetch<NetworkProxy>(isEdit ? "/network-proxies/" + String(proxy.id) : "/network-proxies", {
        method: isEdit ? "PUT" : "POST",
        body: JSON.stringify(body),
      })
      toast.success(isEdit ? "代理已更新" : "代理已创建")
      onOpenChange(false)
      onSaved()
    } catch (cause) {
      const message = cause instanceof Error ? cause.message.trim() : ""
      setError(message && message.length <= 120 && !/:\/\//.test(message) ? message : "保存失败，请检查填写内容")
    } finally {
      setSaving(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange} busy={saving}>
      <DialogContent formLayout className="sm:max-w-xl">
        <DialogHeader>
          <DialogTitle>{isEdit ? `${proxy.name} · 网络代理` : "新建网络代理"}</DialogTitle>
          <DialogDescription className="sr-only">填写代理的地址、认证与可用状态</DialogDescription>
        </DialogHeader>
        <form onSubmit={save} >
          <DialogBody className="space-y-4">
          <div className="grid gap-3 sm:grid-cols-[1fr_9rem]">
            <div className="space-y-1.5">
              <Label htmlFor="network-proxy-name">名称</Label>
              <Input
                id="network-proxy-name"
                value={form.name}
                onChange={(event) => setForm({ ...form, name: event.target.value })}
                disabled={saving}
                required
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="network-proxy-protocol">协议</Label>
              <Select value={form.protocol} onValueChange={(value) => setForm({ ...form, protocol: value as NetworkProxyProtocol })} disabled={saving}>
                <SelectTrigger id="network-proxy-protocol" className="w-full"><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="http">HTTP</SelectItem>
                  <SelectItem value="https">HTTPS</SelectItem>
                  <SelectItem value="socks5">SOCKS5</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>

          <div className="grid gap-3 sm:grid-cols-[1fr_9rem]">
            <div className="space-y-1.5">
              <Label htmlFor="network-proxy-host">主机地址</Label>
              <Input
                id="network-proxy-host"
                value={form.host}
                onChange={(event) => setForm({ ...form, host: event.target.value })}
                disabled={saving}
                required
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="network-proxy-port">端口</Label>
              <Input
                id="network-proxy-port"
                type="number"
                min="1"
                max="65535"
                value={form.port}
                onChange={(event) => setForm({ ...form, port: event.target.value })}
                disabled={saving}
                required
              />
            </div>
          </div>

          <div className="grid gap-3 sm:grid-cols-2">
            <div className="space-y-1.5">
              <Label htmlFor="network-proxy-username">认证用户名</Label>
              <Input
                id="network-proxy-username"
                autoComplete="off"
                placeholder={isEdit ? "留空保持原认证" : undefined}
                value={form.username}
                onChange={(event) => setForm({ ...form, username: event.target.value })}
                disabled={saving || form.clearCredentials}
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="network-proxy-password">认证密码</Label>
              <Input
                id="network-proxy-password"
                type="password"
                autoComplete="new-password"
                placeholder={isEdit ? "留空保持原认证" : undefined}
                value={form.password}
                onChange={(event) => setForm({ ...form, password: event.target.value })}
                disabled={saving || form.clearCredentials}
              />
            </div>
          </div>

          {isEdit && proxy.auth_configured ? (
            <div className="flex items-center justify-between rounded-lg border border-danger/20 bg-danger/5 px-3 py-2">
              <Label htmlFor="network-proxy-clear-credentials" className="text-sm font-medium">清除认证</Label>
              <Switch
                id="network-proxy-clear-credentials"
                checked={form.clearCredentials}
                onCheckedChange={(clearCredentials) => setForm({ ...form, clearCredentials })}
                disabled={saving}
              />
            </div>
          ) : null}

          <div className="grid gap-3 sm:grid-cols-2">
            <div className="flex items-center justify-between rounded-lg border border-border px-3 py-2">
              <Label htmlFor="network-proxy-clean-ip" className="text-sm font-medium">标记为纯净 IP</Label>
              <Switch
                id="network-proxy-clean-ip"
                checked={form.cleanIP}
                onCheckedChange={(cleanIP) => setForm({ ...form, cleanIP })}
                disabled={saving}
              />
            </div>
            <div className="flex items-center justify-between rounded-lg border border-border px-3 py-2">
              <Label htmlFor="network-proxy-enabled" className="text-sm font-medium">启用</Label>
              <Switch
                id="network-proxy-enabled"
                checked={form.enabled}
                onCheckedChange={(enabled) => setForm({ ...form, enabled })}
                disabled={saving}
              />
            </div>
          </div>


          </DialogBody>
          {error ? <DialogError>{error}</DialogError> : null}
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)} disabled={saving}>取消</Button>
            <Button type="submit" disabled={saving}>{saving ? "保存中" : "保存"}</Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}
