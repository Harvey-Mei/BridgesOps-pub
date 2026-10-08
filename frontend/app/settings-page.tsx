import { HelpHint } from "@/components/ui/help-hint";
import { useEffect, useRef, useState } from "react";
import { toast } from "sonner";
import {
  Clock3,
  Database,
  Eye,
  EyeOff,
  MonitorCog,
  Network,
  RefreshCw,
  Server,
  Settings2,
  ShieldCheck,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { apiFetch } from "@/lib/api";
import { useTriggerRefresh } from "@/lib/refresh-context";
import type {
  AppVersion,
  ApplyConfigResult,
  SystemConfig,
} from "@/lib/api-types";
import { confirmDisplayTimeZone, stageDisplayTimeZone } from "@/lib/format";
import {
  useAppVersion,
  useSystemConfig,
} from "@/lib/queries";
import { cn } from "@/lib/utils";
import { NumberDraftInput } from "@/components/number-draft-input";
import { PageHeader } from "@/components/ui/page-header";

const commonDisplayTimeZones = [
  "Asia/Shanghai",
  "Asia/Hong_Kong",
  "Asia/Singapore",
  "Asia/Tokyo",
  "UTC",
  "Europe/London",
  "Europe/Berlin",
  "America/New_York",
  "America/Los_Angeles",
  "Australia/Sydney",
];
const intlTimeZones = (Intl as typeof Intl & { supportedValuesOf?: (key: "timeZone") => string[] })
  .supportedValuesOf?.("timeZone") ?? [];
const displayTimeZoneOptions = Array.from(new Set([
  ...commonDisplayTimeZones,
  ...intlTimeZones,
  Intl.DateTimeFormat().resolvedOptions().timeZone,
].filter(Boolean))).sort();

interface ProxyTestResult {
  ok: boolean;
  latency_ms: number;
  ip: string;
  provider: string;
  error?: string;
}

export default function SettingsPage() {
  const query = useSystemConfig();
  const appVersion = useAppVersion();
  const refresh = useTriggerRefresh();
  const [form, setForm] = useState<SystemConfig | null>(null);
  const [saving, setSaving] = useState(false);
  const [activeSection, setActiveSection] = useState("settings-app");
  const [applying, setApplying] = useState(false);
  const [configSavedPendingApply, setConfigSavedPendingApply] = useState(false);
  const [testingProxy, setTestingProxy] = useState(false);
  const [checkingVersion, setCheckingVersion] = useState(false);
  const [showAdminPassword, setShowAdminPassword] = useState(false);
  const [showTokenSecret, setShowTokenSecret] = useState(false);
  const [showProxyPassword, setShowProxyPassword] = useState(false);
  const [dirtyNumberFields, setDirtyNumberFields] = useState<Set<string>>(() => new Set());
  const dirtyNumberFieldsRef = useRef<Set<string>>(dirtyNumberFields);
  const [versionInfo, setVersionInfo] = useState<AppVersion | null>(null);
  const serverConfigSnapshot = useRef<string | null>(null);
  const savedFormSnapshot = useRef<string | null>(null);
  const currentFormRef = useRef<SystemConfig | null>(null);
  const acceptNextServerConfig = useRef(false);
  currentFormRef.current = form;
  dirtyNumberFieldsRef.current = dirtyNumberFields;

  useEffect(() => {
    if (query.data?.config) {
      const next = query.data.config;
      const snapshot = JSON.stringify(next);
      if (acceptNextServerConfig.current) {
        acceptNextServerConfig.current = false;
        serverConfigSnapshot.current = snapshot;
        const current = currentFormRef.current;
        const continuedEditing = dirtyNumberFieldsRef.current.size > 0
          || Boolean(current && savedFormSnapshot.current && JSON.stringify(current) !== savedFormSnapshot.current);
        if (!continuedEditing) {
          setForm(next);
          setDirtyNumberFields(new Set());
        }
        savedFormSnapshot.current = null;
        return;
      }
      setForm((current) => {
        const dirty = Boolean(
          dirtyNumberFieldsRef.current.size > 0
          || (
            current
            && serverConfigSnapshot.current
            && JSON.stringify(current) !== serverConfigSnapshot.current
          ),
        );
        if (dirty || (current && JSON.stringify(current) === snapshot)) return current;
        return next;
      });
      serverConfigSnapshot.current = snapshot;
    }
  }, [query.data]);

  useEffect(() => {
    if (appVersion.data) {
      setVersionInfo(appVersion.data);
    }
  }, [appVersion.data]);

  useEffect(() => {
    if (!form?.auth.password) setShowAdminPassword(false);
    if (!form?.auth.tokenSecret) setShowTokenSecret(false);
    if (!form?.proxy.password) setShowProxyPassword(false);
  }, [form?.auth.password, form?.auth.tokenSecret, form?.proxy.password]);

  if (query.loading && !form) {
    return (
      <section className="text-sm text-muted-foreground">加载配置中...</section>
    );
  }

  if (query.error && !form) {
    return (
      <section className="text-sm text-destructive">{query.error}</section>
    );
  }

  if (!form) return null;

  const currentForm = form;

  const systemConfigDirty = Boolean(
    dirtyNumberFields.size > 0
    || (serverConfigSnapshot.current && JSON.stringify(currentForm) !== serverConfigSnapshot.current),
  );

  async function handleSave() {
    const shouldSave = systemConfigDirty;
    if (!shouldSave && !configSavedPendingApply) return;
    setSaving(true);
    let saved = false;
    try {
      if (shouldSave) {
        await apiFetch("/settings/config/system", {
          method: "PATCH",
          body: JSON.stringify(currentForm),
        });
        stageDisplayTimeZone(currentForm.app.timeZone);
        setConfigSavedPendingApply(true);
        const savedSnapshot = JSON.stringify(currentForm);
        serverConfigSnapshot.current = savedSnapshot;
        savedFormSnapshot.current = savedSnapshot;
        setDirtyNumberFields(new Set());
        saved = true;
      }
      setApplying(true);
      const result = await apiFetch<ApplyConfigResult>("/settings/apply", {
        method: "POST",
      });
      confirmDisplayTimeZone(currentForm.app.timeZone);
      toast.success(result.message);
      setConfigSavedPendingApply(false);
      if (saved) acceptNextServerConfig.current = true;
      query.refetch();
      refresh();
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "保存并应用失败");
    } finally {
      setSaving(false);
      setApplying(false);
    }
  }

  function updateNumberDraftState(key: string, dirty: boolean) {
    setDirtyNumberFields((current) => {
      const next = new Set(current);
      if (dirty) next.add(key);
      else next.delete(key);
      if (next.size === current.size && Array.from(next).every((item) => current.has(item))) return current;
      return next;
    });
  }

  async function handleTestProxy() {
    setTestingProxy(true);
    try {
      const result = await apiFetch<ProxyTestResult>("/settings/proxy/test", {
        method: "POST",
        body: JSON.stringify(form?.proxy ?? {}),
      });
      if (result.ok) {
        toast.success(
          `代理可用，出口 IP ${result.ip}，延迟 ${result.latency_ms}ms`,
        );
      } else {
        toast.error(result.error ?? "代理测试失败");
      }
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "代理测试失败");
    } finally {
      setTestingProxy(false);
    }
  }

  async function handleCheckVersion() {
    setCheckingVersion(true);
    try {
      const result = await apiFetch<AppVersion>("/version?force=1");
      setVersionInfo(result);
      appVersion.setData(result);
      if (result.update_error) {
        toast.error(result.update_error);
      } else if (result.update_available && result.latest_version) {
        toast.warning(`发现新版本 ${result.latest_version}`);
      } else {
        toast.success("当前已是最新版本");
      }
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "检测更新失败");
    } finally {
      setCheckingVersion(false);
    }
  }

  function discardChanges() {
    const current = query.data?.config;
    if (!current) return;
    serverConfigSnapshot.current = JSON.stringify(current);
    savedFormSnapshot.current = null;
    setForm(current);
    setDirtyNumberFields(new Set());
  }

  return (
    <section className="space-y-4">
      <PageHeader icon={Settings2} title="系统设置" description="管理应用、鉴权、后台任务、数据保留和网络配置" />
      <div className="sticky top-0 z-30 flex flex-wrap items-center justify-between gap-3 rounded-lg border border-border bg-background/95 px-3 py-2.5 shadow-sm backdrop-blur">
        <nav className="flex flex-wrap items-center gap-1" aria-label="系统设置分区">
          {[
            ["settings-app", "应用"],
            ["settings-auth", "鉴权"],
            ["settings-jobs", "后台任务"],
            ["settings-retention", "数据保留"],
            ["settings-upstream", "上游请求"],
            ["settings-proxy", "系统服务代理"],
          ].map(([target, label]) => (
            <button key={target} type="button" aria-controls={target} aria-pressed={activeSection === target} onClick={() => setActiveSection(target)} className={cn("inline-flex h-9 items-center rounded-lg px-3 text-sm transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring", activeSection === target ? "bg-accent font-medium text-accent-foreground" : "text-muted-foreground hover:bg-muted hover:text-foreground")}>
              {label}
            </button>
          ))}
        </nav>
        <div className="flex min-w-0 flex-wrap items-center justify-end gap-2">
          <Badge role="status" aria-live="polite" variant={systemConfigDirty ? "secondary" : "outline"} className={configSavedPendingApply && !systemConfigDirty ? "border-amber-300 bg-amber-50 text-amber-700" : undefined}>
            {applying ? "正在应用" : saving ? "正在保存" : systemConfigDirty ? "有未保存更改" : configSavedPendingApply ? "应用失败待重试" : "已应用"}
          </Badge>
          <Button type="button" size="sm" variant="outline" onClick={systemConfigDirty ? discardChanges : query.refetch} disabled={saving || applying || query.loading || query.refreshing}>
            <RefreshCw className={query.loading || query.refreshing ? "size-3.5 animate-spin" : "size-3.5"} />
            {systemConfigDirty ? "放弃更改" : "刷新"}
          </Button>
          <Button type="button" size="sm" onClick={() => void handleSave()} disabled={saving || applying || (!systemConfigDirty && !configSavedPendingApply)}>
            {applying ? "应用中..." : "保存并应用"}
          </Button>
        </div>
      </div>
      <div className="space-y-4">
              <SectionCard
                id="settings-app"
                hidden={activeSection !== "settings-app"}
                icon={<MonitorCog className="size-4 text-primary" />}
                title="应用信息"
                description="控制页面标题和通知标题前缀。"
              >
                <div className="mb-4 flex flex-wrap items-center gap-2 text-xs">
                  <Badge variant="outline" className="border-border bg-background">
                    当前版本 {versionInfo?.version || "加载中"}
                  </Badge>
                  {versionInfo?.latest_version ? (
                    <Badge
                      variant="outline"
                      className={cn(
                        "border-transparent",
                        versionInfo.update_available
                          ? "bg-amber-50 text-amber-700"
                          : "bg-emerald-50 text-emerald-700",
                      )}
                    >
                      {versionInfo.update_available
                        ? `可更新 ${versionInfo.latest_version}`
                        : "已是最新"}
                    </Badge>
                  ) : null}
                  <Button
                    size="sm"
                    variant="outline"
                    className="h-7 border-border bg-background px-2 text-xs"
                    onClick={handleCheckVersion}
                    disabled={checkingVersion}
                  >
                    <RefreshCw
                      className={cn(
                        "size-3.5",
                        checkingVersion ? "animate-spin" : "",
                      )}
                    />
                    {checkingVersion ? "检测中..." : "检测更新"}
                  </Button>
                </div>
                <div className="grid gap-4 md:grid-cols-3">
                  <Field
                    label="应用标题"
                    description="用于顶部标题和浏览器标签标题。"
                  >
                    <Input
                      value={form.app.title}
                      onChange={(e) =>
                        setForm((prev) =>
                          prev
                            ? {
                                ...prev,
                                app: { ...prev.app, title: e.target.value },
                              }
                            : prev,
                        )
                      }
                    />
                  </Field>
                  <Field
                    label="显示时区"
                    description="控制绝对时间戳、雷达/请求趋势和使用记录筛选；每日财务汇总仍按系统业务日统计。"
                  >
                    <Input
                      list="bridgeops-time-zone-options"
                      value={form.app.timeZone || "Asia/Shanghai"}
                      onChange={(event) =>
                        setForm((prev) =>
                          prev
                            ? {
                                ...prev,
                                app: { ...prev.app, timeZone: event.target.value },
                              }
                            : prev,
                        )
                      }
                    />
                    <datalist id="bridgeops-time-zone-options">
                      {displayTimeZoneOptions.map((timeZone) => <option key={timeZone} value={timeZone} />)}
                    </datalist>
                  </Field>
                  <Field
                    label="通知前缀"
                    description="为空时通知标题不添加前缀。"
                  >
                    <Input
                      value={form.app.notificationPrefix}
                      onChange={(e) =>
                        setForm((prev) =>
                          prev
                            ? {
                                ...prev,
                                app: {
                                  ...prev.app,
                                  notificationPrefix: e.target.value,
                                },
                              }
                            : prev,
                        )
                      }
                    />
                  </Field>
                </div>
              </SectionCard>

            <SectionCard
              id="settings-auth"
                hidden={activeSection !== "settings-auth"}
              icon={<ShieldCheck className="size-4 text-emerald-600" />}
              title="登录鉴权"
              description="控制后台是否需要登录，以及登录令牌的签发方式。"
            >
              <div className="grid gap-4 md:grid-cols-2">
                <InlineSwitch
                  id="auth-enabled"
                  label="启用登录鉴权"
                  description="关闭后前端将直接进入系统，不显示登录页。"
                  checked={form.auth.enabled}
                  onCheckedChange={(checked) =>
                    setForm((prev) =>
                      prev
                        ? { ...prev, auth: { ...prev.auth, enabled: checked } }
                        : prev,
                    )
                  }
                />
                <NoteBox title="热应用说明">
                  应用后新的鉴权配置立即生效，现有无效令牌会在后续请求时被拦截。
                </NoteBox>
              </div>
              <div className="mt-4 grid gap-4 md:grid-cols-2">
                <Field
                  label="管理员账号"
                  description="用于后台登录的固定账号。"
                >
                  <Input
                    value={form.auth.username}
                    onChange={(e) =>
                      setForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              auth: { ...prev.auth, username: e.target.value },
                            }
                          : prev,
                      )
                    }
                  />
                </Field>
                <Field
                  label="登录有效期（小时）"
                  description="登录后令牌的有效时长。"
                >
                  <NumberDraftInput
                    draftKey="sessionTTLHours"
                    value={form.auth.sessionTTLHours}
                    min={1}
                    onDraftDirtyChange={updateNumberDraftState}
                    onValueChange={(value) => setForm((prev) => prev ? {
                      ...prev,
                      auth: { ...prev.auth, sessionTTLHours: value },
                    } : prev)}
                  />
                </Field>
                <Field
                  label="管理员密码"
                  description={form.auth.password
                    ? "已输入新密码，保存并应用后用于新登录。"
                    : form.auth.passwordConfigured
                      ? "已设置；留空会保持现有密码不变。"
                      : "尚未设置；启用登录鉴权前必须输入密码。"}
                >
                  <SecretInput
                    value={form.auth.password}
                    revealed={showAdminPassword}
                    onRevealChange={setShowAdminPassword}
                    placeholder={form.auth.passwordConfigured ? "已设置，留空不修改" : "输入管理员密码"}
                    onChange={(value) =>
                      setForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              auth: { ...prev.auth, password: value },
                            }
                          : prev,
                      )
                    }
                  />
                </Field>
                <Field
                  label="令牌签名密钥"
                  description={form.auth.clearTokenSecret
                    ? "保存后会清除自定义密钥，改用安全主密钥。"
                    : form.auth.tokenSecret
                      ? "已输入新密钥，应用后现有登录令牌会失效。"
                      : form.auth.tokenSecretConfigured
                        ? "已设置；留空会保持现有密钥不变。"
                        : "未单独设置，当前使用安全主密钥。"}
                >
                  <div className="space-y-2">
                    <SecretInput
                      value={form.auth.tokenSecret}
                      revealed={showTokenSecret}
                      onRevealChange={setShowTokenSecret}
                      placeholder={form.auth.tokenSecretConfigured ? "已设置，留空不修改" : "留空使用安全主密钥"}
                      onChange={(value) =>
                        setForm((prev) => prev ? {
                          ...prev,
                          auth: { ...prev.auth, tokenSecret: value, clearTokenSecret: false },
                        } : prev)
                      }
                    />
                    {form.auth.tokenSecretConfigured ? (
                      <Button
                        type="button"
                        variant="ghost"
                        size="sm"
                        className="h-7 px-2 text-xs"
                        onClick={() => setForm((prev) => prev ? {
                          ...prev,
                          auth: {
                            ...prev.auth,
                            tokenSecret: "",
                            clearTokenSecret: !prev.auth.clearTokenSecret,
                          },
                        } : prev)}
                      >
                        {form.auth.clearTokenSecret ? "撤销清除" : "改用安全主密钥"}
                      </Button>
                    ) : null}
                  </div>
                </Field>
              </div>
            </SectionCard>

            <SectionCard
              id="settings-jobs"
                hidden={activeSection !== "settings-jobs"}
              icon={<Clock3 className="size-4 text-muted-foreground" />}
              title="后台任务"
              description="管理余额采集、调度并发和模型雷达刷新周期。"
            >
              <div className="grid gap-4 md:grid-cols-3">
                <Field
                  label="余额采集 Cron"
                  description="控制余额与消费同步的执行周期。"
                >
                  <Input
                    value={form.scheduler.balanceCron}
                    onChange={(e) =>
                      setForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              scheduler: {
                                ...prev.scheduler,
                                balanceCron: e.target.value,
                              },
                            }
                          : prev,
                      )
                    }
                  />
                </Field>
                <Field
                  label="并发数"
                  description="调度器每轮最多同时处理的任务数。"
                >
                  <NumberDraftInput
                    draftKey="schedulerConcurrency"
                    value={form.scheduler.concurrency}
                    min={1}
                    onDraftDirtyChange={updateNumberDraftState}
                    onValueChange={(value) => setForm((prev) => prev ? {
                      ...prev,
                      scheduler: { ...prev.scheduler, concurrency: value },
                    } : prev)}
                  />
                </Field>
                <Field
                  label="模型雷达刷新周期（分钟）"
                  description="后台刷新 Codex 与 Claude 雷达公开摘要；失败时保留最近一次成功数据。"
                >
                  <NumberDraftInput
                    draftKey="radarRefreshIntervalMinutes"
                    value={form.scheduler.codexRadarRefreshIntervalMinutes}
                    min={1}
                    onDraftDirtyChange={updateNumberDraftState}
                    onValueChange={(value) => setForm((prev) => prev ? {
                      ...prev,
                      scheduler: { ...prev.scheduler, codexRadarRefreshIntervalMinutes: value },
                    } : prev)}
                  />
                </Field>
              </div>
            </SectionCard>

            <SectionCard
              id="settings-retention"
                hidden={activeSection !== "settings-retention"}
              icon={<Database className="size-4 text-amber-600" />}
              title="数据保留"
              description="管理历史清理任务以及监控、余额、通知和公告数据的保留时长。"
            >
              <div className="max-w-md">
                <Field
                  label="清理任务 Cron"
                  description="留空则不执行历史数据清理。"
                >
                  <Input
                    value={form.scheduler.retention.cron}
                    onChange={(e) =>
                      setForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              scheduler: {
                                ...prev.scheduler,
                                retention: {
                                  ...prev.scheduler.retention,
                                  cron: e.target.value,
                                },
                              },
                            }
                          : prev,
                      )
                    }
                  />
                </Field>
              </div>
              <div className="mt-4 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                <Field
                  label="监控日志保留天数"
                  description="超过该天数的监控日志会被清理。"
                >
                  <NumberDraftInput
                    draftKey="monitorLogsDays"
                    value={form.scheduler.retention.monitorLogsDays}
                    min={0}
                    onDraftDirtyChange={updateNumberDraftState}
                    onValueChange={(value) => setForm((prev) => prev ? {
                      ...prev,
                      scheduler: { ...prev.scheduler, retention: { ...prev.scheduler.retention, monitorLogsDays: value } },
                    } : prev)}
                  />
                </Field>
                <Field
                  label="余额快照保留天数"
                  description="余额与消费趋势依赖这部分历史快照。"
                >
                  <NumberDraftInput
                    draftKey="balanceSnapshotsDays"
                    value={form.scheduler.retention.balanceSnapshotsDays}
                    min={0}
                    onDraftDirtyChange={updateNumberDraftState}
                    onValueChange={(value) => setForm((prev) => prev ? {
                      ...prev,
                      scheduler: { ...prev.scheduler, retention: { ...prev.scheduler.retention, balanceSnapshotsDays: value } },
                    } : prev)}
                  />
                </Field>
                <Field
                  label="通知日志保留天数"
                  description="通知发送结果的历史留存时长。"
                >
                  <NumberDraftInput
                    draftKey="notificationLogsDays"
                    value={form.scheduler.retention.notificationLogsDays}
                    min={0}
                    onDraftDirtyChange={updateNumberDraftState}
                    onValueChange={(value) => setForm((prev) => prev ? {
                      ...prev,
                      scheduler: { ...prev.scheduler, retention: { ...prev.scheduler.retention, notificationLogsDays: value } },
                    } : prev)}
                  />
                </Field>
                <Field
                  label="上游公告保留天数"
                  description="动态中心上游公告的历史留存时长。"
                >
                  <NumberDraftInput
                    draftKey="announcementsDays"
                    value={form.scheduler.retention.announcementsDays}
                    min={0}
                    onDraftDirtyChange={updateNumberDraftState}
                    onValueChange={(value) => setForm((prev) => prev ? {
                      ...prev,
                      scheduler: { ...prev.scheduler, retention: { ...prev.scheduler.retention, announcementsDays: value } },
                    } : prev)}
                  />
                </Field>
              </div>
            </SectionCard>

          <SectionCard
            id="settings-upstream"
                hidden={activeSection !== "settings-upstream"}
            icon={<Server className="size-4 text-muted-foreground" />}
            title="上游请求"
            description="配置渠道访问上游站点时使用的超时时间和 User-Agent。"
          >
            <div className="grid gap-4 md:grid-cols-2">
              <Field label="超时时间（秒）" description="小于等于 0 时使用默认 30 秒。">
                <NumberDraftInput
                  draftKey="upstreamTimeoutSeconds"
                  value={form.upstream.timeoutSeconds}
                  min={0}
                  onDraftDirtyChange={updateNumberDraftState}
                  onValueChange={(value) => setForm((prev) => prev ? {
                    ...prev,
                    upstream: { ...prev.upstream, timeoutSeconds: value },
                  } : prev)}
                />
              </Field>
              <Field label="User-Agent" description="为空时使用 bridgeops/0.1。">
                <Input
                  value={form.upstream.userAgent}
                  placeholder="bridgeops/0.1"
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            upstream: {
                              ...prev.upstream,
                              userAgent: e.target.value,
                            },
                          }
                        : prev,
                    )
                  }
                />
              </Field>
            </div>
          </SectionCard>

          <SectionCard
            id="settings-proxy"
                hidden={activeSection !== "settings-proxy"}
            icon={<Network className="size-4 text-muted-foreground" />}
            title="系统服务代理"
            description="用于通知、独立打码服务和版本检测；渠道代理请在网络代理页单独绑定。"
            action={
              <Button
                size="sm"
                variant="outline"
                className="border-border bg-background"
                onClick={handleTestProxy}
                disabled={testingProxy}
              >
                <RefreshCw
                  className={cn("size-3.5", testingProxy ? "animate-spin" : "")}
                />
                {testingProxy ? "测试中..." : "测试代理"}
              </Button>
            }
          >
            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
              <InlineSwitch
                id="proxy-enabled"
                label="启用系统服务代理"
                description="关闭后通知、独立打码服务和版本检测保持直连；已单独绑定网络代理的渠道不受影响。"
                checked={form.proxy.enabled}
                onCheckedChange={(checked) =>
                  setForm((prev) =>
                    prev
                      ? {
                          ...prev,
                          proxy: { ...prev.proxy, enabled: checked },
                        }
                      : prev,
                  )
                }
              />
              <InlineSwitch
                id="proxy-version-check"
                label="检测更新走代理"
                description="开启后侧栏版本检测入口和这里的检测更新会使用代理。"
                checked={form.proxy.versionCheckEnabled}
                onCheckedChange={(checked) =>
                  setForm((prev) =>
                    prev
                      ? {
                          ...prev,
                          proxy: {
                            ...prev.proxy,
                            versionCheckEnabled: checked,
                          },
                        }
                      : prev,
                  )
                }
              />
              <Field label="协议" description="支持 HTTP、HTTPS 和 SOCKS5。">
                <Select
                  value={form.proxy.protocol}
                  onValueChange={(value) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            proxy: {
                              ...prev.proxy,
                              protocol: value as "http" | "https" | "socks5",
                            },
                          }
                        : prev,
                    )
                  }
                >
                  <SelectTrigger className="w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="http">HTTP</SelectItem>
                    <SelectItem value="https">HTTPS</SelectItem>
                    <SelectItem value="socks5">SOCKS5</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
              <Field label="主机" description="代理服务器地址，不含协议。">
                <Input
                  value={form.proxy.host}
                  placeholder="127.0.0.1"
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            proxy: { ...prev.proxy, host: e.target.value },
                          }
                        : prev,
                    )
                  }
                />
              </Field>
              <Field label="端口" description="代理服务监听端口。">
                <NumberDraftInput
                  draftKey="proxyPort"
                  value={form.proxy.port}
                  min={0}
                  max={65535}
                  onDraftDirtyChange={updateNumberDraftState}
                  onValueChange={(value) => setForm((prev) => prev ? {
                    ...prev,
                    proxy: { ...prev.proxy, port: value },
                  } : prev)}
                />
              </Field>
              <Field label="账号（可选）" description="代理认证用户名。">
                <Input
                  value={form.proxy.username}
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            proxy: { ...prev.proxy, username: e.target.value },
                          }
                        : prev,
                    )
                  }
                />
              </Field>
              <Field
                label="密码（可选）"
                description={form.proxy.clearPassword
                  ? "保存后会清除已保存的代理密码。"
                  : form.proxy.password
                    ? "已输入新代理密码。"
                    : form.proxy.passwordConfigured
                      ? "已设置；留空会保持现有密码不变。"
                      : "代理认证密码。"}
              >
                <div className="space-y-2">
                  <SecretInput
                    value={form.proxy.password}
                    revealed={showProxyPassword}
                    onRevealChange={setShowProxyPassword}
                    placeholder={form.proxy.passwordConfigured ? "已设置，留空不修改" : "输入代理密码"}
                    onChange={(value) => setForm((prev) => prev ? {
                      ...prev,
                      proxy: { ...prev.proxy, password: value, clearPassword: false },
                    } : prev)}
                  />
                  {form.proxy.passwordConfigured ? (
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      className="h-7 px-2 text-xs"
                      onClick={() => setForm((prev) => prev ? {
                        ...prev,
                        proxy: {
                          ...prev.proxy,
                          password: "",
                          clearPassword: !prev.proxy.clearPassword,
                        },
                      } : prev)}
                    >
                      {form.proxy.clearPassword ? "撤销清除" : "清除已保存密码"}
                    </Button>
                  ) : null}
                </div>
              </Field>
            </div>
          </SectionCard>
      </div>
    </section>
  );
}

function Field({
  label,
  description,
  children,
}: {
  label: string;
  description?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-2">
      <div className="flex items-center gap-1.5">
        <Label className="text-sm font-medium text-foreground">{label}</Label>
        {description ? <HelpHint label={label}>{description}</HelpHint> : null}
      </div>
      {children}
    </div>
  );
}

function SecretInput({
  value,
  placeholder,
  revealed,
  onChange,
  onRevealChange,
}: {
  value: string;
  placeholder: string;
  revealed: boolean;
  onChange: (value: string) => void;
  onRevealChange: (revealed: boolean) => void;
}) {
  return (
    <div className="relative">
      <Input
        type={revealed ? "text" : "password"}
        value={value}
        placeholder={placeholder}
        autoComplete="new-password"
        className="pr-10"
        onChange={(event) => onChange(event.target.value)}
      />
      <Button
        type="button"
        variant="ghost"
        size="icon-sm"
        className="absolute right-1 top-1/2 -translate-y-1/2 text-muted-foreground"
        onClick={() => onRevealChange(!revealed)}
        aria-label={revealed ? "隐藏输入内容" : "显示输入内容"}
        title={revealed ? "隐藏输入内容" : "显示输入内容"}
      >
        {revealed ? <EyeOff className="size-3.5" /> : <Eye className="size-3.5" />}
      </Button>
    </div>
  );
}

function SectionCard({
  id,
  hidden,
  icon,
  title,
  description,
  action,
  children,
}: {
  id?: string;
  hidden?: boolean;
  icon: React.ReactNode;
  title: string;
  description: string;
  action?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <section id={id} hidden={hidden} className="rounded-xl border border-border bg-card p-5">
      <div className="mb-3 flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
        <div className="space-y-1">
          <h2 className="flex items-center gap-2 text-lg font-semibold tracking-tight text-foreground">
            <span aria-hidden="true">{icon}</span>
            {title}
            <HelpHint label={title}>{description}</HelpHint>
          </h2>
        </div>
        {action}
      </div>
      {children}
    </section>
  );
}

function InlineSwitch({
  id,
  label,
  description,
  checked,
  onCheckedChange,
}: {
  id: string;
  label: string;
  description: string;
  checked: boolean;
  onCheckedChange: (checked: boolean) => void;
}) {
  return (
    <div className="flex items-start justify-between gap-4 rounded-lg border border-border bg-background/90 px-4 py-3">
      <div className="space-y-1">
        <Label htmlFor={id} className="text-sm font-medium text-foreground">
          {label}
        </Label>
        <p className="text-[11px] leading-5 text-muted-foreground">
          {description}
        </p>
      </div>
      <Switch id={id} checked={checked} onCheckedChange={onCheckedChange} />
    </div>
  );
}

function NoteBox({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <div className="rounded-lg border border-border bg-muted/40 px-4 py-3 text-sm text-foreground">
      <p className="text-xs font-semibold text-foreground">
        {title}
      </p>
      <p className="mt-1 leading-6">{children}</p>
    </div>
  );
}
