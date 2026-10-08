import { useEffect, useState } from "react"
import { Copy, ExternalLink, FileCode2, KeyRound } from "lucide-react"
import codexIcon from "@lobehub/icons-static-svg/icons/codex-color.svg"
import claudeCodeIcon from "@lobehub/icons-static-svg/icons/claudecode-color.svg"
import geminiCLIIcon from "@lobehub/icons-static-svg/icons/geminicli-color.svg"
import cherryStudioIcon from "@lobehub/icons-static-svg/icons/cherrystudio-color.svg"
import lobeChatIcon from "@lobehub/icons-static-svg/icons/lobehub-color.svg"
import clineIcon from "@lobehub/icons-static-svg/icons/cline.svg"
import rooCodeIcon from "@lobehub/icons-static-svg/icons/roocode.svg"
import kiloCodeIcon from "@lobehub/icons-static-svg/icons/kilocode.svg"
import openAIIcon from "@lobehub/icons-static-svg/icons/openai.svg"
import anthropicIcon from "@lobehub/icons-static-svg/icons/anthropic.svg"
import geminiIcon from "@lobehub/icons-static-svg/icons/gemini-color.svg"
import ccSwitchIcon from "@/assets/client-icons/ccswitch.png"
import chatboxIcon from "@/assets/client-icons/chatbox.png"
import continueIcon from "@/assets/client-icons/continue.svg"
import type { GatewayApp } from "@/lib/api-types"
import {
  buildChatboxClientImport,
  buildContinueClientConfig,
} from "@/components/gateway/client-import-data"
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"

export type ClientImportFile = {
  id: string
  label: string
  path: string
  content: string
  locationLabel?: string
}

export type ClientImportRequest = {
  apiKey: string
  providerName: string
  contextLabel: string
  defaultApp: GatewayApp
  modelName?: string
}

type ClientImportCategory = "cli" | "desktop" | "editor" | "generic"
type ClientImportMode = "ccswitch" | "deeplink" | "config"

type ClientImportClient = {
  id: string
  category: ClientImportCategory
  name: string
  description: string
  icon: string
  monochrome?: boolean
  mode: ClientImportMode
  app?: GatewayApp
  supportedApps?: GatewayApp[]
  unavailableLabel?: string
}

const clientImportCategories: Array<{ id: ClientImportCategory; label: string }> = [
  { id: "cli", label: "编程 CLI" },
  { id: "desktop", label: "桌面客户端" },
  { id: "editor", label: "编辑器插件" },
  { id: "generic", label: "通用协议" },
]

const clientImportClients: ClientImportClient[] = [
  { id: "ccswitch", category: "cli", name: "CCSwitch", description: "导入当前 Key 的 CLI 服务配置", icon: ccSwitchIcon, mode: "ccswitch", supportedApps: ["codex", "claude"] },
  {
    id: "codex-cli",
    category: "cli",
    name: "Codex CLI",
    description: "通过 CCSwitch 导入 OpenAI Responses 配置",
    icon: codexIcon,
    mode: "ccswitch",
    app: "codex",
  },
  {
    id: "claude-code",
    category: "cli",
    name: "Claude Code",
    description: "通过 CCSwitch 导入 Anthropic Messages 配置",
    icon: claudeCodeIcon,
    mode: "ccswitch",
    app: "claude",
  },
  {
    id: "gemini-cli",
    category: "cli",
    name: "Gemini CLI",
    description: "通过 CCSwitch 导入 Gemini 配置",
    icon: geminiCLIIcon,
    mode: "ccswitch",
    app: "gemini",
    unavailableLabel: "待原生协议",
  },
  {
    id: "cherry-studio",
    category: "desktop",
    name: "Cherry Studio",
    description: "在 Cherry Studio 中填写自定义模型服务",
    icon: cherryStudioIcon,
    mode: "config",
  },
  {
    id: "chatbox",
    category: "desktop",
    name: "Chatbox",
    description: "通过 Chatbox 官方协议一键导入",
    icon: chatboxIcon,
    mode: "deeplink",
  },
  {
    id: "lobechat",
    category: "desktop",
    name: "LobeChat",
    description: "复制供应商配置，不把 Key 写入网页链接",
    icon: lobeChatIcon,
    mode: "config",
  },
  {
    id: "cline",
    category: "editor",
    name: "Cline",
    description: "复制编辑器插件的自定义 API 参数",
    icon: clineIcon,
    monochrome: true,
    mode: "config",
  },
  {
    id: "roo-code",
    category: "editor",
    name: "Roo Code",
    description: "复制编辑器插件的自定义 API 参数",
    icon: rooCodeIcon,
    monochrome: true,
    mode: "config",
  },
  {
    id: "continue",
    category: "editor",
    name: "Continue",
    description: "复制可放入 config.yaml 的模型配置",
    icon: continueIcon,
    mode: "config",
  },
  {
    id: "kilo-code",
    category: "editor",
    name: "Kilo Code",
    description: "复制 Custom Provider 配置",
    icon: kiloCodeIcon,
    monochrome: true,
    mode: "config",
  },
  {
    id: "openai-compatible",
    category: "generic",
    name: "OpenAI 兼容客户端",
    description: "适用于支持自定义 OpenAI API 的客户端",
    icon: openAIIcon,
    monochrome: true,
    mode: "config",
    app: "codex",
  },
  {
    id: "anthropic-compatible",
    category: "generic",
    name: "Anthropic 兼容客户端",
    description: "适用于支持 Messages API 的客户端",
    icon: anthropicIcon,
    monochrome: true,
    mode: "config",
    app: "claude",
  },
  {
    id: "gemini-compatible",
    category: "generic",
    name: "Gemini 模型客户端",
    description: "当前通过 OpenAI 兼容协议接入 Gemini 模型",
    icon: geminiIcon,
    mode: "config",
    app: "gemini",
  },
]

function appLabel(app: GatewayApp) {
  if (app === "claude") return "Anthropic Messages"
  if (app === "gemini") return "OpenAI 兼容"
  return "OpenAI Responses"
}

function endpointFor(app: GatewayApp, baseURL: string, originURL: string) {
  return app === "claude" ? originURL : baseURL
}

function resolvedClientApp(client: ClientImportClient, defaultApp: GatewayApp): GatewayApp {
  return client.app ?? defaultApp
}

function clientSupported(client: ClientImportClient, defaultApp: GatewayApp) {
  if (client.unavailableLabel) return false
  if (client.supportedApps && !client.supportedApps.includes(defaultApp)) return false
  return client.app == null || client.app === defaultApp
}

function clientUnavailableLabel(client: ClientImportClient) {
  return client.unavailableLabel || (client.app ? "协议不兼容" : "当前协议不支持")
}

function genericClientFiles(
  client: ClientImportClient,
  request: ClientImportRequest,
  baseURL: string,
  originURL: string,
): ClientImportFile[] {
  const app = resolvedClientApp(client, request.defaultApp)
  const endpoint = endpointFor(app, baseURL, originURL)
  const model = request.modelName?.trim() ?? ""
  const name = `BridgeOps · ${request.contextLabel}`
  const protocol = clientProtocolLabel(client.id, app)
  const apiKeyValue = request.apiKey.trim()
  const common = {
    name,
    protocol,
    base_url: endpoint,
    api_key: apiKeyValue,
    model,
  }

  if (client.id === "continue") {
    const file = buildContinueClientConfig({ apiKey: apiKeyValue, name, app, model, endpoint })
    return file ? [{ ...file, locationLabel: "写入文件：" }] : []
  }

  if (client.id === "cherry-studio" || client.id === "cline" || client.id === "roo-code" || client.id === "kilo-code" || client.id === "lobechat") {
    return [{
      id: `${client.id}-settings`,
      label: `${client.name} 连接参数`,
      path: clientSettingsPath(client.id),
      locationLabel: "填写位置：",
      content: [
        `协议类型: ${protocol}`,
        `Base URL: ${endpoint}`,
        `API Key: ${apiKeyValue}`,
        ...(model ? [`Model ID: ${model}`] : []),
      ].join("\n"),
    }]
  }

  return [{
    id: `${client.id}-settings`,
    label: `${client.name} 自定义供应商`,
    path: `${client.name} / 自定义 API`,
    locationLabel: "填写位置：",
    content: JSON.stringify(common, null, 2),
  }]
}

function clientProtocolLabel(clientID: string, app: GatewayApp) {
  if (app === "claude") return "Anthropic Messages"
  if (app === "gemini") return "OpenAI Compatible"
  if (clientID === "kilo-code" || clientID === "lobechat") return "OpenAI Responses"
  if (clientID === "cherry-studio") return "OpenAI Responses"
  return "OpenAI"
}

function clientSettingsPath(clientID: string) {
  if (clientID === "lobechat") return "设置 / 模型服务商 / 添加模型服务商"
  if (clientID === "cherry-studio") return "设置 / 模型服务 / 添加模型服务商"
  if (clientID === "kilo-code") return "Settings / Providers / Custom provider"
  if (clientID === "roo-code") return "Roo Code / Settings / Providers / 新建 Profile"
  return "Cline / Settings / API Provider"
}

function ClientConfigPreview({
  file,
  onCopy,
  compact = false,
}: {
  file: ClientImportFile
  onCopy: () => void
  compact?: boolean
}) {
  const lines = file.content.split("\n")
  return (
    <div className="overflow-hidden rounded-xl border border-border bg-muted/25">
      <div className="flex items-center justify-between gap-3 border-b border-border bg-card px-3 py-2.5">
        <div className="flex min-w-0 items-center gap-2 text-sm text-foreground" title={file.label}>
          <FileCode2 className="size-4 shrink-0 text-brand" />
          <span className="truncate font-mono">{file.path}</span>
        </div>
        <Button
          type="button"
          size={compact ? "icon-sm" : "sm"}
          variant="outline"
          onClick={onCopy}
          className="shrink-0 border-border bg-card text-brand hover:bg-accent"
          aria-label={`复制 ${file.label}`}
        >
          <Copy className="size-3.5" />
          {!compact ? "复制" : null}
        </Button>
      </div>
      <pre className={cn("overflow-x-auto p-0 font-mono text-xs font-medium leading-6 tracking-normal text-foreground [font-feature-settings:'liga'_1,'calt'_1]", compact ? "max-h-64" : "max-h-80")}>
        <code className="block py-3">
          {lines.map((line, index) => (
            <span key={`${file.id}-${index}`} className="flex min-w-max px-4 hover:bg-accent/35">
              <span className="mr-5 w-5 select-none text-right text-xs font-normal text-muted-foreground/70">{index + 1}</span>
              <span className="whitespace-pre">{line || " "}</span>
            </span>
          ))}
        </code>
      </pre>
    </div>
  )
}

export function ClientImportDialog({
  request,
  baseURL,
  originURL,
  open,
  onOpenChange,
  onLaunchCCSwitch,
  onLaunchDeepLink,
  onCopy,
  buildNativeConfigFiles,
}: {
  request: ClientImportRequest | null
  baseURL: string
  originURL: string
  open: boolean
  onOpenChange: (open: boolean) => void
  onLaunchCCSwitch: (request: ClientImportRequest, app: GatewayApp) => void
  onLaunchDeepLink: (url: string, clientName: string) => void
  onCopy: (value: string, label?: string) => Promise<void> | void
  buildNativeConfigFiles: (apiKey: string, app: GatewayApp, modelName?: string) => ClientImportFile[]
}) {
  const [selectedClientID, setSelectedClientID] = useState("ccswitch")
  const [activeFileID, setActiveFileID] = useState("")

  useEffect(() => {
    if (!open || !request) return
    setSelectedClientID(request.defaultApp === "gemini" ? "gemini-compatible" : "ccswitch")
    setActiveFileID("")
  }, [open, request])

  const selectedClient = clientImportClients.find(client => client.id === selectedClientID && (!request || clientSupported(client, request.defaultApp))) ?? null
  const selectedApp = selectedClient && request ? resolvedClientApp(selectedClient, request.defaultApp) : "codex"
  const selectedOfficialImport = selectedClient?.mode === "deeplink" && request
    ? buildChatboxClientImport({
        providerName: request.providerName,
        app: selectedApp,
        modelName: request.modelName,
        baseURL,
        originURL,
      })
    : null
  const selectedFiles = selectedClient && request
    ? selectedClient.mode === "ccswitch"
      ? buildNativeConfigFiles(request.apiKey, selectedApp, request.modelName)
      : selectedClient.mode === "deeplink" && selectedOfficialImport
        ? [{
            id: `${selectedClient.id}-import`,
            label: `${selectedClient.name} 导入内容`,
            path: "客户端官方导入确认页",
            locationLabel: "导入方式：",
            content: selectedOfficialImport.payload,
          }]
      : genericClientFiles(selectedClient, request, baseURL, originURL)
    : []
  const activeFile = selectedFiles.find(file => file.id === activeFileID) ?? selectedFiles[0]


  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="h-[min(740px,calc(100dvh-2rem))] w-[min(1040px,calc(100vw-2rem))] max-w-none grid-rows-[auto_minmax(0,1fr)] gap-0 overflow-hidden p-0 sm:max-w-none">
        <DialogHeader className="border-b border-border px-5 py-4 pr-12">
          <DialogTitle>{request?.contextLabel || "Key"} · 客户端配置</DialogTitle>
          <DialogDescription className="sr-only">选择客户端后复制配置或导入。</DialogDescription>
        </DialogHeader>
        <div className="grid min-h-0 grid-cols-[210px_minmax(0,1fr)]">
          <nav className="overflow-y-auto border-r border-border bg-muted/20 p-3" aria-label="客户端">
            {clientImportCategories.map(category => <div key={category.id} className="mb-3">
              <div className="px-2 py-1.5 text-xs font-medium text-muted-foreground">{category.label}</div>
              {clientImportClients.filter(client => client.category === category.id).map(client => <button key={client.id} type="button" disabled={Boolean(request && !clientSupported(client, request.defaultApp))} onClick={() => { setSelectedClientID(client.id); setActiveFileID("") }} aria-pressed={selectedClient?.id === client.id} title={request && !clientSupported(client, request.defaultApp) ? clientUnavailableLabel(client) : client.description} className={cn("flex h-9 w-full items-center gap-2 rounded-lg px-2 text-left text-sm disabled:cursor-not-allowed disabled:opacity-40", selectedClient?.id === client.id ? "bg-accent font-medium text-accent-foreground" : "hover:bg-muted")}><img src={client.icon} alt="" aria-hidden="true" className={cn("size-5 shrink-0 object-contain", client.monochrome && "dark:invert")} /><span className="truncate">{client.name}</span></button>)}
            </div>)}
          </nav>
          <section className="min-h-0 overflow-y-auto p-5">
            {selectedClient && request ? <div className="space-y-4">
              <div className="flex items-center justify-between gap-3">
                <p className="min-w-0 truncate text-sm text-muted-foreground" title={request.modelName}>{appLabel(selectedApp)}{request.modelName ? ` · ${request.modelName}` : ""}</p>
                {selectedClient.mode === "ccswitch" ? <Button disabled={!request.modelName?.trim()} onClick={() => onLaunchCCSwitch(request, selectedApp)}><ExternalLink className="size-4" />导入 CCSwitch</Button> : selectedClient.mode === "deeplink" && selectedOfficialImport ? <Button onClick={async () => { await onCopy(request.apiKey, "API Key"); onLaunchDeepLink(selectedOfficialImport.url, selectedClient.name) }}><ExternalLink className="size-4" />复制 Key 并打开</Button> : null}
              </div>
              <div className="flex flex-wrap items-center gap-2">
                <Button size="sm" variant="outline" onClick={() => void onCopy(endpointFor(selectedApp, baseURL, originURL), "API 地址")}>复制地址</Button>
                <Button size="sm" variant="outline" onClick={() => void onCopy(request.apiKey, "API Key")}><KeyRound className="size-3.5" />复制 Key</Button>
              </div>
              {!request.modelName?.trim() ? <p className="rounded-lg border border-warning/30 bg-warning/5 p-3 text-sm text-warning">没有可用的默认模型，请先同步模型或在客户端手动指定。</p> : null}
              {selectedFiles.length > 1 ? <div className="flex flex-wrap gap-2" aria-label="配置文件">{selectedFiles.map(file => <Button key={file.id} size="sm" variant={activeFile?.id === file.id ? "secondary" : "ghost"} onClick={() => setActiveFileID(file.id)}>{file.label}</Button>)}</div> : null}
              {activeFile ? <ClientConfigPreview file={activeFile} onCopy={() => onCopy(activeFile.content, activeFile.label)} /> : <p className="py-6 text-sm text-muted-foreground">该客户端暂无可生成的配置。</p>}
            </div> : null}
          </section>
        </div>
      </DialogContent>
    </Dialog>
  )
}

export { ClientConfigPreview }
