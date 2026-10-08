import { CaptchaStatus } from "@/components/monitor/bottom-panels"
import { ShieldCheck } from "lucide-react"
import { PageHeader } from "@/components/ui/page-header"

export default function CaptchaPage() {
  return <section className="space-y-4"><PageHeader icon={ShieldCheck} title="验证码服务" description="查看验证码识别服务状态和调用情况" /><CaptchaStatus /></section>
}
