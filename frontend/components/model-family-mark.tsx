import { Sparkles } from "lucide-react"
import openAIIcon from "@lobehub/icons-static-svg/icons/openai.svg"
import claudeIcon from "@lobehub/icons-static-svg/icons/claude-color.svg"
import geminiIcon from "@lobehub/icons-static-svg/icons/gemini-color.svg"
import grokIcon from "@lobehub/icons-static-svg/icons/grok.svg"
import deepSeekIcon from "@lobehub/icons-static-svg/icons/deepseek-color.svg"
import qwenIcon from "@lobehub/icons-static-svg/icons/qwen-color.svg"
import mistralIcon from "@lobehub/icons-static-svg/icons/mistral-color.svg"
import llamaIcon from "@lobehub/icons-static-svg/icons/meta-color.svg"
import glmIcon from "@lobehub/icons-static-svg/icons/zhipu-color.svg"
import minimaxIcon from "@lobehub/icons-static-svg/icons/minimax-color.svg"
import moonshotIcon from "@lobehub/icons-static-svg/icons/moonshot.svg"
import kimiIcon from "@lobehub/icons-static-svg/icons/kimi-color.svg"
import yiIcon from "@lobehub/icons-static-svg/icons/yi-color.svg"
import ernieIcon from "@lobehub/icons-static-svg/icons/baidu-color.svg"
import { cn } from "@/lib/utils"
import { modelFamilyIconName, modelFamilyLabel } from "@/lib/model-families"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"

type ModelFamilyMetadata = {
  icon: string
  darkBackground?: boolean
}

const modelFamilyMetadata: Record<string, ModelFamilyMetadata> = {
	openai: { icon: openAIIcon },
	claude: { icon: claudeIcon },
	gemini: { icon: geminiIcon },
	grok: { icon: grokIcon },
	deepseek: { icon: deepSeekIcon },
	qwen: { icon: qwenIcon },
	mistral: { icon: mistralIcon },
	meta: { icon: llamaIcon },
	zhipu: { icon: glmIcon },
	minimax: { icon: minimaxIcon },
	moonshot: { icon: moonshotIcon },
	kimi: { icon: kimiIcon, darkBackground: true },
	yi: { icon: yiIcon },
	baidu: { icon: ernieIcon },
}

export function ModelFamilyIcon({ family, className }: { family: string | null | undefined; className?: string }) {
	const metadata = modelFamilyMetadata[modelFamilyIconName(family)]
  return (
    <span
      aria-hidden="true"
      className={cn(
        "inline-flex size-4 shrink-0 items-center justify-center overflow-hidden rounded-[4px] ring-1 ring-black/10",
        metadata?.darkBackground ? "bg-neutral-950" : "bg-white",
        className,
      )}
    >
      {metadata ? <img src={metadata.icon} alt="" className="size-3.5 object-contain" /> : <Sparkles className="size-3 text-muted-foreground" />}
    </span>
  )
}

export function ModelFamilyMark({ family, className, iconClassName }: { family: string | null | undefined; className?: string; iconClassName?: string }) {
  const label = modelFamilyLabel(family)
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span
          role="img"
          aria-label={label}
          className={cn("inline-flex size-4 shrink-0 items-center justify-center", className)}
        >
          <ModelFamilyIcon family={family} className={iconClassName} />
        </span>
      </TooltipTrigger>
      <TooltipContent>{label}</TooltipContent>
    </Tooltip>
  )
}
