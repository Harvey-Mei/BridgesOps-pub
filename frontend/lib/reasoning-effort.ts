const reasoningEffortAliases: Record<string, string> = {
  "x-high": "xhigh",
  x_high: "xhigh",
  "extra-high": "xhigh",
  extra_high: "xhigh",
}

export function officialReasoningEffortLabel(value: string | null | undefined) {
  const effort = String(value ?? "").trim().toLowerCase()
  return reasoningEffortAliases[effort] ?? effort
}
