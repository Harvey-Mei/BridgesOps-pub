import { z } from "zod"

const optionalText = z.string().nullable().optional().transform((value) => value ?? undefined)
const optionalNumber = z.number().finite().nullable().optional().transform((value) => value ?? undefined)
const optionalNumberSeries = z.array(z.number().finite().nullable()).nullable().optional().transform((value) => value ?? undefined)

const claudeModelSchema = z.object({
  key: z.string().trim().min(1),
  name: optionalText,
  score: optionalNumber,
  iq: optionalNumberSeries,
  pass: optionalNumberSeries,
  valid: optionalNumberSeries,
  invalid: optionalNumberSeries,
  cost: optionalNumberSeries,
  time: optionalNumberSeries,
  cache: optionalNumberSeries,
  latest_at: optionalText,
  latest_label: optionalText,
})

const claudePayloadSchema = z.object({
  updated_at: optionalText,
  labels: z.array(z.string()).nullable().optional().transform((value) => value ?? []),
  iq: z.object({
    updated_at: optionalText,
    models: z.array(claudeModelSchema).nullable().optional().transform((value) => value ?? []),
  }).nullable().optional().transform((value) => value ?? { models: [] }),
})

export type ClaudeRadarModel = z.infer<typeof claudeModelSchema> & { name: string }
export type ClaudeRadarPayload = {
  updated_at?: string
  labels: string[]
  iq: {
    updated_at?: string
    models: ClaudeRadarModel[]
  }
}

export function normalizeClaudeRadarPayload(value: unknown): ClaudeRadarPayload | null {
  const parsed = claudePayloadSchema.safeParse(value)
  if (!parsed.success) return null

  const keys = new Set<string>()
  const models: ClaudeRadarModel[] = []
  for (const model of parsed.data.iq.models) {
    if (keys.has(model.key)) return null
    keys.add(model.key)
    models.push({
      ...model,
      name: model.name?.trim() || model.key,
    })
  }

  return {
    updated_at: parsed.data.updated_at,
    labels: parsed.data.labels,
    iq: {
      updated_at: parsed.data.iq.updated_at,
      models,
    },
  }
}
