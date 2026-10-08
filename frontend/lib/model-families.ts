import catalogDocument from "../../modelcatalogdata/catalog.json"

type ModelFamilyCatalogEntry = (typeof catalogDocument.families)[number]

const families = catalogDocument.families as ModelFamilyCatalogEntry[]
const familyByCode = new Map(families.map((family) => [family.code, family]))
const aliasToCode = new Map<string, string>()

for (const family of families) {
  aliasToCode.set(family.code, family.code)
  for (const alias of family.aliases) aliasToCode.set(alias, family.code)
}

export function normalizeModelFamily(value: string | null | undefined) {
  const family = String(value ?? "").trim().toLowerCase()
  return aliasToCode.get(family) ?? family
}

export function modelFamilyLabel(value: string | null | undefined) {
  const family = normalizeModelFamily(value)
  return familyByCode.get(family)?.display_name ?? (family ? String(value).trim() : familyByCode.get("other")?.display_name ?? "Other")
}

export function modelFamilyIconName(value: string | null | undefined) {
  return familyByCode.get(normalizeModelFamily(value))?.icon ?? "generic"
}

export function modelFamilyBrandColor(value: string | null | undefined) {
  return familyByCode.get(normalizeModelFamily(value))?.brand_color ?? "#6B7280"
}

export function modelFamilyFromModelName(value: string | null | undefined) {
  const model = String(value ?? "").trim().toLowerCase()
  if (!model) return "other"
  for (const family of families) {
    if (!family.auto_assignable) continue
    if (family.match_contains.some((fragment) => model.includes(fragment.toLowerCase())) || containsModelToken(model, family.match_tokens)) {
      return family.code
    }
  }
  return "other"
}

function containsModelToken(value: string, tokens: string[]) {
  const words = value.split(/[^a-z0-9]+/).filter(Boolean)
  return words.some((word) => tokens.some((token) => word === token || word.startsWith(`${token}mini`) || word.startsWith(`${token}pro`)))
}
