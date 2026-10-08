package modelcatalog

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"

	"github.com/Harvey-Mei/BridgeOps/modelcatalogdata"
)

type Family struct {
	Code           string   `json:"code"`
	DisplayName    string   `json:"display_name"`
	Aliases        []string `json:"aliases"`
	CompatibleApps []string `json:"compatible_apps"`
	Icon           string   `json:"icon"`
	BrandColor     string   `json:"brand_color"`
	AutoAssignable bool     `json:"auto_assignable"`
	MatchContains  []string `json:"match_contains"`
	MatchTokens    []string `json:"match_tokens"`
}

type catalogDocument struct {
	Revision string   `json:"revision"`
	Families []Family `json:"families"`
}

var catalog = loadCatalog()

func loadCatalog() catalogDocument {
	var document catalogDocument
	if err := json.Unmarshal(modelcatalogdata.JSON, &document); err != nil {
		panic("invalid embedded model family catalog: " + err.Error())
	}
	if strings.TrimSpace(document.Revision) == "" || len(document.Families) == 0 {
		panic("embedded model family catalog is empty")
	}
	return document
}

func Version() string {
	sum := sha256.Sum256(modelcatalogdata.JSON)
	return hex.EncodeToString(sum[:])
}

func Families() []Family {
	result := make([]Family, len(catalog.Families))
	copy(result, catalog.Families)
	for index := range result {
		result[index].Aliases = append([]string(nil), result[index].Aliases...)
		result[index].CompatibleApps = append([]string(nil), result[index].CompatibleApps...)
		result[index].MatchContains = append([]string(nil), result[index].MatchContains...)
		result[index].MatchTokens = append([]string(nil), result[index].MatchTokens...)
	}
	return result
}

func FamilyOrder() []string {
	result := make([]string, 0, len(catalog.Families))
	for _, family := range catalog.Families {
		result = append(result, family.Code)
	}
	return result
}

func FamilyFromModel(name string) string {
	matches := FamiliesFromModel(name)
	if len(matches) == 0 {
		return "other"
	}
	return matches[0]
}

func FamiliesFromModel(name string) []string {
	text := strings.ToLower(strings.TrimSpace(name))
	if text == "" {
		return nil
	}
	out := make([]string, 0)
	for _, family := range catalog.Families {
		if !family.AutoAssignable {
			continue
		}
		matched := false
		for _, fragment := range family.MatchContains {
			if strings.Contains(text, strings.ToLower(fragment)) {
				matched = true
				break
			}
		}
		if !matched && len(family.MatchTokens) > 0 {
			matched = containsModelToken(text, family.MatchTokens)
		}
		if matched {
			out = append(out, family.Code)
		}
	}
	return out
}

func CompatibleApps(families []string) []string {
	requested := make(map[string]struct{}, len(families))
	for _, family := range families {
		requested[strings.ToLower(strings.TrimSpace(family))] = struct{}{}
	}
	seen := make(map[string]struct{})
	for _, family := range catalog.Families {
		if _, ok := requested[family.Code]; !ok {
			continue
		}
		for _, app := range family.CompatibleApps {
			seen[app] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for _, app := range []string{"codex", "claude", "gemini"} {
		if _, ok := seen[app]; ok {
			out = append(out, app)
		}
	}
	return out
}

func SortFamilies(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value != "" {
			seen[value] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for _, family := range catalog.Families {
		if _, ok := seen[family.Code]; ok {
			out = append(out, family.Code)
			delete(seen, family.Code)
		}
	}
	remaining := make([]string, 0, len(seen))
	for value := range seen {
		remaining = append(remaining, value)
	}
	sort.Strings(remaining)
	return append(out, remaining...)
}

func containsModelToken(text string, tokens []string) bool {
	fields := strings.FieldsFunc(text, func(r rune) bool {
		return !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'))
	})
	for _, field := range fields {
		for _, token := range tokens {
			token = strings.ToLower(strings.TrimSpace(token))
			if field == token || strings.HasPrefix(field, token+"mini") || strings.HasPrefix(field, token+"pro") {
				return true
			}
		}
	}
	return false
}
