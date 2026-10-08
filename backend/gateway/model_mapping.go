package gateway

import (
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

type ModelMappingStrategy string

const (
	ModelMappingStrategyDisabled ModelMappingStrategy = "off"
	ModelMappingStrategyIQ       ModelMappingStrategy = "iq"
	ModelMappingStrategyCost     ModelMappingStrategy = "cost"
	ModelMappingStrategyTime     ModelMappingStrategy = "time"
	ModelMappingStrategyBalanced ModelMappingStrategy = "balanced"
	ModelMappingStrategyManual   ModelMappingStrategy = "manual"
)

var ErrModelMappingUnavailable = errors.New("no usable model mapping option")

// ModelMappingOption is one current radar configuration. Model and reasoning
// effort are sent to the upstream only after ordinary candidate selection.
type ModelMappingOption struct {
	ConfigKey       string
	Label           string
	Model           string
	ReasoningEffort string
	Score           *float64
	CostUSD         *float64
	WallSeconds     *float64
}

func NormalizeModelMappingStrategy(raw string) (ModelMappingStrategy, error) {
	switch ModelMappingStrategy(strings.ToLower(strings.TrimSpace(raw))) {
	case "", ModelMappingStrategyDisabled:
		return ModelMappingStrategyDisabled, nil
	case ModelMappingStrategyIQ, ModelMappingStrategyCost, ModelMappingStrategyTime, ModelMappingStrategyBalanced, ModelMappingStrategyManual:
		return ModelMappingStrategy(strings.ToLower(strings.TrimSpace(raw))), nil
	default:
		return "", errors.New("invalid model mapping strategy")
	}
}

func NormalizeModelMappingFamily(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "none", "off":
		return "", nil
	case "codex", "gpt", "openai", "openai-compatible":
		return string(storage.GatewayAppCodex), nil
	case "claude":
		return string(storage.GatewayAppClaude), nil
	default:
		return "", errors.New("model mapping family must be codex or claude")
	}
}

func DefaultModelMappingReasoningEffort(app storage.GatewayApp) string {
	switch app {
	case storage.GatewayAppCodex:
		return "high"
	default:
		return ""
	}
}

func NormalizeModelMappingReasoningEffort(app storage.GatewayApp, raw string) (string, error) {
	effort := strings.ToLower(strings.TrimSpace(raw))
	switch app {
	case storage.GatewayAppClaude:
		if _, ok := ClaudeThinkingBudget(effort); !ok {
			return "", errors.New("claude mapped thinking budget must be a positive integer")
		}
		return effort, nil
	case storage.GatewayAppCodex:
		switch effort {
		case "minimal", "low", "medium", "high", "xhigh", "max":
			return effort, nil
		default:
			return "", errors.New("mapped reasoning effort must be minimal, low, medium, high, xhigh, or max")
		}
	default:
		return "", errors.New("model mapping reasoning is unsupported for this gateway app")
	}
}

// ModelMappingFamilies keeps the radar eligibility rule in one place. A
// candidate must advertise the exact synchronized family; protocol affinity
// alone is not enough because a Codex-compatible upstream may serve Grok or
// other OpenAI-style models.
func ModelMappingFamilies(modelFamilies []string) []string {
	available := make(map[string]struct{}, 2)
	for _, value := range modelFamilies {
		family, err := NormalizeModelMappingFamily(value)
		if err != nil || family == "" {
			continue
		}
		available[family] = struct{}{}
	}
	out := make([]string, 0, len(available))
	for _, family := range []string{string(storage.GatewayAppCodex), string(storage.GatewayAppClaude)} {
		if _, ok := available[family]; ok {
			out = append(out, family)
		}
	}
	return out
}

func ModelMappingFamilySupported(modelFamilies []string, raw string) bool {
	family, err := NormalizeModelMappingFamily(raw)
	if err != nil || family == "" {
		return false
	}
	for _, available := range ModelMappingFamilies(modelFamilies) {
		if available == family {
			return true
		}
	}
	return false
}

// ClaudeThinkingBudget accepts only the native Claude thinking representation.
// Radar data normally has no Claude reasoning setting; this is used for a
// manual mapping so an arbitrary label such as "high" is never sent as an
// invalid budget.
func ClaudeThinkingBudget(raw string) (int64, bool) {
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	return value, err == nil && value > 0
}

// SelectModelMappingOption keeps the automatic policies explicit and
// deterministic. The balanced policy uses the mean of three percentile ranks,
// so score, USD, and elapsed seconds carry equal weight despite different units.
func SelectModelMappingOption(strategy ModelMappingStrategy, options []ModelMappingOption, fixedModel, fixedReasoningEffort string) (ModelMappingOption, error) {
	forcedReasoningEffort := strings.TrimSpace(fixedReasoningEffort)
	if strategy == ModelMappingStrategyManual {
		model := strings.TrimSpace(fixedModel)
		if model == "" {
			return ModelMappingOption{}, errors.New("manual model mapping requires a model")
		}
		return ModelMappingOption{Model: model, ReasoningEffort: forcedReasoningEffort}, nil
	}

	filtered := make([]ModelMappingOption, 0, len(options))
	for _, option := range options {
		if strings.TrimSpace(option.Model) == "" {
			continue
		}
		switch strategy {
		case ModelMappingStrategyIQ:
			if !finiteMappingNumber(option.Score) {
				continue
			}
		case ModelMappingStrategyCost:
			if !finiteMappingNumber(option.CostUSD) {
				continue
			}
		case ModelMappingStrategyTime:
			if !finiteMappingNumber(option.WallSeconds) {
				continue
			}
		case ModelMappingStrategyBalanced:
			if !finiteMappingNumber(option.Score) || !finiteMappingNumber(option.CostUSD) || !finiteMappingNumber(option.WallSeconds) {
				continue
			}
		default:
			return ModelMappingOption{}, ErrModelMappingUnavailable
		}
		filtered = append(filtered, option)
	}
	if len(filtered) == 0 {
		return ModelMappingOption{}, ErrModelMappingUnavailable
	}

	var selected ModelMappingOption
	if strategy == ModelMappingStrategyBalanced {
		selected = selectBalancedModelMapping(filtered)
	} else {
		sort.SliceStable(filtered, func(i, j int) bool {
			left, right := filtered[i], filtered[j]
			var comparison int
			switch strategy {
			case ModelMappingStrategyIQ:
				comparison = compareMappingNumber(left.Score, right.Score, true)
				if comparison == 0 {
					comparison = compareMappingNumber(left.CostUSD, right.CostUSD, false)
				}
				if comparison == 0 {
					comparison = compareMappingNumber(left.WallSeconds, right.WallSeconds, false)
				}
			case ModelMappingStrategyCost:
				comparison = compareMappingNumber(left.CostUSD, right.CostUSD, false)
				if comparison == 0 {
					comparison = compareMappingNumber(left.Score, right.Score, true)
				}
				if comparison == 0 {
					comparison = compareMappingNumber(left.WallSeconds, right.WallSeconds, false)
				}
			case ModelMappingStrategyTime:
				comparison = compareMappingNumber(left.WallSeconds, right.WallSeconds, false)
				if comparison == 0 {
					comparison = compareMappingNumber(left.Score, right.Score, true)
				}
				if comparison == 0 {
					comparison = compareMappingNumber(left.CostUSD, right.CostUSD, false)
				}
			}
			if comparison != 0 {
				return comparison < 0
			}
			return mappingOptionKey(left) < mappingOptionKey(right)
		})
		selected = filtered[0]
	}
	if forcedReasoningEffort != "" {
		selected.ReasoningEffort = forcedReasoningEffort
	}
	return selected, nil
}

func (s *Service) FilterCandidatesForMappedModel(candidates []Candidate, model string) ([]Candidate, []CandidateExclusion, error) {
	model = strings.TrimSpace(model)
	if model == "" {
		return candidates, nil, nil
	}
	compatible := make([]Candidate, 0, len(candidates))
	exclusions := make([]CandidateExclusion, 0)
	for _, candidate := range candidates {
		// A mapped request must only use a candidate with synchronized, exact
		// model capability. Treat an empty list as unknown instead of guessing
		// from the protocol or model family.
		if len(candidate.ModelIDs) == 0 {
			exclusions = append(exclusions, newCandidateExclusion(candidate,
				candidateExclusionReasonModelCapabilityUnknown, model))
			continue
		}
		modelListed := modelIDSupportsRequest(candidate.ModelIDs, model, candidate.App)
		if candidate.Channel.CredentialMode == storage.CredentialModeDirect {
			modelListed = directModelIDSupportsRequest(candidate.ModelIDs, model)
		}
		if !modelListed {
			exclusions = append(exclusions, newCandidateExclusion(candidate,
				candidateExclusionReasonModelNotListed, model))
			continue
		}
		if s != nil && s.Store != nil {
			modelFence, fenced := ModelFactFenceForCandidate(candidate)
			if !fenced {
				exclusions = append(exclusions, newCandidateExclusion(candidate,
					candidateExclusionReasonModelCapabilityUnknown, model))
				continue
			}
			unsupported, err := s.Store.IsModelUnsupportedForIdentity(modelFence, model, modelCapabilityMaxAge)
			if err != nil {
				return nil, nil, err
			}
			if unsupported {
				exclusions = append(exclusions, newCandidateExclusion(candidate,
					candidateExclusionReasonModelNotSupported, model))
				continue
			}
		}
		compatible = append(compatible, candidate)
	}
	return compatible, exclusions, nil
}

func selectBalancedModelMapping(options []ModelMappingOption) ModelMappingOption {
	type ranked struct {
		option ModelMappingOption
		score  float64
	}
	scores := make([]float64, len(options))
	costs := make([]float64, len(options))
	times := make([]float64, len(options))
	for i, option := range options {
		scores[i] = *option.Score
		costs[i] = *option.CostUSD
		times[i] = *option.WallSeconds
	}
	rankedOptions := make([]ranked, len(options))
	for i, option := range options {
		rankedOptions[i] = ranked{
			option: option,
			score:  (mappingPercentile(scores, i, true) + mappingPercentile(costs, i, false) + mappingPercentile(times, i, false)) / 3,
		}
	}
	sort.SliceStable(rankedOptions, func(i, j int) bool {
		left, right := rankedOptions[i], rankedOptions[j]
		if left.score != right.score {
			return left.score > right.score
		}
		if comparison := compareMappingNumber(left.option.Score, right.option.Score, true); comparison != 0 {
			return comparison < 0
		}
		if comparison := compareMappingNumber(left.option.CostUSD, right.option.CostUSD, false); comparison != 0 {
			return comparison < 0
		}
		if comparison := compareMappingNumber(left.option.WallSeconds, right.option.WallSeconds, false); comparison != 0 {
			return comparison < 0
		}
		return mappingOptionKey(left.option) < mappingOptionKey(right.option)
	})
	return rankedOptions[0].option
}

func mappingPercentile(values []float64, index int, higherIsBetter bool) float64 {
	if len(values) <= 1 {
		return 1
	}
	better := 0
	equal := 0
	for _, value := range values {
		if nearlyEqual(value, values[index]) {
			equal++
			continue
		}
		if (higherIsBetter && value < values[index]) || (!higherIsBetter && value > values[index]) {
			better++
		}
	}
	return (float64(better) + float64(equal-1)/2) / float64(len(values)-1)
}

func compareMappingNumber(left, right *float64, higherIsBetter bool) int {
	leftValid := finiteMappingNumber(left)
	rightValid := finiteMappingNumber(right)
	if !leftValid || !rightValid {
		switch {
		case leftValid:
			return -1
		case rightValid:
			return 1
		default:
			return 0
		}
	}
	if nearlyEqual(*left, *right) {
		return 0
	}
	if higherIsBetter {
		if *left > *right {
			return -1
		}
		return 1
	}
	if *left < *right {
		return -1
	}
	return 1
}

func finiteMappingNumber(value *float64) bool {
	return value != nil && !math.IsNaN(*value) && !math.IsInf(*value, 0)
}

func nearlyEqual(left, right float64) bool {
	return math.Abs(left-right) < 1e-9
}

func mappingOptionKey(option ModelMappingOption) string {
	return strings.ToLower(strings.TrimSpace(option.ConfigKey) + "\x00" + strings.TrimSpace(option.Model) + "\x00" + strings.TrimSpace(option.ReasoningEffort))
}
