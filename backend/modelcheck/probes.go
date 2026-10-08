package modelcheck

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"
)

type probeEvidence struct {
	Kind             string `json:"kind"`
	ChallengeHash    string `json:"challenge_hash"`
	Prompt           string `json:"prompt"`
	Expected         string `json:"expected"`
	Response         string `json:"response,omitempty"`
	ActualModel      string `json:"actual_model,omitempty"`
	Passed           bool   `json:"passed"`
	Detail           string `json:"detail,omitempty"`
	Error            string `json:"error,omitempty"`
	PromptTokens     int64  `json:"prompt_tokens,omitempty"`
	CompletionTokens int64  `json:"completion_tokens,omitempty"`
	CacheReadTokens  int64  `json:"cache_read_tokens,omitempty"`
	TotalTokens      int64  `json:"total_tokens,omitempty"`
	FirstByteMS      int64  `json:"first_byte_ms,omitempty"`
	LatencyMS        int64  `json:"latency_ms,omitempty"`
	ArtifactSVG      string `json:"artifact_svg,omitempty"`
	Scene            string `json:"scene,omitempty"`
}

type challenge struct {
	kind      string
	label     string
	prompt    string
	expected  string
	hash      string
	maxTokens int
	scene     string
	evaluate  func(string) (bool, string)
}

func availableProbeCount() int { return 2 }

func buildChallenges(random io.Reader, count int) ([]challenge, error) {
	builders := []func(io.Reader) (challenge, error){
		buildCandyChallenge,
		buildPelicanChallenge,
	}
	if count < 1 {
		count = 1
	}
	if count > len(builders) {
		count = len(builders)
	}
	out := make([]challenge, 0, count)
	for _, build := range builders[:count] {
		item, err := build(random)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256([]byte(item.prompt))
		item.hash = hex.EncodeToString(sum[:])
		out = append(out, item)
	}
	return out, nil
}

func buildArithmeticChallenge(random io.Reader) (challenge, error) {
	a, err := randomRange(random, 17, 59)
	if err != nil {
		return challenge{}, err
	}
	b, err := randomRange(random, 4, 13)
	if err != nil {
		return challenge{}, err
	}
	c, err := randomRange(random, 21, 97)
	if err != nil {
		return challenge{}, err
	}
	d, err := randomRange(random, 8, 41)
	if err != nil {
		return challenge{}, err
	}
	nonce, err := randomNonce(random, 6)
	if err != nil {
		return challenge{}, err
	}
	answer := ((a * b) + c) - d
	expected := fmt.Sprintf(`{"nonce":%q,"answer":%d}`, nonce, answer)
	prompt := fmt.Sprintf("Solve the calculation in order: ((%d * %d) + %d) - %d. Return exactly one JSON object and nothing else, with this schema: {\"nonce\":\"%s\",\"answer\":INTEGER}. Do not use markdown or add fields.", a, b, c, d, nonce)
	return challenge{
		kind: "arithmetic_json", label: "随机算术与 JSON 格式", prompt: prompt, expected: expected, maxTokens: 80,
		evaluate: func(raw string) (bool, string) {
			var got struct {
				Nonce  string `json:"nonce"`
				Answer int    `json:"answer"`
			}
			if err := decodeStrictJSON(raw, &got); err != nil {
				return false, err.Error()
			}
			if got.Nonce != nonce || got.Answer != answer {
				return false, "nonce 或计算结果不正确"
			}
			return true, ""
		},
	}, nil
}

func buildOrderingChallenge(random io.Reader) (challenge, error) {
	order := []string{"Aster", "Birch", "Cedar", "Dune", "Elm"}
	for i := len(order) - 1; i > 0; i-- {
		j, err := randomInt(random, i+1)
		if err != nil {
			return challenge{}, err
		}
		order[i], order[j] = order[j], order[i]
	}
	facts := make([]string, 0, len(order)-1)
	for i := 0; i+1 < len(order); i++ {
		facts = append(facts, fmt.Sprintf("%s is before %s", order[i], order[i+1]))
	}
	for i := len(facts) - 1; i > 0; i-- {
		j, err := randomInt(random, i+1)
		if err != nil {
			return challenge{}, err
		}
		facts[i], facts[j] = facts[j], facts[i]
	}
	nonce, err := randomNonce(random, 6)
	if err != nil {
		return challenge{}, err
	}
	expectedBytes, _ := json.Marshal(struct {
		Nonce string   `json:"nonce"`
		Order []string `json:"order"`
	}{Nonce: nonce, Order: order})
	prompt := fmt.Sprintf("Infer the unique full order from these facts: %s. Return exactly one JSON object and nothing else, with this schema: {\"nonce\":\"%s\",\"order\":[\"first\",\"second\",\"third\",\"fourth\",\"fifth\"]}. Do not use markdown or add fields.", strings.Join(facts, "; "), nonce)
	expectedOrder := append([]string(nil), order...)
	return challenge{
		kind: "constraint_order", label: "约束排序", prompt: prompt, expected: string(expectedBytes), maxTokens: 120,
		evaluate: func(raw string) (bool, string) {
			var got struct {
				Nonce string   `json:"nonce"`
				Order []string `json:"order"`
			}
			if err := decodeStrictJSON(raw, &got); err != nil {
				return false, err.Error()
			}
			if got.Nonce != nonce || len(got.Order) != len(expectedOrder) {
				return false, "nonce 或排序长度不正确"
			}
			for i := range expectedOrder {
				if got.Order[i] != expectedOrder[i] {
					return false, "约束排序结果不正确"
				}
			}
			return true, ""
		},
	}, nil
}

func buildTransformChallenge(random io.Reader) (challenge, error) {
	token, err := randomLetters(random, 8)
	if err != nil {
		return challenge{}, err
	}
	nonce, err := randomNonce(random, 6)
	if err != nil {
		return challenge{}, err
	}
	reversed := reverseASCII(token)
	rotated := reversed[2:] + reversed[:2]
	answer := strings.ToUpper(rotated)
	expected := fmt.Sprintf(`{"nonce":%q,"value":%q}`, nonce, answer)
	prompt := fmt.Sprintf("Transform the token %q using all steps in order: (1) reverse it, (2) rotate the result left by 2 characters, (3) convert to uppercase. Return exactly one JSON object and nothing else, with this schema: {\"nonce\":\"%s\",\"value\":\"RESULT\"}. Do not use markdown or add fields.", token, nonce)
	return challenge{
		kind: "instruction_transform", label: "多步指令遵循", prompt: prompt, expected: expected, maxTokens: 80,
		evaluate: func(raw string) (bool, string) {
			var got struct {
				Nonce string `json:"nonce"`
				Value string `json:"value"`
			}
			if err := decodeStrictJSON(raw, &got); err != nil {
				return false, err.Error()
			}
			if got.Nonce != nonce || got.Value != answer {
				return false, "nonce 或多步转换结果不正确"
			}
			return true, ""
		},
	}, nil
}

func decodeStrictJSON(raw string, value any) error {
	decoder := json.NewDecoder(strings.NewReader(strings.TrimSpace(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("未返回要求的严格 JSON：%w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("JSON 后包含额外内容")
		}
		return fmt.Errorf("JSON 后包含额外内容：%w", err)
	}
	return nil
}

func randomRange(random io.Reader, low, high int) (int, error) {
	if high <= low {
		return low, nil
	}
	value, err := randomInt(random, high-low)
	return low + value, err
}

func randomInt(random io.Reader, max int) (int, error) {
	if max <= 1 {
		return 0, nil
	}
	value, err := randInt(random, big.NewInt(int64(max)))
	if err != nil {
		return 0, err
	}
	return int(value.Int64()), nil
}

var randInt = func(random io.Reader, max *big.Int) (*big.Int, error) {
	limitBytes := (max.BitLen() + 7) / 8
	buffer := make([]byte, limitBytes)
	for {
		if _, err := io.ReadFull(random, buffer); err != nil {
			return nil, err
		}
		if excess := uint(limitBytes*8 - max.BitLen()); excess > 0 {
			buffer[0] &= byte(0xff >> excess)
		}
		value := new(big.Int).SetBytes(buffer)
		if value.Cmp(max) < 0 {
			return value, nil
		}
	}
}

func randomNonce(random io.Reader, bytes int) (string, error) {
	buffer := make([]byte, bytes)
	if _, err := io.ReadFull(random, buffer); err != nil {
		return "", err
	}
	return hex.EncodeToString(buffer), nil
}

func randomLetters(random io.Reader, length int) (string, error) {
	const letters = "abcdefghjkmnpqrstuvwxyz"
	var builder strings.Builder
	builder.Grow(length)
	for range length {
		index, err := randomInt(random, len(letters))
		if err != nil {
			return "", err
		}
		builder.WriteByte(letters[index])
	}
	return builder.String(), nil
}

func reverseASCII(value string) string {
	bytes := []byte(value)
	for left, right := 0, len(bytes)-1; left < right; left, right = left+1, right-1 {
		bytes[left], bytes[right] = bytes[right], bytes[left]
	}
	return string(bytes)
}
