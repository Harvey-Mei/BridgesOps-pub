package providerid

import (
	"errors"
	"strings"
	"testing"
)

func TestNormalizeProviderRequestIDUsesByteExact512Limit(t *testing.T) {
	maximumUTF8 := strings.Repeat("界", 170) + "aa"
	if len(maximumUTF8) != MaxBytes {
		t.Fatalf("maximum UTF-8 fixture bytes = %d, want %d", len(maximumUTF8), MaxBytes)
	}
	tests := []struct {
		name    string
		value   string
		want    string
		wantErr bool
	}{
		{name: "legacy 128 bytes", value: strings.Repeat("a", 128), want: strings.Repeat("a", 128)},
		{name: "maximum ASCII bytes", value: strings.Repeat("a", MaxBytes), want: strings.Repeat("a", MaxBytes)},
		{name: "maximum UTF-8 bytes", value: maximumUTF8, want: maximumUTF8},
		{name: "preserve transport whitespace", value: "  provider-id\t", want: "  provider-id\t"},
		{name: "513 ASCII bytes", value: strings.Repeat("a", MaxBytes+1), wantErr: true},
		{name: "512 bytes plus trailing space", value: strings.Repeat("a", MaxBytes) + " ", wantErr: true},
		{name: "513 UTF-8 bytes", value: strings.Repeat("界", 171), wantErr: true},
		{name: "invalid UTF-8", value: string([]byte{'a', 0xff, 'b'}), wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Normalize(test.value)
			if test.wantErr {
				if !errors.Is(err, ErrInvalid) || got != "" {
					t.Fatalf("Normalize() = %q, %v; want explicit invalid error without truncation", got, err)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("Normalize() = %q, %v; want exact %q", got, err, test.want)
			}
		})
	}
}

func TestNormalizeProviderRequestIDPreservesDistinctWhitespace(t *testing.T) {
	values := []string{
		"provider-id",
		" provider-id",
		"provider-id ",
		"\u00a0provider-id",
		"provider-id\u00a0",
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		normalized, err := Normalize(value)
		if err != nil {
			t.Fatalf("Normalize(%q): %v", value, err)
		}
		if normalized != value {
			t.Fatalf("Normalize(%q) = %q, want byte-exact preservation", value, normalized)
		}
		if _, duplicate := seen[normalized]; duplicate {
			t.Fatalf("distinct provider IDs collapsed to %q", normalized)
		}
		seen[normalized] = struct{}{}
	}
}

func TestNormalizeProviderRequestIDPreservesDistinctCommonPrefixes(t *testing.T) {
	prefix := strings.Repeat("p", MaxBytes-1)
	left, err := Normalize(prefix + "a")
	if err != nil {
		t.Fatal(err)
	}
	right, err := Normalize(prefix + "b")
	if err != nil {
		t.Fatal(err)
	}
	if left == right || left != prefix+"a" || right != prefix+"b" {
		t.Fatalf("common-prefix IDs were not preserved exactly: left=%q right=%q", left, right)
	}
}
