package netproxy

import (
	"encoding/json"
	"errors"
	"strings"
)

// Credentials is encrypted as one JSON blob by the caller. Keeping it separate
// from Endpoint lets storage safely expose protocol and host diagnostics while
// never returning proxy login material.
type Credentials struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

func (c Credentials) Empty() bool {
	return strings.TrimSpace(c.Username) == "" && c.Password == ""
}

func (c Credentials) Encode() (string, error) {
	if c.Empty() {
		return "", nil
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return "", errors.New("encode proxy credentials")
	}
	return string(raw), nil
}

func DecodeCredentials(raw string) (Credentials, error) {
	if strings.TrimSpace(raw) == "" {
		return Credentials{}, nil
	}
	var credentials Credentials
	if err := json.Unmarshal([]byte(raw), &credentials); err != nil {
		return Credentials{}, errors.New("decode proxy credentials")
	}
	return credentials, nil
}
