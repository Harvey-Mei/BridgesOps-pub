package gateway

import (
	"context"
	"errors"
	"strings"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

// directProjectedModels returns the model allowlist that belongs to the
// selected runtime key. New direct rows carry their own credential identity;
// only pre-000070 legacy rows may fall back to the channel aggregate.
func directProjectedModels(channel storage.Channel, key storage.GatewayUpstreamKey) ([]string, bool) {
	if channel.CredentialMode != storage.CredentialModeDirect {
		return nil, false
	}
	if key.DirectCredentialID == nil {
		if !legacyDirectProjectedKey(key) {
			return nil, false
		}
		models, err := storage.ParseDirectModels(channel.DirectModels)
		return models, err == nil && len(models) > 0
	}
	if *key.DirectCredentialID == 0 || key.DirectCredentialRevision == 0 || strings.TrimSpace(key.DirectBaseURL) == "" {
		return nil, false
	}
	models := splitCSV(key.ModelIDs)
	return models, len(models) > 0
}

func legacyDirectProjectedKey(key storage.GatewayUpstreamKey) bool {
	return key.DirectCredentialID == nil && key.DirectCredentialRevision == 0 && strings.TrimSpace(key.DirectBaseURL) == "" &&
		canonicalGroupName(key.GroupName) == storage.DirectChannelGroupName
}

func directProjectedCredentialMatches(credential storage.ChannelDirectCredential, key storage.GatewayUpstreamKey) bool {
	if key.DirectCredentialID == nil || *key.DirectCredentialID == 0 || credential.ID != *key.DirectCredentialID {
		return false
	}
	if credential.Revision == 0 || credential.Revision != key.DirectCredentialRevision || !credential.Enabled {
		return false
	}
	if strings.TrimSpace(credential.APIKeyCipher) == "" || strings.TrimSpace(key.APIKeyCipher) == "" ||
		credential.APIKeyCipher != key.APIKeyCipher {
		return false
	}
	if canonicalGroupName(credential.GroupName) != canonicalGroupName(key.GroupName) {
		return false
	}
	if strings.TrimRight(strings.TrimSpace(credential.BaseURL), "/") != strings.TrimRight(strings.TrimSpace(key.DirectBaseURL), "/") {
		return false
	}
	models, err := storage.ParseDirectModels(credential.Models)
	if err != nil || len(models) == 0 {
		return false
	}
	projected := splitCSV(key.ModelIDs)
	if len(models) != len(projected) {
		return false
	}
	for index := range models {
		if models[index] != projected[index] {
			return false
		}
	}
	return true
}

// requireCurrentDirectProjectedCredential is called at the dispatch fence,
// while the channel operation lock is held. It makes a runtime row unusable if
// its administrator source was replaced, disabled, or cleared after the row
// was projected.
func (s *Service) requireCurrentDirectProjectedCredential(ctx context.Context, channel storage.Channel, key storage.GatewayUpstreamKey) error {
	if channel.CredentialMode != storage.CredentialModeDirect {
		return nil
	}
	if key.DirectCredentialID == nil {
		if legacyDirectProjectedKey(key) {
			return nil
		}
		return errors.New("direct credential source changed before dispatch")
	}
	if s == nil || s.Channels == nil {
		return errors.New("direct credential source is unavailable")
	}
	credentials, err := s.Channels.WithContext(ctx).ListDirectCredentials(channel.ID)
	if err != nil {
		return err
	}
	for _, credential := range credentials {
		if directProjectedCredentialMatches(credential, key) {
			return nil
		}
	}
	return errors.New("direct credential source changed before dispatch")
}
