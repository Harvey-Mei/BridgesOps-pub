package channel

import (
	"context"
	"errors"
	"fmt"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func (s *Service) lockOrdinaryChannelOperation(ctx context.Context, channelID uint) (context.Context, func(), error) {
	if s == nil || s.Channels == nil || ctx == nil || channelID == 0 {
		return ctx, nil, errors.New("channel operation is unavailable")
	}
	if storage.ChannelOperationLockHeld(ctx, channelID) {
		if err := s.Channels.RequireOrdinaryChannelWrite(ctx, channelID); err != nil {
			return ctx, nil, err
		}
		return ctx, func() {}, nil
	}
	unlock, err := s.Channels.LockChannelOperation(ctx, channelID)
	if err != nil {
		return ctx, nil, err
	}
	ctx = storage.WithChannelOperationLockHeld(ctx, channelID)
	if err := s.Channels.RequireOrdinaryChannelWrite(ctx, channelID); err != nil {
		unlock()
		return ctx, nil, err
	}
	return ctx, unlock, nil
}

// channelDeletionConnectorCall resolves only the credential and session frozen
// by deletion prepare. It deliberately does not login, refresh, discover an API
// origin, persist a session, or clear monitor errors while deletion is pending.
func (s *Service) channelDeletionConnectorCall(
	ctx context.Context,
	channelID uint,
	deletionEpoch uint64,
	identityRevision uint64,
) (*connector.Channel, connector.Connector, *connector.AuthSession, error) {
	if s == nil || s.Channels == nil || s.AuthSessions == nil || s.Cipher == nil || ctx == nil {
		return nil, nil, nil, errors.New("channel deletion connector is unavailable")
	}
	channel, err := s.Channels.RequirePendingDeletionFenceContext(ctx, channelID, deletionEpoch, identityRevision)
	if err != nil {
		return nil, nil, nil, err
	}
	resolved, err := s.Resolve(ctx, channel)
	if err != nil {
		return nil, nil, nil, err
	}
	conn, err := connector.For(connector.ChannelType(channel.Type))
	if err != nil {
		return nil, nil, nil, err
	}
	s.applyHTTPConfig(conn)
	if err := applyProxy(conn, resolved); err != nil {
		return nil, nil, nil, err
	}

	var session *connector.AuthSession
	if channel.CredentialMode == storage.CredentialModeToken {
		session, err = s.buildSessionFromToken(channel)
	} else {
		var saved *storage.AuthSession
		saved, err = s.AuthSessions.FindByChannel(channelID)
		if err == nil && saved == nil {
			err = errors.New("frozen channel auth session is missing")
		}
		if err == nil {
			session, err = s.decryptSession(saved)
		}
	}
	if err != nil {
		return nil, nil, nil, err
	}
	if session == nil {
		return nil, nil, nil, errors.New("frozen channel auth session is empty")
	}
	if _, err := s.Channels.RequirePendingDeletionFenceContext(ctx, channelID, deletionEpoch, identityRevision); err != nil {
		return nil, nil, nil, err
	}
	return resolved, conn, session, nil
}

func (s *Service) ListAPIKeysForChannelDeletion(ctx context.Context, channelID uint, deletionEpoch, identityRevision uint64, query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
	resolved, conn, session, err := s.channelDeletionConnectorCall(ctx, channelID, deletionEpoch, identityRevision)
	if err != nil {
		return nil, err
	}
	page, err := conn.ListAPIKeys(ctx, resolved, session, query)
	if err != nil {
		return nil, err
	}
	if _, err := s.Channels.RequirePendingDeletionFenceContext(ctx, channelID, deletionEpoch, identityRevision); err != nil {
		return nil, err
	}
	return page, nil
}

func (s *Service) ListAPIKeyGroupsForChannelDeletion(ctx context.Context, channelID uint, deletionEpoch, identityRevision uint64) ([]connector.APIKeyGroup, error) {
	resolved, conn, session, err := s.channelDeletionConnectorCall(ctx, channelID, deletionEpoch, identityRevision)
	if err != nil {
		return nil, err
	}
	groups, err := conn.ListAPIKeyGroups(ctx, resolved, session)
	if err != nil {
		return nil, err
	}
	if _, err := s.Channels.RequirePendingDeletionFenceContext(ctx, channelID, deletionEpoch, identityRevision); err != nil {
		return nil, err
	}
	return groups, nil
}

func (s *Service) RevealAPIKeyForChannelDeletion(ctx context.Context, channelID uint, deletionEpoch, identityRevision uint64, keyID int64) (string, error) {
	if keyID <= 0 {
		return "", errors.New("upstream api key id is required")
	}
	resolved, conn, session, err := s.channelDeletionConnectorCall(ctx, channelID, deletionEpoch, identityRevision)
	if err != nil {
		return "", err
	}
	plain, err := conn.RevealAPIKey(ctx, resolved, session, keyID)
	if err != nil {
		return "", err
	}
	if _, err := s.Channels.RequirePendingDeletionFenceContext(ctx, channelID, deletionEpoch, identityRevision); err != nil {
		return "", err
	}
	return plain, nil
}

// PrepareAPIKeyDeletionForChannelDeletion resolves the frozen remote call
// before a storage claim is rechecked. The callback repeats the deletion fence
// on both sides of DELETE; a post-call fence failure makes the result unknown.
func (s *Service) PrepareAPIKeyDeletionForChannelDeletion(ctx context.Context, channelID uint, deletionEpoch, identityRevision uint64, keyID int64) (func() error, error) {
	if keyID <= 0 {
		return nil, errors.New("upstream api key id is required")
	}
	resolved, conn, session, err := s.channelDeletionConnectorCall(ctx, channelID, deletionEpoch, identityRevision)
	if err != nil {
		return nil, err
	}
	return func() error {
		if _, err := s.Channels.RequirePendingDeletionFenceContext(ctx, channelID, deletionEpoch, identityRevision); err != nil {
			return err
		}
		deleteErr := conn.DeleteAPIKey(ctx, resolved, session, keyID)
		if _, fenceErr := s.Channels.RequirePendingDeletionFenceContext(ctx, channelID, deletionEpoch, identityRevision); fenceErr != nil {
			return errors.Join(deleteErr, fmt.Errorf("verify deletion fence after remote DELETE: %w", fenceErr))
		}
		return deleteErr
	}, nil
}
