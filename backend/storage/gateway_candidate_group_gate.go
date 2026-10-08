package storage

import (
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// lockCurrentGenerationAssignmentKeyTx is the common structural admission
// gate for an administrator adding a current upstream group to a candidate
// group. Model facts deliberately do not participate here: membership is
// useful before a probe/model sync completes, while mapping changes validate
// those facts separately.
func lockCurrentGenerationAssignmentKeyTx(tx *gorm.DB, expected UpstreamGroupGeneration, change *GatewayUpstreamGroupChange) error {
	if tx == nil {
		return errors.New("gateway store is unavailable")
	}
	if !expected.Valid() {
		return ErrUpstreamGroupGenerationChanged
	}
	domain, err := lockUpstreamRoutingHealthDomainTx(tx, expected.ChannelID, expected.CanonicalGroupName, time.Now(), expected)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrUpstreamGroupAssignmentKeyUnavailable
	}
	if err != nil {
		return err
	}
	if change == nil {
		change, err = currentGenerationUpstreamGroupChangeTx(tx, expected)
		if err != nil {
			return err
		}
	}
	return validateCurrentGenerationStructuralAssignmentKey(expected, change, domain.Key)
}

// currentGenerationUpstreamGroupChangeTx supplies the same remote-key
// identity fence to direct candidate-group edits that change review already
// has. Older installations can have an identity before change history exists;
// in that case the structurally usable, trusted local key is still required.
func currentGenerationUpstreamGroupChangeTx(tx *gorm.DB, expected UpstreamGroupGeneration) (*GatewayUpstreamGroupChange, error) {
	if tx == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	var change GatewayUpstreamGroupChange
	err := tx.Clauses(clause.Locking{Strength: "SHARE"}).
		Where("upstream_group_id = ? AND presence_generation = ? AND kind = ?", expected.ID, expected.PresenceGeneration, "added").
		Order("id DESC").
		First(&change).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &change, nil
}

// validateCurrentGenerationStructuralAssignmentKey stays DB-free so both
// write paths share the exact same decision and its critical cases are cheap
// to exercise without a PostgreSQL test database.
func validateCurrentGenerationStructuralAssignmentKey(expected UpstreamGroupGeneration, change *GatewayUpstreamGroupChange, key *GatewayUpstreamKey) error {
	if !expected.Valid() {
		return ErrUpstreamGroupGenerationChanged
	}
	if key == nil || key.ChannelID != expected.ChannelID || !key.Enabled || key.Provisioning || strings.TrimSpace(key.APIKeyCipher) == "" {
		return ErrUpstreamGroupAssignmentKeyUnavailable
	}
	if change == nil {
		return nil
	}
	if change.ChannelID != expected.ChannelID || change.UpstreamGroupID == nil || change.PresenceGeneration == nil ||
		*change.UpstreamGroupID != expected.ID || *change.PresenceGeneration != expected.PresenceGeneration {
		return ErrUpstreamGroupGenerationChanged
	}
	return nil
}
