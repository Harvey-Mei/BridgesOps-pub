package storage

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Harvey-Mei/BridgeOps/backend/modelcatalog"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (g *Gateway) ListKeyGroupRoutePolicies(keyGroupID uint) ([]GatewayKeyGroupRoutePolicy, error) {
	policies := []GatewayKeyGroupRoutePolicy{}
	query := g.db.Order("key_group_id ASC, gateway_app ASC, model_family ASC")
	if keyGroupID > 0 {
		query = query.Where("key_group_id = ?", keyGroupID)
	}
	return policies, query.Find(&policies).Error
}

func (g *Gateway) FindKeyGroupRoutePolicy(keyGroupID uint, app GatewayApp, modelFamily string) (*GatewayKeyGroupRoutePolicy, error) {
	var policy GatewayKeyGroupRoutePolicy
	err := g.db.Where("key_group_id = ? AND gateway_app = ? AND model_family = ? AND enabled = ?",
		keyGroupID, strings.ToLower(strings.TrimSpace(string(app))), strings.ToLower(strings.TrimSpace(modelFamily)), true).
		First(&policy).Error
	return &policy, err
}

func (g *Gateway) ApplyKeyGroupRoutePolicy(key *GatewayDownstreamKey) (bool, error) {
	if key == nil || key.KeyGroupID == 0 {
		return false, nil
	}
	policy, err := g.FindKeyGroupRoutePolicy(key.KeyGroupID, key.App, key.ModelFamily)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	applyKeyGroupRoutePolicy(key, policy)
	return true, nil
}

func (g *Gateway) SaveKeyGroupRoutePolicy(policy *GatewayKeyGroupRoutePolicy, expectedVersion uint64) error {
	if policy == nil || policy.KeyGroupID == 0 {
		return errors.New("key group route policy requires a key group")
	}
	if policy.ID != 0 && expectedVersion == 0 {
		return ErrGatewayExpectedVersionNeeded
	}
	policy.GatewayApp = strings.ToLower(strings.TrimSpace(policy.GatewayApp))
	policy.ModelFamily = strings.ToLower(strings.TrimSpace(policy.ModelFamily))
	policy.TargetMode = strings.ToLower(strings.TrimSpace(policy.TargetMode))
	if !stringIn(policy.GatewayApp, "codex", "claude", "gemini") {
		return errors.New("invalid key group gateway app")
	}
	compatible := modelcatalog.CompatibleApps([]string{policy.ModelFamily})
	if !containsString(compatible, policy.GatewayApp) {
		return fmt.Errorf("model family %q is not compatible with %s", policy.ModelFamily, policy.GatewayApp)
	}
	if policy.TargetMode == "candidate_group" {
		if policy.DefaultCandidateGroupID == nil || *policy.DefaultCandidateGroupID == 0 {
			return errors.New("candidate group policy requires a target candidate group")
		}
	} else if policy.TargetMode == "all_candidates" {
		policy.DefaultCandidateGroupID = nil
	} else {
		return errors.New("invalid key group route target mode")
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		if err := lockKeyGroup(tx, policy.KeyGroupID); err != nil {
			return err
		}
		if policy.DefaultCandidateGroupID != nil {
			if err := lockCandidateGroup(tx, *policy.DefaultCandidateGroupID); err != nil {
				return err
			}
		}
		if policy.ID == 0 {
			policy.PolicyVersion = 1
			return tx.Create(policy).Error
		}
		var current GatewayKeyGroupRoutePolicy
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, policy.ID).Error; err != nil {
			return err
		}
		if current.PolicyVersion != expectedVersion {
			return ErrGatewayRulesetChanged
		}
		policy.PolicyVersion = current.PolicyVersion + 1
		policy.CreatedAt = current.CreatedAt
		return tx.Save(policy).Error
	})
}

func (g *Gateway) DeleteKeyGroupRoutePolicy(id uint, expectedVersion uint64) error {
	if expectedVersion == 0 {
		return ErrGatewayExpectedVersionNeeded
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		var policy GatewayKeyGroupRoutePolicy
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&policy, id).Error; err != nil {
			return err
		}
		if policy.PolicyVersion != expectedVersion {
			return ErrGatewayRulesetChanged
		}
		return tx.Delete(&policy).Error
	})
}

func validateKeyGroupRoutePolicySnapshotTx(tx *gorm.DB, key *GatewayDownstreamKey) error {
	if key == nil || !key.ValidateRoutePolicySnapshot || key.CandidateTargetSource != "key_group_policy" {
		return nil
	}
	if key.RoutePolicyID == nil || key.RoutePolicyVersion == nil {
		return ErrGatewayRulesetChanged
	}
	var policy GatewayKeyGroupRoutePolicy
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&policy, *key.RoutePolicyID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrGatewayRulesetChanged
		}
		return err
	}
	if !policy.Enabled || policy.PolicyVersion != *key.RoutePolicyVersion || policy.KeyGroupID != key.KeyGroupID ||
		policy.GatewayApp != string(key.App) || policy.ModelFamily != key.ModelFamily || policy.TargetMode != key.CandidateTargetMode {
		return ErrGatewayRulesetChanged
	}
	if policy.TargetMode == "candidate_group" {
		if policy.DefaultCandidateGroupID == nil || *policy.DefaultCandidateGroupID != key.CandidateGroupID {
			return ErrGatewayRulesetChanged
		}
	} else if key.CandidateGroupID != 0 {
		return ErrGatewayRulesetChanged
	}
	return nil
}

func applyKeyGroupRoutePolicy(key *GatewayDownstreamKey, policy *GatewayKeyGroupRoutePolicy) {
	if key == nil || policy == nil {
		return
	}
	key.CandidateTargetMode = policy.TargetMode
	key.CandidateTargetSource = "key_group_policy"
	key.RoutePolicyID = &policy.ID
	version := policy.PolicyVersion
	key.RoutePolicyVersion = &version
	key.ValidateRoutePolicySnapshot = true
	if policy.TargetMode == "candidate_group" && policy.DefaultCandidateGroupID != nil {
		key.CandidateGroupID = *policy.DefaultCandidateGroupID
	} else {
		key.CandidateGroupID = 0
	}
}
