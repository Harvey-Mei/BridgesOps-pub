package notify

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

// Dispatcher 把单条事件 fan-out 到所有启用的通知渠道，并按 Policy 做去抖。
type Dispatcher struct {
	repo     *storage.Notifications
	cipher   *crypto.Cipher
	log      *slog.Logger
	mu       sync.RWMutex
	policy   Policy
	proxy    config.ProxyConfig
	cooldown CooldownStore
}

// NewDispatcher 用 *storage.Notifications 作为 CooldownStore 的具体实现，
// 跨重启的冷却记录持久化在 notification_cooldowns 表里。
//
// 如果上层需要注入 stub（比如单测），可以用 NewDispatcherWithCooldown。
func NewDispatcher(repo *storage.Notifications, cipher *crypto.Cipher, log *slog.Logger, policy Policy) *Dispatcher {
	return NewDispatcherWithCooldown(repo, cipher, log, policy, repo)
}

func NewDispatcherWithCooldown(repo *storage.Notifications, cipher *crypto.Cipher, log *slog.Logger, policy Policy, cooldown CooldownStore) *Dispatcher {
	if policy.SendMaxAttempts <= 0 {
		policy.SendMaxAttempts = 1
	}
	return &Dispatcher{
		repo:     repo,
		cipher:   cipher,
		log:      log,
		policy:   policy,
		cooldown: cooldown,
	}
}

// Policy 返回当前策略，便于调用方做条件分支（如是否走批量路径）。
func (d *Dispatcher) Policy() Policy {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.policy
}

func (d *Dispatcher) UpdatePolicy(policy Policy) {
	if policy.SendMaxAttempts <= 0 {
		policy.SendMaxAttempts = 1
	}
	d.mu.Lock()
	d.policy = policy
	d.mu.Unlock()
}

func (d *Dispatcher) UpdateProxyConfig(cfg config.ProxyConfig) {
	d.mu.Lock()
	d.proxy = cfg
	d.mu.Unlock()
}

func (d *Dispatcher) proxyURL() (string, error) {
	d.mu.RLock()
	cfg := d.proxy
	d.mu.RUnlock()
	return cfg.ActiveURL()
}

// Send 把消息发送到一个具体的渠道（用于"测试发送"按钮）。
// 不走 Policy 过滤 / 不走重试——测试场景要求快速反馈，失败立刻显示出来。
func (d *Dispatcher) Send(ctx context.Context, ch *storage.NotificationChannel, msg Message) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	msg = d.withNotificationPrefix(msg)
	cfgJSON, err := d.cipher.Decrypt(ch.ConfigCipher)
	if err != nil {
		return fmt.Errorf("decrypt config: %w", err)
	}
	n, err := Build(ch, cfgJSON)
	if err != nil {
		return err
	}
	if err := d.applyProxy(ch, n); err != nil {
		return err
	}
	err = n.Send(ctx, msg)
	d.logResult(ctx, ch.ID, msg, err)
	return err
}

// Dispatch 按事件类型广播到所有启用的通知渠道，返回累计错误（部分失败也会写日志）。
//
// 订阅过滤：渠道配置 Subscriptions 非空时，必须有任意一条订阅命中 msg 才发送；
// 空订阅列表（""/null/[]）视为"订阅一切"，向后兼容已有通知渠道。
//
// 去抖：balance_low 同渠道在 BalanceLowCooldown 内不重复推送，状态在数据库里持久化。
// 失败：按 SendMaxAttempts 进行指数退避重试。
func (d *Dispatcher) Dispatch(ctx context.Context, msg Message) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return d.fanout(ctx, msg, nil)
}

// DispatchRateBatch 把一次扫描收集到的多条 RateChange 按 Policy 合并 / 过滤后推送。
//
//   - 先按 MinChangePct 过滤掉小变动
//   - 然后对每个通知渠道：先用它自己的 Subscriptions 切片出它关心的 changes，
//     再按 BatchRateChanges 决定合并发送 1 条还是逐条发送
//
// 关键：合并消息只包含订阅匹配的子集，避免"全合并后 ModelName=” 被 groups 模式订阅过滤掉"的边界。
func (d *Dispatcher) DispatchRateBatch(ctx context.Context, channel *storage.Channel, changes []RateChange) error {
	return d.DispatchRateEventBatch(ctx, channel, storage.EventRateChanged, changes)
}

func (d *Dispatcher) DispatchRateEventBatch(ctx context.Context, channel *storage.Channel, event storage.NotificationEvent, changes []RateChange) error {
	if channel == nil || len(changes) == 0 {
		return nil
	}
	policy := d.Policy()

	filtered := make([]RateChange, 0, len(changes))
	for _, c := range changes {
		if event != storage.EventRateChanged || c.ChangePctAbove(policy.MinChangePct) {
			filtered = append(filtered, c)
		}
	}
	if len(filtered) == 0 {
		return nil
	}

	notifyChannels, err := d.repo.ListEnabledChannelsContext(ctx)
	if err != nil {
		return err
	}
	if len(notifyChannels) == 0 {
		return nil
	}

	var errs []error
	for i := range notifyChannels {
		nch := notifyChannels[i]
		subs, _ := ParseSubscriptions(nch.Subscriptions)

		// 切出该通知渠道关心的 changes 子集。
		matching := subsetForSubscriptions(channel.ID, event, filtered, subs)
		if len(matching) == 0 {
			continue
		}

		if policy.BatchRateChanges {
			merged := BuildRateBatchMessage(channel, event, matching)
			if err := d.sendOne(ctx, &nch, merged); err != nil {
				errs = append(errs, err)
			}
		} else {
			// 用户显式关掉合并：仍按订阅切片，但逐条发。
			for _, c := range matching {
				single := BuildRateBatchMessage(channel, event, []RateChange{c})
				if err := d.sendOne(ctx, &nch, single); err != nil {
					errs = append(errs, err)
				}
			}
		}
	}
	return errors.Join(errs...)
}

func (d *Dispatcher) DispatchRateStructureBatch(ctx context.Context, channel *storage.Channel, change RateStructureChange) error {
	if channel == nil || len(change.Added)+len(change.Removed) == 0 {
		return nil
	}
	notifyChannels, err := d.repo.ListEnabledChannelsContext(ctx)
	if err != nil {
		return err
	}
	if len(notifyChannels) == 0 {
		return nil
	}

	var errs []error
	for i := range notifyChannels {
		nch := notifyChannels[i]
		subs, _ := ParseSubscriptions(nch.Subscriptions)
		matching := RateStructureChange{
			Added:   subsetForSubscriptions(channel.ID, storage.EventRateStructureChanged, change.Added, subs),
			Removed: subsetForSubscriptions(channel.ID, storage.EventRateStructureChanged, change.Removed, subs),
		}
		if len(matching.Added)+len(matching.Removed) == 0 {
			continue
		}
		msg := BuildRateStructureMessage(channel, matching)
		if err := d.sendOne(ctx, &nch, msg); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// subsetForSubscriptions 把 changes 过滤成"匹配 subs 订阅规则"的子集。
// subs 为空（订阅一切）→ 返回全部。
func subsetForSubscriptions(upstreamID uint, event storage.NotificationEvent, changes []RateChange, subs []Subscription) []RateChange {
	if len(subs) == 0 {
		return changes
	}
	out := make([]RateChange, 0, len(changes))
	for _, c := range changes {
		stub := Message{
			Event:     event,
			ChannelID: upstreamID,
			ModelName: c.GroupName,
		}
		if AnyMatch(subs, stub) {
			out = append(out, c)
		}
	}
	return out
}

type contextCooldownStore interface {
	TryClaimCooldownContext(context.Context, uint, storage.NotificationEvent, time.Duration) (bool, time.Time, error)
	ReleaseCooldownClaimContext(context.Context, uint, storage.NotificationEvent, time.Time) error
}

type notificationCooldownClaim struct {
	store     contextCooldownStore
	channelID uint
	event     storage.NotificationEvent
	claimedAt time.Time
}

func (c *notificationCooldownClaim) release() {
	if c == nil || c.store == nil || c.claimedAt.IsZero() {
		return
	}
	releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = c.store.ReleaseCooldownClaimContext(releaseCtx, c.channelID, c.event, c.claimedAt)
}

// claimCooldown 判断是否要按 cooldown 跳过本次发送。生产存储返回一个
// 精确 claim token；如果 identity deadline 在任何网络发送前到期，调用方
// 会条件撤销该 token，避免旧账号占用新账号的通知冷却。
func (d *Dispatcher) claimCooldown(ctx context.Context, msg Message) (bool, *notificationCooldownClaim, error) {
	policy := d.Policy()
	if msg.ChannelID == 0 {
		return false, nil, nil
	}

	cooldown := time.Duration(0)
	switch msg.Event {
	case storage.EventBalanceLow:
		cooldown = policy.BalanceLowCooldown
	case storage.EventSubscriptionDailyLow, storage.EventSubscriptionWeeklyLow, storage.EventSubscriptionMonthlyLow, storage.EventSubscriptionExpiring:
		cooldown = policy.SubscriptionAlertCooldown
	default:
		return false, nil, nil
	}
	if cooldown <= 0 {
		return false, nil, nil
	}
	if d.cooldown == nil {
		return false, nil, nil
	}
	if err := ctx.Err(); err != nil {
		return false, nil, err
	}
	var (
		ok        bool
		claimedAt time.Time
		err       error
		claim     *notificationCooldownClaim
	)
	if store, supportsContext := d.cooldown.(contextCooldownStore); supportsContext {
		ok, claimedAt, err = store.TryClaimCooldownContext(ctx, msg.ChannelID, msg.Event, cooldown)
		if ok && !claimedAt.IsZero() {
			claim = &notificationCooldownClaim{store: store, channelID: msg.ChannelID, event: msg.Event, claimedAt: claimedAt}
		}
	} else {
		ok, err = d.cooldown.TryClaimCooldown(msg.ChannelID, msg.Event, cooldown)
	}
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return false, nil, contextErr
		}
		if d.log != nil {
			d.log.Warn("cooldown lookup failed, sending anyway",
				"err", err, "channel_id", msg.ChannelID, "event", msg.Event)
		}
		return false, nil, nil
	}
	if !ok && d.log != nil {
		d.log.Debug("notification suppressed by cooldown",
			"event", msg.Event,
			"channel_id", msg.ChannelID,
			"cooldown", cooldown,
		)
	}
	if !ok {
		return true, nil, nil
	}
	if contextErr := ctx.Err(); contextErr != nil {
		claim.release()
		return false, nil, contextErr
	}
	return false, claim, nil
}

// fanout 广播给所有启用的通知渠道（仅给 Dispatch 用，DispatchRateBatch 自己控订阅切片）。
//
// extraFilter 可选：用于在 ParseSubscriptions / AnyMatch 之后做额外裁剪；
// 当前没有调用方传入，保留参数位是为以后扩展。
func (d *Dispatcher) fanout(ctx context.Context, msg Message, extraFilter func(*storage.NotificationChannel) bool) error {
	channels, err := d.repo.ListEnabledChannelsContext(ctx)
	if err != nil {
		return err
	}
	if len(channels) == 0 {
		return nil
	}
	eligible := make([]storage.NotificationChannel, 0, len(channels))
	for i := range channels {
		ch := channels[i]
		subs, _ := ParseSubscriptions(ch.Subscriptions)
		if len(subs) > 0 && !AnyMatch(subs, msg) {
			continue
		}
		if extraFilter != nil && !extraFilter(&ch) {
			continue
		}
		eligible = append(eligible, ch)
	}
	if len(eligible) == 0 {
		return nil
	}
	suppressed, claim, err := d.claimCooldown(ctx, msg)
	if err != nil {
		return err
	}
	if suppressed {
		return nil
	}
	var errs []error
	anyNetworkAttempt := false
	for i := range eligible {
		attempted, sendErr := d.sendOneTracked(ctx, &eligible[i], msg)
		anyNetworkAttempt = anyNetworkAttempt || attempted
		if sendErr != nil {
			errs = append(errs, sendErr)
			if !attempted && !anyNetworkAttempt && ctx.Err() != nil {
				claim.release()
				return errors.Join(errs...)
			}
		}
	}
	if !anyNetworkAttempt && ctx.Err() != nil {
		claim.release()
	}
	return errors.Join(errs...)
}

// sendOne 给单个通知渠道发送一条消息，包含"解密配置 → 构造 Notifier → 重试发送 → 写日志"。
func (d *Dispatcher) sendOne(ctx context.Context, ch *storage.NotificationChannel, msg Message) error {
	_, err := d.sendOneTracked(ctx, ch, msg)
	return err
}

func (d *Dispatcher) sendOneTracked(ctx context.Context, ch *storage.NotificationChannel, msg Message) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	msg = d.withNotificationPrefix(msg)
	cfgJSON, err := d.cipher.Decrypt(ch.ConfigCipher)
	if err != nil {
		d.logResult(ctx, ch.ID, msg, err)
		return false, fmt.Errorf("decrypt %s: %w", ch.Name, err)
	}
	n, err := Build(ch, cfgJSON)
	if err != nil {
		d.logResult(ctx, ch.ID, msg, err)
		return false, fmt.Errorf("build %s: %w", ch.Name, err)
	}
	if err := d.applyProxy(ch, n); err != nil {
		d.logResult(ctx, ch.ID, msg, err)
		return false, fmt.Errorf("proxy %s: %w", ch.Name, err)
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	attempted, sendErr := d.sendWithRetry(ctx, ch.Name, n, msg)
	d.logResult(ctx, ch.ID, msg, sendErr)
	if sendErr != nil {
		return attempted, fmt.Errorf("send via %s: %w", ch.Name, sendErr)
	}
	return attempted, nil
}

func (d *Dispatcher) applyProxy(ch *storage.NotificationChannel, n Notifier) error {
	if ch == nil || !ch.ProxyEnabled {
		return nil
	}
	proxyURL, err := d.proxyURL()
	if err != nil {
		return err
	}
	if proxyURL == "" {
		return nil
	}
	setter, ok := n.(ProxySetter)
	if !ok {
		return nil
	}
	setter.SetProxy(proxyURL)
	return nil
}

func (d *Dispatcher) withNotificationPrefix(msg Message) Message {
	prefix := d.Policy().NotificationPrefix
	if prefix == "" || msg.Subject == "" {
		return msg
	}
	msg.Subject = prefix + msg.Subject
	return msg
}

// sendWithRetry 指数退避重试发送。
//
// 重试策略：
//   - 最多 SendMaxAttempts 次（含首发）
//   - 退避 = 2^(attempt-1) * 1s，上限 30s（即 1s / 2s / 4s / 8s / 16s / 30s ...）
//   - ctx 被取消时立即返回，不再等待
//
// 注意：所有错误都会重试，包括 "Telegram 401 unauthorized" 这类永久错误。
// 单用户场景下，简单胜过复杂；如果反复重试相同 401，反正最多 SendMaxAttempts 次就停了。
func (d *Dispatcher) sendWithRetry(ctx context.Context, channelName string, n Notifier, msg Message) (bool, error) {
	maxAttempts := d.Policy().SendMaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	var lastErr error
	attempted := false
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return attempted, err
		}
		attempted = true
		err := n.Send(ctx, msg)
		if err == nil {
			return true, nil
		}
		lastErr = err
		if attempt == maxAttempts {
			break
		}
		delay := backoffDelay(attempt)
		if d.log != nil {
			d.log.Warn("notify send failed, will retry",
				"channel", channelName,
				"attempt", attempt,
				"max_attempts", maxAttempts,
				"retry_in", delay,
				"err", err,
			)
		}
		select {
		case <-ctx.Done():
			return attempted, ctx.Err()
		case <-time.After(delay):
		}
	}
	return attempted, lastErr
}

// backoffDelay 指数退避：第 1 次重试等 1s，第 2 次等 2s，第 3 次等 4s …
// 上限 30s 避免重试链拉得过长。
func backoffDelay(attempt int) time.Duration {
	const maxDelay = 30 * time.Second
	delay := time.Duration(1<<uint(attempt-1)) * time.Second
	if delay > maxDelay || delay <= 0 {
		return maxDelay
	}
	return delay
}

func (d *Dispatcher) logResult(ctx context.Context, channelID uint, msg Message, sendErr error) {
	log := &storage.NotificationLog{
		ChannelID:         channelID,
		UpstreamChannelID: msg.ChannelID,
		Event:             msg.Event,
		Subject:           msg.Subject,
		Body:              msg.Body,
		Success:           sendErr == nil,
	}
	if sendErr != nil {
		log.ErrorMessage = sendErr.Error()
	}
	if err := d.repo.AppendLogContext(ctx, log); err != nil && d.log != nil && ctx.Err() == nil {
		d.log.Warn("append notification log", "err", err)
	}
}
