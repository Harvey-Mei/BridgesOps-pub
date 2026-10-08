package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/Harvey-Mei/BridgeOps/backend/notify"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

func registerNotifications(g *gin.RouterGroup, d *Deps) {
	gpc := g.Group("/notifications/channels")
	gpc.GET("", func(c *gin.Context) {
		list, err := d.Notifies.ListChannels()
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		for i := range list {
			list[i].Config = notificationEditableConfig(d, &list[i])
		}
		c.JSON(http.StatusOK, gin.H{"data": list})
	})
	gpc.POST("", func(c *gin.Context) { createNotifyChannel(c, d) })
	gpc.PUT("/:id", func(c *gin.Context) { updateNotifyChannel(c, d) })
	gpc.DELETE("/:id", func(c *gin.Context) {
		id, err := uintParam(c, "id")
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		if err := d.Notifies.DeleteChannel(id); err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})
	gpc.POST("/:id/test", func(c *gin.Context) { testNotify(c, d) })

	g.GET("/notifications/logs", func(c *gin.Context) {
		page, pageSize, err := parsePageQuery(c)
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		list, total, err := d.Notifies.ListLogsPage(page, pageSize)
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		channels, err := d.Notifies.ListChannels()
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		channelMeta := make(map[uint]gin.H, len(channels))
		for _, ch := range channels {
			channelMeta[ch.ID] = gin.H{
				"channel_name": ch.Name,
				"channel_type": ch.Type,
			}
		}
		items := make([]gin.H, 0, len(list))
		for _, item := range list {
			meta := channelMeta[item.ChannelID]
			row := gin.H{
				"id":                  item.ID,
				"channel_id":          item.ChannelID,
				"upstream_channel_id": item.UpstreamChannelID,
				"event":               item.Event,
				"subject":             item.Subject,
				"body":                item.Body,
				"success":             item.Success,
				"error_message":       item.ErrorMessage,
				"sent_at":             item.SentAt,
			}
			for k, v := range meta {
				row[k] = v
			}
			items = append(items, row)
		}
		pages := 1
		if total > 0 {
			pages = int((total + int64(pageSize) - 1) / int64(pageSize))
		}
		c.JSON(http.StatusOK, gin.H{"data": gin.H{
			"items":     items,
			"total":     total,
			"page":      page,
			"page_size": pageSize,
			"pages":     pages,
		}})
	})
}

type notifyChannelInput struct {
	Name          string                          `json:"name" binding:"required"`
	Type          storage.NotificationChannelType `json:"type" binding:"required"`
	Config        string                          `json:"config"` // JSON string；编辑时可留空保留原值
	Subscriptions string                          `json:"subscriptions"`
	Enabled       bool                            `json:"enabled"`
	ProxyEnabled  bool                            `json:"proxy_enabled"`
}

// notificationEditableConfig exposes only fields that are useful to retain
// during an edit and safe to return to an authenticated browser. Credentials
// stay encrypted server-side and are merged unchanged when the browser omits
// them on update.
func notificationEditableConfig(d *Deps, channel *storage.NotificationChannel) map[string]any {
	if d == nil || d.Cipher == nil || channel == nil || channel.ConfigCipher == "" {
		return nil
	}
	plain, err := d.Cipher.Decrypt(channel.ConfigCipher)
	if err != nil {
		return nil
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(plain), &raw); err != nil {
		return nil
	}

	keys := []string{}
	switch channel.Type {
	case storage.NotifyTelegram:
		keys = []string{"chat_id"}
	case storage.NotifyWebhook:
		keys = []string{"method"}
	case storage.NotifyEmail:
		keys = []string{"host", "port", "username", "from", "from_name", "to", "use_tls"}
	case storage.NotifyServerChan3:
		keys = []string{"uid"}
	}
	if len(keys) == 0 {
		return nil
	}
	out := make(map[string]any, len(keys))
	for _, key := range keys {
		if value, ok := raw[key]; ok {
			out[key] = value
		}
	}
	return out
}

func mergeNotifyConfig(cipherText, incoming string, cipher interface {
	Encrypt(string) (string, error)
	Decrypt(string) (string, error)
}) (string, error) {
	var patch map[string]any
	if err := json.Unmarshal([]byte(incoming), &patch); err != nil {
		return "", err
	}
	if patch == nil {
		return "", errors.New("config must be a JSON object")
	}

	var current map[string]any
	plain, err := cipher.Decrypt(cipherText)
	if err != nil {
		return "", err
	}
	if err := json.Unmarshal([]byte(plain), &current); err != nil {
		return "", err
	}
	if current == nil {
		current = map[string]any{}
	}
	for key, value := range patch {
		current[key] = value
	}
	merged, err := json.Marshal(current)
	if err != nil {
		return "", err
	}
	return cipher.Encrypt(string(merged))
}

// normalizeSubscriptions 把输入的订阅 JSON 字符串规整为 "[]" 或合法订阅规则数组。
// 解析失败返回错误以便 API 返回 400。
func normalizeSubscriptions(raw string) (string, error) {
	if raw == "" || raw == "null" {
		return "[]", nil
	}
	var list []notify.Subscription
	if err := json.Unmarshal([]byte(raw), &list); err != nil {
		return "", err
	}
	out, err := json.Marshal(list)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func createNotifyChannel(c *gin.Context, d *Deps) {
	var in notifyChannelInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if in.Config == "" {
		fail(c, http.StatusBadRequest, errors.New("config is required"))
		return
	}
	subs, err := normalizeSubscriptions(in.Subscriptions)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	cipherCfg, err := d.Cipher.Encrypt(in.Config)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	ch := &storage.NotificationChannel{
		Name:          in.Name,
		Type:          in.Type,
		ConfigCipher:  cipherCfg,
		Subscriptions: subs,
		Enabled:       in.Enabled,
		ProxyEnabled:  in.ProxyEnabled,
	}
	if err := validateNotificationChannelConfig(ch, in.Config); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if err := d.Notifies.CreateChannel(ch); err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": ch})
}

func updateNotifyChannel(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	ch, err := d.Notifies.FindChannel(id)
	if err != nil {
		fail(c, http.StatusNotFound, err)
		return
	}
	var in notifyChannelInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	subs, err := normalizeSubscriptions(in.Subscriptions)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if in.Type != ch.Type && in.Config == "" {
		fail(c, http.StatusBadRequest, errors.New("changing notification channel type requires a complete new configuration"))
		return
	}
	configCipher := ch.ConfigCipher
	if in.Config != "" {
		if in.Type == ch.Type {
			cipherCfg, err := mergeNotifyConfig(ch.ConfigCipher, in.Config, d.Cipher)
			if err != nil {
				fail(c, http.StatusBadRequest, err)
				return
			}
			configCipher = cipherCfg
		} else {
			cipherCfg, err := d.Cipher.Encrypt(in.Config)
			if err != nil {
				fail(c, http.StatusInternalServerError, err)
				return
			}
			configCipher = cipherCfg
		}
	}
	plainConfig, err := d.Cipher.Decrypt(configCipher)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	validation := &storage.NotificationChannel{Type: in.Type}
	if err := validateNotificationChannelConfig(validation, plainConfig); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	ch.Name = in.Name
	ch.Type = in.Type
	ch.Enabled = in.Enabled
	ch.ProxyEnabled = in.ProxyEnabled
	ch.Subscriptions = subs
	ch.ConfigCipher = configCipher
	if err := d.Notifies.UpdateChannel(ch); err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": ch})
}

func validateNotificationChannelConfig(channel *storage.NotificationChannel, raw string) error {
	if strings.TrimSpace(raw) == "" {
		return errors.New("notification configuration is required")
	}
	if _, err := notify.Build(channel, raw); err != nil {
		return err
	}
	return nil
}

func testNotify(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	ch, err := d.Notifies.FindChannel(id)
	if err != nil {
		fail(c, http.StatusNotFound, err)
		return
	}
	msg := notify.Message{
		Subject: "测试通知",
		Body:    "这是一条来自 BridgeOps 的测试消息。",
	}
	if err := d.Dispatcher.Send(c.Request.Context(), ch, msg); err != nil {
		c.JSON(http.StatusOK, gin.H{"ok": false, "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}
