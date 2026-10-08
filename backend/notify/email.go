package notify

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/mail"
	"net/smtp"
	"strings"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func init() {
	Register(storage.NotifyEmail, func(raw string) (Notifier, error) { return newEmail(raw) })
}

type emailConfig struct {
	Host     string   `json:"host"`      // smtp.example.com
	Port     int      `json:"port"`      // 465 / 587
	Username string   `json:"username"`  // SMTP 用户名
	Password string   `json:"password"`  // SMTP 密码 / 授权码
	From     string   `json:"from"`      // 发件人（可与 Username 不同）
	FromName string   `json:"from_name"` // 发件人显示名
	To       []string `json:"to"`        // 收件人列表
	UseTLS   bool     `json:"use_tls"`   // 是否使用隐式 TLS（一般 465 端口）
}

type email struct{ cfg emailConfig }

func newEmail(raw string) (*email, error) {
	var cfg emailConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return nil, err
	}
	if cfg.Host == "" || cfg.Port == 0 || cfg.From == "" || len(cfg.To) == 0 {
		return nil, errors.New("email config requires host/port/from/to")
	}
	if strings.ContainsAny(cfg.FromName, "\r\n") {
		return nil, errors.New("email from_name contains an invalid line break")
	}
	from, err := mail.ParseAddress(cfg.From)
	if err != nil {
		return nil, fmt.Errorf("invalid email from address: %w", err)
	}
	cfg.From = from.Address
	return &email{cfg: cfg}, nil
}

func (e *email) Type() storage.NotificationChannelType { return storage.NotifyEmail }

func (e *email) Send(ctx context.Context, msg Message) error {
	if ctx == nil {
		ctx = context.Background()
	}
	addr := fmt.Sprintf("%s:%d", e.cfg.Host, e.cfg.Port)
	auth := smtp.PlainAuth("", e.cfg.Username, e.cfg.Password, e.cfg.Host)

	body := buildEmailBody(e.cfg.FromName, e.cfg.From, e.cfg.To, msg.Subject, msg.Body)
	sendCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	return sendSMTPContext(sendCtx, addr, e.cfg.Host, auth, e.cfg.From, e.cfg.To, []byte(body), e.cfg.UseTLS)
}

func buildEmailBody(fromName, from string, to []string, subject, body string) string {
	fromHeader := from
	if fromName != "" {
		fromHeader = (&mail.Address{Name: fromName, Address: from}).String()
	}
	headers := []string{
		"From: " + fromHeader,
		"To: " + strings.Join(to, ", "),
		"Subject: " + subject,
		"MIME-Version: 1.0",
		"Content-Type: text/plain; charset=UTF-8",
	}
	return strings.Join(headers, "\r\n") + "\r\n\r\n" + body
}

// sendSMTPContext owns the socket so cancellation closes the actual SMTP
// operation instead of merely abandoning a goroutine that may deliver later.
func sendSMTPContext(ctx context.Context, addr, host string, auth smtp.Auth, from string, to []string, body []byte, implicitTLS bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	rawConn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("smtp dial: %w", err)
	}
	conn := rawConn
	closeOnCancel := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = rawConn.Close()
		case <-closeOnCancel:
		}
	}()
	defer close(closeOnCancel)
	defer rawConn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = rawConn.SetDeadline(deadline)
	}

	if implicitTLS {
		tlsConn := tls.Client(rawConn, &tls.Config{ServerName: host})
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("smtp tls handshake: %w", err)
		}
		conn = tlsConn
	}

	client, err := smtp.NewClient(conn, host)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("smtp new client: %w", err)
	}
	defer client.Close()

	if !implicitTLS {
		if ok, _ := client.Extension("STARTTLS"); ok {
			if err := client.StartTLS(&tls.Config{ServerName: host}); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("smtp starttls: %w", err)
			}
		}
	}
	if auth != nil {
		if err := client.Auth(auth); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("smtp auth: %w", err)
		}
	}
	if err := client.Mail(from); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("smtp mail: %w", err)
	}
	for _, rcpt := range to {
		if err := client.Rcpt(rcpt); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("smtp rcpt %s: %w", rcpt, err)
		}
	}
	w, err := client.Data()
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("smtp data: %w", err)
	}
	if _, err := w.Write(body); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("smtp write: %w", err)
	}
	if err := w.Close(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("smtp close: %w", err)
	}
	return nil
}

// sendTLS 通过 SMTPS（隐式 TLS，常见于 465）发送邮件。
func sendTLS(addr, host string, auth smtp.Auth, from string, to []string, body []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	return sendSMTPContext(ctx, addr, host, auth, from, to, body, true)
}
