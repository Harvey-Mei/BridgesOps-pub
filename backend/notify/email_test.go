package notify

import (
	"context"
	"errors"
	"net"
	"net/mail"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestBuildEmailBodyUsesDisplayName(t *testing.T) {
	fromName := "BridgeOps 运维"
	from := "alerts@example.com"
	body := buildEmailBody(fromName, from, []string{"operator@example.com"}, "状态更新", "内容")
	want := "From: " + (&mail.Address{Name: fromName, Address: from}).String()
	if !strings.Contains(body, want) {
		t.Fatalf("email body missing display name header: %s", body)
	}
	if strings.Contains(body, "From: "+from+"\r\n") {
		t.Fatalf("email body used bare sender header: %s", body)
	}
}

func TestNewEmailRejectsUnsafeDisplayName(t *testing.T) {
	_, err := newEmail(`{"host":"smtp.example.com","port":465,"from":"alerts@example.com","from_name":"Ops\r\nBcc: attacker@example.com","to":["operator@example.com"]}`)
	if err == nil {
		t.Fatal("newEmail accepted a line break in from_name")
	}
}

func TestEmailSendCancelsBlockedSMTPBanner(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- conn
		}
	}()

	host, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split address: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}
	sender := &email{cfg: emailConfig{
		Host: host, Port: port, From: "alerts@example.com", To: []string{"operator@example.com"},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- sender.Send(ctx, Message{Subject: "blocked", Body: "blocked"})
	}()
	var serverConn net.Conn
	select {
	case conn := <-accepted:
		serverConn = conn
	case <-time.After(time.Second):
		t.Fatal("SMTP server did not accept the test connection")
	}
	defer serverConn.Close()
	startedAt := time.Now()
	cancel()
	select {
	case err = <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked SMTP error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked SMTP send did not stop after cancellation")
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("blocked SMTP canceled after %s", elapsed)
	}
}
