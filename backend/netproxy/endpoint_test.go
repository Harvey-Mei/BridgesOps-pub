package netproxy

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type endpointTestRoundTripperFunc func(*http.Request) (*http.Response, error)

func (fn endpointTestRoundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func TestEndpointURLNormalizesSOCKS5HAndEscapesCredentials(t *testing.T) {
	endpoint := Endpoint{Protocol: "socks5h", Host: "[2001:db8::1]", Port: 1080, Username: "u@x", Password: "p:/?"}
	got, err := endpoint.URL()
	if err != nil {
		t.Fatal(err)
	}
	if got != "socks5://u%40x:p%3A%2F%3F@[2001:db8::1]:1080" {
		t.Fatalf("URL = %q", got)
	}
}

func TestNewTransportDirectClearsEnvironmentProxy(t *testing.T) {
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.Proxy = http.ProxyFromEnvironment
	transport, err := NewTransport(base, "")
	if err != nil {
		t.Fatal(err)
	}
	if transport.Proxy != nil {
		t.Fatal("direct transport retained an environment proxy")
	}
}

func TestDoWithoutRedirectsReturnsEveryFirstHopLocationVerbatim(t *testing.T) {
	redirectedCalls := atomic.Int32{}
	checkRedirectCalls := atomic.Int32{}
	redirected := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirectedCalls.Add(1)
	}))
	defer redirected.Close()
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		checkRedirectCalls.Add(1)
		return nil
	}}

	for _, status := range []int{
		http.StatusMovedPermanently,
		http.StatusFound,
		http.StatusSeeOther,
		http.StatusTemporaryRedirect,
		http.StatusPermanentRedirect,
	} {
		for _, location := range []string{redirected.URL + "/replayed", "%gh"} {
			t.Run(strconv.Itoa(status)+"/"+location, func(t *testing.T) {
				initialCalls := atomic.Int32{}
				initial := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					initialCalls.Add(1)
					w.Header().Set("Location", location)
					w.Header().Set("X-First-Hop", "preserved")
					w.WriteHeader(status)
					_, _ = w.Write([]byte("first-hop-body"))
				}))
				defer initial.Close()

				request, err := http.NewRequest(http.MethodPost, initial.URL, strings.NewReader("body"))
				if err != nil {
					t.Fatal(err)
				}
				response, err := DoWithoutRedirects(client, request)
				if err != nil {
					t.Fatalf("do without redirects: %v", err)
				}
				defer response.Body.Close()
				raw, readErr := io.ReadAll(response.Body)
				if readErr != nil {
					t.Fatalf("read first-hop body: %v", readErr)
				}
				if response.StatusCode != status || response.Header.Get("Location") != location || response.Header.Get("X-First-Hop") != "preserved" || string(raw) != "first-hop-body" {
					t.Fatalf("response status/location=%d/%q, want %d/%q", response.StatusCode, response.Header.Get("Location"), status, location)
				}
				if initialCalls.Load() != 1 {
					t.Fatalf("initial calls=%d, want 1", initialCalls.Load())
				}
			})
		}
	}
	if redirectedCalls.Load() != 0 {
		t.Fatalf("redirect target calls=%d, want 0", redirectedCalls.Load())
	}
	if checkRedirectCalls.Load() != 0 {
		t.Fatalf("custom CheckRedirect calls=%d, want 0", checkRedirectCalls.Load())
	}
}

func TestDoWithoutRedirectsReturnsMalformedLocationForAll3xxStatuses(t *testing.T) {
	for status := 300; status < 400; status++ {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			client := &http.Client{Transport: endpointTestRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: status,
					Header:     http.Header{"Location": []string{"%gh"}, "X-First-Hop": []string{"preserved"}},
					Body:       io.NopCloser(strings.NewReader("first-hop-body")),
					Request:    request,
				}, nil
			})}
			request, err := http.NewRequest(http.MethodGet, "https://first-hop.invalid/request", nil)
			if err != nil {
				t.Fatal(err)
			}
			response, err := DoWithoutRedirects(client, request)
			if err != nil {
				t.Fatalf("status %d returned malformed Location error: %v", status, err)
			}
			defer response.Body.Close()
			raw, readErr := io.ReadAll(response.Body)
			if readErr != nil || string(raw) != "first-hop-body" {
				t.Fatalf("status %d body=%q err=%v", status, raw, readErr)
			}
			if response.StatusCode != status || response.Header.Get("Location") != "%gh" || response.Header.Get("X-First-Hop") != "preserved" {
				t.Fatalf("status %d response=%d headers=%v", status, response.StatusCode, response.Header)
			}
		})
	}
}

func TestDoWithoutRedirectsPreservesCookieJar(t *testing.T) {
	var requestCookie string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requestCookie = request.Header.Get("Cookie")
		w.Header().Set("Set-Cookie", "returned=stored; Path=/")
		w.Header().Set("Location", "%gh")
		w.WriteHeader(http.StatusFound)
	}))
	defer server.Close()

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	jar.SetCookies(request.URL, []*http.Cookie{{Name: "sent", Value: "from-jar"}})
	client := &http.Client{Jar: jar}
	response, err := DoWithoutRedirects(client, request)
	if err != nil {
		t.Fatalf("cookie-jar redirect: %v", err)
	}
	_ = response.Body.Close()
	if requestCookie != "sent=from-jar" || response.Header.Get("Location") != "%gh" {
		t.Fatalf("cookie-jar request/response=%q/%q", requestCookie, response.Header.Get("Location"))
	}
	foundReturned := false
	for _, cookie := range jar.Cookies(request.URL) {
		if cookie.Name == "returned" && cookie.Value == "stored" {
			foundReturned = true
		}
	}
	if !foundReturned {
		t.Fatalf("response cookie was not stored in jar: %v", jar.Cookies(request.URL))
	}
}

func TestDoWithoutRedirectsPreservesClientTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	}))
	defer server.Close()

	request, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	startedAt := time.Now()
	response, err := DoWithoutRedirects(&http.Client{Timeout: 30 * time.Millisecond}, request)
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	var netErr net.Error
	if err == nil || !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("client timeout error=%v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("client timeout took %s, want under 1s", elapsed)
	}
}

func TestSOCKS5TransportUsesProxyWithAuthentication(t *testing.T) {
	upstreamHit := atomic.Bool{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamHit.Store(true)
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	address, hits := startSOCKS5Server(t, "bridge", "secret")
	client, err := NewClient(&http.Client{Timeout: 3 * time.Second}, "socks5://bridge:secret@"+address)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Get(upstream.URL)
	if err != nil {
		t.Fatalf("request through SOCKS5: %v", err)
	}
	defer response.Body.Close()
	if _, err := io.ReadAll(response.Body); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !upstreamHit.Load() || !hits.Load() {
		t.Fatalf("status=%d upstream=%t socks=%t", response.StatusCode, upstreamHit.Load(), hits.Load())
	}
}

func TestParseURLNeverReturnsCredentialsInErrors(t *testing.T) {
	_, err := ParseURL("ftp://bridge:secret@example.com:21")
	if err == nil {
		t.Fatal("expected unsupported protocol error")
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("proxy password leaked in error: %v", err)
	}
}

func startSOCKS5Server(t *testing.T, username, password string) (string, *atomic.Bool) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hits := &atomic.Bool{}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go serveSOCKS5(conn, username, password, hits)
		}
	}()
	return listener.Addr().String(), hits
}

func serveSOCKS5(conn net.Conn, username, password string, hits *atomic.Bool) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	reader := bufio.NewReader(conn)
	header := make([]byte, 2)
	if _, err := io.ReadFull(reader, header); err != nil || header[0] != 5 {
		return
	}
	methods := make([]byte, int(header[1]))
	if _, err := io.ReadFull(reader, methods); err != nil {
		return
	}
	if _, err := conn.Write([]byte{5, 2}); err != nil {
		return
	}
	authHeader := make([]byte, 2)
	if _, err := io.ReadFull(reader, authHeader); err != nil || authHeader[0] != 1 {
		return
	}
	user := make([]byte, int(authHeader[1]))
	if _, err := io.ReadFull(reader, user); err != nil {
		return
	}
	passwordLength, err := reader.ReadByte()
	if err != nil {
		return
	}
	pass := make([]byte, int(passwordLength))
	if _, err := io.ReadFull(reader, pass); err != nil {
		return
	}
	if string(user) != username || string(pass) != password {
		_, _ = conn.Write([]byte{1, 1})
		return
	}
	if _, err := conn.Write([]byte{1, 0}); err != nil {
		return
	}

	request := make([]byte, 4)
	if _, err := io.ReadFull(reader, request); err != nil || request[0] != 5 || request[1] != 1 {
		return
	}
	host, ok := readSOCKS5Host(reader, request[3])
	if !ok {
		return
	}
	portBytes := make([]byte, 2)
	if _, err := io.ReadFull(reader, portBytes); err != nil {
		return
	}
	port := int(portBytes[0])<<8 | int(portBytes[1])
	upstream, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(port)), 3*time.Second)
	if err != nil {
		_, _ = conn.Write([]byte{5, 5, 0, 1, 0, 0, 0, 0, 0, 0})
		return
	}
	defer upstream.Close()
	if _, err := conn.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0}); err != nil {
		return
	}
	hits.Store(true)
	_ = conn.SetDeadline(time.Time{})
	go func() { _, _ = io.Copy(upstream, reader) }()
	_, _ = io.Copy(conn, upstream)
}

func readSOCKS5Host(reader *bufio.Reader, addressType byte) (string, bool) {
	switch addressType {
	case 1:
		ip := make([]byte, net.IPv4len)
		if _, err := io.ReadFull(reader, ip); err != nil {
			return "", false
		}
		return net.IP(ip).String(), true
	case 3:
		length, err := reader.ReadByte()
		if err != nil {
			return "", false
		}
		host := make([]byte, int(length))
		if _, err := io.ReadFull(reader, host); err != nil {
			return "", false
		}
		return string(host), true
	case 4:
		ip := make([]byte, net.IPv6len)
		if _, err := io.ReadFull(reader, ip); err != nil {
			return "", false
		}
		return net.IP(ip).String(), true
	default:
		return "", false
	}
}
