package api

import (
	"fmt"
	"html"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/Harvey-Mei/BridgeOps/backend/auth"
	"github.com/gin-gonic/gin"
)

const frontendLoginPath = "/login"

type frontendAuthProvider interface {
	CurrentAuth() *auth.Service
}

func registerFrontendLogin(r *gin.Engine, authProvider frontendAuthProvider) {
	r.GET(frontendLoginPath, func(c *gin.Context) {
		target := frontendLoginTarget(c.Query("next"))
		if !frontendAuthenticationEnabled(authProvider) || frontendRequestAuthorized(c.Request, authProvider) {
			c.Redirect(http.StatusSeeOther, target)
			return
		}
		renderFrontendLogin(c, http.StatusOK, "", target)
	})
	r.POST(frontendLoginPath, func(c *gin.Context) {
		target := frontendLoginTarget(c.PostForm("next"))
		if authProvider == nil {
			c.Redirect(http.StatusSeeOther, target)
			return
		}
		authSvc := authProvider.CurrentAuth()
		if authSvc == nil {
			c.Redirect(http.StatusSeeOther, target)
			return
		}
		token, expiresAt, err := authSvc.Login(strings.TrimSpace(c.PostForm("username")), c.PostForm("password"))
		if err != nil {
			renderFrontendLogin(c, http.StatusUnauthorized, "账号或密码错误", target)
			return
		}
		setAuthSessionCookie(c, token, expiresAt)
		c.Redirect(http.StatusSeeOther, target)
	})
}

func frontendAuthenticationEnabled(authProvider frontendAuthProvider) bool {
	return authProvider != nil && authProvider.CurrentAuth() != nil
}

func frontendRequestAuthorized(r *http.Request, authProvider frontendAuthProvider) bool {
	if authProvider == nil {
		return true
	}
	authSvc := authProvider.CurrentAuth()
	if authSvc == nil {
		return true
	}
	_, err := authSvc.AuthenticateRequest(r)
	return err == nil
}

func rejectFrontendRequest(c *gin.Context) {
	if frontendAssetRequest(c.Request.URL.Path) {
		c.Header("Cache-Control", "no-store")
		c.Header("X-Content-Type-Options", "nosniff")
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}
	target := frontendLoginTarget(c.Request.URL.Path)
	c.Header("Cache-Control", "no-store")
	c.Redirect(http.StatusFound, frontendLoginPath+"?next="+url.QueryEscape(target))
}

func frontendAssetRequest(requestPath string) bool {
	clean := strings.TrimPrefix(requestPath, "/")
	if clean == "" {
		return false
	}
	base := path.Base(clean)
	return strings.HasPrefix(clean, "assets/") || (base != "." && strings.Contains(base, "."))
}

func frontendLoginTarget(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || !strings.HasPrefix(value, "/") || strings.HasPrefix(value, "//") || strings.Contains(value, "\\") {
		return "/"
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.IsAbs() || parsed.Host != "" {
		return "/"
	}
	return value
}

func renderFrontendLogin(c *gin.Context, status int, failure, target string) {
	c.Header("Cache-Control", "no-store")
	c.Header("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
	c.Header("Referrer-Policy", "same-origin")
	c.Header("X-Content-Type-Options", "nosniff")
	failureMarkup := ""
	if failure != "" {
		failureMarkup = `<p class="error" role="alert">` + html.EscapeString(failure) + `</p>`
	}
	body := fmt.Sprintf(frontendLoginHTML, failureMarkup, html.EscapeString(target))
	c.Data(status, "text/html; charset=utf-8", []byte(body))
}

const frontendLoginHTML = `<!doctype html>
<html lang="zh-CN">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>BridgeOps</title>
    <style>
      :root { color-scheme: light dark; font-family: "Geist", ui-sans-serif, system-ui, sans-serif; --page: #fff; --text: #29384a; --muted: #63748a; --border: #dfe6ee; --input: #f7f9fc; --action: #4774aa; --action-text: #fff; --focus: rgba(71, 116, 170, .2); --panel: #f1f5fa; --pattern: rgba(71, 116, 170, .09); --error-bg: #fff4f4; --error-border: #f2caca; --error-text: #ad2f2f; }
      * { box-sizing: border-box; }
      body { background: var(--page); color: var(--text); margin: 0; }
      .auth-screen { display: grid; min-height: 100svh; }
.auth-brand-panel {
  position: relative;
  display: none;
  min-width: 0;
  align-items: center;
  overflow: hidden;
  background: #15283e;
  padding: 48px;
}

.auth-brand-panel::before {
  position: absolute;
  inset: 0;
  background-image: linear-gradient(rgb(174 202 234 / 0.035) 1px, transparent 1px), linear-gradient(90deg, rgb(174 202 234 / 0.035) 1px, transparent 1px);
  background-size: 64px 64px;
  content: "";
}

.auth-brand-panel::after {
  position: absolute;
  top: 15%%;
  left: 5%%;
  width: 80%%;
  aspect-ratio: 1;
  background: radial-gradient(circle, rgb(123 169 221 / 0.22), transparent 70%%);
  content: "";
  pointer-events: none;
}

.auth-brand-art {
  position: absolute;
  top: 8%%;
  left: 8%%;
  width: 120%%;
  aspect-ratio: 1;
  transform: translate(-50%%, -50%%);
  pointer-events: none;
}

.auth-orbit {
  position: absolute;
  border: 1.5px solid rgb(174 202 234 / 0.18);
  border-radius: 50%%;
}

.auth-orbit::before {
  position: absolute;
  top: -3px;
  left: 50%%;
  width: 8px;
  height: 8px;
  transform: translateX(-50%%);
  border-radius: 50%%;
  background: #c9ddf4;
  box-shadow: 0 0 18px rgb(157 196 242 / 0.45);
  content: "";
}

.auth-orbit-one { inset: 0; }
.auth-orbit-two { inset: 16%%; opacity: 0.75; }

.auth-wordmark {
  position: relative;
  z-index: 1;
  width: 100%%;
  color: #dbe8f7;
  font-size: clamp(5rem, 10.2vw, 11.5rem);
  font-weight: 700;
  line-height: 1;
  letter-spacing: -0.065em;
  text-align: right;
  user-select: none;
}

.auth-wordmark span {
  display: block;
}

.auth-wordmark span + span {
  color: #819cb9;
}

.auth-main-panel {
  position: relative;
  isolation: isolate;
  display: flex;
  min-width: 0;
  min-height: 100svh;
  align-items: center;
  justify-content: center;
  padding: 48px 24px;
  overflow: hidden;
  background-color: var(--panel);
  background-image: radial-gradient(var(--pattern) 0.8px, transparent 0.8px);
  background-size: 24px 24px;
}

.auth-panel-art {
  position: absolute;
  inset: 0;
  width: 100%%;
  height: 100%%;
  color: var(--muted);
  opacity: 0.16;
  pointer-events: none;
}

.auth-form-column {
  position: relative;
  z-index: 1;
  display: flex;
  width: 100%%;
  max-width: 400px;
  flex-direction: column;
  align-items: center;
  gap: 28px;
  padding: 32px;
  border: 1px solid var(--border);
  border-radius: 24px;
  background: var(--page);
  box-shadow: 0 12px 36px rgb(37 67 104 / 0.06);
}

.auth-mark {
  display: flex;
  width: 52px;
  height: 52px;
  flex-shrink: 0;
  border-radius: 14px;
  background: var(--action);
  color: var(--action-text);
}

.auth-mark svg { width: 100%%; height: 100%%; }
.auth-card { width: 100%%; }

.auth-form-header {
  display: flex;
  width: 100%%;
  align-items: center;
  gap: 16px;
  border-bottom: 1px solid var(--border);
  padding-bottom: 24px;
}

.auth-brand-name {
  min-width: 0;
  overflow-wrap: anywhere;
  font-size: 23px;
  font-weight: 600;
  letter-spacing: -0.04em;
}

@keyframes auth-orbit-turn {
  to { transform: rotate(360deg); }
}

@keyframes auth-word-drift {
  to { transform: translateY(-8px); }
}

@keyframes auth-light-drift {
  from { transform: translate(-5%%, 0); opacity: 0.45; }
  to { transform: translate(12%%, 18%%); opacity: 1; }
}

@media (min-width: 1024px) and (prefers-reduced-motion: no-preference) {
  .auth-orbit-one { animation: auth-orbit-turn 28s linear infinite; animation-delay: -8s; }
  .auth-orbit-two { animation: auth-orbit-turn 36s linear infinite reverse; animation-delay: -20s; }
  .auth-wordmark span { animation: auth-word-drift 7s ease-in-out infinite alternate; }
  .auth-wordmark span + span { animation-delay: -4s; }
  .auth-brand-panel::after { animation: auth-light-drift 14s ease-in-out infinite alternate; }
}

@media (min-width: 1024px) {
  .auth-screen {
    grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
  }

  .auth-brand-panel {
    display: flex;
  }

  .auth-main-panel {
    padding: 48px;
  }
}

@media (max-width: 479px) {
  .auth-main-panel {
    padding-top: 80px;
    padding-bottom: 64px;
  }

  .auth-form-column {
    padding: 28px 24px;
  }
}


      .visually-hidden { position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0; }
      form { display: flex; flex-direction: column; gap: 20px; margin: 0; }
      label { display: block; color: var(--muted); font-size: 12px; font-weight: 500; margin-bottom: 8px; }
      input { background: var(--input); border: 1px solid var(--border); border-radius: 12px; color: var(--text); font: inherit; font-size: 16px; height: 54px; padding: 10px 16px; width: 100%%; }
      input::placeholder { color: var(--muted); opacity: 1; }
      input:focus-visible { border-color: var(--muted); box-shadow: 0 0 0 3px var(--focus); outline: 0; }
      button { display: flex; align-items: center; justify-content: space-between; background: var(--action); border: 0; border-radius: 12px; color: var(--action-text); cursor: pointer; font: inherit; font-size: 14px; font-weight: 600; height: 52px; padding: 0 16px; width: 100%%; }
      button:hover { filter: brightness(.95); }
      button:focus-visible { outline: 2px solid var(--muted); outline-offset: 3px; }
      button svg { width: 18px; height: 18px; flex-shrink: 0; }
      .error { background: var(--error-bg); border: 1px solid var(--error-border); border-radius: 8px; color: var(--error-text); font-size: 14px; line-height: 1.5; margin: 0; padding: 10px 12px; }
      @media (min-width: 768px) { input { font-size: 14px; } }
      @media (prefers-color-scheme: dark) { :root { --page: #1f2935; --text: #e3ebf5; --muted: #a1b0c3; --border: #354456; --input: #253140; --action: #9bbde5; --action-text: #192b41; --focus: rgba(155, 189, 229, .25); --panel: #171e28; --pattern: rgba(155, 189, 229, .09); --error-bg: #3a1a22; --error-border: #7a3542; --error-text: #ffb4bd; } }
    </style>
  </head>
  <body>
    <div class="auth-screen">
      <aside class="auth-brand-panel" aria-hidden="true">
        <div class="auth-brand-art"><span class="auth-orbit auth-orbit-one"></span><span class="auth-orbit auth-orbit-two"></span></div>
        <div class="auth-wordmark"><span>BRIDGE</span><span>OPS</span></div>
      </aside>
      <main class="auth-main-panel">
        <svg class="auth-panel-art" viewBox="0 0 720 1000" preserveAspectRatio="xMidYMid slice" aria-hidden="true">
          <g fill="none" stroke="currentColor" stroke-width="1">
            <path d="M-40 210h130l100-110h220l110 110h240M-40 860h200l120-110h210l120 110h150"/>
            <circle cx="190" cy="100" r="7"/><circle cx="410" cy="100" r="7"/>
            <circle cx="280" cy="750" r="7"/><circle cx="610" cy="860" r="7"/>
          </g>
        </svg>
        <div class="auth-form-column">
          <div class="auth-form-header" aria-hidden="true">
            <div class="auth-mark">
              <svg viewBox="0 0 64 64" focusable="false">
                <g fill="none" stroke="currentColor" stroke-width="3.5" stroke-linecap="round" stroke-linejoin="round">
                  <path d="M14 46V34a18 18 0 0 1 36 0v12M8 38h48"/>
                  <path d="M24 26v12m16-12v12" stroke-width="2.5"/>
                </g>
              </svg>
            </div>
            <span class="auth-brand-name">BridgeOps</span>
          </div>
          <section class="auth-card" aria-labelledby="login-title">
            <h1 id="login-title" class="visually-hidden">BridgeOps 登录</h1>
            <form method="post" action="/login">
              <input type="hidden" name="next" value="%[2]s">
              <div>
                <label for="username">账号</label>
                <input id="username" name="username" autocomplete="username" autocapitalize="none" placeholder="输入账号" required>
              </div>
              <div>
                <label for="password">密码</label>
                <input id="password" name="password" type="password" autocomplete="current-password" placeholder="输入密码" required>
              </div>
              %[1]s
              <button type="submit"><span>登录</span><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M5 12h14m-6-6 6 6-6 6"/></svg></button>
            </form>
          </section>
        </div>
      </main>
    </div>
  </body>
</html>`
