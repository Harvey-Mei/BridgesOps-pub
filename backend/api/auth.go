package api

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const authSessionCookieName = "uh_token"

func registerAuth(g *gin.RouterGroup, d *Deps) {
	g.POST("/auth/login", func(c *gin.Context) { login(c, d) })
	g.GET("/auth/me", func(c *gin.Context) { whoami(c, d) })
	g.POST("/auth/logout", func(c *gin.Context) {
		clearAuthSessionCookie(c)
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})
}

type loginInput struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

func login(c *gin.Context, d *Deps) {
	// 鉴权关闭：任何登录请求都直接成功；前端在 /auth/me 已经知道无需登录。
	authSvc := d.Runtime.CurrentAuth()
	if authSvc == nil {
		c.JSON(http.StatusOK, gin.H{
			"data": gin.H{
				"auth_disabled": true,
				"username":      "anonymous",
			},
		})
		return
	}
	var in loginInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	token, exp, err := authSvc.Login(in.Username, in.Password)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	setAuthSessionCookie(c, token, exp)
	c.JSON(http.StatusOK, gin.H{
		"data": gin.H{
			"expires_at": exp.Unix(),
			"username":   authSvc.Username(),
		},
	})
}

// whoami 既是"前端启动探测"接口也是"已登录信息"接口。
//
//   - 鉴权关闭 → 返回 {auth_disabled: true}，前端据此跳过登录页
//   - 鉴权开启但未带 token → 中间件已经在前面 401 拦截，根本走不到这里
//   - 鉴权开启 + 有效 token → 返回 username
func whoami(c *gin.Context, d *Deps) {
	if d.Runtime.CurrentAuth() == nil {
		c.JSON(http.StatusOK, gin.H{
			"data": gin.H{
				"auth_disabled": true,
				"username":      "anonymous",
			},
		})
		return
	}
	sub, _ := c.Get("authSubject")
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"username": sub}})
}

func setAuthSessionCookie(c *gin.Context, token string, expiresAt time.Time) {
	maxAge := int(time.Until(expiresAt).Seconds())
	if maxAge < 1 {
		maxAge = 1
	}
	http.SetCookie(c.Writer, &http.Cookie{
		Name:     authSessionCookieName,
		Value:    token,
		Path:     "/",
		Expires:  expiresAt,
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   authCookieSecure(c.Request),
		SameSite: http.SameSiteLaxMode,
	})
}

func clearAuthSessionCookie(c *gin.Context) {
	http.SetCookie(c.Writer, &http.Cookie{
		Name:     authSessionCookieName,
		Value:    "",
		Path:     "/",
		Expires:  time.Unix(1, 0),
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   authCookieSecure(c.Request),
		SameSite: http.SameSiteLaxMode,
	})
}

func authCookieSecure(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	forwardedProto := strings.Split(r.Header.Get("X-Forwarded-Proto"), ",")
	return len(forwardedProto) > 0 && strings.EqualFold(strings.TrimSpace(forwardedProto[0]), "https")
}
