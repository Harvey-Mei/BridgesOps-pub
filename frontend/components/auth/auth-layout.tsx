import type { ReactNode } from "react"

export function AuthLayout({ children, title = "BridgeOps" }: { children: ReactNode; title?: string }) {
  return (
    <div className="auth-screen">
      <aside className="auth-brand-panel" aria-hidden="true">
        <div className="auth-brand-art">
          <span className="auth-orbit auth-orbit-one" />
          <span className="auth-orbit auth-orbit-two" />
        </div>
        <div className="auth-wordmark">
          <span>BRIDGE</span>
          <span>OPS</span>
        </div>
      </aside>
      <main className="auth-main-panel">
        <svg className="auth-panel-art" viewBox="0 0 720 1000" preserveAspectRatio="xMidYMid slice" aria-hidden="true">
          <g fill="none" stroke="currentColor" strokeWidth="1">
            <path d="M-40 210h130l100-110h220l110 110h240M-40 860h200l120-110h210l120 110h150" />
            <circle cx="190" cy="100" r="7" />
            <circle cx="410" cy="100" r="7" />
            <circle cx="280" cy="750" r="7" />
            <circle cx="610" cy="860" r="7" />
          </g>
        </svg>
        <div className="auth-form-column">
          <div className="auth-form-header" aria-hidden="true">
            <div className="auth-mark">
              <svg viewBox="0 0 64 64" focusable="false">
                <g fill="none" stroke="currentColor" strokeWidth="3.5" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M14 46V34a18 18 0 0 1 36 0v12M8 38h48" />
                  <path d="M24 26v12m16-12v12" strokeWidth="2.5" />
                </g>
              </svg>
            </div>
            <span className="auth-brand-name">{title}</span>
          </div>
          {children}
        </div>
      </main>
    </div>
  )
}
