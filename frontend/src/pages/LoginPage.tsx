/* ═══════════════════════════════════════════════
   Login Page — email / password + Shoonya OAuth
   ═══════════════════════════════════════════════ */
import { useState } from 'react'
import { Link, useNavigate, useLocation } from 'react-router-dom'
import {
  Activity, Shield, TrendingUp, BarChart2,
  Zap, Eye, EyeOff, LogIn, Loader2, ArrowLeft,
} from 'lucide-react'
import { useAuthStore, useToastStore } from '../stores'
import { api } from '../lib/api'

export default function LoginPage() {
  const navigate = useNavigate()
  const location = useLocation()
  const { setUser, setAuthenticated } = useAuthStore()
  const { toast } = useToastStore()

  const [email, setEmail]       = useState('')
  const [password, setPassword] = useState('')
  const [showPw, setShowPw]     = useState(false)
  const [loading, setLoading]   = useState(false)
  const [mode, setMode]         = useState<'email' | 'oauth'>('email')
  const [oauthLoading, setOauthLoading] = useState(false)

  async function handleEmailLogin(e: React.FormEvent) {
    e.preventDefault()
    if (!email || !password) { toast('Enter email and password', 'error'); return }
    setLoading(true)
    try {
      const res = await api.login(email, password)
      localStorage.setItem('st_token', res.access_token)
      setUser({
        id:        res.user.id,
        name:      res.user.name,
        email:     res.user.email,
        role:      res.user.role as any,
        phone:     res.user.phone,
        createdAt: new Date().toISOString(),
      })
      setAuthenticated(true)
      toast(`Welcome back, ${res.user.name}!`, 'success')
      const from = (location.state as any)?.from ?? '/app'
      navigate(from, { replace: true })
    } catch (err: any) {
      const msg = err?.message ?? 'Login failed'
      try { const j = JSON.parse(msg); toast(j.detail ?? msg, 'error') }
      catch { toast(msg, 'error') }
    } finally {
      setLoading(false)
    }
  }

  async function handleShoonyaOAuth() {
    setOauthLoading(true)
    try {
      const res = await api.shoonyaConnect()
      toast('Shoonya OAuth completed', 'success')
    } catch (err: any) {
      toast(err?.message ?? 'OAuth failed', 'error')
    } finally {
      setOauthLoading(false)
    }
  }

  return (
    <div className="min-h-screen bg-bg-base flex">
      {/* ── Left panel — branding ───────────────────── */}
      <div className="hidden lg:flex flex-col justify-between w-[46%] bg-bg-surface border-r border-border p-12 relative overflow-hidden">
        <div className="absolute top-[-100px] left-[-100px] w-[400px] h-[400px] rounded-full bg-brand/5 blur-[80px]" />
        <div className="absolute bottom-[-80px] right-[-80px] w-[300px] h-[300px] rounded-full bg-accent/5 blur-[60px]" />

        {/* Logo */}
        <div className="flex items-center gap-3 relative z-10">
          <div className="w-10 h-10 rounded-xl bg-brand/15 border border-brand/30 flex items-center justify-center">
            <Activity className="w-5 h-5 text-brand" />
          </div>
          <div>
            <div className="text-xl font-bold text-text-bright tracking-wide">
              Smart<span className="text-brand">Trader</span>
            </div>
            <div className="text-[11px] text-text-muted uppercase tracking-widest">
              Multi-User Trading Platform
            </div>
          </div>
        </div>

        {/* Features */}
        <div className="relative z-10 space-y-6">
          <h2 className="text-3xl font-bold text-text-bright leading-tight">
            Professional trading<br />terminal for<br />
            <span className="text-brand">Indian Markets</span>
          </h2>
          <div className="space-y-3">
            {[
              { icon: Shield,     text: 'Per-user Shoonya OAuth — headless, credential-safe' },
              { icon: TrendingUp, text: 'Real-time positions, orders & P&L from live session' },
              { icon: BarChart2,  text: 'Option chain with OI, IV & greeks from live feed' },
              { icon: Zap,        text: 'Webhook automation — entry / exit / adjust / test mode' },
              { icon: Activity,   text: 'Supreme Risk Manager — trailing max-loss & cooldown' },
            ].map(({ icon: Icon, text }, i) => (
              <div key={i} className="flex items-start gap-3 text-[13px] text-text-sec">
                <Icon className="w-4 h-4 text-brand shrink-0 mt-0.5" />
                <span>{text}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="text-[11px] text-text-muted relative z-10">
          © 2026 Smart Trader • Finvasia / Shoonya API
        </div>
      </div>

      {/* ── Right panel — form ───────────────────────── */}
      <div className="flex-1 flex items-center justify-center p-6">
        <div className="w-full max-w-[380px] space-y-6 animate-slide-up">

          {/* Mobile logo */}
          <div className="lg:hidden flex items-center gap-2 mb-4">
            <Activity className="w-6 h-6 text-brand" />
            <span className="text-lg font-bold text-text-bright">
              Smart<span className="text-brand">Trader</span>
            </span>
          </div>

          <div>
            <h1 className="text-2xl font-bold text-text-bright">Welcome back</h1>
            <p className="text-text-sec text-sm mt-1">Sign in to your Smart Trader account</p>
          </div>

          {/* Admin access hint */}
          {(location.state as any)?.from === '/admin' && (
            <div className="rounded-lg border border-warning/30 bg-warning/5 p-3 flex items-start gap-2.5">
              <span className="text-warning text-lg leading-none mt-0.5">🔐</span>
              <div>
                <div className="text-[12px] font-semibold text-text-bright">Admin Panel Access</div>
                <div className="text-[11px] text-text-muted mt-0.5">Use admin credentials to continue.</div>
              </div>
            </div>
          )}

          {/* ── Email / Password form ─────────────────── */}
          {mode === 'email' && (
            <form onSubmit={handleEmailLogin} className="space-y-4">
              <div>
                <label className="block text-[11px] text-text-muted mb-1.5">Email address</label>
                <input
                  type="email"
                  value={email}
                  onChange={e => setEmail(e.target.value)}
                  placeholder="you@example.com"
                  autoFocus
                  className="input-base w-full"
                  required
                />
              </div>
              <div>
                <label className="block text-[11px] text-text-muted mb-1.5">Password</label>
                <div className="relative">
                  <input
                    type={showPw ? 'text' : 'password'}
                    value={password}
                    onChange={e => setPassword(e.target.value)}
                    placeholder="Enter your password"
                    className="input-base w-full pr-10"
                    required
                  />
                  <button
                    type="button"
                    onClick={() => setShowPw(!showPw)}
                    className="absolute right-3 top-1/2 -translate-y-1/2 text-text-muted hover:text-text-sec"
                  >
                    {showPw ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                  </button>
                </div>
              </div>

              <button
                type="submit"
                disabled={loading}
                className="btn-primary w-full py-2.5 text-[13px] font-semibold justify-center"
              >
                {loading
                  ? <><Loader2 className="w-4 h-4 animate-spin" /> Signing in…</>
                  : <><LogIn className="w-4 h-4" /> Sign In</>
                }
              </button>

              {/* Demo quick-fill */}
              <div className="rounded-lg border border-border bg-bg-soft p-3 flex items-center justify-between gap-3">
                <div>
                  <div className="text-[11px] font-medium text-text-bright">Try Demo Account</div>
                  <div className="text-[10px] text-text-muted">Explore the platform without registering</div>
                </div>
                <button
                  type="button"
                  onClick={() => { setEmail('demo@smarttrader.in'); setPassword('Demo@1234') }}
                  className="shrink-0 px-3 py-1.5 rounded-md border border-brand/40 text-brand text-[11px] font-medium hover:bg-brand/10 transition-colors"
                >
                  Quick Fill
                </button>
              </div>
            </form>
          )}

          {/* ── Shoonya OAuth mode ───────────────────── */}
          {mode === 'oauth' && (
            <div className="space-y-4">
              <button
                onClick={() => setMode('email')}
                className="flex items-center gap-1.5 text-[12px] text-text-muted hover:text-text-sec"
              >
                <ArrowLeft className="w-3.5 h-3.5" /> Back to email login
              </button>
              <div className="rounded-xl border border-brand/30 bg-brand/5 p-5 space-y-4">
                <div className="flex items-center gap-3">
                  <div className="w-9 h-9 rounded-lg bg-brand/15 border border-brand/25 flex items-center justify-center shrink-0">
                    <Shield className="w-4.5 h-4.5 text-brand" />
                  </div>
                  <div>
                    <div className="text-[13px] font-semibold text-text-bright">Shoonya Account</div>
                    <div className="text-[11px] text-text-muted">Finvasia Securities · OAuth 2.0</div>
                  </div>
                </div>
                <p className="text-[11px] text-text-sec leading-relaxed">
                  OAuth runs on the server using your saved broker credentials.
                  Configure them first in <strong>Settings → Broker Sessions</strong>.
                </p>
                <button
                  onClick={handleShoonyaOAuth}
                  disabled={oauthLoading}
                  className="btn-primary w-full py-2.5 text-[13px] font-semibold justify-center"
                >
                  {oauthLoading
                    ? <><Loader2 className="w-4 h-4 animate-spin" /> Connecting via OAuth…</>
                    : <><Shield className="w-4 h-4" /> Connect Shoonya Account</>
                  }
                </button>
              </div>
            </div>
          )}

          {/* ── Divider ────────────────────────────── */}
          {mode === 'email' && (
            <>
              <div className="relative">
                <div className="divider" />
                <span className="absolute left-1/2 -translate-x-1/2 -translate-y-1/2 bg-bg-base px-3 text-[11px] text-text-muted whitespace-nowrap">
                  or
                </span>
              </div>

              <button
                onClick={() => setMode('oauth')}
                className="btn-outline w-full py-2.5 text-[13px] justify-center gap-2"
              >
                <Shield className="w-4 h-4 text-brand" />
                Connect via Shoonya OAuth
              </button>
            </>
          )}

          {/* ── Register link ───────────────────────── */}
          <p className="text-center text-[12px] text-text-muted">
            Don&apos;t have an account?{' '}
            <Link to="/register" className="text-brand hover:underline font-medium">
              Create account
            </Link>
          </p>

          {/* ── Back to landing ─────────────────────── */}
          <div className="flex justify-center">
            <Link to="/" className="text-[11px] text-text-muted hover:text-text-sec flex items-center gap-1">
              <ArrowLeft className="w-3 h-3" /> Back to home
            </Link>
          </div>

        </div>
      </div>
    </div>
  )
}
