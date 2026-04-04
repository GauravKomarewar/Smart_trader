/* ═══════════════════════════════════════════════
   Admin Login Page
   Secure, distinct from the regular user login.
   No demo credentials. Mobile-responsive.
   ═══════════════════════════════════════════════ */
import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  ShieldCheck, Eye, EyeOff, LogIn, Loader2,
  AlertTriangle, Lock, KeyRound, Activity,
} from 'lucide-react'
import { useAuthStore, useToastStore } from '../stores'
import { api } from '../lib/api'

export default function AdminLoginPage() {
  const navigate  = useNavigate()
  const { setUser, setAuthenticated } = useAuthStore()
  const { toast } = useToastStore()

  const [email,    setEmail]    = useState('')
  const [password, setPassword] = useState('')
  const [showPw,   setShowPw]   = useState(false)
  const [loading,  setLoading]  = useState(false)

  async function handleLogin(e: React.FormEvent) {
    e.preventDefault()
    if (!email || !password) { toast('Enter credentials', 'error'); return }
    setLoading(true)
    try {
      const res = await api.login(email, password)
      if (res.user.role !== 'admin') {
        toast('Admin access denied — not an admin account', 'error')
        return
      }
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
      toast(`Admin access granted — welcome, ${res.user.name}`, 'success')
      navigate('/admin', { replace: true })
    } catch (err: any) {
      const msg = err?.message ?? 'Login failed'
      try { const j = JSON.parse(msg); toast(j.detail ?? msg, 'error') }
      catch { toast(msg, 'error') }
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-bg-base p-4">

      {/* Background glow */}
      <div className="fixed inset-0 pointer-events-none">
        <div className="absolute top-[-120px] right-[-100px] w-[420px] h-[420px] rounded-full bg-loss/4 blur-[100px]" />
        <div className="absolute bottom-[-80px] left-[-80px] w-[300px] h-[300px] rounded-full bg-brand/4 blur-[80px]" />
      </div>

      <div className="w-full max-w-[400px] relative z-10 animate-slide-up">

        {/* Header badge */}
        <div className="flex justify-center mb-6">
          <div className="inline-flex flex-col items-center gap-2">
            <div className="w-14 h-14 rounded-2xl bg-bg-surface border border-border flex items-center justify-center shadow-lg">
              <ShieldCheck className="w-7 h-7 text-brand" />
            </div>
            <div className="text-center">
              <div className="text-[18px] font-bold text-text-bright tracking-tight">
                Smart<span className="text-brand">Trader</span>
              </div>
              <div className="text-[11px] text-text-muted uppercase tracking-widest mt-0.5">
                Admin Portal
              </div>
            </div>
          </div>
        </div>

        {/* Security notice */}
        <div className="rounded-xl border border-warning/25 bg-warning/5 p-3.5 mb-5 flex items-start gap-3">
          <AlertTriangle className="w-4 h-4 text-warning shrink-0 mt-0.5" />
          <div>
            <div className="text-[12px] font-semibold text-text-bright">Restricted Access</div>
            <div className="text-[11px] text-text-muted mt-0.5 leading-relaxed">
              This portal is for system administrators only. Unauthorized access attempts are logged.
            </div>
          </div>
        </div>

        {/* Login card */}
        <div className="bg-bg-surface border border-border rounded-2xl shadow-modal overflow-hidden">
          {/* Top bar */}
          <div className="px-6 pt-5 pb-4 border-b border-border/60">
            <div className="flex items-center gap-2">
              <Lock className="w-4 h-4 text-text-muted" />
              <h1 className="text-[14px] font-semibold text-text-bright">Administrator Sign In</h1>
            </div>
            <p className="text-[11px] text-text-muted mt-1">
              Admin credentials required to proceed
            </p>
          </div>

          {/* Form */}
          <form onSubmit={handleLogin} className="px-6 py-5 space-y-4">
            <div>
              <label className="block text-[11px] font-medium text-text-muted mb-1.5 uppercase tracking-wider">
                Admin Email
              </label>
              <input
                type="email"
                value={email}
                onChange={e => setEmail(e.target.value)}
                placeholder="admin@yourdomain.com"
                autoFocus
                autoComplete="username"
                className="input-base w-full"
                required
              />
            </div>

            <div>
              <label className="block text-[11px] font-medium text-text-muted mb-1.5 uppercase tracking-wider">
                Password
              </label>
              <div className="relative">
                <input
                  type={showPw ? 'text' : 'password'}
                  value={password}
                  onChange={e => setPassword(e.target.value)}
                  placeholder="Enter admin password"
                  autoComplete="current-password"
                  className="input-base w-full pr-10"
                  required
                />
                <button
                  type="button"
                  onClick={() => setShowPw(!showPw)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-text-muted hover:text-text-sec"
                  tabIndex={-1}
                >
                  {showPw ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                </button>
              </div>
            </div>

            <button
              type="submit"
              disabled={loading}
              className="btn-primary w-full py-2.5 text-[13px] font-semibold justify-center mt-2"
            >
              {loading
                ? <><Loader2 className="w-4 h-4 animate-spin" /> Authenticating…</>
                : <><KeyRound className="w-4 h-4" /> Access Admin Panel</>
              }
            </button>
          </form>

          {/* Footer link */}
          <div className="px-6 pb-4 flex items-center justify-between">
            <a
              href="/login"
              className="flex items-center gap-1.5 text-[11px] text-text-muted hover:text-text-sec transition-colors"
            >
              <Activity className="w-3 h-3" />
              Go to user login
            </a>
            <span className="text-[10px] text-text-muted px-2 py-0.5 bg-bg-elevated rounded border border-border">
              Admin only
            </span>
          </div>
        </div>

        {/* Footer */}
        <div className="mt-4 text-center text-[10px] text-text-muted">
          Smart Trader Admin • All access is audited and logged
        </div>
      </div>
    </div>
  )
}
