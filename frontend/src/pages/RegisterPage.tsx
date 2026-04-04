/* ═══════════════════════════════════════════════
   Register Page — create new account
   ═══════════════════════════════════════════════ */
import { useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { Activity, Eye, EyeOff, UserPlus, Loader2, ArrowLeft, CheckCircle } from 'lucide-react'
import { useAuthStore, useToastStore } from '../stores'
import { api } from '../lib/api'

export default function RegisterPage() {
  const navigate = useNavigate()
  const { setUser, setAuthenticated } = useAuthStore()
  const { toast } = useToastStore()

  const [name,     setName]     = useState('')
  const [email,    setEmail]    = useState('')
  const [phone,    setPhone]    = useState('')
  const [password, setPassword] = useState('')
  const [confirm,  setConfirm]  = useState('')
  const [showPw,   setShowPw]   = useState(false)
  const [loading,  setLoading]  = useState(false)

  const strong = password.length >= 8

  async function handleRegister(e: React.FormEvent) {
    e.preventDefault()
    if (!name || !email || !password) { toast('All fields required', 'error'); return }
    if (password !== confirm) { toast('Passwords do not match', 'error'); return }
    if (!strong) { toast('Password must be at least 8 characters', 'error'); return }
    setLoading(true)
    try {
      const res = await api.register(email, name, password, phone || undefined)
      localStorage.setItem('st_token', res.access_token)
      setUser({
        id: res.user.id,
        name: res.user.name,
        email: res.user.email,
        role: res.user.role as any,
        createdAt: new Date().toISOString(),
      })
      setAuthenticated(true)
      toast(`Account created! Welcome, ${res.user.name}!`, 'success')
      navigate('/app', { replace: true })
    } catch (err: any) {
      const msg = err?.message ?? 'Registration failed'
      try { const j = JSON.parse(msg); toast(j.detail ?? msg, 'error') }
      catch { toast(msg, 'error') }
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen bg-bg-base flex items-center justify-center p-6">
      <div className="w-full max-w-[420px] space-y-6 animate-slide-up">

        {/* Logo */}
        <div className="flex items-center gap-2.5">
          <div className="w-9 h-9 rounded-xl bg-brand/15 border border-brand/30 flex items-center justify-center">
            <Activity className="w-4.5 h-4.5 text-brand" />
          </div>
          <span className="text-[17px] font-bold text-text-bright">
            Smart<span className="text-brand">Trader</span>
          </span>
        </div>

        <div>
          <h1 className="text-2xl font-bold text-text-bright">Create account</h1>
          <p className="text-text-sec text-sm mt-1">
            Start trading on Smart Trader platform
          </p>
        </div>

        <form onSubmit={handleRegister} className="space-y-4">
          {/* Name */}
          <div>
            <label className="block text-[11px] text-text-muted mb-1.5">Full Name</label>
            <input
              type="text"
              value={name}
              onChange={e => setName(e.target.value)}
              placeholder="Your name"
              autoFocus
              className="input-base w-full"
              required
            />
          </div>

          {/* Email */}
          <div>
            <label className="block text-[11px] text-text-muted mb-1.5">Email address</label>
            <input
              type="email"
              value={email}
              onChange={e => setEmail(e.target.value)}
              placeholder="you@example.com"
              className="input-base w-full"
              required
            />
          </div>

          {/* Phone (optional) */}
          <div>
            <label className="block text-[11px] text-text-muted mb-1.5">
              Phone <span className="text-text-muted/60">(optional)</span>
            </label>
            <input
              type="tel"
              value={phone}
              onChange={e => setPhone(e.target.value)}
              placeholder="+91 9999000000"
              className="input-base w-full"
            />
          </div>

          {/* Password */}
          <div>
            <label className="block text-[11px] text-text-muted mb-1.5">Password</label>
            <div className="relative">
              <input
                type={showPw ? 'text' : 'password'}
                value={password}
                onChange={e => setPassword(e.target.value)}
                placeholder="Min 8 characters"
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
            {password.length > 0 && (
              <div className={`mt-1.5 flex items-center gap-1.5 text-[11px] ${strong ? 'text-profit' : 'text-loss'}`}>
                <CheckCircle className="w-3 h-3" />
                {strong ? 'Strong password' : 'At least 8 characters required'}
              </div>
            )}
          </div>

          {/* Confirm */}
          <div>
            <label className="block text-[11px] text-text-muted mb-1.5">Confirm Password</label>
            <input
              type={showPw ? 'text' : 'password'}
              value={confirm}
              onChange={e => setConfirm(e.target.value)}
              placeholder="Repeat password"
              className="input-base w-full"
              required
            />
          </div>

          {/* Submit */}
          <button
            type="submit"
            disabled={loading}
            className="btn-primary w-full py-2.5 text-[13px] font-semibold justify-center"
          >
            {loading
              ? <><Loader2 className="w-4 h-4 animate-spin" /> Creating account…</>
              : <><UserPlus className="w-4 h-4" /> Create Account</>
            }
          </button>
        </form>

        <p className="text-center text-[12px] text-text-muted">
          Already have an account?{' '}
          <Link to="/login" className="text-brand hover:underline font-medium">Sign in</Link>
        </p>

        <div className="flex justify-center">
          <Link to="/" className="text-[11px] text-text-muted hover:text-text-sec flex items-center gap-1">
            <ArrowLeft className="w-3 h-3" /> Back to home
          </Link>
        </div>

      </div>
    </div>
  )
}
