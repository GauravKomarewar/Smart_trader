/* ═══════════════════════════════════════════════
   Landing Page — Smart Trader Platform
   ═══════════════════════════════════════════════ */
import { useNavigate } from 'react-router-dom'
import {
  Activity, TrendingUp, Shield, BarChart2, Zap, Users,
  ChevronRight, CheckCircle, ArrowRight, Lock,
  Bell, RefreshCw, Cpu, LineChart,
} from 'lucide-react'

const FEATURES = [
  {
    icon: Shield,
    title: 'Shoonya OAuth Login',
    desc: 'Daily headless OAuth via Finvasia API. Credentials stored per user, never exposed.',
  },
  {
    icon: BarChart2,
    title: 'Live Option Chain',
    desc: 'Real-time OI, IV, greeks and PCR with WebSocket tick data from Shoonya.',
  },
  {
    icon: Cpu,
    title: 'Execution Guard',
    desc: 'Duplicate entry protection, cross-strategy conflict prevention, delta execution.',
  },
  {
    icon: TrendingUp,
    title: 'Supreme Risk Manager',
    desc: 'Absolute + trailing max-loss enforcement. Cooldown management. Force-exit on breach.',
  },
  {
    icon: Zap,
    title: 'Webhook Alerts',
    desc: 'TradingView / Chartink webhook with entry/exit/adjust/test-mode support.',
  },
  {
    icon: LineChart,
    title: 'Strategy Engine',
    desc: 'Paper & live mode, equity curve tracking, backtesting panel.',
  },
  {
    icon: Bell,
    title: 'Smart Alerts',
    desc: 'Risk warnings, order fills, P&L milestones — toast + sound notifications.',
  },
  {
    icon: Users,
    title: 'Multi-User Platform',
    desc: 'Admin panel with full user management, per-user broker sessions and risk state.',
  },
]

const STATS = [
  { value: '< 50ms', label: 'Order latency' },
  { value: '99.9%', label: 'Uptime SLA' },
  { value: '8+', label: 'Broker integrations' },
  { value: '100%', label: 'API coverage' },
]

export default function LandingPage() {
  const navigate = useNavigate()

  return (
    <div className="min-h-screen bg-bg-base text-text-bright overflow-x-hidden">

      {/* ── Navbar ─────────────────────────────────────────────────── */}
      <nav className="fixed top-0 left-0 right-0 z-50 border-b border-border/50 bg-bg-base/80 backdrop-blur-xl">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 h-16 flex items-center justify-between">
          <div className="flex items-center gap-2.5">
            <div className="w-8 h-8 rounded-lg bg-brand/15 border border-brand/30 flex items-center justify-center">
              <Activity className="w-4 h-4 text-brand" />
            </div>
            <span className="text-[16px] font-bold">
              Smart<span className="text-brand">Trader</span>
            </span>
          </div>
          <div className="hidden sm:flex items-center gap-6 text-[13px] text-text-sec">
            <a href="#features" className="hover:text-text-bright transition-colors">Features</a>
            <a href="#how"      className="hover:text-text-bright transition-colors">How it works</a>
            <a href="#pricing"  className="hover:text-text-bright transition-colors">Pricing</a>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => navigate('/login')}
              className="btn-ghost btn-sm text-[12px]"
            >
              Sign In
            </button>
            <button
              onClick={() => navigate('/register')}
              className="btn-primary btn-sm text-[12px]"
            >
              Get Started
            </button>
          </div>
        </div>
      </nav>

      {/* ── Hero ───────────────────────────────────────────────────── */}
      <section className="relative pt-32 pb-24 px-4 overflow-hidden">
        {/* glow blobs */}
        <div className="absolute top-[-200px] left-1/2 -translate-x-1/2 w-[800px] h-[500px] rounded-full bg-brand/8 blur-[120px] pointer-events-none" />
        <div className="absolute top-[100px] right-[-100px] w-[400px] h-[400px] rounded-full bg-accent/6 blur-[80px] pointer-events-none" />

        <div className="max-w-4xl mx-auto text-center relative z-10">
          <div className="inline-flex items-center gap-2 px-3 py-1.5 rounded-full border border-brand/30 bg-brand/8 text-brand text-[12px] font-medium mb-6">
            <span className="w-1.5 h-1.5 rounded-full bg-brand animate-pulse" />
            Built for Indian Derivative Markets
          </div>

          <h1 className="text-4xl sm:text-5xl md:text-6xl font-extrabold leading-tight tracking-tight mb-6">
            Professional Trading<br />
            Terminal for<br />
            <span className="bg-gradient-to-r from-brand to-accent bg-clip-text text-transparent">
              Indian Markets
            </span>
          </h1>

          <p className="text-text-sec text-[16px] sm:text-[18px] leading-relaxed mb-10 max-w-2xl mx-auto">
            Multi-user SaaS platform with live Shoonya broker integration,
            execution guard, supreme risk manager, webhook automation
            and full option chain analytics.
          </p>

          <div className="flex flex-col sm:flex-row gap-3 justify-center">
            <button
              onClick={() => navigate('/register')}
              className="btn-primary px-8 py-3 text-[14px] font-semibold"
            >
              Start Free Trial
              <ArrowRight className="w-4 h-4" />
            </button>
            <button
              onClick={() => navigate('/login')}
              className="btn-outline px-8 py-3 text-[14px]"
            >
              Sign In
            </button>
          </div>
        </div>
      </section>

      {/* ── Stats ──────────────────────────────────────────────────── */}
      <section className="py-12 border-y border-border/40">
        <div className="max-w-5xl mx-auto px-4 grid grid-cols-2 md:grid-cols-4 gap-6">
          {STATS.map(s => (
            <div key={s.label} className="text-center">
              <div className="text-3xl font-extrabold text-brand mb-1">{s.value}</div>
              <div className="text-[12px] text-text-muted uppercase tracking-widest">{s.label}</div>
            </div>
          ))}
        </div>
      </section>

      {/* ── Features ───────────────────────────────────────────────── */}
      <section id="features" className="py-20 px-4">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-14">
            <h2 className="text-3xl font-bold mb-3">Everything you need to trade</h2>
            <p className="text-text-sec text-[15px]">Production-grade components built for Indian derivative markets</p>
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
            {FEATURES.map(f => {
              const Icon = f.icon
              return (
                <div key={f.title}
                  className="p-5 rounded-xl border border-border bg-bg-surface hover:border-brand/40 transition-colors group"
                >
                  <div className="w-9 h-9 rounded-lg bg-brand/10 border border-brand/20 flex items-center justify-center mb-4 group-hover:bg-brand/15 transition-colors">
                    <Icon className="w-4.5 h-4.5 text-brand" />
                  </div>
                  <div className="text-[13px] font-semibold text-text-bright mb-2">{f.title}</div>
                  <div className="text-[12px] text-text-muted leading-relaxed">{f.desc}</div>
                </div>
              )
            })}
          </div>
        </div>
      </section>

      {/* ── How It Works ───────────────────────────────────────────── */}
      <section id="how" className="py-20 px-4 bg-bg-surface/40">
        <div className="max-w-4xl mx-auto">
          <div className="text-center mb-14">
            <h2 className="text-3xl font-bold mb-3">How it works</h2>
            <p className="text-text-sec text-[15px]">Up and running in minutes</p>
          </div>
          <div className="space-y-6">
            {[
              {
                step: '01',
                title: 'Create your account',
                desc: 'Register with email & password. First user becomes platform admin. Invite team members with role-based access.',
                icon: Users,
              },
              {
                step: '02',
                title: 'Add your broker credentials',
                desc: 'Go to Settings → Broker Session. Enter your Shoonya (or other broker) credentials. They\'re encrypted at rest on the server.',
                icon: Lock,
              },
              {
                step: '03',
                title: 'Connect via OAuth',
                desc: 'Click "Connect". Our server runs headless OAuth using your credentials and holds the daily session token securely.',
                icon: Shield,
              },
              {
                step: '04',
                title: 'Trade with live data',
                desc: 'Real-time option chain, positions, P&L. Set risk limits. Activate strategies. Send TradingView webhooks for auto-execution.',
                icon: TrendingUp,
              },
            ].map((step, i) => {
              const Icon = step.icon
              return (
                <div key={i} className="flex gap-5 p-5 rounded-xl border border-border bg-bg-base">
                  <div className="w-12 h-12 rounded-xl bg-brand/10 border border-brand/20 flex items-center justify-center shrink-0">
                    <Icon className="w-5 h-5 text-brand" />
                  </div>
                  <div>
                    <div className="text-[11px] text-brand font-mono mb-1">STEP {step.step}</div>
                    <div className="text-[14px] font-semibold text-text-bright mb-1.5">{step.title}</div>
                    <div className="text-[13px] text-text-muted leading-relaxed">{step.desc}</div>
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      </section>

      {/* ── Pricing ────────────────────────────────────────────────── */}
      <section id="pricing" className="py-20 px-4">
        <div className="max-w-4xl mx-auto text-center">
          <h2 className="text-3xl font-bold mb-3">Simple pricing</h2>
          <p className="text-text-sec text-[15px] mb-12">Self-hosted, no per-trade fees</p>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
            {[
              {
                tier: 'Starter',
                price: 'Free',
                perMonth: '',
                features: ['1 user', '1 broker', 'Paper trading', 'Basic dashboard', 'Email alerts'],
                cta: 'Get started free',
                highlight: false,
              },
              {
                tier: 'Pro',
                price: '₹999',
                perMonth: '/month',
                features: ['5 users', '3 brokers', 'Live trading', 'Webhook automation', 'Risk manager', 'Option chain'],
                cta: 'Start Pro',
                highlight: true,
              },
              {
                tier: 'Enterprise',
                price: 'Custom',
                perMonth: '',
                features: ['Unlimited users', 'All brokers', 'White-label', 'Priority support', 'Custom strategies'],
                cta: 'Contact us',
                highlight: false,
              },
            ].map(plan => (
              <div key={plan.tier}
                className={`p-6 rounded-xl border ${plan.highlight ? 'border-brand bg-brand/5' : 'border-border bg-bg-surface'}`}
              >
                <div className="text-[12px] text-text-muted uppercase tracking-widest mb-2">{plan.tier}</div>
                <div className="text-3xl font-extrabold text-text-bright mb-0.5">
                  {plan.price}
                  {plan.perMonth && <span className="text-[14px] text-text-muted font-normal">{plan.perMonth}</span>}
                </div>
                <div className="my-4 border-t border-border" />
                <ul className="space-y-2 mb-6">
                  {plan.features.map(f => (
                    <li key={f} className="flex items-center gap-2 text-[12px] text-text-sec">
                      <CheckCircle className="w-3.5 h-3.5 text-profit shrink-0" />
                      {f}
                    </li>
                  ))}
                </ul>
                <button
                  onClick={() => navigate('/register')}
                  className={`w-full py-2 rounded-lg text-[13px] font-medium transition-colors ${
                    plan.highlight
                      ? 'bg-brand text-white hover:bg-brand/90'
                      : 'border border-border hover:border-brand hover:text-brand'
                  }`}
                >
                  {plan.cta}
                </button>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ── CTA ────────────────────────────────────────────────────── */}
      <section className="py-20 px-4 border-t border-border bg-bg-surface/30">
        <div className="max-w-xl mx-auto text-center">
          <h2 className="text-3xl font-bold mb-4">Ready to trade smarter?</h2>
          <p className="text-text-sec text-[15px] mb-8">
            Join traders using Smart Trader for disciplined,
            automated, risk-managed trading on Indian markets.
          </p>
          <button
            onClick={() => navigate('/register')}
            className="btn-primary px-10 py-3 text-[14px] font-semibold"
          >
            Create Free Account
            <ChevronRight className="w-4 h-4" />
          </button>
        </div>
      </section>

      {/* ── Footer ─────────────────────────────────────────────────── */}
      <footer className="border-t border-border py-8 px-4">
        <div className="max-w-6xl mx-auto flex flex-col sm:flex-row items-center justify-between gap-4 text-[11px] text-text-muted">
          <div className="flex items-center gap-2">
            <Activity className="w-4 h-4 text-brand" />
            <span>Smart<span className="text-brand">Trader</span> © 2026</span>
          </div>
          <div className="flex flex-col sm:flex-row items-center gap-3 text-center sm:text-right">
            <span
              className="text-text-sec cursor-default select-none"
              onClick={e => { if (e.ctrlKey || e.metaKey) navigate('/admin-login') }}
            >Made with ❤️ by <span className="font-medium text-text-bright">Gaurav Komarewar</span></span>
            <span className="hidden sm:inline text-border">·</span>
            <a href="tel:+918830077989" className="hover:text-brand transition-colors">+91 88300 77989</a>
            <span className="hidden sm:inline text-border">·</span>
            <a href="mailto:gktradeslog@gmail.com" className="hover:text-brand transition-colors">gktradeslog@gmail.com</a>
          </div>
        </div>
      </footer>

    </div>
  )
}
