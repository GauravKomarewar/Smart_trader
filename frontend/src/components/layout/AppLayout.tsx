import { Outlet, NavLink, useLocation, useNavigate } from 'react-router-dom'
import { useState, useEffect, useRef } from 'react'
import { useUIStore, useAuthStore, useDashboardStore, useSettingsStore } from '../../stores'
import { useKeyboard, useClock, useMarketState, useLogout, useDashboardData } from '../../hooks'
import { cn, fmtINR, pnlClass } from '../../lib/utils'
import { playOrderFill, playOrderReject } from '../../lib/sounds'
import {
  LayoutDashboard, TrendingUp, Layers, BookOpen, Settings,
  Menu, X, Activity, ChevronDown, Search,
  Keyboard, Plus, ChevronRight, GitBranch, ShieldCheck,
  BarChart2,
} from 'lucide-react'
import { LogOut } from 'lucide-react'
import PlaceOrderModal from '../modals/PlaceOrderModal'
import ChartModal from '../modals/ChartModal'
import KeyboardShortcutsModal from '../common/KeyboardShortcutsModal'
import GlobalSearch from '../common/GlobalSearch'

type NavItem = { to: string; icon: typeof LayoutDashboard; label: string; exact?: boolean }

const NAV: NavItem[] = [
  { to: '/app',             icon: LayoutDashboard, label: 'Dashboard',     exact: true },
  { to: '/app/market',       icon: TrendingUp,      label: 'Market'        },
  { to: '/app/option-chain', icon: Layers,          label: 'Option Chain'  },
  { to: '/app/watchlist',    icon: BookOpen,        label: 'Watchlist'     },
  { to: '/app/strategies',      icon: GitBranch,  label: 'Strategies'    },
  { to: '/app/positions',       icon: ShieldCheck, label: 'Positions'     },
  { to: '/app/greeks',          icon: Activity,    label: 'Greeks'        },
  { to: '/app/analytics',        icon: BarChart2,   label: 'Analytics'      },
  { to: '/app/broker-accounts',  icon: Activity,    label: 'Brokers'        },
  { to: '/app/broker-diagnostics', icon: Activity,  label: 'Diagnostics'    },
  { to: '/app/settings',         icon: Settings,    label: 'Settings'       },
]

export default function AppLayout() {
  useDashboardData()  // HTTP fallback for dashboard data (WS is primary)
  const { sidebarOpen, setSidebarOpen, openOrderModal,
          searchOpen, setSearchOpen, shortcutsOpen, setShortcutsOpen } = useUIStore()
  const { user, accounts, activeAccountId, setActiveAccount, isBrokerLive } = useAuthStore()
  const { data } = useDashboardStore()
  const { settings } = useSettingsStore()
  const logout = useLogout()
  const location = useLocation()
  const clock = useClock()
  const mktState = useMarketState()

  // ── Sound alerts: watch order statuses ──
  const prevOrdersRef = useRef<string[]>([])
  const soundReadyRef  = useRef(false)  // skip first render to avoid replaying old orders
  useEffect(() => {
    if (!settings.soundAlerts || !data?.orders) return
    const current = data.orders
    const prevIds = prevOrdersRef.current
    if (soundReadyRef.current) {
      current.forEach(o => {
        // key = id+status — only triggers when status actually changes
        if (!prevIds.includes(o.id + o.status)) {
          if (o.status === 'COMPLETE') playOrderFill()
          else if (o.status === 'REJECTED' || o.status === 'CANCELLED') playOrderReject()
        }
      })
    }
    soundReadyRef.current = true
    prevOrdersRef.current = current.map(o => o.id + o.status)
  }, [data?.orders, settings.soundAlerts])

  const pageTitle: Record<string, string> = {
    '/app': 'Dashboard', '/app/market': 'Market & Screener',
    '/app/option-chain': 'Option Chain', '/app/watchlist': 'Watchlist & Chart',
    '/app/strategies': 'Strategies', '/app/strategy-builder': 'Strategy Builder',
    '/app/analytics': 'Historical Analytics',
    '/app/broker-accounts': 'Broker Accounts',
    '/app/broker-diagnostics': 'Broker Diagnostics',
    '/app/settings': 'Settings',
  }
  const title = pageTitle[location.pathname] ?? location.pathname.split('/').filter(Boolean).pop() ?? 'Dashboard'

  const account = accounts.find(a => a.id === activeAccountId)
  const summary = data?.accountSummary

  // — global keyboard shortcuts —
  useKeyboard('F1', () => setShortcutsOpen(true))
  useKeyboard('ctrl+/', () => setSearchOpen(true))
  useKeyboard('ctrl+o', () => openOrderModal())
  useKeyboard('Escape', () => {
    setSidebarOpen(false)
    setSearchOpen(false)
  })

  const mktColors = { pre: 'text-text-sec', open: 'text-profit', post: 'text-loss' }
  const mktLabels = { pre: '◦ Pre-Market', open: '● Market Open', post: '● Market Closed' }

  return (
    <div className="flex h-screen overflow-hidden bg-bg-base">

      {/* ── Sidebar ── */}
      <aside className={cn(
        'fixed inset-y-0 left-0 z-50 w-[220px] bg-bg-surface border-r border-border',
        'flex flex-col transition-transform duration-200',
        sidebarOpen ? 'translate-x-0' : '-translate-x-full'
      )}>
        {/* Brand */}
        <div className="flex items-center gap-2.5 px-4 h-14 border-b border-border shrink-0">
          <div className="w-7 h-7 rounded-lg bg-brand/15 border border-brand/30 flex items-center justify-center">
            <Activity className="w-4 h-4 text-brand" />
          </div>
          <div>
            <span className="text-sm font-bold text-text-bright tracking-wide">Smart</span>
            <span className="text-sm font-bold text-brand">Trader</span>
          </div>
          <button
            onClick={() => setSidebarOpen(false)}
            className="ml-auto lg:hidden text-text-muted hover:text-text-bright"
            aria-label="Close sidebar"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Account selector */}
        {accounts.length > 0 && (
          <div className="px-3 py-2 border-b border-border/50">
            <div className="relative">
              <select
                value={activeAccountId ?? ''}
                onChange={e => setActiveAccount(e.target.value)}
                className="w-full px-2 py-1.5 bg-bg-elevated border border-border rounded text-[11px] text-text-pri
                           appearance-none cursor-pointer focus:outline-none focus:border-brand/50"
              >
                {accounts.map(a => (
                  <option key={a.id} value={a.id}>{a.name}</option>
                ))}
              </select>
              <ChevronDown className="w-3 h-3 text-text-muted absolute right-2 top-1/2 -translate-y-1/2 pointer-events-none" />
            </div>
            {account && (
              <div className="flex items-center justify-between mt-1.5 px-1">
                <span className="text-[10px] text-text-muted">Available</span>
                <span className="text-[11px] font-mono text-profit">{fmtINR(account.availableMargin)}</span>
              </div>
            )}
          </div>
        )}

        {/* Navigation */}
        <nav className="flex-1 overflow-y-auto py-2 px-2 space-y-0.5">
          {NAV.map(item => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.exact}
              onClick={() => setSidebarOpen(false)}
              className={({ isActive }) => cn(
                'nav-item',
                isActive && 'active'
              )}
            >
              <item.icon className="w-4 h-4 shrink-0" />
              <span className="flex-1 truncate">{item.label}</span>
            </NavLink>
          ))}
        </nav>

        {/* Place Order Button */}
        <div className="px-3 py-2">
          <button
            onClick={() => { openOrderModal(); setSidebarOpen(false) }}
            className="w-full btn-primary py-2.5 rounded-lg text-[12px] font-bold"
          >
            <Plus className="w-4 h-4" /> Place Order
            <span className="ml-auto kbd">Ctrl+O</span>
          </button>
        </div>

        {/* Status footer */}
        <div className="px-3 py-3 border-t border-border space-y-1.5 shrink-0">
          <div className="flex items-center justify-between text-[11px]">
            <span className={cn('font-medium', mktColors[mktState])}>{mktLabels[mktState]}</span>
            <span className="text-text-muted font-mono tabular-nums">{clock}</span>
          </div>
          {summary && (
            <div className="flex items-center justify-between text-[11px]">
              <span className="text-text-muted">Day P&L</span>
              <span className={cn('font-mono font-semibold', pnlClass(summary.dayPnl))}>
                {summary.dayPnl >= 0 ? '+' : ''}{fmtINR(summary.dayPnl)}
              </span>
            </div>
          )}
          {/* Logout */}
          <button
            onClick={logout}
            className="w-full flex items-center gap-2 text-[11px] text-text-muted hover:text-loss px-1 py-0.5 rounded transition-colors mt-1"
          >
            <LogOut className="w-3.5 h-3.5" />
            <span>Sign out</span>
            {user && <span className="ml-auto truncate max-w-[90px]">{user.name}</span>}
          </button>
          {user?.role === 'admin' && (
            <NavLink
              to="/admin"
              className={({ isActive }) => cn(
                'w-full flex items-center gap-2 text-[11px] px-1 py-0.5 rounded transition-colors',
                isActive ? 'text-brand' : 'text-text-muted hover:text-brand'
              )}
            >
              <ShieldCheck className="w-3.5 h-3.5" />
              <span>Admin Panel</span>
            </NavLink>
          )}
        </div>
      </aside>

      {/* Mobile overlay */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 z-40 bg-black/60 lg:hidden"
          onClick={() => setSidebarOpen(false)}
        />
      )}

      {/* ── Main content ── */}
      <div className={cn(
        'flex-1 flex flex-col min-w-0 overflow-hidden',
        'transition-[margin] duration-200',
        sidebarOpen ? 'lg:ml-[220px]' : 'ml-0'
      )}>
        {/* Top bar */}
        <header className="flex items-center gap-3 px-4 h-12 bg-bg-surface border-b border-border shrink-0">
          <button
            onClick={() => setSidebarOpen(!sidebarOpen)}
            className="text-text-muted hover:text-text-bright"
            aria-label="Toggle sidebar"
          >
            <Menu className="w-5 h-5" />
          </button>

          {/* Breadcrumb */}
          <div className="flex items-center gap-1.5 text-sm">
            <span className="text-text-muted text-xs">Smart Trader</span>
            <ChevronRight className="w-3 h-3 text-text-muted" />
            <span className="text-text-bright font-semibold text-[13px]">{title}</span>
          </div>

          <div className="flex-1" />

          {/* Search */}
          <button
            onClick={() => setSearchOpen(true)}
            className="flex items-center gap-2 px-3 py-1.5 bg-bg-elevated border border-border rounded
                       text-text-muted text-[12px] hover:border-brand/40 hover:text-text-pri transition-colors"
          >
            <Search className="w-3.5 h-3.5" />
            <span className="hidden sm:inline">Search instrument…</span>
            <kbd className="hidden md:inline ml-1 kbd">Ctrl+/</kbd>
          </button>

          {/* Market clock */}
          <div className="hidden md:flex items-center gap-2 text-[12px]">
            <span className={cn('font-semibold', mktColors[mktState])}>{mktLabels[mktState]}</span>
            <span className="text-text-muted font-mono tabular-nums">{clock}</span>
          </div>

          {/* Shortcuts button */}
          <button
            onClick={() => setShortcutsOpen(true)}
            className="text-text-muted hover:text-text-bright transition-colors"
            title="Keyboard shortcuts (F1)"
          >
            <Keyboard className="w-4 h-4" />
          </button>
        </header>

        {/* Demo mode banner — only when broker is NOT connected */}
        {!isBrokerLive && (
          <div className="shrink-0 bg-brand/10 border-b border-brand/20 px-4 py-1.5 flex items-center gap-2 text-[11px]">
            <span className="inline-flex items-center gap-1 bg-brand/20 text-brand font-semibold px-2 py-0.5 rounded text-[10px] tracking-wide">DEMO</span>
            <span className="text-text-sec">No broker connected — showing sample data.</span>
            <button
              onClick={() => { window.location.href = '/app/settings' }}
              className="ml-auto text-brand hover:underline font-medium"
            >Connect Broker →</button>
          </div>
        )}

        {/* Page content */}
        <main className="flex-1 overflow-hidden">
          <Outlet />
        </main>
      </div>

      {/* ── Global Modals — read store internally ── */}
      <PlaceOrderModal />
      <ChartModal />
      <KeyboardShortcutsModal />
      <GlobalSearch />
    </div>
  )
}
