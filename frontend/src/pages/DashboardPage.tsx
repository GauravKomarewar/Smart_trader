/* ═══════════════════════════════════════════
   Dashboard Page — Pro terminal home
   ═══════════════════════════════════════════ */
import { useState, useEffect } from 'react'
import { useDashboardData } from '../hooks'
import { useDashboardStore } from '../stores'
import KPICards from '../components/dashboard/KPICards'
import PositionManager from '../components/dashboard/PositionManager'
import HoldingsTable from '../components/dashboard/HoldingsTable'
import OrderManagement from '../components/dashboard/OrderManagement'
import TradeBook from '../components/dashboard/TradeBook'
import RiskManager from '../components/dashboard/RiskManager'
import { cn, fmtINR, pnlClass, timeAgo } from '../lib/utils'
import { RefreshCw, Wifi, WifiOff, LayoutDashboard, User2, Clock, AlertCircle, TrendingUp, Layers, WalletCards } from 'lucide-react'
import { useAuthStore } from '../stores'
import { api } from '../lib/api'

type DashTab = 'positions' | 'holdings' | 'orders' | 'trades'

export default function DashboardPage() {
  useDashboardData()
  const { data, isLoading, lastUpdate } = useDashboardStore()
  const { isBrokerLive, accounts, activeAccountId } = useAuthStore()
  const account = accounts.find(a => a.id === activeAccountId)
  const [activeTab, setActiveTab] = useState<DashTab>('positions')

  const tabs: { key: DashTab; label: string; count?: number }[] = [
    { key: 'positions', label: 'Positions', count: data?.positions.length },
    { key: 'holdings',  label: 'Holdings',  count: data?.holdings.length },
    { key: 'orders',    label: 'Orders',    count: data?.orders.length },
    { key: 'trades',    label: 'Trade Book',count: data?.trades.length },
  ]

  return (
    <div className="h-full overflow-y-auto">
      <div className="p-4 space-y-4 min-h-full">

        {/* ── Top strip: status + account info ── */}
        <div className="flex items-center gap-3 flex-wrap">
          <div className="flex items-center gap-2">
            <LayoutDashboard className="w-4 h-4 text-brand" />
            <span className="text-sm font-semibold text-text-bright">Dashboard</span>
          </div>
          <div className="flex items-center gap-1.5 text-[11px]">
            {isBrokerLive
              ? <><Wifi className="w-3.5 h-3.5 text-profit" /><span className="text-profit">Live</span></>
              : <><WifiOff className="w-3.5 h-3.5 text-text-muted" /><span className="text-text-muted">No broker</span></>}
          </div>
          {account && (
            <span className="text-[11px] text-text-muted">
              {account.clientId} · {account.broker.toUpperCase()}
            </span>
          )}
          <div className="flex-1" />
          {isLoading && <RefreshCw className="w-3.5 h-3.5 text-text-muted animate-spin" />}
          <span className="text-[11px] text-text-muted">
            Updated {timeAgo(lastUpdate || null)}
          </span>
        </div>

        {/* ── Multi-broker Account Summary ── */}
        <BrokerAccountSummaryStrip />

        {/* ── KPI Cards ── */}
        <KPICards />

        {/* ── Main grid: positions/holdings tabs + risk/account ── */}
        <div className="grid grid-cols-1 xl:grid-cols-[1fr_280px] gap-4">

          {/* Left: tabbed data tables */}
          <div className="flex flex-col min-h-[420px]">
            {/* Tab bar */}
            <div className="flex items-center gap-0 bg-bg-surface border border-border rounded-t-lg overflow-hidden">
              {tabs.map(t => (
                <button
                  key={t.key}
                  onClick={() => setActiveTab(t.key)}
                  className={cn(
                    'flex items-center gap-2 px-4 py-2.5 text-[12px] font-medium transition-colors border-b-2 whitespace-nowrap',
                    activeTab === t.key
                      ? 'border-brand text-brand bg-brand/5'
                      : 'border-transparent text-text-sec hover:text-text-bright hover:bg-bg-hover'
                  )}
                >
                  {t.label}
                  {t.count !== undefined && (
                    <span className={cn('badge', activeTab === t.key ? 'badge-brand' : 'badge-neutral')}>
                      {t.count}
                    </span>
                  )}
                </button>
              ))}
            </div>

            {/* Tab content */}
            <div className="flex-1 bg-bg-card border border-t-0 border-border rounded-b-lg overflow-hidden">
              {activeTab === 'positions' && <PositionManager />}
              {activeTab === 'holdings'  && <HoldingsTable />}
              {activeTab === 'orders'    && <OrderManagement />}
              {activeTab === 'trades'    && <TradeBook />}
            </div>
          </div>

          {/* Right: Risk + Account cards */}
          <div className="space-y-4">
            <RiskManager />
            <AccountSummaryCard />
          </div>
        </div>

      </div>
    </div>
  )
}

// ── Multi-broker Account Summary Strip ────────────────────────────────────────
function BrokerAccountSummaryStrip() {
  const [accounts, setAccounts] = useState<any[]>([])
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    load()
    const t = setInterval(load, 30_000)
    return () => clearInterval(t)
  }, [])

  const load = async () => {
    if (loading) return
    setLoading(true)
    try {
      const res = await api.accountSummary()
      setAccounts(res.accounts ?? [])
    } catch {
      setAccounts([])
    } finally {
      setLoading(false)
    }
  }

  if (accounts.length === 0) return null

  return (
    <div className="flex gap-3 overflow-x-auto pb-1">
      {accounts.map(a => (
        <BrokerMiniCard key={a.config_id} acc={a} />
      ))}
    </div>
  )
}

function BrokerMiniCard({ acc }: { acc: any }) {
  const isLive    = acc.is_live
  const available = acc.available ?? 0
  const used      = acc.used ?? 0
  const total     = (acc.total ?? (available + used)) || 0
  const usedPct   = total > 0 ? Math.min((used / total) * 100, 100) : 0
  const modeColor = isLive ? 'text-profit border-profit/20 bg-profit/5' : 'text-text-muted border-border bg-bg-surface'

  return (
    <div className={cn(
      'shrink-0 rounded-xl border p-3 min-w-[180px] max-w-[220px] space-y-2 transition-colors',
      isLive ? 'border-profit/20 bg-profit/5' : 'border-border bg-bg-surface opacity-60'
    )}>
      {/* Header */}
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-1.5 min-w-0">
          <WalletCards className="w-3.5 h-3.5 shrink-0 text-brand" />
          <span className="text-[11px] font-semibold text-text-bright truncate">{acc.client_id}</span>
        </div>
        <span className={cn(
          'text-[9px] font-semibold px-1.5 py-0.5 rounded border uppercase tracking-wide shrink-0',
          modeColor
        )}>
          {acc.mode}
        </span>
      </div>

      {/* Broker name */}
      <div className="text-[10px] text-text-muted">{acc.broker_name}</div>

      {/* Margin bar */}
      {total > 0 && (
        <div className="space-y-0.5">
          <div className="flex justify-between text-[9px] text-text-muted">
            <span>Margin</span><span>{usedPct.toFixed(0)}%</span>
          </div>
          <div className="h-1 bg-bg-elevated rounded-full overflow-hidden">
            <div
              className="h-full rounded-full transition-all"
              style={{
                width: `${usedPct}%`,
                backgroundColor: usedPct > 80 ? '#f43f5e' : usedPct > 60 ? '#f59e0b' : '#22d3ee',
              }}
            />
          </div>
        </div>
      )}

      {/* Stats */}
      <div className="grid grid-cols-2 gap-x-2 gap-y-0.5 text-[10px]">
        <span className="text-text-muted">Available</span>
        <span className="font-mono text-profit text-right">{fmtINR(available)}</span>
        <span className="text-text-muted">Used</span>
        <span className="font-mono text-warning text-right">{fmtINR(used)}</span>
        {acc.positions_count > 0 && (
          <>
            <span className="text-text-muted flex items-center gap-0.5">
              <Layers className="w-2.5 h-2.5" />Positions
            </span>
            <span className="font-mono text-text-sec text-right">{acc.positions_count}</span>
          </>
        )}
      </div>
    </div>
  )
}


// ── Account Summary Card ──────────────────────────
function AccountSummaryCard() {
  const { data } = useDashboardStore()
  const { isBrokerLive } = useAuthStore()
  const summary = data?.accountSummary

  const [info, setInfo] = useState<{
    isLive: boolean; clientId: string | null; brokerName: string | null;
    loginAt: string | null; limits: Record<string, number>
  } | null>(null)

  useEffect(() => {
    if (!isBrokerLive) return
    const load = async () => {
      try { setInfo(await api.accountInfo()) } catch { /* silent */ }
    }
    load()
    const t = setInterval(load, 30_000)
    return () => clearInterval(t)
  }, [isBrokerLive])

  if (!isBrokerLive) {
    return (
      <div className="bg-bg-card border border-border rounded-lg p-4">
        <div className="flex items-center gap-2 text-text-muted mb-3">
          <AlertCircle className="w-4 h-4 shrink-0" />
          <span className="text-[12px] font-semibold">Account Summary</span>
        </div>
        <div className="text-[11px] text-text-muted text-center py-4">
          No broker connected.<br />
          <a href="/app/settings/brokers" className="text-brand hover:underline">Connect a broker →</a>
        </div>
      </div>
    )
  }

  const lim = info?.limits ?? {}
  const available  = lim.marginAvailable ?? lim.cash ?? (summary?.availableMargin ?? 0)
  const used       = lim.marginUsed ?? (summary?.usedMargin ?? 0)
  const total      = (lim.totalBalance ?? (available + used)) || 0
  const usedPct    = total > 0 ? (used / total) * 100 : 0
  const cash       = lim.cash ?? 0
  const payin      = lim.payin ?? 0

  const rows = [
    { label: 'Client ID',       value: info?.clientId   ?? '—',                   cls: 'text-text-bright font-medium' },
    { label: 'Broker',          value: (info?.brokerName ?? '—').toUpperCase(),    cls: 'text-brand' },
    { label: 'Available',       value: fmtINR(available),                          cls: 'text-profit' },
    { label: 'Margin Used',     value: fmtINR(used),                               cls: 'text-warning' },
    { label: 'Cash Balance',    value: fmtINR(cash),                               cls: 'text-text-bright' },
    ...(payin > 0 ? [{ label: 'Payin Today', value: fmtINR(payin), cls: 'text-profit' }] : []),
    { label: 'Unrealized P&L',  value: (summary?.unrealizedPnl !== undefined ? ((summary.unrealizedPnl >= 0 ? '+' : '') + fmtINR(summary.unrealizedPnl)) : '—'), cls: pnlClass(summary?.unrealizedPnl ?? 0) },
    { label: 'Realized P&L',    value: (summary?.realizedPnl  !== undefined ? ((summary.realizedPnl  >= 0 ? '+' : '') + fmtINR(summary.realizedPnl))  : '—'), cls: pnlClass(summary?.realizedPnl  ?? 0) },
  ]

  return (
    <div className="bg-bg-card border border-border rounded-lg p-4 space-y-3">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <User2 className="w-3.5 h-3.5 text-profit" />
          <span className="text-[12px] font-semibold text-text-bright">Account</span>
        </div>
        <span className="flex items-center gap-1 text-[10px] text-profit border border-profit/20 bg-profit/10 px-2 py-0.5 rounded">
          ● Live
        </span>
      </div>

      {/* Margin bar */}
      {total > 0 && (
        <div className="space-y-1">
          <div className="flex justify-between text-[10px] text-text-muted">
            <span>Margin utilized</span>
            <span>{usedPct.toFixed(1)}%</span>
          </div>
          <div className="h-1.5 bg-bg-elevated rounded-full overflow-hidden">
            <div
              className="h-full rounded-full transition-all duration-500"
              style={{
                width: `${Math.min(usedPct, 100)}%`,
                backgroundColor: usedPct > 80 ? '#f43f5e' : usedPct > 60 ? '#f59e0b' : '#22d3ee',
              }}
            />
          </div>
        </div>
      )}

      {rows.map((r, i) => (
        <div key={i} className="flex items-center justify-between text-[12px]">
          <span className="text-text-muted">{r.label}</span>
          <span className={cn('font-mono font-semibold', r.cls)}>{r.value}</span>
        </div>
      ))}

      {info?.loginAt && (
        <div className="flex items-center gap-1 text-[10px] text-text-muted pt-1 border-t border-border/50">
          <Clock className="w-3 h-3" />
          <span>Session: {new Date(info.loginAt).toLocaleString('en-IN', { dateStyle: 'short', timeStyle: 'short' })}</span>
        </div>
      )}
    </div>
  )
}
