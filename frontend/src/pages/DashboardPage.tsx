/* ═══════════════════════════════════════════
   Dashboard Page — Pro terminal home
   Broker account cards + combined KPIs + full-width data tables
   ═══════════════════════════════════════════ */
import { useState, useMemo } from 'react'
import { useDashboardStore, useBrokerAccountsStore } from '../stores'
import type { BrokerAccountWS } from '../stores'
import KPICards from '../components/dashboard/KPICards'
import PositionManager from '../components/dashboard/PositionManager'
import HoldingsTable from '../components/dashboard/HoldingsTable'
import OrderManagement from '../components/dashboard/OrderManagement'
import TradeBook from '../components/dashboard/TradeBook'
import RiskManager from '../components/dashboard/RiskManager'
import { cn, fmtINR, pnlClass, pnlSign, timeAgo } from '../lib/utils'
import {
  RefreshCw, Wifi, WifiOff, LayoutDashboard, Clock, AlertCircle,
  TrendingUp, TrendingDown, Layers, WalletCards, ShieldCheck, ShieldAlert,
  Banknote, BarChart2, Activity, Zap, CircleDot, FileText, Package,
} from 'lucide-react'
import { useAuthStore } from '../stores'

type DashTab = 'positions' | 'holdings' | 'orders' | 'trades'

type BrokerAccountInfo = BrokerAccountWS

export default function DashboardPage() {
  const { data, isLoading, lastUpdate } = useDashboardStore()
  const { isBrokerLive } = useAuthStore()
  const [activeTab, setActiveTab] = useState<DashTab>('positions')
  const brokerAccounts = useBrokerAccountsStore(s => s.accounts)

  // Aggregated KPIs across all accounts
  const agg = useMemo(() => {
    const live = brokerAccounts.filter(a => a.is_live)
    return {
      totalCash:      live.reduce((s, a) => s + a.cash, 0),
      totalCollateral: live.reduce((s, a) => s + a.collateral, 0),
      totalAvailable: live.reduce((s, a) => s + a.available_margin, 0),
      totalUsed:      live.reduce((s, a) => s + a.used_margin, 0),
      totalBalance:   live.reduce((s, a) => s + a.total_balance, 0),
      totalDayPnl:    live.reduce((s, a) => s + a.day_pnl, 0),
      totalUnrealized: live.reduce((s, a) => s + a.unrealized_pnl, 0),
      totalRealized:  live.reduce((s, a) => s + a.realized_pnl, 0),
      totalPositions: live.reduce((s, a) => s + a.positions_count, 0),
      totalOrders:    live.reduce((s, a) => s + a.orders_count, 0),
      totalTrades:    live.reduce((s, a) => s + a.trades_count, 0),
      liveCount:      live.length,
      offlineCount:   brokerAccounts.filter(a => !a.is_live).length,
    }
  }, [brokerAccounts])

  const tabs: { key: DashTab; label: string; icon: typeof LayoutDashboard; count?: number }[] = [
    { key: 'positions', label: 'Positions', icon: Layers,   count: data?.positions.length },
    { key: 'holdings',  label: 'Holdings',  icon: Package,  count: data?.holdings.length },
    { key: 'orders',    label: 'Orders',    icon: FileText, count: data?.orders.length },
    { key: 'trades',    label: 'Trade Book',icon: BarChart2,count: data?.trades.length },
  ]

  return (
    <div className="h-full overflow-y-auto">
      <div className="p-4 space-y-4 min-h-full">

        {/* ── Top strip: status + update time ── */}
        <div className="flex items-center gap-3 flex-wrap">
          <div className="flex items-center gap-2">
            <LayoutDashboard className="w-4 h-4 text-brand" />
            <span className="text-sm font-semibold text-text-bright">Dashboard</span>
          </div>
          <div className="flex items-center gap-1.5 text-[11px]">
            {isBrokerLive
              ? <><Wifi className="w-3.5 h-3.5 text-profit" /><span className="text-profit">Live ({agg.liveCount} broker{agg.liveCount !== 1 ? 's' : ''})</span></>
              : <><WifiOff className="w-3.5 h-3.5 text-text-muted" /><span className="text-text-muted">No broker</span></>}
          </div>
          <div className="flex-1" />
          {isLoading && <RefreshCw className="w-3.5 h-3.5 text-text-muted animate-spin" />}
          <span className="text-[11px] text-text-muted">Updated {timeAgo(lastUpdate || null)}</span>
        </div>

        {/* ── Broker Account Cards ── */}
        {brokerAccounts.length > 0 && (
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-3">
            {brokerAccounts.map(acc => (
              <BrokerAccountCard key={acc.config_id} acc={acc} />
            ))}
          </div>
        )}

        {/* ── Combined KPI Strip ── */}
        <CombinedKPIStrip agg={agg} summary={data?.accountSummary} />

        {/* ── Full-width tabbed data tables (no sidebar risk panel) ── */}
        <div className="flex flex-col min-h-[460px]">
          {/* Tab bar */}
          <div className="flex items-center gap-0 bg-bg-surface border border-border rounded-t-lg overflow-x-auto">
            {tabs.map(t => (
              <button
                key={t.key}
                onClick={() => setActiveTab(t.key)}
                className={cn(
                  'flex items-center gap-2 px-5 py-3 text-[12px] font-medium transition-colors border-b-2 whitespace-nowrap',
                  activeTab === t.key
                    ? 'border-brand text-brand bg-brand/5'
                    : 'border-transparent text-text-sec hover:text-text-bright hover:bg-bg-hover'
                )}
              >
                <t.icon className="w-3.5 h-3.5" />
                {t.label}
                {t.count !== undefined && (
                  <span className={cn('badge', activeTab === t.key ? 'badge-brand' : 'badge-neutral')}>
                    {t.count}
                  </span>
                )}
              </button>
            ))}
            <div className="flex-1" />
            <span className="text-[10px] text-text-muted pr-4">Combined · All Brokers</span>
          </div>

          {/* Tab content */}
          <div className="flex-1 bg-bg-card border border-t-0 border-border rounded-b-lg overflow-hidden">
            {activeTab === 'positions' && <PositionManager />}
            {activeTab === 'holdings'  && <HoldingsTable />}
            {activeTab === 'orders'    && <OrderManagement />}
            {activeTab === 'trades'    && <TradeBook />}
          </div>
        </div>
      </div>
    </div>
  )
}


// ── Broker Account Card (rich detail) ─────────────────────────────────────────
function BrokerAccountCard({ acc }: { acc: BrokerAccountInfo }) {
  const isLive = acc.is_live
  const total  = acc.total_balance || (acc.available_margin + acc.used_margin) || 1
  const usedPct = total > 0 ? Math.min((acc.used_margin / total) * 100, 100) : 0
  const limit  = acc.cash + acc.collateral

  return (
    <div className={cn(
      'rounded-xl border p-4 space-y-3 transition-all',
      isLive
        ? 'border-profit/20 bg-gradient-to-br from-profit/5 to-transparent'
        : 'border-border bg-bg-surface opacity-50'
    )}>
      {/* Header: broker name + status badge */}
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2 min-w-0">
          <WalletCards className="w-4 h-4 shrink-0 text-brand" />
          <div className="min-w-0">
            <div className="text-[13px] font-bold text-text-bright truncate">{acc.client_id}</div>
            <div className="text-[10px] text-text-muted">{acc.broker_name} · {acc.mode.toUpperCase()}</div>
          </div>
        </div>
        <div className="flex flex-col items-end gap-1 shrink-0">
          <span className={cn(
            'text-[9px] font-bold px-2 py-0.5 rounded-full border uppercase tracking-wider',
            isLive
              ? 'text-profit border-profit/30 bg-profit/10'
              : 'text-text-muted border-border bg-bg-elevated'
          )}>
            {isLive ? '● LIVE' : '○ OFFLINE'}
          </span>
          {isLive && (
            <span className={cn(
              'text-[9px] font-medium px-1.5 py-0.5 rounded border',
              acc.risk_status
                ? 'text-profit border-profit/20 bg-profit/5'
                : 'text-loss border-loss/20 bg-loss/5'
            )}>
              {acc.risk_status ? 'Risk OK' : 'Risk Halt'}
            </span>
          )}
          {isLive && acc.data_stale && (
            <span className="text-[9px] font-medium px-1.5 py-0.5 rounded border text-warning border-warning/30 bg-warning/10">
              ⚠ Stale data
            </span>
          )}
        </div>
      </div>

      {/* Day P&L (large) */}
      {isLive && (
        <div className="flex items-center justify-between">
          <span className="text-[11px] text-text-muted">Day P&L</span>
          <span className={cn('text-[16px] font-bold font-mono', pnlClass(acc.day_pnl))}>
            {pnlSign(acc.day_pnl)}{fmtINR(acc.day_pnl)}
          </span>
        </div>
      )}

      {/* Margin utilization bar */}
      {isLive && total > 0 && (
        <div className="space-y-1">
          <div className="flex justify-between text-[9px] text-text-muted">
            <span>Margin</span>
            <span>{usedPct.toFixed(0)}% used</span>
          </div>
          <div className="h-1.5 bg-bg-elevated rounded-full overflow-hidden">
            <div
              className="h-full rounded-full transition-all duration-500"
              style={{
                width: `${usedPct}%`,
                backgroundColor: usedPct > 80 ? '#f43f5e' : usedPct > 60 ? '#f59e0b' : '#22d3ee',
              }}
            />
          </div>
        </div>
      )}

      {/* Stats grid — broker API data */}
      {isLive && (
        <div className="grid grid-cols-2 gap-x-3 gap-y-1.5 text-[11px]">
          <StatRow icon={Banknote}   label="Cash" value={fmtINR(acc.cash)} cls="text-profit" />
          <StatRow icon={ShieldCheck} label="Collateral" value={fmtINR(acc.collateral)} cls="text-accent" />
          <StatRow icon={TrendingUp}  label="Available" value={fmtINR(acc.available_margin)} cls="text-profit" />
          <StatRow icon={TrendingDown} label="Used" value={fmtINR(acc.used_margin)} cls="text-warning" />
          <StatRow icon={Banknote}    label="Total Bal." value={fmtINR(acc.total_balance)} cls="text-text-bright" />
          <StatRow icon={Banknote}    label="Limit" value={fmtINR(limit)} cls="text-text-bright" />
          {acc.payin > 0 && <StatRow icon={TrendingUp} label="Payin" value={fmtINR(acc.payin)} cls="text-profit" />}
          {acc.payout > 0 && <StatRow icon={TrendingDown} label="Payout" value={fmtINR(acc.payout)} cls="text-loss" />}
          <StatRow icon={Activity}    label="Unrealized" value={`${pnlSign(acc.unrealized_pnl)}${fmtINR(acc.unrealized_pnl)}`} cls={pnlClass(acc.unrealized_pnl)} />
          <StatRow icon={Zap}         label="Realized" value={`${pnlSign(acc.realized_pnl)}${fmtINR(acc.realized_pnl)}`} cls={pnlClass(acc.realized_pnl)} />
          <StatRow icon={Layers}      label="Positions" value={String(acc.positions_count)} cls="text-text-bright" />
          <StatRow icon={FileText}    label="Orders" value={`${acc.completed_orders}/${acc.orders_count}`} cls="text-text-bright" />
          <StatRow icon={BarChart2}   label="Trades" value={String(acc.trades_count)} cls="text-text-bright" />
          <StatRow icon={CircleDot}   label="Open Ord." value={String(acc.open_orders)} cls={acc.open_orders > 0 ? 'text-warning' : 'text-text-sec'} />
        </div>
      )}

      {/* Connected time */}
      {isLive && acc.connected_at && (
        <div className="flex items-center gap-1 text-[9px] text-text-muted pt-1 border-t border-border/30">
          <Clock className="w-3 h-3" />
          <span>Connected {timeAgo(acc.connected_at)}</span>
        </div>
      )}

      {/* Error */}
      {acc.error && (
        <div className="flex items-center gap-1.5 text-[10px] text-loss bg-loss/5 rounded px-2 py-1">
          <AlertCircle className="w-3 h-3 shrink-0" /> {acc.error}
        </div>
      )}

      {/* Risk halt reason */}
      {!acc.risk_status && acc.risk_halt_reason && (
        <div className="flex items-center gap-1.5 text-[10px] text-loss bg-loss/5 rounded px-2 py-1">
          <ShieldAlert className="w-3 h-3 shrink-0" /> {acc.risk_halt_reason}
        </div>
      )}
    </div>
  )
}

function StatRow({ icon: Icon, label, value, cls }: { icon: typeof TrendingUp; label: string; value: string; cls: string }) {
  return (
    <div className="flex items-center justify-between">
      <span className="flex items-center gap-1 text-text-muted">
        <Icon className="w-3 h-3" />{label}
      </span>
      <span className={cn('font-mono font-semibold', cls)}>{value}</span>
    </div>
  )
}


// ── Combined KPI Strip ────────────────────────────────────────────────────────
function CombinedKPIStrip({ agg, summary }: {
  agg: {
    totalCash: number; totalCollateral: number; totalAvailable: number; totalUsed: number;
    totalBalance: number; totalDayPnl: number; totalUnrealized: number; totalRealized: number;
    totalPositions: number; totalOrders: number; totalTrades: number;
    liveCount: number; offlineCount: number;
  };
  summary?: { totalEquity: number; dayPnl: number; dayPnlPct: number; unrealizedPnl: number; realizedPnl: number; usedMargin: number; availableMargin: number } | null;
}) {
  const totalLimit = agg.totalCash + agg.totalCollateral
  const dayPnl = agg.totalDayPnl || summary?.dayPnl || 0

  const cards = [
    {
      label: 'Total Limit',
      value: fmtINR(totalLimit),
      sub: `Cash: ${fmtINR(agg.totalCash)} + Collateral: ${fmtINR(agg.totalCollateral)}`,
      cls: 'text-text-bright',
      icon: Banknote,
      iconCls: 'text-profit',
    },
    {
      label: 'Day P&L',
      value: `${pnlSign(dayPnl)}${fmtINR(dayPnl)}`,
      sub: `${agg.liveCount} account${agg.liveCount !== 1 ? 's' : ''} active`,
      cls: pnlClass(dayPnl),
      icon: dayPnl >= 0 ? TrendingUp : TrendingDown,
      iconCls: pnlClass(dayPnl),
    },
    {
      label: 'Unrealized P&L',
      value: `${pnlSign(agg.totalUnrealized)}${fmtINR(agg.totalUnrealized)}`,
      sub: `${agg.totalPositions} open positions`,
      cls: pnlClass(agg.totalUnrealized),
      icon: Activity,
      iconCls: 'text-brand',
    },
    {
      label: 'Realized P&L',
      value: `${pnlSign(agg.totalRealized)}${fmtINR(agg.totalRealized)}`,
      sub: `${agg.totalTrades} trades today`,
      cls: pnlClass(agg.totalRealized),
      icon: Zap,
      iconCls: 'text-accent',
    },
    {
      label: 'Margin Used',
      value: fmtINR(agg.totalUsed),
      sub: `Available: ${fmtINR(agg.totalAvailable)}`,
      cls: 'text-warning',
      icon: BarChart2,
      iconCls: 'text-warning',
    },
    {
      label: 'Total Orders',
      value: String(agg.totalOrders),
      sub: `${agg.totalTrades} filled trades`,
      cls: 'text-text-bright',
      icon: FileText,
      iconCls: 'text-brand',
    },
  ]

  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
      {cards.map((c, i) => (
        <div key={i} className="kpi-card group hover:border-brand/30 transition-colors cursor-default">
          <div className="flex items-center justify-between">
            <span className="section-title">{c.label}</span>
            <c.icon className={cn('w-3.5 h-3.5', c.iconCls)} />
          </div>
          <div className={cn('text-base font-bold font-mono mt-1', c.cls)}>{c.value}</div>
          {c.sub && <div className="text-[10px] text-text-muted">{c.sub}</div>}
        </div>
      ))}
    </div>
  )
}
