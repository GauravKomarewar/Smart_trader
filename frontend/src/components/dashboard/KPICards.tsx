/* ═══════════════════════════════════
   Dashboard — KPI Cards strip
   ═══════════════════════════════════ */
import { cn, fmtINR, pnlClass } from '../../lib/utils'
import { useDashboardStore, useAuthStore } from '../../stores'
import { TrendingUp, TrendingDown, Shield, Wallet, BarChart2, Activity } from 'lucide-react'

export default function KPICards() {
  const { data } = useDashboardStore()
  const { accounts, activeAccountId } = useAuthStore()
  const account = accounts.find(a => a.id === activeAccountId)
  const summary = data?.accountSummary
  const risk = data?.riskMetrics

  const cards = [
    {
      label: 'Day P&L',
      value: summary ? fmtINR(summary.dayPnl) : '—',
      sub: summary ? `${summary.dayPnl >= 0 ? '+' : ''}${summary.dayPnlPct.toFixed(2)}%` : '',
      cls: pnlClass(summary?.dayPnl ?? 0),
      icon: summary?.dayPnl && summary.dayPnl >= 0 ? TrendingUp : TrendingDown,
      iconCls: pnlClass(summary?.dayPnl ?? 0),
    },
    {
      label: 'Unrealized P&L',
      value: summary ? fmtINR(summary.unrealizedPnl) : '—',
      sub: 'Open positions',
      cls: pnlClass(summary?.unrealizedPnl ?? 0),
      icon: BarChart2,
      iconCls: 'text-brand',
    },
    {
      label: 'Realized P&L',
      value: summary ? fmtINR(summary.realizedPnl) : '—',
      sub: 'Today',
      cls: pnlClass(summary?.realizedPnl ?? 0),
      icon: Activity,
      iconCls: 'text-accent',
    },
    {
      label: 'Available Margin',
      value: account ? fmtINR(account.availableMargin) : '—',
      sub: account ? `Used: ${fmtINR(account.usedMargin)}` : '',
      cls: 'text-text-bright',
      icon: Wallet,
      iconCls: 'text-profit',
    },
    {
      label: 'Risk Status',
      value: risk?.riskStatus ?? 'SAFE',
      sub: risk ? `${risk.positionCount} open pos` : '',
      cls: risk?.riskStatus === 'SAFE' ? 'text-profit'
         : risk?.riskStatus === 'WARNING' ? 'text-warning' : 'text-loss',
      icon: Shield,
      iconCls: risk?.riskStatus === 'SAFE' ? 'text-profit'
             : risk?.riskStatus === 'WARNING' ? 'text-warning' : 'text-loss',
    },
    {
      label: 'Total Equity',
      value: account ? fmtINR(account.totalBalance) : '—',
      sub: 'Account value',
      cls: 'text-text-bright',
      icon: TrendingUp,
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
