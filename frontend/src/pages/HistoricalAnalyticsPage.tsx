/* ═══════════════════════════════════════════
   Historical Analytics Page
   – Equity curve, statistics, strategy breakdown
   ═══════════════════════════════════════════ */
import { useState, useEffect, useCallback } from 'react'
import { api } from '../lib/api'
import { fmtINR, pnlClass, cn } from '../lib/utils'
import {
  BarChart2, TrendingUp, TrendingDown, Award, Calendar,
  Loader2, RefreshCw, ChevronDown,
} from 'lucide-react'

interface EquityPoint { date: string; pnl: number; equity: number; trade_count: number; win_count: number; loss_count: number }
interface Statistics {
  total_days: number; profitable_days: number; loss_days: number
  total_pnl: number; avg_daily_pnl: number
  avg_win_day: number; avg_loss_day: number
  win_rate: number; profit_factor: number; max_drawdown: number
  total_trades: number
}
interface StrategyBreakdown { strategy: string; total_pnl: number; trades: number; wins: number; trading_days: number }

export default function HistoricalAnalyticsPage() {
  const [days, setDays]           = useState(30)
  const [equity, setEquity]       = useState<EquityPoint[]>([])
  const [stats, setStats]         = useState<Statistics | null>(null)
  const [breakdown, setBreakdown] = useState<StrategyBreakdown[]>([])
  const [loading, setLoading]     = useState(true)
  const [error, setError]         = useState('')

  const load = useCallback(async () => {
    setLoading(true)
    setError('')
    try {
      const [eq, st, bd] = await Promise.all([
        api.get<EquityPoint[]>(`/analytics/equity-curve?days=${days}`),
        api.get<Statistics>(`/analytics/statistics?days=${days}`),
        api.get<StrategyBreakdown[]>(`/analytics/strategy-breakdown?days=${days}`),
      ])
      setEquity(eq)
      setStats(st)
      setBreakdown(bd)
    } catch (e: any) {
      setError(e.message || 'Failed to load analytics')
    } finally {
      setLoading(false)
    }
  }, [days])

  useEffect(() => { load() }, [load])

  const maxEquity = Math.max(...equity.map(e => e.equity), 1)
  const minEquity = Math.min(...equity.map(e => e.equity), 0)
  const range     = maxEquity - minEquity || 1

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <BarChart2 className="text-blue-400" size={24} />
          <h1 className="text-xl font-semibold text-white">Historical Analytics</h1>
        </div>
        <div className="flex items-center gap-3">
          <select
            value={days}
            onChange={e => setDays(Number(e.target.value))}
            className="input-base text-sm"
          >
            <option value={7}>7 days</option>
            <option value={14}>14 days</option>
            <option value={30}>30 days</option>
            <option value={60}>60 days</option>
            <option value={90}>90 days</option>
          </select>
          <button onClick={load} className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-white/5 hover:bg-white/10 text-sm text-gray-300 transition">
            <RefreshCw size={14} />
            Refresh
          </button>
        </div>
      </div>

      {error && (
        <div className="p-3 rounded-lg bg-red-500/10 border border-red-500/20 text-red-400 text-sm">{error}</div>
      )}

      {loading ? (
        <div className="flex justify-center py-20"><Loader2 className="animate-spin text-blue-400" size={32} /></div>
      ) : (
        <>
          {/* Stats cards */}
          {stats && (
            <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4">
              <StatCard label="Total PnL"    value={fmtINR(stats.total_pnl)} color={stats.total_pnl >= 0 ? 'green' : 'red'} />
              <StatCard label="Avg Daily"    value={fmtINR(stats.avg_daily_pnl)} color={stats.avg_daily_pnl >= 0 ? 'green' : 'red'} />
              <StatCard label="Win Rate"     value={`${stats.win_rate.toFixed(1)}%`} color="blue" />
              <StatCard label="Profit Factor" value={stats.profit_factor.toString()} color="purple" />
              <StatCard label="Max Drawdown" value={fmtINR(stats.max_drawdown)} color="red" />
              <StatCard label="Total Trades" value={stats.total_trades.toString()} color="gray" />
            </div>
          )}

          {/* Win/Loss day summary */}
          {stats && (
            <div className="grid grid-cols-3 gap-4">
              <div className="bg-white/5 rounded-xl border border-white/10 p-4 text-center">
                <p className="text-xs text-gray-400 mb-1">Profitable Days</p>
                <p className="text-2xl font-semibold text-emerald-400">{stats.profitable_days}</p>
                <p className="text-xs text-gray-500">Avg win: {fmtINR(stats.avg_win_day)}</p>
              </div>
              <div className="bg-white/5 rounded-xl border border-white/10 p-4 text-center">
                <p className="text-xs text-gray-400 mb-1">Loss Days</p>
                <p className="text-2xl font-semibold text-red-400">{stats.loss_days}</p>
                <p className="text-xs text-gray-500">Avg loss: {fmtINR(stats.avg_loss_day)}</p>
              </div>
              <div className="bg-white/5 rounded-xl border border-white/10 p-4 text-center">
                <p className="text-xs text-gray-400 mb-1">Total Days</p>
                <p className="text-2xl font-semibold text-gray-300">{stats.total_days}</p>
                <p className="text-xs text-gray-500">Trading days tracked</p>
              </div>
            </div>
          )}

          {/* Equity Curve */}
          <div className="bg-white/5 rounded-xl border border-white/10 p-5">
            <h2 className="text-sm font-medium text-gray-400 uppercase mb-4">Equity Curve</h2>
            {equity.length === 0 ? (
              <div className="flex flex-col items-center py-10 text-gray-600">
                <BarChart2 size={32} className="mb-2 opacity-30" />
                <p>No equity data for this period</p>
              </div>
            ) : (
              <div className="relative h-48">
                {/* Y-axis labels */}
                <div className="absolute left-0 top-0 bottom-0 flex flex-col justify-between pr-2 text-[10px] text-gray-600 w-16">
                  <span className={cn(pnlClass(maxEquity))}>{fmtINR(maxEquity)}</span>
                  <span className="text-gray-600">{fmtINR((maxEquity + minEquity) / 2)}</span>
                  <span className={cn(pnlClass(minEquity))}>{fmtINR(minEquity)}</span>
                </div>
                {/* Chart area */}
                <div className="ml-16 h-full relative">
                  <svg width="100%" height="100%" viewBox={`0 0 ${equity.length * 10} 100`} preserveAspectRatio="none">
                    <defs>
                      <linearGradient id="eq-grad" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stopColor="#22c55e" stopOpacity="0.3" />
                        <stop offset="100%" stopColor="#22c55e" stopOpacity="0.02" />
                      </linearGradient>
                    </defs>
                    {/* Fill */}
                    <polyline
                      fill="url(#eq-grad)"
                      stroke="none"
                      points={[
                        `0,100`,
                        ...equity.map((p, i) => `${i * 10 + 5},${100 - ((p.equity - minEquity) / range) * 95}`),
                        `${(equity.length - 1) * 10 + 5},100`,
                      ].join(' ')}
                    />
                    {/* Line */}
                    <polyline
                      fill="none"
                      stroke="#22c55e"
                      strokeWidth="1.5"
                      points={equity.map((p, i) =>
                        `${i * 10 + 5},${100 - ((p.equity - minEquity) / range) * 95}`
                      ).join(' ')}
                    />
                    {/* Zero line */}
                    {minEquity < 0 && (
                      <line
                        x1="0" x2={equity.length * 10}
                        y1={100 - ((0 - minEquity) / range) * 95}
                        y2={100 - ((0 - minEquity) / range) * 95}
                        stroke="#6b7280" strokeWidth="0.5" strokeDasharray="4 4"
                      />
                    )}
                  </svg>
                </div>
              </div>
            )}

            {/* Daily PnL bars */}
            {equity.length > 0 && (
              <div className="mt-4 flex items-end gap-0.5 h-16 border-t border-white/10 pt-3">
                {equity.map((p, i) => {
                  const maxPnl = Math.max(...equity.map(e => Math.abs(e.pnl)), 1)
                  const h = Math.abs(p.pnl) / maxPnl * 100
                  return (
                    <div
                      key={i}
                      title={`${p.date}: ${fmtINR(p.pnl)}`}
                      className={cn(
                        'flex-1 rounded-t-sm min-h-[2px]',
                        p.pnl >= 0 ? 'bg-emerald-500/60' : 'bg-red-500/60'
                      )}
                      style={{ height: `${h}%` }}
                    />
                  )
                })}
              </div>
            )}
          </div>

          {/* Strategy Breakdown */}
          {breakdown.length > 0 && (
            <div className="bg-white/5 rounded-xl border border-white/10 p-5">
              <h2 className="text-sm font-medium text-gray-400 uppercase mb-4">Strategy Breakdown</h2>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-white/10 text-gray-400 text-xs">
                      <th className="pb-2 text-left">Strategy</th>
                      <th className="pb-2 text-right">Total PnL</th>
                      <th className="pb-2 text-right">Trades</th>
                      <th className="pb-2 text-right">Wins</th>
                      <th className="pb-2 text-right">Win Rate</th>
                      <th className="pb-2 text-right">Trading Days</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-white/5">
                    {breakdown.map(b => (
                      <tr key={b.strategy} className="hover:bg-white/5">
                        <td className="py-2 text-gray-200">{b.strategy}</td>
                        <td className={cn('py-2 text-right font-mono', b.total_pnl >= 0 ? 'text-emerald-400' : 'text-red-400')}>
                          {fmtINR(b.total_pnl)}
                        </td>
                        <td className="py-2 text-right text-gray-300">{b.trades}</td>
                        <td className="py-2 text-right text-emerald-400">{b.wins}</td>
                        <td className="py-2 text-right text-blue-300">
                          {b.trades > 0 ? `${((b.wins / b.trades) * 100).toFixed(1)}%` : '—'}
                        </td>
                        <td className="py-2 text-right text-gray-400">{b.trading_days}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  )
}

function StatCard({ label, value, color }: {
  label: string; value: string
  color: 'green' | 'red' | 'blue' | 'purple' | 'gray'
}) {
  const colors = {
    green:  'text-emerald-400',
    red:    'text-red-400',
    blue:   'text-blue-400',
    purple: 'text-purple-400',
    gray:   'text-gray-300',
  }
  return (
    <div className="bg-white/5 rounded-xl border border-white/10 p-3 text-center">
      <p className="text-xs text-gray-500 mb-1">{label}</p>
      <p className={cn('text-base font-semibold', colors[color])}>{value}</p>
    </div>
  )
}
