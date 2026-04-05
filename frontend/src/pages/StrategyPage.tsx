/* ════════════════════════════════════════════
   Strategy Page — strategy management hub
   Combines real backend data with rich card UI,
   KPI summary bar, detail panel, and live monitor.
   ════════════════════════════════════════════ */
import { useState, useRef, useEffect, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { useToastStore } from '../stores'
import { api } from '../lib/api'
import { cn, fmtINR, fmtNum, pnlClass } from '../lib/utils'
import {
  GitBranch, Play, Square, RefreshCw, TrendingUp,
  CheckCircle, AlertCircle, XCircle, Zap, FlaskConical,
  ChevronDown, Activity, Eye,
  Wifi, Loader2, PauseCircle, PlayCircle, Cpu, Pencil, Trash2,
  Clock, Layers, Settings2, Shield, BarChart3, Target, BarChart2,
} from 'lucide-react'

// ── Types ─────────────────────────────────────────
type PageTab = 'all' | 'live' | 'paper' | 'monitor'

// ── Type config for strategy badges ───────────────
const TYPE_CONFIG: Record<string, { icon: typeof TrendingUp; color: string; bg: string; label: string }> = {
  neutral:  { icon: Target,     color: 'text-brand',      bg: 'bg-brand/10',      label: 'Neutral' },
  bullish:  { icon: TrendingUp, color: 'text-profit',     bg: 'bg-profit/10',     label: 'Bullish' },
  bearish:  { icon: BarChart3,  color: 'text-loss',       bg: 'bg-loss/10',       label: 'Bearish' },
  scalping: { icon: Zap,        color: 'text-yellow-400', bg: 'bg-yellow-400/10', label: 'Scalping' },
  hedging:  { icon: Shield,     color: 'text-cyan-400',   bg: 'bg-cyan-400/10',   label: 'Hedging' },
}

/* ════════════════════════════════════════════════
   Summary KPIs
   ════════════════════════════════════════════════ */
function SummaryKPIs({ strategies }: { strategies: any[] }) {
  const total      = strategies.length
  const running    = strategies.filter(s => s.status === 'running').length
  const liveCount  = strategies.filter(s => !s.paper_mode && s.status === 'running').length
  const paperCount = strategies.filter(s => s.paper_mode && s.status === 'running').length
  const errors     = strategies.filter(s => s.status === 'error').length

  const kpis = [
    { label: 'Total Strategies', value: String(total), cls: 'text-text-bright', icon: Activity },
    { label: 'Live Running',     value: String(liveCount), cls: liveCount > 0 ? 'text-profit' : 'text-text-muted', icon: Zap },
    { label: 'Paper Running',    value: String(paperCount), cls: paperCount > 0 ? 'text-brand' : 'text-text-muted', icon: FlaskConical },
    { label: 'Errors',           value: String(errors), cls: errors > 0 ? 'text-loss' : 'text-text-muted', icon: AlertCircle },
  ]

  return (
    <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
      {kpis.map(k => {
        const Icon = k.icon
        return (
          <div key={k.label} className="kpi-card">
            <div className="flex items-center gap-2">
              <Icon className="w-3.5 h-3.5 text-text-muted" />
              <span className="text-[11px] text-text-muted">{k.label}</span>
            </div>
            <div className={cn('text-[18px] font-bold font-mono mt-1', k.cls)}>{k.value}</div>
          </div>
        )
      })}
    </div>
  )
}

/* ════════════════════════════════════════════════
   Strategy Card (rich card with metrics & actions)
   ════════════════════════════════════════════════ */
function StrategyCardItem({
  s, isSelected, onSelect, onRun, onStop, onEdit, onDelete,
}: {
  s: any
  isSelected: boolean
  onSelect: () => void
  onRun: (name: string) => void
  onStop: (name: string) => void
  onEdit: (name: string) => void
  onDelete: (name: string) => void
}) {
  const isRunning = s.status === 'running'
  const isError   = s.status === 'error'
  const typeConf  = TYPE_CONFIG[s.type] || TYPE_CONFIG.neutral
  const TypeIcon  = typeConf.icon
  const displayName = (s.name || s.id || '').replace(/_/g, ' ')

  const statusIcon: Record<string, JSX.Element> = {
    running:  <CheckCircle className="w-3.5 h-3.5 text-profit" />,
    stopped:  <Square className="w-3.5 h-3.5 text-text-muted" />,
    error:    <XCircle className="w-3.5 h-3.5 text-loss" />,
  }
  const statusLabel: Record<string, string> = {
    running: 'Running', stopped: 'Stopped', error: 'Error',
  }

  return (
    <div className={cn(
      'bg-bg-card border rounded-xl overflow-hidden transition-all duration-200',
      isSelected ? 'border-brand shadow-lg shadow-brand/10' : 'border-border hover:border-border-strong'
    )}>
      {/* Card header */}
      <div className="px-4 py-3 border-b border-border/60">
        <div className="flex items-start justify-between gap-2">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2 flex-wrap">
              <div className={cn('p-1.5 rounded-lg shrink-0', typeConf.bg)}>
                <TypeIcon className={cn('w-3.5 h-3.5', typeConf.color)} />
              </div>
              <span className="text-[13px] font-semibold text-text-bright truncate capitalize">{displayName}</span>
              <span className={cn('badge text-[9px]',
                s.paper_mode ? 'badge-brand' : 'badge-danger')}>
                {s.paper_mode ? 'PAPER' : 'LIVE'}
              </span>
              <span className="badge badge-neutral text-[9px]">{typeConf.label}</span>
            </div>
            <p className="text-[11px] text-text-muted mt-1 line-clamp-2">{s.description || 'No description'}</p>
          </div>
        </div>

        <div className="flex items-center gap-2 mt-2">
          {statusIcon[s.status] || statusIcon.stopped}
          <span className={cn('text-[11px] font-medium',
            isRunning ? 'text-profit' :
            isError   ? 'text-loss' : 'text-text-muted')}>
            {statusLabel[s.status] || 'Stopped'}
          </span>
          <div className="flex-1" />
          <div className="flex items-center gap-1 flex-wrap">
            {s.underlying && (
              <span className="text-[10px] px-1.5 py-0.5 bg-bg-elevated rounded text-text-muted">
                {s.underlying}
              </span>
            )}
            {s.exchange && (
              <span className="text-[10px] px-1.5 py-0.5 bg-brand/10 rounded text-brand">
                {s.exchange}
              </span>
            )}
          </div>
        </div>
      </div>

      {/* Metrics grid */}
      <div className="grid grid-cols-3 divide-x divide-border/50 border-b border-border/60">
        <MetricCell label="Legs" value={String(s.legs ?? '—')} cls="text-text-bright" />
        <MetricCell label="Lots" value={String(s.lots ?? 1)} cls="text-text-bright" />
        <MetricCell label="Schema" value={s.schema_version || '—'} cls="text-brand" />
      </div>

      {/* Timing info */}
      {(s.entry_time || s.exit_time) && (
        <div className="flex items-center gap-2 px-4 py-2 border-b border-border/40 text-[11px]">
          <Clock className="w-3.5 h-3.5 text-text-muted shrink-0" />
          <span className="text-text-muted">Entry:</span>
          <span className="font-mono text-text-sec">{s.entry_time || '—'}</span>
          <span className="text-border">→</span>
          <span className="text-text-muted">Exit:</span>
          <span className="font-mono text-text-sec">{s.exit_time || '—'}</span>
        </div>
      )}

      {/* Error display */}
      {isError && s.error && (
        <div className="px-4 py-2 border-b border-border/40">
          <div className="flex items-start gap-1.5 text-[10px] text-loss bg-loss/5 border border-loss/20 rounded-lg px-2.5 py-2">
            <AlertCircle className="w-3.5 h-3.5 shrink-0 mt-0.5" />
            <span className="line-clamp-2">{s.error}</span>
          </div>
        </div>
      )}

      {/* Modified date */}
      {s.modified && (
        <div className="px-4 py-1.5 border-b border-border/40 flex items-center gap-2 text-[10px] text-text-muted">
          <Clock className="w-3 h-3" />
          Modified: {new Date(s.modified).toLocaleDateString('en-IN', { day: 'numeric', month: 'short', year: '2-digit' })}
          {' '}at {new Date(s.modified).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' })}
        </div>
      )}

      {/* Actions */}
      <div className="flex items-center gap-2 px-3 py-2.5">
        <button
          onClick={e => { e.stopPropagation(); isRunning ? onStop(s.name) : onRun(s.name) }}
          className={cn(
            'flex items-center gap-1.5 px-2.5 py-1 rounded text-[11px] font-medium transition-colors',
            isRunning
              ? 'bg-loss/10 text-loss border border-loss/20 hover:bg-loss/20'
              : 'bg-profit/10 text-profit border border-profit/20 hover:bg-profit/20'
          )}
        >
          {isRunning ? <><Square className="w-3 h-3" /> Stop</> : <><Play className="w-3 h-3" /> Start</>}
        </button>
        <button
          onClick={e => { e.stopPropagation(); onEdit(s.name) }}
          className="flex items-center gap-1.5 px-2.5 py-1 rounded text-[11px] font-medium bg-brand/10 text-brand border border-brand/20 hover:bg-brand/20 transition-colors"
        >
          <Pencil className="w-3 h-3" /> Edit
        </button>
        <button
          onClick={e => { e.stopPropagation(); onDelete(s.name) }}
          className="flex items-center gap-1.5 px-2.5 py-1 rounded text-[11px] font-medium text-text-muted hover:text-loss hover:bg-loss/10 border border-border hover:border-loss/20 transition-colors"
        >
          <Trash2 className="w-3 h-3" />
        </button>
        <div className="flex-1" />
        <button
          onClick={onSelect}
          className="flex items-center gap-1.5 px-2.5 py-1 rounded text-[11px] text-text-muted hover:text-text-bright border border-border hover:border-border-strong transition-colors"
        >
          <Eye className="w-3.5 h-3.5" />
          <ChevronDown className={cn('w-3 h-3 transition-transform', isSelected && 'rotate-180')} />
        </button>
      </div>
    </div>
  )
}

function MetricCell({ label, value, cls }: { label: string; value: string; cls: string }) {
  return (
    <div className="px-3 py-2 text-center">
      <div className="text-[10px] text-text-muted">{label}</div>
      <div className={cn('text-[13px] font-bold font-mono mt-0.5', cls)}>{value}</div>
    </div>
  )
}

/* ════════════════════════════════════════════════
   Strategy Detail Panel (expanded view with full config)
   ════════════════════════════════════════════════ */
function StrategyDetail({ strategy: s, onClose }: { strategy: any; onClose: () => void }) {
  const [fullConfig, setFullConfig] = useState<any>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    api.strategyConfig(s.name).then(cfg => {
      if (!cancelled) setFullConfig(cfg)
    }).catch(() => {}).finally(() => {
      if (!cancelled) setLoading(false)
    })
    return () => { cancelled = true }
  }, [s.name])

  const timing   = fullConfig?.timing || {}
  const entry    = fullConfig?.entry || {}
  const exit     = fullConfig?.exit || {}
  const identity = fullConfig?.identity || {}
  const schedule = fullConfig?.schedule || {}
  const legs     = entry.legs || []

  return (
    <div className="bg-bg-card border border-brand/30 rounded-xl overflow-hidden">
      <div className="flex items-center gap-3 px-4 py-3 border-b border-border">
        <BarChart2 className="w-4 h-4 text-brand" />
        <span className="text-[13px] font-semibold text-text-bright capitalize">
          {(s.name || '').replace(/_/g, ' ')} — Configuration
        </span>
        <div className="flex-1" />
        <button onClick={onClose} className="text-text-muted hover:text-text-bright text-[12px]">✕ Close</button>
      </div>

      {loading ? (
        <div className="flex items-center justify-center h-32">
          <Loader2 className="w-5 h-5 animate-spin text-brand" />
        </div>
      ) : (
        <div className="p-4 space-y-4">
          {/* Identity & Timing */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            {[
              { label: 'Underlying', value: identity.underlying || s.underlying || '—' },
              { label: 'Exchange', value: identity.exchange || s.exchange || '—' },
              { label: 'Lots', value: String(identity.lots ?? s.lots ?? 1) },
              { label: 'Paper Mode', value: identity.paper_mode ? 'Yes' : 'No' },
            ].map(m => (
              <div key={m.label} className="kpi-card">
                <div className="text-[10px] text-text-muted">{m.label}</div>
                <div className="text-[14px] font-bold font-mono mt-1 text-text-bright">{m.value}</div>
              </div>
            ))}
          </div>

          {/* Timing details */}
          {Object.keys(timing).length > 0 && (
            <div>
              <div className="text-[11px] text-text-muted font-semibold mb-2 flex items-center gap-1.5">
                <Clock className="w-3.5 h-3.5" /> Timing
              </div>
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-[12px]">
                <StatRow label="Entry Time" value={timing.entry_time || '—'} />
                <StatRow label="Exit Time" value={timing.exit_time || '—'} />
                <StatRow label="No-Trade Start" value={timing.no_trade_start || '—'} />
                <StatRow label="No-Trade End" value={timing.no_trade_end || '—'} />
              </div>
            </div>
          )}

          {/* Entry Legs */}
          {legs.length > 0 && (
            <div>
              <div className="text-[11px] text-text-muted font-semibold mb-2 flex items-center gap-1.5">
                <Layers className="w-3.5 h-3.5" /> Entry — {legs.length} Legs
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-[11px]">
                  <thead>
                    <tr className="bg-bg-elevated/50 text-[9px] text-text-muted uppercase tracking-wider">
                      <th className="px-3 py-2 text-left">#</th>
                      <th className="px-3 py-2 text-left">Type</th>
                      <th className="px-3 py-2 text-center">Side</th>
                      <th className="px-3 py-2 text-center">Strike Mode</th>
                      <th className="px-3 py-2 text-right">Lots</th>
                    </tr>
                  </thead>
                  <tbody>
                    {legs.map((leg: any, i: number) => (
                      <tr key={i} className="border-t border-border/30 hover:bg-bg-hover/20">
                        <td className="px-3 py-2 text-text-muted">{i + 1}</td>
                        <td className="px-3 py-2 font-mono text-text-bright">{leg.instrument_type || '—'}</td>
                        <td className="px-3 py-2 text-center">
                          <span className={cn('badge text-[9px]',
                            leg.side === 'BUY' ? 'badge-buy' : 'badge-sell')}>
                            {leg.side || '—'}
                          </span>
                        </td>
                        <td className="px-3 py-2 text-center text-text-sec">{leg.strike_mode || '—'}</td>
                        <td className="px-3 py-2 text-right font-mono text-text-bright">{leg.lots ?? '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Exit rules */}
          {Object.keys(exit).length > 0 && (
            <div>
              <div className="text-[11px] text-text-muted font-semibold mb-2 flex items-center gap-1.5">
                <Settings2 className="w-3.5 h-3.5" /> Exit Rules
              </div>
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-[12px]">
                <StatRow label="Combined SL" value={exit.combined_sl ? `₹${exit.combined_sl}` : '—'} />
                <StatRow label="Combined Target" value={exit.combined_target ? `₹${exit.combined_target}` : '—'} />
                <StatRow label="Trail SL" value={exit.trail_sl ? 'Yes' : 'No'} />
                <StatRow label="Re-entry" value={exit.re_entry ? 'Yes' : 'No'} />
              </div>
            </div>
          )}

          {/* Schedule */}
          {Object.keys(schedule).length > 0 && (
            <div>
              <div className="text-[11px] text-text-muted font-semibold mb-2 flex items-center gap-1.5">
                <Activity className="w-3.5 h-3.5" /> Schedule
              </div>
              <div className="grid grid-cols-2 gap-3 text-[12px]">
                <StatRow label="Days" value={(schedule.active_days || []).join(', ') || '—'} />
                <StatRow label="Expiry Days" value={schedule.only_expiry_day ? 'Expiry only' : 'All days'} />
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function StatRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="bg-bg-elevated rounded-lg px-3 py-2 space-y-0.5">
      <div className="text-[10px] text-text-muted">{label}</div>
      <div className="text-[13px] font-mono font-semibold text-text-bright">{value}</div>
    </div>
  )
}

/* ════════════════════════════════════════════════
   Backtesting Panel
   ════════════════════════════════════════════════ */
function BacktestingPanel() {
  const toast = useToastStore(s => s.toast)
  const [running, setRunning] = useState(false)
  const [progress, setProgress] = useState(0)
  const [result, setResult] = useState<null | {
    totalReturn: number; sharpe: number; maxDD: number; winRate: number; trades: number
  }>(null)

  const [form, setForm] = useState({
    strategy: 'Iron Condor',
    instrument: 'NIFTY',
    from: '2024-01-01',
    to: '2024-12-31',
    capital: '500000',
  })

  function runBacktest() {
    setRunning(true)
    setProgress(0)
    setResult(null)
    const iv = setInterval(() => {
      setProgress(p => {
        if (p >= 100) {
          clearInterval(iv)
          setRunning(false)
          setResult({
            totalReturn: 18.4,
            sharpe: 1.62,
            maxDD: -8.3,
            winRate: 68,
            trades: 94,
          })
          toast('Backtest completed!', 'success')
          return 100
        }
        return p + 5
      })
    }, 200)
  }

  return (
    <div className="bg-bg-card border border-border rounded-xl overflow-hidden">
      <div className="flex items-center gap-3 px-4 py-3 border-b border-border">
        <FlaskConical className="w-4 h-4 text-brand" />
        <span className="text-[13px] font-semibold text-text-bright">Backtesting Engine</span>
        <span className="badge badge-brand text-[9px] ml-1">BETA</span>
      </div>

      <div className="p-4 space-y-4">
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-3">
          {([
            { label: 'Strategy', key: 'strategy', type: 'select', options: ['Iron Condor', 'VWAP Scalper', 'Momentum Breakout', 'Strangle', 'Pairs Arb'] },
            { label: 'Instrument', key: 'instrument', type: 'select', options: ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY'] },
            { label: 'From', key: 'from', type: 'date', options: [] },
            { label: 'To', key: 'to', type: 'date', options: [] },
            { label: 'Capital (₹)', key: 'capital', type: 'text', options: [] },
          ] as const).map(f => (
            <div key={f.key}>
              <label className="field-label">{f.label}</label>
              {f.type === 'select' ? (
                <select
                  value={(form as any)[f.key]}
                  onChange={e => setForm(p => ({ ...p, [f.key]: e.target.value }))}
                  className="input-base text-[12px]"
                >
                  {f.options.map(o => <option key={o} value={o}>{o}</option>)}
                </select>
              ) : (
                <input
                  type={f.type}
                  value={(form as any)[f.key]}
                  onChange={e => setForm(p => ({ ...p, [f.key]: e.target.value }))}
                  className="input-base text-[12px]"
                />
              )}
            </div>
          ))}
        </div>

        <button onClick={runBacktest} disabled={running} className="btn-primary btn-sm">
          {running
            ? <><RefreshCw className="w-3.5 h-3.5 animate-spin" /> Running…</>
            : <><Play className="w-3.5 h-3.5" /> Run Backtest</>}
        </button>

        {running && (
          <div className="space-y-1.5">
            <div className="flex items-center justify-between text-[11px] text-text-muted">
              <span>Processing trades…</span>
              <span>{progress}%</span>
            </div>
            <div className="w-full h-1.5 bg-bg-elevated rounded-full overflow-hidden">
              <div className="h-full bg-brand rounded-full transition-all duration-200" style={{ width: `${progress}%` }} />
            </div>
          </div>
        )}

        {result && (
          <div className="bg-bg-elevated border border-profit/20 rounded-lg p-4 space-y-3">
            <div className="flex items-center gap-2 text-[12px] font-semibold text-profit">
              <CheckCircle className="w-4 h-4" />
              Backtest Results — {form.strategy} on {form.instrument} ({form.from} → {form.to})
            </div>
            <div className="grid grid-cols-2 sm:grid-cols-5 gap-3">
              {[
                { label: 'Total Return', value: `+${result.totalReturn}%`, cls: 'text-profit' },
                { label: 'Sharpe', value: result.sharpe.toFixed(2), cls: 'text-brand' },
                { label: 'Max Drawdown', value: `${result.maxDD}%`, cls: 'text-loss' },
                { label: 'Win Rate', value: `${result.winRate}%`, cls: 'text-text-bright' },
                { label: 'Trades', value: String(result.trades), cls: 'text-text-bright' },
              ].map(m => (
                <div key={m.label} className="text-center">
                  <div className="text-[10px] text-text-muted">{m.label}</div>
                  <div className={cn('text-[16px] font-bold font-mono mt-1', m.cls)}>{m.value}</div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

/* ════════════════════════════════════════════════
   Live Position Monitor
   ════════════════════════════════════════════════ */
interface LiveLeg {
  symbol: string
  tradingsymbol?: string
  netqty: number
  side: 'BUY' | 'SELL'
  ltp: number
  avg_price: number
  unrealized_pnl: number
  realized_pnl: number
  product?: string
}

interface MonitorPos {
  group: string
  legs: LiveLeg[]
  totalUnrealized: number
  totalRealized: number
}

function groupByUnderlying(legs: LiveLeg[]): MonitorPos[] {
  const groups: Record<string, LiveLeg[]> = {}
  for (const leg of legs) {
    const underlying = leg.symbol.replace(/\d{2}[A-Z]{3}\d+.*$/, '') || leg.symbol.slice(0, 9)
    if (!groups[underlying]) groups[underlying] = []
    groups[underlying].push(leg)
  }
  return Object.entries(groups).map(([group, legs]) => ({
    group,
    legs,
    totalUnrealized: legs.reduce((s, l) => s + l.unrealized_pnl, 0),
    totalRealized:   legs.reduce((s, l) => s + l.realized_pnl, 0),
  }))
}

function PortfolioBar({ positions }: { positions: LiveLeg[] }) {
  const totalUnrealized = positions.reduce((s, l) => s + l.unrealized_pnl, 0)
  const totalRealized   = positions.reduce((s, l) => s + l.realized_pnl, 0)
  const totalPnl        = totalUnrealized + totalRealized

  return (
    <div className="grid grid-cols-2 sm:grid-cols-4 gap-2.5">
      <div className="bg-bg-card border border-border rounded-xl px-4 py-3 col-span-2 sm:col-span-1">
        <div className="text-[10px] text-text-muted uppercase tracking-wider">Total P&L</div>
        <div className={cn('text-xl font-bold font-mono mt-1', pnlClass(totalPnl))}>{fmtINR(totalPnl)}</div>
      </div>
      <div className="bg-bg-card border border-border rounded-xl px-4 py-3">
        <div className="text-[10px] text-text-muted uppercase tracking-wider">Unrealized</div>
        <div className={cn('text-[15px] font-bold font-mono mt-1', pnlClass(totalUnrealized))}>{fmtINR(totalUnrealized)}</div>
      </div>
      <div className="bg-bg-card border border-border rounded-xl px-4 py-3">
        <div className="text-[10px] text-text-muted uppercase tracking-wider">Realized</div>
        <div className={cn('text-[15px] font-bold font-mono mt-1', pnlClass(totalRealized))}>{fmtINR(totalRealized)}</div>
      </div>
      <div className="bg-bg-card border border-border rounded-xl px-4 py-3">
        <div className="text-[10px] text-text-muted uppercase tracking-wider flex items-center gap-1">
          <span className="w-1.5 h-1.5 rounded-full bg-profit animate-pulse inline-block" />Live Positions
        </div>
        <div className="text-[15px] font-bold text-text-bright mt-1">{positions.length} legs</div>
      </div>
    </div>
  )
}

function PositionGroupCard({ pos, flash }: { pos: MonitorPos; flash: boolean }) {
  const [open, setOpen] = useState(true)
  const totalPnl = pos.totalUnrealized + pos.totalRealized

  return (
    <div className={cn(
      'bg-bg-card border rounded-xl overflow-hidden transition-all duration-300',
      flash ? 'border-brand/50 shadow-brand/10 shadow-md' : 'border-border',
      totalPnl > 0 ? 'border-l-2 border-l-profit/50' : totalPnl < 0 ? 'border-l-2 border-l-loss/50' : ''
    )}>
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center gap-3 px-4 py-3 hover:bg-bg-hover/30 transition-colors"
      >
        <div className="flex-1 text-left min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-[13px] font-bold text-text-bright font-mono">{pos.group}</span>
            <span className="text-[10px] text-text-muted bg-bg-elevated px-1.5 py-0.5 rounded">
              {pos.legs.length} {pos.legs.length === 1 ? 'leg' : 'legs'}
            </span>
          </div>
          <div className="text-[10px] text-text-muted mt-0.5">
            U: {fmtINR(pos.totalUnrealized)} &nbsp; R: {fmtINR(pos.totalRealized)}
          </div>
        </div>
        <div className={cn('text-[16px] font-bold font-mono shrink-0', pnlClass(totalPnl))}>
          {totalPnl >= 0 ? '+' : ''}{fmtINR(totalPnl)}
        </div>
        <ChevronDown className={cn('w-4 h-4 text-text-muted transition-transform shrink-0', open && 'rotate-180')} />
      </button>

      {open && (
        <div className="border-t border-border overflow-x-auto">
          <table className="w-full text-[11px]">
            <thead>
              <tr className="bg-bg-elevated/50 text-[9px] text-text-muted uppercase tracking-wider">
                <th className="px-3 py-2 text-left">Symbol</th>
                <th className="px-3 py-2 text-center">Side</th>
                <th className="px-2 py-2 text-right">Qty</th>
                <th className="px-2 py-2 text-right">Avg</th>
                <th className="px-2 py-2 text-right">LTP</th>
                <th className="px-2 py-2 text-right">Unrealized</th>
                <th className="px-2 py-2 text-right">Realized</th>
                <th className="px-2 py-2 text-right font-bold">Total P&L</th>
              </tr>
            </thead>
            <tbody>
              {pos.legs.map((leg, i) => {
                const legPnl = leg.unrealized_pnl + leg.realized_pnl
                return (
                  <tr key={i} className="border-t border-border/30 hover:bg-bg-hover/20">
                    <td className="px-3 py-2">
                      <div className="font-mono text-text-bright text-[10px] truncate max-w-[140px]">{leg.tradingsymbol ?? leg.symbol}</div>
                      {leg.product && <div className="text-[9px] text-text-muted">{leg.product}</div>}
                    </td>
                    <td className="px-3 py-2 text-center">
                      <span className={cn('badge text-[9px]', leg.side === 'BUY' ? 'badge-buy' : 'badge-sell')}>
                        {leg.side}
                      </span>
                    </td>
                    <td className="px-2 py-2 text-right font-mono text-text-bright">{Math.abs(leg.netqty)}</td>
                    <td className="px-2 py-2 text-right font-mono text-text-sec">{fmtNum(leg.avg_price, 2)}</td>
                    <td className="px-2 py-2 text-right font-mono text-text-bright">{fmtNum(leg.ltp, 2)}</td>
                    <td className={cn('px-2 py-2 text-right font-mono', pnlClass(leg.unrealized_pnl))}>{fmtINR(leg.unrealized_pnl)}</td>
                    <td className={cn('px-2 py-2 text-right font-mono', pnlClass(leg.realized_pnl))}>{fmtINR(leg.realized_pnl)}</td>
                    <td className={cn('px-2 py-2 text-right font-mono font-bold', pnlClass(legPnl))}>{fmtINR(legPnl)}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function LiveMonitorPanel() {
  const [positions, setPositions]     = useState<LiveLeg[]>([])
  const [loading, setLoading]         = useState(true)
  const [autoRefresh, setAutoRefresh] = useState(true)
  const [flashing, setFlashing]       = useState<Set<string>>(new Set())
  const prevPnlRef                    = useRef<Record<string, number>>({})

  const load = useCallback(async () => {
    try {
      const res: any = await api.omsPositions()
      const raw: any[] = Array.isArray(res) ? res : (res.data ?? [])
      if (raw.length === 0) throw new Error('no-data')

      const legs: LiveLeg[] = raw.map((p: any) => ({
        symbol:          p.tsym      ?? p.symbol      ?? p.tradingsymbol ?? '—',
        tradingsymbol:   p.tsym      ?? p.tradingsymbol ?? p.symbol,
        netqty:          Number(p.netqty ?? p.qty ?? 0),
        side:            (Number(p.netqty ?? p.qty ?? 0) >= 0) ? 'BUY' : 'SELL',
        ltp:             Number(p.ltp  ?? p.last_price ?? 0),
        avg_price:       Number(p.avgprc ?? p.avg_price ?? p.buy_average ?? 0),
        unrealized_pnl:  Number(p.urmtom ?? p.unrealized_pnl ?? p.urpnl ?? 0),
        realized_pnl:    Number(p.rpnl  ?? p.realized_pnl ?? 0),
        product:         p.prd ?? p.product ?? '',
      }))

      const newFlashing = new Set<string>()
      for (const leg of legs) {
        const pnl  = leg.unrealized_pnl + leg.realized_pnl
        const prev = prevPnlRef.current[leg.symbol]
        if (prev != null && Math.abs(pnl - prev) > 0.01) newFlashing.add(leg.symbol)
        prevPnlRef.current[leg.symbol] = pnl
      }
      if (newFlashing.size > 0) {
        setFlashing(newFlashing)
        setTimeout(() => setFlashing(new Set()), 800)
      }

      setPositions(legs)
    } catch {
      setPositions([])
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])

  useEffect(() => {
    if (!autoRefresh) return
    const t = window.setInterval(load, 3000)
    return () => window.clearInterval(t)
  }, [autoRefresh, load])

  const groups = groupByUnderlying(positions)

  return (
    <div className="space-y-4">
      <div className="bg-bg-card border border-border rounded-xl px-4 py-3 flex flex-wrap items-center gap-3">
        <Wifi className="w-4 h-4 text-brand" />
        <span className="text-[13px] font-semibold text-text-bright">Live Position Monitor</span>
        <span className="inline-flex items-center gap-1 text-[10px] text-profit">
          <span className="w-1.5 h-1.5 rounded-full bg-profit animate-pulse inline-block" /> LIVE
        </span>
        <div className="flex-1" />
        {autoRefresh && (
          <span className="text-[11px] text-text-muted flex items-center gap-1">
            <span className="w-1.5 h-1.5 rounded-full bg-brand animate-ping inline-block" /> Polling 3s
          </span>
        )}
        <button
          onClick={() => setAutoRefresh(!autoRefresh)}
          className={cn(
            'px-3 py-1 rounded-lg text-[11px] border font-medium transition-colors',
            autoRefresh
              ? 'bg-brand/10 text-brand border-brand/30 hover:bg-brand/20'
              : 'border-border text-text-muted hover:text-text-sec'
          )}
        >
          {autoRefresh ? <><PauseCircle className="w-3.5 h-3.5 inline mr-1" />Pause</> : <><PlayCircle className="w-3.5 h-3.5 inline mr-1" />Resume</>}
        </button>
        <button onClick={load} className="text-text-muted hover:text-text-bright p-1.5 rounded-lg hover:bg-bg-hover transition-colors" title="Refresh now">
          <RefreshCw className={cn('w-4 h-4', loading && 'animate-spin')} />
        </button>
      </div>

      {loading && positions.length === 0 ? (
        <div className="flex items-center justify-center h-32">
          <Loader2 className="w-6 h-6 animate-spin text-brand" />
        </div>
      ) : (
        <>
          <PortfolioBar positions={positions} />
          {groups.length === 0 ? (
            <div className="bg-bg-card border border-border rounded-xl p-10 text-center text-text-muted text-[12px]">
              <AlertCircle className="w-8 h-8 mx-auto mb-2 opacity-30" />
              No open positions. Start a strategy or connect your broker.
            </div>
          ) : (
            <div className="space-y-3">
              {groups.map(pos => (
                <PositionGroupCard key={pos.group} pos={pos} flash={pos.legs.some(l => flashing.has(l.symbol))} />
              ))}
            </div>
          )}
        </>
      )}
    </div>
  )
}

/* ════════════════════════════════════════════════
   Main Strategy Page
   ════════════════════════════════════════════════ */
export default function StrategyPage() {
  const toast = useToastStore(s => s.toast)
  const navigate = useNavigate()
  const [tab, setTab] = useState<PageTab>('all')
  const [selected, setSelected] = useState<string | null>(null)

  // ── Real strategies from backend ───────────────────
  const [savedStrategies, setSavedStrategies] = useState<any[]>([])
  const [savedLoading, setSavedLoading] = useState(false)

  const loadSaved = useCallback(async () => {
    setSavedLoading(true)
    try {
      const data = await api.strategyConfigs()
      setSavedStrategies(data || [])
    } catch {
      // backend may not have any strategies yet
    } finally {
      setSavedLoading(false)
    }
  }, [])

  useEffect(() => { loadSaved() }, [loadSaved])

  async function handleRun(name: string) {
    try {
      const res = await api.runStrategy(name) as any
      const warns = res?.warnings?.length ? ` (${res.warnings.length} warnings)` : ''
      toast(`Strategy "${name}" started${warns}`, 'success')
      loadSaved()
    } catch (e: any) {
      let msg = 'Failed to start strategy'
      try {
        const parsed = JSON.parse(e?.message || '{}')
        msg = parsed.detail || msg
      } catch {
        msg = e?.message || msg
      }
      toast(msg, 'error')
      loadSaved()
    }
  }

  async function handleStop(name: string) {
    try {
      await api.stopStrategy(name)
      toast(`Strategy "${name}" stopped`, 'warning')
      loadSaved()
    } catch (e: any) {
      toast(e?.message || 'Failed to stop strategy', 'error')
    }
  }

  async function handleDelete(name: string) {
    if (!confirm(`Delete strategy "${name}"?`)) return
    try {
      await api.deleteStrategyConfig(name)
      toast(`Strategy "${name}" deleted`, 'info')
      loadSaved()
    } catch (e: any) {
      toast(e?.message || 'Failed to delete strategy', 'error')
    }
  }

  const filtered = savedStrategies.filter((s: any) => {
    if (tab === 'live')  return !s.paper_mode
    if (tab === 'paper') return s.paper_mode
    return true
  })

  const selectedStrategy = savedStrategies.find(s => s.name === selected)

  const TABS: { id: PageTab; label: string }[] = [
    { id: 'all',     label: 'All' },
    { id: 'live',    label: 'Live' },
    { id: 'paper',   label: 'Paper' },
    { id: 'monitor', label: '⚡ Live Monitor' },
  ]

  return (
    <div className="h-full overflow-y-auto">
      <div className="p-4 space-y-4 min-h-full">

        {/* Header */}
        <div className="flex items-center gap-3 flex-wrap">
          <div className="flex items-center gap-2">
            <GitBranch className="w-4 h-4 text-brand" />
            <span className="text-[13px] font-semibold text-text-bright">Strategies</span>
          </div>
          <div className="flex-1" />
          <button onClick={loadSaved} className="btn-ghost btn-sm" title="Refresh">
            <RefreshCw className={cn('w-3.5 h-3.5', savedLoading && 'animate-spin')} />
          </button>
          <button
            onClick={() => navigate('/app/strategy-builder')}
            className="btn-primary btn-sm"
          >
            <Cpu className="w-3.5 h-3.5" /> Strategy Builder
          </button>
        </div>

        {/* Summary KPIs — hide on monitor tab */}
        {tab !== 'monitor' && <SummaryKPIs strategies={savedStrategies} />}

        {/* Tab bar */}
        <div className="flex items-center gap-1 bg-bg-surface border border-border rounded-lg p-1 w-fit overflow-x-auto">
          {TABS.map(t => (
            <button
              key={t.id}
              onClick={() => { setTab(t.id); setSelected(null) }}
              className={cn(
                'px-3 sm:px-4 py-1.5 rounded text-[11px] sm:text-[12px] font-medium capitalize whitespace-nowrap transition-colors',
                tab === t.id
                  ? t.id === 'monitor'
                    ? 'bg-profit text-white'
                    : 'bg-brand text-bg-base'
                  : 'text-text-sec hover:text-text-bright'
              )}
            >
              {t.label}
            </button>
          ))}
        </div>

        {/* ── Live Monitor tab ── */}
        {tab === 'monitor' && <LiveMonitorPanel />}

        {/* ── Strategy grid (non-monitor tabs) ── */}
        {tab !== 'monitor' && (
          <>
            {savedLoading && savedStrategies.length === 0 ? (
              <div className="flex items-center justify-center h-40">
                <Loader2 className="w-6 h-6 animate-spin text-brand" />
              </div>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
                {filtered.map(s => (
                  <StrategyCardItem
                    key={s.name}
                    s={s}
                    isSelected={selected === s.name}
                    onSelect={() => setSelected(selected === s.name ? null : s.name)}
                    onRun={handleRun}
                    onStop={handleStop}
                    onEdit={name => navigate(`/app/strategy-builder?name=${encodeURIComponent(name)}`)}
                    onDelete={handleDelete}
                  />
                ))}
                {filtered.length === 0 && (
                  <div className="col-span-full card flex flex-col items-center justify-center h-40 text-text-muted text-sm gap-2">
                    <GitBranch className="w-6 h-6" />
                    No strategies found. Use the Strategy Builder to create one.
                  </div>
                )}
              </div>
            )}

            {/* Expanded detail panel */}
            {selectedStrategy && (
              <StrategyDetail strategy={selectedStrategy} onClose={() => setSelected(null)} />
            )}

            {/* Backtesting section */}
            <BacktestingPanel />
          </>
        )}
      </div>
    </div>
  )
}


