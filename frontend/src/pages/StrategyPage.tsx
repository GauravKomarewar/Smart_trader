/* ════════════════════════════════════════════
   Strategy Page — strategy management hub
   ════════════════════════════════════════════ */
import { useState, useRef, useEffect, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { createChart, LineSeries, type IChartApi, type UTCTimestamp } from 'lightweight-charts'
import { useToastStore } from '../stores'
import { api } from '../lib/api'
import { cn, fmtINR, fmtNum, pnlClass } from '../lib/utils'
import {
  GitBranch, Play, Square, RefreshCw, TrendingUp, TrendingDown,
  Plus, Settings2, BarChart2, Clock, CheckCircle, AlertCircle,
  XCircle, Zap, FlaskConical, ToggleLeft, ToggleRight, ChevronDown,
  Activity, ArrowUpRight, ArrowDownRight, Eye, Wifi, AlertTriangle,
  Loader2, PauseCircle, PlayCircle, Cpu, Pencil, Trash2,
} from 'lucide-react'

// ── Types ─────────────────────────────────────────
type StrategyMode = 'paper' | 'live'
type StrategyStatus = 'running' | 'stopped' | 'error' | 'backtesting'

interface StrategyCard {
  id: string
  name: string
  description: string
  type: string
  mode: StrategyMode
  status: StrategyStatus
  pnl: number
  pnlPct: number
  winRate: number
  trades: number
  openPositions: number
  maxDrawdown: number
  sharpe: number
  lastSignal: string
  lastSignalTime: string
  instruments: string[]
  equity: { time: number; value: number }[]
}

// ── Mock strategies ────────────────────────────────
function makeCurve(start: number, drift: number, vol: number, days = 60): { time: number; value: number }[] {
  const pts: { time: number; value: number }[] = []
  let val = start
  const now = Math.floor(Date.now() / 1000)
  for (let i = days; i >= 0; i--) {
    val += drift + (Math.random() - 0.5) * vol
    pts.push({ time: (now - i * 86400) as UTCTimestamp, value: Math.round(val) })
  }
  return pts
}

const DEMO_STRATEGIES: StrategyCard[] = [
  {
    id: 's1', name: 'NIFTY Iron Condor',
    description: 'Sells OTM CE & PE on NIFTY weekly expiry. Targets 1% weekly premium.',
    type: 'Options', mode: 'live', status: 'running',
    pnl: 42500, pnlPct: 8.5, winRate: 72, trades: 48, openPositions: 4,
    maxDrawdown: -12500, sharpe: 1.84, lastSignal: 'SELL CE 23200', lastSignalTime: '09:35 AM',
    instruments: ['NIFTY', 'BANKNIFTY'],
    equity: makeCurve(500000, 800, 3000, 60),
  },
  {
    id: 's2', name: 'BankNifty Scalper',
    description: 'Mean-reversion on BANKNIFTY futures using VWAP + RSI divergence.',
    type: 'Futures', mode: 'live', status: 'running',
    pnl: 18200, pnlPct: 3.6, winRate: 61, trades: 112, openPositions: 1,
    maxDrawdown: -8200, sharpe: 1.22, lastSignal: 'BUY FUT', lastSignalTime: '11:02 AM',
    instruments: ['BANKNIFTY'],
    equity: makeCurve(500000, 350, 4000, 60),
  },
  {
    id: 's3', name: 'Momentum Breakout',
    description: 'Intraday momentum breakout on Nifty-50 stocks above 20-day high.',
    type: 'Equity', mode: 'paper', status: 'running',
    pnl: 6800, pnlPct: 1.36, winRate: 54, trades: 37, openPositions: 3,
    maxDrawdown: -5500, sharpe: 0.91, lastSignal: 'BUY HDFC', lastSignalTime: '10:18 AM',
    instruments: ['NIFTY50 basket'],
    equity: makeCurve(500000, 100, 5000, 60),
  },
  {
    id: 's4', name: 'Strangle Harvester',
    description: 'Weekly short strangles on MIDCPNIFTY. Exits at 50% profit or 2× loss.',
    type: 'Options', mode: 'paper', status: 'stopped',
    pnl: -3200, pnlPct: -0.64, winRate: 45, trades: 20, openPositions: 0,
    maxDrawdown: -14000, sharpe: -0.3, lastSignal: '—', lastSignalTime: '—',
    instruments: ['MIDCPNIFTY'],
    equity: makeCurve(500000, -50, 6000, 60),
  },
  {
    id: 's5', name: 'Pairs Arbitrage',
    description: 'Statistical arbitrage between NIFTY & BANKNIFTY spreads.',
    type: 'Futures', mode: 'paper', status: 'backtesting',
    pnl: 0, pnlPct: 0, winRate: 0, trades: 0, openPositions: 0,
    maxDrawdown: 0, sharpe: 0, lastSignal: 'Running backtest…', lastSignalTime: '—',
    instruments: ['NIFTY', 'BANKNIFTY'],
    equity: [],
  },
]

// ── Strategy tabs ──────────────────────────────────
type PageTab = 'all' | 'live' | 'paper' | 'backtesting' | 'monitor'

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

type MonitorMode = 'demo' | 'live'

function demoPositions(): LiveLeg[] {
  return [
    { symbol: 'NIFTY25APR23000CE', tradingsymbol: 'NIFTY25APR23000CE', netqty: -75, side: 'SELL', ltp: 45.20, avg_price: 88.50, unrealized_pnl: 3247, realized_pnl: 0, product: 'MIS' },
    { symbol: 'NIFTY25APR22000PE', tradingsymbol: 'NIFTY25APR22000PE', netqty: -75, side: 'SELL', ltp: 38.75, avg_price: 79.20, unrealized_pnl: 3034, realized_pnl: 0, product: 'MIS' },
    { symbol: 'BANKNIFTY25APR52000CE', tradingsymbol: 'BANKNIFTY25APR52000CE', netqty: -30, side: 'SELL', ltp: 64.10, avg_price: 155.40, unrealized_pnl: 2739, realized_pnl: 0, product: 'MIS' },
    { symbol: 'BANKNIFTY25APR48000PE', tradingsymbol: 'BANKNIFTY25APR48000PE', netqty: -30, side: 'SELL', ltp: 71.30, avg_price: 148.90, unrealized_pnl: 2328, realized_pnl: 0, product: 'MIS' },
    { symbol: 'RELIANCE', tradingsymbol: 'RELIANCE', netqty: 50, side: 'BUY', ltp: 1284.50, avg_price: 1265.00, unrealized_pnl: 975, realized_pnl: 1200, product: 'CNC' },
  ]
}

function groupByUnderlying(legs: LiveLeg[]): MonitorPos[] {
  const groups: Record<string, LiveLeg[]> = {}
  for (const leg of legs) {
    // Try to extract underlying name from symbol
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

function PortfolioBar({ positions, mode }: { positions: LiveLeg[]; mode: MonitorMode }) {
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
          {mode === 'live'
            ? <><span className="w-1.5 h-1.5 rounded-full bg-profit animate-pulse inline-block" />Live</>
            : <><span className="w-1.5 h-1.5 rounded-full bg-text-muted inline-block" />Demo</>
          }
          &nbsp;Positions
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
      {/* Header */}
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

      {/* Legs table */}
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
  const [error, setError]             = useState('')
  const [autoRefresh, setAutoRefresh] = useState(true)
  const [mode, setMode]               = useState<MonitorMode>('demo')
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

      // Detect P&L changes and flash
      const newFlashing = new Set<string>()
      for (const leg of legs) {
        const key  = leg.symbol
        const pnl  = leg.unrealized_pnl + leg.realized_pnl
        const prev = prevPnlRef.current[key]
        if (prev != null && Math.abs(pnl - prev) > 0.01) newFlashing.add(key)
        prevPnlRef.current[key] = pnl
      }
      if (newFlashing.size > 0) {
        setFlashing(newFlashing)
        setTimeout(() => setFlashing(new Set()), 800)
      }

      setPositions(legs)
      setMode('live')
      setError('')
    } catch {
      // Fall back to demo data
      setPositions(demoPositions())
      setMode('demo')
      setError('')
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
      {/* Control bar */}
      <div className="bg-bg-card border border-border rounded-xl px-4 py-3 flex flex-wrap items-center gap-3">
        <Wifi className="w-4 h-4 text-brand" />
        <span className="text-[13px] font-semibold text-text-bright">Live Position Monitor</span>
        {mode === 'demo' && (
          <span className="badge badge-brand text-[9px]">DEMO</span>
        )}
        {mode === 'live' && (
          <span className="inline-flex items-center gap-1 text-[10px] text-profit">
            <span className="w-1.5 h-1.5 rounded-full bg-profit animate-pulse inline-block" />
            LIVE
          </span>
        )}
        <div className="flex-1" />
        {autoRefresh && (
          <span className="text-[11px] text-text-muted flex items-center gap-1">
            <span className="w-1.5 h-1.5 rounded-full bg-brand animate-ping inline-block" />
            Polling 3s
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
        <button
          onClick={load}
          className="text-text-muted hover:text-text-bright p-1.5 rounded-lg hover:bg-bg-hover transition-colors"
          title="Refresh now"
        >
          <RefreshCw className={cn('w-4 h-4', loading && 'animate-spin')} />
        </button>
      </div>

      {loading && positions.length === 0 ? (
        <div className="flex items-center justify-center h-32">
          <Loader2 className="w-6 h-6 animate-spin text-brand" />
        </div>
      ) : (
        <>
          {/* Portfolio summary bar */}
          <PortfolioBar positions={positions} mode={mode} />

          {/* Position groups */}
          {groups.length === 0 ? (
            <div className="bg-bg-card border border-border rounded-xl p-10 text-center text-text-muted text-[12px]">
              <AlertCircle className="w-8 h-8 mx-auto mb-2 opacity-30" />
              No open positions. Start a strategy or connect your broker.
            </div>
          ) : (
            <div className="space-y-3">
              {groups.map(pos => (
                <PositionGroupCard
                  key={pos.group}
                  pos={pos}
                  flash={pos.legs.some(l => flashing.has(l.symbol))}
                />
              ))}
            </div>
          )}
        </>
      )}
    </div>
  )
}

// ── Saved Strategies Panel (from Strategy Builder backend) ────────────────────
interface SavedStrategiesPanelProps {
  strategies: any[]
  loading: boolean
  onRefresh: () => void
  onRun: (name: string) => void
  onStop: (name: string) => void
  onEdit: (name: string) => void
  onDelete: (name: string) => void
}

function SavedStrategiesPanel({
  strategies, loading, onRefresh, onRun, onStop, onEdit, onDelete,
}: SavedStrategiesPanelProps) {
  if (loading && strategies.length === 0) {
    return (
      <div className="bg-bg-surface border border-border rounded-xl p-4">
        <div className="flex items-center gap-2 text-[12px] text-text-muted">
          <Loader2 className="w-4 h-4 animate-spin" /> Loading saved strategies…
        </div>
      </div>
    )
  }

  return (
    <div className="bg-bg-surface border border-border rounded-xl overflow-hidden">
      {/* Header */}
      <div className="flex items-center gap-3 px-4 py-3 border-b border-border">
        <Cpu className="w-4 h-4 text-brand" />
        <span className="text-[12px] font-semibold text-text-bright">Saved Strategies</span>
        <span className="text-[10px] text-text-muted bg-bg-elevated px-2 py-0.5 rounded-full">
          {strategies.length}
        </span>
        <div className="flex-1" />
        <button
          onClick={onRefresh}
          className="text-text-muted hover:text-text-bright p-1 rounded transition-colors"
          title="Refresh"
        >
          <RefreshCw className={cn('w-3.5 h-3.5', loading && 'animate-spin')} />
        </button>
      </div>

      {strategies.length === 0 ? (
        <div className="p-8 text-center">
          <Cpu className="w-8 h-8 mx-auto mb-2 text-text-muted opacity-40" />
          <p className="text-[12px] text-text-muted mb-3">No strategies saved yet.</p>
          <p className="text-[11px] text-text-muted opacity-60">
            Use the Strategy Builder to create and save your first strategy.
          </p>
        </div>
      ) : (
        <div className="divide-y divide-border">
          {strategies.map((s: any) => {
            const isRunning = s.status === 'running'
            const isError   = s.status === 'error'
            return (
              <div key={s.name} className="flex items-center gap-3 px-4 py-3 hover:bg-bg-hover transition-colors">
                {/* Status dot */}
                <span className={cn(
                  'w-2 h-2 rounded-full shrink-0',
                  isRunning ? 'bg-profit animate-pulse' :
                  isError   ? 'bg-loss' :
                  'bg-text-muted opacity-40'
                )} />

                {/* Info */}
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 flex-wrap">
                    <span className="text-[12px] font-semibold text-text-bright truncate">
                      {s.display_name || s.name}
                    </span>
                    {s.underlying && (
                      <span className="text-[10px] text-brand bg-brand/10 px-1.5 py-0.5 rounded">
                        {s.underlying}
                      </span>
                    )}
                    <span className={cn(
                      'text-[10px] px-1.5 py-0.5 rounded font-medium',
                      s.paper_mode
                        ? 'bg-text-muted/10 text-text-muted'
                        : 'bg-profit/10 text-profit'
                    )}>
                      {s.paper_mode ? 'PAPER' : 'LIVE'}
                    </span>
                    {isError && (
                      <span className="text-[10px] text-loss" title={s.error}>⚠ ERROR</span>
                    )}
                  </div>
                  {s.description && (
                    <div className="text-[10px] text-text-muted truncate mt-0.5">{s.description}</div>
                  )}
                </div>

                {/* Actions */}
                <div className="flex items-center gap-1 shrink-0">
                  {isRunning ? (
                    <button
                      onClick={() => onStop(s.name)}
                      title="Stop"
                      className="p-1.5 rounded-lg text-loss hover:bg-loss/10 transition-colors border border-loss/20"
                    >
                      <Square className="w-3.5 h-3.5" />
                    </button>
                  ) : (
                    <button
                      onClick={() => onRun(s.name)}
                      title="Run"
                      className="p-1.5 rounded-lg text-profit hover:bg-profit/10 transition-colors border border-profit/20"
                    >
                      <Play className="w-3.5 h-3.5" />
                    </button>
                  )}
                  <button
                    onClick={() => onEdit(s.name)}
                    title="Edit in Builder"
                    className="p-1.5 rounded-lg text-brand hover:bg-brand/10 transition-colors border border-brand/20"
                  >
                    <Pencil className="w-3.5 h-3.5" />
                  </button>
                  <button
                    onClick={() => onDelete(s.name)}
                    title="Delete"
                    className="p-1.5 rounded-lg text-text-muted hover:text-loss hover:bg-loss/10 transition-colors border border-transparent hover:border-loss/20"
                  >
                    <Trash2 className="w-3.5 h-3.5" />
                  </button>
                </div>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

export default function StrategyPage() {
  const toast = useToastStore(s => s.toast)
  const navigate = useNavigate()
  const [tab, setTab] = useState<PageTab>('all')
  const [strategies, setStrategies] = useState(DEMO_STRATEGIES)
  const [selected, setSelected] = useState<string | null>(null)

  // ── Real strategies from backend ───────────────────────────────────────────
  const [savedStrategies, setSavedStrategies] = useState<any[]>([])
  const [savedLoading, setSavedLoading] = useState(false)

  const loadSaved = useCallback(async () => {
    setSavedLoading(true)
    try {
      const data = await api.strategyStatus()
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
      await api.runStrategy(name)
      toast(`Strategy "${name}" started`, 'success')
      loadSaved()
    } catch (e: any) {
      toast(e?.message || 'Failed to start strategy', 'error')
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

  const filtered = strategies.filter(s => {
    if (tab === 'live')       return s.mode === 'live'
    if (tab === 'paper')      return s.mode === 'paper'
    if (tab === 'backtesting') return s.status === 'backtesting'
    return true
  })

  function toggleMode(id: string) {
    setStrategies(prev => prev.map(s => {
      if (s.id !== id) return s
      const next: StrategyMode = s.mode === 'paper' ? 'live' : 'paper'
      toast(`${s.name}: switched to ${next.toUpperCase()} mode`, 'info')
      return { ...s, mode: next }
    }))
  }

  function toggleStatus(id: string) {
    setStrategies(prev => prev.map(s => {
      if (s.id !== id) return s
      if (s.status === 'running') {
        toast(`${s.name}: stopped`, 'warning')
        return { ...s, status: 'stopped' as StrategyStatus }
      }
      toast(`${s.name}: started`, 'success')
      return { ...s, status: 'running' as StrategyStatus }
    }))
  }

  const selectedStrategy = strategies.find(s => s.id === selected)

  const TABS: { id: PageTab; label: string; icon?: typeof GitBranch }[] = [
    { id: 'all',         label: 'All' },
    { id: 'live',        label: 'Live' },
    { id: 'paper',       label: 'Paper' },
    { id: 'backtesting', label: 'Backtesting' },
    { id: 'monitor',     label: '⚡ Live Monitor' },
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
          <button
            onClick={() => navigate('/app/strategy-builder')}
            className="btn-primary btn-sm"
          >
            <Cpu className="w-3.5 h-3.5" /> Strategy Builder
          </button>
        </div>

        {/* ── Saved Strategies Panel ── */}
        {tab !== 'monitor' && (
          <SavedStrategiesPanel
            strategies={savedStrategies}
            loading={savedLoading}
            onRefresh={loadSaved}
            onRun={handleRun}
            onStop={handleStop}
            onEdit={name => navigate(`/app/strategy-builder?name=${encodeURIComponent(name)}`)}
            onDelete={handleDelete}
          />
        )}

        {/* Summary KPIs — hide on monitor tab */}
        {tab !== 'monitor' && <SummaryKPIs strategies={strategies} />}

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
            <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
              {filtered.map(s => (
                <StrategyCardItem
                  key={s.id}
                  strategy={s}
                  isSelected={selected === s.id}
                  onSelect={() => setSelected(selected === s.id ? null : s.id)}
                  onToggleMode={() => toggleMode(s.id)}
                  onToggleStatus={() => toggleStatus(s.id)}
                />
              ))}
            </div>

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

// ── Summary KPIs ──────────────────────────────────
function SummaryKPIs({ strategies }: { strategies: StrategyCard[] }) {
  const running = strategies.filter(s => s.status === 'running')
  const totalPnl = strategies.reduce((a, s) => a + s.pnl, 0)
  const liveCount = strategies.filter(s => s.mode === 'live' && s.status === 'running').length
  const paperCount = strategies.filter(s => s.mode === 'paper' && s.status === 'running').length
  const avgWinRate = running.length
    ? running.reduce((a, s) => a + s.winRate, 0) / running.length
    : 0

  const kpis = [
    { label: 'Total P&L', value: fmtINR(totalPnl), cls: pnlClass(totalPnl), icon: Activity },
    { label: 'Live Running', value: String(liveCount), cls: 'text-profit', icon: Zap },
    { label: 'Paper Running', value: String(paperCount), cls: 'text-brand', icon: FlaskConical },
    { label: 'Avg Win Rate', value: `${avgWinRate.toFixed(1)}%`, cls: 'text-text-bright', icon: TrendingUp },
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

// ── Strategy Card ──────────────────────────────────
function StrategyCardItem({
  strategy: s, isSelected, onSelect, onToggleMode, onToggleStatus,
}: {
  strategy: StrategyCard
  isSelected: boolean
  onSelect: () => void
  onToggleMode: () => void
  onToggleStatus: () => void
}) {
  const statusIcon = {
    running:     <CheckCircle className="w-3.5 h-3.5 text-profit" />,
    stopped:     <Square className="w-3.5 h-3.5 text-text-muted" />,
    error:       <XCircle className="w-3.5 h-3.5 text-loss" />,
    backtesting: <RefreshCw className="w-3.5 h-3.5 text-brand animate-spin" />,
  }[s.status]

  const statusLabel = {
    running: 'Running', stopped: 'Stopped', error: 'Error', backtesting: 'Backtesting',
  }[s.status]

  return (
    <div className={cn(
      'bg-bg-card border rounded-xl overflow-hidden transition-all duration-200',
      isSelected ? 'border-brand shadow-lg shadow-brand/10' : 'border-border hover:border-border-strong'
    )}>
      {/* Card header */}
      <div className="px-4 py-3 border-b border-border/60">
        <div className="flex items-start justify-between gap-2">
          <div className="min-w-0">
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-[13px] font-semibold text-text-bright truncate">{s.name}</span>
              <span className={cn('badge text-[9px]',
                s.mode === 'live' ? 'badge-danger' : 'badge-brand')}>
                {s.mode.toUpperCase()}
              </span>
              <span className="badge badge-neutral text-[9px]">{s.type}</span>
            </div>
            <p className="text-[11px] text-text-muted mt-0.5 line-clamp-2">{s.description}</p>
          </div>
        </div>

        <div className="flex items-center gap-2 mt-2">
          {statusIcon}
          <span className={cn('text-[11px] font-medium',
            s.status === 'running' ? 'text-profit' :
            s.status === 'error'   ? 'text-loss' :
            s.status === 'backtesting' ? 'text-brand' : 'text-text-muted')}>
            {statusLabel}
          </span>
          <div className="flex-1" />
          <div className="flex items-center gap-1 flex-wrap">
            {s.instruments.map(i => (
              <span key={i} className="text-[10px] px-1.5 py-0.5 bg-bg-elevated rounded text-text-muted">
                {i}
              </span>
            ))}
          </div>
        </div>
      </div>

      {/* Metrics grid */}
      {s.status !== 'backtesting' && (
        <div className="grid grid-cols-3 divide-x divide-border/50 border-b border-border/60">
          <MetricCell label="P&L" value={fmtINR(s.pnl)} cls={pnlClass(s.pnl)} />
          <MetricCell label="Win Rate" value={`${s.winRate}%`}
            cls={s.winRate >= 60 ? 'text-profit' : s.winRate >= 45 ? 'text-warning' : 'text-loss'} />
          <MetricCell label="Trades" value={String(s.trades)} cls="text-text-bright" />
        </div>
      )}

      {/* Last signal */}
      {s.status !== 'backtesting' && s.lastSignal !== '—' && (
        <div className="px-4 py-2 border-b border-border/40 flex items-center gap-2">
          <Activity className="w-3.5 h-3.5 text-text-muted shrink-0" />
          <span className="text-[11px] text-text-muted">Last signal:</span>
          <span className="text-[11px] font-mono text-brand truncate">{s.lastSignal}</span>
          <span className="text-[10px] text-text-muted ml-auto shrink-0">{s.lastSignalTime}</span>
        </div>
      )}

      {/* Actions */}
      <div className="flex items-center gap-2 px-3 py-2.5">
        {/* Mode toggle */}
        <button
          onClick={e => { e.stopPropagation(); onToggleMode() }}
          className={cn(
            'flex items-center gap-1.5 px-2.5 py-1 rounded text-[11px] font-medium transition-colors',
            s.mode === 'live'
              ? 'bg-loss/10 text-loss border border-loss/20 hover:bg-loss/20'
              : 'bg-brand/10 text-brand border border-brand/20 hover:bg-brand/20'
          )}
          title={`Switch to ${s.mode === 'live' ? 'paper' : 'live'} mode`}
        >
          {s.mode === 'live'
            ? <ToggleRight className="w-3.5 h-3.5" />
            : <ToggleLeft className="w-3.5 h-3.5" />}
          {s.mode === 'live' ? 'Live' : 'Paper'}
        </button>

        {/* Start/Stop */}
        <button
          onClick={e => { e.stopPropagation(); onToggleStatus() }}
          disabled={s.status === 'backtesting'}
          className={cn(
            'flex items-center gap-1.5 px-2.5 py-1 rounded text-[11px] font-medium transition-colors',
            s.status === 'running'
              ? 'bg-loss/10 text-loss border border-loss/20 hover:bg-loss/20'
              : s.status === 'backtesting'
              ? 'bg-brand/10 text-brand border border-brand/20 opacity-60 cursor-not-allowed'
              : 'bg-profit/10 text-profit border border-profit/20 hover:bg-profit/20'
          )}
        >
          {s.status === 'running'
            ? <><Square className="w-3 h-3" /> Stop</>
            : s.status === 'backtesting'
            ? <><RefreshCw className="w-3 h-3" /> Testing</>
            : <><Play className="w-3 h-3" /> Start</>}
        </button>

        <div className="flex-1" />

        {/* Detail toggle */}
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

// ── Strategy Detail / Equity Curve ────────────────
function StrategyDetail({ strategy: s, onClose }: { strategy: StrategyCard; onClose: () => void }) {
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)

  useEffect(() => {
    if (!chartContainerRef.current || s.equity.length === 0) return

    const chart = createChart(chartContainerRef.current, {
      layout: { background: { color: 'transparent' }, textColor: '#7b8398' },
      grid: { vertLines: { color: '#252b3b' }, horzLines: { color: '#252b3b' } },
      crosshair: { mode: 1 },
      rightPriceScale: { borderColor: '#252b3b' },
      timeScale: { borderColor: '#252b3b', timeVisible: true },
      width: chartContainerRef.current.clientWidth,
      height: 200,
    })

    const finalVal = s.equity[s.equity.length - 1]?.value ?? 0
    const startVal = s.equity[0]?.value ?? 0
    const isProfit = finalVal >= startVal

    const line = chart.addSeries(LineSeries, {
      color: isProfit ? '#22c55e' : '#f43f5e',
      lineWidth: 2,
      priceLineVisible: false,
    })

    line.setData(s.equity.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
    chart.timeScale().fitContent()
    chartRef.current = chart

    const ro = new ResizeObserver(entries => {
      const { width } = entries[0].contentRect
      chart.applyOptions({ width })
    })
    ro.observe(chartContainerRef.current)

    return () => { ro.disconnect(); chart.remove() }
  }, [s])

  return (
    <div className="bg-bg-card border border-brand/30 rounded-xl overflow-hidden">
      <div className="flex items-center gap-3 px-4 py-3 border-b border-border">
        <BarChart2 className="w-4 h-4 text-brand" />
        <span className="text-[13px] font-semibold text-text-bright">{s.name} — Performance</span>
        <div className="flex-1" />
        <button onClick={onClose} className="text-text-muted hover:text-text-bright text-[12px]">✕ Close</button>
      </div>

      <div className="p-4 space-y-4">
        {/* Metrics row */}
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          {[
            { label: 'Total P&L', value: fmtINR(s.pnl), cls: pnlClass(s.pnl) },
            { label: 'P&L %', value: `${s.pnlPct >= 0 ? '+' : ''}${s.pnlPct.toFixed(2)}%`, cls: pnlClass(s.pnlPct) },
            { label: 'Max Drawdown', value: fmtINR(s.maxDrawdown), cls: 'text-loss' },
            { label: 'Sharpe Ratio', value: s.sharpe.toFixed(2), cls: s.sharpe >= 1 ? 'text-profit' : 'text-warning' },
          ].map(m => (
            <div key={m.label} className="kpi-card">
              <div className="text-[10px] text-text-muted">{m.label}</div>
              <div className={cn('text-[16px] font-bold font-mono mt-1', m.cls)}>{m.value}</div>
            </div>
          ))}
        </div>

        {/* Equity chart */}
        {s.equity.length > 0 ? (
          <div>
            <div className="text-[11px] text-text-muted mb-2">Equity Curve (60 days)</div>
            <div ref={chartContainerRef} className="w-full" />
          </div>
        ) : (
          <div className="h-32 flex items-center justify-center text-text-muted text-[12px]">
            Backtest in progress…
          </div>
        )}

        {/* Trade stats */}
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-[12px]">
          <StatRow label="Total Trades" value={String(s.trades)} />
          <StatRow label="Win Rate" value={`${s.winRate}%`} />
          <StatRow label="Open Positions" value={String(s.openPositions)} />
          <StatRow label="Mode" value={s.mode.toUpperCase()} />
        </div>
      </div>
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

// ── Backtesting Panel ─────────────────────────────
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
        <span className="badge badge-brand text-[9px] ml-1">DEMO</span>
      </div>

      <div className="p-4 space-y-4">
        {/* Form */}
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-3">
          {[
            { label: 'Strategy', key: 'strategy', type: 'select', options: ['Iron Condor', 'VWAP Scalper', 'Momentum Breakout', 'Strangle', 'Pairs Arb'] },
            { label: 'Instrument', key: 'instrument', type: 'select', options: ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY'] },
            { label: 'From', key: 'from', type: 'date', options: [] },
            { label: 'To', key: 'to', type: 'date', options: [] },
            { label: 'Capital (₹)', key: 'capital', type: 'text', options: [] },
          ].map(f => (
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

        <button
          onClick={runBacktest}
          disabled={running}
          className="btn-primary btn-sm"
        >
          {running
            ? <><RefreshCw className="w-3.5 h-3.5 animate-spin" /> Running…</>
            : <><Play className="w-3.5 h-3.5" /> Run Backtest</>}
        </button>

        {/* Progress */}
        {running && (
          <div className="space-y-1.5">
            <div className="flex items-center justify-between text-[11px] text-text-muted">
              <span>Processing trades…</span>
              <span>{progress}%</span>
            </div>
            <div className="w-full h-1.5 bg-bg-elevated rounded-full overflow-hidden">
              <div
                className="h-full bg-brand rounded-full transition-all duration-200"
                style={{ width: `${progress}%` }}
              />
            </div>
          </div>
        )}

        {/* Results */}
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
