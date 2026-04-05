/* ════════════════════════════════════════════
   Strategy Page — strategy management hub
   ════════════════════════════════════════════ */
import { useState, useRef, useEffect, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { useToastStore } from '../stores'
import { api } from '../lib/api'
import { cn, fmtINR, fmtNum, pnlClass } from '../lib/utils'
import {
  GitBranch, Play, Square, RefreshCw,
  CheckCircle, AlertCircle,
  ChevronDown,
  Wifi,
  Loader2, PauseCircle, PlayCircle, Cpu, Pencil, Trash2,
} from 'lucide-react'

// ── Types ─────────────────────────────────────────
type PageTab = 'all' | 'live' | 'paper' | 'monitor'

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

type MonitorMode = 'live'

function demoPositions(): LiveLeg[] {
  return []
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
  const [mode, setMode]               = useState<MonitorMode>('live')
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
      // Show empty state instead of fake demo data
      setPositions([])
      setMode('live')
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

  // ── Real strategies from backend ───────────────────────────────────────────
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

  const filtered = savedStrategies.filter((s: any) => {
    if (tab === 'live')  return !s.paper_mode
    if (tab === 'paper') return s.paper_mode
    return true
  })

  const TABS: { id: PageTab; label: string; icon?: typeof GitBranch }[] = [
    { id: 'all',         label: 'All' },
    { id: 'live',        label: 'Live' },
    { id: 'paper',       label: 'Paper' },
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

        {/* Tab bar */}
        <div className="flex items-center gap-1 bg-bg-surface border border-border rounded-lg p-1 w-fit overflow-x-auto">
          {TABS.map(t => (
            <button
              key={t.id}
              onClick={() => setTab(t.id)}
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

        {/* ── Saved Strategies (non-monitor tabs) ── */}
        {tab !== 'monitor' && (
          <SavedStrategiesPanel
            strategies={filtered}
            loading={savedLoading}
            onRefresh={loadSaved}
            onRun={handleRun}
            onStop={handleStop}
            onEdit={name => navigate(`/app/strategy-builder?name=${encodeURIComponent(name)}`)}
            onDelete={handleDelete}
          />
        )}
      </div>
    </div>
  )
}


