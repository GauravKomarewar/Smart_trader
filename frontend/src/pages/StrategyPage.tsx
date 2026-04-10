/* ════════════════════════════════════════════
   Strategy Page — strategy management hub
   Fresh UI with broker/symbol run-time selection,
   quick-run presets, live monitor, and run history.
   ════════════════════════════════════════════ */
import { useState, useRef, useEffect, useCallback, useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import { useToastStore } from '../stores'
import { api } from '../lib/api'
import { cn } from '../lib/utils'
import {
  GitBranch, Play, Square, RefreshCw, TrendingUp,
  AlertCircle, Zap, FlaskConical,
  Activity,
  Wifi, Loader2, PauseCircle, PlayCircle, Cpu, Pencil, Trash2,
  Clock, Layers, Settings2, Shield, BarChart3, Target, BarChart2,
  Repeat, ChevronRight, X, Search, History, ChevronDown, ChevronUp,
  ArrowUpRight, ArrowDownRight, Info,
} from 'lucide-react'
import {
  ResponsiveContainer, AreaChart, Area, XAxis, YAxis, Tooltip as RTooltip,
  CartesianGrid, ReferenceLine,
} from 'recharts'

// ── Types ─────────────────────────────────────────
type PageTab = 'all' | 'running' | 'monitor' | 'history'

// ── Quick-run presets stored in localStorage ──────
const QR_KEY = 'st_quick_run_presets'
type QuickRunPreset = { symbol: string; exchange: string; broker: string; paper: boolean }

function loadPresets(): Record<string, QuickRunPreset> {
  try { return JSON.parse(localStorage.getItem(QR_KEY) || '{}') } catch { return {} }
}
function savePreset(name: string, p: QuickRunPreset) {
  const all = loadPresets()
  all[name] = p
  localStorage.setItem(QR_KEY, JSON.stringify(all))
}

// ── Type config for strategy badges ───────────────
const TYPE_CONFIG: Record<string, { icon: typeof TrendingUp; color: string; bg: string; label: string }> = {
  neutral:     { icon: Target,     color: 'text-brand',      bg: 'bg-brand/10',      label: 'Neutral' },
  bullish:     { icon: TrendingUp, color: 'text-profit',     bg: 'bg-profit/10',     label: 'Bullish' },
  bearish:     { icon: BarChart3,  color: 'text-loss',       bg: 'bg-loss/10',       label: 'Bearish' },
  scalping:    { icon: Zap,        color: 'text-yellow-400', bg: 'bg-yellow-400/10', label: 'Scalping' },
  hedging:     { icon: Shield,     color: 'text-cyan-400',   bg: 'bg-cyan-400/10',   label: 'Hedging' },
  directional: { icon: TrendingUp, color: 'text-profit',     bg: 'bg-profit/10',     label: 'Directional' },
  spread:      { icon: Layers,     color: 'text-orange-400', bg: 'bg-orange-400/10', label: 'Spread' },
  volatility:  { icon: Activity,   color: 'text-purple-400', bg: 'bg-purple-400/10', label: 'Volatility' },
  gamma_scalp: { icon: Zap,        color: 'text-yellow-400', bg: 'bg-yellow-400/10', label: 'Gamma Scalp' },
  custom:      { icon: Settings2,  color: 'text-text-muted', bg: 'bg-bg-elevated',   label: 'Custom' },
}

/* ════════════════════════════════════════════════
   Summary KPIs
   ════════════════════════════════════════════════ */
function SummaryKPIs({ strategies }: { strategies: any[] }) {
  const total      = strategies.length
  const liveCount  = strategies.filter(s => !s.paper_mode && s.status === 'running').length
  const paperCount = strategies.filter(s => s.paper_mode && s.status === 'running').length
  const errors     = strategies.filter(s => s.status === 'error').length

  const kpis = [
    { label: 'Total',  value: String(total), cls: 'text-text-bright', icon: Activity, live: false },
    { label: 'Live',   value: String(liveCount), cls: liveCount > 0 ? 'text-loss' : 'text-text-muted', icon: Zap, live: liveCount > 0 },
    { label: 'Paper',  value: String(paperCount), cls: paperCount > 0 ? 'text-brand' : 'text-text-muted', icon: FlaskConical, live: false },
    { label: 'Errors', value: String(errors), cls: errors > 0 ? 'text-loss' : 'text-text-muted', icon: AlertCircle, live: false },
  ]

  return (
    <div className="grid grid-cols-4 gap-2">
      {kpis.map(k => {
        const Icon = k.icon
        return (
          <div key={k.label} className="kpi-card">
            <div className="flex items-center gap-1.5">
              {k.live ? (
                <span className="w-2 h-2 rounded-full bg-loss live-dot-blink shadow-[0_0_6px_rgba(244,63,94,0.5)]" />
              ) : (
                <Icon className="w-3 h-3 text-text-muted" />
              )}
              <span className="text-[10px] text-text-muted">{k.label}</span>
            </div>
            <div className={cn('text-[18px] font-bold font-mono mt-0.5', k.cls)}>{k.value}</div>
          </div>
        )
      })}
    </div>
  )
}

/* ════════════════════════════════════════════════
   Symbol Picker — grouped dropdown with search
   Handles 80+ symbols with category tabs
   ════════════════════════════════════════════════ */
function SymbolPicker({
  symbols, loading, selectedSymbol, selectedExchange, onSelect,
}: {
  symbols: any[]; loading: boolean;
  selectedSymbol: string; selectedExchange: string;
  onSelect: (sym: string, exch: string) => void;
}) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState('')
  const [cat, setCat] = useState<string>('index')
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!open) return
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [open])

  const fallback = [
    { symbol: 'NIFTY', exchange: 'NFO', category: 'index' },
    { symbol: 'BANKNIFTY', exchange: 'NFO', category: 'index' },
    { symbol: 'FINNIFTY', exchange: 'NFO', category: 'index' },
    { symbol: 'SENSEX', exchange: 'BFO', category: 'index' },
  ]
  const all = symbols.length > 0 ? symbols : fallback

  const categories = useMemo(() => {
    const cats: Record<string, any[]> = {}
    for (const s of all) {
      const c = s.category || 'stock'
      if (!cats[c]) cats[c] = []
      cats[c].push(s)
    }
    return cats
  }, [all])

  const catOrder = ['index', 'commodity', 'stock']
  const catLabels: Record<string, string> = { index: 'Indices', commodity: 'MCX', stock: 'Stocks' }

  const filtered = useMemo(() => {
    const list = categories[cat] || []
    if (!search) return list
    const q = search.toLowerCase()
    return list.filter((s: any) => s.symbol.toLowerCase().includes(q))
  }, [categories, cat, search])

  if (loading) return <Loader2 className="w-4 h-4 animate-spin text-brand" />

  return (
    <div className="relative" ref={ref} onClick={e => e.stopPropagation()}>
      <button
        onClick={() => setOpen(!open)}
        className={cn(
          'flex items-center gap-2 px-3 py-1.5 rounded-lg text-[11px] font-bold font-mono border transition-all w-full max-w-xs',
          'bg-brand/10 text-brand border-brand/30 hover:bg-brand/20'
        )}
      >
        <span>{selectedSymbol}</span>
        <span className="text-[9px] text-text-muted font-normal">{selectedExchange}</span>
        <ChevronDown className={cn('w-3 h-3 ml-auto transition-transform', open && 'rotate-180')} />
      </button>

      {open && (
        <div className="absolute z-[100] mt-1 left-0 w-96 bg-bg-card border border-border rounded-xl shadow-2xl overflow-hidden">
          <div className="px-3 py-2 border-b border-border">
            <div className="flex items-center gap-2 px-2 py-1 bg-bg-elevated rounded-lg">
              <Search className="w-3 h-3 text-text-muted" />
              <input
                autoFocus
                value={search}
                onChange={e => setSearch(e.target.value)}
                placeholder="Search symbols..."
                className="flex-1 bg-transparent text-[11px] text-text-bright outline-none"
              />
            </div>
          </div>
          <div className="flex items-center gap-1 px-3 py-1.5 border-b border-border">
            {catOrder.filter(c => categories[c]?.length).map(c => (
              <button
                key={c}
                onClick={() => { setCat(c); setSearch('') }}
                className={cn(
                  'px-2 py-0.5 rounded text-[10px] font-semibold transition-colors',
                  cat === c ? 'bg-brand/20 text-brand' : 'text-text-muted hover:text-text-sec'
                )}
              >
                {catLabels[c] || c} ({categories[c]?.length || 0})
              </button>
            ))}
          </div>
          <div className="max-h-64 overflow-y-auto p-2">
            <div className="flex flex-wrap gap-1">
              {filtered.map((s: any) => (
                <button
                  key={`${s.exchange}:${s.symbol}`}
                  onClick={() => { onSelect(s.symbol, s.exchange); setOpen(false) }}
                  className={cn(
                    'px-2 py-1 rounded text-[10px] font-bold font-mono border transition-all',
                    selectedSymbol === s.symbol && selectedExchange === s.exchange
                      ? 'bg-brand/20 text-brand border-brand/50'
                      : 'bg-bg-elevated text-text-sec border-border hover:border-brand/30'
                  )}
                >
                  {s.symbol}
                </button>
              ))}
              {filtered.length === 0 && (
                <span className="text-[11px] text-text-muted p-2">No matches</span>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

/* ════════════════════════════════════════════════
   Strategy Card — compact row with inline run panel
   ════════════════════════════════════════════════ */
function StrategyCard({
  s, expanded, onToggle, onRun, onStop, onEdit, onDelete, brokers, symbols, loadingBS,
}: {
  s: any
  expanded: boolean
  onToggle: () => void
  onRun: (name: string, overrides?: any) => Promise<void>
  onStop: (name: string) => void
  onEdit: (name: string) => void
  onDelete: (name: string) => void
  brokers: any[]
  symbols: any[]
  loadingBS: boolean
}) {
  const isRunning = s.status === 'running'
  const isError   = s.status === 'error'
  const isPaper   = !!s.paper_mode
  const typeConf  = TYPE_CONFIG[s.type] || TYPE_CONFIG.neutral
  const TypeIcon  = typeConf.icon
  const displayName = (s.name || s.id || '').replace(/_/g, ' ')
  const legCount = s.legs ?? 0
  const lots = s.lots ?? 1

  // Quick-run state (saved per strategy)
  const presets = loadPresets()
  const saved = presets[s.name]
  const [selSymbol, setSelSymbol]     = useState(saved?.symbol || 'NIFTY')
  const [selExchange, setSelExchange] = useState(saved?.exchange || 'NFO')
  const [selBroker, setSelBroker]     = useState(saved?.broker || '__paper__')
  const [running, setRunning]         = useState(false)

  // Sync broker selection if brokers load after mount
  useEffect(() => {
    if (!brokers.length) return
    if (!saved?.broker) {
      const paper = brokers.find(b => b.mode === 'PAPER')
      if (paper) setSelBroker(paper.config_id)
    }
  }, [brokers, saved?.broker])

  const runIsPaper = selBroker === '__paper__' || brokers.find(b => b.config_id === selBroker)?.mode === 'PAPER'

  async function handleQuickRun() {
    setRunning(true)
    const overrides = {
      symbol: selSymbol,
      exchange: selExchange,
      paper_mode: runIsPaper,
      broker_config_id: selBroker,
    }
    savePreset(s.name, { symbol: selSymbol, exchange: selExchange, broker: selBroker, paper: runIsPaper })
    try {
      await onRun(s.name, overrides)
    } catch { /* parent handles toast */ } finally {
      setRunning(false)
    }
  }

  const modeClass = isPaper ? 'strat-card-mock' : isRunning ? 'strat-card-live' : ''
  const runClass  = isRunning ? 'strat-card-running' : ''

  return (
    <div className={cn('relative rounded-xl border transition-all duration-200', modeClass, runClass, expanded && 'ring-1 ring-brand/30')}>
      {/* ── Card Header (always visible) ── */}
      <div className="px-4 py-3 cursor-pointer" onClick={onToggle}>
        <div className="flex items-center gap-2.5 flex-wrap">
          {/* Status dot */}
          {isRunning ? (
            <span className="w-2.5 h-2.5 rounded-full bg-profit live-dot-blink shrink-0 shadow-[0_0_8px_rgba(34,197,94,0.6)]" />
          ) : isError ? (
            <span className="w-2.5 h-2.5 rounded-full bg-loss shrink-0" />
          ) : (
            <span className="w-2.5 h-2.5 rounded-full bg-text-muted/30 shrink-0" />
          )}

          {/* Type icon */}
          <div className={cn('p-1 rounded-md shrink-0', typeConf.bg)}>
            <TypeIcon className={cn('w-3.5 h-3.5', typeConf.color)} />
          </div>

          {/* Name */}
          <span className="text-[13px] font-semibold text-text-bright capitalize truncate">{displayName}</span>

          {/* Mode badge */}
          {isRunning && (
            isPaper ? (
              <span className="text-[9px] px-2 py-0.5 rounded-full font-bold border border-brand/40 bg-brand/15 text-brand tracking-wider">🧪 PAPER</span>
            ) : (
              <span className="text-[9px] px-2 py-0.5 rounded-full font-bold border border-loss/40 bg-loss/15 text-[#fda4af] tracking-wider">
                <span className="w-1.5 h-1.5 rounded-full bg-current live-dot-blink inline-block mr-1" />LIVE
              </span>
            )
          )}

          {/* Status */}
          {isRunning && (
            <span className="text-[9px] px-2 py-0.5 rounded-full font-bold border border-profit/40 bg-profit/15 text-profit">RUNNING</span>
          )}

          <span className="text-[9px] badge badge-neutral">{typeConf.label}</span>

          {/* Meta pills */}
          <div className="flex items-center gap-1.5 ml-auto shrink-0">
            <span className="text-[10px] text-text-muted font-mono">{legCount}L × {lots}</span>
            {s.entry_time && <span className="text-[9px] text-text-muted">{s.entry_time}–{s.exit_time || '15:15'}</span>}
          </div>

          {/* Arrow */}
          <ChevronRight className={cn('w-3.5 h-3.5 text-text-muted transition-transform shrink-0', expanded && 'rotate-90')} />
        </div>

        {/* Error */}
        {isError && s.error && (
          <div className="mt-2 flex items-start gap-1.5 text-[10px] text-loss bg-loss/5 border border-loss/20 rounded-lg px-2.5 py-2">
            <AlertCircle className="w-3.5 h-3.5 shrink-0 mt-0.5" />
            <span className="line-clamp-2">{s.error}</span>
          </div>
        )}

        {/* Running metrics */}
        {isRunning && s.active_legs != null && (
          <div className="mt-2 flex items-center gap-4 text-[11px]">
            <span className="text-text-muted">Legs: <strong className="text-text-bright font-mono">{s.active_legs}</strong></span>
            <span className="text-text-muted">PnL: <strong className={cn('font-mono', Number(s.combined_pnl) >= 0 ? 'text-profit' : 'text-loss')}>
              ₹{Number(s.combined_pnl ?? 0).toFixed(0)}
            </strong></span>
            {s.spot_price && <span className="text-text-muted">Spot: <strong className="text-brand font-mono">{Number(s.spot_price).toFixed(0)}</strong></span>}
          </div>
        )}
      </div>

      {/* ── Expanded Panel ── */}
      {expanded && (
        <div className="border-t border-white/[0.06]">
          {/* Actions row */}
          <div className="flex items-center gap-2 px-4 py-2 border-b border-white/[0.06] flex-wrap">
            <button onClick={() => onEdit(s.name)} className="flex items-center gap-1.5 px-2.5 py-1 rounded text-[11px] font-medium bg-brand/10 text-brand border border-brand/20 hover:bg-brand/20 transition-colors">
              <Pencil className="w-3 h-3" /> Edit
            </button>
            <button onClick={() => onDelete(s.name)} className="flex items-center gap-1.5 px-2.5 py-1 rounded text-[11px] font-medium text-text-muted hover:text-loss hover:bg-loss/10 border border-white/[0.08] hover:border-loss/20 transition-colors">
              <Trash2 className="w-3 h-3" /> Delete
            </button>
            {isRunning && (
              <button onClick={() => onStop(s.name)} className="flex items-center gap-1.5 px-2.5 py-1 rounded text-[11px] font-medium bg-loss/10 text-loss border border-loss/20 hover:bg-loss/20 transition-colors">
                <Square className="w-3 h-3" /> Stop
              </button>
            )}
          </div>

          {/* Run panel (only when not running) */}
          {!isRunning && (
            <div className="px-4 py-3 space-y-3">
              {/* Symbol selection — grouped dropdown with search */}
              <div>
                <label className="text-[9px] text-text-muted uppercase tracking-wider font-semibold block mb-1">Symbol</label>
                <SymbolPicker
                  symbols={symbols}
                  loading={loadingBS}
                  selectedSymbol={selSymbol}
                  selectedExchange={selExchange}
                  onSelect={(sym, exch) => { setSelSymbol(sym); setSelExchange(exch) }}
                />
              </div>

              {/* Broker selection */}
              <div>
                <label className="text-[9px] text-text-muted uppercase tracking-wider font-semibold block mb-1">Broker</label>
                <div className="flex items-center gap-1.5 flex-wrap">
                  {loadingBS ? (
                    <Loader2 className="w-4 h-4 animate-spin text-brand" />
                  ) : brokers.map(b => (
                    <button
                      key={b.config_id}
                      onClick={(e) => { e.stopPropagation(); setSelBroker(b.config_id) }}
                      className={cn(
                        'px-2.5 py-1 rounded-md text-[10px] font-semibold border transition-all',
                        selBroker === b.config_id
                          ? b.mode === 'PAPER'
                            ? 'bg-brand/20 text-brand border-brand/50'
                            : 'bg-loss/20 text-[#fda4af] border-loss/50'
                          : 'bg-bg-elevated text-text-sec border-border hover:border-brand/30'
                      )}
                    >
                      {b.mode === 'PAPER' ? '🧪 ' : '⚡ '}{b.label}
                    </button>
                  ))}
                </div>
              </div>

              {/* Run bar */}
              <div className="flex items-center gap-3 pt-1">
                <div className="flex items-center gap-2 text-[10px] text-text-muted">
                  <span className="font-mono font-bold text-brand">{selSymbol}</span>
                  <span>·</span>
                  <span>{selExchange}</span>
                  <span>·</span>
                  <span className={cn('font-bold', runIsPaper ? 'text-brand' : 'text-[#fda4af]')}>
                    {runIsPaper ? '🧪 Paper' : '⚡ Live'}
                  </span>
                </div>
                <div className="flex-1" />

                {/* Quick-run indicator */}
                {saved && (
                  <span className="text-[9px] text-text-muted flex items-center gap-1">
                    <Repeat className="w-3 h-3" /> Last: {saved.symbol} / {saved.paper ? 'Paper' : 'Live'}
                  </span>
                )}

                <button
                  onClick={handleQuickRun}
                  disabled={running}
                  className={cn(
                    'flex items-center gap-1.5 px-4 py-1.5 rounded-lg text-[11px] font-bold transition-all',
                    runIsPaper
                      ? 'bg-brand text-bg-base hover:bg-brand/80'
                      : 'bg-loss text-white hover:bg-loss/80',
                    running && 'opacity-50 cursor-not-allowed'
                  )}
                >
                  {running ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Play className="w-3.5 h-3.5" />}
                  {runIsPaper ? 'Run Paper' : 'Run LIVE'}
                </button>
              </div>
            </div>
          )}

          {/* Strategy config detail (when running) */}
          {isRunning && <StrategyRunDetail name={s.name} />}
        </div>
      )}
    </div>
  )
}

/* ════════════════════════════════════════════════
   Strategy Run Detail — shows config when running
   ════════════════════════════════════════════════ */
function StrategyRunDetail({ name }: { name: string }) {
  const [cfg, setCfg] = useState<any>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let c = false
    api.strategyConfig(name).then(d => { if (!c) setCfg(d) }).catch(() => {}).finally(() => { if (!c) setLoading(false) })
    return () => { c = true }
  }, [name])

  if (loading) return <div className="flex items-center justify-center h-20"><Loader2 className="w-4 h-4 animate-spin text-brand" /></div>
  if (!cfg) return null

  const timing = cfg.timing || {}
  const entry  = cfg.entry || {}
  const exit   = cfg.exit || {}
  const legs   = entry.legs || []

  return (
    <div className="px-4 py-3 space-y-3 text-[11px]">
      {/* Timing */}
      <div className="flex items-center gap-3 text-text-muted">
        <Clock className="w-3.5 h-3.5 shrink-0" />
        <span>Entry: <strong className="text-text-bright font-mono">{timing.entry_window_start || '—'}</strong></span>
        <span>–</span>
        <span>Exit: <strong className="text-text-bright font-mono">{timing.eod_exit_time || '—'}</strong></span>
      </div>

      {/* Legs table */}
      {legs.length > 0 && (
        <div>
          <div className="text-[9px] text-text-muted uppercase tracking-wider font-semibold mb-1 flex items-center gap-1.5">
            <Layers className="w-3 h-3" /> {legs.length} Legs
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-[10px]">
              <thead>
                <tr className="text-[8px] text-text-muted uppercase tracking-wider bg-bg-elevated/50">
                  <th className="px-2 py-1.5 text-left">Tag</th>
                  <th className="px-2 py-1.5 text-center">Side</th>
                  <th className="px-2 py-1.5 text-center">Type</th>
                  <th className="px-2 py-1.5 text-center">Strike</th>
                  <th className="px-2 py-1.5 text-right">Lots</th>
                </tr>
              </thead>
              <tbody>
                {legs.map((leg: any, i: number) => (
                  <tr key={i} className="border-t border-border/30">
                    <td className="px-2 py-1.5 font-mono text-brand">{leg.tag || leg.label || `L${i+1}`}</td>
                    <td className="px-2 py-1.5 text-center">
                      <span className={cn('text-[9px] font-bold px-1.5 py-0.5 rounded', leg.side === 'BUY' ? 'text-profit bg-profit/10' : 'text-loss bg-loss/10')}>{leg.side}</span>
                    </td>
                    <td className="px-2 py-1.5 text-center text-text-sec">{leg.option_type || leg.instrument || '—'}</td>
                    <td className="px-2 py-1.5 text-center font-mono text-text-bright">{leg.strike_selection} {leg.strike_value || ''}</td>
                    <td className="px-2 py-1.5 text-right font-mono text-text-bright">{leg.lots ?? 1}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Exit rules summary */}
      {exit.stop_loss?.amount && (
        <div className="text-text-muted">
          SL: <strong className="text-loss font-mono">₹{exit.stop_loss.amount}</strong>
          {exit.profit_target?.amount && <> · Target: <strong className="text-profit font-mono">₹{exit.profit_target.amount}</strong></>}
          {exit.trailing?.trail_amount && <> · Trail: <strong className="text-yellow-400 font-mono">₹{exit.trailing.trail_amount}</strong></>}
        </div>
      )}
    </div>
  )
}

/* ════════════════════════════════════════════════
   Live Position Monitor
   Shoonya-style: imperative DOM updates to avoid flicker.
   HTML structure is built once, then values are patched
   in-place via data-attributes. Only the container ref
   is React-managed; everything inside is direct DOM.
   ════════════════════════════════════════════════ */

const POLL_MS = 1000   // 1s polling to match strategy tick cadence

// ── Helpers (same as Shoonya) ─────────────────────
const f2 = (n: unknown) => { const v = Number(n || 0); return Number.isFinite(v) ? v.toFixed(2) : '0.00' }
const pSign = (n: unknown) => { const v = Number(n || 0); return (v >= 0 ? '+' : '') + f2(v) }
const pCol = (n: unknown) => Number(n || 0) >= 0 ? 'var(--profit)' : 'var(--loss, #f43f5e)'
const dCls = (n: unknown) => { const v = Number(n || 0); return v > 0.05 ? 'text-profit' : v < -0.05 ? 'text-loss' : 'text-text-muted' }
const esc = (s: unknown) => String(s ?? '').replace(/[&<>"']/g, m => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m] || m))
const intFmt = new Intl.NumberFormat('en-IN', { maximumFractionDigits: 0 })
const dec2Fmt = new Intl.NumberFormat('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
const dec4Fmt = new Intl.NumberFormat('en-IN', { minimumFractionDigits: 4, maximumFractionDigits: 4 })

type SummaryFieldType = 'int' | 'pnl' | 'float2' | 'float4'

function fmtSummaryValue(val: unknown, type: SummaryFieldType): string {
  const num = Number(val || 0)
  if (!Number.isFinite(num)) return type === 'int' ? '0' : '0.00'
  if (type === 'int') return intFmt.format(num)
  if (type === 'float4') return dec4Fmt.format(num)
  if (type === 'float2') return dec2Fmt.format(num)
  return (num >= 0 ? '+' : '') + dec2Fmt.format(num)
}

function stableLegs(legs: any[]): any[] {
  return [...legs].sort((a: any, b: any) => {
    const strikeDiff = Number(a.strike ?? 0) - Number(b.strike ?? 0)
    if (strikeDiff !== 0) return strikeDiff
    const sideDiff = String(a.side ?? '').localeCompare(String(b.side ?? ''))
    if (sideDiff !== 0) return sideDiff
    return String(a.tag ?? '').localeCompare(String(b.tag ?? ''))
  })
}

function flashEl(el: Element) {
  el.classList.remove('val-flash')
  void (el as HTMLElement).offsetWidth
  el.classList.add('val-flash')
}

/** Update a data-field element's text; flash on change */
function upd(card: Element, field: string, text: string, color?: string) {
  const el = card.querySelector(`[data-field="${field}"]`) as HTMLElement | null
  if (!el) return
  if (el.textContent !== text) { el.textContent = text; flashEl(el) }
  if (color !== undefined && el.style.color !== color) el.style.color = color
}


function LiveMonitorPanel() {
  const containerRef = useRef<HTMLDivElement>(null)
  const summaryRef   = useRef<HTMLDivElement>(null)
  const groupsRef    = useRef<HTMLDivElement>(null)
  const [loading, setLoading]       = useState(true)
  const [autoRefresh, setAutoRefresh] = useState(true)
  const prevGroupNamesRef = useRef('')
  const legCacheRef       = useRef<Record<string, string>>({})
  const initializedRef    = useRef(false)
  const pollInFlightRef   = useRef(false)

  // ── Build monitor summary KPI grid ──
  function renderSummaryGrid(cols: [string, unknown, SummaryFieldType][]) {
    const container = summaryRef.current
    if (!container) return
    if (!container.querySelector('[data-mid]')) {
      // First render — build HTML
      container.innerHTML = cols.map(([label, val, type]) => `
        <div class="bg-bg-card border border-border rounded-lg px-3 py-2.5">
          <div class="text-[9px] text-text-muted uppercase tracking-wider font-semibold">${esc(label)}</div>
          <div class="text-[17px] font-bold font-mono tabular-nums whitespace-nowrap mt-1" data-mid="${esc(label)}"
               style="${type === 'pnl' ? `color:${pCol(val)}` : 'color:rgb(var(--c-text-bright))'}">${fmtSummaryValue(val, type)}</div>
        </div>`).join('')
      return
    }
    // Subsequent renders — update in-place
    cols.forEach(([label, val, type]) => {
      const el = container.querySelector(`[data-mid="${CSS.escape(label)}"]`) as HTMLElement | null
      if (!el) return
      const text = fmtSummaryValue(val, type)
      if (el.textContent !== text) { el.textContent = text; flashEl(el) }
      if (type === 'pnl') { const c = pCol(val); if (el.style.color !== c) el.style.color = c }
    })
  }

  // ── Build leg table HTML with per-cell data attributes for in-place updates ──
  function legTableHtml(legs: any[]): string {
    if (!legs.length) return '<div class="text-text-muted text-[12px] py-2 px-1">No legs.</div>'
    const orderedLegs = stableLegs(legs)
    return `
    <div class="overflow-x-auto">
      <table class="w-full text-[11px]" style="border-collapse:collapse;table-layout:fixed">
        <colgroup>
          <col style="width:100px"><!-- Tag -->
          <col style="width:48px"><!-- Side -->
          <col style="width:84px"><!-- Strike -->
          <col style="width:72px"><!-- Entry -->
          <col style="width:72px"><!-- LTP -->
          <col style="width:60px"><!-- Qty -->
          <col style="width:60px"><!-- Delta -->
          <col style="width:56px"><!-- Theta -->
          <col style="width:52px"><!-- IV -->
          <col style="width:80px"><!-- Unrealized -->
          <col style="width:72px"><!-- Realized -->
          <col style="width:80px"><!-- Total PnL -->
          <col style="width:72px"><!-- Status -->
        </colgroup>
        <thead>
          <tr class="text-[9px] text-text-muted uppercase tracking-wider" style="background:rgba(var(--c-bg-elevated),0.5)">
            <th class="px-2 py-2 text-left font-semibold">Tag</th>
            <th class="px-1 py-2 text-center font-semibold">Side</th>
            <th class="px-2 py-2 text-left font-semibold">Strike</th>
            <th class="px-2 py-2 text-right font-semibold">Entry</th>
            <th class="px-2 py-2 text-right font-semibold">LTP</th>
            <th class="px-2 py-2 text-right font-semibold">Qty</th>
            <th class="px-2 py-2 text-right font-semibold">Δ</th>
            <th class="px-2 py-2 text-right font-semibold">Θ</th>
            <th class="px-2 py-2 text-right font-semibold">IV</th>
            <th class="px-2 py-2 text-right font-semibold">Unrealized</th>
            <th class="px-2 py-2 text-right font-semibold">Realized</th>
            <th class="px-2 py-2 text-right font-semibold">Total PnL</th>
            <th class="px-2 py-2 text-center font-semibold">Status</th>
          </tr>
        </thead>
        <tbody>
          ${orderedLegs.map((p: any) => {
            const uPnl = Number(p.unrealized_pnl ?? p.urmtom ?? 0)
            const rPnl = Number(p.realized_pnl ?? p.rpnl ?? 0)
            const tPnl = uPnl + rPnl
            const isActive = p.is_active !== false
            const status = p.order_status ?? (isActive ? 'ACTIVE' : 'CLOSED')
            const statusCls = status === 'FAILED' ? 'text-loss bg-loss/10'
              : status === 'SIMULATED' ? 'text-yellow-400 bg-yellow-400/10'
              : isActive ? 'text-profit bg-profit/10' : 'text-text-muted bg-bg-elevated'
            const tag = esc(p.tag ?? '—')
            const orderQty = Number(p.order_qty ?? p.qty ?? p.netqty ?? 0)
            const lots = p.lots != null ? Number(p.lots) : null
            const lotSz = p.lot_size != null ? Number(p.lot_size) : null
            return `
            <tr data-tag="${tag}" style="border-top:1px solid rgba(var(--c-border),0.3);${!isActive ? 'opacity:0.5' : ''}">
              <td class="px-2 py-1.5 font-mono text-brand text-[10px] truncate" title="${tag}">${tag}</td>
              <td class="px-1 py-1.5 text-center">
                <span class="text-[9px] font-bold px-1.5 py-0.5 rounded ${p.side === 'BUY' ? 'text-profit bg-profit/10' : 'text-loss bg-loss/10'}">${esc(p.side ?? '')}</span>
              </td>
              <td class="px-2 py-1.5 font-mono text-text-bright text-[11px]">${p.strike ? `${Number(p.strike).toFixed(0)}${p.option_type || ''}` : p.instrument === 'FUT' ? 'FUT' : '—'}</td>
              <td class="px-2 py-1.5 text-right font-mono text-text-sec tabular-nums whitespace-nowrap">${f2(p.entry_price ?? p.avg_price ?? p.avgprc)}</td>
              <td class="px-2 py-1.5 text-right font-mono text-text-bright tabular-nums whitespace-nowrap" data-col="ltp">${f2(p.ltp)}</td>
              <td class="px-2 py-1.5 text-right font-mono text-text-bright tabular-nums whitespace-nowrap" data-col="qty">${Math.abs(orderQty)}${lots != null && lotSz != null ? `<div class="text-[8px] text-text-muted leading-tight">${lots}L×${lotSz}</div>` : ''}</td>
              <td class="px-2 py-1.5 text-right font-mono text-text-sec tabular-nums whitespace-nowrap" data-col="delta">${p.delta != null ? Number(p.delta).toFixed(3) : '—'}</td>
              <td class="px-2 py-1.5 text-right font-mono text-text-sec tabular-nums whitespace-nowrap" data-col="theta">${p.theta != null ? Number(p.theta).toFixed(2) : '—'}</td>
              <td class="px-2 py-1.5 text-right font-mono text-text-sec tabular-nums whitespace-nowrap" data-col="iv">${p.iv != null && Number(p.iv) > 0 ? Number(p.iv).toFixed(1) + '%' : '—'}</td>
              <td class="px-2 py-1.5 text-right font-mono tabular-nums whitespace-nowrap" data-col="upnl" style="color:${pCol(uPnl)}">${pSign(uPnl)}</td>
              <td class="px-2 py-1.5 text-right font-mono tabular-nums whitespace-nowrap" data-col="rpnl" style="color:${pCol(rPnl)}">${pSign(rPnl)}</td>
              <td class="px-2 py-1.5 text-right font-mono font-bold tabular-nums whitespace-nowrap" data-col="tpnl" style="color:${pCol(tPnl)}">${pSign(tPnl)}</td>
              <td class="px-2 py-1.5 text-center"><span class="text-[8px] px-1.5 py-0.5 rounded ${statusCls}" data-col="status">${esc(status)}</span></td>
            </tr>`
          }).join('')}
        </tbody>
      </table>
    </div>`
  }

  /** Update a single leg table section in-place (per-cell). Rebuild only if leg count changes. */
  function updateLegTableInPlace(container: Element, legs: any[], cacheKey: string) {
    const orderedLegs = stableLegs(legs)
    // Get current tags from DOM
    const existingTags: string[] = []
    container.querySelectorAll('tr[data-tag]').forEach(r => {
      const t = r.getAttribute('data-tag')
      if (t) existingTags.push(t)
    })
    const newTags = orderedLegs.map((p: any) => esc(p.tag ?? '—'))

    // If leg count or tag set changed → full rebuild
    if (existingTags.length !== newTags.length || existingTags.join(',') !== newTags.join(',')) {
      const html = legTableHtml(orderedLegs)
      legCacheRef.current[cacheKey] = html
      container.innerHTML = html
      return
    }

    // Same tags → update each cell in-place
    for (const p of orderedLegs) {
      const tagVal = esc(p.tag ?? '—')
      const row = container.querySelector(`tr[data-tag="${CSS.escape(tagVal)}"]`)
      if (!row) continue

      const uPnl = Number(p.unrealized_pnl ?? p.urmtom ?? 0)
      const rPnl = Number(p.realized_pnl ?? p.rpnl ?? 0)
      const tPnl = uPnl + rPnl

      // LTP
      const ltpEl = row.querySelector('[data-col="ltp"]') as HTMLElement | null
      if (ltpEl) {
        const ltpText = f2(p.ltp)
        if (ltpEl.textContent !== ltpText) { ltpEl.textContent = ltpText; flashEl(ltpEl) }
      }

      const qtyEl = row.querySelector('[data-col="qty"]') as HTMLElement | null
      if (qtyEl) {
        const orderQty = Number(p.order_qty ?? p.qty ?? p.netqty ?? 0)
        const lots = p.lots != null ? Number(p.lots) : null
        const lotSz = p.lot_size != null ? Number(p.lot_size) : null
        const qtyText = `${Math.abs(orderQty)}${lots != null && lotSz != null ? `\n${lots}L×${lotSz}` : ''}`
        const currentQtyText = qtyEl.innerText.trim()
        if (currentQtyText !== qtyText.trim()) {
          qtyEl.innerHTML = `${Math.abs(orderQty)}${lots != null && lotSz != null ? `<div class="text-[8px] text-text-muted leading-tight">${lots}L×${lotSz}</div>` : ''}`
        }
      }

      // Delta
      const deltaEl = row.querySelector('[data-col="delta"]') as HTMLElement | null
      if (deltaEl) {
        const dt = p.delta != null ? Number(p.delta).toFixed(3) : '—'
        if (deltaEl.textContent !== dt) deltaEl.textContent = dt
      }

      // Theta
      const thetaEl = row.querySelector('[data-col="theta"]') as HTMLElement | null
      if (thetaEl) {
        const tt = p.theta != null ? Number(p.theta).toFixed(2) : '—'
        if (thetaEl.textContent !== tt) thetaEl.textContent = tt
      }

      // IV
      const ivEl = row.querySelector('[data-col="iv"]') as HTMLElement | null
      if (ivEl) {
        const ivt = p.iv != null && Number(p.iv) > 0 ? Number(p.iv).toFixed(1) + '%' : '—'
        if (ivEl.textContent !== ivt) ivEl.textContent = ivt
      }

      // Unrealized PnL
      const upEl = row.querySelector('[data-col="upnl"]') as HTMLElement | null
      if (upEl) {
        const ut = pSign(uPnl)
        if (upEl.textContent !== ut) { upEl.textContent = ut; flashEl(upEl) }
        upEl.style.color = pCol(uPnl) as string
      }

      // Realized PnL
      const rpEl = row.querySelector('[data-col="rpnl"]') as HTMLElement | null
      if (rpEl) {
        const rt = pSign(rPnl)
        if (rpEl.textContent !== rt) { rpEl.textContent = rt; flashEl(rpEl) }
        rpEl.style.color = pCol(rPnl) as string
      }

      // Total PnL
      const tpEl = row.querySelector('[data-col="tpnl"]') as HTMLElement | null
      if (tpEl) {
        const ttpnl = pSign(tPnl)
        if (tpEl.textContent !== ttpnl) { tpEl.textContent = ttpnl; flashEl(tpEl) }
        tpEl.style.color = pCol(tPnl) as string
      }

      const stEl = row.querySelector('[data-col="status"]') as HTMLElement | null
      if (stEl) {
        const isActive = p.is_active !== false
        const status = p.order_status ?? (isActive ? 'ACTIVE' : 'CLOSED')
        const statusCls = status === 'FAILED' ? 'text-loss bg-loss/10'
          : status === 'SIMULATED' ? 'text-yellow-400 bg-yellow-400/10'
          : isActive ? 'text-profit bg-profit/10' : 'text-text-muted bg-bg-elevated'
        if (stEl.textContent !== status) stEl.textContent = status
        stEl.className = `text-[8px] px-1.5 py-0.5 rounded ${statusCls}`
      }
    }
  }

  // ── Build full strategy group card HTML ──
  function buildGroupHtml(name: string, mon: any, isPaper: boolean): string {
    const s = mon?.summary ?? {}
    const md = mon?.market_data ?? {}
    const allLegs: any[] = mon?.legs ?? []
    const activeLegs = stableLegs(allLegs.filter((l: any) => l.is_active !== false))
    const closedLegs = stableLegs(allLegs.filter((l: any) => l.is_active === false))
    const totalPnl = Number(s.total_pnl ?? (Number(s.combined_pnl ?? 0) + Number(s.realised_pnl ?? s.cumulative_daily_pnl ?? 0)))
    const unrealised = Number(s.unrealised_pnl ?? s.combined_pnl ?? 0)
    const realised = Number(s.realised_pnl ?? s.cumulative_daily_pnl ?? 0)
    const isRunning = mon?.status === 'running'
    const modeClass = isPaper ? 'strat-card-mock' : 'strat-card-live'
    const runClass = isRunning ? 'strat-card-running' : ''

    // Compute runtime from entry_time
    let runtimeStr = '—'
    if (s.entry_time) {
      const elapsed = Math.max(0, Math.floor((Date.now() - new Date(s.entry_time).getTime()) / 60000))
      const h = Math.floor(elapsed / 60)
      const m = elapsed % 60
      runtimeStr = h > 0 ? `${h}h ${m}m` : `${m}m`
    }

    const activeHtml = legTableHtml(activeLegs)
    const closedHtml = legTableHtml(closedLegs)
    legCacheRef.current[`${name}:active`] = activeHtml
    legCacheRef.current[`${name}:closed`] = closedHtml

    return `
    <div class="relative rounded-xl overflow-hidden border ${modeClass} ${runClass} strat-card-enter" data-strat="${esc(name)}" style="margin-bottom:12px">
      <div style="position:relative;z-index:2">
        <!-- Header -->
        <div class="px-4 py-3" style="border-bottom:1px solid rgba(255,255,255,0.06)">
          <div class="flex items-center gap-2 flex-wrap">
            <span class="w-2.5 h-2.5 rounded-full shrink-0 ${isRunning ? 'bg-profit live-dot-blink' : 'bg-text-muted/50'}" style="${isRunning ? 'box-shadow:0 0 8px rgba(34,197,94,0.6)' : ''}" data-field="sdot"></span>
            <span class="text-[13px] font-semibold text-text-bright font-mono">${esc(name)}</span>
            ${isPaper
              ? '<span class="inline-flex items-center gap-1 text-[9px] px-2 py-0.5 rounded-full font-bold border border-brand/40 bg-brand/15 text-brand tracking-wider">🧪 PAPER</span>'
              : '<span class="inline-flex items-center gap-1 text-[9px] px-2 py-0.5 rounded-full font-bold border border-loss/40 bg-loss/15 tracking-wider" style="color:#fda4af"><span class="w-1.5 h-1.5 rounded-full bg-current live-dot-blink"></span> LIVE</span>'}
            <span class="inline-flex items-center gap-1 text-[9px] px-2 py-0.5 rounded-full font-bold" style="${isRunning ? 'background:rgba(34,197,94,0.15);color:#22c55e;border:1px solid rgba(34,197,94,0.4)' : 'background:rgba(var(--c-bg-elevated),1);color:rgb(var(--c-text-muted));border:1px solid rgba(var(--c-border),1)'}" data-field="abadge">
              ${isRunning ? '<span class="w-1.5 h-1.5 rounded-full bg-current live-dot-blink"></span>' : ''}
              ${isRunning ? 'RUNNING' : 'STOPPED'}
            </span>
            <span class="text-[10px] text-text-muted ml-auto" data-field="entry-info">
              ${s.entered_today ? `Entered • ${s.active_legs ?? 0}/${s.total_legs ?? 0} legs active` : 'Waiting for entry'}
            </span>
            <span class="text-[14px] font-bold font-mono px-2.5 py-0.5 rounded-lg" style="color:${pCol(totalPnl)};border:1px solid ${totalPnl >= 0 ? 'rgba(34,197,94,0.3)' : 'rgba(244,63,94,0.3)'};background:${totalPnl >= 0 ? 'rgba(34,197,94,0.1)' : 'rgba(244,63,94,0.1)'}" data-field="pnlbadge">${pSign(totalPnl)}</span>
          </div>
        </div>

        <!-- Metrics grid -->
        <div class="grid grid-cols-4 sm:grid-cols-8 gap-0" style="border-bottom:1px solid rgba(255,255,255,0.06)">
          ${[
            ['Spot', md.spot_price != null ? Number(md.spot_price).toFixed(2) : '—', 'spot'],
            ['ATM', md.atm_strike != null ? Number(md.atm_strike).toFixed(0) : '—', 'atm'],
            ['Net Δ', s.net_delta != null ? Number(s.net_delta).toFixed(4) : '—', 'delta'],
            ['Θ', s.portfolio_theta != null ? Number(s.portfolio_theta).toFixed(2) : '—', 'theta'],
            ['PnL', pSign(totalPnl), 'pnl'],
            ['PnL%', `${(Number(s.combined_pnl_pct ?? 0)).toFixed(1)}%`, 'pnlpct'],
            ['Adj', String(s.adjustments_today ?? 0), 'adj'],
            ['Exit In', s.minutes_to_exit != null ? `${s.minutes_to_exit}m` : '—', 'exitin'],
          ].map(([label, val, field]) => `
            <div class="px-3 py-2 text-center" style="border-right:1px solid rgba(255,255,255,0.04)">
              <div class="text-[9px] text-text-muted uppercase tracking-wider">${label}</div>
                <div class="text-[12px] font-bold font-mono tabular-nums whitespace-nowrap mt-0.5 text-text-bright" data-field="${field}"
                   ${field === 'pnl' || field === 'pnlpct' ? `style="color:${pCol(totalPnl)}"` : ''}>${val}</div>
            </div>`).join('')}
        </div>

        <!-- Greeks row -->
        <div class="flex items-center gap-4 px-4 py-2 text-[11px] text-text-muted flex-wrap" style="border-bottom:1px solid rgba(255,255,255,0.06)">
          <span>Runtime: <strong class="text-text-bright font-mono" data-field="runtime">${runtimeStr}</strong></span>
          <span>Realized: <strong class="font-mono" data-field="realized" style="color:${pCol(realised)}">${f2(realised)}</strong></span>
          <span>Unrealized: <strong class="font-mono" data-field="unrealized" style="color:${pCol(unrealised)}">${f2(unrealised)}</strong></span>
          <span>Active: <strong class="text-text-bright font-mono" data-field="actlegs">${activeLegs.length}</strong></span>
          <span>Closed: <strong class="text-text-bright font-mono" data-field="clslegs">${closedLegs.length}</strong></span>
          <span>Adj(life): <strong class="text-text-bright font-mono" data-field="lifetimeadj">${s.lifetime_adjustments ?? 0}</strong></span>
          <span>Γ: <strong class="font-mono text-text-sec" data-field="gamma">${f2(s.portfolio_gamma)}</strong></span>
          <span>ν: <strong class="font-mono text-text-sec" data-field="vega">${f2(s.portfolio_vega)}</strong></span>
          ${s.trailing_stop_active ? `<span class="text-yellow-400">⚡ Trail SL: <strong class="font-mono">${f2(s.trailing_stop_level)}</strong></span>` : ''}
        </div>

        <!-- Active Legs -->
        <div class="px-4 pt-2">
          <div class="text-[9px] text-text-muted uppercase tracking-wider font-semibold mb-1.5 flex items-center gap-2">
            Active Legs
            <span style="flex:1;height:1px;background:rgba(var(--c-brand),0.15)"></span>
          </div>
          <div data-legs="active">${activeHtml}</div>
        </div>

        <!-- Closed Legs -->
        <div class="px-4 pt-2 pb-3">
          <div class="text-[9px] text-text-muted uppercase tracking-wider font-semibold mb-1.5 flex items-center gap-2">
            Closed Legs
            <span style="flex:1;height:1px;background:rgba(var(--c-border),0.3)"></span>
          </div>
          <div data-legs="closed">${closedHtml}</div>
        </div>
      </div>
    </div>`
  }

  // ── Update an existing strategy group card in-place ──
  function updateGroupInPlace(card: Element, name: string, mon: any) {
    const s = mon?.summary ?? {}
    const md = mon?.market_data ?? {}
    const allLegs: any[] = mon?.legs ?? []
    const activeLegs = stableLegs(allLegs.filter((l: any) => l.is_active !== false))
    const closedLegs = stableLegs(allLegs.filter((l: any) => l.is_active === false))
    const isRunning = mon?.status === 'running'
    const totalPnl = Number(s.total_pnl ?? (Number(s.combined_pnl ?? 0) + Number(s.realised_pnl ?? s.cumulative_daily_pnl ?? 0)))
    const unrealised = Number(s.unrealised_pnl ?? s.combined_pnl ?? 0)
    const realised = Number(s.realised_pnl ?? s.cumulative_daily_pnl ?? 0)

    // PnL badge — show total (unrealized + realized)
    const pb = card.querySelector('[data-field="pnlbadge"]') as HTMLElement | null
    if (pb) {
      const pt = pSign(totalPnl)
      if (pb.textContent !== pt) { pb.textContent = pt; flashEl(pb) }
      pb.style.color = pCol(totalPnl) as string
      pb.style.borderColor = totalPnl >= 0 ? 'rgba(34,197,94,0.3)' : 'rgba(244,63,94,0.3)'
      pb.style.background = totalPnl >= 0 ? 'rgba(34,197,94,0.1)' : 'rgba(244,63,94,0.1)'
    }

    // Status dot
    const sd = card.querySelector('[data-field="sdot"]') as HTMLElement | null
    if (sd) {
      sd.className = `w-2.5 h-2.5 rounded-full shrink-0 ${isRunning ? 'bg-profit live-dot-blink' : 'bg-text-muted/50'}`
      sd.style.boxShadow = isRunning ? '0 0 8px rgba(34,197,94,0.6)' : ''
    }

    const badge = card.querySelector('[data-field="abadge"]') as HTMLElement | null
    if (badge) {
      badge.innerHTML = `${isRunning ? '<span class="w-1.5 h-1.5 rounded-full bg-current live-dot-blink"></span>' : ''}${isRunning ? 'RUNNING' : 'STOPPED'}`
      badge.style.background = isRunning ? 'rgba(34,197,94,0.15)' : 'rgba(var(--c-bg-elevated),1)'
      badge.style.color = isRunning ? '#22c55e' : 'rgb(var(--c-text-muted))'
      badge.style.border = isRunning ? '1px solid rgba(34,197,94,0.4)' : '1px solid rgba(var(--c-border),1)'
    }

    // Entry info
    upd(card, 'entry-info', s.entered_today ? `Entered • ${s.active_legs ?? 0}/${s.total_legs ?? 0} legs active` : 'Waiting for entry')

    // Metrics
    upd(card, 'spot', md.spot_price != null ? Number(md.spot_price).toFixed(2) : '—')
    upd(card, 'atm', md.atm_strike != null ? Number(md.atm_strike).toFixed(0) : '—')
    upd(card, 'delta', s.net_delta != null ? Number(s.net_delta).toFixed(4) : '—')
    upd(card, 'theta', s.portfolio_theta != null ? Number(s.portfolio_theta).toFixed(2) : '—')
    upd(card, 'pnl', pSign(totalPnl), pCol(totalPnl) as string)
    upd(card, 'pnlpct', `${(Number(s.combined_pnl_pct ?? 0)).toFixed(1)}%`, pCol(totalPnl) as string)
    upd(card, 'adj', String(s.adjustments_today ?? 0))
    upd(card, 'exitin', s.minutes_to_exit != null ? `${s.minutes_to_exit}m` : '—')

    // Runtime computed from entry_time
    let runtimeStr = '—'
    if (s.entry_time) {
      const elapsed = Math.max(0, Math.floor((Date.now() - new Date(s.entry_time).getTime()) / 60000))
      const h = Math.floor(elapsed / 60)
      const m = elapsed % 60
      runtimeStr = h > 0 ? `${h}h ${m}m` : `${m}m`
    }
    upd(card, 'runtime', runtimeStr)

    // Realized / Unrealized PnL
    upd(card, 'realized', f2(realised), pCol(realised) as string)
    upd(card, 'unrealized', f2(unrealised), pCol(unrealised) as string)
    upd(card, 'actlegs', String(activeLegs.length))
    upd(card, 'clslegs', String(closedLegs.length))
    upd(card, 'lifetimeadj', String(s.lifetime_adjustments ?? 0))
    upd(card, 'gamma', f2(s.portfolio_gamma))
    upd(card, 'vega', f2(s.portfolio_vega))

    // Leg tables — per-cell in-place updates (no full table rebuild)
    const activeTbl = card.querySelector('[data-legs="active"]')
    if (activeTbl) updateLegTableInPlace(activeTbl, activeLegs, `${name}:active`)
    const closedTbl = card.querySelector('[data-legs="closed"]')
    if (closedTbl) updateLegTableInPlace(closedTbl, closedLegs, `${name}:closed`)
  }

  const load = useCallback(async () => {
    if (pollInFlightRef.current) return
    pollInFlightRef.current = true
    try {
      // 1) Get all running strategies
      const statuses: any[] = await api.strategyStatus()
      const running = (statuses || []).filter((s: any) => s.status === 'running')

      if (running.length === 0) {
        // No running strategies
        renderSummaryGrid([
          ['Running Strategies', 0, 'int'],
          ['Active Legs', 0, 'int'],
          ["Day's PnL", 0, 'pnl'],
        ])
        if (groupsRef.current && prevGroupNamesRef.current !== '') {
          prevGroupNamesRef.current = ''
          groupsRef.current.innerHTML = '<div class="text-text-muted text-[12px] p-4 text-center">No running strategies. Start a strategy to see live positions here.</div>'
        }
        return
      }

      // 2) Fetch monitor data for ALL running strategies in parallel
      const monResults = await Promise.allSettled(
        running.map((s: any) => api.strategyMonitor(s.name).then(mon => ({ name: s.name, mon, paper: s.paper_mode })))
      )
      const monitorData: { name: string; mon: any; paper: boolean }[] = []
      for (const r of monResults) {
        if (r.status === 'fulfilled') monitorData.push(r.value)
      }
      monitorData.sort((a, b) => a.name.localeCompare(b.name))

      // 3) Compute aggregate summary
      let totalPnl = 0, totalRealized = 0, totalUnrealized = 0
      let totalDelta = 0, totalTheta = 0, totalGamma = 0, totalVega = 0
      let totalActive = 0, totalClosed = 0
      for (const { mon } of monitorData) {
        const s = mon?.summary ?? {}
        const unrealised = Number(s.unrealised_pnl ?? s.combined_pnl ?? 0)
        const realised = Number(s.realised_pnl ?? s.cumulative_daily_pnl ?? 0)
        totalUnrealized += unrealised
        totalRealized += realised
        totalPnl += Number(s.total_pnl ?? (unrealised + realised))
        totalDelta += Number(s.net_delta ?? 0)
        totalTheta += Number(s.portfolio_theta ?? 0)
        totalGamma += Number(s.portfolio_gamma ?? 0)
        totalVega += Number(s.portfolio_vega ?? 0)
        totalActive += Number(s.active_legs ?? 0)
        totalClosed += (mon?.legs ?? []).filter((l: any) => l.is_active === false).length
      }

      // 4) Render summary KPI grid (in-place updates)
      renderSummaryGrid([
        ['Running', monitorData.length, 'int'],
        ['Active Legs', totalActive, 'int'],
        ['Closed Legs', totalClosed, 'int'],
        ["Day's PnL", totalPnl, 'pnl'],
        ['Unrealized', totalUnrealized, 'pnl'],
        ['Realized', totalRealized, 'pnl'],
        ['Net Delta', totalDelta, 'float4'],
        ['Theta', totalTheta, 'float2'],
      ])

      // 5) Render strategy group cards (Shoonya-style: build once, update in-place)
      const container = groupsRef.current
      if (!container) return
      const newGroupNames = monitorData.map(d => d.name).sort().join('\0')
      if (newGroupNames !== prevGroupNamesRef.current) {
        // Groups changed (strategy started/stopped) — rebuild all cards
        prevGroupNamesRef.current = newGroupNames
        legCacheRef.current = {}
        container.innerHTML = monitorData.map(d => buildGroupHtml(d.name, d.mon, d.paper)).join('')
      } else {
        // Same groups — update each card in-place (no DOM teardown!)
        const cardMap: Record<string, Element> = {}
        container.querySelectorAll('[data-strat]').forEach(c => {
          const attr = c.getAttribute('data-strat')
          if (attr) cardMap[attr] = c
        })
        for (const { name, mon } of monitorData) {
          const card = cardMap[name]
          if (card) updateGroupInPlace(card, name, mon)
        }
      }
    } catch (err) {
      // Network error — don't wipe the UI
      console.warn('Monitor poll error:', err)
    } finally {
      pollInFlightRef.current = false
      setLoading(false)
      initializedRef.current = true
    }
  }, [])

  useEffect(() => { load() }, [load])

  useEffect(() => {
    if (!autoRefresh) return
    const t = window.setInterval(load, POLL_MS)
    return () => window.clearInterval(t)
  }, [autoRefresh, load])

  return (
    <div className="space-y-4" ref={containerRef}>
      {/* Toolbar */}
      <div className="bg-bg-card border border-border rounded-xl px-4 py-3 flex flex-wrap items-center gap-3">
        <Wifi className="w-4 h-4 text-brand" />
        <span className="text-[13px] font-semibold text-text-bright">Live Position Monitor</span>
        <span className="inline-flex items-center gap-1 text-[10px] text-profit">
          <span className="w-1.5 h-1.5 rounded-full bg-profit live-dot-blink inline-block" style={{ boxShadow: '0 0 6px rgba(34,197,94,0.5)' }} /> LIVE
        </span>
        <div className="flex-1" />
        {autoRefresh && (
          <span className="text-[11px] text-text-muted flex items-center gap-1">
            <span className="w-1.5 h-1.5 rounded-full bg-brand animate-ping inline-block" /> Polling 1.0s
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
          <RefreshCw className={cn('w-4 h-4', loading && !initializedRef.current && 'animate-spin')} />
        </button>
      </div>

      {/* Loading state — only on first load */}
      {loading && !initializedRef.current ? (
        <div className="flex items-center justify-center h-32">
          <Loader2 className="w-6 h-6 animate-spin text-brand" />
        </div>
      ) : (
        <>
          {/* Summary KPI Grid */}
          <div ref={summaryRef} className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-8 gap-2.5" />

          {/* Strategy Group Cards */}
          <div ref={groupsRef}>
            <div className="text-text-muted text-[12px] p-4 text-center">Loading strategies…</div>
          </div>
        </>
      )}
    </div>
  )
}

/* ════════════════════════════════════════════════
   PnL Chart — recharts AreaChart for a run
   ════════════════════════════════════════════════ */
function PnlChart({ runId }: { runId: string }) {
  const [data, setData] = useState<any[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let c = false
    api.strategyRunPnl(runId).then(d => {
      if (!c) setData((d || []).map((p: any) => ({
        time: new Date(p.created_at).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' }),
        pnl: Number(p.pnl || 0),
        spot: Number(p.spot_price || 0),
      })))
    }).catch(() => {}).finally(() => { if (!c) setLoading(false) })
    return () => { c = true }
  }, [runId])

  if (loading) return <div className="flex items-center justify-center h-32"><Loader2 className="w-4 h-4 animate-spin text-brand" /></div>
  if (data.length === 0) return <div className="text-[11px] text-text-muted text-center py-6">No PnL data recorded yet for this run.</div>

  const minPnl = Math.min(...data.map(d => d.pnl))
  const maxPnl = Math.max(...data.map(d => d.pnl))

  return (
    <div className="w-full h-48">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
          <defs>
            <linearGradient id="pnlGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="rgb(var(--c-brand))" stopOpacity={0.3} />
              <stop offset="95%" stopColor="rgb(var(--c-brand))" stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
          <XAxis dataKey="time" tick={{ fontSize: 9, fill: 'rgb(var(--c-text-muted))' }} interval="preserveStartEnd" />
          <YAxis
            tick={{ fontSize: 9, fill: 'rgb(var(--c-text-muted))' }}
            domain={[Math.floor(minPnl * 1.1), Math.ceil(maxPnl * 1.1)]}
            tickFormatter={v => `₹${v}`}
          />
          <RTooltip
            contentStyle={{ background: 'rgb(var(--c-bg-card))', border: '1px solid rgb(var(--c-border))', borderRadius: '8px', fontSize: 11 }}
            formatter={(v: any) => [`₹${Number(v).toFixed(2)}`, 'PnL']}
          />
          <ReferenceLine y={0} stroke="rgba(255,255,255,0.15)" strokeDasharray="3 3" />
          <Area
            type="monotone"
            dataKey="pnl"
            stroke="rgb(var(--c-brand))"
            fill="url(#pnlGrad)"
            strokeWidth={2}
            dot={false}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  )
}

/* ════════════════════════════════════════════════
   Event Timeline — decision audit trail
   ════════════════════════════════════════════════ */
function EventTimeline({ runId }: { runId: string }) {
  const [events, setEvents] = useState<any[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let c = false
    api.strategyRunEvents(runId).then(d => { if (!c) setEvents(d || []) }).catch(() => {}).finally(() => { if (!c) setLoading(false) })
    return () => { c = true }
  }, [runId])

  if (loading) return <div className="flex items-center justify-center h-20"><Loader2 className="w-4 h-4 animate-spin text-brand" /></div>
  if (events.length === 0) return <div className="text-[11px] text-text-muted text-center py-4">No events recorded for this run.</div>

  const eventColors: Record<string, string> = {
    ENTRY: 'text-profit bg-profit/15 border-profit/30',
    EXIT: 'text-loss bg-loss/15 border-loss/30',
    ADJUSTMENT: 'text-yellow-400 bg-yellow-400/15 border-yellow-400/30',
    TRAIL_ACTIVATE: 'text-cyan-400 bg-cyan-400/15 border-cyan-400/30',
    PROFIT_STEP: 'text-brand bg-brand/15 border-brand/30',
    SL_HIT: 'text-loss bg-loss/15 border-loss/30',
  }
  const eventIcons: Record<string, typeof ArrowUpRight> = {
    ENTRY: ArrowUpRight,
    EXIT: ArrowDownRight,
    ADJUSTMENT: Repeat,
    TRAIL_ACTIVATE: Shield,
    PROFIT_STEP: Target,
    SL_HIT: AlertCircle,
  }

  return (
    <div className="space-y-1.5 max-h-72 overflow-y-auto">
      {events.map((ev, i) => {
        const cls = eventColors[ev.event_type] || 'text-text-muted bg-bg-elevated border-border'
        const Icon = eventIcons[ev.event_type] || Info
        const time = ev.created_at ? new Date(ev.created_at).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '—'
        return (
          <div key={i} className={cn('flex items-start gap-2 px-3 py-2 rounded-lg border', cls.split(' ').slice(1).join(' '))}>
            <Icon className={cn('w-3.5 h-3.5 shrink-0 mt-0.5', cls.split(' ')[0])} />
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2">
                <span className={cn('text-[10px] font-bold uppercase', cls.split(' ')[0])}>{ev.event_type}</span>
                <span className="text-[9px] text-text-muted">{time}</span>
                {ev.leg_tag && <span className="text-[9px] font-mono text-brand">{ev.leg_tag}</span>}
                <span className="ml-auto text-[10px] font-mono" style={{ color: Number(ev.pnl_at_event) >= 0 ? 'var(--profit)' : 'var(--loss, #f43f5e)' }}>
                  ₹{Number(ev.pnl_at_event || 0).toFixed(0)}
                </span>
              </div>
              {ev.reason && <div className="text-[10px] text-text-sec mt-0.5">{ev.reason}</div>}
              {ev.spot_at_event > 0 && <span className="text-[9px] text-text-muted">Spot: {Number(ev.spot_at_event).toFixed(0)}</span>}
            </div>
          </div>
        )
      })}
    </div>
  )
}

/* ════════════════════════════════════════════════
   Run Detail Modal — legs + events + PnL chart
   ════════════════════════════════════════════════ */
function RunDetailPanel({ runId, onClose }: { runId: string; onClose: () => void }) {
  const [run, setRun] = useState<any>(null)
  const [loading, setLoading] = useState(true)
  const [detailTab, setDetailTab] = useState<'legs' | 'events' | 'chart'>('chart')

  useEffect(() => {
    let c = false
    api.strategyRunDetail(runId).then(d => { if (!c) setRun(d) }).catch(() => {}).finally(() => { if (!c) setLoading(false) })
    return () => { c = true }
  }, [runId])

  if (loading) return <div className="flex items-center justify-center h-40"><Loader2 className="w-5 h-5 animate-spin text-brand" /></div>
  if (!run) return <div className="text-text-muted text-[12px] text-center py-4">Run not found.</div>

  const pnl = Number(run.cumulative_daily_pnl || 0)
  const legs = run.legs || []
  const activeLegs = legs.filter((l: any) => l.is_active)
  const closedLegs = legs.filter((l: any) => !l.is_active)
  const statusCls = run.status === 'RUNNING' ? 'text-profit bg-profit/15' : run.status === 'ERROR' ? 'text-loss bg-loss/15' : 'text-text-muted bg-bg-elevated'

  return (
    <div className="bg-bg-card border border-border rounded-xl overflow-hidden">
      {/* Header */}
      <div className="px-4 py-3 border-b border-border flex items-center gap-2 flex-wrap">
        <span className="text-[13px] font-semibold text-text-bright font-mono">{run.strategy_name}</span>
        <span className={cn('text-[9px] px-2 py-0.5 rounded-full font-bold border', statusCls)}>{run.status}</span>
        <span className="text-[10px] text-text-muted">{run.symbol}/{run.exchange}</span>
        {run.paper_mode && <span className="text-[9px] text-brand font-bold">🧪 PAPER</span>}
        <div className="flex-1" />
        <span className="text-[13px] font-bold font-mono" style={{ color: pnl >= 0 ? 'var(--profit)' : 'var(--loss, #f43f5e)' }}>
          {pnl >= 0 ? '+' : ''}₹{pnl.toFixed(2)}
        </span>
        <button onClick={onClose} className="p-1 rounded hover:bg-bg-hover text-text-muted hover:text-text-bright transition-colors">
          <X className="w-4 h-4" />
        </button>
      </div>

      {/* Summary row */}
      <div className="px-4 py-2 text-[10px] text-text-muted flex items-center gap-4 border-b border-border flex-wrap">
        <span>Started: <strong className="text-text-bright">{run.started_at ? new Date(run.started_at).toLocaleString('en-IN') : '—'}</strong></span>
        <span>Stopped: <strong className="text-text-bright">{run.stopped_at ? new Date(run.stopped_at).toLocaleString('en-IN') : '—'}</strong></span>
        <span>Peak PnL: <strong className="text-profit font-mono">₹{Number(run.peak_pnl || 0).toFixed(0)}</strong></span>
        <span>Adj: <strong className="text-text-bright font-mono">{run.adjustments_today || 0}</strong></span>
        <span>Trades: <strong className="text-text-bright font-mono">{run.total_trades_today || 0}</strong></span>
        <span>Legs: <strong className="text-text-bright font-mono">{legs.length}</strong></span>
        {run.entry_reason && <span>Entry: <strong className="text-profit">{run.entry_reason}</strong></span>}
        {run.exit_reason && <span>Exit: <strong className="text-loss">{run.exit_reason}</strong></span>}
      </div>

      {/* Detail tabs */}
      <div className="flex items-center gap-1 px-4 py-2 border-b border-border">
        {([['chart', 'PnL Chart'], ['events', 'Event Log'], ['legs', 'Legs']] as const).map(([id, label]) => (
          <button
            key={id}
            onClick={() => setDetailTab(id)}
            className={cn(
              'px-2.5 py-1 rounded text-[10px] font-semibold transition-colors',
              detailTab === id ? 'bg-brand/20 text-brand' : 'text-text-muted hover:text-text-sec'
            )}
          >
            {label}
          </button>
        ))}
      </div>

      {/* Content */}
      <div className="px-4 py-3">
        {detailTab === 'chart' && <PnlChart runId={runId} />}
        {detailTab === 'events' && <EventTimeline runId={runId} />}
        {detailTab === 'legs' && (
          <div className="space-y-3">
            {activeLegs.length > 0 && (
              <div>
                <div className="text-[9px] text-text-muted uppercase tracking-wider font-semibold mb-1">Active Legs ({activeLegs.length})</div>
                <LegTable legs={activeLegs} />
              </div>
            )}
            {closedLegs.length > 0 && (
              <div>
                <div className="text-[9px] text-text-muted uppercase tracking-wider font-semibold mb-1">Closed Legs ({closedLegs.length})</div>
                <LegTable legs={closedLegs} />
              </div>
            )}
            {legs.length === 0 && <div className="text-[11px] text-text-muted text-center py-4">No legs recorded.</div>}
          </div>
        )}
      </div>
    </div>
  )
}

/** Compact leg table for run detail */
function LegTable({ legs }: { legs: any[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-[10px]">
        <thead>
          <tr className="text-[8px] text-text-muted uppercase tracking-wider bg-bg-elevated/50">
            <th className="px-2 py-1.5 text-left">Tag</th>
            <th className="px-1 py-1.5 text-center">Side</th>
            <th className="px-2 py-1.5 text-left">Strike</th>
            <th className="px-2 py-1.5 text-right">Entry</th>
            <th className="px-2 py-1.5 text-right">Exit</th>
            <th className="px-2 py-1.5 text-right">LTP</th>
            <th className="px-2 py-1.5 text-right">Qty</th>
            <th className="px-2 py-1.5 text-right">PnL</th>
            <th className="px-2 py-1.5 text-left">Reason</th>
          </tr>
        </thead>
        <tbody>
          {legs.map((leg: any, i: number) => {
            const entry = Number(leg.entry_price || 0)
            const exit = leg.exit_price != null ? Number(leg.exit_price) : null
            const ltp = Number(leg.ltp || 0)
            const price = exit ?? ltp
            const side = leg.side === 'BUY' ? 1 : -1
            const pnl = (price - entry) * side * Number(leg.qty || 1)
            const isActive = leg.is_active !== false
            return (
              <tr key={i} className={cn('border-t border-border/30', !isActive && 'opacity-60')}>
                <td className="px-2 py-1.5 font-mono text-brand">{leg.tag}</td>
                <td className="px-1 py-1.5 text-center">
                  <span className={cn('text-[9px] font-bold px-1 py-0.5 rounded', leg.side === 'BUY' ? 'text-profit bg-profit/10' : 'text-loss bg-loss/10')}>{leg.side}</span>
                </td>
                <td className="px-2 py-1.5 font-mono text-text-bright">{leg.strike ? `${Number(leg.strike).toFixed(0)}${leg.option_type || ''}` : '—'}</td>
                <td className="px-2 py-1.5 text-right font-mono text-text-sec">{entry.toFixed(2)}</td>
                <td className="px-2 py-1.5 text-right font-mono text-text-sec">{exit != null ? exit.toFixed(2) : '—'}</td>
                <td className="px-2 py-1.5 text-right font-mono text-text-bright">{ltp.toFixed(2)}</td>
                <td className="px-2 py-1.5 text-right font-mono text-text-bright">{leg.qty}</td>
                <td className="px-2 py-1.5 text-right font-mono font-bold" style={{ color: pnl >= 0 ? 'var(--profit)' : 'var(--loss, #f43f5e)' }}>
                  {pnl >= 0 ? '+' : ''}₹{pnl.toFixed(0)}
                </td>
                <td className="px-2 py-1.5 text-text-sec max-w-[120px] truncate" title={leg.exit_reason || leg.entry_reason || ''}>
                  {leg.exit_reason || leg.entry_reason || '—'}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

/* ════════════════════════════════════════════════
   History Panel — run history with detail/playback
   ════════════════════════════════════════════════ */
function HistoryPanel() {
  const [runs, setRuns] = useState<any[]>([])
  const [loading, setLoading] = useState(true)
  const [selectedRun, setSelectedRun] = useState<string | null>(null)
  const [filterStrategy, setFilterStrategy] = useState('')
  const [filterStatus, setFilterStatus] = useState<string>('')

  const load = useCallback(() => {
    setLoading(true)
    const params: any = { limit: 100 }
    if (filterStrategy) params.strategy_name = filterStrategy
    if (filterStatus) params.status = filterStatus
    api.strategyRuns(params).then(d => setRuns(d || [])).catch(() => {}).finally(() => setLoading(false))
  }, [filterStrategy, filterStatus])

  useEffect(() => { load() }, [load])

  // Get unique strategy names for filter
  const strategyNames = useMemo(() => {
    const names = new Set(runs.map(r => r.strategy_name))
    return Array.from(names).sort()
  }, [runs])

  // Aggregate stats
  const totalPnl = runs.reduce((s, r) => s + Number(r.cumulative_daily_pnl || 0), 0)
  const winners = runs.filter(r => Number(r.cumulative_daily_pnl || 0) > 0).length
  const losers = runs.filter(r => Number(r.cumulative_daily_pnl || 0) < 0).length

  return (
    <div className="space-y-3">
      {/* Toolbar */}
      <div className="bg-bg-card border border-border rounded-xl px-4 py-3">
        <div className="flex items-center gap-3 flex-wrap">
          <History className="w-4 h-4 text-brand" />
          <span className="text-[13px] font-semibold text-text-bright">Run History</span>
          <div className="flex-1" />

          {/* Filters */}
          <select
            value={filterStrategy}
            onChange={e => setFilterStrategy(e.target.value)}
            className="bg-bg-elevated border border-border rounded-lg px-2 py-1 text-[10px] text-text-bright outline-none"
          >
            <option value="">All Strategies</option>
            {strategyNames.map(n => <option key={n} value={n}>{n}</option>)}
          </select>
          <select
            value={filterStatus}
            onChange={e => setFilterStatus(e.target.value)}
            className="bg-bg-elevated border border-border rounded-lg px-2 py-1 text-[10px] text-text-bright outline-none"
          >
            <option value="">All Status</option>
            <option value="STOPPED">Stopped</option>
            <option value="ERROR">Error</option>
            <option value="RUNNING">Running</option>
          </select>
          <button onClick={load} className="text-text-muted hover:text-text-bright p-1.5 rounded-lg hover:bg-bg-hover transition-colors">
            <RefreshCw className={cn('w-3.5 h-3.5', loading && 'animate-spin')} />
          </button>
        </div>

        {/* Aggregate KPIs */}
        <div className="grid grid-cols-4 gap-3 mt-3">
          <div className="text-center">
            <div className="text-[9px] text-text-muted uppercase">Total Runs</div>
            <div className="text-[16px] font-bold font-mono text-text-bright">{runs.length}</div>
          </div>
          <div className="text-center">
            <div className="text-[9px] text-text-muted uppercase">Total PnL</div>
            <div className="text-[16px] font-bold font-mono" style={{ color: totalPnl >= 0 ? 'var(--profit)' : 'var(--loss, #f43f5e)' }}>
              {totalPnl >= 0 ? '+' : ''}₹{totalPnl.toFixed(0)}
            </div>
          </div>
          <div className="text-center">
            <div className="text-[9px] text-text-muted uppercase">Winners</div>
            <div className="text-[16px] font-bold font-mono text-profit">{winners}</div>
          </div>
          <div className="text-center">
            <div className="text-[9px] text-text-muted uppercase">Losers</div>
            <div className="text-[16px] font-bold font-mono text-loss">{losers}</div>
          </div>
        </div>
      </div>

      {/* Selected run detail */}
      {selectedRun && (
        <RunDetailPanel runId={selectedRun} onClose={() => setSelectedRun(null)} />
      )}

      {/* Run list */}
      {loading ? (
        <div className="flex items-center justify-center h-32"><Loader2 className="w-5 h-5 animate-spin text-brand" /></div>
      ) : runs.length === 0 ? (
        <div className="card flex flex-col items-center justify-center h-40 text-text-muted text-sm gap-2">
          <History className="w-6 h-6" />
          No strategy runs found. Run a strategy to start building history.
        </div>
      ) : (
        <div className="space-y-1.5">
          {runs.map(r => {
            const pnl = Number(r.cumulative_daily_pnl || 0)
            const isSelected = selectedRun === r.run_id
            const statusCls = r.status === 'RUNNING' ? 'text-profit' : r.status === 'ERROR' ? 'text-loss' : 'text-text-muted'
            const duration = r.started_at && r.stopped_at
              ? (() => {
                  const ms = new Date(r.stopped_at).getTime() - new Date(r.started_at).getTime()
                  const mins = Math.floor(ms / 60000)
                  const h = Math.floor(mins / 60)
                  const m = mins % 60
                  return h > 0 ? `${h}h ${m}m` : `${m}m`
                })()
              : r.status === 'RUNNING' ? 'Active' : '—'

            return (
              <div
                key={r.run_id}
                onClick={() => setSelectedRun(isSelected ? null : r.run_id)}
                className={cn(
                  'bg-bg-card border rounded-xl px-4 py-2.5 cursor-pointer transition-all hover:border-brand/30',
                  isSelected ? 'border-brand/50 ring-1 ring-brand/20' : 'border-border'
                )}
              >
                <div className="flex items-center gap-2 flex-wrap">
                  {/* Status dot */}
                  <span className={cn('w-2 h-2 rounded-full shrink-0',
                    r.status === 'RUNNING' ? 'bg-profit live-dot-blink' : r.status === 'ERROR' ? 'bg-loss' : 'bg-text-muted/40'
                  )} />

                  {/* Strategy name */}
                  <span className="text-[12px] font-semibold text-text-bright capitalize">{(r.strategy_name || '').replace(/_/g, ' ')}</span>

                  {/* Symbol */}
                  <span className="text-[10px] font-mono text-brand font-bold">{r.symbol}/{r.exchange}</span>

                  {/* Status */}
                  <span className={cn('text-[9px] font-bold uppercase', statusCls)}>{r.status}</span>

                  {/* Paper badge */}
                  {r.paper_mode && <span className="text-[8px] text-brand font-bold">PAPER</span>}

                  {/* Meta */}
                  <div className="flex items-center gap-3 ml-auto text-[10px] text-text-muted shrink-0">
                    <span>Legs: <strong className="text-text-bright">{r.total_legs || 0}</strong></span>
                    <span>Events: <strong className="text-text-bright">{r.event_count || 0}</strong></span>
                    <span>{duration}</span>
                    <span className="text-[10px] font-mono">{r.started_at ? new Date(r.started_at).toLocaleDateString('en-IN') : '—'}</span>
                  </div>

                  {/* PnL */}
                  <span className="text-[12px] font-bold font-mono shrink-0" style={{ color: pnl >= 0 ? 'var(--profit)' : 'var(--loss, #f43f5e)' }}>
                    {pnl >= 0 ? '+' : ''}₹{pnl.toFixed(0)}
                  </span>

                  <ChevronRight className={cn('w-3.5 h-3.5 text-text-muted transition-transform shrink-0', isSelected && 'rotate-90')} />
                </div>
                {r.exit_reason && (
                  <div className="mt-1 text-[9px] text-text-muted">Exit: <span className="text-text-sec">{r.exit_reason}</span></div>
                )}
              </div>
            )
          })}
        </div>
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
  const [expanded, setExpanded] = useState<string | null>(null)

  // ── Real strategies from backend ───────────────────
  const [savedStrategies, setSavedStrategies] = useState<any[]>([])
  const [savedLoading, setSavedLoading] = useState(false)

  // ── Shared broker + symbol data (loaded once) ──────
  const [brokers, setBrokers] = useState<any[]>([])
  const [symbols, setSymbols] = useState<any[]>([])
  const [loadingBS, setLoadingBS] = useState(true)

  const loadSaved = useCallback(async () => {
    setSavedLoading(true)
    try {
      const data = await api.strategyConfigs()
      setSavedStrategies(data || [])
    } catch { /* backend may not have any strategies yet */ } finally {
      setSavedLoading(false)
    }
  }, [])

  useEffect(() => { loadSaved() }, [loadSaved])

  // Load brokers + symbols once
  useEffect(() => {
    let c = false
    Promise.allSettled([
      api.strategyBrokers(),
      api.availableSymbols(),
    ]).then(([br, sy]) => {
      if (c) return
      if (br.status === 'fulfilled') setBrokers(br.value || [])
      if (sy.status === 'fulfilled') setSymbols(sy.value || [])
    }).finally(() => { if (!c) setLoadingBS(false) })
    return () => { c = true }
  }, [])

  async function handleRun(name: string, overrides?: any) {
    try {
      const res = await api.runStrategy(name, overrides) as any
      const warns = res?.warnings?.length ? ` (${res.warnings.length} warnings)` : ''
      toast(`Strategy "${name}" started${warns}`, 'success')
      setExpanded(null)
      loadSaved()
    } catch (e: any) {
      let msg = 'Failed to start strategy'
      try {
        const parsed = JSON.parse(e?.message || '{}')
        msg = parsed.detail || msg
      } catch { msg = e?.message || msg }
      toast(msg, 'error')
      loadSaved()
      throw e
    }
  }

  async function handleStop(name: string) {
    try {
      await api.stopStrategy(name)
      toast(`Strategy "${name}" stopped`, 'warning')
      loadSaved()
    } catch (e: any) { toast(e?.message || 'Failed to stop strategy', 'error') }
  }

  async function handleDelete(name: string) {
    if (!confirm(`Delete strategy "${name}"?`)) return
    try {
      await api.deleteStrategyConfig(name)
      toast(`Strategy "${name}" deleted`, 'info')
      loadSaved()
    } catch (e: any) { toast(e?.message || 'Failed to delete strategy', 'error') }
  }

  const filtered = savedStrategies.filter((s: any) => {
    if (tab === 'running') return s.status === 'running'
    return true
  })

  const runningCount = savedStrategies.filter(s => s.status === 'running').length
  const liveCount    = savedStrategies.filter(s => !s.paper_mode && s.status === 'running').length

  const TABS: { id: PageTab; label: string; badge?: number; badgeLive?: boolean }[] = [
    { id: 'all',     label: 'All Strategies' },
    { id: 'running', label: 'Running', badge: runningCount, badgeLive: true },
    { id: 'monitor', label: 'Live Monitor', badge: runningCount, badgeLive: true },
    { id: 'history', label: 'History' },
  ]

  return (
    <div className="h-full overflow-y-auto">
      <div className="p-4 space-y-3 min-h-full">

        {/* Header */}
        <div className="flex items-center gap-3 flex-wrap">
          <div className="flex items-center gap-2">
            <GitBranch className="w-4 h-4 text-brand" />
            <span className="text-[13px] font-semibold text-text-bright">Strategies</span>
          </div>
          {runningCount > 0 && (
            <div className="flex items-center gap-1.5 px-2.5 py-0.5 bg-black/30 border border-white/[0.08] rounded-full text-[10px] font-mono font-semibold">
              <span className="w-2 h-2 rounded-full bg-profit live-dot-blink shadow-[0_0_6px_rgba(34,197,94,0.6)]" />
              <span className="text-profit">{runningCount} Running</span>
              {liveCount > 0 && <span className="text-[#fda4af]">({liveCount} Live)</span>}
            </div>
          )}
          <div className="flex-1" />
          <button onClick={loadSaved} className="btn-ghost btn-sm" title="Refresh">
            <RefreshCw className={cn('w-3.5 h-3.5', savedLoading && 'animate-spin')} />
          </button>
          <button onClick={() => navigate('/app/strategy-builder')} className="btn-primary btn-sm">
            <Cpu className="w-3.5 h-3.5" /> Builder
          </button>
        </div>

        {/* Summary KPIs — hide on monitor/history tab */}
        {tab !== 'monitor' && tab !== 'history' && <SummaryKPIs strategies={savedStrategies} />}

        {/* Tab bar */}
        <div className="flex items-center gap-1 bg-bg-surface border border-border rounded-lg p-1 w-fit">
          {TABS.map(t => (
            <button
              key={t.id}
              onClick={() => { setTab(t.id); setExpanded(null) }}
              className={cn(
                'px-3 py-1.5 rounded text-[11px] font-medium whitespace-nowrap transition-colors flex items-center gap-1.5',
                tab === t.id
                  ? t.id === 'monitor' ? 'bg-profit text-white' : 'bg-brand text-bg-base'
                  : 'text-text-sec hover:text-text-bright'
              )}
            >
              {t.id === 'monitor' && <span className={cn('w-1.5 h-1.5 rounded-full inline-block', tab === t.id ? 'bg-white live-dot-blink' : 'bg-profit live-dot-blink')} />}
              {t.label}
              {t.badge != null && t.badge > 0 && (
                <span className={cn('text-[9px] font-bold px-1.5 py-0 rounded-full ml-0.5',
                  tab === t.id ? 'bg-white/20 text-white' : t.badgeLive ? 'bg-profit/15 text-profit' : 'bg-brand/15 text-brand'
                )}>{t.badge}</span>
              )}
            </button>
          ))}
        </div>

        {/* ── Live Monitor tab ── */}
        {tab === 'monitor' && <LiveMonitorPanel />}

        {/* ── History tab ── */}
        {tab === 'history' && <HistoryPanel />}

        {/* ── Strategy list (non-monitor, non-history tabs) ── */}
        {tab !== 'monitor' && tab !== 'history' && (
          <>
            {savedLoading && savedStrategies.length === 0 ? (
              <div className="flex items-center justify-center h-40">
                <Loader2 className="w-6 h-6 animate-spin text-brand" />
              </div>
            ) : (
              <div className="space-y-2">
                {filtered.map(s => (
                  <StrategyCard
                    key={s.name}
                    s={s}
                    expanded={expanded === s.name}
                    onToggle={() => setExpanded(expanded === s.name ? null : s.name)}
                    onRun={handleRun}
                    onStop={handleStop}
                    onEdit={name => navigate(`/app/strategy-builder?name=${encodeURIComponent(name)}`)}
                    onDelete={handleDelete}
                    brokers={brokers}
                    symbols={symbols}
                    loadingBS={loadingBS}
                  />
                ))}
                {filtered.length === 0 && (
                  <div className="card flex flex-col items-center justify-center h-40 text-text-muted text-sm gap-2">
                    <GitBranch className="w-6 h-6" />
                    {tab === 'running' ? 'No running strategies.' : 'No strategies found. Use the Builder to create one.'}
                  </div>
                )}
              </div>
            )}
          </>
        )}
      </div>
    </div>
  )
}

