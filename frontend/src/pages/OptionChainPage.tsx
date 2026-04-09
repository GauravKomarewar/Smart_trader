/* ════════════════════════════════════════════
   Option Chain Page
   Full chain + analytics + basket order
   ════════════════════════════════════════════ */
import { useState, useEffect, useRef, useMemo, useCallback } from 'react'
import { useOptionChain } from '../hooks'
import { useOptionChainStore, useUIStore, useToastStore } from '../stores'
import { cn, fmtNum, fmtOI, fmtINR, ivColor } from '../lib/utils'
import { api } from '../lib/api'
import { uid } from '../lib/utils'
import { marketWs, type MarketTick } from '../lib/ws'
import {
  Layers, ShoppingCart, BarChart2, RefreshCw, Plus, Minus,
  Trash2, CheckCircle2, AlertCircle, Settings2, ChevronDown,
} from 'lucide-react'
import type { BasketLeg, TransactionType, OrderType } from '../types'

// Underlying → F&O exchange mapping
const UNDERLYING_MAP: Record<string, string> = {
  NIFTY: 'NFO', BANKNIFTY: 'NFO', FINNIFTY: 'NFO', MIDCPNIFTY: 'NFO',
  SENSEX: 'BFO', BANKEX: 'BFO',
  CRUDEOIL: 'MCX', GOLD: 'MCX', SILVER: 'MCX', NATURALGAS: 'MCX', COPPER: 'MCX',
  USDINR: 'CDS',
}
const UNDERLYINGS = Object.keys(UNDERLYING_MAP)

type OCTab = 'chain' | 'analytics' | 'basket'

// ── Column definitions ─────────────────────────────
interface ColDef { id: string; label: string; symbol?: string; defaultOn: boolean }
const COLUMN_DEFS: ColDef[] = [
  { id: 'oiChange', label: 'OI Chg',   defaultOn: true  },
  { id: 'oi',       label: 'OI',        defaultOn: true  },
  { id: 'volume',   label: 'Volume',    defaultOn: true  },
  { id: 'iv',       label: 'IV',        defaultOn: true  },
  { id: 'delta',    label: 'Δ Delta',   symbol: 'Δ', defaultOn: true  },
  { id: 'gamma',    label: 'Γ Gamma',   symbol: 'Γ', defaultOn: false },
  { id: 'theta',    label: 'Θ Theta',   symbol: 'Θ', defaultOn: false },
  { id: 'vega',     label: 'ν Vega',    symbol: 'ν', defaultOn: false },
]
const COL_STORAGE_KEY = 'oc_visible_cols'

export default function OptionChainPage() {
  useOptionChain()
  const { data, selectedUnderlying, selectedExpiry, setUnderlying, setExpiry, isLoading } = useOptionChainStore()
  const [tab, setTab] = useState<OCTab>('chain')

  return (
    <div className="h-full overflow-y-auto">
      <div className="p-4 space-y-3">
        {/* Controls row */}
        <div className="flex items-center gap-3 flex-wrap">
          <div className="flex items-center gap-2">
            <Layers className="w-4 h-4 text-brand" />
            <span className="text-[13px] font-semibold text-text-bright">Option Chain</span>
          </div>

          {/* Underlying selector */}
          <select
            value={selectedUnderlying}
            onChange={e => setUnderlying(e.target.value)}
            className="select-base w-36 text-[12px] py-1.5"
          >
            {UNDERLYINGS.map(u => <option key={u} value={u}>{u}</option>)}
          </select>

          {/* Expiry selector */}
          {data && data.expiries && data.expiries.length > 0 && (
            <select
              value={selectedExpiry || data.expiry}
              onChange={e => setExpiry(e.target.value)}
              className="select-base w-44 text-[12px] py-1.5"
            >
              {data.expiries.map(e => {
                // Format ISO date "2026-04-07" → "07 Apr 2026"
                try {
                  const d = new Date(e + 'T00:00:00')
                  const label = d.toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' })
                  return <option key={e} value={e}>{label}</option>
                } catch {
                  return <option key={e} value={e}>{e}</option>
                }
              })}
            </select>
          )}

          {/* Stats */}
          {data && (
            <>
              <div className="text-[11px] text-text-muted">
                Spot: <span className="font-mono text-text-bright font-bold">{fmtNum(data.underlyingLtp)}</span>
              </div>
              {(data as any).source === 'scriptmaster' && (
                <span className="text-[10px] px-2 py-0.5 rounded bg-warning/15 text-warning font-medium">Offline — ScriptMaster data</span>
              )}
              <div className="text-[11px] text-text-muted">
                PCR: <span className={cn('font-mono font-semibold',
                  data.pcr > 1.2 ? 'text-profit' : data.pcr < 0.7 ? 'text-loss' : 'text-warning')}>
                  {data.pcr.toFixed(2)}
                </span>
              </div>
              <div className="text-[11px] text-text-muted">
                Max Pain: <span className="font-mono text-warning">{data.maxPainStrike}</span>
              </div>
            </>
          )}

          {isLoading && <RefreshCw className="w-3.5 h-3.5 text-brand animate-spin" />}
        </div>

        {/* Tab bar */}
        <div className="flex items-center gap-1 bg-bg-surface border border-border rounded-lg p-1 w-fit">
          {(['chain', 'analytics', 'basket'] as OCTab[]).map(t => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={cn(
                'px-4 py-1.5 rounded text-[12px] font-medium capitalize transition-colors',
                tab === t ? 'bg-brand text-bg-base' : 'text-text-sec hover:text-text-bright'
              )}
            >
              {t === 'chain' ? 'Option Chain' : t === 'analytics' ? 'Analytics' : 'Basket Order'}
            </button>
          ))}
        </div>

        {tab === 'chain'     && <OptionChainTable />}
        {tab === 'analytics' && <OptionAnalytics />}
        {tab === 'basket'    && <BasketOrder />}
      </div>
    </div>
  )
}

// ── Column selector dropdown ───────────────────────
function ColumnSelector({ visible, onChange }: {
  visible: Set<string>
  onChange: (id: string, on: boolean) => void
}) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    function handler(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    if (open) document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [open])

  return (
    <div className="relative" ref={ref}>
      <button
        onClick={() => setOpen(v => !v)}
        className={cn(
          'flex items-center gap-1.5 px-2.5 py-1.5 rounded text-[11px] font-medium transition-colors border',
          open
            ? 'bg-brand/15 text-brand border-brand/30'
            : 'text-text-sec hover:text-text-bright border-border hover:border-brand/40'
        )}
      >
        <Settings2 className="w-3 h-3" />
        Columns
        <ChevronDown className={cn('w-3 h-3 transition-transform', open && 'rotate-180')} />
      </button>

      {open && (
        <div className="absolute right-0 top-full mt-1 z-50 bg-bg-card border border-border rounded-lg shadow-xl p-3 min-w-[160px]">
          <div className="text-[10px] font-semibold text-text-muted uppercase tracking-wider mb-2 px-1">
            Toggle Columns
          </div>
          {COLUMN_DEFS.map(col => (
            <label key={col.id} className="flex items-center gap-2 px-1 py-1 rounded hover:bg-bg-hover cursor-pointer">
              <input
                type="checkbox"
                checked={visible.has(col.id)}
                onChange={e => onChange(col.id, e.target.checked)}
                className="w-3 h-3 accent-brand"
              />
              <span className="text-[12px] text-text-sec">{col.label}</span>
            </label>
          ))}
        </div>
      )}
    </div>
  )
}

// ── Value-change flash helper ──────────────────
type SnapKey = string  // "strike:side:field"
type SnapMap = Map<SnapKey, number>

function useValueFlash(rows: any[] | undefined) {
  const prevRef = useRef<SnapMap>(new Map())
  const [flashSet, setFlashSet] = useState<Set<SnapKey>>(new Set())

  useEffect(() => {
    if (!rows || rows.length === 0) return
    const prev = prevRef.current
    const next: SnapMap = new Map()
    const flashing = new Set<SnapKey>()

    for (const row of rows) {
      for (const side of ['call', 'put'] as const) {
        for (const field of ['ltp', 'oi', 'volume', 'iv'] as const) {
          const key: SnapKey = `${row.strike}:${side}:${field}`
          const val = row[side]?.[field] ?? 0
          next.set(key, val)
          const oldVal = prev.get(key)
          if (oldVal !== undefined && oldVal !== val && val !== 0) {
            flashing.add(key)
          }
        }
      }
    }
    prevRef.current = next

    if (flashing.size > 0) {
      setFlashSet(flashing)
      const timer = setTimeout(() => setFlashSet(new Set()), 800)
      return () => clearTimeout(timer)
    }
  }, [rows])

  return flashSet
}

// ── Full Option Chain Table ────────────────────────
function OptionChainTable() {
  const { data, addToBasket, isLoading } = useOptionChainStore()
  const { openOrderModal } = useUIStore()
  const [highlightOI, setHighlightOI] = useState(true)
  const flashSet = useValueFlash(data?.rows)

  // ── Live tick overlay for instant LTP updates between REST polls ──
  const [tickOverlay, setTickOverlay] = useState<Record<string, number>>({})

  useEffect(() => {
    if (!data?.rows?.length) return
    // Collect all option trading symbols for subscription
    const symbols: string[] = []
    for (const row of data.rows) {
      const callSym = (row.call as any)?.trading_symbol
      const putSym = (row.put as any)?.trading_symbol
      if (callSym) symbols.push(callSym)
      if (putSym) symbols.push(putSym)
    }
    // Also subscribe to underlying for spot price
    const underlying = data.underlying
    if (underlying) symbols.push(underlying)

    if (symbols.length === 0) return

    // Clear stale overlay from previous underlying/expiry
    setTickOverlay({})

    marketWs.connect()
    marketWs.subscribe(symbols)

    const normSym = (s: string) => s.toUpperCase().replace(/-INDEX|-EQ|-BE/g, '').replace(/\s/g, '')
    const unsub = marketWs.onTick((tick: MarketTick) => {
      const sym = normSym(tick.symbol)
      if (tick.ltp > 0) {
        setTickOverlay(prev => ({ ...prev, [sym]: tick.ltp }))
      }
    })

    return () => {
      unsub()
      // Unsubscribe old symbols — visible strikes only pattern
      marketWs.unsubscribe(symbols)
    }
  }, [data?.underlying, data?.expiry, data?.rows?.length])

  // Apply tick overlay to get effective LTP for a side
  const getLtp = useCallback((side: any): number => {
    const tradingSym = side?.trading_symbol
    if (tradingSym && tickOverlay[tradingSym.toUpperCase().replace(/-INDEX|-EQ|-BE/g, '').replace(/\s/g, '')] != null) {
      return tickOverlay[tradingSym.toUpperCase().replace(/-INDEX|-EQ|-BE/g, '').replace(/\s/g, '')]
    }
    return side?.ltp ?? 0
  }, [tickOverlay])

  // Helper: returns flash CSS class if this cell's value just changed
  const flashCls = (strike: number, side: 'call' | 'put', field: string) =>
    flashSet.has(`${strike}:${side}:${field}`) ? 'oc-flash' : ''

  // Persistent column visibility
  const [visibleCols, setVisibleCols] = useState<Set<string>>(() => {
    try {
      const saved = localStorage.getItem(COL_STORAGE_KEY)
      if (saved) return new Set(JSON.parse(saved))
    } catch { /* ignore */ }
    return new Set(COLUMN_DEFS.filter(c => c.defaultOn).map(c => c.id))
  })

  function toggleCol(id: string, on: boolean) {
    setVisibleCols(prev => {
      const next = new Set(prev)
      if (on) { next.add(id) } else { next.delete(id) }
      localStorage.setItem(COL_STORAGE_KEY, JSON.stringify([...next]))
      return next
    })
  }

  if (!data) return <Skeleton />
  if (data.rows.length === 0) return (
    <div className="card flex items-center justify-center h-40 text-text-muted text-sm">No option chain data available. Select an underlying and expiry above.</div>
  )

  const maxCallOI = Math.max(...data.rows.map(r => r.call.oi), 1)
  const maxPutOI  = Math.max(...data.rows.map(r => r.put.oi), 1)

  // Ordered call-side columns (left to right): oiChange, oi, volume, iv, delta, gamma, theta, vega
  const callCols  = COLUMN_DEFS.filter(c => visibleCols.has(c.id))
  // Put-side columns are the mirror (reversed) of call-side: vega, theta, gamma, delta, iv, volume, oi, oiChange
  const putCols   = [...callCols].reverse()

  function renderCallCell(col: ColDef, row: any) {
    const c = row.call
    switch (col.id) {
      case 'oiChange': return (
        <td key={col.id} className={cn('px-2 py-1.5 text-right font-mono bg-profit/3', c.oiChange >= 0 ? 'text-profit' : 'text-loss')}>
          {c.oiChange >= 0 ? '+' : ''}{fmtOI(c.oiChange)}
        </td>
      )
      case 'oi': return (
        <td key={col.id} className={cn('px-2 py-1.5 text-right font-mono text-text-sec bg-profit/3 relative', flashCls(row.strike, 'call', 'oi'))}>
          {highlightOI && <div className="absolute inset-y-0 right-0 bg-profit/12 transition-all" style={{ width: `${(c.oi / maxCallOI) * 100}%` }} />}
          <span className="relative z-10">{fmtOI(c.oi)}</span>
        </td>
      )
      case 'volume': return <td key={col.id} className={cn('px-2 py-1.5 text-right font-mono text-text-muted bg-profit/3', flashCls(row.strike, 'call', 'volume'))}>{fmtOI(c.volume)}</td>
      case 'iv':     return <td key={col.id} className={cn('px-2 py-1.5 text-right font-mono font-semibold bg-profit/3', ivColor(c.iv), flashCls(row.strike, 'call', 'iv'))}>{c.iv?.toFixed(1) ?? '—'}</td>
      case 'delta':  return <td key={col.id} className="px-2 py-1.5 text-right font-mono text-text-sec bg-profit/3">{(c.delta ?? 0).toFixed(3)}</td>
      case 'gamma':  return <td key={col.id} className="px-2 py-1.5 text-right font-mono text-text-muted bg-profit/3">{(c.gamma ?? 0).toFixed(5)}</td>
      case 'theta':  return <td key={col.id} className={cn('px-2 py-1.5 text-right font-mono bg-profit/3', (c.theta ?? 0) < 0 ? 'text-loss/70' : 'text-text-muted')}>{(c.theta ?? 0).toFixed(2)}</td>
      case 'vega':   return <td key={col.id} className="px-2 py-1.5 text-right font-mono text-blue-400/80 bg-profit/3">{(c.vega ?? 0).toFixed(2)}</td>
      default:       return <td key={col.id} className="bg-profit/3" />
    }
  }

  function renderPutCell(col: ColDef, row: any) {
    const p = row.put
    switch (col.id) {
      case 'oiChange': return (
        <td key={col.id} className={cn('px-2 py-1.5 text-left font-mono bg-loss/3', p.oiChange >= 0 ? 'text-profit' : 'text-loss')}>
          {p.oiChange >= 0 ? '+' : ''}{fmtOI(p.oiChange)}
        </td>
      )
      case 'oi': return (
        <td key={col.id} className={cn('px-2 py-1.5 text-left font-mono text-text-sec bg-loss/3 relative', flashCls(row.strike, 'put', 'oi'))}>
          {highlightOI && <div className="absolute inset-y-0 left-0 bg-loss/12 transition-all" style={{ width: `${(p.oi / maxPutOI) * 100}%` }} />}
          <span className="relative z-10">{fmtOI(p.oi)}</span>
        </td>
      )
      case 'volume': return <td key={col.id} className={cn('px-2 py-1.5 text-left font-mono text-text-muted bg-loss/3', flashCls(row.strike, 'put', 'volume'))}>{fmtOI(p.volume)}</td>
      case 'iv':     return <td key={col.id} className={cn('px-2 py-1.5 text-left font-mono font-semibold bg-loss/3', ivColor(p.iv), flashCls(row.strike, 'put', 'iv'))}>{p.iv?.toFixed(1) ?? '—'}</td>
      case 'delta':  return <td key={col.id} className="px-2 py-1.5 text-left font-mono text-text-sec bg-loss/3">{(p.delta ?? 0).toFixed(3)}</td>
      case 'gamma':  return <td key={col.id} className="px-2 py-1.5 text-left font-mono text-text-muted bg-loss/3">{(p.gamma ?? 0).toFixed(5)}</td>
      case 'theta':  return <td key={col.id} className={cn('px-2 py-1.5 text-left font-mono bg-loss/3', (p.theta ?? 0) < 0 ? 'text-loss/70' : 'text-text-muted')}>{(p.theta ?? 0).toFixed(2)}</td>
      case 'vega':   return <td key={col.id} className="px-2 py-1.5 text-left font-mono text-blue-400/80 bg-loss/3">{(p.vega ?? 0).toFixed(2)}</td>
      default:       return <td key={col.id} className="bg-loss/3" />
    }
  }

  return (
    <div className="bg-bg-card border border-border rounded-lg overflow-hidden relative">
      <div className="flex items-center gap-3 px-4 py-2.5 border-b border-border">
        <span className="text-[12px] text-text-sec">{data.rows.length} strikes</span>
        <label className="flex items-center gap-2 text-[11px] text-text-sec cursor-pointer">
          <input type="checkbox" checked={highlightOI} onChange={e => setHighlightOI(e.target.checked)} className="w-3 h-3" />
          OI heatmap
        </label>
        <div className="flex-1" />
        <ColumnSelector visible={visibleCols} onChange={toggleCol} />
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-[11px] border-collapse">
          <thead>
            <tr className="border-b border-border/50">
              {/* Call side headers (left to right: oiChange → delta → LTP) */}
              {callCols.map(col => (
                <th key={`ch-${col.id}`} className="px-2 py-2 text-[10px] font-medium text-profit uppercase tracking-wider text-right bg-profit/5">
                  {col.symbol ?? col.label}
                </th>
              ))}
              <th className="px-2 py-2 text-[10px] font-medium text-profit uppercase tracking-wider text-right bg-profit/5 w-20">LTP</th>
              {/* Strike */}
              <th className="px-2 py-2 text-[10px] font-medium text-text-bright uppercase tracking-wider text-center w-24 bg-brand/8">Strike</th>
              {/* Put side headers (mirrored: LTP → delta → oiChange) */}
              <th className="px-2 py-2 text-[10px] font-medium text-loss uppercase tracking-wider text-left bg-loss/5 w-20">LTP</th>
              {putCols.map(col => (
                <th key={`ph-${col.id}`} className="px-2 py-2 text-[10px] font-medium text-loss uppercase tracking-wider text-left bg-loss/5">
                  {col.symbol ?? col.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.rows.map(row => {
              return (
                <tr
                  key={row.strike}
                  className={cn('border-b border-border/20 hover:bg-bg-hover/40 transition-colors',
                    row.isATM && 'oc-atm')}
                >
                  {/* CALL SIDE */}
                  {callCols.map(col => renderCallCell(col, row))}
                  {/* Call LTP (always visible) */}
                  <td
                    className={cn('px-2 py-1.5 text-right font-mono font-bold text-profit bg-profit/5 cursor-pointer hover:underline', flashCls(row.strike, 'call', 'ltp'))}
                    onClick={() => openOrderModal((row.call as any)?.trading_symbol || data.underlying + `${row.strike}CE`, (data as any)?.exchange || UNDERLYING_MAP[data.underlying] || 'NFO')}
                  >
                    {fmtNum(getLtp(row.call))}
                  </td>

                  {/* STRIKE */}
                  <td className={cn("px-2 py-1.5 text-center font-bold font-mono text-[12px] bg-brand/5",
                    row.isATM ? 'text-brand' : 'text-text-bright')}>
                    {row.strike}
                    {row.isATM && <span className="ml-1 text-[9px] badge badge-brand">ATM</span>}
                  </td>

                  {/* PUT LTP (always visible) */}
                  <td
                    className={cn('px-2 py-1.5 text-left font-mono font-bold text-loss bg-loss/5 cursor-pointer hover:underline', flashCls(row.strike, 'put', 'ltp'))}
                    onClick={() => openOrderModal((row.put as any)?.trading_symbol || data.underlying + `${row.strike}PE`, (data as any)?.exchange || UNDERLYING_MAP[data.underlying] || 'NFO')}
                  >
                    {fmtNum(getLtp(row.put))}
                  </td>
                  {/* PUT SIDE */}
                  {putCols.map(col => renderPutCell(col, row))}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Option Analytics ──────────────────────────────
function OptionAnalytics() {
  const { data } = useOptionChainStore()
  if (!data) return <Skeleton />
  if (data.rows.length === 0) return (
    <div className="card flex items-center justify-center h-40 text-text-muted text-sm">No analytics data — option chain is empty.</div>
  )

  const maxCallOI = Math.max(...data.rows.map(r => r.call.oi), 1)
  const maxPutOI  = Math.max(...data.rows.map(r => r.put.oi), 1)
  const maxCallOIRow = data.rows.find(r => r.call.oi === maxCallOI)
  const maxPutOIRow  = data.rows.find(r => r.put.oi  === maxPutOI)

  const totalCallOI = data.rows.reduce((s, r) => s + r.call.oi, 0)
  const totalPutOI  = data.rows.reduce((s, r) => s + r.put.oi,  0)
  const totalCallVol = data.rows.reduce((s, r) => s + r.call.volume, 0)
  const totalPutVol  = data.rows.reduce((s, r) => s + r.put.volume,  0)

  const atm = data.rows.find(r => r.isATM)

  const metrics = [
    { label: 'Put-Call Ratio (OI)',    value: data.pcr.toFixed(3),  cls: data.pcr > 1.2 ? 'text-profit' : data.pcr < 0.7 ? 'text-loss' : 'text-warning' },
    { label: 'Max Pain Strike',         value: String(data.maxPainStrike), cls: 'text-warning' },
    { label: 'Max Call OI Strike',      value: String(maxCallOIRow?.strike ?? '—'), cls: 'text-loss' },
    { label: 'Max Put OI Strike',       value: String(maxPutOIRow?.strike ?? '—'),  cls: 'text-profit' },
    { label: 'Total Call OI',           value: fmtOI(totalCallOI),  cls: 'text-loss' },
    { label: 'Total Put OI',            value: fmtOI(totalPutOI),   cls: 'text-profit' },
    { label: 'Total Call Volume',       value: fmtOI(totalCallVol), cls: 'text-text-pri' },
    { label: 'Total Put Volume',        value: fmtOI(totalPutVol),  cls: 'text-text-pri' },
    { label: 'ATM Call IV',             value: atm ? `${atm.call.iv.toFixed(2)}%` : '—', cls: ivColor(atm?.call.iv ?? 0) },
    { label: 'ATM Put IV',              value: atm ? `${atm.put.iv.toFixed(2)}%` : '—',  cls: ivColor(atm?.put.iv ?? 0) },
    { label: 'ATM Call Delta',          value: atm ? atm.call.delta.toFixed(4) : '—', cls: 'text-text-pri' },
    { label: 'ATM Put Delta',           value: atm ? atm.put.delta.toFixed(4) : '—',  cls: 'text-text-pri' },
  ]

  return (
    <div className="space-y-4">
      {/* Metrics grid */}
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
        {metrics.map((m, i) => (
          <div key={i} className="kpi-card">
            <div className="section-title">{m.label}</div>
            <div className={cn('text-base font-bold font-mono mt-1', m.cls)}>{m.value}</div>
          </div>
        ))}
      </div>

      {/* OI Bar Chart by strike */}
      <div className="bg-bg-card border border-border rounded-lg p-4">
        <h3 className="text-[12px] font-semibold text-text-bright mb-3">OI Distribution by Strike</h3>
        <div className="space-y-1 max-h-80 overflow-y-auto">
          {data.rows.map(row => {
            const callW = (row.call.oi / maxCallOI) * 100
            const putW  = (row.put.oi  / maxPutOI)  * 100
            return (
              <div key={row.strike} className={cn('flex items-center gap-2 group', row.isATM && 'bg-brand/5 rounded')}>
                {/* Call bar (right to left) */}
                <div className="flex-1 flex justify-end">
                  <div className="relative h-5 bg-profit/8 rounded-l overflow-hidden" style={{ width: `${callW}%`, minWidth: 4 }}>
                    <div className="absolute inset-0 bg-profit/30 rounded-l" />
                    <span className="absolute right-1 text-[9px] font-mono text-profit leading-5">{fmtOI(row.call.oi)}</span>
                  </div>
                </div>
                {/* Strike label */}
                <div className={cn('text-[11px] font-mono font-bold w-16 text-center shrink-0',
                  row.isATM ? 'text-brand' : 'text-text-bright')}>
                  {row.strike}
                </div>
                {/* Put bar (left to right) */}
                <div className="flex-1">
                  <div className="relative h-5 bg-loss/8 rounded-r overflow-hidden" style={{ width: `${putW}%`, minWidth: 4 }}>
                    <div className="absolute inset-0 bg-loss/30 rounded-r" />
                    <span className="absolute left-1 text-[9px] font-mono text-loss leading-5">{fmtOI(row.put.oi)}</span>
                  </div>
                </div>
              </div>
            )
          })}
        </div>
        <div className="flex justify-between mt-3 text-[10px] text-text-muted">
          <span className="text-profit">← Call OI</span>
          <span className="text-loss">Put OI →</span>
        </div>
      </div>
    </div>
  )
}

// ── Basket Order ──────────────────────────────────
function BasketOrder() {
  const { basket, addToBasket, removeFromBasket, clearBasket } = useOptionChainStore()
  const { data } = useOptionChainStore()
  const { toast } = useToastStore()
  const [placing, setPlacing] = useState(false)

  const [newLeg, setNewLeg] = useState({
    strike: '', type: 'CE' as 'CE' | 'PE', txn: 'BUY' as TransactionType,
    qty: 1, orderType: 'MARKET' as OrderType, price: 0,
  })

  function addLeg() {
    if (!data || !newLeg.strike) return
    const row = data.rows.find(r => String(r.strike) === newLeg.strike)
    const side = newLeg.type === 'CE' ? row?.call : row?.put
    const ltp = side?.ltp
    // Use trading_symbol from ScriptMaster-enriched option chain if available
    const tsym = (side as any)?.trading_symbol || `${data.underlying}${data.expiry}${newLeg.strike}${newLeg.type}`
    const lotSize = (side as any)?.lot_size || (data as any)?.lot_size || 50
    addToBasket({
      id: uid(), symbol: tsym, tradingsymbol: tsym, exchange: (data as any)?.exchange || UNDERLYING_MAP[data.underlying] || 'NFO',
      transactionType: newLeg.txn, quantity: newLeg.qty * lotSize,
      orderType: newLeg.orderType, price: ltp ?? newLeg.price, product: 'MIS', ltp,
    })
  }

  async function placeBasket() {
    if (!basket.length) return
    setPlacing(true)
    try {
      await Promise.all(basket.map(leg =>
        api.placeOrder({
          symbol: leg.symbol, tradingsymbol: leg.tradingsymbol, exchange: leg.exchange,
          transactionType: leg.transactionType, quantity: leg.quantity,
          orderType: leg.orderType, price: leg.price, product: leg.product,
        })
      ))
      toast(`${basket.length} basket orders placed successfully`, 'success')
      clearBasket()
    } catch {
      toast('Failed to place basket orders', 'error')
    } finally {
      setPlacing(false)
    }
  }

  const totalPremium = basket.reduce((s, b) =>
    s + (b.transactionType === 'BUY' ? -b.quantity : b.quantity) * (b.ltp ?? b.price), 0)

  return (
    <div className="space-y-4">
      {/* Add leg form */}
      <div className="bg-bg-card border border-border rounded-lg p-4">
        <h3 className="text-[12px] font-semibold text-text-bright mb-3">Add Option Leg</h3>
        <div className="flex flex-wrap items-end gap-3">
          <div className="space-y-1">
            <label className="section-title">Strike</label>
            <select
              value={newLeg.strike}
              onChange={e => setNewLeg(p => ({ ...p, strike: e.target.value }))}
              className="select-base w-28 text-[12px]"
            >
              <option value="">Select</option>
              {data?.rows.map(r => <option key={r.strike} value={String(r.strike)}>{r.strike}</option>)}
            </select>
          </div>
          <div className="space-y-1">
            <label className="section-title">Type</label>
            <select
              value={newLeg.type}
              onChange={e => setNewLeg(p => ({ ...p, type: e.target.value as any }))}
              className="select-base w-20 text-[12px]"
            >
              <option value="CE">CE</option>
              <option value="PE">PE</option>
            </select>
          </div>
          <div className="space-y-1">
            <label className="section-title">B/S</label>
            <select
              value={newLeg.txn}
              onChange={e => setNewLeg(p => ({ ...p, txn: e.target.value as any }))}
              className="select-base w-20 text-[12px]"
            >
              <option value="BUY">BUY</option>
              <option value="SELL">SELL</option>
            </select>
          </div>
          <div className="space-y-1">
            <label className="section-title">Lots</label>
            <input
              type="number" min={1} value={newLeg.qty}
              onChange={e => setNewLeg(p => ({ ...p, qty: parseInt(e.target.value) || 1 }))}
              className="input-base w-20 text-[12px] text-right"
            />
          </div>
          <button onClick={addLeg} className="btn-primary btn-sm">
            <Plus className="w-3.5 h-3.5" /> Add Leg
          </button>
        </div>
      </div>

      {/* Basket legs */}
      <div className="bg-bg-card border border-border rounded-lg overflow-hidden">
        <div className="flex items-center gap-3 px-4 py-3 border-b border-border">
          <ShoppingCart className="w-4 h-4 text-warning" />
          <span className="text-[12px] font-semibold text-text-bright">Basket</span>
          <span className="badge badge-neutral">{basket.length} legs</span>
          <div className="flex-1" />
          <span className={cn('text-[12px] font-mono font-semibold', totalPremium >= 0 ? 'text-profit' : 'text-loss')}>
            Net: {totalPremium >= 0 ? '+' : ''}{fmtINR(totalPremium)}
          </span>
          {basket.length > 0 && (
            <button onClick={clearBasket} className="btn-ghost btn-xs text-text-muted hover:text-loss">
              <Trash2 className="w-3 h-3" /> Clear
            </button>
          )}
        </div>

        {basket.length === 0 ? (
          <div className="flex items-center justify-center h-24 text-text-muted text-[12px]">
            Add legs above to build a strategy
          </div>
        ) : (
          <table className="data-table">
            <thead>
              <tr>
                <th className="text-left px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Symbol</th>
                <th className="text-center px-3 py-2 text-[10px] font-medium text-text-muted uppercase">B/S</th>
                <th className="text-right px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Qty</th>
                <th className="text-right px-3 py-2 text-[10px] font-medium text-text-muted uppercase">LTP</th>
                <th className="px-3 py-2 w-8"></th>
              </tr>
            </thead>
            <tbody>
              {basket.map(leg => (
                <tr key={leg.id}>
                  <td className="px-3 py-2 text-[12px] font-medium text-text-bright truncate max-w-[180px]">{leg.tradingsymbol}</td>
                  <td className="px-3 py-2 text-center">
                    <span className={cn('badge', leg.transactionType === 'BUY' ? 'badge-buy' : 'badge-sell')}>
                      {leg.transactionType}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-right text-[12px] font-mono text-text-pri">{leg.quantity}</td>
                  <td className="px-3 py-2 text-right text-[12px] font-mono text-text-bright">{leg.ltp ? fmtNum(leg.ltp) : '—'}</td>
                  <td className="px-3 py-2">
                    <button onClick={() => removeFromBasket(leg.id)} className="btn-ghost btn-xs !px-1 hover:text-loss">
                      <Minus className="w-3 h-3" />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}

        {basket.length > 0 && (
          <div className="px-4 py-3 border-t border-border flex justify-end gap-3">
            <button onClick={clearBasket} className="btn-outline btn-sm">Cancel</button>
            <button
              onClick={placeBasket}
              disabled={placing}
              className="btn-primary btn-sm"
            >
              {placing ? <RefreshCw className="w-3.5 h-3.5 animate-spin" /> : <CheckCircle2 className="w-3.5 h-3.5" />}
              Place {basket.length} Orders
            </button>
          </div>
        )}
      </div>
    </div>
  )
}

function Skeleton() {
  return (
    <div className="space-y-3">
      {[...Array(6)].map((_, i) => (
        <div key={i} className="skeleton h-10 w-full rounded" />
      ))}
    </div>
  )
}
