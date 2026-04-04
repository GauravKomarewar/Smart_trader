/* ════════════════════════════════════════════
   Market & Screener Page
   ════════════════════════════════════════════ */
import { useState } from 'react'
import { useMarketIndices, useScreenerData, useGlobalMarkets } from '../hooks'
import { useMarketStore, useUIStore } from '../stores'
import { cn, fmtNum, fmtVol, changeCls, fmtINRCompact } from '../lib/utils'
import { TrendingUp, TrendingDown, BarChart2, Search, Filter, ArrowUpDown, Globe } from 'lucide-react'
import type { IndexQuote, ScreenerRow } from '../types'

type MarketTab = 'indices' | 'heatmap' | 'screener' | 'global'

export default function MarketPage() {
  useMarketIndices()
  useScreenerData()
  useGlobalMarkets()
  const [tab, setTab] = useState<MarketTab>('indices')

  const tabs = [
    { key: 'indices' as MarketTab,  label: 'Indices & Sectors' },
    { key: 'global'  as MarketTab,  label: 'Global Markets' },
    { key: 'heatmap' as MarketTab,  label: 'Market Heatmap' },
    { key: 'screener' as MarketTab, label: 'Screener' },
  ]

  return (
    <div className="h-full overflow-y-auto">
      <div className="p-4 space-y-4">
        {/* Tab bar */}
        <div className="flex items-center gap-1 bg-bg-surface border border-border rounded-lg p-1 w-fit">
          {tabs.map(t => (
            <button
              key={t.key}
              onClick={() => setTab(t.key)}
              className={cn(
                'px-4 py-1.5 rounded text-[12px] font-medium transition-colors',
                tab === t.key ? 'bg-brand text-bg-base' : 'text-text-sec hover:text-text-bright'
              )}
            >
              {t.label}
            </button>
          ))}
        </div>

        {tab === 'indices'  && <IndicesPanel />}
        {tab === 'global'   && <GlobalMarketsPanel />}
        {tab === 'heatmap'  && <HeatmapPanel />}
        {tab === 'screener' && <ScreenerPanel />}
      </div>
    </div>
  )
}

// ── Indices Panel ────────────────────────────────
function IndicesPanel() {
  const indices = useMarketStore(s => s.indices)
  const { openChartModal } = useUIStore()

  return (
    <div className="space-y-4">
      {/* Index cards grid */}
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-3">
        {indices.map(idx => (
          <IndexCard key={idx.token} idx={idx} onChart={() => openChartModal(idx.token)} />
        ))}
      </div>

      {/* Full table */}
      <div className="bg-bg-card border border-border rounded-lg overflow-hidden">
        <div className="px-4 py-3 border-b border-border">
          <span className="text-[13px] font-semibold text-text-bright">Detailed Index Table</span>
        </div>
        <div className="overflow-x-auto">
          <table className="data-table">
            <thead>
              <tr>
                {['Index', 'LTP', 'Chg', 'Chg %', 'Open', 'High', 'Low', 'Volume', 'Adv/Dec'].map(h => (
                  <th key={h} className="px-4 py-2.5 text-[10px] font-medium text-text-muted uppercase tracking-wider text-right first:text-left whitespace-nowrap">
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {indices.map(idx => (
                <tr key={idx.token} className="cursor-pointer"
                  onClick={() => openChartModal(idx.token)}>
                  <td className="px-4 py-2.5">
                    <span className="text-[13px] font-semibold text-text-bright">{idx.symbol}</span>
                    <span className="ml-2 text-[10px] text-text-muted">{idx.exchange}</span>
                  </td>
                  <td className="px-4 py-2.5 text-right text-[13px] font-mono font-bold text-text-bright">{fmtNum(idx.ltp)}</td>
                  <td className={cn('px-4 py-2.5 text-right text-[12px] font-mono', changeCls(idx.change))}>
                    {idx.change >= 0 ? '+' : ''}{fmtNum(idx.change)}
                  </td>
                  <td className={cn('px-4 py-2.5 text-right text-[12px] font-mono font-semibold', changeCls(idx.changePct))}>
                    {idx.changePct >= 0 ? '+' : ''}{idx.changePct.toFixed(2)}%
                  </td>
                  <td className="px-4 py-2.5 text-right text-[12px] font-mono text-text-sec">{fmtNum(idx.open)}</td>
                  <td className="px-4 py-2.5 text-right text-[12px] font-mono text-profit">{fmtNum(idx.high)}</td>
                  <td className="px-4 py-2.5 text-right text-[12px] font-mono text-loss">{fmtNum(idx.low)}</td>
                  <td className="px-4 py-2.5 text-right text-[11px] font-mono text-text-muted">{fmtVol(idx.volume)}</td>
                  <td className="px-4 py-2.5 text-right text-[11px]">
                    <span className="text-profit">{idx.advances}</span>
                    <span className="text-text-muted mx-1">/</span>
                    <span className="text-loss">{idx.declines}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

function IndexCard({ idx, onChart }: { idx: IndexQuote; onChart: () => void }) {
  const isUp = idx.changePct >= 0
  const adRatio = idx.advances / Math.max(1, idx.advances + idx.declines)

  return (
    <div
      onClick={onChart}
      className={cn(
        'bg-bg-card border rounded-lg p-3 cursor-pointer transition-all duration-150 space-y-1.5',
        'hover:border-brand/40 hover:bg-bg-elevated',
        isUp ? 'border-profit/20' : 'border-loss/20'
      )}
    >
      <div className="flex items-start justify-between gap-2">
        <span className="text-[11px] font-semibold text-text-pri leading-tight">{idx.symbol}</span>
        {isUp
          ? <TrendingUp className="w-3.5 h-3.5 text-profit shrink-0" />
          : <TrendingDown className="w-3.5 h-3.5 text-loss shrink-0" />}
      </div>
      <div className="text-[16px] font-bold font-mono text-text-bright">{fmtNum(idx.ltp)}</div>
      <div className={cn('text-[12px] font-mono font-semibold', changeCls(idx.changePct))}>
        {idx.changePct >= 0 ? '+' : ''}{idx.changePct.toFixed(2)}%
        <span className="text-[10px] font-normal ml-1 text-text-muted">
          ({idx.change >= 0 ? '+' : ''}{fmtNum(idx.change)})
        </span>
      </div>
      {/* A/D mini bar */}
      <div className="h-1 bg-loss/20 rounded-full overflow-hidden">
        <div className="h-full bg-profit/60 rounded-full" style={{ width: `${adRatio * 100}%` }} />
      </div>
      <div className="flex justify-between text-[9px] text-text-muted">
        <span className="text-profit">{idx.advances}A</span>
        <span className="text-loss">{idx.declines}D</span>
      </div>
    </div>
  )
}

// ── Global Markets Panel ──────────────────────────
function GlobalMarketsPanel() {
  const globalMarkets = useMarketStore(s => s.globalMarkets)

  const commodities = globalMarkets.filter(r => r.category === 'commodity')
  const forex       = globalMarkets.filter(r => r.category === 'forex')

  return (
    <div className="space-y-6">
      {/* Commodities */}
      <div className="bg-bg-card border border-border rounded-lg overflow-hidden">
        <div className="px-4 py-3 border-b border-border flex items-center gap-2">
          <Globe className="w-4 h-4 text-brand" />
          <span className="text-[13px] font-semibold text-text-bright">Commodities (MCX)</span>
        </div>
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-px bg-border">
          {commodities.map(r => (
            <GlobalCard key={r.symbol} item={r} />
          ))}
        </div>
      </div>

      {/* Forex / Currency */}
      <div className="bg-bg-card border border-border rounded-lg overflow-hidden">
        <div className="px-4 py-3 border-b border-border flex items-center gap-2">
          <TrendingUp className="w-4 h-4 text-brand" />
          <span className="text-[13px] font-semibold text-text-bright">Currency Futures (NSE)</span>
        </div>
        <div className="overflow-x-auto">
          <table className="data-table w-full">
            <thead>
              <tr>
                {['Pair', 'LTP (₹)', 'Change', 'Chg %', 'Unit'].map(h => (
                  <th key={h} className="px-4 py-2.5 text-[10px] font-medium text-text-muted uppercase tracking-wider text-right first:text-left whitespace-nowrap">
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {forex.map(r => (
                <tr key={r.symbol}>
                  <td className="px-4 py-2.5">
                    <div className="text-[13px] font-semibold text-text-bright">{r.name}</div>
                    <div className="text-[10px] text-text-muted">{r.symbol}</div>
                  </td>
                  <td className="px-4 py-2.5 text-right text-[13px] font-mono font-bold text-text-bright">
                    {r.ltp?.toFixed(2)}
                  </td>
                  <td className={cn('px-4 py-2.5 text-right text-[12px] font-mono', changeCls(r.change))}>
                    {r.change >= 0 ? '+' : ''}{r.change?.toFixed(4)}
                  </td>
                  <td className={cn('px-4 py-2.5 text-right text-[12px] font-mono font-semibold', changeCls(r.changePct))}>
                    {r.changePct >= 0 ? '+' : ''}{r.changePct?.toFixed(2)}%
                  </td>
                  <td className="px-4 py-2.5 text-right text-[11px] text-text-muted">{r.unit}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {globalMarkets.length > 0 && globalMarkets[0].source === 'demo' && (
        <div className="text-[11px] text-text-muted text-center">
          Demo data shown — connect Fyers API from Admin &gt; Data Sources for live prices
        </div>
      )}
    </div>
  )
}

function GlobalCard({ item }: { item: any }) {
  const isUp = (item.changePct ?? 0) >= 0
  return (
    <div className="bg-bg-card px-3 py-3 space-y-1">
      <div className="text-[11px] font-semibold text-text-bright leading-tight">{item.name}</div>
      <div className="text-[14px] font-bold font-mono text-text-bright">
        {typeof item.ltp === 'number' && item.ltp > 1000 ? fmtNum(item.ltp) : item.ltp?.toFixed(2)}
      </div>
      <div className={cn('text-[11px] font-mono font-semibold', isUp ? 'text-profit' : 'text-loss')}>
        {isUp ? '+' : ''}{(item.changePct ?? 0).toFixed(2)}%
      </div>
      <div className="text-[9px] text-text-muted">{item.unit}</div>
    </div>
  )
}

// ── Heatmap Panel ─────────────────────────────────
function HeatmapPanel() {
  const indices = useMarketStore(s => s.indices)
  const screener = useMarketStore(s => s.screener)
  const allRows  = [...screener].sort((a, b) => Math.abs(b.changePct) - Math.abs(a.changePct))

  function heatColor(pct: number): string {
    const abs = Math.min(Math.abs(pct), 5) / 5
    if (pct > 0) return `rgba(34,197,94,${0.15 + abs * 0.6})`
    if (pct < 0) return `rgba(244,63,94,${0.15 + abs * 0.6})`
    return 'rgba(37,43,59,0.8)'
  }

  return (
    <div className="space-y-4">
      {/* Index heatmap */}
      <div className="bg-bg-card border border-border rounded-lg p-4">
        <h3 className="text-[12px] font-semibold text-text-bright mb-3">Index Heatmap</h3>
        <div className="flex flex-wrap gap-2">
          {indices.map(idx => (
            <div
              key={idx.token}
              className="heatmap-cell px-4 py-3 min-w-[100px]"
              style={{ background: heatColor(idx.changePct), border: '1px solid rgba(255,255,255,.06)' }}
            >
              <div className="text-[11px] font-semibold text-text-bright">{idx.symbol}</div>
              <div className="text-[12px] font-bold font-mono" style={{ color: idx.changePct >= 0 ? '#22c55e' : '#f43f5e' }}>
                {idx.changePct >= 0 ? '+' : ''}{idx.changePct.toFixed(2)}%
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Stock heatmap */}
      <div className="bg-bg-card border border-border rounded-lg p-4">
        <h3 className="text-[12px] font-semibold text-text-bright mb-3">Nifty 50 Heatmap (by change)</h3>
        <div className="flex flex-wrap gap-1.5">
          {allRows.map(s => (
            <div
              key={s.symbol}
              className="heatmap-cell px-2.5 py-2 min-w-[80px] text-[10px]"
              style={{ background: heatColor(s.changePct), border: '1px solid rgba(255,255,255,.05)' }}
              title={`${s.name}: ${s.changePct.toFixed(2)}%`}
            >
              <div className="font-semibold text-text-bright truncate">{s.symbol}</div>
              <div className="font-mono" style={{ color: s.changePct >= 0 ? '#22c55e' : '#f43f5e' }}>
                {s.changePct >= 0 ? '+' : ''}{s.changePct.toFixed(2)}%
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

// ── Screener Panel ────────────────────────────────
function ScreenerPanel() {
  const screener = useMarketStore(s => s.screener)
  const { openChartModal, openOrderModal } = useUIStore()
  const [search, setSearch] = useState('')
  const [sortKey, setSortKey] = useState<keyof ScreenerRow>('changePct')
  const [sortDir, setSortDir] = useState<1 | -1>(-1)
  const [filter, setFilter] = useState<'all' | 'gainers' | 'losers' | 'active'>('all')

  const filtered = screener
    .filter(r => {
      if (search && !r.symbol.toLowerCase().includes(search.toLowerCase()) &&
          !r.name.toLowerCase().includes(search.toLowerCase())) return false
      if (filter === 'gainers' && r.changePct <= 0) return false
      if (filter === 'losers' && r.changePct >= 0) return false
      return true
    })
    .sort((a, b) => {
      const av = a[sortKey] as number, bv = b[sortKey] as number
      if (typeof av === 'number') return (av - bv) * sortDir
      return String(av).localeCompare(String(bv)) * sortDir
    })

  function toggleSort(key: keyof ScreenerRow) {
    if (sortKey === key) setSortDir(d => d === 1 ? -1 : 1)
    else { setSortKey(key); setSortDir(-1) }
  }

  const SH = ({ label, col }: { label: string; col: keyof ScreenerRow }) => (
    <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider cursor-pointer hover:text-text-sec select-none text-right"
      onClick={() => toggleSort(col)}>
      <div className="flex items-center justify-end gap-1">
        {label}
        {sortKey === col && <ArrowUpDown className="w-3 h-3" />}
      </div>
    </th>
  )

  return (
    <div className="bg-bg-card border border-border rounded-lg">
      {/* Controls */}
      <div className="flex items-center gap-3 px-4 py-3 border-b border-border flex-wrap">
        <div className="relative">
          <Search className="w-3.5 h-3.5 absolute left-2.5 top-1/2 -translate-y-1/2 text-text-muted" />
          <input
            value={search}
            onChange={e => setSearch(e.target.value)}
            className="input-base pl-8 py-1.5 w-48 text-[12px]"
            placeholder="Search symbol…"
          />
        </div>
        <div className="flex items-center gap-1">
          {(['all', 'gainers', 'losers'] as const).map(f => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={cn('px-3 py-1 rounded text-[11px] font-medium capitalize transition-colors',
                filter === f
                  ? f === 'gainers' ? 'bg-profit/15 text-profit'
                  : f === 'losers'  ? 'bg-loss/15 text-loss'
                  : 'bg-brand/15 text-brand'
                  : 'text-text-muted hover:text-text-sec')}
            >
              {f}
            </button>
          ))}
        </div>
        <div className="flex-1" />
        <span className="text-[11px] text-text-muted">{filtered.length} stocks</span>
      </div>

      <div className="overflow-auto">
        <table className="data-table">
          <thead className="sticky top-0 bg-bg-card z-10">
            <tr>
              <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-left cursor-pointer hover:text-text-sec"
                onClick={() => toggleSort('symbol')}>Symbol</th>
              <SH label="LTP" col="ltp" />
              <SH label="Chg" col="change" />
              <SH label="Chg %" col="changePct" />
              <SH label="Volume" col="volume" />
              <SH label="Mkt Cap" col="marketCap" />
              <SH label="P/E" col="pe" />
              <SH label="52W H" col="high52w" />
              <SH label="52W L" col="low52w" />
              <SH label="RSI" col="rsi" />
              <th className="px-3 py-2 w-16"></th>
            </tr>
          </thead>
          <tbody>
            {filtered.map(r => (
              <tr key={r.symbol} className="group cursor-pointer" onClick={() => openChartModal(r.symbol)}>
                <td className="px-3 py-2">
                  <div className="text-[12px] font-semibold text-text-bright">{r.symbol}</div>
                  <div className="text-[10px] text-text-muted truncate max-w-[120px]">{r.name}</div>
                </td>
                <td className="px-3 py-2 text-right text-[12px] font-mono font-bold text-text-bright">{fmtNum(r.ltp)}</td>
                <td className={cn('px-3 py-2 text-right text-[11px] font-mono', changeCls(r.change))}>
                  {r.change >= 0 ? '+' : ''}{fmtNum(r.change)}
                </td>
                <td className={cn('px-3 py-2 text-right text-[12px] font-mono font-semibold', changeCls(r.changePct))}>
                  {r.changePct >= 0 ? '+' : ''}{r.changePct.toFixed(2)}%
                </td>
                <td className="px-3 py-2 text-right text-[11px] font-mono text-text-sec">{fmtVol(r.volume)}</td>
                <td className="px-3 py-2 text-right text-[11px] font-mono text-text-sec">
                  {r.marketCap ? fmtINRCompact(r.marketCap * 100) : '—'}
                </td>
                <td className="px-3 py-2 text-right text-[11px] font-mono text-text-sec">
                  {r.pe ? r.pe.toFixed(1) : '—'}
                </td>
                <td className="px-3 py-2 text-right text-[11px] font-mono text-profit">{r.high52w ? fmtNum(r.high52w) : '—'}</td>
                <td className="px-3 py-2 text-right text-[11px] font-mono text-loss">{r.low52w ? fmtNum(r.low52w) : '—'}</td>
                <td className={cn('px-3 py-2 text-right text-[11px] font-mono font-semibold',
                  r.rsi && r.rsi > 70 ? 'text-loss' : r.rsi && r.rsi < 30 ? 'text-profit' : 'text-text-sec')}>
                  {r.rsi ? r.rsi.toFixed(1) : '—'}
                </td>
                <td className="px-3 py-2">
                  <button
                    onClick={e => { e.stopPropagation(); openOrderModal(r.symbol) }}
                    className="btn-buy btn-xs opacity-0 group-hover:opacity-100 transition-opacity"
                  >
                    Buy
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
