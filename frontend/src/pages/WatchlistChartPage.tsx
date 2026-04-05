/* ════════════════════════════════════════════
   Watchlist & Chart Page
   Split layout: watchlist left, chart right
   ════════════════════════════════════════════ */
import { useState, useEffect, useRef, useCallback } from 'react'
import { createChart, CandlestickSeries, HistogramSeries, type IChartApi, type ISeriesApi, type UTCTimestamp } from 'lightweight-charts'
import { useWatchlistStore, useToastStore, useUIStore } from '../stores'
import { useInstrumentSearch, useKeyboard } from '../hooks'
import { cn, fmtNum, changeCls, fmtVol } from '../lib/utils'
import { api } from '../lib/api'
import {
  Search, Plus, X, BarChart2, BookOpen, ChevronDown,
  TrendingUp, TrendingDown, Trash2, MoreHorizontal,
  Maximize2, List, PlusCircle,
} from 'lucide-react'
import type { WatchlistItem, ChartInterval } from '../types'

// ── Live quote cache (fetched from API) ──────────────────
const _quoteCache: Record<string, { ltp: number; changePct: number; change: number; volume: number }> = {}

function getCachedQuote(symbol: string) {
  return _quoteCache[symbol] ?? { ltp: 0, changePct: 0, change: 0, volume: 0 }
}

async function fetchQuote(symbol: string) {
  try {
    const res = await api.get(`/market/quote/${encodeURIComponent(symbol)}`) as any
    if (res && res.ltp) {
      _quoteCache[symbol] = { ltp: res.ltp, changePct: res.changePct ?? 0, change: res.change ?? 0, volume: res.volume ?? 0 }
    }
  } catch { /* silent */ }
  return getCachedQuote(symbol)
}

const CHART_INTERVALS: ChartInterval[] = ['1m','3m','5m','15m','30m','1h','4h','D','W']

export default function WatchlistChartPage() {
  const { watchlists, activeId, setActive, addWatchlist } = useWatchlistStore()
  const activeWatchlist = watchlists.find(w => w.id === activeId) ?? watchlists[0]
  const [selectedSymbol, setSelectedSymbol] = useState<string | null>(
    activeWatchlist?.items[0]?.symbol ?? null
  )
  // Mobile view toggle
  const [mobileView, setMobileView] = useState<'list' | 'chart'>('list')

  useKeyboard('ctrl+w', () => {}) // placeholder

  return (
    <div className="h-full flex flex-col overflow-hidden">
      {/* Mobile view toggle bar */}
      <div className="flex sm:hidden items-center gap-1 px-3 py-2 bg-bg-surface border-b border-border shrink-0">
        <button
          onClick={() => setMobileView('list')}
          className={cn(
            'flex items-center gap-1.5 px-3 py-1.5 rounded text-[12px] font-medium transition-colors',
            mobileView === 'list' ? 'bg-brand/15 text-brand' : 'text-text-sec hover:text-text-bright'
          )}
        >
          <List className="w-3.5 h-3.5" /> Watchlist
        </button>
        <button
          onClick={() => { setMobileView('chart'); }}
          className={cn(
            'flex items-center gap-1.5 px-3 py-1.5 rounded text-[12px] font-medium transition-colors',
            mobileView === 'chart' ? 'bg-brand/15 text-brand' : 'text-text-sec hover:text-text-bright'
          )}
        >
          <BarChart2 className="w-3.5 h-3.5" /> Chart
          {selectedSymbol && <span className="ml-1 text-[10px] text-text-muted">{selectedSymbol}</span>}
        </button>
      </div>

      {/* Main split layout */}
      <div className="flex-1 flex overflow-hidden">
        {/* Left: Watchlist panel — always visible on sm+, toggleable on mobile */}
        <div className={cn(
          'w-full sm:w-[280px] sm:shrink-0 sm:border-r sm:border-border sm:flex-col sm:bg-bg-surface',
          'sm:flex',
          mobileView === 'list' ? 'flex flex-col bg-bg-surface' : 'hidden sm:flex'
        )}>
          <WatchlistPanel
            selected={selectedSymbol}
            onSelect={(s) => { setSelectedSymbol(s); setMobileView('chart') }}
          />
        </div>

        {/* Right: Chart + order panel */}
        <div className={cn(
          'flex-1 flex flex-col overflow-hidden',
          mobileView === 'chart' ? 'flex' : 'hidden sm:flex'
        )}>
          {selectedSymbol ? (
            <ChartPanel symbol={selectedSymbol} />
          ) : (
            <div className="flex-1 flex items-center justify-center text-text-muted text-sm">
              <div className="text-center space-y-2">
                <BarChart2 className="w-12 h-12 mx-auto opacity-20" />
                <p>Select a symbol from the watchlist</p>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

// ── Watchlist Panel ───────────────────────────────
function WatchlistPanel({ selected, onSelect }: {
  selected: string | null
  onSelect: (s: string) => void
}) {
  const { watchlists, activeId, setActive, addWatchlist, addItem, removeItem } = useWatchlistStore()
  const activeWL = watchlists.find(w => w.id === activeId) ?? watchlists[0]
  const [search, setSearch] = useState('')
  const [showSearch, setShowSearch] = useState(false)
  const [newWLName, setNewWLName] = useState('')
  const [showNewWL, setShowNewWL] = useState(false)
  const searchRef = useRef<HTMLInputElement>(null)
  const { results, loading } = useInstrumentSearch(search)

  useEffect(() => {
    if (showSearch) searchRef.current?.focus()
  }, [showSearch])

  // Keyboard: '/' to open search
  useKeyboard('/', () => setShowSearch(true))

  return (
    <>
      {/* Header */}
      <div className="px-3 py-2.5 border-b border-border space-y-2">
        {/* Watchlist tabs */}
        <div className="flex items-center gap-1 overflow-x-auto pb-0.5">
          {watchlists.map(wl => (
            <button
              key={wl.id}
              onClick={() => setActive(wl.id)}
              className={cn(
                'shrink-0 px-2.5 py-1 rounded text-[11px] font-medium transition-colors whitespace-nowrap',
                wl.id === activeId ? 'bg-brand/15 text-brand' : 'text-text-muted hover:text-text-sec'
              )}
            >
              {wl.name}
            </button>
          ))}
          <button
            onClick={() => setShowNewWL(!showNewWL)}
            className="shrink-0 text-text-muted hover:text-brand transition-colors p-1"
            title="New watchlist"
          >
            <PlusCircle className="w-3.5 h-3.5" />
          </button>
        </div>

        {showNewWL && (
          <div className="flex gap-1">
            <input
              autoFocus
              value={newWLName}
              onChange={e => setNewWLName(e.target.value)}
              onKeyDown={e => {
                if (e.key === 'Enter' && newWLName.trim()) {
                  addWatchlist(newWLName.trim()); setNewWLName(''); setShowNewWL(false)
                }
                if (e.key === 'Escape') setShowNewWL(false)
              }}
              className="input-base text-[11px] py-1 flex-1"
              placeholder="Watchlist name…"
            />
          </div>
        )}

        {/* Search bar */}
        <div className="relative">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-text-muted" />
          <input
            ref={searchRef}
            value={search}
            onChange={e => setSearch(e.target.value)}
            onFocus={() => setShowSearch(true)}
            onBlur={() => setTimeout(() => { setShowSearch(false); setSearch('') }, 200)}
            className="input-base w-full pl-8 py-1.5 text-[12px]"
            placeholder="Search & add (press /)"
          />
          {search && (
            <button onClick={() => setSearch('')} className="absolute right-2.5 top-1/2 -translate-y-1/2 text-text-muted">
              <X className="w-3.5 h-3.5" />
            </button>
          )}
        </div>

        {/* Search results dropdown */}
        {showSearch && search.length >= 2 && (
          <div className="absolute left-3 right-3 top-[calc(100%+2px)] z-50 bg-bg-elevated border border-border rounded-lg shadow-modal overflow-hidden">
            {loading ? (
              <div className="p-3 text-[11px] text-text-muted text-center">Searching…</div>
            ) : results.length === 0 ? (
              <div className="p-3 text-[11px] text-text-muted text-center">No results</div>
            ) : (
              <div className="max-h-60 overflow-y-auto">
                {results.map((r: any, i) => (
                  <button
                    key={i}
                    onMouseDown={() => {
                      addItem(activeId, { symbol: r.symbol, tradingsymbol: r.tradingsymbol, exchange: r.exchange, type: r.type })
                      setSearch('')
                    }}
                    className="w-full flex items-center gap-3 px-3 py-2 hover:bg-bg-hover text-left transition-colors"
                  >
                    <div className="flex-1">
                      <div className="text-[12px] font-medium text-text-bright">{r.symbol}</div>
                      <div className="text-[10px] text-text-muted">{r.exchange} · {r.type}</div>
                    </div>
                    <Plus className="w-3.5 h-3.5 text-brand" />
                  </button>
                ))}
              </div>
            )}
          </div>
        )}
      </div>

      {/* Items list */}
      <div className="flex-1 overflow-y-auto relative">
        {!activeWL?.items.length ? (
          <div className="flex flex-col items-center justify-center h-32 text-text-muted text-[11px] gap-2">
            <List className="w-6 h-6 opacity-30" />
            <p>Search to add symbols</p>
          </div>
        ) : (
          activeWL.items.map(item => (
            <WatchlistRow
              key={item.id}
              item={item}
              isSelected={selected === item.symbol}
              onSelect={() => onSelect(item.symbol)}
              onRemove={() => removeItem(activeId, item.symbol)}
            />
          ))
        )}
      </div>
    </>
  )
}

function WatchlistRow({ item, isSelected, onSelect, onRemove }: {
  item: WatchlistItem
  isSelected: boolean
  onSelect: () => void
  onRemove: () => void
}) {
  const [quote, setQuote] = useState(getCachedQuote(item.symbol))
  const { openOrderModal } = useUIStore()
  const [hover, setHover] = useState(false)

  useEffect(() => {
    fetchQuote(item.symbol).then(setQuote)
    const t = setInterval(() => fetchQuote(item.symbol).then(setQuote), 5000)
    return () => clearInterval(t)
  }, [item.symbol])

  return (
    <div
      className={cn(
        'flex items-center px-3 py-2.5 border-b border-border/40 cursor-pointer group transition-colors',
        isSelected ? 'bg-brand/8 border-l-2 border-l-brand' : 'hover:bg-bg-hover'
      )}
      onClick={onSelect}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
    >
      <div className="flex-1 min-w-0">
        <div className="text-[12px] font-semibold text-text-bright truncate">{item.symbol}</div>
        <div className="text-[10px] text-text-muted">{item.exchange}</div>
      </div>
      <div className="text-right">
        <div className="text-[13px] font-mono font-bold text-text-bright">{fmtNum(quote.ltp)}</div>
        <div className={cn('text-[11px] font-mono', changeCls(quote.changePct))}>
          {quote.changePct >= 0 ? '+' : ''}{quote.changePct.toFixed(2)}%
        </div>
      </div>
      {/* Hover actions */}
      <div className={cn('flex items-center gap-0.5 ml-2 transition-opacity', hover ? 'opacity-100' : 'opacity-0')}>
        <button
          onClick={e => { e.stopPropagation(); openOrderModal(item.symbol, item.exchange) }}
          className="btn-buy btn-xs !px-1.5 !py-0.5"
          title="Buy"
        >B</button>
        <button
          onClick={e => { e.stopPropagation(); openOrderModal(item.symbol, item.exchange) }}
          className="btn-sell btn-xs !px-1.5 !py-0.5"
          title="Sell"
        >S</button>
        <button
          onClick={e => { e.stopPropagation(); onRemove() }}
          className="btn-ghost btn-xs !px-1 !py-0.5 hover:text-loss"
          title="Remove"
        >
          <X className="w-3 h-3" />
        </button>
      </div>
    </div>
  )
}

// ── Chart Panel ───────────────────────────────────
function ChartPanel({ symbol }: { symbol: string }) {
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const seriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null)
  const volSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null)
  const [interval, setInterval] = useState<ChartInterval>('5m')
  const { openOrderModal, openChartModal } = useUIStore()

  const [quote, setQuote] = useState(getCachedQuote(symbol))

  useEffect(() => {
    fetchQuote(symbol).then(setQuote)
    const t = window.setInterval(() => fetchQuote(symbol).then(setQuote), 5000)
    return () => clearInterval(t)
  }, [symbol])

  useEffect(() => {
    if (!containerRef.current) return

    const chart = createChart(containerRef.current, {
      layout: {
        background: { color: '#161b28' },
        textColor: '#7b8398',
      },
      grid: {
        vertLines: { color: '#1c2133' },
        horzLines: { color: '#1c2133' },
      },
      crosshair: { mode: 1 },
      rightPriceScale: { borderColor: '#252b3b' },
      timeScale: { borderColor: '#252b3b', timeVisible: true },
      handleScroll: { mouseWheel: true, pressedMouseMove: true },
      handleScale: { mouseWheel: true, pinch: true },
    })
    chartRef.current = chart

    const series = chart.addSeries(CandlestickSeries, {
      upColor: '#22c55e',
      downColor: '#f43f5e',
      borderUpColor: '#22c55e',
      borderDownColor: '#f43f5e',
      wickUpColor: '#22c55e',
      wickDownColor: '#f43f5e',
    })
    seriesRef.current = series

    const volSeries = chart.addSeries(HistogramSeries, {
      color: '#22d3ee',
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
    })
    chart.priceScale('volume').applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
    })
    volSeriesRef.current = volSeries

    // No candle data available — chart will be empty until live data source is connected
    // Show a single point at current LTP if available
    if (quote.ltp > 0) {
      const now = Math.floor(Date.now() / 1000) as UTCTimestamp
      series.setData([{ time: now, open: quote.ltp, high: quote.ltp, low: quote.ltp, close: quote.ltp }])
    }
    chart.timeScale().fitContent()

    const ro = new ResizeObserver(() => {
      if (containerRef.current) {
        chart.resize(containerRef.current.clientWidth, containerRef.current.clientHeight)
      }
    })
    ro.observe(containerRef.current)

    return () => {
      ro.disconnect()
      chart.remove()
      chartRef.current = null
    }
  }, [symbol, interval])

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* Chart toolbar */}
      <div className="flex items-center gap-3 px-4 py-2.5 border-b border-border bg-bg-surface shrink-0 flex-wrap">
        <span className="text-[13px] font-bold text-text-bright">{symbol}</span>
        <div className={cn('text-[13px] font-mono font-bold', changeCls(quote.changePct))}>
          {fmtNum(quote.ltp)}
          <span className="text-[11px] ml-2">
            {quote.changePct >= 0 ? '+' : ''}{quote.changePct.toFixed(2)}%
          </span>
        </div>

        <div className="flex-1" />

        {/* Interval selector */}
        <div className="flex items-center gap-0.5 bg-bg-elevated border border-border rounded overflow-hidden">
          {CHART_INTERVALS.map(ivl => (
            <button
              key={ivl}
              onClick={() => setInterval(ivl)}
              className={cn(
                'px-2 py-1 text-[10px] font-medium transition-colors',
                interval === ivl ? 'bg-brand text-bg-base' : 'text-text-muted hover:text-text-sec'
              )}
            >
              {ivl}
            </button>
          ))}
        </div>

        {/* Actions */}
        <button onClick={() => openOrderModal(symbol)} className="btn-buy btn-sm">
          <TrendingUp className="w-3.5 h-3.5" /> Buy
        </button>
        <button onClick={() => openOrderModal(symbol)} className="btn-sell btn-sm">
          <TrendingDown className="w-3.5 h-3.5" /> Sell
        </button>
        <button
          onClick={() => openChartModal(symbol)}
          className="btn-ghost btn-xs"
          title="Open fullscreen chart"
        >
          <Maximize2 className="w-3.5 h-3.5" />
        </button>
      </div>

      {/* Chart */}
      <div ref={containerRef} className="flex-1" />
    </div>
  )
}
