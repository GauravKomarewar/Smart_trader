/* ════════════════════════════════════════════
   Watchlist & Chart Page
   Split layout: watchlist left, depth middle, chart right
   ════════════════════════════════════════════ */
import { useState, useEffect, useRef, useCallback } from 'react'
import {
  createChart, CandlestickSeries, HistogramSeries, LineSeries, AreaSeries, BarSeries,
  LineStyle,
  type IChartApi, type ISeriesApi, type UTCTimestamp,
} from 'lightweight-charts'
import { useWatchlistStore, useUIStore, useMarketDepthStore } from '../stores'
import { useInstrumentSearch, useKeyboard } from '../hooks'
import { cn, fmtNum, changeCls } from '../lib/utils'
import { api } from '../lib/api'
import { ws, marketWs, type MarketTick } from '../lib/ws'
import {
  computeSMA, computeEMA, computeBB, computeRSI, computeMACD,
  computeZigZag, computeUniversalLevels, computeHASmooth, computeSRBoxes,
  computeVWAP, computeSupertrend, computePSAR, computeATR, computePivots,
  computeHeikinAshi,
  type Candle,
} from '../lib/chartIndicators'
import {
  Search, Plus, X, BarChart2,
  TrendingUp, TrendingDown, Maximize2, List, PlusCircle,
  CandlestickChart, Volume2, BookOpen, Minus, RotateCcw, Ruler, Hexagon,
} from 'lucide-react'
import type { WatchlistItem, ChartInterval } from '../types'

// ── Live quote cache (from /ws/market) ───────────────
const _liveQuotes: Record<string, { ltp: number; changePct: number; change: number; volume: number }> = {}

function quoteKey(symbol: string, exchange: string): string {
  return `${exchange}:${normSym(symbol)}`
}

/** Normalize a symbol for comparison: strip exchange prefix, suffixes, and spaces. */
function normSym(s: string): string {
  const stripped = s.toUpperCase().split(':').pop() ?? s.toUpperCase()
  return stripped
    .replace(/-INDEX$/, '')
    .replace(/-EQ$/, '')
    .replace(/-BE$/, '')
    .replace(/\s+/g, '')
}

/** Detect exchange from symbol pattern. */
function detectExchange(sym: string): string {
  const upper = sym.toUpperCase().replace(/\s+/g, '').split(':').pop() ?? ''
  // MCX commodities
  const MCX_ROOTS = ['CRUDEOIL','CRUDEOILM','GOLD','GOLDM','GOLDPETAL','SILVER','SILVERM','SILVERMIC','NATURALGAS','COPPER','ZINC','LEAD','NICKEL','ALUMINIUM','MENTHAOIL','COTTON']
  if (MCX_ROOTS.some(r => upper.startsWith(r) && (upper.length > r.length || upper === r))) return 'MCX'
  // CDS currencies
  if (/^(USDINR|EURINR|GBPINR|JPYINR)/.test(upper)) return 'CDS'
  // BFO (BSE F&O)
  if (/^(SENSEX|BANKEX)/.test(upper)) return 'BFO'
  // NFO derivatives
  if (/\d{3,}(CE|PE)/i.test(upper)) return 'NFO'
  if (/\d{2}[A-Z]{3}\d{2}[CP]\d+$/i.test(upper)) return 'NFO'
  if (/\d+FUT$/i.test(upper)) return 'NFO'
  return 'NSE'
}

function displaySymbol(item?: Pick<WatchlistItem, 'symbol' | 'tradingsymbol'> | null): string {
  return item?.tradingsymbol || item?.symbol || ''
}

function isDerivativeType(type?: string): boolean {
  return ['OPT', 'FUT', 'CE', 'PE'].includes(String(type || '').toUpperCase())
}

// Kick off connection once the module is loaded
marketWs.connect()

async function fetchRestQuote(symbol: string, exchange: string) {
  try {
    const res = await api.get(`/market/quote/${encodeURIComponent(symbol)}?exchange=${encodeURIComponent(exchange)}`) as any
    if (res && res.ltp) {
      _liveQuotes[quoteKey(symbol, exchange)] = {
        ltp: res.ltp, changePct: res.changePct ?? 0,
        change: res.change ?? 0, volume: res.volume ?? 0,
      }
    }
  } catch { /* silent */ }
}

function getLiveQuote(symbol: string, exchange: string) {
  return _liveQuotes[quoteKey(symbol, exchange)] ?? { ltp: 0, changePct: 0, change: 0, volume: 0 }
}

const CHART_INTERVALS: ChartInterval[] = ['1m','3m','5m','15m','30m','1h','4h','D','W']

// ── Indicator config (same as fullscreen ChartModal) ──
type ChartType = 'candlestick' | 'heikinashi' | 'line' | 'area' | 'bar'
const CLASSIC_IND = ['MA(9)', 'MA(21)', 'EMA(50)', 'BB(20)', 'VWAP', 'RSI(14)', 'MACD'] as const
const ADVANCED_IND = ['Supertrend', 'PSAR', 'Pivots', 'ATR', 'ZigZag', 'Levels', 'HA Smooth', 'S/R Zones'] as const
const ALL_IND = [...CLASSIC_IND, ...ADVANCED_IND]
const IND_COLORS: Record<string, string> = {
  'MA(9)': '#f59e0b', 'MA(21)': '#3b82f6', 'EMA(50)': '#a855f7',
  'BB-U': 'rgba(147,197,253,.45)', 'BB-M': 'rgba(147,197,253,.65)', 'BB-L': 'rgba(147,197,253,.45)',
  'VWAP': '#f97316', 'MACD': '#22d3ee', 'MACD-S': '#f97316', 'RSI': '#facc15',
  'ST-Up': '#22c55e', 'ST-Dn': '#f43f5e', 'PSAR': '#fbbf24', 'ATR': '#f472b6',
  'Pivot': '#94a3b8', 'R1': '#f43f5e', 'R2': 'rgba(244,63,94,.5)', 'S1': '#22c55e', 'S2': 'rgba(34,197,94,.5)',
  'ZigZag': '#e879f9', 'HASmooth': '#6ee7b7',
}

// ── Drawing tool types ───────────────────────────
type DrawTool = 'none' | 'hline' | 'trendline' | 'fib'
interface DrawClick { time: number; price: number }

export default function WatchlistChartPage() {
  const { watchlists, activeId } = useWatchlistStore()
  const activeWatchlist = watchlists.find(w => w.id === activeId) ?? watchlists[0]
  const [selectedItemId, setSelectedItemId] = useState<string | null>(
    activeWatchlist?.items[0]?.id ?? null
  )
  const selectedItem = activeWatchlist?.items.find(i => i.id === selectedItemId) ?? activeWatchlist?.items[0] ?? null
  const chartSymbol = selectedItem ? displaySymbol(selectedItem) : null
  const chartExchange = selectedItem?.exchange || (chartSymbol ? detectExchange(chartSymbol) : 'NSE')
  // Mobile view toggle
  const [mobileView, setMobileView] = useState<'list' | 'chart'>('list')
  const [showDepth, setShowDepth] = useState(true)

  useKeyboard('ctrl+w', () => {}) // placeholder

  useEffect(() => {
    if (!activeWatchlist?.items?.length) {
      setSelectedItemId(null)
      return
    }
    if (!selectedItemId || !activeWatchlist.items.some(i => i.id === selectedItemId)) {
      setSelectedItemId(activeWatchlist.items[0].id)
    }
  }, [activeWatchlist, selectedItemId])

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
          {chartSymbol && <span className="ml-1 text-[10px] text-text-muted">{chartSymbol}</span>}
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
            selectedId={selectedItemId}
            onSelect={(itemId) => { setSelectedItemId(itemId); setMobileView('chart') }}
          />
        </div>

        {/* Right: Market Depth + Chart */}
        <div className={cn(
          'flex-1 flex overflow-hidden',
          mobileView === 'chart' ? 'flex' : 'hidden sm:flex'
        )}>
          {/* Market Depth Panel — visible on all screen sizes when toggled */}
          {showDepth && chartSymbol && (
            <div className="w-[200px] lg:w-[240px] shrink-0 border-r border-border bg-bg-surface overflow-y-auto">
              <MarketDepthPanel symbol={chartSymbol} exchange={chartExchange} />
            </div>
          )}

          {/* Chart Panel */}
          <div className="flex-1 flex flex-col overflow-hidden">
            {chartSymbol ? (
              <ChartPanel symbol={chartSymbol} exchange={chartExchange} showDepth={showDepth} onToggleDepth={() => setShowDepth(d => !d)} />
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
    </div>
  )
}

// ── Watchlist Panel ───────────────────────────────
function WatchlistPanel({ selectedId, onSelect }: {
  selectedId: string | null
  onSelect: (itemId: string) => void
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

  // Subscribe all watchlist symbols to MarketWS when the watchlist loads
  useEffect(() => {
    const items = activeWL?.items ?? []
    if (items.length) {
      marketWs.subscribe(items.map(i => i.tradingsymbol || i.symbol))
    }
  }, [activeWL?.items])

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

        {/* Search bar + results */}
        <div className="relative">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-text-muted z-10" />
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
            <button onClick={() => setSearch('')} className="absolute right-2.5 top-1/2 -translate-y-1/2 text-text-muted z-10">
              <X className="w-3.5 h-3.5" />
            </button>
          )}

          {/* Search results dropdown */}
          {showSearch && search.length >= 2 && (
            <div className="absolute left-0 right-0 top-full mt-1 z-50 bg-bg-elevated border border-border rounded-lg shadow-modal overflow-hidden">
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
                        const tsym = r.trading_symbol || r.tradingsymbol || r.symbol
                        const watchSymbol = isDerivativeType(r.type) ? tsym : (r.symbol || tsym)
                        addItem(activeId, { symbol: watchSymbol, tradingsymbol: tsym, exchange: r.exchange, type: r.type })
                        setSearch('')
                      }}
                      className="w-full flex items-center gap-3 px-3 py-2 hover:bg-bg-hover text-left transition-colors"
                    >
                      <div className="flex-1">
                        <div className="text-[12px] font-medium text-text-bright">{r.trading_symbol || r.tradingsymbol || r.symbol}</div>
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
              isSelected={selectedId === item.id}
              onSelect={() => onSelect(item.id)}
              onRemove={() => removeItem(activeId, item.id)}
            />
          ))
        )}
      </div>
    </>
  )
}


// ── Watchlist Row ─────────────────────────────────
function WatchlistRow({ item, isSelected, onSelect, onRemove }: {
  item: WatchlistItem
  isSelected: boolean
  onSelect: () => void
  onRemove: () => void
}) {
  const { openOrderModal } = useUIStore()
  const tickSym = displaySymbol(item)
  const [quote, setQuote] = useState(getLiveQuote(tickSym, item.exchange))
  const [hover, setHover] = useState(false)

  useEffect(() => {
    fetchRestQuote(tickSym, item.exchange).then(() => setQuote(getLiveQuote(tickSym, item.exchange)))
    marketWs.subscribe([tickSym])
    return marketWs.onTick((tick) => {
      if (normSym(tick.symbol) === normSym(tickSym)) {
        const q = { ltp: tick.ltp, changePct: tick.changePct ?? 0, change: tick.change ?? 0, volume: tick.volume ?? 0 }
        _liveQuotes[quoteKey(tickSym, item.exchange)] = q
        setQuote(q)
      }
    })
  }, [tickSym, item.exchange])

  return (
    <div
      className={cn(
        'flex items-center gap-2 px-3 py-2 cursor-pointer transition-colors border-b border-border/40',
        'hover:bg-bg-hover',
        isSelected && 'bg-brand/10 border-l-2 border-l-brand'
      )}
      onClick={onSelect}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
    >
      <div className="flex-1 min-w-0">
        <div className="text-[12px] font-semibold text-text-bright truncate">{displaySymbol(item)}</div>
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
          onClick={e => { e.stopPropagation(); openOrderModal(displaySymbol(item), item.exchange) }}
          className="btn-buy btn-xs !px-1.5 !py-0.5"
          title="Buy"
        >B</button>
        <button
          onClick={e => { e.stopPropagation(); openOrderModal(displaySymbol(item), item.exchange) }}
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

// ── Market Depth Panel ────────────────────────────
function MarketDepthPanel({ symbol, exchange }: { symbol: string; exchange: string }) {
  const [depth, setDepth] = useState<{ bids: any[]; asks: any[]; total_buy_qty: number; total_sell_qty: number } | null>(null)

  // WS-fed market depth (primary)
  const wsDepth = useMarketDepthStore(s => s.data)
  const wsLastUpdate = useMarketDepthStore(s => s.lastUpdate)

  // Subscribe to WS depth feed
  useEffect(() => {
    if (ws.isOpen) ws.subscribeMarketDepth(symbol)
    return () => { ws.unsubscribeMarketDepth() }
  }, [symbol])

  // Sync from WS store
  useEffect(() => {
    if (wsDepth && wsLastUpdate > 0) {
      setDepth(wsDepth as any)
    }
  }, [wsDepth, wsLastUpdate])

  useEffect(() => {
    let mounted = true
    const fetchDepth = () => {
      // Skip REST if WS pushed recently (< 6s)
      const lastWs = useMarketDepthStore.getState().lastUpdate
      if (lastWs && Date.now() - lastWs < 6_000) return
      api.marketDepth(symbol, exchange)
        .then(d => { if (mounted) setDepth(d as any) })
        .catch(() => {})
    }
    fetchDepth()
    const iv = window.setInterval(fetchDepth, 10_000)  // REST fallback at 10s — WS is primary (~2s)
    return () => { mounted = false; clearInterval(iv) }
  }, [symbol, exchange])

  if (!depth) return (
    <div className="flex items-center justify-center h-full text-text-muted text-[11px]">Loading depth…</div>
  )

  const bids = depth.bids?.slice(0, 5) ?? []
  const asks = depth.asks?.slice(0, 5) ?? []

  // When no depth data available, show a helpful message with LTP from tick
  if (!bids.length && !asks.length) {
    const liveQ = getLiveQuote(symbol, exchange)
    return (
      <div className="p-2 space-y-2">
        <div className="text-[11px] font-semibold text-text-bright px-1 flex items-center gap-1.5">
          <BookOpen className="w-3.5 h-3.5 text-brand" /> Market Depth
        </div>
        {liveQ.ltp > 0 && (
          <div className="text-center py-2">
            <div className="text-[10px] text-text-muted">LTP</div>
            <div className="text-[16px] font-mono font-bold text-text-bright">{fmtNum(liveQ.ltp)}</div>
            <div className={cn('text-[11px] font-mono', changeCls(liveQ.changePct))}>
              {liveQ.changePct >= 0 ? '+' : ''}{liveQ.changePct.toFixed(2)}%
            </div>
          </div>
        )}
        <div className="text-center text-[10px] text-text-muted py-4">
          Depth data unavailable
          <div className="text-[9px] mt-1 opacity-60">Market may be closed or broker not connected</div>
        </div>
      </div>
    )
  }

  const maxQty = Math.max(
    ...bids.map((b: any) => b.qty || 0),
    ...asks.map((a: any) => a.qty || 0),
    1
  )

  return (
    <div className="p-2 space-y-2">
      <div className="text-[11px] font-semibold text-text-bright px-1 flex items-center gap-1.5">
        <BookOpen className="w-3.5 h-3.5 text-brand" /> Market Depth
      </div>

      {/* Asks (sell) — reversed so best ask is at bottom */}
      <div className="space-y-0.5">
        <div className="grid grid-cols-3 text-[9px] text-text-muted font-medium px-1 pb-0.5">
          <span>Orders</span><span className="text-center">Price</span><span className="text-right">Qty</span>
        </div>
        {[...asks].reverse().map((a: any, i: number) => (
          <div key={`a${i}`} className="relative grid grid-cols-3 text-[11px] font-mono py-0.5 px-1">
            <div className="absolute inset-0 bg-loss/10 rounded-sm" style={{ width: `${(a.qty / maxQty) * 100}%`, right: 0, left: 'auto' }} />
            <span className="relative text-text-muted">{a.orders || '-'}</span>
            <span className="relative text-center text-loss font-medium">{fmtNum(a.price)}</span>
            <span className="relative text-right">{(a.qty || 0).toLocaleString()}</span>
          </div>
        ))}
      </div>

      {/* Spread indicator */}
      {bids.length > 0 && asks.length > 0 && (
        <div className="text-center text-[10px] text-text-muted py-1 border-y border-border/40">
          Spread: <span className="text-text-bright font-mono">{(asks[0]?.price - bids[0]?.price).toFixed(2)}</span>
        </div>
      )}

      {/* Bids (buy) */}
      <div className="space-y-0.5">
        {bids.map((b: any, i: number) => (
          <div key={`b${i}`} className="relative grid grid-cols-3 text-[11px] font-mono py-0.5 px-1">
            <div className="absolute inset-0 bg-profit/10 rounded-sm" style={{ width: `${(b.qty / maxQty) * 100}%` }} />
            <span className="relative text-text-muted">{b.orders || '-'}</span>
            <span className="relative text-center text-profit font-medium">{fmtNum(b.price)}</span>
            <span className="relative text-right">{(b.qty || 0).toLocaleString()}</span>
          </div>
        ))}
      </div>

      {/* Total buy/sell qty */}
      <div className="flex items-center gap-1 pt-1 border-t border-border/40">
        <div className="flex-1">
          <div className="text-[9px] text-text-muted">Total Buy</div>
          <div className="text-[11px] font-mono text-profit">{(depth.total_buy_qty || 0).toLocaleString()}</div>
        </div>
        <div className="flex-1 text-right">
          <div className="text-[9px] text-text-muted">Total Sell</div>
          <div className="text-[11px] font-mono text-loss">{(depth.total_sell_qty || 0).toLocaleString()}</div>
        </div>
      </div>

      {/* Buy/Sell ratio bar */}
      {(depth.total_buy_qty > 0 || depth.total_sell_qty > 0) && (
        <div className="h-1.5 rounded-full overflow-hidden bg-bg-elevated flex">
          <div className="bg-profit/60 rounded-l-full" style={{ width: `${(depth.total_buy_qty / (depth.total_buy_qty + depth.total_sell_qty)) * 100}%` }} />
          <div className="bg-loss/60 flex-1 rounded-r-full" />
        </div>
      )}
    </div>
  )
}

// ── Enhanced Chart Panel with Full Indicators ─────
function ChartPanel({ symbol, exchange, showDepth, onToggleDepth }: {
  symbol: string; exchange: string; showDepth: boolean; onToggleDepth: () => void
}) {
  const containerRef = useRef<HTMLDivElement>(null)
  const subContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const mainSeriesRef = useRef<ISeriesApi<any> | null>(null)
  const [interval, setInterval] = useState<ChartInterval>('5m')
  const [chartType, setChartType] = useState<ChartType>('candlestick')
  const [showVolume, setShowVolume] = useState(true)
  const [activeIndicators, setActiveIndicators] = useState<string[]>(['MA(21)', 'VWAP', 'Supertrend'])
  const [crosshairData, setCrosshairData] = useState<{o:number;h:number;l:number;c:number}|null>(null)
  const { openOrderModal, openChartModal } = useUIStore()

  // Drawing tools state
  const [drawTool, setDrawTool] = useState<DrawTool>('none')
  const drawClicksRef = useRef<DrawClick[]>([])
  const [drawHint, setDrawHint] = useState('')

  const toggleInd = useCallback((ind: string) => {
    setActiveIndicators(a => a.includes(ind) ? a.filter(x => x !== ind) : [...a, ind])
  }, [])

  const [quote, setQuote] = useState(getLiveQuote(symbol, exchange))

  // Live tick subscription for quote header
  useEffect(() => {
    fetchRestQuote(symbol, exchange).then(() => setQuote(getLiveQuote(symbol, exchange)))
    marketWs.subscribe([symbol])
    return marketWs.onTick((tick: MarketTick) => {
      if (normSym(tick.symbol) === normSym(symbol)) {
        setQuote({ ltp: tick.ltp, changePct: tick.changePct ?? 0, change: tick.change ?? 0, volume: tick.volume ?? 0 })
      }
    })
  }, [symbol, exchange])

  // Reset draw tool when symbol changes
  useEffect(() => { setDrawTool('none'); drawClicksRef.current = []; setDrawHint('') }, [symbol])

  const needsSubChart = activeIndicators.includes('RSI(14)') || activeIndicators.includes('MACD') || activeIndicators.includes('ATR')

  // Zoom helpers
  const zoomIn = useCallback(() => {
    if (!chartRef.current) return
    const ts = chartRef.current.timeScale()
    const range = ts.getVisibleLogicalRange()
    if (range) { const mid = (range.from + range.to) / 2, span = (range.to - range.from) * 0.35; ts.setVisibleLogicalRange({ from: mid - span, to: mid + span }) }
  }, [])
  const zoomOut = useCallback(() => {
    if (!chartRef.current) return
    const ts = chartRef.current.timeScale()
    const range = ts.getVisibleLogicalRange()
    if (range) { const mid = (range.from + range.to) / 2, span = (range.to - range.from) * 0.75; ts.setVisibleLogicalRange({ from: mid - span, to: mid + span }) }
  }, [])
  const zoomFit = useCallback(() => { chartRef.current?.timeScale().fitContent() }, [])

  // Build chart with full indicator support
  useEffect(() => {
    if (!containerRef.current) return
    let cancelled = false
    const container = containerRef.current
    // Track resources created inside RAF for proper cleanup
    let _chart: IChartApi | null = null
    let _subChart: IChartApi | null = null
    let _unsubTick: (() => void) | null = null
    let _ro: ResizeObserver | null = null

    const rafId = requestAnimationFrame(() => {
      if (cancelled || !container.isConnected) return

      const chart = createChart(container, {
        autoSize: true,
        layout: { background: { color: '#0b0e17' }, textColor: '#7b8398', fontSize: 11, fontFamily: '-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif' },
        grid: { vertLines: { color: '#1c2133' }, horzLines: { color: '#1c2133' } },
        crosshair: { mode: 1, vertLine: { color: '#22d3ee', labelBackgroundColor: '#22d3ee' }, horzLine: { color: '#22d3ee', labelBackgroundColor: '#22d3ee' } },
        rightPriceScale: { borderColor: '#252b3b', scaleMargins: { top: 0.08, bottom: showVolume ? 0.18 : 0.05 } },
        timeScale: { borderColor: '#252b3b', timeVisible: true, secondsVisible: false, barSpacing: 8, rightOffset: 5 },
        handleScroll: { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true },
        handleScale: { mouseWheel: true, pinch: true, axisPressedMouseMove: true, axisDoubleClickReset: true },
      })
      _chart = chart
      chartRef.current = chart

      // Sub chart for RSI/MACD/ATR
      let subChart: IChartApi | null = null
      if (needsSubChart && subContainerRef.current) {
        subChart = createChart(subContainerRef.current, {
          autoSize: true,
          layout: { background: { color: '#0b0e17' }, textColor: '#7b8398', fontSize: 10 },
          grid: { vertLines: { color: '#1c2133' }, horzLines: { color: '#1c2133' } },
          rightPriceScale: { borderColor: '#252b3b' },
          timeScale: { borderColor: '#252b3b', timeVisible: true, visible: true },
        })
        _subChart = subChart
      }

      // Main series — support Heikin Ashi as chart type
      let mainSeries: ISeriesApi<any>
      if (chartType === 'candlestick' || chartType === 'heikinashi') {
        mainSeries = chart.addSeries(CandlestickSeries, { upColor: '#22c55e', downColor: '#f43f5e', borderUpColor: '#22c55e', borderDownColor: '#f43f5e', wickUpColor: '#22c55e', wickDownColor: '#f43f5e' })
      } else if (chartType === 'area') {
        mainSeries = chart.addSeries(AreaSeries, { lineColor: '#22d3ee', topColor: 'rgba(34,211,238,.25)', bottomColor: 'rgba(34,211,238,.02)', lineWidth: 2 })
      } else if (chartType === 'bar') {
        mainSeries = chart.addSeries(BarSeries, { upColor: '#22c55e', downColor: '#f43f5e' })
      } else {
        mainSeries = chart.addSeries(LineSeries, { color: '#22d3ee', lineWidth: 2 })
      }
      mainSeriesRef.current = mainSeries

      // Volume
      const volSeries = chart.addSeries(HistogramSeries, { color: '#22d3ee', priceFormat: { type: 'volume' }, priceScaleId: 'vol' })
      chart.priceScale('vol').applyOptions({ scaleMargins: { top: 0.85, bottom: 0 } })

      // Crosshair
      chart.subscribeCrosshairMove(param => {
        if (param.point) {
          const d = param.seriesData.get(mainSeries)
          if (d && 'open' in d) setCrosshairData({ o: (d as any).open, h: (d as any).high, l: (d as any).low, c: (d as any).close })
          else setCrosshairData(null)
        } else setCrosshairData(null)
      })

      // Chart click handler for drawing tools
      chart.subscribeClick(param => {
        if (!param?.point || !mainSeriesRef.current) return
        const price = mainSeriesRef.current.coordinateToPrice(param.point.y)
        const time = param.time as number
        if (price == null || isNaN(price as number)) return
        handleDrawClick(time, price as number)
      })

      // Sync timescales
      if (subChart) {
        chart.timeScale().subscribeVisibleLogicalRangeChange(range => { if (range) subChart!.timeScale().setVisibleLogicalRange(range) })
        subChart.timeScale().subscribeVisibleLogicalRangeChange(range => { if (range) chart.timeScale().setVisibleLogicalRange(range) })
      }

      // Fetch OHLCV & draw indicators
      api.marketOhlcv(symbol, interval, exchange || detectExchange(symbol), 1500)
        .then((resp: any) => {
          if (cancelled) return
          try {
          const rawCandles = resp?.candles ?? [] as Candle[]
          if (!rawCandles.length) return

          // Apply Heikin Ashi transform if needed
          const candles = chartType === 'heikinashi' ? computeHeikinAshi(rawCandles) : rawCandles

          if (chartType === 'area' || chartType === 'line') {
            mainSeries.setData(candles.map((c: Candle) => ({ time: c.time as UTCTimestamp, value: c.close })))
          } else {
            mainSeries.setData(candles.map((c: Candle) => ({ time: c.time as UTCTimestamp, open: c.open, high: c.high, low: c.low, close: c.close })))
          }

          if (showVolume) {
            volSeries.setData(candles.map((c: Candle) => ({ time: c.time as UTCTimestamp, value: c.volume ?? 0, color: c.close >= c.open ? 'rgba(34,197,94,.35)' : 'rgba(244,63,94,.35)' })))
          }

          // Use raw candles for indicator calculations (not HA-transformed)
          const indData = rawCandles

          // Classic Indicators
          const on = (k: string) => activeIndicators.includes(k)
          if (on('MA(9)'))  chart.addSeries(LineSeries, { color: IND_COLORS['MA(9)'],  lineWidth: 1, lastValueVisible: false, priceLineVisible: false }).setData(computeSMA(indData, 9).map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
          if (on('MA(21)')) chart.addSeries(LineSeries, { color: IND_COLORS['MA(21)'], lineWidth: 1, lastValueVisible: false, priceLineVisible: false }).setData(computeSMA(indData, 21).map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
          if (on('EMA(50)'))chart.addSeries(LineSeries, { color: IND_COLORS['EMA(50)'],lineWidth: 1, lastValueVisible: false, priceLineVisible: false }).setData(computeEMA(indData, 50).map(p => ({ time: p.time as UTCTimestamp, value: p.value })))

          if (on('BB(20)')) {
            const bb = computeBB(indData, 20)
            chart.addSeries(LineSeries, { color: IND_COLORS['BB-U'], lineWidth: 1, lineStyle: LineStyle.Dashed, lastValueVisible: false, priceLineVisible: false }).setData(bb.upper.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
            chart.addSeries(LineSeries, { color: IND_COLORS['BB-M'], lineWidth: 1, lineStyle: LineStyle.Dotted, lastValueVisible: false, priceLineVisible: false }).setData(bb.middle.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
            chart.addSeries(LineSeries, { color: IND_COLORS['BB-L'], lineWidth: 1, lineStyle: LineStyle.Dashed, lastValueVisible: false, priceLineVisible: false }).setData(bb.lower.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
          }

          // VWAP
          if (on('VWAP')) {
            chart.addSeries(LineSeries, { color: IND_COLORS['VWAP'], lineWidth: 2, lastValueVisible: true, priceLineVisible: false }).setData(computeVWAP(indData).map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
          }

          if (on('RSI(14)') && subChart) {
            const rsi = computeRSI(indData, 14)
            const rsiS = subChart.addSeries(LineSeries, { color: IND_COLORS['RSI'], lineWidth: 2, priceLineVisible: false, lastValueVisible: true })
            rsiS.setData(rsi.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
            rsiS.createPriceLine({ price: 70, color: 'rgba(244,63,94,.4)', lineWidth: 1, lineStyle: LineStyle.Dashed, axisLabelVisible: true, title: '70' })
            rsiS.createPriceLine({ price: 30, color: 'rgba(34,197,94,.4)', lineWidth: 1, lineStyle: LineStyle.Dashed, axisLabelVisible: true, title: '30' })
          }

          if (on('MACD') && subChart) {
            const macd = computeMACD(indData)
            subChart.addSeries(LineSeries, { color: IND_COLORS['MACD'], lineWidth: 2, lastValueVisible: false, priceLineVisible: false }).setData(macd.macd.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
            subChart.addSeries(LineSeries, { color: IND_COLORS['MACD-S'], lineWidth: 1, lastValueVisible: false, priceLineVisible: false }).setData(macd.signal.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
            subChart.addSeries(HistogramSeries, { lastValueVisible: false, priceLineVisible: false }).setData(macd.histogram.map(p => ({ time: p.time as UTCTimestamp, value: p.value, color: p.color })))
          }

          // Supertrend
          if (on('Supertrend')) {
            const st = computeSupertrend(indData, 10, 3)
            if (st.up.length) chart.addSeries(LineSeries, { color: IND_COLORS['ST-Up'], lineWidth: 2, lastValueVisible: false, priceLineVisible: false }).setData(st.up.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
            if (st.dn.length) chart.addSeries(LineSeries, { color: IND_COLORS['ST-Dn'], lineWidth: 2, lastValueVisible: false, priceLineVisible: false }).setData(st.dn.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
          }

          // Parabolic SAR
          if (on('PSAR')) {
            const psar = computePSAR(indData)
            if (psar.length) {
              chart.addSeries(LineSeries, { color: IND_COLORS['PSAR'], lineWidth: 0, pointMarkersVisible: true, lastValueVisible: false, priceLineVisible: false })
                .setData(psar.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
            }
          }

          // Pivot Points
          if (on('Pivots')) {
            const pvt = computePivots(indData)
            if (pvt.pivot.length) {
              chart.addSeries(LineSeries, { color: IND_COLORS['Pivot'], lineWidth: 1, lineStyle: LineStyle.Dotted, lastValueVisible: false, priceLineVisible: false }).setData(pvt.pivot.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
              chart.addSeries(LineSeries, { color: IND_COLORS['R1'], lineWidth: 1, lineStyle: LineStyle.Dotted, lastValueVisible: false, priceLineVisible: false }).setData(pvt.r1.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
              chart.addSeries(LineSeries, { color: IND_COLORS['S1'], lineWidth: 1, lineStyle: LineStyle.Dotted, lastValueVisible: false, priceLineVisible: false }).setData(pvt.s1.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
              chart.addSeries(LineSeries, { color: IND_COLORS['R2'], lineWidth: 1, lineStyle: LineStyle.Dotted, lastValueVisible: false, priceLineVisible: false }).setData(pvt.r2.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
              chart.addSeries(LineSeries, { color: IND_COLORS['S2'], lineWidth: 1, lineStyle: LineStyle.Dotted, lastValueVisible: false, priceLineVisible: false }).setData(pvt.s2.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
            }
          }

          // ATR in sub-chart
          if (on('ATR') && subChart) {
            const atr = computeATR(indData, 14)
            if (atr.length) subChart.addSeries(LineSeries, { color: IND_COLORS['ATR'], lineWidth: 2, priceLineVisible: false, lastValueVisible: true }).setData(atr.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
          }

          // Advanced Indicators
          if (on('ZigZag')) {
            const zz = computeZigZag(indData, 8)
            if (zz.length > 1) {
              chart.addSeries(LineSeries, { color: IND_COLORS['ZigZag'], lineWidth: 2, lastValueVisible: false, priceLineVisible: false, pointMarkersVisible: true })
                .setData(zz.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
            }
          }

          if (on('Levels')) {
            const levels = computeUniversalLevels(indData, 12)
            for (const lv of levels) {
              mainSeries.createPriceLine({ price: lv.value, color: 'rgba(251,191,36,.35)', lineWidth: 1, lineStyle: LineStyle.Dotted, axisLabelVisible: false, title: '' })
            }
          }

          if (on('HA Smooth')) {
            const ha = computeHASmooth(indData, 2)
            // Strength line with per-bar green/red coloring (Shoonya-style)
            const validStrength = ha.strength.filter(p => isFinite(p.value))
            const validRef = ha.reference.filter(p => isFinite(p.value))
            if (validStrength.length > 0) {
              chart.addSeries(LineSeries, { lineWidth: 2, lastValueVisible: true, priceLineVisible: false, color: '#00fc26' })
                .setData(validStrength.map(p => ({ time: p.time as UTCTimestamp, value: p.value, color: p.color })))
            }
            if (validRef.length > 0) {
              chart.addSeries(LineSeries, { color: '#9ca3af', lineWidth: 1, lineStyle: LineStyle.Dashed, lastValueVisible: false, priceLineVisible: false })
                .setData(validRef.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
            }
          }

          if (on('S/R Zones')) {
            const boxes = computeSRBoxes(indData, 15, 0.002)
            for (const box of boxes) {
              const alpha = Math.min(0.15 + box.strength * 0.03, 0.45)
              const color = box.type === 'resistance' ? `rgba(244,63,94,${alpha})` : `rgba(34,197,94,${alpha})`
              mainSeries.createPriceLine({ price: box.top, color, lineWidth: 1, lineStyle: LineStyle.Dotted, axisLabelVisible: false, title: '' })
              mainSeries.createPriceLine({ price: box.bottom, color, lineWidth: 1, lineStyle: LineStyle.Dotted, axisLabelVisible: false, title: '' })
              mainSeries.createPriceLine({ price: box.price, color, lineWidth: 2, lineStyle: LineStyle.Solid, axisLabelVisible: true, title: `${box.type === 'resistance' ? 'R' : 'S'} ${box.price.toFixed(1)}` })
            }
          }

          chart.timeScale().fitContent()
          subChart?.timeScale().fitContent()
          } catch (err) { console.error('Chart render error:', err) }
        })
        .catch(() => {
          // Seed from live quote fallback
          const q = getLiveQuote(symbol, exchange)
          if (q.ltp > 0) {
            const now = Math.floor(Date.now() / 60_000) * 60 as UTCTimestamp
            mainSeries.setData([{ time: now, open: q.ltp, high: q.ltp, low: q.ltp, close: q.ltp }])
            chart.timeScale().fitContent()
          }
        })

      // Live ticks
      const _TF_SEC: Record<string, number> = { '1m': 60, '3m': 180, '5m': 300, '15m': 900, '30m': 1800, '1h': 3600, '4h': 14400, 'D': 86400, 'W': 604800 }
      const barSec = _TF_SEC[interval] ?? 60
      let curBar: { time: number; open: number; high: number; low: number; close: number } | null = null

      const unsubTick = marketWs.onTick((tick: MarketTick) => {
        if (normSym(tick.symbol) !== normSym(symbol)) return
        const ltp = tick.ltp
        if (!ltp || !chart) return
        const barTime = Math.floor(Date.now() / 1000 / barSec) * barSec
        if (!curBar || curBar.time !== barTime) {
          curBar = { time: barTime, open: ltp, high: ltp, low: ltp, close: ltp }
        } else {
          curBar.high = Math.max(curBar.high, ltp)
          curBar.low = Math.min(curBar.low, ltp)
          curBar.close = ltp
        }
        try {
          if (chartType === 'area' || chartType === 'line') {
            mainSeries.update({ time: curBar.time as UTCTimestamp, value: curBar.close })
          } else {
            mainSeries.update({ time: curBar.time as UTCTimestamp, open: curBar.open, high: curBar.high, low: curBar.low, close: curBar.close })
          }
          if (showVolume) {
            volSeries.update({ time: curBar.time as UTCTimestamp, value: tick.volume ?? 0, color: curBar.close >= curBar.open ? 'rgba(34,197,94,.35)' : 'rgba(244,63,94,.35)' })
          }
        } catch { /* bar order */ }
      })
      _unsubTick = unsubTick

      const ro = new ResizeObserver(() => {
        if (containerRef.current) chart.resize(containerRef.current.clientWidth, containerRef.current.clientHeight)
        if (subContainerRef.current && subChart) subChart.resize(subContainerRef.current.clientWidth, subContainerRef.current.clientHeight)
      })
      _ro = ro
      ro.observe(container)
    }) // end raf

    return () => {
      cancelled = true
      cancelAnimationFrame(rafId)
      _unsubTick?.()
      _ro?.disconnect()
      if (_chart) { try { _chart.remove() } catch {} }
      if (_subChart) { try { _subChart.remove() } catch {} }
      chartRef.current = null
      mainSeriesRef.current = null
    }
  }, [symbol, exchange, interval, chartType, showVolume, activeIndicators])

  // Drawing click handler
  function handleDrawClick(time: number, price: number) {
    if (drawTool === 'none') return
    const chart = chartRef.current, ms = mainSeriesRef.current
    if (!chart || !ms) return

    if (drawTool === 'hline') {
      ms.createPriceLine({ price, color: '#2962ff', lineWidth: 1, lineStyle: LineStyle.Dashed, axisLabelVisible: true, title: fmtNum(price) })
      setDrawTool('none'); setDrawHint(''); return
    }
    if (drawTool === 'trendline') {
      drawClicksRef.current.push({ time, price })
      if (drawClicksRef.current.length === 1) { setDrawHint('Click point 2'); return }
      if (drawClicksRef.current.length >= 2) {
        const [p1, p2] = drawClicksRef.current
        const pts = p1.time < p2.time
          ? [{ time: p1.time as UTCTimestamp, value: p1.price }, { time: p2.time as UTCTimestamp, value: p2.price }]
          : [{ time: p2.time as UTCTimestamp, value: p2.price }, { time: p1.time as UTCTimestamp, value: p1.price }]
        chart.addSeries(LineSeries, { color: '#2962ff', lineWidth: 1.5, priceLineVisible: false, lastValueVisible: false }).setData(pts)
        drawClicksRef.current = []; setDrawTool('none'); setDrawHint(''); return
      }
    }
    if (drawTool === 'fib') {
      drawClicksRef.current.push({ time, price })
      if (drawClicksRef.current.length === 1) { setDrawHint('Click low/high point 2'); return }
      if (drawClicksRef.current.length >= 2) {
        const [p1, p2] = drawClicksRef.current
        const hi = Math.max(p1.price, p2.price), lo = Math.min(p1.price, p2.price), diff = hi - lo
        const levels = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1]
        const colors = ['#ef5350','#ff9800','#ffeb3b','#4caf50','#2196f3','#9c27b0','#ef5350']
        const labels = ['0%','23.6%','38.2%','50%','61.8%','78.6%','100%']
        levels.forEach((l, i) => {
          const p = hi - diff * l
          ms.createPriceLine({ price: p, color: colors[i], lineWidth: 1, lineStyle: LineStyle.Dashed, axisLabelVisible: true, title: `Fib ${labels[i]} ${fmtNum(p)}` })
        })
        drawClicksRef.current = []; setDrawTool('none'); setDrawHint(''); return
      }
    }
  }

  function activateDrawTool(tool: DrawTool) {
    drawClicksRef.current = []
    if (drawTool === tool) { setDrawTool('none'); setDrawHint(''); return }
    setDrawTool(tool)
    const hints: Record<DrawTool, string> = { none: '', hline: 'Click to place H-Line', trendline: 'Click point 1', fib: 'Click high/low point 1' }
    setDrawHint(hints[tool])
  }

  const CT_ICONS: [ChartType, string, any][] = [
    ['candlestick', 'Candles', CandlestickChart],
    ['heikinashi', 'HA', Hexagon],
    ['line', 'Line', TrendingUp],
    ['bar', 'Bar', BarChart2],
  ]

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* Toolbar row 1 — symbol, intervals, chart type, volume */}
      <div className="flex items-center gap-2 px-3 py-1.5 border-b border-border bg-bg-surface shrink-0 flex-wrap gap-y-1">
        <span className="text-[13px] font-bold text-text-bright">{symbol}</span>
        <div className={cn('text-[13px] font-mono font-bold', changeCls(quote.changePct))}>
          {fmtNum(quote.ltp)}
          <span className="text-[11px] ml-1.5">{quote.changePct >= 0 ? '+' : ''}{quote.changePct.toFixed(2)}%</span>
        </div>

        {/* Crosshair OHLC */}
        {crosshairData && (
          <div className="flex items-center gap-2 text-[10px] font-mono ml-2">
            <span>O <span className="text-text-bright">{crosshairData.o.toFixed(2)}</span></span>
            <span>H <span className="text-profit">{crosshairData.h.toFixed(2)}</span></span>
            <span>L <span className="text-loss">{crosshairData.l.toFixed(2)}</span></span>
            <span>C <span className="text-text-bright">{crosshairData.c.toFixed(2)}</span></span>
          </div>
        )}

        <div className="flex-1" />

        {/* Interval selector */}
        <div className="flex items-center gap-0.5 bg-bg-elevated border border-border rounded overflow-hidden">
          {CHART_INTERVALS.map(ivl => (
            <button key={ivl} onClick={() => setInterval(ivl)}
              className={cn('px-1.5 py-0.5 text-[10px] font-medium transition-colors', interval === ivl ? 'bg-brand text-bg-base' : 'text-text-muted hover:text-text-sec')}>
              {ivl}
            </button>
          ))}
        </div>

        {/* Chart type — including Heikin Ashi */}
        <div className="flex items-center gap-0.5">
          {CT_ICONS.map(([t, label, Icon]) => (
            <button key={t} onClick={() => setChartType(t)}
              className={cn('btn-ghost btn-xs gap-0.5', chartType === t && 'text-brand bg-brand/10')} title={label}>
              <Icon className="w-3 h-3" /> <span className="text-[9px]">{label}</span>
            </button>
          ))}
        </div>

        {/* Volume toggle */}
        <button onClick={() => setShowVolume(v => !v)} className={cn('btn-ghost btn-xs gap-0.5', showVolume && 'text-brand')}>
          <Volume2 className="w-3 h-3" /> Vol
        </button>

        {/* Depth toggle */}
        <button onClick={onToggleDepth} className={cn('btn-ghost btn-xs gap-0.5', showDepth && 'text-brand')}>
          <BookOpen className="w-3 h-3" /> <span className="hidden sm:inline">Depth</span>
        </button>

        {/* Drawing tools */}
        <div className="flex items-center gap-0.5 border-l border-border pl-2 ml-1">
          <button onClick={() => activateDrawTool('hline')} className={cn('btn-ghost btn-xs', drawTool === 'hline' && 'text-brand bg-brand/10')} title="H-Line">
            <Minus className="w-3 h-3" />
          </button>
          <button onClick={() => activateDrawTool('trendline')} className={cn('btn-ghost btn-xs', drawTool === 'trendline' && 'text-brand bg-brand/10')} title="Trend Line">
            <TrendingUp className="w-3 h-3" />
          </button>
          <button onClick={() => activateDrawTool('fib')} className={cn('btn-ghost btn-xs', drawTool === 'fib' && 'text-brand bg-brand/10')} title="Fibonacci">
            <Ruler className="w-3 h-3" />
          </button>
        </div>

        {/* Zoom */}
        <div className="flex items-center gap-0.5 border-l border-border pl-2 ml-1">
          <button onClick={zoomOut} className="btn-ghost btn-xs" title="Zoom out"><Minus className="w-3 h-3" /></button>
          <button onClick={zoomFit} className="btn-ghost btn-xs" title="Fit"><RotateCcw className="w-3 h-3" /></button>
          <button onClick={zoomIn} className="btn-ghost btn-xs" title="Zoom in"><Plus className="w-3 h-3" /></button>
        </div>

        {/* Actions */}
        <button onClick={() => openOrderModal(symbol, exchange)} className="btn-buy btn-xs">
          <TrendingUp className="w-3 h-3" /> Buy
        </button>
        <button onClick={() => openOrderModal(symbol, exchange)} className="btn-sell btn-xs">
          <TrendingDown className="w-3 h-3" /> Sell
        </button>
        <button onClick={() => openChartModal(symbol)} className="btn-ghost btn-xs" title="Fullscreen">
          <Maximize2 className="w-3 h-3" />
        </button>
      </div>

      {/* Toolbar row 2 — Indicators */}
      <div className="flex items-center gap-0.5 px-3 py-1 border-b border-border/60 bg-bg-surface/80 shrink-0 flex-wrap overflow-x-auto">
        <span className="text-[9px] text-text-muted font-medium mr-1">IND</span>
        {ALL_IND.map(ind => (
          <button key={ind} onClick={() => toggleInd(ind)}
            className={cn(
              'px-1.5 py-0.5 rounded text-[9px] font-medium transition-colors border whitespace-nowrap',
              activeIndicators.includes(ind)
                ? ADVANCED_IND.includes(ind as any) ? 'bg-fuchsia-500/20 text-fuchsia-400 border-fuchsia-500/40' : 'bg-accent/20 text-accent border-accent/40'
                : 'text-text-muted border-transparent hover:border-border'
            )}>
            {ind}
          </button>
        ))}
      </div>

      {/* Drawing hint */}
      {drawHint && (
        <div className="text-center text-[11px] text-brand bg-brand/10 py-1 shrink-0">{drawHint}</div>
      )}

      {/* Chart area */}
      <div className="flex-1 flex flex-col" style={{ cursor: drawTool !== 'none' ? 'crosshair' : undefined }}>
        <div ref={containerRef} className={needsSubChart ? 'flex-[3]' : 'flex-1'} />
        {needsSubChart && <div ref={subContainerRef} className="flex-1 border-t border-border min-h-[80px]" />}
      </div>
    </div>
  )
}
