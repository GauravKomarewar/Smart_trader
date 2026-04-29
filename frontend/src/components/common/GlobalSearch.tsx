import { useState, useEffect, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { useUIStore, useWatchlistStore } from '../../stores'
import { useInstrumentSearch } from '../../hooks'
import { cn } from '../../lib/utils'
import { Search, X, TrendingUp, List, ChevronRight, Zap } from 'lucide-react'

const RECENT_KEY = 'st:recent_search'

function loadRecent(): string[] {
  try { return JSON.parse(localStorage.getItem(RECENT_KEY) ?? '[]') } catch { return [] }
}
function saveRecent(sym: string) {
  const r = [sym, ...loadRecent().filter(x => x !== sym)].slice(0, 8)
  localStorage.setItem(RECENT_KEY, JSON.stringify(r))
}

function isDerivativeType(type?: string) {
  return ['OPT', 'FUT', 'CE', 'PE'].includes(String(type || '').toUpperCase())
}

export default function GlobalSearch() {
  const { searchOpen, setSearchOpen, openOrderModal } = useUIStore()
  const { activeId, addItem } = useWatchlistStore()
  const [query, setQuery] = useState('')
  const [recent, setRecent] = useState<string[]>([])
  const inputRef = useRef<HTMLInputElement>(null)
  const { results, loading } = useInstrumentSearch(query)
  const navigate = useNavigate()

  useEffect(() => {
    if (searchOpen) {
      setRecent(loadRecent())
      setTimeout(() => inputRef.current?.focus(), 50)
    } else {
      setQuery('')
    }
  }, [searchOpen])

  if (!searchOpen) return null

  const handleSelect = (item: any) => {
    // brokerSym = broker trading symbol (for WS/API), smartName = smart_trader_name (for display)
    const brokerSym = item.trading_symbol || item.tradingsymbol || item.symbol
    const smartName = item.normalized_trading_symbol || item.trading_symbol || item.symbol
    saveRecent(smartName)
    setRecent(loadRecent())
    navigate('/app/watchlist')
    addItem(activeId, { symbol: brokerSym, tradingsymbol: smartName, exchange: item.exchange, type: item.type })
    setSearchOpen(false)
  }

  return (
    <div
      className="fixed inset-0 z-[130] flex items-start justify-center pt-[15vh] bg-black/60 backdrop-blur-sm"
      onClick={e => { if (e.target === e.currentTarget) setSearchOpen(false) }}
    >
      <div className="w-[580px] bg-bg-card border border-border rounded-2xl shadow-modal overflow-hidden">
        {/* Input */}
        <div className="flex items-center gap-3 px-4 py-3.5 border-b border-border">
          <Search className="w-4 h-4 text-text-muted shrink-0" />
          <input
            ref={inputRef}
            value={query}
            onChange={e => setQuery(e.target.value)}
            onKeyDown={e => e.key === 'Escape' && setSearchOpen(false)}
            className="flex-1 bg-transparent outline-none text-[14px] text-text-bright placeholder:text-text-muted"
            placeholder="Search instruments, stocks, F&O…"
          />
          {query && (
            <button onClick={() => setQuery('')} className="text-text-muted hover:text-text-sec">
              <X className="w-4 h-4" />
            </button>
          )}
        </div>

        {/* Results */}
        <div className="max-h-[380px] overflow-y-auto">
          {query.length < 2 ? (
            <>
              {recent.length > 0 && (
                <div className="px-4 pt-3 pb-1">
                  <div className="text-[10px] font-semibold text-text-muted uppercase tracking-wider mb-2">Recent</div>
                  <div className="flex flex-wrap gap-1.5">
                    {recent.map(r => (
                      <button
                        key={r}
                        onClick={() => { saveRecent(r); navigate('/app/watchlist') ; setSearchOpen(false) }}
                        className="px-2.5 py-1 bg-bg-elevated border border-border rounded text-[11px] text-text-sec hover:text-brand hover:border-brand/50 transition-colors"
                      >
                        {r}
                      </button>
                    ))}
                  </div>
                </div>
              )}
              <div className="px-4 pt-3 pb-3">
                <div className="text-[10px] font-semibold text-text-muted uppercase tracking-wider mb-2">Quick Actions</div>
                {[
                  { icon: TrendingUp, label: 'Open Place Order', action: () => { setSearchOpen(false); openOrderModal() } },
                  { icon: List, label: 'Go to Watchlist', action: () => { navigate('/app/watchlist'); setSearchOpen(false) } },
                  { icon: Zap, label: 'Option Chain', action: () => { navigate('/app/option-chain'); setSearchOpen(false) } },
                ].map(a => (
                  <button
                    key={a.label}
                    onClick={a.action}
                    className="w-full flex items-center gap-3 px-3 py-2 rounded-lg hover:bg-bg-hover text-left transition-colors"
                  >
                    <a.icon className="w-3.5 h-3.5 text-brand" />
                    <span className="text-[12px] text-text-sec">{a.label}</span>
                    <ChevronRight className="w-3 h-3 text-text-muted ml-auto" />
                  </button>
                ))}
              </div>
            </>
          ) : loading ? (
            <div className="py-8 text-center text-[12px] text-text-muted">Searching…</div>
          ) : results.length === 0 ? (
            <div className="py-8 text-center text-[12px] text-text-muted">No results for "{query}"</div>
          ) : (
            <div className="py-2">
              {results.map((r: any, i) => (
                <button
                  key={i}
                  onClick={() => handleSelect(r)}
                  className="w-full flex items-center gap-3 px-4 py-2.5 hover:bg-bg-hover transition-colors text-left"
                >
                  <div className="w-8 h-8 rounded-lg bg-brand/10 flex items-center justify-center shrink-0">
                    <TrendingUp className="w-3.5 h-3.5 text-brand" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="text-[13px] font-semibold text-text-bright">{r.trading_symbol || r.tradingsymbol || r.symbol}</div>
                                        <div className="text-[13px] font-semibold text-text-bright">{r.normalized_trading_symbol || r.trading_symbol || r.tradingsymbol || r.symbol}</div>
                    <div className="text-[10px] text-text-muted">{r.name || r.symbol} · {r.exchange} · {r.type}{r.lot_size > 1 ? ` · Lot: ${r.lot_size}` : ''}</div>
                  </div>
                  <div className="flex items-center gap-1.5 text-[10px] text-text-muted">
                    <span className="badge badge-blue px-1.5 py-0.5">{r.exchange}</span>
                  </div>
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Footer hint */}
        <div className="flex items-center gap-4 px-4 py-2.5 border-t border-border text-[10px] text-text-muted bg-bg-surface">
          <span><span className="kbd">↑↓</span> navigate</span>
          <span><span className="kbd">Enter</span> add to watchlist</span>
          <span><span className="kbd">Esc</span> close</span>
        </div>
      </div>
    </div>
  )
}
