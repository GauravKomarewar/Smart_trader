/* ════════════════════════════════════════════
   Place Order Modal — Multi-Broker Edition
   Full green/red BUY/SELL backgrounds,
   multi-broker account selection, keyboard nav
   ════════════════════════════════════════════ */
import { useState, useEffect, useRef } from 'react'
import { useUIStore, useAuthStore, useDashboardStore, useToastStore } from '../../stores'
import { useInstrumentSearch } from '../../hooks'
import { cn, fmtINR, fmtNum } from '../../lib/utils'
import { api } from '../../lib/api'
import { X, Search, AlertTriangle, CheckCircle, ChevronDown, WalletCards, Check } from 'lucide-react'
import type { PlaceOrderForm, Exchange } from '../../types'

type OrderType     = 'MARKET' | 'LIMIT' | 'SL' | 'SL-M'
type ProductType   = 'MIS' | 'NRML' | 'CNC'
type Validity      = 'DAY' | 'IOC'
type ExecutionType = 'ENTRY' | 'EXIT' | 'ADJUSTMENT'

const ORDER_TYPES: OrderType[]       = ['MARKET', 'LIMIT', 'SL', 'SL-M']
const PRODUCT_TYPES: ProductType[]   = ['MIS', 'NRML', 'CNC']
const VALIDITIES: Validity[]         = ['DAY', 'IOC']
const EXECUTION_TYPES: ExecutionType[] = ['ENTRY', 'EXIT', 'ADJUSTMENT']

const PRODUCT_DESC: Record<ProductType, string> = {
  MIS: 'Intraday — sq-off by 3:20 PM',
  NRML: 'Carry forward overnight',
  CNC: 'Equity delivery',
}

const EXEC_DESC: Record<ExecutionType, string> = {
  ENTRY:      'Opening a new position',
  EXIT:       'Closing an existing position',
  ADJUSTMENT: 'Modifying / rolling a position',
}

const EXEC_COLOR: Record<ExecutionType, string> = {
  ENTRY:      'text-profit',
  EXIT:       'text-loss',
  ADJUSTMENT: 'text-brand',
}

export default function PlaceOrderModal() {
  const { orderModalOpen, orderModalSymbol, orderModalExchange, closeOrderModal } = useUIStore()
  const { accounts, activeAccountId } = useAuthStore()
  const { data: dashData } = useDashboardStore()
  const toast = useToastStore(s => s.toast)
  const inputRef = useRef<HTMLInputElement>(null)

  const [side, setSide]               = useState<'BUY' | 'SELL'>('BUY')
  const [symbol, setSymbol]           = useState('')
  const [exchange, setExchange]       = useState<Exchange>('NSE')
  const [searchQ, setSearchQ]         = useState('')
  const [showSearch, setShowSearch]   = useState(false)
  const [orderType, setOrderType]     = useState<OrderType>('MARKET')
  const [product, setProduct]         = useState<ProductType>('MIS')
  const [validity, setValidity]       = useState<Validity>('DAY')
  const [execType, setExecType]       = useState<ExecutionType>('ENTRY')
  const [qty, setQty]                 = useState(1)
  const [price, setPrice]             = useState<number | ''>('')
  const [trigPrice, setTrigPrice]     = useState<number | ''>('')
  const [tag, setTag]                 = useState('')
  const [submitting, setSubmitting]   = useState(false)
  const [confirm, setConfirm]         = useState(false)

  // Multi-broker account selection
  const [selectedAccounts, setSelectedAccounts] = useState<Set<string>>(new Set())
  const [brokerAccounts, setBrokerAccounts]     = useState<any[]>([])

  const { results: searchResults, loading: searchLoading } = useInstrumentSearch(searchQ)

  // Load live broker accounts for selection
  useEffect(() => {
    if (!orderModalOpen) return
    const load = async () => {
      try {
        const res = await api.brokerAccounts()
        const live = (res.accounts ?? []).filter((a: any) => a.is_live)
        setBrokerAccounts(live)
        // Default: select active account or all live accounts
        if (live.length > 0) {
          if (activeAccountId && live.some((a: any) => a.config_id === activeAccountId)) {
            setSelectedAccounts(new Set([activeAccountId]))
          } else {
            setSelectedAccounts(new Set(live.map((a: any) => a.config_id)))
          }
        }
      } catch { setBrokerAccounts([]) }
    }
    load()
  }, [orderModalOpen])

  // Sync symbol when modal opens with a symbol
  useEffect(() => {
    if (orderModalOpen) {
      setSymbol(orderModalSymbol ?? '')
      setExchange((orderModalExchange as Exchange) || 'NSE')
      setSearchQ('')
      setShowSearch(!orderModalSymbol)
      setConfirm(false)
      setSide('BUY')
      setOrderType('MARKET')
      setProduct('MIS')
      setValidity('DAY')
      setExecType('ENTRY')
      setQty(1)
      setPrice('')
      setTrigPrice('')
      setTag('')
      setTimeout(() => inputRef.current?.focus(), 50)
    }
  }, [orderModalOpen, orderModalSymbol])

  // Keyboard shortcuts inside modal
  useEffect(() => {
    if (!orderModalOpen) return
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') { closeOrderModal(); return }
      if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) { handleSubmit(); return }
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLSelectElement) return
      if (e.key === 'b' || e.key === 'B') setSide('BUY')
      if (e.key === 's' || e.key === 'S') setSide('SELL')
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [orderModalOpen, qty, price, trigPrice, symbol])

  // Account helpers
  const toggleAccount = (id: string) => {
    setSelectedAccounts(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }
  const selectAll = () => setSelectedAccounts(new Set(brokerAccounts.map(a => a.config_id)))
  const allSelected = brokerAccounts.length > 0 && selectedAccounts.size === brokerAccounts.length
  const totalAvailMargin = brokerAccounts
    .filter(a => selectedAccounts.has(a.config_id))
    .reduce((s, a) => s + (a.available_margin ?? 0), 0)

  const needsPrice    = orderType === 'LIMIT' || orderType === 'SL'
  const needsTrigger  = orderType === 'SL' || orderType === 'SL-M'

  const estValue = qty * (typeof price === 'number' ? price : 0)
  const reqMargin = estValue > 0 ? (product === 'CNC' ? estValue : estValue * 0.2) : 0

  const handleSubmit = async () => {
    if (!symbol) { toast('Select a symbol', 'warning'); return }
    if (needsPrice && !price) { toast('Enter price', 'warning'); return }
    if (needsTrigger && !trigPrice) { toast('Enter trigger price', 'warning'); return }
    if (qty < 1) { toast('Quantity must be ≥ 1', 'warning'); return }
    if (selectedAccounts.size === 0) { toast('Select at least one broker account', 'warning'); return }

    if (!confirm) { setConfirm(true); return }

    setSubmitting(true)
    const accountIds = Array.from(selectedAccounts)
    
    try {
      // Single unified call — backend routes to all selected accounts
      const result: any = await api.placeOrder({
        accountIds,
        symbol,
        exchange,
        transactionType: side === 'BUY' ? 'B' : 'S',
        productType:     product,
        orderType:       orderType === 'MARKET' ? 'MKT' : orderType === 'LIMIT' ? 'LMT' : orderType === 'SL' ? 'SL-LMT' : 'SL-MKT',
        quantity:        qty,
        price:           typeof price === 'number' ? price : 0,
        triggerPrice:    typeof trigPrice === 'number' ? trigPrice : 0,
        validity,
        tag:             [execType, tag].filter(Boolean).join(':') || 'SmartTrader',
        executionType:   execType,
        strategyName:    'manual',
      })

      if (result.success) {
        const orderCount = result.order_count ?? result.orders?.length ?? 0
        toast(
          `${execType} ${side} ${qty}×${symbol} — ${orderCount} order(s) through ${accountIds.length} account(s)`,
          'success'
        )
        closeOrderModal()
      } else {
        const errors = result.errors?.join('; ') ?? 'Order failed'
        toast(`Failed: ${errors}`, 'error')
      }
    } catch (err: any) {
      toast(`Order failed: ${err?.message ?? 'Unknown error'}`, 'error')
    }

    setSubmitting(false)
    setConfirm(false)
  }

  if (!orderModalOpen) return null

  const isBuy = side === 'BUY'

  return (
    <div
      className="fixed inset-0 z-[100] flex items-center justify-center bg-black/60 backdrop-blur-sm"
      onClick={e => { if (e.target === e.currentTarget) closeOrderModal() }}
    >
      <div className={cn(
        'w-full max-w-[520px] mx-4 rounded-2xl shadow-modal overflow-hidden border transition-colors',
        isBuy
          ? 'bg-[#062e1a] border-[#16a34a]/40'
          : 'bg-[#2e0610] border-[#ef4444]/40'
      )}>
        {/* Header — full color */}
        <div className={cn(
          'flex items-center justify-between px-5 py-3',
          isBuy ? 'bg-[#16a34a]' : 'bg-[#ef4444]'
        )}>
          {/* BUY / SELL toggle */}
          <div className="flex gap-1 bg-black/20 rounded-lg p-0.5">
            <button
              onClick={() => { setSide('BUY'); setConfirm(false) }}
              className={cn(
                'px-5 py-1.5 rounded text-[13px] font-bold transition-all',
                isBuy ? 'bg-white text-[#16a34a] shadow' : 'text-white/70 hover:text-white'
              )}
            >
              BUY <span className="text-[9px] font-normal ml-1 opacity-70">B</span>
            </button>
            <button
              onClick={() => { setSide('SELL'); setConfirm(false) }}
              className={cn(
                'px-5 py-1.5 rounded text-[13px] font-bold transition-all',
                !isBuy ? 'bg-white text-[#ef4444] shadow' : 'text-white/70 hover:text-white'
              )}
            >
              SELL <span className="text-[9px] font-normal ml-1 opacity-70">S</span>
            </button>
          </div>
          <button onClick={() => closeOrderModal()} className="text-white/80 hover:text-white p-1 rounded hover:bg-white/10">
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Body */}
        <div className="p-5 space-y-4">

          {/* ── Broker Account Selector ── */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <label className="text-[11px] font-semibold text-white/60 uppercase tracking-wide">Broker Accounts</label>
              <button
                onClick={allSelected ? () => setSelectedAccounts(new Set()) : selectAll}
                className={cn(
                  'text-[10px] font-semibold px-2 py-0.5 rounded border transition-all',
                  allSelected
                    ? 'text-white/90 border-white/20 bg-white/10'
                    : isBuy
                      ? 'text-[#4ade80] border-[#4ade80]/30 bg-[#4ade80]/10 hover:bg-[#4ade80]/20'
                      : 'text-[#fca5a5] border-[#fca5a5]/30 bg-[#fca5a5]/10 hover:bg-[#fca5a5]/20'
                )}
              >
                {allSelected ? 'Deselect All' : 'Select All'}
              </button>
            </div>
            <div className="flex gap-2 flex-wrap">
              {brokerAccounts.map(acc => {
                const sel = selectedAccounts.has(acc.config_id)
                return (
                  <button
                    key={acc.config_id}
                    onClick={() => toggleAccount(acc.config_id)}
                    className={cn(
                      'flex items-center gap-2 px-3 py-2 rounded-lg border text-[11px] font-medium transition-all',
                      sel
                        ? isBuy
                          ? 'border-[#4ade80]/60 bg-[#4ade80]/15 text-[#4ade80]'
                          : 'border-[#fca5a5]/60 bg-[#fca5a5]/15 text-[#fca5a5]'
                        : 'border-white/10 bg-white/5 text-white/40 hover:text-white/70 hover:border-white/20'
                    )}
                  >
                    <div className={cn(
                      'w-4 h-4 rounded border-2 flex items-center justify-center shrink-0 transition-all',
                      sel
                        ? isBuy ? 'border-[#4ade80] bg-[#4ade80]' : 'border-[#fca5a5] bg-[#fca5a5]'
                        : 'border-white/30'
                    )}>
                      {sel && <Check className="w-3 h-3 text-black" />}
                    </div>
                    <WalletCards className="w-3 h-3" />
                    <span className="font-bold">{acc.client_id}</span>
                    <span className="text-[9px] opacity-60">{acc.broker_name}</span>
                  </button>
                )
              })}
              {brokerAccounts.length === 0 && (
                <span className="text-[11px] text-white/30">No live broker accounts</span>
              )}
            </div>
          </div>

          {/* ── Execution type strip ── */}
          <div>
            <label className="text-[11px] font-semibold text-white/60 uppercase tracking-wide mb-1 block">Execution Type</label>
            <div className="flex gap-1.5">
              {EXECUTION_TYPES.map(et => (
                <button
                  key={et}
                  onClick={() => setExecType(et)}
                  className={cn(
                    'flex-1 py-1.5 rounded text-[11px] font-semibold border transition-all',
                    execType === et
                      ? cn(
                          'border-white/30',
                          et === 'ENTRY' ? 'bg-[#4ade80]/20 text-[#4ade80]' : et === 'EXIT' ? 'bg-[#fca5a5]/20 text-[#fca5a5]' : 'bg-[#60a5fa]/20 text-[#60a5fa]'
                        )
                      : 'border-white/10 text-white/40 hover:text-white/60 bg-white/5'
                  )}
                >
                  {et}
                </button>
              ))}
            </div>
            <p className="text-[10px] text-white/30 mt-1">{EXEC_DESC[execType]}</p>
          </div>

          {/* ── Symbol search ── */}
          <div>
            <label className="text-[11px] font-semibold text-white/60 uppercase tracking-wide">Symbol</label>
            {symbol && !showSearch ? (
              <div className="flex items-center justify-between bg-white/5 border border-white/10 rounded-lg px-3 py-2 mt-1">
                <div>
                  <span className="text-[13px] font-bold text-white">{symbol}</span>
                  <span className="text-[10px] text-white/40 ml-2">{exchange}</span>
                </div>
                <div className="flex items-center gap-3">
                  <span className="text-[12px] font-mono text-white/80">{typeof price === 'number' ? fmtNum(price) : '—'}</span>
                  <button onClick={() => { setShowSearch(true); setSearchQ(symbol) }} className={cn('text-[10px] font-semibold hover:underline', isBuy ? 'text-[#4ade80]' : 'text-[#fca5a5]')}>
                    Change
                  </button>
                </div>
              </div>
            ) : (
              <div className="relative mt-1">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-white/30" />
                <input
                  ref={inputRef}
                  value={searchQ}
                  onChange={e => setSearchQ(e.target.value)}
                  className="w-full bg-white/5 border border-white/10 rounded-lg text-white text-[13px] pl-9 pr-3 py-2 placeholder:text-white/20 focus:outline-none focus:border-white/30"
                  placeholder="Search symbol…"
                  autoComplete="off"
                />
                {searchQ.length >= 2 && (
                  <div className="absolute top-full left-0 right-0 mt-1 bg-[#1a1a2e] border border-white/10 rounded-xl shadow-modal z-10 max-h-48 overflow-y-auto">
                    {searchLoading ? (
                      <div className="py-3 text-center text-[11px] text-white/30">Loading…</div>
                    ) : searchResults.length === 0 ? (
                      <div className="py-3 text-center text-[11px] text-white/30">No results</div>
                    ) : searchResults.map((r: any, i) => (
                      <button
                        key={i}
                        onClick={() => {
                          // For F&O instruments, use trading_symbol; for equity/index use symbol
                          const sym = (r.type === 'FUT' || r.type === 'OPT')
                            ? (r.trading_symbol || r.tradingsymbol || r.symbol)
                            : r.symbol
                          setSymbol(sym)
                          setExchange(r.exchange ?? 'NSE')
                          setSearchQ('')
                          setShowSearch(false)
                        }}
                        className="w-full flex items-center justify-between px-3 py-2 hover:bg-white/5 text-left"
                      >
                        <div>
                          <div className="text-[12px] font-semibold text-white">{r.trading_symbol || r.tradingsymbol || r.symbol}</div>
                          <div className="text-[10px] text-white/40">{r.exchange} · {r.type}{r.lot_size > 1 ? ` · Lot: ${r.lot_size}` : ''}</div>
                        </div>
                        <span className="text-[9px] font-bold text-white/50 bg-white/10 px-2 py-0.5 rounded">{r.exchange}</span>
                      </button>
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>

          {/* ── Order type + Product + Validity row ── */}
          <div className="grid grid-cols-3 gap-3">
            <div>
              <label className="text-[11px] font-semibold text-white/60 uppercase tracking-wide">Order Type</label>
              <select value={orderType} onChange={e => setOrderType(e.target.value as OrderType)}
                className="w-full bg-white/5 border border-white/10 rounded-lg text-white text-[12px] px-3 py-2 mt-1 focus:outline-none">
                {ORDER_TYPES.map(o => <option key={o} value={o} className="bg-[#1a1a2e]">{o}</option>)}
              </select>
            </div>
            <div>
              <label className="text-[11px] font-semibold text-white/60 uppercase tracking-wide">Product</label>
              <select value={product} onChange={e => setProduct(e.target.value as ProductType)}
                className="w-full bg-white/5 border border-white/10 rounded-lg text-white text-[12px] px-3 py-2 mt-1 focus:outline-none">
                {PRODUCT_TYPES.map(p => <option key={p} value={p} className="bg-[#1a1a2e]">{p}</option>)}
              </select>
            </div>
            <div>
              <label className="text-[11px] font-semibold text-white/60 uppercase tracking-wide">Validity</label>
              <select value={validity} onChange={e => setValidity(e.target.value as Validity)}
                className="w-full bg-white/5 border border-white/10 rounded-lg text-white text-[12px] px-3 py-2 mt-1 focus:outline-none">
                {VALIDITIES.map(v => <option key={v} value={v} className="bg-[#1a1a2e]">{v}</option>)}
              </select>
            </div>
          </div>
          <p className="text-[10px] text-white/30 -mt-1">{PRODUCT_DESC[product]}</p>

          {/* ── Qty + Price + Trigger row ── */}
          <div className={cn('grid gap-3', needsPrice && needsTrigger ? 'grid-cols-3' : needsPrice || needsTrigger ? 'grid-cols-2' : 'grid-cols-1')}>
            <div>
              <label className="text-[11px] font-semibold text-white/60 uppercase tracking-wide">Quantity</label>
              <input type="number" min={1} value={qty} onChange={e => setQty(Math.max(1, parseInt(e.target.value) || 1))}
                className="w-full bg-white/5 border border-white/10 rounded-lg text-white text-[13px] font-mono px-3 py-2 mt-1 focus:outline-none focus:border-white/30" />
            </div>
            {needsPrice && (
              <div>
                <label className="text-[11px] font-semibold text-white/60 uppercase tracking-wide">Price</label>
                <input type="number" min={0} step={0.05} value={price} onChange={e => setPrice(e.target.value === '' ? '' : +e.target.value)}
                  className="w-full bg-white/5 border border-white/10 rounded-lg text-white text-[13px] font-mono px-3 py-2 mt-1 focus:outline-none focus:border-white/30"
                  placeholder="0.00" />
              </div>
            )}
            {needsTrigger && (
              <div>
                <label className="text-[11px] font-semibold text-white/60 uppercase tracking-wide">Trigger Price</label>
                <input type="number" min={0} step={0.05} value={trigPrice} onChange={e => setTrigPrice(e.target.value === '' ? '' : +e.target.value)}
                  className="w-full bg-white/5 border border-white/10 rounded-lg text-white text-[13px] font-mono px-3 py-2 mt-1 focus:outline-none focus:border-white/30"
                  placeholder="0.00" />
              </div>
            )}
          </div>

          {/* ── Margin estimate ── */}
          <div className={cn(
            'flex items-start gap-2 rounded-lg px-3 py-2.5 border text-[11px]',
            'bg-white/5 border-white/10'
          )}>
            <CheckCircle className={cn('w-4 h-4 mt-0.5 shrink-0', isBuy ? 'text-[#4ade80]' : 'text-[#fca5a5]')} />
            <div>
              <div className="text-white/60">
                Est. Value: <span className="font-mono font-semibold text-white">{fmtINR(estValue)}</span>
                &nbsp;·&nbsp; Margin: <span className="font-mono font-semibold text-white">{fmtINR(reqMargin)}</span>
              </div>
              <div className="text-white/40">
                Available ({selectedAccounts.size} acct{selectedAccounts.size !== 1 ? 's' : ''}): <span className="font-mono text-white/70">{fmtINR(totalAvailMargin)}</span>
              </div>
            </div>
          </div>

          {/* ── Tag ── */}
          <div>
            <label className="text-[11px] font-semibold text-white/60 uppercase tracking-wide">Tag (optional)</label>
            <input value={tag} onChange={e => setTag(e.target.value)}
              className="w-full bg-white/5 border border-white/10 rounded-lg text-white text-[12px] px-3 py-2 mt-1 focus:outline-none focus:border-white/30"
              placeholder="e.g. scalp, hedge, webhook …" />
          </div>
        </div>

        {/* Footer — inherits BUY/SELL theme */}
        <div className={cn(
          'flex items-center justify-between px-5 py-3.5 border-t',
          isBuy ? 'border-[#16a34a]/30 bg-[#16a34a]/10' : 'border-[#ef4444]/30 bg-[#ef4444]/10'
        )}>
          <div className="text-[10px] text-white/30">
            <span className="inline-block bg-white/10 text-white/50 px-1.5 py-0.5 rounded text-[9px] font-mono mr-1">Ctrl+Enter</span>Submit &nbsp;
            <span className="inline-block bg-white/10 text-white/50 px-1.5 py-0.5 rounded text-[9px] font-mono mr-1">Esc</span>Cancel
          </div>
          <div className="flex gap-2">
            <button onClick={() => closeOrderModal()} className="px-3 py-1.5 rounded text-[12px] text-white/50 hover:text-white/80 hover:bg-white/5 transition-colors">
              Cancel
            </button>
            {confirm ? (
              <button
                onClick={handleSubmit}
                disabled={submitting}
                className={cn(
                  'px-5 py-1.5 rounded-lg text-[13px] font-bold text-white shadow-lg transition-all',
                  isBuy ? 'bg-[#16a34a] hover:bg-[#15803d] shadow-[#16a34a]/30' : 'bg-[#ef4444] hover:bg-[#dc2626] shadow-[#ef4444]/30',
                  submitting && 'opacity-60'
                )}
              >
                {submitting ? '…' : `CONFIRM ${execType} ${side} → ${selectedAccounts.size} acct${selectedAccounts.size !== 1 ? 's' : ''}`}
              </button>
            ) : (
              <button
                onClick={handleSubmit}
                className={cn(
                  'px-5 py-1.5 rounded-lg text-[13px] font-bold text-white shadow-lg transition-all',
                  isBuy ? 'bg-[#16a34a] hover:bg-[#15803d] shadow-[#16a34a]/30' : 'bg-[#ef4444] hover:bg-[#dc2626] shadow-[#ef4444]/30'
                )}
              >
                {side} {qty > 0 ? `× ${qty}` : ''}
                <span className="ml-1.5 text-[10px] opacity-70">{execType} · {selectedAccounts.size} acct{selectedAccounts.size !== 1 ? 's' : ''}</span>
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
