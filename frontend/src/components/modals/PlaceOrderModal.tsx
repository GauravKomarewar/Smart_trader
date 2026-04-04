/* ════════════════════════════════════════════
   Place Order Modal
   Full-featured: symbol, BUY/SELL, order types,
   execution type, lot-size aware, keyboard nav
   ════════════════════════════════════════════ */
import { useState, useEffect, useRef } from 'react'
import { useUIStore, useAuthStore, useDashboardStore, useToastStore } from '../../stores'
import { useInstrumentSearch } from '../../hooks'
import { cn, fmtINR, fmtNum } from '../../lib/utils'
import { api } from '../../lib/api'
import { X, Search, AlertTriangle, CheckCircle, ChevronDown } from 'lucide-react'
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
  const { orderModalOpen, orderModalSymbol, closeOrderModal } = useUIStore()
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

  const { results: searchResults, loading: searchLoading } = useInstrumentSearch(searchQ)

  // Sync symbol when modal opens with a symbol
  useEffect(() => {
    if (orderModalOpen) {
      setSymbol(orderModalSymbol ?? '')
      setExchange('NSE')
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

  const activeAccount = accounts.find(a => a.id === activeAccountId)
  const availableMargin = activeAccount?.availableMargin ?? 0

  const needsPrice    = orderType === 'LIMIT' || orderType === 'SL'
  const needsTrigger  = orderType === 'SL' || orderType === 'SL-M'

  // Mock LTP lookup
  const mockLtp = symbol ? (17000 + Math.random() * 5000) : 0
  const estValue = qty * (typeof price === 'number' ? price : mockLtp)
  const reqMargin = product === 'CNC' ? estValue : estValue * 0.2
  const marginOk  = reqMargin <= availableMargin

  const handleSubmit = async () => {
    if (!symbol) { toast('Select a symbol', 'warning'); return }
    if (needsPrice && !price) { toast('Enter price', 'warning'); return }
    if (needsTrigger && !trigPrice) { toast('Enter trigger price', 'warning'); return }
    if (qty < 1) { toast('Quantity must be ≥ 1', 'warning'); return }

    if (!confirm) { setConfirm(true); return }

    setSubmitting(true)
    try {
      const form: PlaceOrderForm = {
        accountId: activeAccountId ?? '',
        symbol,
        exchange,
        side,
        orderType,
        product,
        qty,
        price: typeof price === 'number' ? price : undefined,
        triggerPrice: typeof trigPrice === 'number' ? trigPrice : undefined,
        validity,
        tag: [execType, tag].filter(Boolean).join(':') || undefined,
      }
      await api.placeOrder(form)
      toast(`${execType} order placed: ${side} ${qty} × ${symbol}`, 'success')
      closeOrderModal()
    } catch (err: any) {
      toast(err?.message ?? 'Order failed', 'error')
    } finally {
      setSubmitting(false)
      setConfirm(false)
    }
  }

  if (!orderModalOpen) return null

  return (
    <div
      className="fixed inset-0 z-[100] flex items-center justify-center bg-black/60 backdrop-blur-sm"
      onClick={e => { if (e.target === e.currentTarget) closeOrderModal() }}
    >
      <div className="w-full max-w-[480px] mx-4 bg-bg-card border border-border rounded-2xl shadow-modal overflow-hidden">
        {/* Header */}
        <div className={cn(
          'flex items-center justify-between px-5 py-3 border-b border-border',
          side === 'BUY' ? 'bg-profit/8' : 'bg-loss/8'
        )}>
          {/* BUY / SELL toggle */}
          <div className="flex gap-1 bg-bg-elevated rounded-lg p-0.5">
            <button
              onClick={() => setSide('BUY')}
              className={cn('px-4 py-1.5 rounded text-[12px] font-bold transition-colors', side === 'BUY' ? 'bg-profit text-white' : 'text-text-muted hover:text-text-sec')}
            >
              BUY <span className="text-[9px] font-normal ml-1 opacity-70">B</span>
            </button>
            <button
              onClick={() => setSide('SELL')}
              className={cn('px-4 py-1.5 rounded text-[12px] font-bold transition-colors', side === 'SELL' ? 'bg-loss text-white' : 'text-text-muted hover:text-text-sec')}
            >
              SELL <span className="text-[9px] font-normal ml-1 opacity-70">S</span>
            </button>
          </div>

          <div className="flex items-center gap-2">
            <span className="text-[11px] text-text-muted">
              {activeAccount?.broker} — {activeAccount?.clientId}
            </span>
            <button onClick={() => closeOrderModal()} className="btn-ghost btn-xs ml-2">
              <X className="w-4 h-4" />
            </button>
          </div>
        </div>

        {/* Body */}
        <div className="p-5 space-y-4">
          {/* Execution type strip */}
          <div>
            <label className="field-label mb-1">Execution Type</label>
            <div className="flex gap-1.5">
              {EXECUTION_TYPES.map(et => (
                <button
                  key={et}
                  onClick={() => setExecType(et)}
                  className={cn(
                    'flex-1 py-1.5 rounded text-[11px] font-semibold border transition-all',
                    execType === et
                      ? cn(
                          'border-current',
                          EXEC_COLOR[et],
                          et === 'ENTRY' ? 'bg-profit/15' : et === 'EXIT' ? 'bg-loss/15' : 'bg-brand/15'
                        )
                      : 'border-border text-text-muted hover:text-text-sec bg-bg-elevated'
                  )}
                >
                  {et}
                </button>
              ))}
            </div>
            <p className="text-[10px] text-text-muted mt-1">{EXEC_DESC[execType]}</p>
          </div>

          {/* Symbol search */}
          <div>
            <label className="field-label">Symbol</label>
            {symbol && !showSearch ? (
              <div className="flex items-center justify-between bg-bg-elevated border border-border rounded-lg px-3 py-2">
                <div>
                  <span className="text-[13px] font-bold text-text-bright">{symbol}</span>
                  <span className="text-[10px] text-text-muted ml-2">{exchange}</span>
                </div>
                <div className="flex items-center gap-3">
                  <span className="text-[12px] font-mono text-text-bright">{fmtNum(mockLtp)}</span>
                  <button onClick={() => { setShowSearch(true); setSearchQ(symbol) }} className="text-[10px] text-brand hover:underline">
                    Change
                  </button>
                </div>
              </div>
            ) : (
              <div className="relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-text-muted" />
                <input
                  ref={inputRef}
                  value={searchQ}
                  onChange={e => setSearchQ(e.target.value)}
                  className="input-base w-full pl-9"
                  placeholder="Search symbol…"
                  autoComplete="off"
                />
                {/* Dropdown */}
                {searchQ.length >= 2 && (
                  <div className="absolute top-full left-0 right-0 mt-1 bg-bg-elevated border border-border rounded-xl shadow-modal z-10 max-h-48 overflow-y-auto">
                    {searchLoading ? (
                      <div className="py-3 text-center text-[11px] text-text-muted">Loading…</div>
                    ) : searchResults.length === 0 ? (
                      <div className="py-3 text-center text-[11px] text-text-muted">No results</div>
                    ) : searchResults.map((r: any, i) => (
                      <button
                        key={i}
                        onClick={() => {
                          setSymbol(r.symbol)
                          setExchange(r.exchange ?? 'NSE')
                          setSearchQ('')
                          setShowSearch(false)
                        }}
                        className="w-full flex items-center justify-between px-3 py-2 hover:bg-bg-hover text-left"
                      >
                        <div>
                          <div className="text-[12px] font-semibold text-text-bright">{r.symbol}</div>
                          <div className="text-[10px] text-text-muted">{r.exchange} · {r.type}</div>
                        </div>
                        <span className="badge badge-blue text-[9px]">{r.exchange}</span>
                      </button>
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Order type + Product + Validity row */}
          <div className="grid grid-cols-3 gap-3">
            <div>
              <label className="field-label">Order Type</label>
              <select value={orderType} onChange={e => setOrderType(e.target.value as OrderType)} className="input-base w-full text-[12px]">
                {ORDER_TYPES.map(o => <option key={o}>{o}</option>)}
              </select>
            </div>
            <div>
              <label className="field-label">Product</label>
              <select value={product} onChange={e => setProduct(e.target.value as ProductType)} className="input-base w-full text-[12px]">
                {PRODUCT_TYPES.map(p => <option key={p}>{p}</option>)}
              </select>
            </div>
            <div>
              <label className="field-label">Validity</label>
              <select value={validity} onChange={e => setValidity(e.target.value as Validity)} className="input-base w-full text-[12px]">
                {VALIDITIES.map(v => <option key={v}>{v}</option>)}
              </select>
            </div>
          </div>

          {/* Product description */}
          <p className="text-[10px] text-text-muted -mt-1">{PRODUCT_DESC[product]}</p>

          {/* Qty + Price + Trigger row */}
          <div className={cn('grid gap-3', needsPrice && needsTrigger ? 'grid-cols-3' : needsPrice || needsTrigger ? 'grid-cols-2' : 'grid-cols-1')}>
            <div>
              <label className="field-label">Quantity</label>
              <input
                type="number"
                min={1}
                value={qty}
                onChange={e => setQty(Math.max(1, parseInt(e.target.value) || 1))}
                className="input-base w-full text-[13px] font-mono"
              />
            </div>
            {needsPrice && (
              <div>
                <label className="field-label">Price</label>
                <input
                  type="number"
                  min={0}
                  step={0.05}
                  value={price}
                  onChange={e => setPrice(e.target.value === '' ? '' : +e.target.value)}
                  className="input-base w-full text-[13px] font-mono"
                  placeholder="0.00"
                />
              </div>
            )}
            {needsTrigger && (
              <div>
                <label className="field-label">Trigger Price</label>
                <input
                  type="number"
                  min={0}
                  step={0.05}
                  value={trigPrice}
                  onChange={e => setTrigPrice(e.target.value === '' ? '' : +e.target.value)}
                  className="input-base w-full text-[13px] font-mono"
                  placeholder="0.00"
                />
              </div>
            )}
          </div>

          {/* Margin estimate */}
          <div className={cn(
            'flex items-start gap-2 rounded-lg px-3 py-2.5 border text-[11px]',
            marginOk ? 'bg-profit/8 border-profit/30' : 'bg-loss/8 border-loss/30'
          )}>
            {marginOk ? (
              <CheckCircle className="w-4 h-4 text-profit mt-0.5 shrink-0" />
            ) : (
              <AlertTriangle className="w-4 h-4 text-loss mt-0.5 shrink-0" />
            )}
            <div>
              <div className="text-text-sec">
                Est. Value: <span className="font-mono font-semibold text-text-bright">{fmtINR(estValue)}</span>
                &nbsp;·&nbsp; Req. Margin: <span className="font-mono font-semibold text-text-bright">{fmtINR(reqMargin)}</span>
              </div>
              <div className={marginOk ? 'text-profit' : 'text-loss'}>
                {marginOk
                  ? `Available: ${fmtINR(availableMargin)} ✓`
                  : `Insufficient margin! Available: ${fmtINR(availableMargin)}`
                }
              </div>
            </div>
          </div>

          {/* Tag */}
          <div>
            <label className="field-label">Tag (optional)</label>
            <input
              value={tag}
              onChange={e => setTag(e.target.value)}
              className="input-base w-full text-[12px]"
              placeholder="e.g. scalp, hedge, webhook …"
            />
          </div>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between px-5 py-3.5 border-t border-border bg-bg-surface">
          <div className="text-[10px] text-text-muted">
            <span className="kbd mr-1">Ctrl+Enter</span>Submit &nbsp;
            <span className="kbd mr-1">Esc</span>Cancel
          </div>
          <div className="flex gap-2">
            <button onClick={() => closeOrderModal()} className="btn-ghost btn-sm">Cancel</button>
            {confirm ? (
              <button
                onClick={handleSubmit}
                disabled={submitting}
                className={cn('btn-sm font-bold', side === 'BUY' ? 'btn-buy' : 'btn-sell')}
              >
                {submitting ? '…' : `Confirm ${execType} ${side}`}
              </button>
            ) : (
              <button
                onClick={handleSubmit}
                className={cn('btn-sm', side === 'BUY' ? 'btn-buy' : 'btn-sell')}
              >
                {side === 'BUY' ? 'Buy' : 'Sell'} {qty > 0 ? `× ${qty}` : ''}
                <span className="ml-1.5 text-[9px] opacity-70">{execType}</span>
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
