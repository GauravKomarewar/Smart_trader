/* ═══════════════════════════════════════════════════════════════════
   Broker Accounts Page — Raw Broker Truth
   Per-broker: Positions (with EXIT), Holdings, Orders, Trade Book
   Row highlighting by broker / position side
   ═══════════════════════════════════════════════════════════════════ */
import { useState, useEffect, useRef } from 'react'
import { cn, fmtINR, fmtNum, pnlClass, pnlSign } from '../lib/utils'
import { ws } from '../lib/ws'
import { useBrokerAccountsStore, useToastStore } from '../stores'
import type { BrokerAccountWS } from '../stores'
import { api } from '../lib/api'
import {
  WalletCards, Wifi, WifiOff, Layers, FileText, BarChart2, Package,
  RefreshCw, AlertCircle, XCircle,
} from 'lucide-react'

type DashTab = 'positions' | 'holdings' | 'orders' | 'trades'

const BROKER_TINTS = [
  'border-l-[3px] border-l-[#3b9ede] bg-[#3b9ede]/5',
  'border-l-[3px] border-l-[#f5a623] bg-[#f5a623]/5',
  'border-l-[3px] border-l-[#7c3aed] bg-[#7c3aed]/5',
  'border-l-[3px] border-l-[#10b981] bg-[#10b981]/5',
]
const BROKER_BADGES = [
  'bg-[#3b9ede]/20 text-[#3b9ede] border border-[#3b9ede]/40',
  'bg-[#f5a623]/20 text-[#f5a623] border border-[#f5a623]/40',
  'bg-[#7c3aed]/20 text-[#7c3aed] border border-[#7c3aed]/40',
  'bg-[#10b981]/20 text-[#10b981] border border-[#10b981]/40',
]

export default function BrokerAccountsPage() {
  const { accounts, brokerData } = useBrokerAccountsStore()
  const { toast } = useToastStore()
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<DashTab>('positions')
  const [showActiveOnly, setShowActiveOnly] = useState(false)
  const [exitAllLoading, setExitAllLoading] = useState(false)
  const selectedRef = useRef<string | null>(null)
  selectedRef.current = selectedId

  useEffect(() => {
    if (!selectedRef.current && accounts.length > 0) {
      setSelectedId(accounts[0].config_id)
    }
  }, [accounts])

  useEffect(() => {
    if (selectedId) ws.subscribeBroker(selectedId)
    return () => { ws.unsubscribeBroker() }
  }, [selectedId])

  const selected = accounts.find(a => a.config_id === selectedId) ?? null
  const selectedIdx = accounts.findIndex(a => a.config_id === selectedId)
  const positions = brokerData?.positions ?? []
  const openPositions = positions.filter((p: any) =>
    parseFloat(p.netqty ?? p.qty ?? p.net_quantity ?? p.quantity ?? 0) !== 0
  )

  const tabs: { key: DashTab; label: string; icon: typeof Layers; count?: number }[] = [
    { key: 'positions', label: 'Positions', icon: Layers,    count: showActiveOnly ? openPositions.length : positions.length },
    { key: 'holdings',  label: 'Holdings',  icon: Package,   count: brokerData?.holdings?.length },
    { key: 'orders',    label: 'Orders',    icon: FileText,  count: brokerData?.orders?.length },
    { key: 'trades',    label: 'Trade Book',icon: BarChart2, count: brokerData?.trades?.length },
  ]

  async function exitAll() {
    if (!selected || !openPositions.length) return
    setExitAllLoading(true)
    try {
      const res: any = await api.squareOffAll(selected.config_id)
      toast(res.success ? `${res.placed} exit order(s) sent` : `Partial: ${res.placed} sent`, res.success ? 'success' : 'warning')
    } catch (e: any) {
      toast(`Exit all failed: ${e?.message || 'error'}`, 'error')
    } finally {
      setExitAllLoading(false)
    }
  }

  return (
    <div className="h-full overflow-y-auto">
      <div className="p-4 space-y-4 min-h-full">
        <div className="flex items-center gap-3">
          <WalletCards className="w-4 h-4 text-brand" />
          <span className="text-sm font-semibold text-text-bright">Broker Accounts — Raw Truth</span>
          <span className="text-[10px] text-text-muted border border-border rounded px-1.5 py-0.5">Live broker data · no system filters</span>
        </div>

        {/* Broker selector */}
        <div className="flex gap-3 flex-wrap">
          {accounts.map((acc, idx) => {
            const isActive = acc.config_id === selectedId
            return (
              <button key={acc.config_id}
                onClick={() => { setSelectedId(acc.config_id); setActiveTab('positions') }}
                className={cn(
                  'flex items-center gap-3 px-4 py-3 rounded-xl border transition-all text-left min-w-[240px]',
                  isActive ? 'border-brand bg-brand/10 ring-1 ring-brand/30' : 'border-border bg-bg-surface hover:border-brand/30'
                )}
              >
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 mb-0.5">
                    <span className={cn('text-[9px] font-bold px-1.5 py-0.5 rounded-sm uppercase tracking-wide', BROKER_BADGES[idx % BROKER_BADGES.length])}>
                      {acc.broker_name}
                    </span>
                    <span className="text-[13px] font-bold text-text-bright">{acc.client_id}</span>
                  </div>
                  <div className="text-[10px] text-text-muted font-mono">
                    Cash: {fmtINR(acc.cash)} · Avail: {fmtINR(acc.available_margin)}
                  </div>
                </div>
                <div className="flex flex-col items-end gap-1">
                  {acc.is_live ? <Wifi className="w-3.5 h-3.5 text-profit" /> : <WifiOff className="w-3.5 h-3.5 text-text-muted" />}
                  <span className={cn('text-[11px] font-mono font-bold', pnlClass(acc.day_pnl))}>
                    {pnlSign(acc.day_pnl)}{fmtINR(acc.day_pnl)}
                  </span>
                </div>
              </button>
            )
          })}
          {accounts.length === 0 && (
            <div className="text-sm text-text-muted py-8 w-full text-center">
              No broker accounts. <a href="/app/settings/brokers" className="text-brand hover:underline">Add broker →</a>
            </div>
          )}
        </div>

        {/* Funds strip */}
        {selected?.is_live && (
          <div className="grid grid-cols-4 sm:grid-cols-8 gap-2">
            {[
              { l: 'Cash',       v: fmtINR(selected.cash),              c: 'text-text-bright' },
              { l: 'Collateral', v: fmtINR(selected.collateral),         c: 'text-accent' },
              { l: 'Available',  v: fmtINR(selected.available_margin),   c: 'text-profit' },
              { l: 'Used',       v: fmtINR(selected.used_margin),        c: 'text-warning' },
              { l: 'Day P&L',    v: `${pnlSign(selected.day_pnl)}${fmtINR(selected.day_pnl)}`,                     c: pnlClass(selected.day_pnl) },
              { l: 'Realized',   v: `${pnlSign(selected.realized_pnl)}${fmtINR(selected.realized_pnl)}`,           c: pnlClass(selected.realized_pnl) },
              { l: 'Unrealized', v: `${pnlSign(selected.unrealized_pnl)}${fmtINR(selected.unrealized_pnl)}`,       c: pnlClass(selected.unrealized_pnl) },
              { l: 'Total',      v: fmtINR(selected.total_balance),      c: 'text-text-bright' },
            ].map(k => (
              <div key={k.l} className="kpi-card !py-1.5 !px-2.5">
                <div className="section-title text-[9px]">{k.l}</div>
                <div className={cn('text-[12px] font-bold font-mono', k.c)}>{k.v}</div>
              </div>
            ))}
          </div>
        )}

        {selected?.error && (
          <div className="flex items-center gap-2 text-[11px] text-loss bg-loss/5 border border-loss/20 rounded-lg px-3 py-2">
            <AlertCircle className="w-4 h-4 shrink-0" />{selected.error}
          </div>
        )}

        {/* Tab content */}
        {selectedId && (
          <div className="flex flex-col min-h-[460px]">
            <div className="flex items-center gap-0 bg-bg-surface border border-border rounded-t-lg overflow-x-auto">
              {tabs.map(t => (
                <button key={t.key} onClick={() => setActiveTab(t.key)}
                  className={cn(
                    'flex items-center gap-2 px-5 py-3 text-[12px] font-medium transition-colors border-b-2 whitespace-nowrap',
                    activeTab === t.key ? 'border-brand text-brand bg-brand/5' : 'border-transparent text-text-sec hover:text-text-bright hover:bg-bg-hover'
                  )}>
                  <t.icon className="w-3.5 h-3.5" />
                  {t.label}
                  {t.count !== undefined && (
                    <span className={cn('badge', activeTab === t.key ? 'badge-brand' : 'badge-neutral')}>{t.count}</span>
                  )}
                </button>
              ))}
              <div className="flex-1" />
              {activeTab === 'positions' && (
                <div className="flex items-center gap-2 pr-3">
                  <button onClick={() => setShowActiveOnly(v => !v)}
                    className={cn(
                      'text-[11px] px-3 py-1.5 rounded-md border transition-colors font-medium',
                      showActiveOnly ? 'bg-brand border-brand text-white' : 'border-border text-text-muted hover:border-brand/40 hover:text-text-sec'
                    )}>
                    Active Only
                  </button>
                  {openPositions.length > 0 && (
                    <button onClick={exitAll} disabled={exitAllLoading}
                      className="flex items-center gap-1.5 text-[11px] px-3 py-1.5 rounded-md border border-loss/60 text-loss hover:bg-loss/10 font-bold disabled:opacity-50 transition-colors">
                      {exitAllLoading ? <RefreshCw className="w-3 h-3 animate-spin" /> : <XCircle className="w-3 h-3" />}
                      Exit All ({openPositions.length})
                    </button>
                  )}
                </div>
              )}
              <span className="text-[10px] text-text-muted pr-4">
                {selected ? `${selected.broker_name} · ${selected.client_id}` : '—'}
              </span>
            </div>

            <div className="flex-1 bg-bg-card border border-t-0 border-border rounded-b-lg overflow-hidden">
              {activeTab === 'positions' && (
                <BrokerPositionsTable
                  data={showActiveOnly ? openPositions : positions}
                  account={selected}
                  brokerIdx={selectedIdx}
                  toast={toast}
                />
              )}
              {activeTab === 'holdings'  && <BrokerHoldingsTable  data={brokerData?.holdings ?? []} />}
              {activeTab === 'orders'    && <BrokerOrdersTable    data={brokerData?.orders ?? []} />}
              {activeTab === 'trades'    && <BrokerTradesTable    data={brokerData?.trades ?? []} />}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

// ─── Positions table with EXIT buttons ─────────────────────────────────────
function BrokerPositionsTable({
  data, account, brokerIdx, toast,
}: {
  data: any[];
  account: BrokerAccountWS | null;
  brokerIdx: number;
  toast: (m: string, t: 'success'|'error'|'warning'|'info') => void;
}) {
  const [exitLoading, setExitLoading] = useState<string|null>(null)
  const rowTint = BROKER_TINTS[brokerIdx % BROKER_TINTS.length]

  if (data.length === 0) return <EmptyState msg="No positions for this broker" />

  async function exitPos(p: any) {
    if (!account) return
    const qty = parseFloat(p.netqty ?? p.qty ?? p.net_quantity ?? p.quantity ?? 0)
    if (qty === 0) return
    const sym  = p.tradingsymbol ?? p.tsym ?? p.symbol ?? ''
    const exch = p.exchange ?? p.exch ?? 'NSE'
    const prd  = p.product ?? p.prd ?? 'NRML'
    const side = qty > 0 ? 'BUY' : 'SELL'
    const key  = `${sym}|${prd}`
    setExitLoading(key)
    try {
      await api.squareOff({ symbol: sym, exchange: exch, product: prd, quantity: Math.abs(Math.round(qty)), side, accountId: account.config_id })
      toast(`Exit sent: ${sym}`, 'success')
    } catch (e: any) {
      toast(`Exit failed: ${e?.message ?? 'error'}`, 'error')
    } finally {
      setExitLoading(null)
    }
  }

  const totalRealized   = data.reduce((s, p) => s + ('realised_pnl' in p ? +p.realised_pnl : 'rpnl' in p ? +p.rpnl : 0), 0)
  const totalUnrealized = data.reduce((s, p) => s + ('unrealised_pnl' in p ? +p.unrealised_pnl : 'urmtom' in p ? +p.urmtom : 0), 0)
  const totalPnl = totalRealized + totalUnrealized

  return (
    <div className="overflow-auto max-h-[600px]">
      <table className="data-table">
        <thead className="sticky top-0 bg-bg-card z-10">
          <tr>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider">Symbol</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider">Exch</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider">Prd</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider text-right">Qty</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider text-right">Avg</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider text-right">LTP</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider text-right">Realized P&L</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider text-right">Unrealized</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider text-right">Total P&L</th>
            <th className="px-3 py-2 w-24"></th>
          </tr>
        </thead>
        <tbody>
          {data.map((p, i) => {
            const qty    = parseFloat(p.netqty ?? p.qty ?? p.net_quantity ?? p.quantity ?? 0)
            const isFlat = qty === 0
            const sym    = p.tradingsymbol ?? p.tsym ?? p.symbol ?? '—'
            const exch   = p.exchange ?? p.exch ?? '—'
            const prd    = p.product ?? p.prd ?? '—'
            const avg    = parseFloat(p.avgprc ?? p.avg_price ?? p.average_price ?? 0)
            const ltp    = parseFloat(p.ltp ?? p.lp ?? p.last_price ?? 0)
            const realized   = 'realised_pnl' in p ? +p.realised_pnl : 'rpnl' in p ? +p.rpnl : 'realized_pnl' in p ? +p.realized_pnl : 0
            const unrealized = 'unrealised_pnl' in p ? +p.unrealised_pnl : 'urmtom' in p ? +p.urmtom : 'unrealized_pnl' in p ? +p.unrealized_pnl : 0
            const total = realized + unrealized
            const key   = `${sym}|${prd}`
            return (
              <tr key={i} className={cn('transition-colors', isFlat ? 'opacity-40' : rowTint, !isFlat && qty > 0 && 'hover:bg-profit/5', !isFlat && qty < 0 && 'hover:bg-loss/5')}>
                <td className="px-3 py-2">
                  <div className="font-semibold text-[12px] text-text-bright">{sym}</div>
                  <div className="mt-0.5">
                    {isFlat
                      ? <span className="text-[9px] text-text-muted">FLAT</span>
                      : <span className={cn('badge text-[9px]', qty > 0 ? 'badge-buy' : 'badge-sell')}>{qty > 0 ? 'LONG' : 'SHORT'}</span>
                    }
                  </div>
                </td>
                <td className="px-3 py-2 text-[11px] text-text-muted">{exch}</td>
                <td className="px-3 py-2 text-[11px] text-text-muted">{prd}</td>
                <td className="px-3 py-2 text-right">
                  <span className={cn('font-mono font-bold text-[12px]', isFlat ? 'text-text-muted' : qty > 0 ? 'text-profit' : 'text-loss')}>
                    {isFlat ? '0' : (qty > 0 ? `+${qty}` : `${qty}`)}
                  </span>
                </td>
                <td className="px-3 py-2 text-right font-mono text-[12px] text-text-sec">{fmtNum(avg)}</td>
                <td className="px-3 py-2 text-right font-mono text-[12px] text-text-bright">{fmtNum(ltp)}</td>
                <td className={cn('px-3 py-2 text-right font-mono font-semibold text-[12px]', pnlClass(realized))}>
                  {pnlSign(realized)}{fmtINR(Math.abs(realized))}
                </td>
                <td className={cn('px-3 py-2 text-right font-mono text-[12px]', pnlClass(unrealized))}>
                  {pnlSign(unrealized)}{fmtINR(Math.abs(unrealized))}
                </td>
                <td className={cn('px-3 py-2 text-right font-mono font-bold text-[12px]', pnlClass(total))}>
                  {pnlSign(total)}{fmtINR(Math.abs(total))}
                </td>
                <td className="px-3 py-2">
                  {!isFlat && (
                    <button onClick={() => exitPos(p)} disabled={exitLoading === key}
                      className="flex items-center gap-1 text-[10px] px-2.5 py-1.5 rounded border border-loss/60 text-loss hover:bg-loss hover:text-white transition-colors font-bold uppercase tracking-wider disabled:opacity-40">
                      {exitLoading === key ? <RefreshCw className="w-3 h-3 animate-spin" /> : <XCircle className="w-3 h-3" />}
                      EXIT
                    </button>
                  )}
                </td>
              </tr>
            )
          })}
        </tbody>
        <tfoot>
          <tr className="border-t border-border bg-bg-elevated/60">
            <td colSpan={6} className="px-3 py-2 text-[11px] text-text-muted">
              {data.filter(p => parseFloat(p.netqty??p.qty??0) !== 0).length} open · {data.filter(p => parseFloat(p.netqty??p.qty??0) === 0).length} flat
            </td>
            <td className={cn('px-3 py-2 text-right font-mono font-bold text-[12px]', pnlClass(totalRealized))}>
              {pnlSign(totalRealized)}{fmtINR(Math.abs(totalRealized))}
            </td>
            <td className={cn('px-3 py-2 text-right font-mono font-bold text-[12px]', pnlClass(totalUnrealized))}>
              {pnlSign(totalUnrealized)}{fmtINR(Math.abs(totalUnrealized))}
            </td>
            <td className={cn('px-3 py-2 text-right font-mono font-bold text-[12px]', pnlClass(totalPnl))}>
              {pnlSign(totalPnl)}{fmtINR(Math.abs(totalPnl))}
            </td>
            <td />
          </tr>
        </tfoot>
      </table>
    </div>
  )
}

function BrokerHoldingsTable({ data }: { data: any[] }) {
  if (data.length === 0) return <EmptyState msg="No holdings for this broker" />
  return (
    <div className="overflow-auto max-h-[600px]">
      <table className="data-table">
        <thead className="sticky top-0 bg-bg-card z-10">
          <tr>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Symbol</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Qty</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Avg Price</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">LTP</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Invested</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">P&L</th>
          </tr>
        </thead>
        <tbody>
          {data.map((h, i) => {
            const qty = parseFloat(h.quantity ?? h.holdingQty ?? h.hldqty ?? 0)
            const avg = parseFloat(h.avgPrice ?? h.average_price ?? h.upldprc ?? 0)
            const ltp = parseFloat(h.ltp ?? h.last_price ?? h.lp ?? 0)
            const pnl = parseFloat(h.pnl ?? h.profitAndLoss ?? 0) || (ltp - avg) * qty
            return (
              <tr key={i} className="hover:bg-bg-hover">
                <td className="px-3 py-2 font-medium text-[12px] text-text-bright">{h.symbol ?? h.tradingSymbol ?? h.tsym ?? '—'}</td>
                <td className="px-3 py-2 text-right font-mono text-[12px]">{qty}</td>
                <td className="px-3 py-2 text-right font-mono text-[12px] text-text-sec">{fmtINR(avg)}</td>
                <td className="px-3 py-2 text-right font-mono text-[12px] text-text-bright">{fmtINR(ltp)}</td>
                <td className="px-3 py-2 text-right font-mono text-[11px] text-text-sec">{fmtINR(avg * qty)}</td>
                <td className={cn('px-3 py-2 text-right font-mono font-semibold text-[12px]', pnlClass(pnl))}>
                  {pnlSign(pnl)}{fmtINR(Math.abs(pnl))}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function BrokerOrdersTable({ data }: { data: any[] }) {
  if (data.length === 0) return <EmptyState msg="No orders for this broker" />
  return (
    <div className="overflow-auto max-h-[600px]">
      <table className="data-table">
        <thead className="sticky top-0 bg-bg-card z-10">
          <tr>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Time</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Symbol</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Side</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Type</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Qty</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Price</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Avg Fill</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Status</th>
          </tr>
        </thead>
        <tbody>
          {data.map((o, i) => {
            const side = (o.side ?? o.transactionType ?? o.trantype ?? '').toUpperCase()
            const isBuy = side === 'BUY' || side === 'B'
            const status = (o.status ?? o.orderStatus ?? '').toUpperCase()
            return (
              <tr key={i} className="hover:bg-bg-hover">
                <td className="px-3 py-2 text-text-muted text-[10px] whitespace-nowrap">{o.orderTime ?? o.order_timestamp ?? o.norentm ?? '—'}</td>
                <td className="px-3 py-2 font-medium text-[12px] text-text-bright">{o.symbol ?? o.tradingSymbol ?? o.tsym ?? '—'}</td>
                <td className="px-3 py-2"><span className={cn('badge text-[9px]', isBuy ? 'badge-buy' : 'badge-sell')}>{isBuy ? 'BUY' : 'SELL'}</span></td>
                <td className="px-3 py-2 text-[11px] text-text-sec">{o.orderType ?? o.order_type ?? o.prctyp ?? '—'}</td>
                <td className="px-3 py-2 text-right font-mono text-[12px]">{o.qty ?? o.quantity ?? o.totalQty ?? 0}</td>
                <td className="px-3 py-2 text-right font-mono text-[12px] text-text-sec">{fmtINR(o.price ?? 0)}</td>
                <td className="px-3 py-2 text-right font-mono text-[12px]">{fmtINR(o.averagePrice ?? o.avgprc ?? o.fillPrice ?? 0)}</td>
                <td className="px-3 py-2">
                  <span className={cn('badge text-[9px]',
                    status.includes('COMPLETE') || status.includes('FILLED') ? 'badge-success' :
                    status.includes('REJECT') || status.includes('CANCEL') ? 'badge-danger' :
                    status.includes('OPEN') || status.includes('PENDING') ? 'badge-warning' : 'badge-neutral'
                  )}>{status || '—'}</span>
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function BrokerTradesTable({ data }: { data: any[] }) {
  if (data.length === 0) return <EmptyState msg="No trades today for this broker" />
  return (
    <div className="overflow-auto max-h-[600px]">
      <table className="data-table">
        <thead className="sticky top-0 bg-bg-card z-10">
          <tr>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Time</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Symbol</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Exchange</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Side</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Qty</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Price</th>
          </tr>
        </thead>
        <tbody>
          {data.map((t, i) => {
            const side = (t.side ?? t.transactionType ?? t.trantype ?? '').toUpperCase()
            const isBuy = side === 'BUY' || side === 'B'
            return (
              <tr key={i} className="hover:bg-bg-hover">
                <td className="px-3 py-2 text-text-muted text-[10px] whitespace-nowrap">{t.fillTime ?? t.trade_timestamp ?? t.norentm ?? '—'}</td>
                <td className="px-3 py-2 font-medium text-[12px] text-text-bright">{t.symbol ?? t.tradingSymbol ?? t.tsym ?? '—'}</td>
                <td className="px-3 py-2 text-[11px] text-text-sec">{t.exchange ?? t.exch ?? '—'}</td>
                <td className="px-3 py-2"><span className={cn('badge text-[9px]', isBuy ? 'badge-buy' : 'badge-sell')}>{isBuy ? 'BUY' : 'SELL'}</span></td>
                <td className="px-3 py-2 text-right font-mono text-[12px]">{t.qty ?? t.quantity ?? t.fillShares ?? 0}</td>
                <td className="px-3 py-2 text-right font-mono text-[12px]">{fmtINR(t.price ?? t.averagePrice ?? t.flprc ?? 0)}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function EmptyState({ msg }: { msg: string }) {
  return <div className="flex items-center justify-center py-16 text-text-muted text-sm">{msg}</div>
}
