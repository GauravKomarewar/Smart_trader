/* ═══════════════════════════════════════════
   Broker Accounts Page — per-broker filtered view
   Broker selector buttons + Position/Holdings/Orders/Tradebook
   ═══════════════════════════════════════════ */
import { useState, useEffect, useRef } from 'react'
import { cn, fmtINR, pnlClass, pnlSign, timeAgo } from '../lib/utils'
import { ws } from '../lib/ws'
import { useBrokerAccountsStore } from '../stores'
import type { BrokerAccountWS } from '../stores'
import {
  WalletCards, Wifi, WifiOff, Layers, FileText, BarChart2, Package,
  RefreshCw, TrendingUp, TrendingDown, Activity, AlertCircle, Clock,
} from 'lucide-react'

type DashTab = 'positions' | 'holdings' | 'orders' | 'trades'

type BrokerAccount = BrokerAccountWS

interface BrokerData {
  positions: any[]
  holdings: any[]
  orders: any[]
  trades: any[]
  accountSummary: any
}

export default function BrokerAccountsPage() {
  const { accounts, brokerData } = useBrokerAccountsStore()
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<DashTab>('positions')
  const selectedRef = useRef<string | null>(null)

  // Keep ref in sync with state (prevents stale closure)
  selectedRef.current = selectedId

  // Auto-select first account when accounts arrive and nothing is selected
  useEffect(() => {
    if (!selectedRef.current && accounts.length > 0) {
      setSelectedId(accounts[0].config_id)
    }
  }, [accounts])

  // Subscribe WS to the selected broker for per-broker data
  useEffect(() => {
    if (selectedId) {
      ws.subscribeBroker(selectedId)
    }
    return () => { ws.unsubscribeBroker() }
  }, [selectedId])

  const selected = accounts.find(a => a.config_id === selectedId)

  const tabs: { key: DashTab; label: string; icon: typeof Layers; count?: number }[] = [
    { key: 'positions', label: 'Positions', icon: Layers,   count: brokerData?.positions?.length },
    { key: 'holdings',  label: 'Holdings',  icon: Package,  count: brokerData?.holdings?.length },
    { key: 'orders',    label: 'Orders',    icon: FileText, count: brokerData?.orders?.length },
    { key: 'trades',    label: 'Trade Book',icon: BarChart2,count: brokerData?.trades?.length },
  ]

  return (
    <div className="h-full overflow-y-auto">
      <div className="p-4 space-y-4 min-h-full">

        {/* ── Header ── */}
        <div className="flex items-center gap-3">
          <WalletCards className="w-4 h-4 text-brand" />
          <span className="text-sm font-semibold text-text-bright">Broker Accounts</span>
          <div className="flex-1" />
        </div>

        {/* ── Broker selector buttons ── */}
        <div className="flex gap-3 flex-wrap">
          {accounts.map(acc => {
            const isActive = acc.config_id === selectedId
            return (
              <button
                key={acc.config_id}
                onClick={() => setSelectedId(acc.config_id)}
                className={cn(
                  'flex items-center gap-3 px-4 py-3 rounded-xl border transition-all text-left min-w-[220px]',
                  isActive
                    ? 'border-brand bg-brand/10 ring-1 ring-brand/30 shadow-lg shadow-brand/5'
                    : 'border-border bg-bg-surface hover:border-brand/30 hover:bg-bg-hover'
                )}
              >
                {/* Broker icon + info */}
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <WalletCards className={cn('w-4 h-4 shrink-0', isActive ? 'text-brand' : 'text-text-muted')} />
                    <span className="text-[13px] font-bold text-text-bright truncate">{acc.client_id}</span>
                  </div>
                  <div className="text-[10px] text-text-muted mt-0.5">{acc.broker_name}</div>
                </div>

                {/* Status + Day PnL */}
                <div className="flex flex-col items-end gap-1 shrink-0">
                  <span className={cn(
                    'text-[9px] font-bold px-2 py-0.5 rounded-full border uppercase tracking-wider',
                    acc.is_live
                      ? 'text-profit border-profit/30 bg-profit/10'
                      : 'text-text-muted border-border bg-bg-elevated'
                  )}>
                    {acc.is_live ? '● LIVE' : '○ OFF'}
                  </span>
                  {acc.is_live && (
                    <span className={cn('text-[11px] font-mono font-bold', pnlClass(acc.day_pnl))}>
                      {pnlSign(acc.day_pnl)}{fmtINR(acc.day_pnl)}
                    </span>
                  )}
                </div>
              </button>
            )
          })}
          {accounts.length === 0 && (
            <div className="text-sm text-text-muted py-8 text-center w-full">
              No broker accounts configured.
              <a href="/app/settings/brokers" className="text-brand hover:underline ml-1">Add broker →</a>
            </div>
          )}
        </div>

        {/* ── Selected broker summary strip ── */}
        {selected && selected.is_live && (
          <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-8 gap-3">
            <MiniKPI label="Cash" value={fmtINR(selected.cash)} cls="text-profit" />
            <MiniKPI label="Collateral" value={fmtINR(selected.collateral)} cls="text-accent" />
            <MiniKPI label="Available" value={fmtINR(selected.available_margin)} cls="text-profit" />
            <MiniKPI label="Used" value={fmtINR(selected.used_margin)} cls="text-warning" />
            <MiniKPI label="Day P&L" value={`${pnlSign(selected.day_pnl)}${fmtINR(selected.day_pnl)}`} cls={pnlClass(selected.day_pnl)} />
            <MiniKPI label="Unrealized" value={`${pnlSign(selected.unrealized_pnl)}${fmtINR(selected.unrealized_pnl)}`} cls={pnlClass(selected.unrealized_pnl)} />
            <MiniKPI label="Orders" value={`${selected.completed_orders}/${selected.orders_count}`} cls="text-text-bright" />
            <MiniKPI label="Trades" value={String(selected.trades_count)} cls="text-text-bright" />
          </div>
        )}

        {/* ── Error ── */}
        {selected?.error && (
          <div className="flex items-center gap-2 text-[11px] text-loss bg-loss/5 rounded-lg px-3 py-2 border border-loss/20">
            <AlertCircle className="w-4 h-4 shrink-0" />
            {selected.error}
          </div>
        )}

        {/* ── Tabbed data tables (full width) ── */}
        {selectedId && (
          <div className="flex flex-col min-h-[460px]">
            {/* Tab bar */}
            <div className="flex items-center gap-0 bg-bg-surface border border-border rounded-t-lg overflow-x-auto">
              {tabs.map(t => (
                <button
                  key={t.key}
                  onClick={() => setActiveTab(t.key)}
                  className={cn(
                    'flex items-center gap-2 px-5 py-3 text-[12px] font-medium transition-colors border-b-2 whitespace-nowrap',
                    activeTab === t.key
                      ? 'border-brand text-brand bg-brand/5'
                      : 'border-transparent text-text-sec hover:text-text-bright hover:bg-bg-hover'
                  )}
                >
                  <t.icon className="w-3.5 h-3.5" />
                  {t.label}
                  {t.count !== undefined && (
                    <span className={cn('badge', activeTab === t.key ? 'badge-brand' : 'badge-neutral')}>
                      {t.count}
                    </span>
                  )}
                </button>
              ))}
              <div className="flex-1" />
              <span className="text-[10px] text-text-muted pr-4">
                {selected ? `${selected.broker_name} · ${selected.client_id}` : '—'}
              </span>
            </div>

            {/* Tab content */}
            <div className="flex-1 bg-bg-card border border-t-0 border-border rounded-b-lg overflow-hidden">
              {activeTab === 'positions' && <BrokerPositionsTable data={brokerData?.positions ?? []} />}
              {activeTab === 'holdings'  && <BrokerHoldingsTable data={brokerData?.holdings ?? []} />}
              {activeTab === 'orders'    && <BrokerOrdersTable data={brokerData?.orders ?? []} />}
              {activeTab === 'trades'    && <BrokerTradesTable data={brokerData?.trades ?? []} />}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}


// ── Mini KPI card ─────────────────────────────────
function MiniKPI({ label, value, cls }: { label: string; value: string; cls: string }) {
  return (
    <div className="kpi-card !py-2 !px-3">
      <div className="section-title">{label}</div>
      <div className={cn('text-sm font-bold font-mono', cls)}>{value}</div>
    </div>
  )
}


// ── Broker-specific data tables (self-contained, no store dependency) ──

function BrokerPositionsTable({ data }: { data: any[] }) {
  if (data.length === 0) return <EmptyState msg="No open positions for this broker" />
  return (
    <div className="overflow-auto max-h-[500px]">
      <table className="data-table">
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Type</th>
            <th>Qty</th>
            <th>Avg Price</th>
            <th>LTP</th>
            <th className="text-right">P&L</th>
          </tr>
        </thead>
        <tbody>
          {data.map((p, i) => {
            const pnl = p.pnl ?? p.unRealizedPnl ?? p.unrealized_pnl ?? 0
            const qty = p.netQty ?? p.quantity ?? p.net_qty ?? 0
            const isBuy = qty > 0
            return (
              <tr key={i}>
                <td className="font-medium text-text-bright">{p.symbol ?? p.tradingSymbol ?? p.tsym ?? '—'}</td>
                <td><span className={cn('badge', isBuy ? 'badge-buy' : 'badge-sell')}>{isBuy ? 'BUY' : 'SELL'}</span></td>
                <td className="font-mono">{Math.abs(qty)}</td>
                <td className="font-mono">{fmtINR(p.avgPrice ?? p.average_price ?? p.netAvg ?? 0)}</td>
                <td className="font-mono">{fmtINR(p.ltp ?? p.last_price ?? p.lp ?? 0)}</td>
                <td className={cn('text-right font-mono font-semibold', pnlClass(pnl))}>
                  {pnlSign(pnl)}{fmtINR(pnl)}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function BrokerHoldingsTable({ data }: { data: any[] }) {
  if (data.length === 0) return <EmptyState msg="No holdings for this broker" />
  return (
    <div className="overflow-auto max-h-[500px]">
      <table className="data-table">
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Qty</th>
            <th>Avg Price</th>
            <th>LTP</th>
            <th>Invested</th>
            <th className="text-right">P&L</th>
          </tr>
        </thead>
        <tbody>
          {data.map((h, i) => {
            const qty = h.quantity ?? h.holdingQty ?? h.hldqty ?? 0
            const avg = h.avgPrice ?? h.average_price ?? h.upldprc ?? 0
            const ltp = h.ltp ?? h.last_price ?? h.lp ?? 0
            const pnl = h.pnl ?? h.profitAndLoss ?? ((ltp - avg) * qty) ?? 0
            return (
              <tr key={i}>
                <td className="font-medium text-text-bright">{h.symbol ?? h.tradingSymbol ?? h.tsym ?? '—'}</td>
                <td className="font-mono">{qty}</td>
                <td className="font-mono">{fmtINR(avg)}</td>
                <td className="font-mono">{fmtINR(ltp)}</td>
                <td className="font-mono">{fmtINR(avg * qty)}</td>
                <td className={cn('text-right font-mono font-semibold', pnlClass(pnl))}>
                  {pnlSign(pnl)}{fmtINR(pnl)}
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
    <div className="overflow-auto max-h-[500px]">
      <table className="data-table">
        <thead>
          <tr>
            <th>Time</th>
            <th>Symbol</th>
            <th>Side</th>
            <th>Type</th>
            <th>Qty</th>
            <th>Price</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          {data.map((o, i) => {
            const side = (o.side ?? o.transactionType ?? o.trantype ?? '').toUpperCase()
            const isBuy = side === 'BUY' || side === 'B'
            const status = (o.status ?? o.orderStatus ?? '').toUpperCase()
            return (
              <tr key={i}>
                <td className="text-text-muted text-[10px]">{o.orderTime ?? o.order_timestamp ?? o.norentm ?? '—'}</td>
                <td className="font-medium text-text-bright">{o.symbol ?? o.tradingSymbol ?? o.tsym ?? '—'}</td>
                <td><span className={cn('badge', isBuy ? 'badge-buy' : 'badge-sell')}>{isBuy ? 'BUY' : 'SELL'}</span></td>
                <td className="text-text-sec">{o.orderType ?? o.order_type ?? o.prctyp ?? '—'}</td>
                <td className="font-mono">{o.qty ?? o.quantity ?? o.totalQty ?? 0}</td>
                <td className="font-mono">{fmtINR(o.price ?? o.averagePrice ?? o.avgprc ?? 0)}</td>
                <td>
                  <span className={cn(
                    'badge',
                    status.includes('COMPLETE') || status.includes('FILLED') ? 'badge-success' :
                    status.includes('REJECT') || status.includes('CANCEL') ? 'badge-danger' :
                    status.includes('OPEN') || status.includes('PENDING') ? 'badge-warning' :
                    'badge-neutral'
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
  if (data.length === 0) return <EmptyState msg="No trades for this broker today" />
  return (
    <div className="overflow-auto max-h-[500px]">
      <table className="data-table">
        <thead>
          <tr>
            <th>Time</th>
            <th>Symbol</th>
            <th>Side</th>
            <th>Qty</th>
            <th>Price</th>
            <th>Exchange</th>
          </tr>
        </thead>
        <tbody>
          {data.map((t, i) => {
            const side = (t.side ?? t.transactionType ?? t.trantype ?? '').toUpperCase()
            const isBuy = side === 'BUY' || side === 'B'
            return (
              <tr key={i}>
                <td className="text-text-muted text-[10px]">{t.fillTime ?? t.trade_timestamp ?? t.norentm ?? '—'}</td>
                <td className="font-medium text-text-bright">{t.symbol ?? t.tradingSymbol ?? t.tsym ?? '—'}</td>
                <td><span className={cn('badge', isBuy ? 'badge-buy' : 'badge-sell')}>{isBuy ? 'BUY' : 'SELL'}</span></td>
                <td className="font-mono">{t.qty ?? t.quantity ?? t.fillShares ?? 0}</td>
                <td className="font-mono">{fmtINR(t.price ?? t.averagePrice ?? t.flprc ?? 0)}</td>
                <td className="text-text-sec">{t.exchange ?? t.exch ?? '—'}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function EmptyState({ msg }: { msg: string }) {
  return (
    <div className="flex items-center justify-center py-16 text-text-muted text-sm">
      {msg}
    </div>
  )
}
