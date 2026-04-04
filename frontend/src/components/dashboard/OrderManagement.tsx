/* ═══════════════════════════════
   Dashboard — Order Management
   with open-only toggle
   ═══════════════════════════════ */
import { useMemo, useState } from 'react'
import { useDashboardStore, useToastStore } from '../../stores'
import { cn, fmtNum, fmtTime, fmtINR } from '../../lib/utils'
import { api } from '../../lib/api'
import { ShoppingCart, XCircle, RefreshCw, Eye, EyeOff } from 'lucide-react'
import type { Order, OrderStatus } from '../../types'

const OPEN_STATUSES: OrderStatus[] = ['OPEN', 'PENDING', 'AMO', 'TRIGGER_PENDING']

const STATUS_BADGE: Record<OrderStatus, string> = {
  OPEN:            'badge-brand',
  PENDING:         'badge-warn',
  COMPLETE:        'badge-safe',
  CANCELLED:       'badge-neutral',
  REJECTED:        'badge-danger',
  AMO:             'badge-brand',
  TRIGGER_PENDING: 'badge-warn',
}

export default function OrderManagement() {
  const { data, showOnlyOpenOrders, setShowOnlyOpenOrders } = useDashboardStore()
  const { toast } = useToastStore()
  const [cancelling, setCancelling] = useState<string | null>(null)

  const orders = useMemo(() => {
    const all = data?.orders ?? []
    const sorted = [...all].sort((a, b) => new Date(b.placedAt).getTime() - new Date(a.placedAt).getTime())
    return showOnlyOpenOrders ? sorted.filter(o => OPEN_STATUSES.includes(o.status)) : sorted
  }, [data?.orders, showOnlyOpenOrders])

  const openCount = (data?.orders ?? []).filter(o => OPEN_STATUSES.includes(o.status)).length

  async function cancelOrder(order: Order) {
    setCancelling(order.id)
    try {
      await api.cancelOrder(order.id)
      toast(`Order cancelled: ${order.tradingsymbol}`, 'success')
    } catch {
      toast('Failed to cancel order', 'error')
    } finally {
      setCancelling(null)
    }
  }

  return (
    <div className="bg-bg-card border border-border rounded-lg flex flex-col h-full">
      <div className="flex items-center gap-3 px-4 py-3 border-b border-border">
        <ShoppingCart className="w-4 h-4 text-warning" />
        <span className="text-[13px] font-semibold text-text-bright">Orders</span>
        <span className="badge badge-neutral">{orders.length}</span>
        {openCount > 0 && <span className="badge badge-brand">{openCount} open</span>}
        <div className="flex-1" />
        {/* Open-only toggle */}
        <button
          onClick={() => setShowOnlyOpenOrders(!showOnlyOpenOrders)}
          className={cn('flex items-center gap-1.5 text-[11px] px-2 py-1 rounded border transition-colors',
            showOnlyOpenOrders
              ? 'bg-brand/10 border-brand/40 text-brand'
              : 'border-border text-text-muted hover:text-text-pri hover:border-border-dim')}
        >
          {showOnlyOpenOrders ? <Eye className="w-3 h-3" /> : <EyeOff className="w-3 h-3" />}
          Open only
        </button>
      </div>

      <div className="flex-1 overflow-auto">
        {orders.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-32 text-text-muted text-[12px] gap-2">
            <ShoppingCart className="w-8 h-8 opacity-30" />
            {showOnlyOpenOrders ? 'No open orders' : 'No orders today'}
          </div>
        ) : (
          <table className="data-table">
            <thead className="sticky top-0 bg-bg-card z-10">
              <tr>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-left">Symbol</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Type</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Qty</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Price</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Status</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Time</th>
                <th className="px-3 py-2 w-8"></th>
              </tr>
            </thead>
            <tbody>
              {orders.map(o => (
                <tr key={o.id} className="group">
                  <td className="px-3 py-2">
                    <div>
                      <div className="text-[12px] font-medium text-text-bright truncate max-w-[130px]">{o.tradingsymbol}</div>
                      <div className="text-[10px] text-text-muted">{o.exchange} · {o.product}</div>
                    </div>
                  </td>
                  <td className="px-3 py-2">
                    <div className="flex flex-col gap-0.5">
                      <span className={cn('badge text-[9px]', o.transactionType === 'BUY' ? 'badge-buy' : 'badge-sell')}>
                        {o.transactionType}
                      </span>
                      <span className="text-[10px] text-text-muted">{o.orderType}</span>
                    </div>
                  </td>
                  <td className="px-3 py-2 text-right">
                    <div className="text-[12px] font-mono text-text-pri">{o.filledQty}/{o.quantity}</div>
                  </td>
                  <td className="px-3 py-2 text-right">
                    <div className="text-[12px] font-mono text-text-bright">
                      {o.orderType === 'MARKET' ? 'MKT' : fmtNum(o.price)}
                    </div>
                    {o.avgPrice && o.avgPrice !== o.price && (
                      <div className="text-[10px] font-mono text-text-muted">avg {fmtNum(o.avgPrice)}</div>
                    )}
                  </td>
                  <td className="px-3 py-2">
                    <span className={cn('badge', STATUS_BADGE[o.status] ?? 'badge-neutral')}>
                      {o.status.replace(/_/g,' ')}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-[11px] text-text-muted tabular-nums">{fmtTime(o.placedAt)}</td>
                  <td className="px-3 py-2">
                    {OPEN_STATUSES.includes(o.status) && (
                      <button
                        onClick={() => cancelOrder(o)}
                        disabled={cancelling === o.id}
                        className="btn-danger btn-xs !px-1.5 !py-1 opacity-0 group-hover:opacity-100 transition-opacity"
                        title="Cancel order"
                      >
                        {cancelling === o.id
                          ? <RefreshCw className="w-3 h-3 animate-spin" />
                          : <XCircle className="w-3 h-3" />}
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
