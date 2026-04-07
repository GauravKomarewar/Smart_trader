/* ═══════════════════════════════
   Dashboard — Order Management
   with open-only toggle, cancel-all, modify dialog
   ═══════════════════════════════ */
import { useMemo, useState } from 'react'
import { useDashboardStore, useToastStore } from '../../stores'
import { cn, fmtNum, fmtTime } from '../../lib/utils'
import { api } from '../../lib/api'
import { ShoppingCart, XCircle, RefreshCw, Eye, EyeOff, PenLine, Trash2 } from 'lucide-react'
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

interface ModifyState {
  order: Order
  price: string
  qty: string
  orderType: string
}

export default function OrderManagement() {
  const { data, showOnlyOpenOrders, setShowOnlyOpenOrders } = useDashboardStore()
  const { toast } = useToastStore()
  const [cancelling, setCancelling] = useState<string | null>(null)
  const [cancellingAll, setCancellingAll] = useState(false)
  const [modify, setModify] = useState<ModifyState | null>(null)
  const [modifying, setModifying] = useState(false)

  const orders = useMemo(() => {
    const all = data?.orders ?? []
    const sorted = [...all].sort((a, b) => new Date(b.placedAt).getTime() - new Date(a.placedAt).getTime())
    return showOnlyOpenOrders ? sorted.filter(o => OPEN_STATUSES.includes(o.status)) : sorted
  }, [data?.orders, showOnlyOpenOrders])

  const openOrders = useMemo(() => orders.filter(o => OPEN_STATUSES.includes(o.status)), [orders])
  const openCount = (data?.orders ?? []).filter(o => OPEN_STATUSES.includes(o.status)).length

  async function cancelOrder(order: Order) {
    setCancelling(order.id)
    try {
      const accountId = (order as any).accountId || (order as any).account_id || ''
      await api.cancelOrder(order.id, accountId)
      toast(`Cancelled: ${order.tradingsymbol}`, 'success')
    } catch {
      toast('Failed to cancel order', 'error')
    } finally {
      setCancelling(null)
    }
  }

  async function cancelAll() {
    const accountId = openOrders[0] && ((openOrders[0] as any).accountId || (openOrders[0] as any).account_id || '')
    if (!accountId) { toast('Cannot determine account for cancel-all', 'error'); return }
    setCancellingAll(true)
    try {
      const res: any = await api.cancelAllOrders(accountId)
      toast(`Cancelled ${res.cancelled ?? 0} orders`, 'success')
    } catch {
      toast('Failed to cancel all orders', 'error')
    } finally {
      setCancellingAll(false)
    }
  }

  function openModify(order: Order) {
    setModify({
      order,
      price: String(order.price ?? ''),
      qty: String(order.quantity ?? ''),
      orderType: order.orderType ?? 'LMT',
    })
  }

  async function submitModify() {
    if (!modify) return
    setModifying(true)
    try {
      const accountId = (modify.order as any).accountId || (modify.order as any).account_id || ''
      await api.modifyOrder(modify.order.id, {
        accountId,
        price: parseFloat(modify.price) || undefined,
        quantity: parseInt(modify.qty) || undefined,
        orderType: modify.orderType,
      })
      toast(`Modified: ${modify.order.tradingsymbol}`, 'success')
      setModify(null)
    } catch {
      toast('Failed to modify order', 'error')
    } finally {
      setModifying(false)
    }
  }

  return (
    <>
    <div className="bg-bg-card border border-border rounded-lg flex flex-col h-full">
      <div className="flex items-center gap-3 px-4 py-3 border-b border-border">
        <ShoppingCart className="w-4 h-4 text-warning" />
        <span className="text-[13px] font-semibold text-text-bright">Orders</span>
        <span className="badge badge-neutral">{orders.length}</span>
        {openCount > 0 && <span className="badge badge-brand">{openCount} open</span>}
        <div className="flex-1" />
        {/* Cancel All */}
        {openCount > 0 && (
          <button
            onClick={cancelAll}
            disabled={cancellingAll}
            className="flex items-center gap-1.5 text-[11px] px-2 py-1 rounded border border-danger/40 text-danger hover:bg-danger/10 transition-colors disabled:opacity-50"
            title="Cancel all open orders"
          >
            {cancellingAll ? <RefreshCw className="w-3 h-3 animate-spin" /> : <Trash2 className="w-3 h-3" />}
            Cancel All
          </button>
        )}
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
                <th className="px-3 py-2 w-16"></th>
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
                      <div className="flex gap-1">
                        <button
                          onClick={() => openModify(o)}
                          className="btn-ghost btn-xs !px-1.5 !py-1"
                          title="Modify order"
                        >
                          <PenLine className="w-3 h-3" />
                        </button>
                        <button
                          onClick={() => cancelOrder(o)}
                          disabled={cancelling === o.id}
                          className="btn-danger btn-xs !px-1.5 !py-1"
                          title="Cancel order"
                        >
                          {cancelling === o.id
                            ? <RefreshCw className="w-3 h-3 animate-spin" />
                            : <XCircle className="w-3 h-3" />}
                        </button>
                      </div>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>

    {/* Modify Order Modal */}
    {modify && (
      <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
        <div className="bg-bg-card border border-border rounded-xl p-5 w-80 shadow-xl">
          <div className="text-[14px] font-semibold text-text-bright mb-4">
            Modify Order — {modify.order.tradingsymbol}
          </div>
          <div className="space-y-3">
            <div>
              <label className="text-[11px] text-text-muted mb-1 block">Order Type</label>
              <select
                value={modify.orderType}
                onChange={e => setModify(m => m ? {...m, orderType: e.target.value} : null)}
                className="input-sm w-full"
              >
                <option value="LMT">Limit</option>
                <option value="MKT">Market</option>
                <option value="SL-LMT">SL-Limit</option>
                <option value="SL-MKT">SL-Market</option>
              </select>
            </div>
            <div>
              <label className="text-[11px] text-text-muted mb-1 block">Price</label>
              <input
                type="number"
                step="0.05"
                value={modify.price}
                onChange={e => setModify(m => m ? {...m, price: e.target.value} : null)}
                className="input-sm w-full"
                placeholder="0.00"
              />
            </div>
            <div>
              <label className="text-[11px] text-text-muted mb-1 block">Quantity</label>
              <input
                type="number"
                step="1"
                value={modify.qty}
                onChange={e => setModify(m => m ? {...m, qty: e.target.value} : null)}
                className="input-sm w-full"
                placeholder="1"
              />
            </div>
          </div>
          <div className="flex gap-2 mt-5">
            <button onClick={() => setModify(null)} className="btn-ghost flex-1">Cancel</button>
            <button
              onClick={submitModify}
              disabled={modifying}
              className="btn-primary flex-1"
            >
              {modifying ? <RefreshCw className="w-3 h-3 animate-spin mx-auto" /> : 'Modify'}
            </button>
          </div>
        </div>
      </div>
    )}
    </>
  )
}

