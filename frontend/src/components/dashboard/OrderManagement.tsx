/* ═══════════════════════════════
   Dashboard — Order Management
   with open-only toggle, cancel-all, modify dialog
   ═══════════════════════════════ */
import { useMemo, useState } from 'react'
import { useDashboardStore, useBrokerAccountsStore, useToastStore } from '../../stores'
import { cn, fmtNum, fmtTime } from '../../lib/utils'
import { api } from '../../lib/api'
import { ShoppingCart, XCircle, RefreshCw, PenLine, Trash2 } from 'lucide-react'
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

const ORDER_TYPE_TO_API: Record<string, string> = {
  LIMIT: 'LMT',
  MARKET: 'MKT',
  SL: 'SL-LMT',
  'SL-M': 'SL-MKT',
  LMT: 'LMT',
  MKT: 'MKT',
  'SL-LMT': 'SL-LMT',
  'SL-MKT': 'SL-MKT',
}

// Per-broker color palette (matches PositionManager)
const BROKER_ROW_TINTS = [
  'border-l-[3px] border-l-[#3b9ede] bg-[#3b9ede]/[0.04]',
  'border-l-[3px] border-l-[#f5a623] bg-[#f5a623]/[0.04]',
  'border-l-[3px] border-l-[#7c3aed] bg-[#7c3aed]/[0.04]',
  'border-l-[3px] border-l-[#10b981] bg-[#10b981]/[0.04]',
]
const BROKER_BADGES = [
  'bg-[#3b9ede]/20 text-[#3b9ede] border border-[#3b9ede]/40',
  'bg-[#f5a623]/20 text-[#f5a623] border border-[#f5a623]/40',
  'bg-[#7c3aed]/20 text-[#7c3aed] border border-[#7c3aed]/40',
  'bg-[#10b981]/20 text-[#10b981] border border-[#10b981]/40',
]

interface ModifyState {
  order: Order
  price: string
  qty: string
  orderType: string
  triggerPrice: string
  validity: string
}

export default function OrderManagement() {
  const { data, orderFilter, setOrderFilter } = useDashboardStore()
  const { accounts: brokerAccounts } = useBrokerAccountsStore()
  const { toast } = useToastStore()
  const [cancelling, setCancelling] = useState<string | null>(null)
  const [cancellingAll, setCancellingAll] = useState(false)
  const [modify, setModify] = useState<ModifyState | null>(null)
  const [modifying, setModifying] = useState(false)

  // Build accountId → broker info map
  const brokerMap = useMemo(() => {
    const m: Record<string, { name: string; shortName: string; idx: number; configId: string }> = {}
    brokerAccounts.forEach((acc, i) => {
      const entry = { name: acc.broker_name, shortName: acc.client_id, idx: i, configId: acc.config_id }
      m[acc.config_id] = entry
      if (acc.client_id) m[acc.client_id] = entry
    })
    return m
  }, [brokerAccounts])

  function resolveAccountId(order: Order) {
    const direct = (order as any).accountId || (order as any).account_id || ''
    if (direct) return direct
    const clientId = (order as any).clientId || ''
    return clientId ? (brokerMap[clientId]?.configId ?? '') : ''
  }

  const orders = useMemo(() => {
    const all = data?.orders ?? []
    // Status priority: PENDING/OPEN first, then CANCELLED, then COMPLETE/REJECTED
    const statusPriority: Record<string, number> = { OPEN: 0, PENDING: 0, AMO: 0, TRIGGER_PENDING: 0, CANCELLED: 1, COMPLETE: 2, REJECTED: 2 }
    const sorted = [...all].sort((a, b) => {
      const pa = statusPriority[a.status] ?? 1
      const pb = statusPriority[b.status] ?? 1
      if (pa !== pb) return pa - pb
      return new Date(b.placedAt).getTime() - new Date(a.placedAt).getTime()
    })
    if (orderFilter === 'all') return sorted
    if (orderFilter === 'pending') return sorted.filter(o => OPEN_STATUSES.includes(o.status))
    if (orderFilter === 'complete') return sorted.filter(o => o.status === 'COMPLETE')
    if (orderFilter === 'cancelled') return sorted.filter(o => o.status === 'CANCELLED')
    if (orderFilter === 'rejected') return sorted.filter(o => o.status === 'REJECTED')
    return sorted
  }, [data?.orders, orderFilter])

  const openOrders = useMemo(() => (data?.orders ?? []).filter(o => OPEN_STATUSES.includes(o.status)), [data?.orders])
  const openCount  = openOrders.length
  const allOrders  = data?.orders ?? []
  const tabCounts  = useMemo(() => ({
    all:       allOrders.length,
    pending:   allOrders.filter(o => OPEN_STATUSES.includes(o.status)).length,
    complete:  allOrders.filter(o => o.status === 'COMPLETE').length,
    cancelled: allOrders.filter(o => o.status === 'CANCELLED').length,
    rejected:  allOrders.filter(o => o.status === 'REJECTED').length,
  }), [allOrders])

  async function cancelOrder(order: Order) {
    const requestOrderId = (order as any).brokerOrderId || (order as any).orderId || order.id
    const accountId = resolveAccountId(order)
    if (!requestOrderId || !accountId) {
      toast('This order is not yet ready for cancellation', 'warning')
      return
    }
    if ((order as any).actionable === false) {
      toast('Waiting for broker order id before cancellation becomes available', 'warning')
      return
    }
    setCancelling(order.id)
    try {
      await api.cancelOrder(requestOrderId, accountId)
      toast(`Cancelled: ${order.tradingsymbol}`, 'success')
    } catch (e: any) {
      toast(e?.message || 'Failed to cancel order', 'error')
    } finally {
      setCancelling(null)
    }
  }

  async function cancelAll() {
    setCancellingAll(true)
    const byAccount: Record<string, Order[]> = {}
    for (const order of openOrders) {
      const accountId = resolveAccountId(order)
      if (!accountId) continue
      byAccount[accountId] = byAccount[accountId] ?? []
      byAccount[accountId].push(order)
    }
    if (Object.keys(byAccount).length === 0) {
      setCancellingAll(false)
      toast('Cannot determine account for cancel-all', 'error')
      return
    }
    try {
      let cancelled = 0
      let errors = 0
      for (const accountId of Object.keys(byAccount)) {
        try {
          const res: any = await api.cancelAllOrders(accountId)
          cancelled += res.cancelled ?? 0
          errors += res.errors?.length ?? 0
        } catch {
          errors += byAccount[accountId].length
        }
      }
      if (errors > 0) toast(`Cancelled ${cancelled} orders, ${errors} failed`, 'warning')
      else toast(`Cancelled ${cancelled} orders`, 'success')
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
      orderType: ORDER_TYPE_TO_API[order.orderType ?? 'LIMIT'] ?? 'LMT',
      triggerPrice: String(order.triggerPrice ?? ''),
      validity: order.validity ?? 'DAY',
    })
  }

  async function submitModify() {
    if (!modify) return
    setModifying(true)
    try {
      const requestOrderId = (modify.order as any).brokerOrderId || (modify.order as any).orderId || modify.order.id
      const accountId = resolveAccountId(modify.order)
      if (!requestOrderId || !accountId) {
        toast('This order is not yet ready for modification', 'warning')
        return
      }
      if ((modify.order as any).actionable === false) {
        toast('Waiting for broker order id before modification becomes available', 'warning')
        return
      }
      await api.modifyOrder(requestOrderId, {
        accountId,
        price: parseFloat(modify.price) || undefined,
        quantity: parseInt(modify.qty) || undefined,
        orderType: modify.orderType,
        triggerPrice: parseFloat(modify.triggerPrice) || undefined,
        validity: modify.validity || undefined,
      })
      toast(`Modified: ${modify.order.tradingsymbol}`, 'success')
      setModify(null)
    } catch (e: any) {
      toast(e?.message || 'Failed to modify order', 'error')
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
        <span className="badge badge-neutral">{allOrders.length}</span>
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
      </div>
      {/* Sub-tabs */}
      <div className="flex items-center gap-1 px-3 py-2 border-b border-border bg-bg-elevated/30">
        {([
          { key: 'pending', label: 'Pending' },
          { key: 'complete', label: 'Complete' },
          { key: 'cancelled', label: 'Cancelled' },
          { key: 'rejected', label: 'Rejected' },
          { key: 'all', label: 'All' },
        ] as const).map(tab => (
          <button
            key={tab.key}
            onClick={() => setOrderFilter(tab.key)}
            className={cn('text-[11px] px-2.5 py-1 rounded border font-medium transition-colors',
              orderFilter === tab.key
                ? 'bg-brand/15 border-brand/50 text-brand'
                : 'border-border text-text-muted hover:text-text-pri hover:border-border-dim')}
          >
            {tab.label}
            {tabCounts[tab.key] > 0 && (
              <span className="ml-1 text-[9px] opacity-70">{tabCounts[tab.key]}</span>
            )}
          </button>
        ))}
      </div>

      <div className="flex-1 overflow-auto">
        {orders.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-32 text-text-muted text-[12px] gap-2">
            <ShoppingCart className="w-8 h-8 opacity-30" />
            {orderFilter !== 'all' ? `No ${orderFilter} orders` : 'No orders today'}
          </div>
        ) : (
          <table className="data-table">
            <thead className="sticky top-0 bg-bg-card z-10">
              <tr>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider">Broker</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-left">Symbol</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Type</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Product</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Qty</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Price</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Trig</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Status</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Time</th>
                <th className="px-3 py-2 w-16"></th>
              </tr>
            </thead>
            <tbody>
              {orders.map(o => {
                const accountId = resolveAccountId(o)
                const broker = brokerMap[accountId] || brokerMap[(o as any).clientId || '']
                const bidx = broker?.idx ?? 0
                const requestOrderId = (o as any).brokerOrderId || (o as any).orderId || o.id
                const canAct = OPEN_STATUSES.includes(o.status) && !!accountId && !!requestOrderId && ((o as any).actionable !== false)
                return (
                <tr key={o.id} className={cn('group transition-colors', BROKER_ROW_TINTS[bidx % BROKER_ROW_TINTS.length])}>
                  {/* Broker */}
                  <td className="px-3 py-2">
                    <div className="flex flex-col gap-0.5">
                      <span className={cn('text-[9px] font-bold px-1.5 py-0.5 rounded-sm uppercase tracking-wide w-fit', BROKER_BADGES[bidx % BROKER_BADGES.length])}>
                        {broker?.name ?? accountId.slice(0, 6)}
                      </span>
                      <span className="text-[9px] text-text-muted">{broker?.shortName ?? ''}</span>
                    </div>
                  </td>
                  <td className="px-3 py-2">
                    <div>
                      <div className="text-[12px] font-medium text-text-bright truncate max-w-[130px]">{o.tradingsymbol}</div>
                      <div className="text-[10px] text-text-muted">{o.exchange}</div>
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
                  <td className="px-3 py-2 text-[10px] text-text-muted">{o.product}</td>
                  <td className="px-3 py-2 text-right">
                    <div className="text-[12px] font-mono text-text-pri">{fmtNum(o.filledQty, 0)}/{fmtNum(o.quantity, 0)}</div>
                  </td>
                  <td className="px-3 py-2 text-right">
                    <div className="text-[12px] font-mono text-text-bright">
                      {o.orderType === 'MARKET' ? 'MKT' : fmtNum(o.price)}
                    </div>
                    {o.avgPrice && o.avgPrice !== o.price && (
                      <div className="text-[10px] font-mono text-text-muted">avg {fmtNum(o.avgPrice)}</div>
                    )}
                  </td>
                  <td className="px-3 py-2 text-right text-[11px] font-mono text-text-sec">
                    {o.triggerPrice ? fmtNum(o.triggerPrice) : '—'}
                  </td>
                  <td className="px-3 py-2">
                    <span className={cn('badge', STATUS_BADGE[o.status] ?? 'badge-neutral')}>
                      {o.status.replace(/_/g,' ')}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-[11px] text-text-muted tabular-nums">{fmtTime(o.placedAt)}</td>
                  <td className="px-3 py-2">
                    {canAct && (
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
                )
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>

    {/* Modify Order Modal */}
    {modify && (
      <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
        <div className="bg-bg-card border border-border rounded-xl p-5 w-[420px] shadow-xl">
          <div className="text-[14px] font-semibold text-text-bright mb-1">
            Modify Order — {modify.order.tradingsymbol}
          </div>
          <div className="text-[11px] text-text-muted mb-4">
            {modify.order.exchange} · {modify.order.product} · {modify.order.transactionType}
          </div>
          <div className="grid grid-cols-2 gap-3">
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
              <label className="text-[11px] text-text-muted mb-1 block">Validity</label>
              <select
                value={modify.validity}
                onChange={e => setModify(m => m ? {...m, validity: e.target.value} : null)}
                className="input-sm w-full"
              >
                <option value="DAY">DAY</option>
                <option value="IOC">IOC</option>
                <option value="GTT">GTT</option>
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
              <label className="text-[11px] text-text-muted mb-1 block">Trigger Price</label>
              <input
                type="number"
                step="0.05"
                value={modify.triggerPrice}
                onChange={e => setModify(m => m ? {...m, triggerPrice: e.target.value} : null)}
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
