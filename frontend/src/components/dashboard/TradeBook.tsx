/* ═══════════════════════════════════
   Dashboard — Tradebook (raw trades)
   ═══════════════════════════════════ */
import { useMemo } from 'react'
import { useDashboardStore, useBrokerAccountsStore } from '../../stores'
import { cn, fmtINR, fmtNum, fmtTime } from '../../lib/utils'
import { FileText } from 'lucide-react'

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

export default function TradeBook() {
  const trades = useDashboardStore(s => s.data?.trades ?? [])
  const { accounts: brokerAccounts } = useBrokerAccountsStore()

  const brokerMap = useMemo(() => {
    const m: Record<string, { name: string; shortName: string; idx: number }> = {}
    brokerAccounts.forEach((acc, i) => {
      m[acc.config_id] = { name: acc.broker_name, shortName: acc.client_id, idx: i }
    })
    return m
  }, [brokerAccounts])

  return (
    <div className="bg-bg-card border border-border rounded-lg flex flex-col h-full">
      <div className="flex items-center gap-3 px-4 py-3 border-b border-border">
        <FileText className="w-4 h-4 text-text-sec" />
        <span className="text-[13px] font-semibold text-text-bright">Trade Book</span>
        <span className="badge badge-neutral">{trades.length}</span>
        <span className="ml-auto text-[10px] text-text-muted">Broker raw data</span>
      </div>

      <div className="flex-1 overflow-auto">
        {trades.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-32 text-text-muted text-[12px] gap-2">
            <FileText className="w-8 h-8 opacity-30" />
            No trades today
          </div>
        ) : (
          <table className="data-table">
            <thead className="sticky top-0 bg-bg-card z-10">
              <tr>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider">Broker</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-left">Symbol</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Side</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Product</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Qty</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Price</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Value</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Charges</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Order ID</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Time</th>
              </tr>
            </thead>
            <tbody>
              {trades.map((t, i) => {
                const sym      = (t as any).tradingsymbol ?? (t as any).symbol ?? '—'
                const side     = ((t as any).transactionType ?? (t as any).side ?? '').toUpperCase()
                const qty      = (t as any).quantity ?? (t as any).qty ?? 0
                const price    = (t as any).price ?? 0
                const value    = (t as any).value != null ? (t as any).value : qty * price
                const charges  = (t as any).charges ?? 0
                const tradedAt = (t as any).tradedAt ?? (t as any).timestamp ?? ''
                const tid      = (t as any).id ?? (t as any).trade_id ?? String(i)
                const orderId  = (t as any).orderId ?? (t as any).order_id ?? ''
                const exchange = (t as any).exchange ?? ''
                const product  = (t as any).product ?? ''
                const accountId = (t as any).account_id ?? (t as any).accountId ?? ''
                const broker   = brokerMap[accountId]
                const bidx     = broker?.idx ?? 0
                return (
                <tr key={tid} className={cn('transition-colors', BROKER_ROW_TINTS[bidx % BROKER_ROW_TINTS.length])}>
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
                    <div className="text-[12px] font-medium text-text-bright truncate max-w-[130px]">{sym}</div>
                    <div className="text-[10px] text-text-muted">{exchange}</div>
                  </td>
                  <td className="px-3 py-2">
                    <span className={cn('badge', side === 'BUY' ? 'badge-buy' : 'badge-sell')}>
                      {side || '—'}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-[10px] text-text-muted">{product}</td>
                  <td className="px-3 py-2 text-[12px] font-mono text-right text-text-pri">{qty}</td>
                  <td className="px-3 py-2 text-[12px] font-mono text-right text-text-bright">{fmtNum(price)}</td>
                  <td className="px-3 py-2 text-[12px] font-mono text-right text-text-pri">{fmtINR(value)}</td>
                  <td className="px-3 py-2 text-[11px] font-mono text-right text-loss">{fmtINR(charges)}</td>
                  <td className="px-3 py-2 text-[10px] font-mono text-text-muted truncate max-w-[80px]" title={orderId}>{orderId ? orderId.slice(-6) : '—'}</td>
                  <td className="px-3 py-2 text-[11px] text-text-muted tabular-nums">{fmtTime(tradedAt)}</td>
                </tr>
                )
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
