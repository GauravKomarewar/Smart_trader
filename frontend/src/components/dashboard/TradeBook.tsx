/* ═══════════════════════════════════
   Dashboard — Tradebook (raw trades)
   ═══════════════════════════════════ */
import { useDashboardStore } from '../../stores'
import { cn, fmtINR, fmtNum, fmtTime } from '../../lib/utils'
import { FileText } from 'lucide-react'

export default function TradeBook() {
  const trades = useDashboardStore(s => s.data?.trades ?? [])

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
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-left">Symbol</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Side</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Qty</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Price</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Value</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Charges</th>
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
                return (
                <tr key={tid}>
                  <td className="px-3 py-2">
                    <div className="text-[12px] font-medium text-text-bright truncate max-w-[130px]">{sym}</div>
                    <div className="text-[10px] text-text-muted">{(t as any).exchange} · {(t as any).product}</div>
                  </td>
                  <td className="px-3 py-2">
                    <span className={cn('badge', side === 'BUY' ? 'badge-buy' : 'badge-sell')}>
                      {side || '—'}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-[12px] font-mono text-right text-text-pri">{qty}</td>
                  <td className="px-3 py-2 text-[12px] font-mono text-right text-text-bright">{fmtNum(price)}</td>
                  <td className="px-3 py-2 text-[12px] font-mono text-right text-text-pri">{fmtINR(value)}</td>
                  <td className="px-3 py-2 text-[11px] font-mono text-right text-loss">{fmtINR(charges)}</td>
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
