/* ═══════════════════════════════
   Dashboard — Holdings Table
   ═══════════════════════════════ */
import { useState, useMemo } from 'react'
import { useDashboardStore, useUIStore } from '../../stores'
import { cn, fmtINR, fmtNum, pnlClass } from '../../lib/utils'
import { BarChart2, Package, ChevronsUpDown } from 'lucide-react'
import type { Holding } from '../../types'

type SortKey = 'pnl' | 'pnlPct' | 'currentValue' | 'symbol' | 'dayChange'

export default function HoldingsTable() {
  const { data } = useDashboardStore()
  const { openChartModal } = useUIStore()
  const [sortKey, setSortKey] = useState<SortKey>('pnl')
  const [sortDir, setSortDir] = useState<1 | -1>(-1)

  const holdings = useMemo(() => {
    const hs = data?.holdings ?? []
    return [...hs].sort((a, b) => {
      const av = a[sortKey] as any, bv = b[sortKey] as any
      if (typeof av === 'number') return (av - bv) * sortDir
      return String(av).localeCompare(String(bv)) * sortDir
    })
  }, [data?.holdings, sortKey, sortDir])

  const totalInvested = holdings.reduce((s, h) => s + h.investedValue, 0)
  const totalCurrent  = holdings.reduce((s, h) => s + h.currentValue, 0)
  const totalPnl      = totalCurrent - totalInvested

  function toggleSort(key: SortKey) {
    if (sortKey === key) setSortDir(d => d === 1 ? -1 : 1)
    else { setSortKey(key); setSortDir(-1) }
  }

  const SH = ({ label, col }: { label: string; col: SortKey }) => (
    <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider cursor-pointer hover:text-text-sec select-none text-right"
      onClick={() => toggleSort(col)}>
      <div className="flex items-center justify-end gap-1">
        {label}
        {sortKey === col && <ChevronsUpDown className="w-3 h-3" />}
      </div>
    </th>
  )

  return (
    <div className="bg-bg-card border border-border rounded-lg flex flex-col h-full">
      <div className="flex items-center gap-3 px-4 py-3 border-b border-border">
        <Package className="w-4 h-4 text-accent" />
        <span className="text-[13px] font-semibold text-text-bright">Holdings</span>
        <span className="badge badge-neutral">{holdings.length}</span>
        <div className="flex-1" />
        <span className={cn('text-[12px] font-mono font-semibold', pnlClass(totalPnl))}>
          {totalPnl >= 0 ? '+' : ''}{fmtINR(totalPnl)}
        </span>
      </div>

      <div className="flex-1 overflow-auto">
        {holdings.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-32 text-text-muted text-[12px] gap-2">
            <Package className="w-8 h-8 opacity-30" />
            No holdings found
          </div>
        ) : (
          <table className="data-table">
            <thead className="sticky top-0 bg-bg-card z-10">
              <tr>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-left cursor-pointer hover:text-text-sec"
                  onClick={() => toggleSort('symbol')}>Symbol</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Qty</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Avg Cost</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">LTP</th>
                <SH label="Current" col="currentValue" />
                <SH label="P&L" col="pnl" />
                <SH label="P&L %" col="pnlPct" />
                <SH label="Day %" col="dayChange" />
                <th className="px-3 py-2 w-8"></th>
              </tr>
            </thead>
            <tbody>
              {holdings.map(h => (
                <tr key={h.id} className="group">
                  <td className="px-3 py-2">
                    <div>
                      <div className="text-[12px] font-medium text-text-bright">{h.symbol}</div>
                      <div className="text-[10px] text-text-muted">{h.exchange}</div>
                    </div>
                  </td>
                  <td className="px-3 py-2 text-[12px] font-mono text-right text-text-pri">{h.quantity}</td>
                  <td className="px-3 py-2 text-[12px] font-mono text-right text-text-sec">{fmtNum(h.avgCost)}</td>
                  <td className="px-3 py-2 text-[12px] font-mono text-right text-text-bright">{fmtNum(h.ltp)}</td>
                  <td className="px-3 py-2 text-[12px] font-mono text-right text-text-pri">{fmtINR(h.currentValue)}</td>
                  <td className={cn('px-3 py-2 text-[12px] font-mono font-semibold text-right', pnlClass(h.pnl))}>
                    {h.pnl >= 0 ? '+' : ''}{fmtINR(h.pnl)}
                  </td>
                  <td className={cn('px-3 py-2 text-[11px] font-mono text-right', pnlClass(h.pnlPct ?? 0))}>
                    {(h.pnlPct ?? 0) >= 0 ? '+' : ''}{(h.pnlPct ?? 0).toFixed(2)}%
                  </td>
                  <td className={cn('px-3 py-2 text-[11px] font-mono text-right', pnlClass(h.dayChange ?? 0))}>
                    {(h.dayChange ?? 0) >= 0 ? '+' : ''}{(h.dayChangePct ?? 0).toFixed(2)}%
                  </td>
                  <td className="px-3 py-2">
                    <button
                      onClick={() => openChartModal(h.symbol)}
                      className="btn-ghost btn-xs !px-1.5 !py-1 opacity-0 group-hover:opacity-100 transition-opacity"
                    >
                      <BarChart2 className="w-3 h-3" />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
            <tfoot>
              <tr className="border-t border-border bg-bg-elevated/50">
                <td className="px-3 py-2 text-[11px] text-text-muted" colSpan={4}>Total Portfolio</td>
                <td className="px-3 py-2 text-[11px] font-mono text-right text-text-pri">{fmtINR(totalCurrent)}</td>
                <td className={cn('px-3 py-2 text-[12px] font-mono font-bold text-right', pnlClass(totalPnl))}>
                  {totalPnl >= 0 ? '+' : ''}{fmtINR(totalPnl)}
                </td>
                <td className={cn('px-3 py-2 text-[11px] font-mono text-right', pnlClass(totalPnl))}>
                  {((totalPnl / totalInvested) * 100).toFixed(2)}%
                </td>
                <td colSpan={2}></td>
              </tr>
            </tfoot>
          </table>
        )}
      </div>
    </div>
  )
}
