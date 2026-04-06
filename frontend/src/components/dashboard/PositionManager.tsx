/* ═══════════════════════════════
   Dashboard — Position Manager
   Pro-grade with sq-off actions
   Shows OPEN + CLOSED positions
   ═══════════════════════════════ */
import { useState, useMemo, useRef } from 'react'
import { useDashboardStore, useToastStore, useUIStore } from '../../stores'
import { cn, fmtINR, fmtNum, pnlClass } from '../../lib/utils'
import { api } from '../../lib/api'
import { TrendingUp, TrendingDown, RefreshCw, XCircle, ChevronsUpDown, BarChart2, Filter } from 'lucide-react'
import type { Position } from '../../types'

type SortKey = 'pnl' | 'value' | 'symbol' | 'pnlPct'
type PosFilter = 'all' | 'open' | 'closed'

export default function PositionManager() {
  const { data, setData } = useDashboardStore()
  const { toast } = useToastStore()
  const { openOrderModal, openChartModal } = useUIStore()

  const [sortKey, setSortKey] = useState<SortKey>('pnl')
  const [sortDir, setSortDir] = useState<1 | -1>(-1)
  const [filter, setFilter] = useState<PosFilter>('all')
  const [sqOffLoading, setSqOffLoading] = useState<string | null>(null)
  const [sqOffAllLoading, setSqOffAllLoading] = useState(false)

  // Use a stable reference to avoid re-renders from identical data
  const prevRef = useRef<string>('')

  const positions = useMemo(() => {
    const ps = data?.positions ?? []
    // Filter by status
    const filtered = filter === 'all' ? ps
      : ps.filter((p: any) => filter === 'open' ? p.status !== 'CLOSED' : p.status === 'CLOSED')
    // Backend already sorts (OPEN first, then by PnL). Apply user sort on top.
    return [...filtered].sort((a, b) => {
      // Always keep OPEN before CLOSED
      const statusA = (a as any).status === 'CLOSED' ? 1 : 0
      const statusB = (b as any).status === 'CLOSED' ? 1 : 0
      if (statusA !== statusB) return statusA - statusB
      const av = a[sortKey] as number, bv = b[sortKey] as number
      if (typeof av === 'number') return (av - bv) * sortDir
      return String(av).localeCompare(String(bv)) * sortDir
    })
  }, [data?.positions, sortKey, sortDir, filter])

  const openPositions = useMemo(() =>
    (data?.positions ?? []).filter((p: any) => p.status !== 'CLOSED'),
  [data?.positions])

  const totalPnl = positions.reduce((s, p) => s + p.pnl, 0)
  const totalValue = positions.reduce((s, p) => s + Math.abs(p.value), 0)
  const openCount = openPositions.length
  const closedCount = (data?.positions ?? []).length - openCount

  function toggleSort(key: SortKey) {
    if (sortKey === key) setSortDir(d => d === 1 ? -1 : 1)
    else { setSortKey(key); setSortDir(-1) }
  }

  async function squareOff(pos: Position) {
    setSqOffLoading(pos.id)
    try {
      await api.squareOff(pos.id)
      toast(`Squared off ${pos.symbol}`, 'success')
    } catch {
      toast(`Failed to square off ${pos.symbol}`, 'error')
    } finally {
      setSqOffLoading(null)
    }
  }

  async function squareOffAll() {
    if (!openPositions.length) return
    const accountId = openPositions[0].accountId
    setSqOffAllLoading(true)
    try {
      await api.squareOffAll(accountId)
      toast('All positions squared off', 'success')
    } catch {
      toast('Failed to square off all positions', 'error')
    } finally {
      setSqOffAllLoading(false)
    }
  }

  const SortHdr = ({ label, col }: { label: string; col: SortKey }) => (
    <th
      className="px-3 py-2 text-left text-[10px] font-medium text-text-muted uppercase tracking-wider cursor-pointer hover:text-text-sec select-none"
      onClick={() => toggleSort(col)}
    >
      <div className="flex items-center gap-1">
        {label}
        {sortKey === col && <ChevronsUpDown className="w-3 h-3" />}
      </div>
    </th>
  )

  return (
    <div className="bg-bg-card border border-border rounded-lg flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center gap-3 px-4 py-3 border-b border-border">
        <TrendingUp className="w-4 h-4 text-brand" />
        <span className="text-[13px] font-semibold text-text-bright">Positions</span>
        <span className="badge badge-neutral">{positions.length}</span>

        {/* Filter pills */}
        <div className="flex items-center gap-1 ml-2">
          {(['all', 'open', 'closed'] as PosFilter[]).map(f => (
            <button key={f} onClick={() => setFilter(f)}
              className={cn(
                'text-[10px] px-2 py-0.5 rounded-full border transition-colors capitalize',
                filter === f
                  ? 'border-brand text-brand bg-brand/10'
                  : 'border-border text-text-muted hover:text-text-sec'
              )}
            >
              {f}{f === 'open' ? ` (${openCount})` : f === 'closed' ? ` (${closedCount})` : ''}
            </button>
          ))}
        </div>

        <div className="flex-1" />
        <span className={cn('text-[12px] font-mono font-semibold', pnlClass(totalPnl))}>
          {totalPnl >= 0 ? '+' : ''}{fmtINR(totalPnl)}
        </span>
        {openPositions.length > 0 && (
          <button
            onClick={squareOffAll}
            disabled={sqOffAllLoading}
            className="btn-danger btn-xs"
          >
            {sqOffAllLoading ? <RefreshCw className="w-3 h-3 animate-spin" /> : <XCircle className="w-3 h-3" />}
            Sq. All
          </button>
        )}
      </div>

      {/* Table */}
      <div className="flex-1 overflow-auto">
        {positions.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-32 text-text-muted text-[12px] gap-2">
            <BarChart2 className="w-8 h-8 opacity-30" />
            No positions
          </div>
        ) : (
          <table className="data-table">
            <thead className="sticky top-0 bg-bg-card z-10">
              <tr>
                <SortHdr label="Symbol" col="symbol" />
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Status</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Qty</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Avg</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">LTP</th>
                <SortHdr label="P&L" col="pnl" />
                <SortHdr label="%" col="pnlPct" />
                <SortHdr label="Value" col="value" />
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Exch</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Broker</th>
                <th className="px-3 py-2 w-20"></th>
              </tr>
            </thead>
            <tbody>
              {positions.map(p => {
                const isClosed = (p as any).status === 'CLOSED'
                return (
                <tr key={p.id} className={cn('group', isClosed && 'opacity-60')}>
                  <td className="px-3 py-2">
                    <div className="flex flex-col">
                      <span className="text-[12px] font-medium text-text-bright leading-tight max-w-[140px] truncate">{p.tradingsymbol}</span>
                      <div className="flex items-center gap-1.5 mt-0.5">
                        <span className={cn('badge text-[9px]', p.side === 'BUY' ? 'badge-buy' : 'badge-sell')}>{p.side}</span>
                        <span className="text-[10px] text-text-muted">{p.product}</span>
                      </div>
                    </div>
                  </td>
                  <td className="px-3 py-2">
                    <span className={cn('badge text-[9px]',
                      isClosed ? 'bg-text-muted/10 text-text-muted border-text-muted/20' : 'bg-profit/10 text-profit border-profit/20'
                    )}>
                      {isClosed ? 'CLOSED' : 'OPEN'}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-[12px] font-mono text-text-pri">{p.quantity}</td>
                  <td className="px-3 py-2 text-[12px] font-mono text-right text-text-sec">{fmtNum(p.avgPrice)}</td>
                  <td className="px-3 py-2 text-[12px] font-mono text-right text-text-bright">{fmtNum(p.ltp)}</td>
                  <td className={cn('px-3 py-2 text-[12px] font-mono font-semibold', pnlClass(p.pnl))}>
                    {p.pnl >= 0 ? '+' : ''}{fmtINR(p.pnl)}
                  </td>
                  <td className={cn('px-3 py-2 text-[11px] font-mono', pnlClass(p.pnlPct ?? 0))}>
                    {(p.pnlPct ?? 0) >= 0 ? '+' : ''}{(p.pnlPct ?? 0).toFixed(2)}%
                  </td>
                  <td className="px-3 py-2 text-[11px] font-mono text-right text-text-sec">{fmtINR(Math.abs(p.value ?? 0))}</td>
                  <td className="px-3 py-2 text-[10px] text-text-muted">{(p as any).exchange || ''}</td>
                  <td className="px-3 py-2 text-[10px] text-text-muted">{(p as any).accountId?.slice(0, 8) || ''}</td>
                  <td className="px-3 py-2">
                    {!isClosed && (
                    <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                      <button
                        onClick={() => openChartModal(p.symbol)}
                        className="btn-ghost btn-xs !px-1.5 !py-1"
                        title="Chart"
                      >
                        <BarChart2 className="w-3 h-3" />
                      </button>
                      <button
                        onClick={() => squareOff(p)}
                        disabled={sqOffLoading === p.id}
                        className="btn-danger btn-xs !px-1.5 !py-1"
                        title="Square off"
                      >
                        {sqOffLoading === p.id
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
            {/* Footer */}
            <tfoot>
              <tr className="border-t border-border bg-bg-elevated/50">
                <td className="px-3 py-2 text-[11px] text-text-muted" colSpan={5}>Total</td>
                <td className={cn('px-3 py-2 text-[12px] font-mono font-bold', pnlClass(totalPnl))}>
                  {totalPnl >= 0 ? '+' : ''}{fmtINR(totalPnl)}
                </td>
                <td></td>
                <td className="px-3 py-2 text-[11px] font-mono text-right text-text-sec">{fmtINR(totalValue)}</td>
                <td colSpan={3}></td>
              </tr>
            </tfoot>
          </table>
        )}
      </div>
    </div>
  )
}
