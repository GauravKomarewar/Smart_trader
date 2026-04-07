/* ═══════════════════════════════════════════════════════════════════════
   Dashboard — Position Manager (Pro Mode)
   
   • Broker name as first column with color-coded row highlighting
   • Filter: All / Open / Closed pills
   • Toggle: "Active Only" + "⚙ Position Manager" mode
   • Position Manager mode: SL / Target / Trail / Trail@ editable inputs
     with Activate/Deactivate per row (stored in pendingEdits)
   • EXIT button always visible (not just on hover)
   • EXIT ALL targets open positions only
   ═══════════════════════════════════════════════════════════════════════ */
import { useState, useMemo, useRef, useCallback, useEffect } from 'react'
import { useDashboardStore, useBrokerAccountsStore, useToastStore, useUIStore } from '../../stores'
import { cn, fmtINR, fmtNum, pnlClass, pnlSign } from '../../lib/utils'
import { api } from '../../lib/api'
import { TrendingUp, RefreshCw, XCircle, ChevronsUpDown, BarChart2, Settings2 } from 'lucide-react'
import type { Position } from '../../types'

type SortKey = 'pnl' | 'value' | 'symbol' | 'pnlPct'
type PosFilter = 'all' | 'open' | 'closed'

// Per-broker color palette  (must match BrokerAccountsPage tints)
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

interface PmEdits {
  [key: string]: { stop_loss?: string; target?: string; trailing_value?: string; trail_when?: string }
}
interface ManagedExit {
  [key: string]: { stop_loss?: number; target?: number; trailing_value?: number; trail_when?: number; initial_ltp?: number; base_stop_loss?: number; trail_stop?: number; active: boolean }
}

export default function PositionManager() {
  const { data, setData } = useDashboardStore()
  const { accounts: brokerAccounts } = useBrokerAccountsStore()
  const { toast } = useToastStore()
  const { openChartModal } = useUIStore()

  const [sortKey, setSortKey]         = useState<SortKey>('pnl')
  const [sortDir, setSortDir]         = useState<1 | -1>(-1)
  const [filter, setFilter]           = useState<PosFilter>('all')
  const [showActiveOnly, setShowActiveOnly] = useState(false)
  const [pmMode, setPmMode]           = useState(false)
  const [sqOffLoading, setSqOffLoading]     = useState<string | null>(null)
  const [sqOffAllLoading, setSqOffAllLoading] = useState(false)

  // Position Manager state: pending user edits + managed exits
  const [pmEdits, setPmEdits]         = useState<PmEdits>({})
  const [managed, setManaged]         = useState<ManagedExit>({})

  // Load saved SL settings from server on mount and refresh periodically
  useEffect(() => {
    let cancelled = false
    const load = async () => {
      try {
        const res: any = await api.getSLSettings()
        if (cancelled) return
        const settings = res?.data ?? []
        const mgd: ManagedExit = {}
        for (const s of settings) {
          if (s.pos_key && s.active) {
            mgd[s.pos_key] = {
              stop_loss: s.stop_loss ?? undefined,
              target: s.target ?? undefined,
              trailing_value: s.trailing_value ?? undefined,
              trail_when: s.trail_when ?? undefined,
              initial_ltp: s.initial_ltp ?? undefined,
              base_stop_loss: s.base_stop_loss ?? undefined,
              trail_stop: s.trail_stop ?? undefined,
              active: true,
            }
          }
        }
        setManaged(prev => {
          // Merge server state with any in-progress user edits
          const merged = { ...mgd }
          // Preserve local managed entries that user just activated but server hasn't synced yet
          for (const [k, v] of Object.entries(prev)) {
            if (v.active && !merged[k]) merged[k] = v
          }
          return merged
        })
      } catch { /* silent */ }
    }
    load()
    const interval = window.setInterval(load, 10000)  // refresh every 10s
    return () => { cancelled = true; clearInterval(interval) }
  }, [])

  // Build accountId → { brokerName, idx } map from broker accounts store
  const brokerMap = useMemo(() => {
    const m: Record<string, { name: string; shortName: string; idx: number }> = {}
    brokerAccounts.forEach((acc, i) => {
      m[acc.config_id] = { name: acc.broker_name, shortName: acc.client_id, idx: i }
    })
    return m
  }, [brokerAccounts])

  const allPositions = data?.positions ?? []
  const openPositions = useMemo(() => allPositions.filter((p: any) => p.status !== 'CLOSED'), [allPositions])
  const closedPositions = useMemo(() => allPositions.filter((p: any) => p.status === 'CLOSED'), [allPositions])

  const filtered = useMemo(() => {
    let ps = allPositions
    if (filter === 'open')   ps = openPositions
    if (filter === 'closed') ps = closedPositions
    if (showActiveOnly)      ps = ps.filter((p: any) => p.status !== 'CLOSED')
    return [...ps].sort((a, b) => {
      const closedA = (a as any).status === 'CLOSED' ? 1 : 0
      const closedB = (b as any).status === 'CLOSED' ? 1 : 0
      if (closedA !== closedB) return closedA - closedB
      const av = (a as any)[sortKey] as number
      const bv = (b as any)[sortKey] as number
      if (typeof av === 'number') return (av - bv) * sortDir
      return String(av).localeCompare(String(bv)) * sortDir
    })
  }, [allPositions, openPositions, closedPositions, filter, showActiveOnly, sortKey, sortDir])

  const totalPnl   = filtered.reduce((s, p) => s + p.pnl, 0)
  const totalValue = filtered.reduce((s, p) => s + Math.abs(p.value ?? 0), 0)

  function toggleSort(key: SortKey) {
    if (sortKey === key) setSortDir(d => d === 1 ? -1 : 1)
    else { setSortKey(key); setSortDir(-1) }
  }

  async function squareOff(pos: Position) {
    const info = brokerMap[(pos as any).accountId]
    if (!info) { toast('Cannot exit: broker info not found', 'error'); return }
    setSqOffLoading(pos.id)
    try {
      await api.squareOff({
        symbol:    pos.tradingsymbol ?? pos.symbol,
        exchange:  (pos as any).exchange ?? 'NSE',
        product:   (pos as any).product  ?? 'NRML',
        quantity:  Math.abs(pos.quantity),
        side:      pos.side ?? (pos.quantity > 0 ? 'BUY' : 'SELL'),
        accountId: (pos as any).accountId,
      })
      toast(`Exit sent: ${pos.tradingsymbol ?? pos.symbol}`, 'success')
    } catch (e: any) {
      toast(`Exit failed: ${e?.message ?? 'error'}`, 'error')
    } finally {
      setSqOffLoading(null)
    }
  }

  async function squareOffAll() {
    if (!openPositions.length) return
    // Group by accountId and exit each broker's positions
    const byAccount: Record<string, any[]> = {}
    for (const p of openPositions) {
      const aid = (p as any).accountId
      if (aid) { byAccount[aid] = byAccount[aid] ?? []; byAccount[aid].push(p) }
    }
    setSqOffAllLoading(true)
    let placed = 0, errors = 0
    try {
      for (const [accountId] of Object.entries(byAccount)) {
        try {
          const res: any = await api.squareOffAll(accountId)
          placed += res.placed ?? 0
          errors += (res.errors?.length ?? 0)
        } catch { errors++ }
      }
      if (errors === 0) toast(`All ${placed} positions exited`, 'success')
      else toast(`${placed} exited, ${errors} errors`, 'warning')
    } finally {
      setSqOffAllLoading(false)
    }
  }

  // PM: update a pending edit field
  function setPmField(key: string, field: string, value: string) {
    setPmEdits(prev => ({
      ...prev,
      [key]: { ...(prev[key] ?? {}), [field]: value },
    }))
  }

  // PM: activate/update managed exit for a row
  async function activateManaged(posKey: string, accountId: string, currentLtp?: number) {
    const edits = pmEdits[posKey] ?? {}
    const prev = managed[posKey]
    const settings = {
      stop_loss:      edits.stop_loss      ? parseFloat(edits.stop_loss)      : prev?.stop_loss,
      target:         edits.target         ? parseFloat(edits.target)         : prev?.target,
      trailing_value: edits.trailing_value ? parseFloat(edits.trailing_value) : prev?.trailing_value,
      trail_when:     edits.trail_when     ? parseFloat(edits.trail_when)     : prev?.trail_when,
    }
    // Capture initial_ltp on first activation (current LTP at activation time)
    const initialLtp = prev?.initial_ltp ?? currentLtp ?? undefined
    // base_stop_loss = original SL before any trailing
    const baseStopLoss = prev?.base_stop_loss ?? settings.stop_loss ?? undefined

    setManaged(prev => ({
      ...prev,
      [posKey]: { active: true, ...settings, initial_ltp: initialLtp, base_stop_loss: baseStopLoss },
    }))
    setPmEdits(prev => { const n = { ...prev }; delete n[posKey]; return n })
    try {
      await api.setSLSettings({
        configId: accountId,
        posKey,
        active: true,
        stopLoss: settings.stop_loss || null,
        target: settings.target || null,
        trailingValue: settings.trailing_value || null,
        trailWhen: settings.trail_when || null,
        initialLtp: initialLtp || null,
        baseStopLoss: baseStopLoss || null,
      })
      toast(`Position manager ACTIVE — SL/TG/Trail monitoring started`, 'success')
    } catch {
      toast('Failed to save SL settings to server', 'error')
    }
  }

  async function deactivateManaged(posKey: string, accountId: string) {
    setManaged(prev => { const n = { ...prev }; delete n[posKey]; return n })
    setPmEdits(prev => { const n = { ...prev }; delete n[posKey]; return n })
    try {
      await api.setSLSettings({ configId: accountId, posKey, active: false })
    } catch { /* non-fatal */ }
    toast(`Position manager deactivated`, 'info')
  }

  const SortHdr = ({ label, col }: { label: string; col: SortKey }) => (
    <th className="px-3 py-2 text-left text-[10px] font-medium text-text-muted uppercase tracking-wider cursor-pointer hover:text-text-sec select-none"
      onClick={() => toggleSort(col)}>
      <div className="flex items-center gap-1">
        {label}
        {sortKey === col && <ChevronsUpDown className="w-3 h-3" />}
      </div>
    </th>
  )

  return (
    <div className="bg-bg-card border border-border rounded-lg flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center gap-2 px-4 py-2.5 border-b border-border flex-wrap gap-y-2">
        <TrendingUp className="w-4 h-4 text-brand shrink-0" />
        <span className="text-[13px] font-semibold text-text-bright">Positions</span>
        <span className="badge badge-neutral">{filtered.length}</span>

        {/* Filter pills */}
        {(['all', 'open', 'closed'] as PosFilter[]).map(f => (
          <button key={f} onClick={() => setFilter(f)}
            className={cn(
              'text-[10px] px-2 py-0.5 rounded-full border transition-colors capitalize',
              filter === f ? 'border-brand text-brand bg-brand/10' : 'border-border text-text-muted hover:text-text-sec'
            )}>
            {f}{f === 'open' ? ` (${openPositions.length})` : f === 'closed' ? ` (${closedPositions.length})` : ''}
          </button>
        ))}

        <div className="flex-1" />

        {/* Active only toggle */}
        <button onClick={() => setShowActiveOnly(v => !v)}
          className={cn(
            'text-[11px] px-2.5 py-1 rounded-md border transition-colors font-medium',
            showActiveOnly ? 'bg-brand border-brand text-white' : 'border-border text-text-muted hover:border-brand/40'
          )}>
          Active Only
        </button>

        {/* Position Manager toggle */}
        <button onClick={() => setPmMode(v => !v)}
          className={cn(
            'flex items-center gap-1.5 text-[11px] px-2.5 py-1 rounded-md border transition-colors font-bold',
            pmMode ? 'bg-brand border-brand text-white' : 'border-brand text-brand hover:bg-brand/10'
          )}>
          <Settings2 className="w-3.5 h-3.5" />
          Position Manager
        </button>

        <span className={cn('text-[12px] font-mono font-semibold', pnlClass(totalPnl))}>
          {totalPnl >= 0 ? '+' : ''}{fmtINR(totalPnl)}
        </span>

        {openPositions.length > 0 && (
          <button onClick={squareOffAll} disabled={sqOffAllLoading}
            className="flex items-center gap-1 btn-danger btn-xs">
            {sqOffAllLoading ? <RefreshCw className="w-3 h-3 animate-spin" /> : <XCircle className="w-3 h-3" />}
            Exit All
          </button>
        )}
      </div>

      {/* Table */}
      <div className="flex-1 overflow-auto">
        {filtered.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-32 text-text-muted text-[12px] gap-2">
            <BarChart2 className="w-8 h-8 opacity-30" />
            No positions
          </div>
        ) : (
          <table className="data-table">
            <thead className="sticky top-0 bg-bg-card z-10">
              <tr>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider">Broker</th>
                <SortHdr label="Symbol" col="symbol" />
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider">Status</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider text-right">Qty</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider text-right">Avg</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider text-right">LTP</th>
                <SortHdr label="P&L" col="pnl" />
                <SortHdr label="%" col="pnlPct" />
                <SortHdr label="Value" col="value" />
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider">Exch</th>
                {pmMode && <>
                  <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-center">SL</th>
                  <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-center">Target</th>
                  <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-center">Trail</th>
                  <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-center">Trail@</th>
                  <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-center whitespace-nowrap">Start LTP</th>
                  <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-center">Act@</th>
                  <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-center">Status</th>
                  <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-center">Action</th>
                </>}
                <th className="px-3 py-2 w-20"></th>
              </tr>
            </thead>
            <tbody>
              {filtered.map(p => {
                const isClosed  = (p as any).status === 'CLOSED'
                const accountId = (p as any).accountId ?? ''
                const broker    = brokerMap[accountId]
                const bidx      = broker?.idx ?? 0
                const posKey    = `${p.tradingsymbol ?? p.symbol}|${(p as any).product ?? ''}`
                const isMgd     = !!managed[posKey]
                const edits     = pmEdits[posKey] ?? {}

                return (
                  <tr key={p.id}
                    className={cn(
                      'transition-colors',
                      isClosed ? 'opacity-50' : BROKER_ROW_TINTS[bidx % BROKER_ROW_TINTS.length],
                      !isClosed && p.side === 'BUY'  && 'hover:bg-profit/5',
                      !isClosed && p.side === 'SELL' && 'hover:bg-loss/5',
                    )}
                  >
                    {/* Broker column */}
                    <td className="px-3 py-2">
                      <div className="flex flex-col gap-0.5">
                        <span className={cn('text-[9px] font-bold px-1.5 py-0.5 rounded-sm uppercase tracking-wide w-fit', BROKER_BADGES[bidx % BROKER_BADGES.length])}>
                          {broker?.name ?? accountId.slice(0, 6)}
                        </span>
                        <span className="text-[9px] text-text-muted">{broker?.shortName ?? ''}</span>
                      </div>
                    </td>

                    {/* Symbol */}
                    <td className="px-3 py-2">
                      <div className="font-semibold text-[12px] text-text-bright leading-tight">{p.tradingsymbol}</div>
                      <div className="flex items-center gap-1 mt-0.5">
                        <span className={cn('badge text-[9px]', p.side === 'BUY' ? 'badge-buy' : 'badge-sell')}>{p.side}</span>
                        <span className="text-[10px] text-text-muted">{(p as any).product}</span>
                      </div>
                    </td>

                    {/* Status */}
                    <td className="px-3 py-2">
                      <span className={cn('badge text-[9px]',
                        isClosed ? 'bg-text-muted/10 text-text-muted border-text-muted/20' : 'bg-profit/10 text-profit border-profit/20'
                      )}>
                        {isClosed ? 'CLOSED' : 'OPEN'}
                      </span>
                    </td>

                    <td className="px-3 py-2 text-right font-mono text-[12px] text-text-pri">{p.quantity}</td>
                    <td className="px-3 py-2 text-right font-mono text-[12px] text-text-sec">{fmtNum(p.avgPrice)}</td>
                    <td className="px-3 py-2 text-right font-mono text-[12px] text-text-bright">{fmtNum(p.ltp)}</td>
                    <td className={cn('px-3 py-2 font-mono font-semibold text-[12px]', pnlClass(p.pnl))}>
                      {p.pnl >= 0 ? '+' : ''}{fmtINR(p.pnl)}
                    </td>
                    <td className={cn('px-3 py-2 font-mono text-[11px]', pnlClass(p.pnlPct ?? 0))}>
                      {(p.pnlPct ?? 0) >= 0 ? '+' : ''}{(p.pnlPct ?? 0).toFixed(2)}%
                    </td>
                    <td className="px-3 py-2 font-mono text-right text-[11px] text-text-sec">{fmtINR(Math.abs(p.value ?? 0))}</td>
                    <td className="px-3 py-2 text-[10px] text-text-muted">{(p as any).exchange || ''}</td>

                    {/* Position Manager columns */}
                    {pmMode && isClosed && <td colSpan={8} className="px-3 py-2 text-center text-[10px] text-text-muted">—</td>}
                    {pmMode && !isClosed && (() => {
                      const mgdRow = managed[posKey]
                      // Compute next trail activation price
                      let nextTrailPrice: number | undefined
                      if (isMgd && mgdRow?.initial_ltp && mgdRow?.trailing_value && mgdRow?.trail_when) {
                        const isLong = p.side === 'BUY'
                        const bsl = mgdRow.base_stop_loss ?? mgdRow.stop_loss ?? 0
                        const stepsDone = mgdRow.trailing_value > 0
                          ? Math.floor(Math.abs((mgdRow.stop_loss ?? bsl) - bsl) / mgdRow.trailing_value)
                          : 0
                        nextTrailPrice = mgdRow.initial_ltp + ((isLong ? 1 : -1) * ((stepsDone + 1) * mgdRow.trail_when))
                      }
                      return <>
                        {(['stop_loss', 'target', 'trailing_value', 'trail_when'] as const).map(field => (
                          <td key={field} className="px-2 py-1.5">
                            <input
                              type="number"
                              step="any"
                              value={edits[field] ?? (mgdRow?.[field] != null ? String(mgdRow[field]) : '')}
                              onChange={e => setPmField(posKey, field, e.target.value)}
                              placeholder="—"
                              className={cn(
                                'w-20 bg-bg-surface border text-[11px] font-mono px-1.5 py-1 rounded text-text-bright focus:outline-none focus:border-brand',
                                edits[field] ? 'border-brand/60' : 'border-border'
                              )}
                            />
                          </td>
                        ))}
                        {/* Start LTP — read-only, captured at activation */}
                        <td className="px-2 py-1.5 text-center">
                          <span className="text-[11px] font-mono text-text-sec">
                            {isMgd && mgdRow?.initial_ltp ? fmtNum(mgdRow.initial_ltp) : '—'}
                          </span>
                        </td>
                        {/* Act@ — next trail activation price */}
                        <td className="px-2 py-1.5 text-center">
                          <span className="text-[11px] font-mono text-brand">
                            {nextTrailPrice != null ? fmtNum(nextTrailPrice) : '—'}
                          </span>
                        </td>
                        <td className="px-2 py-1.5 text-center">
                          <span className={cn('badge text-[9px]', isMgd ? 'bg-profit/10 text-profit border-profit/30' : 'bg-text-muted/10 text-text-muted border-text-muted/20')}>
                            {isMgd ? 'ACTIVE' : 'OFF'}
                          </span>
                        </td>
                        <td className="px-2 py-1.5">
                          <div className="flex items-center gap-1">
                            <button onClick={() => activateManaged(posKey, accountId, p.ltp)}
                              className="text-[10px] px-2 py-1 rounded border border-brand/60 text-brand hover:bg-brand/10 transition-colors font-medium whitespace-nowrap">
                              {isMgd ? 'Update' : 'Activate'}
                            </button>
                            {isMgd && (
                              <button onClick={() => deactivateManaged(posKey, accountId)}
                                className="text-[10px] px-2 py-1 rounded border border-loss/40 text-loss hover:bg-loss/10 transition-colors font-medium">
                                Off
                              </button>
                            )}
                          </div>
                        </td>
                      </>
                    })()}

                    {/* Actions */}
                    <td className="px-3 py-2">
                      {!isClosed && (
                        <div className="flex items-center gap-1">
                          <button onClick={() => openChartModal(p.symbol)}
                            className="btn-ghost btn-xs !px-1.5 !py-1" title="Chart">
                            <BarChart2 className="w-3 h-3" />
                          </button>
                          <button onClick={() => squareOff(p)} disabled={sqOffLoading === p.id}
                            className="flex items-center gap-0.5 text-[10px] px-2 py-1.5 rounded border border-loss/60 text-loss hover:bg-loss hover:text-white transition-colors font-bold uppercase disabled:opacity-40"
                            title={`Exit ${p.tradingsymbol}`}>
                            {sqOffLoading === p.id
                              ? <RefreshCw className="w-3 h-3 animate-spin" />
                              : <XCircle className="w-3 h-3" />}
                            EXIT
                          </button>
                        </div>
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
            <tfoot>
              <tr className="border-t border-border bg-bg-elevated/50">
                <td colSpan={pmMode ? 10 : 5} className="px-3 py-2 text-[11px] text-text-muted">
                  Total — {openPositions.length} open, {closedPositions.length} closed
                </td>
                <td className={cn('px-3 py-2 font-mono font-bold text-[12px]', pnlClass(totalPnl))}>
                  {totalPnl >= 0 ? '+' : ''}{fmtINR(totalPnl)}
                </td>
                <td />
                <td className="px-3 py-2 font-mono text-right text-[11px] text-text-sec">{fmtINR(totalValue)}</td>
                <td colSpan={pmMode ? 10 : 2} />
              </tr>
            </tfoot>
          </table>
        )}
      </div>
    </div>
  )
}
