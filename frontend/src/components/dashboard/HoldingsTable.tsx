/* ═══════════════════════════════
   Dashboard — Holdings Table
   with SL/TG/Trail management
   ═══════════════════════════════ */
import { useState, useMemo, useEffect } from 'react'
import { useDashboardStore, useBrokerAccountsStore, useUIStore, useToastStore } from '../../stores'
import { cn, fmtINR, fmtNum, pnlClass } from '../../lib/utils'
import { BarChart2, Package, ChevronsUpDown, ShieldCheck, Settings2, X } from 'lucide-react'
import type { Holding } from '../../types'
import { api } from '../../lib/api'

type SortKey = 'pnl' | 'pnlPct' | 'currentValue' | 'symbol' | 'dayChange'

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

interface ManagedHolding {
  active: boolean
  stop_loss?: number
  target?: number
  trailing_value?: number
  trail_when?: number
  initial_ltp?: number
  base_stop_loss?: number
  trail_stop?: number
}

export default function HoldingsTable() {
  const { data } = useDashboardStore()
  const { accounts: brokerAccounts } = useBrokerAccountsStore()
  const { openChartModal } = useUIStore()
  const { toast } = useToastStore()
  const [sortKey, setSortKey] = useState<SortKey>('pnl')
  const [sortDir, setSortDir] = useState<1 | -1>(-1)
  const [expanded, setExpanded] = useState<string | null>(null)
  const [edits, setEdits] = useState<Record<string, Record<string, string>>>({})
  const [managed, setManaged] = useState<Record<string, ManagedHolding>>({})
  const [hmMode, setHmMode] = useState(false)

  // Build accountId → { brokerName, idx } map
  const brokerMap = useMemo(() => {
    const m: Record<string, { name: string; shortName: string; idx: number }> = {}
    brokerAccounts.forEach((acc, i) => {
      m[acc.config_id] = { name: acc.broker_name, shortName: acc.client_id, idx: i }
    })
    return m
  }, [brokerAccounts])

  // Load saved SL settings from server (for holdings with CNC product)
  useEffect(() => {
    let cancelled = false
    const load = async () => {
      try {
        const res: any = await api.getSLSettings()
        if (cancelled) return
        const settings = res?.data ?? []
        const mgd: Record<string, ManagedHolding> = {}
        for (const s of settings) {
          if (s.pos_key && s.active && s.pos_key.endsWith('|CNC')) {
            mgd[s.pos_key] = {
              active: true,
              stop_loss: s.stop_loss ?? undefined,
              target: s.target ?? undefined,
              trailing_value: s.trailing_value ?? undefined,
              trail_when: s.trail_when ?? undefined,
              initial_ltp: s.initial_ltp ?? undefined,
              base_stop_loss: s.base_stop_loss ?? undefined,
              trail_stop: s.trail_stop ?? undefined,
            }
          }
        }
        setManaged(prev => {
          const merged = { ...mgd }
          for (const [k, v] of Object.entries(prev)) {
            if (v.active && !merged[k]) merged[k] = v
          }
          return merged
        })
      } catch { /* silent */ }
    }
    load()
    const interval = window.setInterval(load, 10000)
    return () => { cancelled = true; clearInterval(interval) }
  }, [])

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

  function setField(sym: string, field: string, value: string) {
    setEdits(prev => ({ ...prev, [sym]: { ...(prev[sym] ?? {}), [field]: value } }))
  }

  async function activate(h: Holding, accountId: string) {
    const sym = h.symbol
    const posKey = `${sym}|CNC`
    const e = edits[sym] ?? {}
    const prev = managed[posKey]
    const settings = {
      stop_loss:      e.stop_loss      ? parseFloat(e.stop_loss)      : prev?.stop_loss,
      target:         e.target         ? parseFloat(e.target)         : prev?.target,
      trailing_value: e.trailing_value ? parseFloat(e.trailing_value) : prev?.trailing_value,
      trail_when:     e.trail_when     ? parseFloat(e.trail_when)     : prev?.trail_when,
    }
    const initialLtp = prev?.initial_ltp ?? h.ltp ?? undefined
    const baseStopLoss = prev?.base_stop_loss ?? settings.stop_loss ?? undefined
    setManaged(prev => ({ ...prev, [posKey]: { active: true, ...settings, initial_ltp: initialLtp, base_stop_loss: baseStopLoss } }))
    setEdits(prev => { const n = { ...prev }; delete n[sym]; return n })
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
      toast(`Holding manager ACTIVE — SL/TG/Trail for ${sym}`, 'success')
    } catch {
      toast('Failed to save SL settings', 'error')
    }
  }

  async function deactivate(sym: string, accountId: string) {
    const posKey = `${sym}|CNC`
    setManaged(prev => { const n = { ...prev }; delete n[posKey]; return n })
    try {
      await api.setSLSettings({ configId: accountId, posKey, active: false })
    } catch { /* non-fatal */ }
    toast(`Holding manager deactivated for ${sym}`, 'info')
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
        <button onClick={() => setHmMode(v => !v)}
          className={cn(
            'flex items-center gap-1.5 text-[11px] px-2.5 py-1 rounded-md border transition-colors font-bold',
            hmMode ? 'bg-brand border-brand text-white' : 'border-brand text-brand hover:bg-brand/10'
          )}>
          <Settings2 className="w-3.5 h-3.5" />
          Holding Manager
        </button>
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
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider">Broker</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-left cursor-pointer hover:text-text-sec"
                  onClick={() => toggleSort('symbol')}>Symbol</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Qty</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Avg Cost</th>
                <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">LTP</th>
                <SH label="Current" col="currentValue" />
                <SH label="P&L" col="pnl" />
                <SH label="P&L %" col="pnlPct" />
                <SH label="Day %" col="dayChange" />
                {hmMode && <>
                  <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-center">SL</th>
                  <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-center">Target</th>
                  <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-center">Trail</th>
                  <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-center">Trail@</th>
                  <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-center whitespace-nowrap">Start LTP</th>
                  <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-center">Status</th>
                  <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-center">Action</th>
                </>}
                <th className="px-3 py-2 w-12"></th>
              </tr>
            </thead>
            <tbody>
              {holdings.map(h => {
                const accountId = (h as any).account_id || (h as any).accountId || ''
                const broker = brokerMap[accountId]
                const bidx = broker?.idx ?? 0
                const posKey = `${h.symbol}|CNC`
                const isMgd = !!managed[posKey]
                const mgdRow = managed[posKey]
                const isExpanded = expanded === h.symbol
                const e = edits[h.symbol] ?? {}
                return (
                  <>
                  <tr key={h.id} className={cn('group transition-colors', BROKER_ROW_TINTS[bidx % BROKER_ROW_TINTS.length])}>
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
                        <div className="text-[12px] font-medium text-text-bright">{h.symbol}</div>
                        <div className="text-[10px] text-text-muted">{h.exchange}
                          {isMgd && <span className="ml-1 text-profit text-[9px] font-bold">● SL/TG</span>}
                        </div>
                      </div>
                    </td>
                    <td className="px-3 py-2 text-[12px] font-mono text-right text-text-pri">{fmtNum(h.quantity, 0)}</td>
                    <td className="px-3 py-2 text-[12px] font-mono text-right text-text-sec">{fmtNum(h.avgCost)}</td>
                    <td className="px-3 py-2 text-[12px] font-mono text-right text-text-bright">{fmtNum(h.ltp)}</td>
                    <td className="px-3 py-2 text-[12px] font-mono text-right text-text-pri">{fmtINR(h.currentValue)}</td>
                    <td className={cn('px-3 py-2 text-[12px] font-mono font-semibold text-right', pnlClass(h.pnl))}>
                      {h.pnl >= 0 ? '+' : ''}{fmtINR(h.pnl)}
                    </td>
                    <td className={cn('px-3 py-2 text-[11px] font-mono text-right', pnlClass(h.pnlPct ?? 0))}>
                      {(h.pnlPct ?? 0) >= 0 ? '+' : ''}{fmtNum(h.pnlPct ?? 0)}%
                    </td>
                    <td className={cn('px-3 py-2 text-[11px] font-mono text-right', pnlClass(h.dayChange ?? 0))}>
                      {(h.dayChange ?? 0) >= 0 ? '+' : ''}{fmtNum(h.dayChangePct ?? 0)}%
                    </td>
                    {/* Holding Manager inline columns */}
                    {hmMode && (() => {
                      return <>
                        {(['stop_loss', 'target', 'trailing_value', 'trail_when'] as const).map(field => (
                          <td key={field} className="px-2 py-1.5">
                            <input
                              type="number"
                              step="any"
                              value={e[field] ?? (mgdRow?.[field] != null ? String(mgdRow[field]) : '')}
                              onChange={ev => setField(h.symbol, field, ev.target.value)}
                              placeholder="—"
                              className={cn(
                                'w-20 bg-bg-surface border text-[11px] font-mono px-1.5 py-1 rounded text-text-bright focus:outline-none focus:border-brand',
                                e[field] ? 'border-brand/60' : 'border-border'
                              )}
                            />
                          </td>
                        ))}
                        <td className="px-2 py-1.5 text-center">
                          <span className="text-[11px] font-mono text-text-sec">
                            {isMgd && mgdRow?.initial_ltp ? fmtNum(mgdRow.initial_ltp) : '—'}
                          </span>
                        </td>
                        <td className="px-2 py-1.5 text-center">
                          <span className={cn('badge text-[9px]', isMgd ? 'bg-profit/10 text-profit border-profit/30' : 'bg-text-muted/10 text-text-muted border-text-muted/20')}>
                            {isMgd ? 'ACTIVE' : 'OFF'}
                          </span>
                        </td>
                        <td className="px-2 py-1.5">
                          <div className="flex items-center gap-1">
                            <button onClick={() => activate(h, accountId)}
                              className="text-[10px] px-2 py-1 rounded border border-brand/60 text-brand hover:bg-brand/10 transition-colors font-medium whitespace-nowrap">
                              {isMgd ? 'Update' : 'Activate'}
                            </button>
                            {isMgd && (
                              <button onClick={() => deactivate(h.symbol, accountId)}
                                className="text-[10px] px-2 py-1 rounded border border-loss/40 text-loss hover:bg-loss/10 transition-colors font-medium">
                                Off
                              </button>
                            )}
                          </div>
                        </td>
                      </>
                    })()}
                    <td className="px-3 py-2">
                      <div className="flex gap-1">
                        <button
                          onClick={() => openChartModal(h.symbol)}
                          className="btn-ghost btn-xs !px-1.5 !py-1 opacity-0 group-hover:opacity-100 transition-opacity"
                        >
                          <BarChart2 className="w-3 h-3" />
                        </button>
                      </div>
                    </td>
                  </tr>
                  </>
                )
              })}
            </tbody>
            <tfoot>
              <tr className="border-t border-border bg-bg-elevated/50">
                <td className="px-3 py-2 text-[11px] text-text-muted" colSpan={5}>Total Portfolio</td>
                <td className="px-3 py-2 text-[11px] font-mono text-right text-text-pri">{fmtINR(totalCurrent)}</td>
                <td className={cn('px-3 py-2 text-[12px] font-mono font-bold text-right', pnlClass(totalPnl))}>
                  {totalPnl >= 0 ? '+' : ''}{fmtINR(totalPnl)}
                </td>
                <td className={cn('px-3 py-2 text-[11px] font-mono text-right', pnlClass(totalPnl))}>
                  {totalInvested > 0 ? fmtNum((totalPnl / totalInvested) * 100) : '0.00'}%
                </td>
                <td colSpan={hmMode ? 9 : 2}></td>
              </tr>
            </tfoot>
          </table>
        )}
      </div>
    </div>
  )
}

