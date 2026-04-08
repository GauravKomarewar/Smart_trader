/* ═══════════════════════════════════════════════════════════════════
   Broker Accounts Page — Raw Broker Truth
   Per-broker: Positions (with EXIT), Holdings, Orders, Trade Book
   Row highlighting by broker / position side
   ═══════════════════════════════════════════════════════════════════ */
import { useState, useEffect, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { cn, fmtINR, fmtNum, fmtTime, pnlClass, pnlSign } from '../lib/utils'
import { ws } from '../lib/ws'
import { useBrokerAccountsStore, useToastStore } from '../stores'
import type { BrokerAccountWS } from '../stores'
import { api } from '../lib/api'
import {
  WalletCards, Wifi, WifiOff, Layers, FileText, BarChart2, Package,
  RefreshCw, AlertCircle, XCircle, PenLine, ShieldCheck, X,
  Activity, Terminal, CheckCircle, AlertTriangle, Loader2,
} from 'lucide-react'

type DashTab = 'positions' | 'holdings' | 'orders' | 'trades' | 'diagnostics'

const BROKER_TINTS = [
  'border-l-[3px] border-l-[#3b9ede] bg-[#3b9ede]/5',
  'border-l-[3px] border-l-[#f5a623] bg-[#f5a623]/5',
  'border-l-[3px] border-l-[#7c3aed] bg-[#7c3aed]/5',
  'border-l-[3px] border-l-[#10b981] bg-[#10b981]/5',
]
const BROKER_BADGES = [
  'bg-[#3b9ede]/20 text-[#3b9ede] border border-[#3b9ede]/40',
  'bg-[#f5a623]/20 text-[#f5a623] border border-[#f5a623]/40',
  'bg-[#7c3aed]/20 text-[#7c3aed] border border-[#7c3aed]/40',
  'bg-[#10b981]/20 text-[#10b981] border border-[#10b981]/40',
]

export default function BrokerAccountsPage({ initialTab = 'positions' }: { initialTab?: DashTab }) {
  const { accounts, brokerData } = useBrokerAccountsStore()
  const { toast } = useToastStore()
  const navigate = useNavigate()
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<DashTab>(initialTab)
  const [showActiveOnly, setShowActiveOnly] = useState(false)
  const [exitAllLoading, setExitAllLoading] = useState(false)
  const selectedRef = useRef<string | null>(null)
  selectedRef.current = selectedId

  useEffect(() => {
    setActiveTab(initialTab)
  }, [initialTab])

  useEffect(() => {
    if (!selectedRef.current && accounts.length > 0) {
      setSelectedId(accounts[0].config_id)
    }
  }, [accounts])

  useEffect(() => {
    if (selectedId) ws.subscribeBroker(selectedId)
    return () => { ws.unsubscribeBroker() }
  }, [selectedId])

  const selected = accounts.find(a => a.config_id === selectedId) ?? null
  const selectedIdx = accounts.findIndex(a => a.config_id === selectedId)
  const positions = brokerData?.positions ?? []
  const openPositions = positions.filter((p: any) =>
    parseFloat(p.quantity ?? 0) !== 0
  )

  const tabs: { key: DashTab; label: string; icon: typeof Layers; count?: number }[] = [
    { key: 'positions', label: 'Positions', icon: Layers,    count: showActiveOnly ? openPositions.length : positions.length },
    { key: 'holdings',  label: 'Holdings',  icon: Package,   count: brokerData?.holdings?.length },
    { key: 'orders',    label: 'Orders',    icon: FileText,  count: brokerData?.orders?.length },
    { key: 'trades',    label: 'Trade Book',icon: BarChart2, count: brokerData?.trades?.length },
    { key: 'diagnostics', label: 'Diagnostics', icon: Activity },
  ]

  async function exitAll() {
    if (!selected || !openPositions.length) return
    setExitAllLoading(true)
    try {
      const res: any = await api.squareOffAll(selected.config_id)
      toast(res.success ? `${res.placed} exit order(s) sent` : `Partial: ${res.placed} sent`, res.success ? 'success' : 'warning')
    } catch (e: any) {
      toast(`Exit all failed: ${e?.message || 'error'}`, 'error')
    } finally {
      setExitAllLoading(false)
    }
  }

  return (
    <div className="h-full overflow-y-auto">
      <div className="p-4 space-y-4 min-h-full">
        <div className="flex items-center gap-3">
          <WalletCards className="w-4 h-4 text-brand" />
          <span className="text-sm font-semibold text-text-bright">Broker Accounts — Raw Truth</span>
          <span className="text-[10px] text-text-muted border border-border rounded px-1.5 py-0.5">Live broker data · no system filters</span>
          <button
            onClick={() => navigate('/app/broker-diagnostics')}
            className="ml-auto text-[11px] px-2.5 py-1 rounded border border-border text-text-muted hover:text-text-bright hover:border-brand/40 transition-colors"
          >
            Open Diagnostics
          </button>
        </div>

        {/* Broker selector */}
        <div className="flex gap-3 flex-wrap">
          {accounts.map((acc, idx) => {
            const isActive = acc.config_id === selectedId
            return (
              <button key={acc.config_id}
                onClick={() => { setSelectedId(acc.config_id); setActiveTab(initialTab === 'diagnostics' ? 'diagnostics' : 'positions') }}
                className={cn(
                  'flex items-center gap-3 px-4 py-3 rounded-xl border transition-all text-left min-w-[240px]',
                  isActive ? 'border-brand bg-brand/10 ring-1 ring-brand/30' : 'border-border bg-bg-surface hover:border-brand/30'
                )}
              >
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 mb-0.5">
                    <span className={cn('text-[9px] font-bold px-1.5 py-0.5 rounded-sm uppercase tracking-wide', BROKER_BADGES[idx % BROKER_BADGES.length])}>
                      {acc.broker_name}
                    </span>
                    <span className="text-[13px] font-bold text-text-bright">{acc.client_id}</span>
                  </div>
                  <div className="text-[10px] text-text-muted font-mono">
                    Cash: {fmtINR(acc.cash)} · Avail: {fmtINR(acc.available_margin)}
                  </div>
                </div>
                <div className="flex flex-col items-end gap-1">
                  {acc.is_live ? <Wifi className="w-3.5 h-3.5 text-profit" /> : <WifiOff className="w-3.5 h-3.5 text-text-muted" />}
                  <span className={cn('text-[11px] font-mono font-bold', pnlClass(acc.day_pnl))}>
                    {pnlSign(acc.day_pnl)}{fmtINR(acc.day_pnl)}
                  </span>
                </div>
              </button>
            )
          })}
          {accounts.length === 0 && (
            <div className="text-sm text-text-muted py-8 w-full text-center">
              No broker accounts. <a href="/app/settings/brokers" className="text-brand hover:underline">Add broker →</a>
            </div>
          )}
        </div>

        {/* Funds strip */}
        {selected?.is_live && (
          <div className="grid grid-cols-4 sm:grid-cols-8 gap-2">
            {[
              { l: 'Cash',       v: fmtINR(selected.cash),              c: 'text-text-bright' },
              { l: 'Collateral', v: fmtINR(selected.collateral),         c: 'text-accent' },
              { l: 'Available',  v: fmtINR(selected.available_margin),   c: 'text-profit' },
              { l: 'Used',       v: fmtINR(selected.used_margin),        c: 'text-warning' },
              { l: 'Day P&L',    v: `${pnlSign(selected.day_pnl)}${fmtINR(selected.day_pnl)}`,                     c: pnlClass(selected.day_pnl) },
              { l: 'Realized',   v: `${pnlSign(selected.realized_pnl)}${fmtINR(selected.realized_pnl)}`,           c: pnlClass(selected.realized_pnl) },
              { l: 'Unrealized', v: `${pnlSign(selected.unrealized_pnl)}${fmtINR(selected.unrealized_pnl)}`,       c: pnlClass(selected.unrealized_pnl) },
              { l: 'Total',      v: fmtINR(selected.total_balance),      c: 'text-text-bright' },
            ].map(k => (
              <div key={k.l} className="kpi-card !py-1.5 !px-2.5">
                <div className="section-title text-[9px]">{k.l}</div>
                <div className={cn('text-[12px] font-bold font-mono', k.c)}>{k.v}</div>
              </div>
            ))}
          </div>
        )}

        {selected?.error && (
          <div className="flex items-center gap-2 text-[11px] text-loss bg-loss/5 border border-loss/20 rounded-lg px-3 py-2">
            <AlertCircle className="w-4 h-4 shrink-0" />{selected.error}
          </div>
        )}

        {/* Tab content */}
        {selectedId && (
          <div className="flex flex-col min-h-[460px]">
            <div className="flex items-center gap-0 bg-bg-surface border border-border rounded-t-lg overflow-x-auto">
              {tabs.map(t => (
                <button key={t.key} onClick={() => setActiveTab(t.key)}
                  className={cn(
                    'flex items-center gap-2 px-5 py-3 text-[12px] font-medium transition-colors border-b-2 whitespace-nowrap',
                    activeTab === t.key ? 'border-brand text-brand bg-brand/5' : 'border-transparent text-text-sec hover:text-text-bright hover:bg-bg-hover'
                  )}>
                  <t.icon className="w-3.5 h-3.5" />
                  {t.label}
                  {t.count !== undefined && (
                    <span className={cn('badge', activeTab === t.key ? 'badge-brand' : 'badge-neutral')}>{t.count}</span>
                  )}
                </button>
              ))}
              <div className="flex-1" />
              {activeTab === 'positions' && (
                <div className="flex items-center gap-2 pr-3">
                  <button onClick={() => setShowActiveOnly(v => !v)}
                    className={cn(
                      'text-[11px] px-3 py-1.5 rounded-md border transition-colors font-medium',
                      showActiveOnly ? 'bg-brand border-brand text-white' : 'border-border text-text-muted hover:border-brand/40 hover:text-text-sec'
                    )}>
                    Active Only
                  </button>
                  {openPositions.length > 0 && (
                    <button onClick={exitAll} disabled={exitAllLoading}
                      className="flex items-center gap-1.5 text-[11px] px-3 py-1.5 rounded-md border border-loss/60 text-loss hover:bg-loss/10 font-bold disabled:opacity-50 transition-colors">
                      {exitAllLoading ? <RefreshCw className="w-3 h-3 animate-spin" /> : <XCircle className="w-3 h-3" />}
                      Exit All ({openPositions.length})
                    </button>
                  )}
                </div>
              )}
              <span className="text-[10px] text-text-muted pr-4">
                {selected ? `${selected.broker_name} · ${selected.client_id}` : '—'}
              </span>
            </div>

            <div className="flex-1 bg-bg-card border border-t-0 border-border rounded-b-lg overflow-hidden">
              {activeTab === 'positions' && (
                <BrokerPositionsTable
                  data={showActiveOnly ? openPositions : positions}
                  account={selected}
                  brokerIdx={selectedIdx}
                  toast={toast}
                />
              )}
              {activeTab === 'holdings'  && <BrokerHoldingsTable  data={brokerData?.holdings ?? []} accountId={selectedId ?? ''} account={selected} brokerIdx={selectedIdx} toast={toast} />}
              {activeTab === 'orders'    && <BrokerOrdersTable    data={brokerData?.orders ?? []} accountId={selectedId ?? ''} account={selected} brokerIdx={selectedIdx} toast={toast} />}
              {activeTab === 'trades'    && <BrokerTradesTable    data={brokerData?.trades ?? []} account={selected} brokerIdx={selectedIdx} />}
              {activeTab === 'diagnostics' && <DiagnosticsSection toast={toast} accounts={accounts} selectedConfigId={selectedId} />}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

// ─── Positions table with EXIT buttons ─────────────────────────────────────
function BrokerPositionsTable({
  data, account, brokerIdx, toast,
}: {
  data: any[];
  account: BrokerAccountWS | null;
  brokerIdx: number;
  toast: (m: string, t: 'success'|'error'|'warning'|'info') => void;
}) {
  const [exitLoading, setExitLoading] = useState<string|null>(null)
  const rowTint = BROKER_TINTS[brokerIdx % BROKER_TINTS.length]

  if (data.length === 0) return <EmptyState msg="No positions for this broker" />

  async function exitPos(p: any) {
    if (!account) return
    const qty = parseFloat(p.quantity ?? 0)
    if (qty === 0) return
    const sym  = p.tradingsymbol ?? p.symbol ?? ''
    const exch = p.exchange ?? 'NSE'
    const prd  = p.product ?? 'NRML'
    const side = qty > 0 ? 'BUY' : 'SELL'
    const key  = `${sym}|${prd}`
    setExitLoading(key)
    try {
      await api.squareOff({ symbol: sym, exchange: exch, product: prd, quantity: Math.abs(Math.round(qty)), side, accountId: account.config_id })
      toast(`Exit sent: ${sym}`, 'success')
    } catch (e: any) {
      toast(`Exit failed: ${e?.message ?? 'error'}`, 'error')
    } finally {
      setExitLoading(null)
    }
  }

  const totalUnrealized = data.reduce((s, p) => s + parseFloat(p.dayPnl ?? 0), 0)
  const totalRealized   = data.reduce((s, p) => s + (parseFloat(p.pnl ?? 0) - parseFloat(p.dayPnl ?? 0)), 0)
  const totalPnl = totalRealized + totalUnrealized

  return (
    <div className="overflow-auto max-h-[600px]">
      <table className="data-table">
        <thead className="sticky top-0 bg-bg-card z-10">
          <tr>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider">Symbol</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider">Exch</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider">Prd</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider text-right">Qty</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider text-right">Avg</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider text-right">LTP</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider text-right">Realized P&L</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider text-right">Unrealized</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase tracking-wider text-right">Total P&L</th>
            <th className="px-3 py-2 w-24"></th>
          </tr>
        </thead>
        <tbody>
          {data.map((p, i) => {
            const qty    = parseFloat(p.quantity ?? 0)
            const isFlat = qty === 0
            const sym    = p.tradingsymbol ?? p.symbol ?? '—'
            const exch   = p.exchange ?? '—'
            const prd    = p.product ?? '—'
            const avg    = parseFloat(p.avgPrice ?? 0)
            const ltp    = parseFloat(p.ltp ?? 0)
            const unrealized = parseFloat(p.dayPnl ?? 0)
            const realized   = parseFloat(p.pnl ?? 0) - unrealized
            const total = parseFloat(p.pnl ?? 0)
            const key   = `${sym}|${prd}`
            return (
              <tr key={i} className={cn('transition-colors', isFlat ? 'opacity-40' : rowTint, !isFlat && qty > 0 && 'hover:bg-profit/5', !isFlat && qty < 0 && 'hover:bg-loss/5')}>
                <td className="px-3 py-2">
                  <div className="font-semibold text-[12px] text-text-bright">{sym}</div>
                  <div className="mt-0.5">
                    {isFlat
                      ? <span className="text-[9px] text-text-muted">FLAT</span>
                      : <span className={cn('badge text-[9px]', qty > 0 ? 'badge-buy' : 'badge-sell')}>{qty > 0 ? 'LONG' : 'SHORT'}</span>
                    }
                  </div>
                </td>
                <td className="px-3 py-2 text-[11px] text-text-muted">{exch}</td>
                <td className="px-3 py-2 text-[11px] text-text-muted">{prd}</td>
                <td className="px-3 py-2 text-right">
                  <span className={cn('font-mono font-bold text-[12px]', isFlat ? 'text-text-muted' : qty > 0 ? 'text-profit' : 'text-loss')}>
                    {isFlat ? '0' : (qty > 0 ? `+${qty}` : `${qty}`)}
                  </span>
                </td>
                <td className="px-3 py-2 text-right font-mono text-[12px] text-text-sec">{fmtNum(avg)}</td>
                <td className="px-3 py-2 text-right font-mono text-[12px] text-text-bright">{fmtNum(ltp)}</td>
                <td className={cn('px-3 py-2 text-right font-mono font-semibold text-[12px]', pnlClass(realized))}>
                  {pnlSign(realized)}{fmtINR(Math.abs(realized))}
                </td>
                <td className={cn('px-3 py-2 text-right font-mono text-[12px]', pnlClass(unrealized))}>
                  {pnlSign(unrealized)}{fmtINR(Math.abs(unrealized))}
                </td>
                <td className={cn('px-3 py-2 text-right font-mono font-bold text-[12px]', pnlClass(total))}>
                  {pnlSign(total)}{fmtINR(Math.abs(total))}
                </td>
                <td className="px-3 py-2">
                  {!isFlat && (
                    <button onClick={() => exitPos(p)} disabled={exitLoading === key}
                      className="flex items-center gap-1 text-[10px] px-2.5 py-1.5 rounded border border-loss/60 text-loss hover:bg-loss hover:text-white transition-colors font-bold uppercase tracking-wider disabled:opacity-40">
                      {exitLoading === key ? <RefreshCw className="w-3 h-3 animate-spin" /> : <XCircle className="w-3 h-3" />}
                      EXIT
                    </button>
                  )}
                </td>
              </tr>
            )
          })}
        </tbody>
        <tfoot>
          <tr className="border-t border-border bg-bg-elevated/60">
            <td colSpan={6} className="px-3 py-2 text-[11px] text-text-muted">
              {data.filter(p => parseFloat(p.quantity??0) !== 0).length} open · {data.filter(p => parseFloat(p.quantity??0) === 0).length} flat
            </td>
            <td className={cn('px-3 py-2 text-right font-mono font-bold text-[12px]', pnlClass(totalRealized))}>
              {pnlSign(totalRealized)}{fmtINR(Math.abs(totalRealized))}
            </td>
            <td className={cn('px-3 py-2 text-right font-mono font-bold text-[12px]', pnlClass(totalUnrealized))}>
              {pnlSign(totalUnrealized)}{fmtINR(Math.abs(totalUnrealized))}
            </td>
            <td className={cn('px-3 py-2 text-right font-mono font-bold text-[12px]', pnlClass(totalPnl))}>
              {pnlSign(totalPnl)}{fmtINR(Math.abs(totalPnl))}
            </td>
            <td />
          </tr>
        </tfoot>
      </table>
    </div>
  )
}

function BrokerHoldingsTable({ data, accountId, account, brokerIdx, toast }: { data: any[]; accountId: string; account: BrokerAccountWS | null; brokerIdx: number; toast: (m: string, t: 'success'|'error'|'warning'|'info') => void }) {
  const [expanded, setExpanded] = useState<string | null>(null)
  const [edits, setEdits] = useState<Record<string, Record<string, string>>>({})
  const [managed, setManaged] = useState<Record<string, Record<string, any>>>({})

  if (data.length === 0) return <EmptyState msg="No holdings for this broker" />

  function setField(sym: string, field: string, val: string) {
    setEdits(prev => ({ ...prev, [sym]: { ...(prev[sym] ?? {}), [field]: val } }))
  }

  async function activate(h: any, sym: string) {
    const e = edits[sym] ?? {}
    const settings = {
      stop_loss:      e.stop_loss      ? parseFloat(e.stop_loss)      : managed[sym]?.stop_loss,
      target:         e.target         ? parseFloat(e.target)         : managed[sym]?.target,
      trailing_value: e.trailing_value ? parseFloat(e.trailing_value) : managed[sym]?.trailing_value,
      trail_when:     e.trail_when     ? parseFloat(e.trail_when)     : managed[sym]?.trail_when,
    }
    setManaged(prev => ({ ...prev, [sym]: { active: true, ...settings } }))
    setEdits(prev => { const n = { ...prev }; delete n[sym]; return n })
    try {
      await api.setSLSettings({ configId: accountId, posKey: `${sym}|CNC`, active: true, stopLoss: settings.stop_loss || null, target: settings.target || null, trailingValue: settings.trailing_value || null, trailWhen: settings.trail_when || null })
      toast(`SL/TG active for ${sym}`, 'success')
    } catch { toast('Failed to save SL settings', 'error') }
  }

  async function deactivate(sym: string) {
    setManaged(prev => { const n = { ...prev }; delete n[sym]; return n })
    try { await api.setSLSettings({ configId: accountId, posKey: `${sym}|CNC`, active: false }) } catch { /* non-fatal */ }
    toast(`SL/TG deactivated for ${sym}`, 'info')
  }

  return (
    <div className="overflow-auto max-h-[600px]">
      <table className="data-table">
        <thead className="sticky top-0 bg-bg-card z-10">
          <tr>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Broker</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Symbol</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Exchange</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Qty</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Avg Price</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">LTP</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Curr. Value</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Invested</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">P&L</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">%</th>
            <th className="px-3 py-2 w-10"></th>
          </tr>
        </thead>
        <tbody>
          {data.map((h, i) => {
            const sym  = h.symbol ?? `H${i}`
            const qty  = parseFloat(h.quantity ?? 0)
            const avg  = parseFloat(h.avgCost ?? 0)
            const ltp  = parseFloat(h.ltp ?? 0)
            const exch = h.exchange ?? 'NSE'
            const pnl  = parseFloat(h.pnl ?? 0) || (ltp - avg) * qty
            const pnlPct = avg > 0 ? ((ltp - avg) / avg) * 100 : 0
            const curVal = ltp * qty
            const isMgd    = !!managed[sym]
            const isExpanded = expanded === sym
            const e    = edits[sym] ?? {}
            return (
              <>
              <tr key={`${sym}-${i}`} className="group hover:bg-bg-hover">
                <td className="px-3 py-2">
                  <span className={cn('text-[9px] font-bold px-1.5 py-0.5 rounded-sm uppercase tracking-wide', BROKER_BADGES[brokerIdx % BROKER_BADGES.length])}>
                    {account?.broker_name ?? '—'}
                  </span>
                  <div className="text-[9px] text-text-muted mt-0.5">{account?.client_id ?? ''}</div>
                </td>
                <td className="px-3 py-2">
                  <div className="font-medium text-[12px] text-text-bright">{sym}</div>
                  {isMgd && <span className="text-[9px] text-profit font-bold">● SL/TG</span>}
                </td>
                <td className="px-3 py-2 text-[11px] text-text-muted">{exch}</td>
                <td className="px-3 py-2 text-right font-mono text-[12px]">{qty}</td>
                <td className="px-3 py-2 text-right font-mono text-[12px] text-text-sec">{fmtINR(avg)}</td>
                <td className="px-3 py-2 text-right font-mono text-[12px] text-text-bright">{fmtINR(ltp)}</td>
                <td className="px-3 py-2 text-right font-mono text-[11px] text-text-sec">{fmtINR(curVal)}</td>
                <td className="px-3 py-2 text-right font-mono text-[11px] text-text-sec">{fmtINR(avg * qty)}</td>
                <td className={cn('px-3 py-2 text-right font-mono font-semibold text-[12px]', pnlClass(pnl))}>
                  {pnlSign(pnl)}{fmtINR(Math.abs(pnl))}
                </td>
                <td className={cn('px-3 py-2 text-right font-mono text-[11px]', pnlClass(pnlPct))}>
                  {pnlPct >= 0 ? '+' : ''}{pnlPct.toFixed(2)}%
                </td>
                <td className="px-3 py-2">
                  <button onClick={() => setExpanded(isExpanded ? null : sym)}
                    className={cn('btn-ghost btn-xs !px-1.5 !py-1 transition-opacity', isMgd ? 'text-profit' : 'opacity-0 group-hover:opacity-100')}
                    title="SL/TG/Trail">
                    <ShieldCheck className="w-3 h-3" />
                  </button>
                </td>
              </tr>
              {isExpanded && (
                <tr key={`${sym}-sl`} className="bg-bg-elevated/40 border-t border-brand/20">
                  <td colSpan={11} className="px-3 py-2">
                    <div className="flex items-center gap-3 flex-wrap">
                      <span className="text-[10px] font-semibold text-text-muted uppercase">SL/TG/Trail</span>
                      {(['stop_loss', 'target', 'trailing_value', 'trail_when'] as const).map(field => (
                        <div key={field} className="flex items-center gap-1">
                          <label className="text-[10px] text-text-muted capitalize">{field.replace(/_/g,' ')}</label>
                          <input type="number" step="any"
                            value={e[field] ?? (managed[sym]?.[field] != null ? String(managed[sym][field]) : '')}
                            onChange={ev => setField(sym, field, ev.target.value)}
                            placeholder="—"
                            className="w-20 bg-bg-surface border border-border text-[11px] font-mono px-1.5 py-1 rounded text-text-bright focus:outline-none focus:border-brand"
                          />
                        </div>
                      ))}
                      <button onClick={() => activate(h, sym)}
                        className="text-[10px] px-2 py-1 rounded border border-brand/60 text-brand hover:bg-brand/10 font-medium">
                        {isMgd ? 'Update' : 'Activate'}
                      </button>
                      {isMgd && (
                        <button onClick={() => deactivate(sym)}
                          className="text-[10px] px-2 py-1 rounded border border-loss/40 text-loss hover:bg-loss/10 font-medium">
                          Deactivate
                        </button>
                      )}
                      <button onClick={() => setExpanded(null)} className="ml-auto btn-ghost btn-xs">
                        <X className="w-3 h-3" />
                      </button>
                    </div>
                  </td>
                </tr>
              )}
              </>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

const OPEN_STATUSES_STR = ['OPEN', 'PENDING', 'AMO', 'TRIGGER_PENDING']

function BrokerOrdersTable({ data, accountId, account, brokerIdx, toast }: { data: any[]; accountId: string; account: BrokerAccountWS | null; brokerIdx: number; toast: (m: string, t: 'success'|'error'|'warning'|'info') => void }) {
  const [cancelling, setCancelling] = useState<string | null>(null)
  const [cancellingAll, setCancellingAll] = useState(false)
  const [modify, setModify] = useState<{ order: any; price: string; qty: string; orderType: string; triggerPrice: string; validity: string } | null>(null)
  const [modifying, setModifying] = useState(false)
  const [showOpenOnly, setShowOpenOnly] = useState(false)

  const openOrders = data.filter(o => OPEN_STATUSES_STR.includes((o.status ?? '').toUpperCase()))
  const displayed  = showOpenOnly ? openOrders : data

  async function cancelOne(o: any) {
    const oid = o.brokerOrderId ?? o.orderId ?? o.id
    const acct = o.accountId ?? accountId
    if (o.actionable === false || !oid || !acct) {
      toast('This order is not yet ready for cancellation', 'warning')
      return
    }
    setCancelling(oid)
    try {
      await api.cancelOrder(oid, acct)
      toast(`Cancelled: ${o.tradingsymbol ?? o.symbol}`, 'success')
    } catch (e: any) { toast(e?.message || 'Failed to cancel order', 'error') }
    finally { setCancelling(null) }
  }

  async function cancelAll() {
    if (!accountId) { toast('Cannot determine account', 'error'); return }
    setCancellingAll(true)
    try {
      const res: any = await api.cancelAllOrders(accountId)
      toast(`Cancelled ${res.cancelled ?? 0} orders`, 'success')
    } catch { toast('Failed to cancel all', 'error') }
    finally { setCancellingAll(false) }
  }

  async function submitModify() {
    if (!modify) return
    setModifying(true)
    const oid = modify.order.brokerOrderId ?? modify.order.orderId ?? modify.order.id
    try {
      if (modify.order.actionable === false || !oid || !accountId) {
        toast('This order is not yet ready for modification', 'warning')
        return
      }
      const payload: any = { accountId: accountId, price: parseFloat(modify.price) || undefined, quantity: parseInt(modify.qty) || undefined, orderType: modify.orderType }
      if (modify.triggerPrice) payload.triggerPrice = parseFloat(modify.triggerPrice)
      if (modify.validity) payload.validity = modify.validity
      await api.modifyOrder(oid, payload)
      toast(`Modified: ${modify.order.tradingsymbol ?? modify.order.symbol}`, 'success')
      setModify(null)
    } catch (e: any) { toast(e?.message || 'Failed to modify order', 'error') }
    finally { setModifying(false) }
  }

  if (data.length === 0) return <EmptyState msg="No orders for this broker" />

  return (
    <>
    <div className="flex items-center gap-2 px-3 py-2 border-b border-border bg-bg-elevated/30">
      <button onClick={() => setShowOpenOnly(v => !v)}
        className={cn('text-[11px] px-2.5 py-1 rounded border font-medium transition-colors',
          showOpenOnly ? 'bg-brand border-brand text-white' : 'border-border text-text-muted hover:border-brand/40')}>
        Open Only
      </button>
      {openOrders.length > 0 && (
        <button onClick={cancelAll} disabled={cancellingAll}
          className="flex items-center gap-1 text-[11px] px-2.5 py-1 rounded border border-loss/60 text-loss hover:bg-loss/10 font-bold disabled:opacity-50">
          {cancellingAll ? <RefreshCw className="w-3 h-3 animate-spin" /> : <XCircle className="w-3 h-3" />}
          Cancel All ({openOrders.length})
        </button>
      )}
      <span className="ml-auto text-[10px] text-text-muted">{displayed.length} orders</span>
    </div>
    <div className="overflow-auto max-h-[560px]">
      <table className="data-table">
        <thead className="sticky top-0 bg-bg-card z-10">
          <tr>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Broker</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Time</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Symbol</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Exchange</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Side</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Type</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Qty</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Price</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Trig Price</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Avg Fill</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Product</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Validity</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Status</th>
            <th className="px-3 py-2 w-24"></th>
          </tr>
        </thead>
        <tbody>
          {displayed.map((o, i) => {
            const side   = (o.transactionType ?? '').toUpperCase()
            const isBuy  = side === 'BUY' || side === 'B'
            const status = (o.status ?? '').toUpperCase()
            const isOpen = OPEN_STATUSES_STR.includes(status)
            const oid    = o.brokerOrderId ?? o.orderId ?? o.id ?? String(i)
            return (
              <tr key={oid} className="hover:bg-bg-hover">
                <td className="px-3 py-2">
                  <span className={cn('text-[9px] font-bold px-1.5 py-0.5 rounded-sm uppercase tracking-wide', BROKER_BADGES[brokerIdx % BROKER_BADGES.length])}>
                    {account?.broker_name ?? '—'}
                  </span>
                  <div className="text-[9px] text-text-muted mt-0.5">{account?.client_id ?? ''}</div>
                </td>
                <td className="px-3 py-2 text-text-muted text-[10px] whitespace-nowrap">{fmtTime(o.placedAt ?? '')}</td>
                <td className="px-3 py-2 font-medium text-[12px] text-text-bright">{o.tradingsymbol ?? o.symbol ?? '—'}</td>
                <td className="px-3 py-2 text-[11px] text-text-muted">{o.exchange ?? '—'}</td>
                <td className="px-3 py-2"><span className={cn('badge text-[9px]', isBuy ? 'badge-buy' : 'badge-sell')}>{isBuy ? 'BUY' : 'SELL'}</span></td>
                <td className="px-3 py-2 text-[11px] text-text-sec">{o.orderType ?? '—'}</td>
                <td className="px-3 py-2 text-right font-mono text-[12px]">{o.quantity ?? 0}</td>
                <td className="px-3 py-2 text-right font-mono text-[12px] text-text-sec">{fmtINR(o.price ?? 0)}</td>
                <td className="px-3 py-2 text-right font-mono text-[12px] text-text-muted">{fmtINR(o.triggerPrice ?? 0)}</td>
                <td className="px-3 py-2 text-right font-mono text-[12px]">{fmtINR(o.avgPrice ?? 0)}</td>
                <td className="px-3 py-2 text-[11px] text-text-muted">{o.product ?? '—'}</td>
                <td className="px-3 py-2 text-[11px] text-text-muted">{o.validity ?? '—'}</td>
                <td className="px-3 py-2">
                  <span className={cn('badge text-[9px]',
                    status.includes('COMPLETE') || status.includes('FILLED') ? 'badge-success' :
                    status.includes('REJECT') || status.includes('CANCEL') ? 'badge-danger' :
                    status.includes('OPEN') || status.includes('PENDING') ? 'badge-warning' : 'badge-neutral'
                  )}>{status || '—'}</span>
                </td>
                <td className="px-3 py-2">
                  {isOpen && (
                    <div className="flex gap-1">
                      <button onClick={() => setModify({
                        order: o,
                        price: String(o.price ?? ''),
                        qty: String(o.quantity ?? ''),
                        orderType: o.orderType ?? 'LMT',
                        triggerPrice: String(o.triggerPrice ?? ''),
                        validity: o.validity ?? 'DAY',
                      })}
                        className="btn-ghost btn-xs !px-1.5 !py-1 text-text-muted hover:text-brand" title="Modify">
                        <PenLine className="w-3 h-3" />
                      </button>
                      <button onClick={() => cancelOne(o)} disabled={cancelling === oid}
                        className="btn-ghost btn-xs !px-1.5 !py-1 text-text-muted hover:text-loss" title="Cancel">
                        {cancelling === oid ? <RefreshCw className="w-3 h-3 animate-spin" /> : <XCircle className="w-3 h-3" />}
                      </button>
                    </div>
                  )}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>

    {modify && (
      <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm" onClick={() => setModify(null)}>
        <div className="bg-bg-card border border-border rounded-xl p-5 w-96 space-y-3 shadow-2xl" onClick={e => e.stopPropagation()}>
          <div className="flex items-center justify-between">
            <span className="text-[13px] font-semibold text-text-bright">Modify Order</span>
            <button onClick={() => setModify(null)} className="btn-ghost btn-xs"><X className="w-3.5 h-3.5" /></button>
          </div>
          <div className="flex items-center gap-2 bg-bg-elevated border border-border rounded px-3 py-2">
            <span className="text-[12px] font-semibold text-text-bright">{modify.order.tradingsymbol ?? modify.order.symbol}</span>
            <span className={cn('badge text-[9px]', (modify.order.transactionType ?? '').toUpperCase().includes('B') ? 'badge-buy' : 'badge-sell')}>
              {modify.order.transactionType}
            </span>
            <span className="text-[10px] text-text-muted">{modify.order.exchange ?? ''}</span>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div className="flex flex-col gap-1">
              <label className="text-[10px] text-text-muted uppercase">Price</label>
              <input type="number" step="any"
                value={modify.price}
                onChange={e => setModify(prev => prev ? { ...prev, price: e.target.value } : null)}
                className="bg-bg-surface border border-border rounded px-2 py-1.5 text-[12px] font-mono text-text-bright focus:outline-none focus:border-brand"
              />
            </div>
            <div className="flex flex-col gap-1">
              <label className="text-[10px] text-text-muted uppercase">Quantity</label>
              <input type="number" step="1"
                value={modify.qty}
                onChange={e => setModify(prev => prev ? { ...prev, qty: e.target.value } : null)}
                className="bg-bg-surface border border-border rounded px-2 py-1.5 text-[12px] font-mono text-text-bright focus:outline-none focus:border-brand"
              />
            </div>
            <div className="flex flex-col gap-1">
              <label className="text-[10px] text-text-muted uppercase">Trigger Price</label>
              <input type="number" step="any"
                value={modify.triggerPrice}
                onChange={e => setModify(prev => prev ? { ...prev, triggerPrice: e.target.value } : null)}
                className="bg-bg-surface border border-border rounded px-2 py-1.5 text-[12px] font-mono text-text-bright focus:outline-none focus:border-brand"
              />
            </div>
            <div className="flex flex-col gap-1">
              <label className="text-[10px] text-text-muted uppercase">Order Type</label>
              <select value={modify.orderType} onChange={e => setModify(prev => prev ? { ...prev, orderType: e.target.value } : null)}
                className="bg-bg-surface border border-border rounded px-2 py-1.5 text-[12px] text-text-bright focus:outline-none focus:border-brand">
                {['LMT', 'MKT', 'SL', 'SL-M'].map(t => <option key={t} value={t}>{t}</option>)}
              </select>
            </div>
          </div>
          <div className="flex flex-col gap-1">
            <label className="text-[10px] text-text-muted uppercase">Validity</label>
            <select value={modify.validity} onChange={e => setModify(prev => prev ? { ...prev, validity: e.target.value } : null)}
              className="bg-bg-surface border border-border rounded px-2 py-1.5 text-[12px] text-text-bright focus:outline-none focus:border-brand">
              {['DAY', 'IOC', 'GTC'].map(t => <option key={t} value={t}>{t}</option>)}
            </select>
          </div>
          <button onClick={submitModify} disabled={modifying}
            className="w-full btn-primary text-[12px] py-2 disabled:opacity-50 flex items-center justify-center gap-2">
            {modifying && <RefreshCw className="w-3.5 h-3.5 animate-spin" />}
            Confirm Modify
          </button>
        </div>
      </div>
    )}
    </>
  )
}

function BrokerTradesTable({ data, account, brokerIdx }: { data: any[]; account: BrokerAccountWS | null; brokerIdx: number }) {
  if (data.length === 0) return <EmptyState msg="No trades today for this broker" />
  return (
    <div className="overflow-auto max-h-[600px]">
      <table className="data-table">
        <thead className="sticky top-0 bg-bg-card z-10">
          <tr>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Broker</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Time</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Trade ID</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Order ID</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Symbol</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Exchange</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Side</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase">Product</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Qty</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Price</th>
            <th className="px-3 py-2 text-[10px] font-medium text-text-muted uppercase text-right">Value</th>
          </tr>
        </thead>
        <tbody>
          {data.map((t, i) => {
            const side  = (t.transactionType ?? '').toUpperCase()
            const isBuy = side === 'BUY' || side === 'B'
            const qty   = t.quantity ?? 0
            const price = t.price ?? 0
            const value = t.value != null ? t.value : qty * price
            return (
              <tr key={t.tradeId ?? i} className="hover:bg-bg-hover">
                <td className="px-3 py-2">
                  <span className={cn('text-[9px] font-bold px-1.5 py-0.5 rounded-sm uppercase tracking-wide', BROKER_BADGES[brokerIdx % BROKER_BADGES.length])}>
                    {account?.broker_name ?? '—'}
                  </span>
                  <div className="text-[9px] text-text-muted mt-0.5">{account?.client_id ?? ''}</div>
                </td>
                <td className="px-3 py-2 text-text-muted text-[10px] whitespace-nowrap">{fmtTime(t.tradedAt ?? '')}</td>
                <td className="px-3 py-2 text-[10px] text-text-muted font-mono">{t.tradeId ?? '—'}</td>
                <td className="px-3 py-2 text-[10px] text-text-muted font-mono">{t.orderId ?? '—'}</td>
                <td className="px-3 py-2">
                  <div className="font-medium text-[12px] text-text-bright">{t.tradingsymbol ?? t.symbol ?? '—'}</div>
                </td>
                <td className="px-3 py-2 text-[11px] text-text-sec">{t.exchange ?? '—'}</td>
                <td className="px-3 py-2"><span className={cn('badge text-[9px]', isBuy ? 'badge-buy' : 'badge-sell')}>{isBuy ? 'BUY' : 'SELL'}</span></td>
                <td className="px-3 py-2 text-[11px] text-text-muted">{t.product ?? '—'}</td>
                <td className="px-3 py-2 text-right font-mono text-[12px]">{qty}</td>
                <td className="px-3 py-2 text-right font-mono text-[12px]">{fmtINR(price)}</td>
                <td className="px-3 py-2 text-right font-mono text-[12px] text-text-sec">{fmtINR(value)}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function EmptyState({ msg }: { msg: string }) {
  return <div className="flex items-center justify-center py-16 text-text-muted text-sm">{msg}</div>
}

// ─── Diagnostics Section ─────────────────────────────────────────────────────
type DiagCall = 'profile' | 'positions' | 'orderbook' | 'funds' | 'holdings' | 'tradebook'
const DIAG_CALLS: { key: DiagCall; label: string }[] = [
  { key: 'profile',    label: 'Profile' },
  { key: 'funds',      label: 'Funds' },
  { key: 'positions',  label: 'Positions' },
  { key: 'orderbook',  label: 'Order Book' },
  { key: 'holdings',   label: 'Holdings' },
  { key: 'tradebook',  label: 'Tradebook' },
]

function DiagnosticsSection({ toast, accounts, selectedConfigId }: {
  toast: (m: string, t: 'success'|'error'|'warning'|'info') => void
  accounts: BrokerAccountWS[]
  selectedConfigId: string | null
}) {
  const [health, setHealth] = useState<{ label: string; status: 'ok' | 'warn' | 'error'; detail: string }[]>([])
  const [healthLoading, setHealthLoading] = useState(false)
  const [diagCall, setDiagCall] = useState<DiagCall>('funds')
  const [diagResult, setDiagResult] = useState<any>(null)
  const [diagRunning, setDiagRunning] = useState(false)

  useEffect(() => { runHealthChecks() }, [])

  const runHealthChecks = async () => {
    setHealthLoading(true)
    const checks: typeof health = []
    const t0 = Date.now()
    try {
      const res = await fetch('/api/health')
      const ms = Date.now() - t0
      if (res.ok) {
        const body = await res.json()
        checks.push({ label: 'Backend API', status: 'ok', detail: `Responding in ${ms}ms · mode=${body.mode}` })
        checks.push({
          label: 'Broker Sessions',
          status: (body.active_accounts ?? 0) > 0 ? 'ok' : 'warn',
          detail: `${body.active_accounts ?? 0} account(s) connected`,
        })
      } else {
        checks.push({ label: 'Backend API', status: 'error', detail: `HTTP ${res.status}` })
      }
    } catch (e: any) {
      checks.push({ label: 'Backend API', status: 'error', detail: String(e) })
    }
    setHealth(checks)
    setHealthLoading(false)
  }

  const runDiagnose = async () => {
    if (!selectedConfigId) { toast('Select a broker account first', 'error'); return }
    setDiagRunning(true)
    setDiagResult(null)
    try {
      const res = await api.brokerDiagnose(selectedConfigId, diagCall)
      setDiagResult(res)
    } catch (e: any) {
      setDiagResult({ ok: false, error: String(e), data: null })
    } finally {
      setDiagRunning(false)
    }
  }

  const STATUS_ICON: Record<string, React.ReactNode> = {
    ok:    <CheckCircle className="w-4 h-4 text-profit shrink-0" />,
    warn:  <AlertTriangle className="w-4 h-4 text-warning shrink-0" />,
    error: <XCircle className="w-4 h-4 text-loss shrink-0" />,
  }
  const STATUS_BADGE: Record<string, string> = { ok: 'badge-green', warn: 'badge-yellow', error: 'badge-red' }

  const selectedAcc = accounts.find(a => a.config_id === selectedConfigId)

  return (
    <div className="p-4 space-y-4">
      {/* System Health */}
      <div className="bg-bg-elevated border border-border rounded-lg p-4">
        <div className="flex items-center justify-between mb-3">
          <span className="text-[12px] font-semibold text-text-bright">System Health</span>
          <button onClick={runHealthChecks} disabled={healthLoading}
            className="flex items-center gap-1 text-[11px] text-text-muted hover:text-text-sec">
            <RefreshCw className={cn('w-3 h-3', healthLoading && 'animate-spin')} /> Refresh
          </button>
        </div>
        <div className="space-y-2.5">
          {health.length === 0 && healthLoading && (
            <div className="text-[11px] text-text-muted flex items-center gap-2">
              <Loader2 className="w-3.5 h-3.5 animate-spin" /> Checking…
            </div>
          )}
          {health.map(c => (
            <div key={c.label} className="flex items-center gap-3">
              {STATUS_ICON[c.status]}
              <div className="flex-1 min-w-0">
                <div className="text-[12px] font-medium text-text-sec">{c.label}</div>
                <div className="text-[10px] text-text-muted truncate">{c.detail}</div>
              </div>
              <span className={cn('badge', STATUS_BADGE[c.status])}>{c.status}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Broker API Inspector */}
      <div className="bg-bg-elevated border border-border rounded-lg p-4">
        <div className="flex items-center gap-2 mb-4">
          <Terminal className="w-3.5 h-3.5 text-brand" />
          <span className="text-[12px] font-semibold text-text-bright">Broker API Inspector</span>
          {selectedAcc && (
            <span className="text-[10px] text-text-muted">
              Testing: {selectedAcc.broker_name} · {selectedAcc.client_id}
            </span>
          )}
        </div>

        {!selectedConfigId ? (
          <div className="text-[11px] text-text-muted py-4 text-center">Select a broker account above to run diagnostics</div>
        ) : (
          <div className="space-y-4">
            <div>
              <div className="text-[10px] text-text-muted mb-1.5 uppercase font-medium tracking-wide">Select API Call</div>
              <div className="flex flex-wrap gap-1.5">
                {DIAG_CALLS.map(c => (
                  <button key={c.key}
                    onClick={() => { setDiagCall(c.key); setDiagResult(null) }}
                    className={cn(
                      'px-3 py-1 rounded text-[11px] font-medium border transition-all',
                      diagCall === c.key
                        ? 'bg-brand text-white border-brand'
                        : 'bg-bg-surface border-border text-text-sec hover:border-brand/40'
                    )}>
                    {c.label}
                  </button>
                ))}
              </div>
            </div>

            <button onClick={runDiagnose} disabled={diagRunning}
              className="flex items-center gap-2 text-[11px] px-3 py-1.5 rounded bg-brand text-white hover:bg-brand/90 disabled:opacity-50 font-medium">
              {diagRunning
                ? <><Loader2 className="w-3.5 h-3.5 animate-spin" />Calling {diagCall}…</>
                : <><Terminal className="w-3.5 h-3.5" />Call {DIAG_CALLS.find(c => c.key === diagCall)?.label}</>
              }
            </button>

            {diagResult && (
              <div>
                <div className="flex items-center justify-between mb-1.5">
                  <div className="text-[10px] uppercase text-text-muted font-medium tracking-wide flex items-center gap-2">
                    Raw Response
                    {diagResult.ok
                      ? <span className="badge badge-green">OK · {diagResult.elapsed_ms}ms</span>
                      : <span className="badge badge-red">ERROR</span>
                    }
                  </div>
                  <button className="text-[10px] text-text-muted hover:text-text-sec"
                    onClick={() => { navigator.clipboard.writeText(JSON.stringify(diagResult, null, 2)); toast('Copied', 'success') }}>
                    Copy
                  </button>
                </div>
                <pre className="bg-bg-surface border border-border rounded-lg p-3 text-[10px] text-text-sec overflow-auto max-h-96 font-mono leading-relaxed">
                  {JSON.stringify(diagResult, null, 2)}
                </pre>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
