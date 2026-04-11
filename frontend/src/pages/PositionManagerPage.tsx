/* ═══════════════════════════════════════════
   Position Manager Page
   – Live positions from PostgreSQL with SL/Target/Trailing controls
   ═══════════════════════════════════════════ */
import { useState, useEffect, useCallback } from 'react'
import { api } from '../lib/api'
import { fmtINR, pnlClass, cn } from '../lib/utils'
import { usePositionsDetailStore } from '../stores'
import {
  ShieldAlert, TrendingDown, Target, RefreshCw, X, Edit2, Check,
  ArrowUp, ArrowDown, Loader2, Info,
} from 'lucide-react'

interface Position {
  command_id:      string
  symbol:          string
  exchange:        string
  side:            string
  quantity:        number
  product:         string
  stop_loss:       number | null
  target:          number | null
  trailing_type:   string | null
  trailing_value:  number | null
  trail_when:      number | null
  status:          string
  strategy_name:   string
  broker_order_id: string | null
  created_at:      string
}

interface SLEditState {
  commandId: string
  stopLoss:  string
  target:    string
}

export default function PositionManagerPage() {
  const [positions, setPositions] = useState<Position[]>([])
  const [loading, setLoading]     = useState(true)
  const [error, setError]         = useState('')
  const [editing, setEditing]     = useState<SLEditState | null>(null)
  const [saving, setSaving]       = useState(false)
  const [exiting, setExiting]     = useState<string | null>(null)

  // WS-fed positions (primary source)
  const wsPositions = usePositionsDetailStore(s => s.positions)
  const wsLastUpdate = usePositionsDetailStore(s => s.lastUpdate)

  // Sync from WS store when WS pushes new data
  useEffect(() => {
    if (wsPositions.length > 0 || wsLastUpdate > 0) {
      setPositions(wsPositions as Position[])
      setLoading(false)
    }
  }, [wsPositions, wsLastUpdate])

  const load = useCallback(async () => {
    // Skip REST if WS pushed recently (< 1s) — WS is primary at ~1s cycle
    const lastWs = usePositionsDetailStore.getState().lastUpdate
    if (lastWs && Date.now() - lastWs < 1_000) return
    try {
      const data = await api.get<Position[]>('/positions')
      setPositions(data)
    } catch (e: any) {
      setError(e.message || 'Failed to load positions')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    load()
    const interval = setInterval(load, 1_000)  // 1s REST fallback when WS misses
    return () => clearInterval(interval)
  }, [load])

  const startEdit = (p: Position) => {
    setEditing({
      commandId: p.command_id,
      stopLoss:  p.stop_loss?.toString() ?? '',
      target:    p.target?.toString() ?? '',
    })
  }

  const saveEdit = async () => {
    if (!editing) return
    setSaving(true)
    try {
      await api.put(`/positions/${editing.commandId}/sl`, {
        stop_loss: parseFloat(editing.stopLoss) || 0,
        target:    editing.target ? parseFloat(editing.target) : null,
      })
      setEditing(null)
      await load()
    } catch (e: any) {
      alert('Save failed: ' + e.message)
    } finally {
      setSaving(false)
    }
  }

  const cancelOrder = async (commandId: string) => {
    if (!confirm('Cancel this order?')) return
    setExiting(commandId)
    try {
      await api.post(`/positions/${commandId}/cancel`, {})
      await load()
    } catch (e: any) {
      alert('Cancel failed: ' + e.message)
    } finally {
      setExiting(null)
    }
  }

  return (
    <div className="p-6 space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <ShieldAlert className="text-blue-400" size={24} />
          <h1 className="text-xl font-semibold text-white">Position Manager</h1>
          <span className="text-sm text-gray-400">(PostgreSQL — live)</span>
        </div>
        <button
          onClick={load}
          className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-white/5 hover:bg-white/10 text-sm text-gray-300 transition"
        >
          <RefreshCw size={14} />
          Refresh
        </button>
      </div>

      {error && (
        <div className="flex items-center gap-2 p-3 rounded-lg bg-red-500/10 border border-red-500/20 text-red-400 text-sm">
          <Info size={14} />
          {error}
        </div>
      )}

      {loading ? (
        <div className="flex justify-center py-12">
          <Loader2 className="animate-spin text-blue-400" size={32} />
        </div>
      ) : positions.length === 0 ? (
        <div className="text-center py-16 text-gray-500">
          <ShieldAlert size={48} className="mx-auto mb-4 opacity-30" />
          <p className="text-lg">No open positions</p>
        </div>
      ) : (
        <div className="overflow-x-auto rounded-xl border border-white/10">
          <table className="w-full text-sm">
            <thead className="bg-white/5 border-b border-white/10">
              <tr>
                <th className="px-4 py-3 text-left text-gray-400 font-medium">Symbol</th>
                <th className="px-4 py-3 text-left text-gray-400 font-medium">Side</th>
                <th className="px-4 py-3 text-right text-gray-400 font-medium">Qty</th>
                <th className="px-4 py-3 text-left text-gray-400 font-medium">Strategy</th>
                <th className="px-4 py-3 text-left text-gray-400 font-medium">Product</th>
                <th className="px-4 py-3 text-right text-gray-400 font-medium">Stop Loss</th>
                <th className="px-4 py-3 text-right text-gray-400 font-medium">Target</th>
                <th className="px-4 py-3 text-left text-gray-400 font-medium">Trailing</th>
                <th className="px-4 py-3 text-left text-gray-400 font-medium">Status</th>
                <th className="px-4 py-3 text-center text-gray-400 font-medium">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/5">
              {positions.map(pos => {
                const isEditing = editing?.commandId === pos.command_id
                return (
                  <tr key={pos.command_id} className="hover:bg-white/5 transition">
                    <td className="px-4 py-3">
                      <span className="font-mono text-blue-300">{pos.symbol}</span>
                      <span className="ml-2 text-xs text-gray-500">{pos.exchange}</span>
                    </td>
                    <td className="px-4 py-3">
                      <span className={cn(
                        'flex items-center gap-1 font-medium',
                        pos.side === 'BUY' ? 'text-emerald-400' : 'text-red-400'
                      )}>
                        {pos.side === 'BUY' ? <ArrowUp size={12} /> : <ArrowDown size={12} />}
                        {pos.side}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-right text-white">{pos.quantity}</td>
                    <td className="px-4 py-3 text-gray-300">{pos.strategy_name || '—'}</td>
                    <td className="px-4 py-3">
                      <span className="px-2 py-0.5 rounded text-xs bg-white/10 text-gray-300">
                        {pos.product}
                      </span>
                    </td>

                    {/* Stop Loss */}
                    <td className="px-4 py-3 text-right">
                      {isEditing ? (
                        <input
                          type="number"
                          value={editing!.stopLoss}
                          onChange={e => setEditing(prev => prev ? { ...prev, stopLoss: e.target.value } : null)}
                          className="w-24 px-2 py-1 rounded bg-red-900/20 border border-red-500/30 text-red-300 text-right text-xs"
                          placeholder="SL price"
                        />
                      ) : (
                        <span className="text-red-400">
                          {pos.stop_loss ? fmtINR(pos.stop_loss) : '—'}
                        </span>
                      )}
                    </td>

                    {/* Target */}
                    <td className="px-4 py-3 text-right">
                      {isEditing ? (
                        <input
                          type="number"
                          value={editing!.target}
                          onChange={e => setEditing(prev => prev ? { ...prev, target: e.target.value } : null)}
                          className="w-24 px-2 py-1 rounded bg-emerald-900/20 border border-emerald-500/30 text-emerald-300 text-right text-xs"
                          placeholder="Target"
                        />
                      ) : (
                        <span className="text-emerald-400">
                          {pos.target ? fmtINR(pos.target) : '—'}
                        </span>
                      )}
                    </td>

                    {/* Trailing */}
                    <td className="px-4 py-3">
                      {pos.trailing_type ? (
                        <span className="text-xs px-2 py-0.5 rounded bg-purple-500/20 text-purple-300">
                          {pos.trailing_type} {pos.trailing_value}
                        </span>
                      ) : '—'}
                    </td>

                    {/* Status */}
                    <td className="px-4 py-3">
                      <StatusBadge status={pos.status} />
                    </td>

                    {/* Actions */}
                    <td className="px-4 py-3">
                      <div className="flex items-center justify-center gap-2">
                        {isEditing ? (
                          <>
                            <button
                              onClick={saveEdit}
                              disabled={saving}
                              className="p-1.5 rounded bg-emerald-500/20 hover:bg-emerald-500/30 text-emerald-400 transition"
                              title="Save"
                            >
                              {saving ? <Loader2 size={12} className="animate-spin" /> : <Check size={12} />}
                            </button>
                            <button
                              onClick={() => setEditing(null)}
                              className="p-1.5 rounded bg-white/5 hover:bg-white/10 text-gray-400 transition"
                              title="Cancel edit"
                            >
                              <X size={12} />
                            </button>
                          </>
                        ) : (
                          <>
                            <button
                              onClick={() => startEdit(pos)}
                              className="p-1.5 rounded bg-blue-500/20 hover:bg-blue-500/30 text-blue-400 transition"
                              title="Edit SL/Target"
                            >
                              <Edit2 size={12} />
                            </button>
                            <button
                              onClick={() => cancelOrder(pos.command_id)}
                              disabled={exiting === pos.command_id}
                              className="p-1.5 rounded bg-red-500/20 hover:bg-red-500/30 text-red-400 transition"
                              title="Cancel order"
                            >
                              {exiting === pos.command_id
                                ? <Loader2 size={12} className="animate-spin" />
                                : <X size={12} />
                              }
                            </button>
                          </>
                        )}
                      </div>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      <p className="text-xs text-gray-600 text-right">
        Auto-refreshes every 5s • {positions.length} open position{positions.length !== 1 ? 's' : ''}
      </p>
    </div>
  )
}

function StatusBadge({ status }: { status: string }) {
  const map: Record<string, string> = {
    CREATED:          'bg-gray-500/20 text-gray-400',
    SENT_TO_BROKER:   'bg-yellow-500/20 text-yellow-400',
    EXECUTED:         'bg-emerald-500/20 text-emerald-400',
    FAILED:           'bg-red-500/20 text-red-400',
  }
  return (
    <span className={cn('px-2 py-0.5 rounded text-xs font-medium', map[status] ?? 'bg-white/10 text-gray-400')}>
      {status}
    </span>
  )
}
