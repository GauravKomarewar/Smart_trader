/* ═══════════════════════════════
   Dashboard — Risk Manager card
   ═══════════════════════════════ */
import { cn, fmtINR } from '../../lib/utils'
import { useDashboardStore } from '../../stores'
import { Shield, AlertTriangle, CheckCircle2, XCircle } from 'lucide-react'

export default function RiskManager() {
  const risk = useDashboardStore(s => s.data?.riskMetrics)

  if (!risk) return (
    <div className="bg-bg-card border border-border rounded-lg p-4 space-y-3">
      <div className="skeleton h-4 w-32 rounded" />
      <div className="skeleton h-16 w-full rounded" />
    </div>
  )

  const pnlPct = Math.min(Math.abs(risk.dailyPnl / risk.dailyPnlLimit) * 100, 100)
  const levPct = (risk.leverageUsed / risk.maxLeverage) * 100
  const posPct = (risk.positionCount / risk.maxPositions) * 100

  const statusCfg = {
    SAFE:     { cls: 'text-profit',  bg: 'bg-profit',  icon: CheckCircle2 },
    WARNING:  { cls: 'text-warning', bg: 'bg-warning',  icon: AlertTriangle },
    CRITICAL: { cls: 'text-loss',    bg: 'bg-loss',     icon: XCircle },
    BREACHED: { cls: 'text-loss',    bg: 'bg-loss/80',  icon: XCircle },
  }
  const sc = statusCfg[risk.riskStatus] ?? statusCfg.SAFE

  return (
    <div className="bg-bg-card border border-border rounded-lg p-4 space-y-3">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Shield className="w-4 h-4 text-text-sec" />
          <span className="text-[12px] font-semibold text-text-bright">Risk Manager</span>
        </div>
        <div className={cn('flex items-center gap-1.5 badge', risk.riskStatus === 'SAFE' ? 'badge-safe' : risk.riskStatus === 'WARNING' ? 'badge-warn' : 'badge-danger')}>
          <sc.icon className="w-3 h-3" />
          {risk.riskStatus}
        </div>
      </div>

      {/* Meters */}
      <div className="space-y-2.5">
        <Meter label="Daily Loss" used={Math.abs(risk.dailyPnl)} limit={Math.abs(risk.dailyPnlLimit)} pct={pnlPct}
          valueStr={`${fmtINR(Math.abs(risk.dailyPnl))} / ${fmtINR(Math.abs(risk.dailyPnlLimit))}`}
          color={pnlPct > 80 ? '#f43f5e' : pnlPct > 60 ? '#f59e0b' : '#22c55e'} />
        <Meter label="Leverage" used={risk.leverageUsed} limit={risk.maxLeverage} pct={levPct}
          valueStr={`${risk.leverageUsed.toFixed(2)}x / ${risk.maxLeverage}x`}
          color={levPct > 80 ? '#f43f5e' : levPct > 60 ? '#f59e0b' : '#22d3ee'} />
        <Meter label="Positions" used={risk.positionCount} limit={risk.maxPositions} pct={posPct}
          valueStr={`${risk.positionCount} / ${risk.maxPositions}`}
          color={posPct > 80 ? '#f59e0b' : '#818cf8'} />
      </div>

      {/* Alerts */}
      {risk.alerts.length > 0 && (
        <div className="space-y-1">
          {risk.alerts.slice(0, 2).map(a => (
            <div key={a.id} className={cn('text-[11px] px-2 py-1 rounded flex items-start gap-2',
              a.level === 'critical' ? 'bg-loss/10 text-loss' :
              a.level === 'warning'  ? 'bg-warning/10 text-warning' : 'bg-info/10 text-info')}>
              <AlertTriangle className="w-3 h-3 shrink-0 mt-px" />
              {a.message}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

function Meter({ label, pct, valueStr, color }: {
  label: string; used?: number; limit?: number; pct: number; valueStr: string; color: string
}) {
  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between text-[11px]">
        <span className="text-text-muted">{label}</span>
        <span className="text-text-sec font-mono">{valueStr}</span>
      </div>
      <div className="h-1.5 bg-bg-elevated rounded-full overflow-hidden">
        <div
          className="h-full rounded-full transition-all duration-500"
          style={{ width: `${Math.min(pct, 100)}%`, backgroundColor: color }}
        />
      </div>
    </div>
  )
}
