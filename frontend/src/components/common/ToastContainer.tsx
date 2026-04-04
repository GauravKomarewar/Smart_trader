import { useEffect } from 'react'
import { useToastStore } from '../../stores'
import { cn } from '../../lib/utils'
import { CheckCircle, XCircle, AlertTriangle, Info, X } from 'lucide-react'
import type { Toast } from '../../types'

const ICONS = {
  success: CheckCircle,
  error:   XCircle,
  warning: AlertTriangle,
  info:    Info,
}
const COLORS = {
  success: 'border-profit/50 bg-profit/10 text-profit',
  error:   'border-loss/50 bg-loss/10 text-loss',
  warning: 'border-warning/50 bg-warning/10 text-warning',
  info:    'border-brand/50 bg-brand/10 text-brand',
}

export default function ToastContainer() {
  const { toasts, dismiss } = useToastStore()

  return (
    <div className="fixed bottom-5 right-5 z-[200] flex flex-col gap-2.5 items-end pointer-events-none">
      {toasts.map((t: Toast) => <ToastItem key={t.id} toast={t} onDismiss={() => dismiss(t.id)} />)}
    </div>
  )
}

function ToastItem({ toast: t, onDismiss }: { toast: Toast; onDismiss: () => void }) {
  const Icon = ICONS[t.type]

  useEffect(() => {
    const id = setTimeout(onDismiss, 4000)
    return () => clearTimeout(id)
  }, [])

  return (
    <div
      className={cn(
        'pointer-events-auto flex items-start gap-2.5 px-3.5 py-2.5 rounded-xl border shadow-modal',
        'min-w-[240px] max-w-[360px] backdrop-blur-sm bg-bg-card/95',
        COLORS[t.type],
        'animate-slide-up'
      )}
    >
      <Icon className="w-4 h-4 shrink-0 mt-0.5" />
      <div className="flex-1 min-w-0">
        <p className="text-[12px] font-medium text-text-bright leading-snug">{t.message}</p>
      </div>
      <button onClick={onDismiss} className="shrink-0 opacity-60 hover:opacity-100">
        <X className="w-3.5 h-3.5" />
      </button>
    </div>
  )
}
