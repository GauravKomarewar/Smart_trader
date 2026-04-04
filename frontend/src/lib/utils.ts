import { type ClassValue, clsx } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

// ── Number formatters ────────────────────────────
const INR = new Intl.NumberFormat('en-IN', {
  style:    'currency',
  currency: 'INR',
  maximumFractionDigits: 2,
  minimumFractionDigits: 2,
})

const INR_COMPACT = new Intl.NumberFormat('en-IN', {
  style:          'currency',
  currency:       'INR',
  notation:       'compact',
  maximumFractionDigits: 2,
})

const PCT = new Intl.NumberFormat('en-IN', {
  style:                  'percent',
  minimumFractionDigits:  2,
  maximumFractionDigits:  2,
  signDisplay:            'always',
})

export function fmtINR(n: number | null | undefined): string {
  if (n == null || isNaN(n)) return '—'
  return INR.format(n)
}

export function fmtINRCompact(n: number | null | undefined): string {
  if (n == null || isNaN(n)) return '—'
  return INR_COMPACT.format(n)
}

export function fmtPct(n: number | null | undefined): string {
  if (n == null || isNaN(n)) return '—'
  return PCT.format(n / 100)
}

export function fmtNum(n: number | null | undefined, decimals = 2): string {
  if (n == null || isNaN(n)) return '—'
  return n.toLocaleString('en-IN', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  })
}

export function fmtVol(n: number | null | undefined): string {
  if (n == null || isNaN(n)) return '—'
  if (n >= 1_00_00_000) return `${(n / 1_00_00_000).toFixed(2)}Cr`
  if (n >= 1_00_000)    return `${(n / 1_00_000).toFixed(2)}L`
  if (n >= 1_000)       return `${(n / 1_000).toFixed(1)}K`
  return String(n)
}

export function fmtOI(n: number): string {
  return fmtVol(n)
}

// ── PnL colour ───────────────────────────────────
export function pnlClass(n: number | null | undefined): string {
  if (n == null) return 'text-text-sec'
  return n > 0 ? 'text-profit' : n < 0 ? 'text-loss' : 'text-text-sec'
}

export function pnlSign(n: number): string {
  return n >= 0 ? '+' : ''
}

// ── Time helpers ─────────────────────────────────
export function timeAgo(ts: number | string | null): string {
  if (!ts) return 'never'
  const diff = Date.now() - (typeof ts === 'string' ? new Date(ts).getTime() : ts)
  if (diff < 5_000)   return 'just now'
  if (diff < 60_000)  return `${Math.floor(diff / 1_000)}s ago`
  if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`
  return `${Math.floor(diff / 3_600_000)}h ago`
}

export function fmtTime(ts: number | string | null): string {
  if (!ts) return '—'
  const d = typeof ts === 'string' ? new Date(ts) : new Date(ts)
  return d.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

export function fmtDate(ts: number | string | null): string {
  if (!ts) return '—'
  return new Date(ts).toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' })
}

export function istClock(): string {
  const now = new Date()
  const ist = new Date(now.getTime() + (5.5 * 3600000 - now.getTimezoneOffset() * 60000))
  const h = ist.getUTCHours(), m = ist.getUTCMinutes(), s = ist.getUTCSeconds()
  return `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`
}

export function marketState(): 'pre' | 'open' | 'post' {
  const now = new Date()
  const ist = new Date(now.getTime() + (5.5 * 3600000 - now.getTimezoneOffset() * 60000))
  const tot = ist.getUTCHours() * 60 + ist.getUTCMinutes()
  if (tot < 9 * 60 + 15)  return 'pre'
  if (tot <= 15 * 60 + 30) return 'open'
  return 'post'
}

// ── Change percent style ────────────────────────
export function changeCls(v: number): string {
  return v > 0 ? 'text-profit' : v < 0 ? 'text-loss' : 'text-text-sec'
}

// ── Misc ─────────────────────────────────────────
export function clamp(n: number, min: number, max: number) {
  return Math.min(Math.max(n, min), max)
}

export function uid(): string {
  return Math.random().toString(36).slice(2, 10)
}

export function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

// ── IV heat color ─────────────────────────────────
export function ivColor(iv: number): string {
  if (iv > 60) return 'text-loss'
  if (iv > 40) return 'text-warning'
  if (iv > 25) return 'text-brand'
  return 'text-profit'
}
