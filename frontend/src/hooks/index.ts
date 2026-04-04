/* ═══════════════════════════════════════════════
   SMART TRADER — Custom Hooks
   ═══════════════════════════════════════════════ */
import { useEffect, useRef, useState, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { api } from '../lib/api'
import { ws } from '../lib/ws'
import {
  useAuthStore, useDashboardStore, useMarketStore,
  useOptionChainStore, useToastStore,
} from '../stores'
import {
  DEMO_INDICES, DEMO_SCREENER,
  generateOptionChain,
} from '../lib/mockData'
import type { DashboardData, IndexQuote, ScreenerRow, RiskMetrics } from '../types'

// ── Empty dashboard (no fake data - used when no broker connected) ──
const EMPTY_RISK: RiskMetrics = {
  accountId: '',
  dailyPnl: 0,
  dailyPnlLimit: 0,
  mtmPnl: 0,
  maxPositionValue: 0,
  leverageUsed: 0,
  maxLeverage: 0,
  positionCount: 0,
  maxPositions: 0,
  riskStatus: 'SAFE',
  alerts: [],
}

const EMPTY_DASHBOARD: DashboardData = {
  positions: [],
  holdings: [],
  orders: [],
  trades: [],
  riskMetrics: EMPTY_RISK,
  accountSummary: {
    totalEquity: 0,
    dayPnl: 0,
    dayPnlPct: 0,
    unrealizedPnl: 0,
    realizedPnl: 0,
    usedMargin: 0,
    availableMargin: 0,
  },
}

// ── Auth check on mount ──────────────────────────
export function useAuthCheck() {
  const { setUser, setAccounts, setAuthenticated, setChecking, setIsBrokerLive } = useAuthStore()

  useEffect(() => {
    const token = localStorage.getItem('st_token')
    if (!token) {
      setChecking(false)
      return
    }

    api.get<{ id: string; email: string; name: string; role: string; phone?: string }>('/auth/me')
      .then(async (u: any) => {
        setUser({
          id: u.id,
          name: u.name,
          email: u.email,
          role: u.role,
          phone: u.phone,
          createdAt: u.created_at ?? new Date().toISOString(),
        })
        setAuthenticated(true)
        // Check broker connection status from DB (user-aware)
        try {
          const status = await api.brokerStatus()
          setIsBrokerLive(status.isLive === true)
          if (status.isLive && status.clientId) {
            setAccounts([{
              id: status.clientId,
              userId: u.id,
              broker: (status.broker ?? 'shoonya') as any,
              clientId: status.clientId,
              name: status.clientId,
              status: 'connected',
              lastSync: status.loginAt,
              availableMargin: 0,
              usedMargin: 0,
              totalBalance: 0,
            }])
          }
        } catch {
          // Fall back to old status endpoint
          try {
            const status = await api.shoonyaStatus()
            setIsBrokerLive(status.loggedIn === true && status.mode !== 'demo')
          } catch {
            setIsBrokerLive(false)
          }
        }
      })
      .catch(() => {
        localStorage.removeItem('st_token')
      })
      .finally(() => setChecking(false))
  }, [])

  // Poll broker status every 30s so banner auto-updates after connect/disconnect
  useEffect(() => {
    const pollBroker = async () => {
      if (!localStorage.getItem('st_token')) return
      try {
        const status = await api.brokerStatus()
        setIsBrokerLive(status.isLive === true)
      } catch { /* silent */ }
    }
    const t = setInterval(pollBroker, 30_000)
    return () => clearInterval(t)
  }, [])
}

// ── Dashboard polling ────────────────────────────
export function useDashboardData() {
  const { isBrokerLive, isAuthenticated } = useAuthStore()
  const { setData, setLoading } = useDashboardStore()
  const { toast } = useToastStore()
  const intervalRef = useRef<ReturnType<typeof setInterval>>()

  const fetch = useCallback(async () => {
    if (!isAuthenticated) {
      // Not logged in — show empty dashboard (not fake demo data)
      setData(EMPTY_DASHBOARD)
      return
    }
    if (!isBrokerLive) {
      // Logged in but no broker connected — show empty dashboard with clear state
      setData(EMPTY_DASHBOARD)
      return
    }
    try {
      const data = await api.liveDashboard() as DashboardData
      // Only use empty data if source explicitly says demo AND we have no real data
      if ((data as any).source === 'demo' && !(data as any).positions?.length) {
        // Broker connected but no positions yet — show empty dashboard, not fake demo
        setData({ ...data, positions: [], orders: [], holdings: [], trades: [] } as DashboardData)
      } else {
        setData(data)
      }
    } catch {
      // Don't show fake demo data when broker is connected but API fails
      // Just keep last known state
    }
  }, [isAuthenticated, isBrokerLive])

  useEffect(() => {
    setLoading(true)
    fetch()
    intervalRef.current = setInterval(fetch, 5000)
    return () => clearInterval(intervalRef.current)
  }, [fetch])
}

// ── Market indices polling ───────────────────────
export function useMarketIndices() {
  const { setIndices } = useMarketStore()
  const { isAuthenticated } = useAuthStore()

  useEffect(() => {
    const load = async () => {
      try {
        // Always try live API first — Fyers provides data even without broker login
        const res = await api.indices() as any
        const data = Array.isArray(res) ? res : (res.data ?? [])
        if (data.length > 0) {
          setIndices(data)
          return
        }
      } catch { /* fall through */ }
      // Only use demo indices if API completely fails
      setIndices(DEMO_INDICES)
    }
    load()
    const t = setInterval(load, 5000)
    return () => clearInterval(t)
  }, [isAuthenticated])
}

// ── Screener data ────────────────────────────────
export function useScreenerData() {
  const { setScreener } = useMarketStore()
  const { isAuthenticated } = useAuthStore()

  useEffect(() => {
    const load = async () => {
      try {
        // Always try API — screener works with Fyers (no broker login needed)
        const res = await api.screener({}) as any
        const data = Array.isArray(res) ? res : (res.data ?? [])
        if (data.length > 0) {
          setScreener(data)
          return
        }
      } catch { /* fall through */ }
      setScreener(DEMO_SCREENER)
    }
    load()
    const t = setInterval(load, 30000)
    return () => clearInterval(t)
  }, [isAuthenticated])
}

// ── Global markets (commodities + forex) ─────────
export function useGlobalMarkets() {
  const { setGlobalMarkets } = useMarketStore()
  const { isAuthenticated } = useAuthStore()

  useEffect(() => {
    const load = async () => {
      try {
        const res = await api.globalMarkets() as any
        const data = Array.isArray(res) ? res : (res.data ?? [])
        if (data.length > 0) setGlobalMarkets(data)
      } catch { /* silent */ }
    }
    load()
    const t = setInterval(load, 5000)
    return () => clearInterval(t)
  }, [isAuthenticated])
}

// ── Option chain loading ────────────────────────
const OC_LTP_MAP: Record<string, number> = { NIFTY: 24387, BANKNIFTY: 53842, FINNIFTY: 23450, MIDCPNIFTY: 12240, SENSEX: 80248 }

function demoOptionChain(underlying: string, expiry: string) {
  const ltp = OC_LTP_MAP[underlying] ?? 24387
  return generateOptionChain(underlying, ltp, expiry || '10Apr2026')
}

export function useOptionChain() {
  const { selectedUnderlying, selectedExpiry, setData, setLoading } = useOptionChainStore()

  useEffect(() => {
    const load = async () => {
      setLoading(true)
      try {
        // Always try live API first — Fyers provides option chain data for all users
        const data = await api.optionChain(selectedUnderlying, selectedExpiry || undefined) as any
        if (data && data.rows && data.rows.length > 0) {
          setData(data)
          return
        }
      } catch { /* fall through */ }
      // Fall to demo only if API returned nothing or failed
      setData(demoOptionChain(selectedUnderlying, selectedExpiry || '10Apr2026'))
    }
    load()
    const t = setInterval(load, 5000)
    return () => clearInterval(t)
  }, [selectedUnderlying, selectedExpiry])
}

// ── Instrument search ────────────────────────────
export function useInstrumentSearch(query: string) {
  const [results, setResults] = useState<any[]>([])
  const [loading, setLoading] = useState(false)
  const timer = useRef<ReturnType<typeof setTimeout>>()

  useEffect(() => {
    if (query.length < 2) { setResults([]); return }
    clearTimeout(timer.current)
    timer.current = setTimeout(async () => {
      setLoading(true)
      try {
        const data = await api.search(query) as any[]
        setResults(data)
      } catch {
        // Demo fallback
        const demo = ['RELIANCE', 'RELIANCE EQ', 'TCS', 'HDFCBANK', 'INFY', 'ICICIBANK', 'BHARTIARTL']
        setResults(demo.filter(s => s.toLowerCase().includes(query.toLowerCase()))
          .map(s => ({ symbol: s, tradingsymbol: s, exchange: 'NSE', type: 'EQ', name: s })))
      } finally {
        setLoading(false)
      }
    }, 300)
    return () => clearTimeout(timer.current)
  }, [query])

  return { results, loading }
}

// ── Countdown clock ──────────────────────────────
export function useClock() {
  const [clock, setClock] = useState('')
  useEffect(() => {
    const tick = () => {
      const now = new Date()
      const ist = new Date(now.getTime() + (5.5 * 3600000 - now.getTimezoneOffset() * 60000))
      const h = ist.getUTCHours(), m = ist.getUTCMinutes(), s = ist.getUTCSeconds()
      setClock(`${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`)
    }
    tick()
    const t = setInterval(tick, 1000)
    return () => clearInterval(t)
  }, [])
  return clock
}

// ── Market state ─────────────────────────────────
export function useMarketState(): 'pre' | 'open' | 'post' {
  const [state, setState] = useState<'pre' | 'open' | 'post'>('pre')
  useEffect(() => {
    const check = () => {
      const now = new Date()
      const ist = new Date(now.getTime() + (5.5 * 3600000 - now.getTimezoneOffset() * 60000))
      const tot = ist.getUTCHours() * 60 + ist.getUTCMinutes()
      setState(tot < 9*60+15 ? 'pre' : tot <= 15*60+30 ? 'open' : 'post')
    }
    check()
    const t = setInterval(check, 10000)
    return () => clearInterval(t)
  }, [])
  return state
}

// ── Keyboard shortcut registration ───────────────
export function useKeyboard(key: string, callback: () => void, deps: unknown[] = []) {
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      const target = e.target as HTMLElement
      if (target.tagName === 'INPUT' || target.tagName === 'TEXTAREA' || target.isContentEditable) return
      const parts = key.split('+')
      const mainKey = parts[parts.length - 1].toLowerCase()
      const needCtrl  = parts.includes('ctrl')  || parts.includes('mod')
      const needShift = parts.includes('shift')
      const needAlt   = parts.includes('alt')
      if (needCtrl  && !e.ctrlKey && !e.metaKey) return
      if (needShift && !e.shiftKey) return
      if (needAlt   && !e.altKey) return
      if (e.key.toLowerCase() !== mainKey) return
      e.preventDefault()
      callback()
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [key, ...deps])
}

// ── Logout utility ────────────────────────────────
export function useLogout() {
  const { logout } = useAuthStore()
  const navigate = useNavigate()
  return useCallback(async () => {
    try { await api.logout() } catch { /* ignore */ }
    logout()
    navigate('/login')
  }, [])
}
