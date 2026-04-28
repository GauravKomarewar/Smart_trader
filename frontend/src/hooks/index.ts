/* ═══════════════════════════════════════════════
   SMART TRADER — Custom Hooks
   ═══════════════════════════════════════════════ */
import { useEffect, useRef, useState, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { api } from '../lib/api'
import { ws, marketWs, type MarketTick } from '../lib/ws'
import {
  useAuthStore, useDashboardStore, useMarketStore,
  useOptionChainStore, useToastStore, useBrokerAccountsStore,
  usePositionsDetailStore, useStrategyStatusStore, useMarketDepthStore,
} from '../stores'
import type { BrokerAccountWS } from '../stores'

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

function isValidDashboardPayload(data: any): boolean {
  if (!data || typeof data !== 'object') return false
  // Accept any structured response — backend is authoritative, even with empty arrays
  return 'positions' in data || 'orders' in data || 'holdings' in data || 'accountSummary' in data
}

// ── WebSocket live data — connects on auth, pushes to stores ──
export function useLiveData() {
  const isAuthenticated = useAuthStore(s => s.isAuthenticated)
  const connectedRef = useRef(false)

  useEffect(() => {
    if (!isAuthenticated) {
      if (connectedRef.current) {
        ws.disconnect()
        connectedRef.current = false
      }
      return
    }
    const token = localStorage.getItem('st_token')
    if (!token) return
    if (connectedRef.current) return

    // Initial REST fetch so broker account cards show immediately (WS has up to 2s delay)
    api.get<any>('/orders/broker-accounts')
      .then((d: any) => {
        const accs = Array.isArray(d) ? d : (d?.accounts ?? [])
        if (accs.length > 0) useBrokerAccountsStore.getState().setAccounts(accs)
      })
      .catch(() => {})

    // Also fetch dashboard immediately so data shows without waiting for WS
    api.liveDashboard()
      .then((data: any) => {
        if (isValidDashboardPayload(data)) {
          // Only set if store is still empty (WS hasn't pushed yet)
          if (!useDashboardStore.getState().data) {
            useDashboardStore.getState().setData(data as DashboardData)
          }
        }
      })
      .catch(() => {})

    // Handlers — use getState() to avoid subscribing App component to every store
    const onDashboard = (data: any) => {
      if (!isValidDashboardPayload(data)) return
      useDashboardStore.getState().setData(data as DashboardData)
    }
    const onBrokerAccounts = (data: any) => {
      if (!Array.isArray(data)) return
      // Anti-flicker: skip all-zero pushes when we already have meaningful data
      const st = useBrokerAccountsStore.getState()
      const cur = st.accounts
      const hasMeaningful = (accs: any[]) => accs.some(
        (a: any) => a.cash || a.available_margin || a.total_balance || a.used_margin
      )
      if (cur && cur.length > 0 && hasMeaningful(cur)) {
        if (!hasMeaningful(data as any[])) return
      }
      st.setAccounts(data as BrokerAccountWS[])
    }
    const onBrokerData = (data: any) => {
      if (data) useBrokerAccountsStore.getState().setBrokerData(data)
    }

    // Risk alerts — show toasts for real-time risk notifications
    const { toast } = useToastStore.getState()
    const onRiskAlerts = (data: any) => {
      if (!Array.isArray(data)) return
      for (const alert of data) {
        const level = (alert.level || 'INFO').toUpperCase()
        const toastType = level === 'CRITICAL' ? 'error'
          : level === 'WARNING' ? 'warning'
          : 'info'
        toast(
          alert.message || 'Risk event detected',
          toastType,
          level === 'CRITICAL' ? '⚠️ Risk Breach' : '⚠️ Risk Alert',
          level === 'CRITICAL' ? 15000 : 8000,
        )
      }
    }

    // Instant order/position updates — show toast + trigger immediate refresh
    const onOrderUpdate = (data: any) => {
      if (!data) return
      const action = data.action || ''
      const symbol = data.symbol || ''
      if (action === 'placed') {
        toast(`Order placed: ${symbol}`, 'success', '✅ Order', 3000)
      } else if (action === 'cancelled' || action === 'cancelled_all') {
        toast(`Order cancelled${symbol ? ': ' + symbol : ''}`, 'info', '🚫 Cancelled', 3000)
      }
    }
    const onPositionUpdate = (data: any) => {
      if (!data) return
      const action = data.action || ''
      if (action === 'squareoff' || action === 'squareoff_all') {
        toast(`Position squared off`, 'info', '📊 Position', 3000)
      }
    }

    // New WS event handlers for real-time data
    const onPositionsDetail = (data: any) => {
      if (Array.isArray(data)) usePositionsDetailStore.getState().setPositions(data)
    }
    const onStrategyStatus = (data: any) => {
      if (Array.isArray(data)) useStrategyStatusStore.getState().setStatuses(data)
    }
    const onMarketDepth = (data: any) => {
      if (data) useMarketDepthStore.getState().setData(data)
    }
    const onOptionChain = (data: any) => {
      if (data) {
        const { setData: setOCData } = useOptionChainStore.getState()
        setOCData(data)
      }
    }
    const onBrokerStatus = (data: any) => {
      if (data) {
        const { setIsBrokerLive } = useAuthStore.getState()
        setIsBrokerLive(data.isLive === true)
      }
    }

    ws.on('dashboard', onDashboard)
    ws.on('broker_accounts', onBrokerAccounts)
    ws.on('broker_data', onBrokerData)
    ws.on('risk_alerts', onRiskAlerts)
    ws.on('order_update', onOrderUpdate)
    ws.on('position_update', onPositionUpdate)
    ws.on('positions_detail', onPositionsDetail)
    ws.on('strategy_status', onStrategyStatus)
    ws.on('market_depth', onMarketDepth)
    ws.on('option_chain', onOptionChain)
    ws.on('broker_status', onBrokerStatus)
    ws.connect(token)
    // Connect market WS globally for real-time ticks across all pages
    marketWs.connect()
    connectedRef.current = true

    return () => {
      ws.off('dashboard', onDashboard)
      ws.off('broker_accounts', onBrokerAccounts)
      ws.off('broker_data', onBrokerData)
      ws.off('risk_alerts', onRiskAlerts)
      ws.off('order_update', onOrderUpdate)
      ws.off('position_update', onPositionUpdate)
      ws.off('positions_detail', onPositionsDetail)
      ws.off('strategy_status', onStrategyStatus)
      ws.off('market_depth', onMarketDepth)
      ws.off('option_chain', onOptionChain)
      ws.off('broker_status', onBrokerStatus)
      ws.disconnect()
      connectedRef.current = false
    }
  }, [isAuthenticated])
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
      .catch((err: any) => {
        // Clear token on auth errors (401=expired/invalid, 404=user deleted).
        // Network errors (no status) keep the token so a service restart doesn't force re-login.
        if (err?.status === 401 || err?.status === 404) {
          localStorage.removeItem('st_token')
        }
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
    const t = setInterval(pollBroker, 1_000)  // 1s fallback for instant broker-live state updates
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
      setData(EMPTY_DASHBOARD)
      return
    }
    if (!isBrokerLive) {
      setData(EMPTY_DASHBOARD)
      return
    }
    // Skip REST poll if WS pushed recently (< 1s) — avoid overwrite race
    const lastWs = useDashboardStore.getState().lastUpdate
    if (lastWs && Date.now() - lastWs < 1_000) return
    try {
      const data = await api.liveDashboard() as DashboardData
      if (!isValidDashboardPayload(data)) return
      if ((data as any).source === 'demo' && !(data as any).positions?.length) {
        setData({ ...data, positions: [], orders: [], holdings: [], trades: [] } as DashboardData)
      } else {
        setData(data)
      }
    } catch {
      // Keep last known state
    }
  }, [isAuthenticated, isBrokerLive])

  useEffect(() => {
    setLoading(true)
    fetch()
    intervalRef.current = setInterval(fetch, 1_000)  // 1s REST fallback when WS misses
    return () => clearInterval(intervalRef.current)
  }, [fetch])
}

// ── Market indices polling + WS live overlay ─────
const _INDEX_SYMBOLS = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX']

export function useMarketIndices() {
  const { setIndices } = useMarketStore()
  const { isAuthenticated } = useAuthStore()
  const indicesRef = useRef<any[]>([])

  useEffect(() => {
    const load = async () => {
      try {
        // Always try live API first — Fyers provides data even without broker login
        const res = await api.indices() as any
        const data = Array.isArray(res) ? res : (res.data ?? [])
        if (data.length > 0) {
          indicesRef.current = data
          setIndices(data)
          return
        }
      } catch { /* fall through */ }
      // API failed — show empty (no fake data)
      setIndices([])
    }
    load()
    // 1s REST fallback — WS ticks are primary, REST is safety net
    const t = setInterval(load, 1000)

    // Subscribe index symbols to marketWs for instant LTP updates
    marketWs.connect()
    marketWs.subscribe(_INDEX_SYMBOLS)
    const normSym = (s: string) => s.toUpperCase().replace(/-INDEX|-EQ|-BE/g, '').replace(/\s/g, '').replace('NIFTY50', 'NIFTY').replace('NIFTYBANK', 'BANKNIFTY')
    const unsubTick = marketWs.onTick((tick: MarketTick) => {
      const sym = normSym(tick.symbol)
      const idx = indicesRef.current.findIndex(
        (i: any) => normSym(i.symbol ?? i.name ?? '') === sym
      )
      if (idx < 0) return
      const updated = [...indicesRef.current]
      updated[idx] = {
        ...updated[idx],
        ltp: tick.ltp,
        change: tick.change ?? updated[idx].change,
        changePct: tick.changePct ?? updated[idx].changePct,
        high: tick.high || updated[idx].high,
        low: tick.low || updated[idx].low,
      }
      indicesRef.current = updated
      setIndices(updated)
    })

    return () => {
      clearInterval(t)
      unsubTick()
    }
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
      setScreener([])
    }
    load()
    const t = setInterval(load, 1000)  // 1s screener fallback for scalping
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
    const t = setInterval(load, 1000)
    return () => clearInterval(t)
  }, [isAuthenticated])
}

// ── Option chain loading ────────────────────────


// Underlying → equity exchange mapping (backend maps to F&O exchange)
const _UL_EXCHANGE: Record<string, string> = {
  NIFTY: 'NSE', BANKNIFTY: 'NSE', FINNIFTY: 'NSE', MIDCPNIFTY: 'NSE',
  SENSEX: 'BSE', BANKEX: 'BSE',
  CRUDEOIL: 'MCX', GOLD: 'MCX', SILVER: 'MCX', NATURALGAS: 'MCX', COPPER: 'MCX',
  USDINR: 'CDS',
}

export function useOptionChain() {
  const { selectedUnderlying, selectedExpiry, data: existingData, setData, setLoading } = useOptionChainStore()

  useEffect(() => {
    let cancelled = false
    const exchange = _UL_EXCHANGE[selectedUnderlying] || 'NSE'

    // Subscribe via WS for real-time push; SmartTraderWS will restore on reconnect.
    ws.subscribeOptionChain(selectedUnderlying, selectedExpiry || '', exchange)

    const load = async () => {
      // Skip REST only if WS pushed same underlying within last 1s
      const storeState = useOptionChainStore.getState()
      if (
        storeState.data
        && storeState.data.underlying === selectedUnderlying
        && (storeState.data.expiry || '') === (selectedExpiry || '')
      ) {
        const age = Date.now() - storeState.lastUpdate
        if (age < 1_000) return  // WS is actively feeding — skip REST
      }
      if (!existingData) setLoading(true)
      try {
        const data = await api.optionChain(selectedUnderlying, selectedExpiry || undefined, exchange) as any
        if (cancelled) return
        if (data) {
          setData(data)
          return
        }
      } catch { /* fall through */ }
      if (cancelled) return
      setData({ underlying: selectedUnderlying, underlyingLtp: 0, expiry: selectedExpiry || '', expiries: [], pcr: 0, maxPainStrike: 0, rows: [] } as any)
    }

    // Initial REST load (WS needs subscribe handshake first)
    load()
    // 1s REST fallback — WS pushes every ~1s
    const t = setInterval(load, 1_000)
    return () => {
      cancelled = true
      clearInterval(t)
      ws.unsubscribeOptionChain()
    }
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
        const resp = await api.search(query) as any
        const data = Array.isArray(resp) ? resp : (resp?.data ?? [])
        setResults(data)
      } catch {
        setResults([])
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
