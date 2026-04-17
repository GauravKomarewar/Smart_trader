/* ═══════════════════════════════════════════════
   SMART TRADER — Zustand Stores
   ═══════════════════════════════════════════════ */
import { create } from 'zustand'
import { persist } from 'zustand/middleware'
import type {
  User, BrokerAccount, Toast, ToastType,
  DashboardData, Position, Order,
  Watchlist, WatchlistItem, AppSettings,
  OptionChainData, ChartInterval,
} from '../types'
import { uid } from '../lib/utils'

let _toastId = 0

// ── Toast Store ──────────────────────────────────
interface ToastStore {
  toasts: Toast[]
  toast: (message: string, type: ToastType, title?: string, duration?: number) => void
  dismiss: (id: string) => void
}

export const useToastStore = create<ToastStore>((set) => ({
  toasts: [],
  toast: (message, type = 'info', title, duration = 4500) => {
    const id = String(++_toastId)
    set(s => ({ toasts: [...s.toasts, { id, type, message, title, duration }] }))
    if (duration > 0) setTimeout(() =>
      set(s => ({ toasts: s.toasts.filter(t => t.id !== id) })), duration)
  },
  dismiss: (id) => set(s => ({ toasts: s.toasts.filter(t => t.id !== id) })),
}))

// ── Auth Store ───────────────────────────────────
interface AuthStore {
  user: User | null
  accounts: BrokerAccount[]
  activeAccountId: string | null
  isAuthenticated: boolean
  isChecking: boolean
  isBrokerLive: boolean
  setUser: (u: User | null) => void
  setAccounts: (a: BrokerAccount[]) => void
  setActiveAccount: (id: string) => void
  setAuthenticated: (v: boolean) => void
  setChecking: (v: boolean) => void
  setIsBrokerLive: (v: boolean) => void
  logout: () => void
}

export const useAuthStore = create<AuthStore>((set) => ({
  user: null,
  accounts: [],
  activeAccountId: null,
  isAuthenticated: false,
  isChecking: true,
  isBrokerLive: false,
  setUser: (user) => set({ user }),
  setAccounts: (accounts) => set({ accounts, activeAccountId: accounts[0]?.id ?? null }),
  setActiveAccount: (id) => set({ activeAccountId: id }),
  setAuthenticated: (v) => set({ isAuthenticated: v }),
  setChecking: (v) => set({ isChecking: v }),
  setIsBrokerLive: (v) => set({ isBrokerLive: v }),
  logout: () => {
    localStorage.removeItem('st_token')
    set({ user: null, accounts: [], activeAccountId: null, isAuthenticated: false, isBrokerLive: false })
  },
}))

// ── UI Store ─────────────────────────────────────
interface UIStore {
  sidebarOpen: boolean
  orderModalOpen: boolean
  orderModalSymbol: string | undefined
  orderModalExchange: string | undefined
  chartModalOpen: boolean
  chartModalToken: string | undefined
  searchOpen: boolean
  shortcutsOpen: boolean
  setSidebarOpen: (v: boolean) => void
  toggleSidebar: () => void
  openOrderModal: (symbol?: string, exchange?: string) => void
  closeOrderModal: () => void
  openChartModal: (token: string) => void
  closeChartModal: () => void
  setSearchOpen: (v: boolean) => void
  setShortcutsOpen: (v: boolean) => void
}

export const useUIStore = create<UIStore>((set) => ({
  sidebarOpen: typeof window !== 'undefined' && window.innerWidth >= 1024,
  orderModalOpen: false,
  orderModalSymbol: undefined,
  orderModalExchange: undefined,
  chartModalOpen: false,
  chartModalToken: undefined,
  searchOpen: false,
  shortcutsOpen: false,
  setSidebarOpen: (v) => set({ sidebarOpen: v }),
  toggleSidebar: () => set(s => ({ sidebarOpen: !s.sidebarOpen })),
  openOrderModal: (symbol?: string, exchange?: string) => set({ orderModalOpen: true, orderModalSymbol: symbol, orderModalExchange: exchange }),
  closeOrderModal: () => set({ orderModalOpen: false, orderModalSymbol: undefined, orderModalExchange: undefined }),
  openChartModal: (token) => set({ chartModalOpen: true, chartModalToken: token }),
  closeChartModal: () => set({ chartModalOpen: false, chartModalToken: undefined }),
  setSearchOpen: (v) => set({ searchOpen: v }),
  setShortcutsOpen: (v) => set({ shortcutsOpen: v }),
}))

// ── Dashboard / Positions Store ──────────────────
interface DashboardStore {
  data: DashboardData | null
  lastUpdate: number
  isLoading: boolean
  orderFilter: 'all' | 'pending' | 'complete' | 'cancelled' | 'rejected'
  setData: (d: DashboardData) => void
  setLoading: (v: boolean) => void
  setOrderFilter: (v: 'all' | 'pending' | 'complete' | 'cancelled' | 'rejected') => void
  updatePosition: (p: Position) => void
  updateOrder: (o: Order) => void
}

export const useDashboardStore = create<DashboardStore>((set) => ({
  data: null,
  lastUpdate: 0,
  isLoading: true,
  orderFilter: 'pending' as const,
  setData: (data) => set({ data, lastUpdate: Date.now(), isLoading: false }),
  setLoading: (v) => set({ isLoading: v }),
  setOrderFilter: (v) => set({ orderFilter: v }),
  updatePosition: (p) => set(s => {
    if (!s.data) return {}
    const positions = s.data.positions.map(pos => pos.id === p.id ? p : pos)
    return { data: { ...s.data, positions }, lastUpdate: Date.now() }
  }),
  updateOrder: (o) => set(s => {
    if (!s.data) return {}
    const orders = s.data.orders.map(ord => ord.id === o.id ? o : ord)
    return { data: { ...s.data, orders }, lastUpdate: Date.now() }
  }),
}))

// ── Watchlist Store ──────────────────────────────
interface WatchlistStore {
  watchlists: Watchlist[]
  activeId: string
  addWatchlist: (name: string) => void
  removeWatchlist: (id: string) => void
  addItem: (watchlistId: string, item: Omit<WatchlistItem, 'id' | 'addedAt'>) => void
  removeItem: (watchlistId: string, itemId: string) => void
  setActive: (id: string) => void
}

function watchlistInstrumentKey(item: Pick<WatchlistItem, 'exchange' | 'symbol' | 'tradingsymbol'>) {
  const base = item.tradingsymbol || item.symbol
  return `${item.exchange}:${base}`.toUpperCase()
}

const defaultWatchlist: Watchlist = {
  id: 'wl-default',
  name: 'My Watchlist',
  items: [
    { id: uid(), symbol: 'NIFTY',     tradingsymbol: 'NIFTY',     exchange: 'NSE', type: 'IDX', addedAt: Date.now() },
    { id: uid(), symbol: 'BANKNIFTY', tradingsymbol: 'BANKNIFTY', exchange: 'NSE', type: 'IDX', addedAt: Date.now() },
    { id: uid(), symbol: 'RELIANCE',  tradingsymbol: 'RELIANCE',  exchange: 'NSE', type: 'EQ',  addedAt: Date.now() },
    { id: uid(), symbol: 'TCS',       tradingsymbol: 'TCS',       exchange: 'NSE', type: 'EQ',  addedAt: Date.now() },
    { id: uid(), symbol: 'HDFCBANK',  tradingsymbol: 'HDFCBANK',  exchange: 'NSE', type: 'EQ',  addedAt: Date.now() },
    { id: uid(), symbol: 'INFY',      tradingsymbol: 'INFY',      exchange: 'NSE', type: 'EQ',  addedAt: Date.now() },
    { id: uid(), symbol: 'ICICIBANK', tradingsymbol: 'ICICIBANK', exchange: 'NSE', type: 'EQ',  addedAt: Date.now() },
    { id: uid(), symbol: 'LT',        tradingsymbol: 'LT',        exchange: 'NSE', type: 'EQ',  addedAt: Date.now() },
  ],
}

export const useWatchlistStore = create<WatchlistStore>()(
  persist(
    (set) => ({
      watchlists: [defaultWatchlist],
      activeId: 'wl-default',
      addWatchlist: (name) => {
        const id = `wl-${uid()}`
        set(s => ({ watchlists: [...s.watchlists, { id, name, items: [] }] }))
      },
      removeWatchlist: (id) => set(s => ({
        watchlists: s.watchlists.filter(w => w.id !== id),
        activeId: s.activeId === id ? (s.watchlists[0]?.id ?? 'wl-default') : s.activeId,
      })),
      addItem: (wid, item) => set(s => ({
        watchlists: s.watchlists.map(w =>
          w.id !== wid || w.items.some(i => watchlistInstrumentKey(i) === watchlistInstrumentKey(item as WatchlistItem)) ? w
            : { ...w, items: [...w.items, { ...item, id: uid(), addedAt: Date.now() }] }
        ),
      })),
      removeItem: (wid, itemId) => set(s => ({
        watchlists: s.watchlists.map(w =>
          w.id !== wid ? w : { ...w, items: w.items.filter(i => i.id !== itemId) }
        ),
      })),
      setActive: (id) => set({ activeId: id }),
    }),
    { name: 'st-watchlists' }
  )
)

// ── Option Chain Store ────────────────────────────
interface OptionChainStore {
  data: OptionChainData | null
  selectedUnderlying: string
  selectedExpiry: string
  lastUpdate: number
  isLoading: boolean
  basket: import('../types').BasketLeg[]
  setData: (d: OptionChainData) => void
  setUnderlying: (s: string) => void
  setExpiry: (e: string) => void
  setLoading: (v: boolean) => void
  addToBasket: (leg: import('../types').BasketLeg) => void
  removeFromBasket: (id: string) => void
  clearBasket: () => void
}

export const useOptionChainStore = create<OptionChainStore>((set) => ({
  data: null,
  selectedUnderlying: 'NIFTY',
  selectedExpiry: '',
  lastUpdate: 0,
  isLoading: false,
  basket: [],
  setData: (data) => set({ data, isLoading: false, lastUpdate: Date.now() }),
  setUnderlying: (s) => set({ selectedUnderlying: s, data: null }),
  setExpiry: (e) => set({ selectedExpiry: e }),
  setLoading: (v) => set({ isLoading: v }),
  addToBasket: (leg) => set(s => ({ basket: [...s.basket, leg] })),
  removeFromBasket: (id) => set(s => ({ basket: s.basket.filter(b => b.id !== id) })),
  clearBasket: () => set({ basket: [] }),
}))

// ── Settings Store ────────────────────────────────
const defaultSettings: AppSettings = {
  theme: 'dark',
  defaultProduct: 'MIS',
  defaultOrderType: 'LIMIT',
  confirmOrders: true,
  soundAlerts: true,
  notifications: false,
  desktopNotifications: false,
  defaultWatchlist: 'wl-default',
  timezone: 'Asia/Kolkata',
  language: 'en-IN',
  chartInterval: '5m',
  optionChainDepth: 10,
  refetchIntervalMs: 5000,
  dataSource: 'broker',
  fontSize: 'medium',
  density: 'compact',
}

interface SettingsStore {
  settings: AppSettings
  update: (patch: Partial<AppSettings>) => void
}

export const useSettingsStore = create<SettingsStore>()(
  persist(
    (set) => ({
      settings: defaultSettings,
      update: (patch) => set(s => ({ settings: { ...s.settings, ...patch } })),
    }),
    { name: 'st-settings' }
  )
)

// ── Market Store ──────────────────────────────────
interface MarketStore {
  indices: import('../types').IndexQuote[]
  screener: import('../types').ScreenerRow[]
  globalMarkets: any[]
  setIndices: (v: import('../types').IndexQuote[]) => void
  setScreener: (v: import('../types').ScreenerRow[]) => void
  setGlobalMarkets: (v: any[]) => void
}

export const useMarketStore = create<MarketStore>((set) => ({
  indices: [],
  screener: [],
  globalMarkets: [],
  setIndices: (v) => set({ indices: v }),
  setScreener: (v) => set({ screener: v }),
  setGlobalMarkets: (v) => set({ globalMarkets: v }),
}))

// ── Chart Store ────────────────────────────────────
interface ChartStore {
  interval: ChartInterval
  showVolume: boolean
  showIndicators: string[]
  setInterval: (v: ChartInterval) => void
  toggleVolume: () => void
  toggleIndicator: (name: string) => void
}

export const useChartStore = create<ChartStore>((set) => ({
  interval: '5m',
  showVolume: true,
  showIndicators: ['ma20', 'ma50'],
  setInterval: (v) => set({ interval: v }),
  toggleVolume: () => set(s => ({ showVolume: !s.showVolume })),
  toggleIndicator: (name) => set(s => ({
    showIndicators: s.showIndicators.includes(name)
      ? s.showIndicators.filter(i => i !== name)
      : [...s.showIndicators, name],
  })),
}))

// ── Live Broker Accounts Store (fed by WebSocket) ──
export interface BrokerAccountWS {
  config_id: string
  broker_id: string
  broker_name: string
  client_id: string
  is_live: boolean
  mode: string
  connected_at: string | null
  cash: number
  collateral: number
  available_margin: number
  used_margin: number
  total_balance: number
  payin: number
  payout: number
  day_pnl: number
  unrealized_pnl: number
  realized_pnl: number
  positions_count: number
  orders_count: number
  open_orders: number
  completed_orders: number
  trades_count: number
  risk_status: boolean
  risk_daily_pnl: number
  risk_halt_reason: string | null
  risk_force_exit: boolean
  error: string | null
  raw_limits: Record<string, any>
  state?: string
  data_stale?: boolean
}

interface BrokerDataWS {
  positions: any[]
  holdings: any[]
  orders: any[]
  trades: any[]
  config_id: string
}

interface BrokerAccountsStore {
  accounts: BrokerAccountWS[]
  brokerData: BrokerDataWS | null
  lastUpdate: number
  setAccounts: (a: BrokerAccountWS[]) => void
  setBrokerData: (d: BrokerDataWS | null) => void
}

export const useBrokerAccountsStore = create<BrokerAccountsStore>((set) => ({
  accounts: [],
  brokerData: null,
  lastUpdate: 0,
  setAccounts: (accounts) => set({ accounts, lastUpdate: Date.now() }),
  setBrokerData: (brokerData) => set({ brokerData }),
}))

// ── Live Positions Store (fed by WS positions_detail) ──
interface PositionsDetailStore {
  positions: any[]
  lastUpdate: number
  setPositions: (p: any[]) => void
}

export const usePositionsDetailStore = create<PositionsDetailStore>((set) => ({
  positions: [],
  lastUpdate: 0,
  setPositions: (positions) => set({ positions, lastUpdate: Date.now() }),
}))

// ── Live Strategy Status Store (fed by WS strategy_status) ──
interface StrategyStatusStore {
  statuses: any[]
  lastUpdate: number
  setStatuses: (s: any[]) => void
}

export const useStrategyStatusStore = create<StrategyStatusStore>((set) => ({
  statuses: [],
  lastUpdate: 0,
  setStatuses: (statuses) => set({ statuses, lastUpdate: Date.now() }),
}))

// ── Live Market Depth Store (fed by WS market_depth) ──
interface MarketDepthStore {
  data: any | null
  lastUpdate: number
  setData: (d: any) => void
}

export const useMarketDepthStore = create<MarketDepthStore>((set) => ({
  data: null,
  lastUpdate: 0,
  setData: (data) => set({ data, lastUpdate: Date.now() }),
}))
