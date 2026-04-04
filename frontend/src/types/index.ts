/* ═══════════════════════════════════════════════
   SMART TRADER — Central Type Definitions
   ═══════════════════════════════════════════════ */

// ── Auth & User ──────────────────────────────────
export type BrokerName =
  | 'zerodha' | 'upstox' | 'fyers' | 'shoonya' | 'angel' | 'dhan'
  | 'groww' | 'icici_direct' | 'hdfc_sky' | 'kotak' | '5paisa'
  | 'motilal' | 'sharekhan' | 'iifl' | 'alice_blue' | 'choice'
  | 'paper_trade'

export const BROKER_NAMES: BrokerName[] = [
  'zerodha','upstox','fyers','shoonya','angel','dhan',
  'groww','icici_direct','hdfc_sky','kotak','5paisa',
  'motilal','sharekhan','iifl','alice_blue','choice','paper_trade',
]

export interface User {
  id: string
  name: string
  email: string
  phone?: string
  role: 'admin' | 'user' | 'viewer'
  avatar?: string
  createdAt: string
}

export interface BrokerAccount {
  id: string
  userId: string
  broker: BrokerName
  clientId: string
  name: string
  status: 'connected' | 'disconnected' | 'error' | 'expired'
  lastSync: string | null
  // margin fields
  availableMargin: number
  usedMargin: number
  totalBalance: number
}

export interface AuthState {
  user: User | null
  accounts: BrokerAccount[]
  activeAccountId: string | null
  isAuthenticated: boolean
  isChecking: boolean
}

// ── Market ──────────────────────────────────────
export type Exchange = 'NSE' | 'BSE' | 'NFO' | 'BFO' | 'CDS' | 'MCX' | 'NCDEX'

export type InstrumentType = 'EQ' | 'FUT' | 'CE' | 'PE' | 'IDX' | 'ETF' | 'MF'

export interface Instrument {
  token: string
  symbol: string
  tradingsymbol: string
  exchange: Exchange
  type: InstrumentType
  name: string
  lot_size: number
  tick_size: number
  expiry?: string
  strike?: number
}

export interface Quote {
  token: string
  symbol: string
  exchange: Exchange
  ltp: number
  open: number
  high: number
  low: number
  close: number
  prevClose: number
  change: number
  changePct: number
  volume: number
  oi?: number
  bid?: number
  ask?: number
  bidQty?: number
  askQty?: number
  updatedAt: number
}

export interface IndexQuote extends Quote {
  advances: number
  declines: number
  unchanged: number
}

// ── Positions ───────────────────────────────────
export type PositionSide = 'BUY' | 'SELL'
export type ProductType = 'MIS' | 'NRML' | 'CNC' | 'BO' | 'CO'

export interface Position {
  id: string
  accountId: string
  symbol: string
  tradingsymbol: string
  exchange: Exchange
  product: ProductType
  quantity: number        // net qty (+ve = long, -ve = short)
  avgPrice: number
  ltp: number
  pnl: number
  pnlPct: number
  dayPnl: number
  value: number           // qty * ltp
  multiplier: number
  side: PositionSide
  type: InstrumentType
}

// ── Holdings ────────────────────────────────────
export interface Holding {
  id: string
  accountId: string
  symbol: string
  exchange: Exchange
  isin: string
  quantity: number
  avgCost: number
  ltp: number
  currentValue: number
  investedValue: number
  pnl: number
  pnlPct: number
  dayChange: number
  dayChangePct: number
}

// ── Orders ──────────────────────────────────────
export type OrderStatus =
  | 'OPEN' | 'PENDING' | 'COMPLETE' | 'CANCELLED' | 'REJECTED' | 'AMO' | 'TRIGGER_PENDING'

export type OrderType = 'MARKET' | 'LIMIT' | 'SL' | 'SL-M'
export type Validity  = 'DAY' | 'IOC' | 'GTT' | 'GTC'
export type TransactionType = 'BUY' | 'SELL'

export interface Order {
  id: string
  accountId: string
  orderId: string           // broker order id
  symbol: string
  tradingsymbol: string
  exchange: Exchange
  type: InstrumentType
  transactionType: TransactionType
  orderType: OrderType
  product: ProductType
  quantity: number
  filledQty: number
  price: number
  triggerPrice?: number
  avgPrice?: number
  status: OrderStatus
  statusMessage?: string
  validity: Validity
  tag?: string
  placedAt: string
  updatedAt: string
}

// ── Trades ──────────────────────────────────────
export interface Trade {
  id: string
  accountId: string
  orderId: string
  tradeId: string
  symbol: string
  tradingsymbol: string
  exchange: Exchange
  transactionType: TransactionType
  product: ProductType
  quantity: number
  price: number
  value: number
  charges: number
  tradedAt: string
}

// ── Option Chain ─────────────────────────────────
export interface OptionLeg {
  strike: number
  expiry: string
  oi: number
  oiChange: number
  oiChangePct: number
  volume: number
  iv: number
  ltp: number
  bid: number
  ask: number
  delta: number
  gamma: number
  theta: number
  vega: number
  rho: number
}

export interface OptionChainRow {
  strike: number
  isATM: boolean
  call: OptionLeg
  put: OptionLeg
}

export interface OptionChainData {
  underlying: string
  underlyingLtp: number
  expiry: string
  expiries: string[]
  pcr: number            // put-call ratio
  maxPainStrike: number
  rows: OptionChainRow[]
}

// ── Basket Order ─────────────────────────────────
export interface BasketLeg {
  id: string
  symbol: string
  tradingsymbol: string
  exchange: Exchange
  transactionType: TransactionType
  quantity: number
  orderType: OrderType
  price: number
  product: ProductType
  ltp?: number
}

// ── Place Order Form ─────────────────────────────
export interface PlaceOrderForm {
  accountId: string
  symbol: string
  tradingsymbol?: string
  exchange: Exchange
  side: 'BUY' | 'SELL'
  orderType: OrderType
  product: ProductType
  qty: number
  price?: number
  triggerPrice?: number
  validity: Validity
  tag?: string
  disclosedQty?: number
}

// ── Watchlist ────────────────────────────────────
export interface WatchlistItem {
  id: string
  symbol: string
  tradingsymbol: string
  exchange: Exchange
  type: InstrumentType
  addedAt: number
}

export interface Watchlist {
  id: string
  name: string
  items: WatchlistItem[]
}

// ── Risk Manager ─────────────────────────────────
export interface RiskMetrics {
  accountId: string
  dailyPnl: number
  dailyPnlLimit: number        // max allowed daily loss
  mtmPnl: number
  maxPositionValue: number
  leverageUsed: number
  maxLeverage: number
  positionCount: number
  maxPositions: number
  riskStatus: 'SAFE' | 'WARNING' | 'CRITICAL' | 'BREACHED'
  alerts: RiskAlert[]
}

export interface RiskAlert {
  id: string
  level: 'info' | 'warning' | 'critical'
  message: string
  triggeredAt: string
}

// ── Copy Trading ─────────────────────────────────
export interface CopyTradeLink {
  id: string
  masterAccountId: string
  followerAccountId: string
  multiplier: number
  maxOrderValue: number
  copySide: 'both' | 'buy' | 'sell'
  status: 'active' | 'paused' | 'stopped'
  createdAt: string
}

// ── Webhook ──────────────────────────────────────
export interface WebhookConfig {
  id: string
  name: string
  url: string               // the webhook URL to provide to TradingView/Chartink
  token: string             // secret token
  accountIds: string[]      // target accounts
  status: 'active' | 'paused'
  lastTriggered?: string
  triggerCount: number
}

// ── Market Screener ──────────────────────────────
export interface ScreenerRow {
  symbol: string
  tradingsymbol?: string
  name: string
  exchange: string
  ltp: number
  change: number
  changePct: number
  volume: number
  marketCap?: number
  pe?: number
  high52w?: number
  low52w?: number
  rsi?: number
  ma20?: number
  ma50?: number
}

// ── System / Diagnostics ─────────────────────────
export interface SystemStatus {
  backendStatus: 'online' | 'offline' | 'degraded'
  wsStatus: 'connected' | 'disconnected' | 'reconnecting'
  dbStatus: 'ok' | 'error'
  brokerStatuses: Record<string, 'connected' | 'disconnected' | 'error'>
  latencyMs: number
  uptime: number
  version: string
  lastHeartbeat: string
}

// ── Notifications / Toast ─────────────────────────
export type ToastType = 'success' | 'error' | 'warning' | 'info'

export interface Toast {
  id: string
  type: ToastType
  title?: string
  message: string
  duration: number
}

// ── Chart ─────────────────────────────────────────
export interface ChartBar {
  time: number     // unix seconds
  open: number
  high: number
  low: number
  close: number
  volume?: number
}

export type ChartInterval =
  '1m' | '3m' | '5m' | '10m' | '15m' | '30m' | '1h' | '2h' | '4h' | 'D' | 'W'

// ── Settings ──────────────────────────────────────
export interface AppSettings {
  theme: 'dark' | 'midnight' | 'charcoal' | 'light' | 'ocean'
  defaultProduct: ProductType
  defaultOrderType: OrderType
  confirmOrders: boolean
  soundAlerts: boolean
  notifications: boolean
  desktopNotifications: boolean
  defaultWatchlist: string
  timezone: string
  language: string
  chartInterval: ChartInterval
  optionChainDepth: number
  refetchIntervalMs: number
  dataSource: 'broker' | 'fyers' | 'websocket'
  fontSize: 'small' | 'medium' | 'large'
  density: 'compact' | 'normal' | 'comfortable'
}

// ── Dashboard snapshot ────────────────────────────
export interface DashboardData {
  positions: Position[]
  holdings: Holding[]
  orders: Order[]
  trades: Trade[]
  riskMetrics: RiskMetrics | null
  accountSummary: {
    totalEquity: number
    dayPnl: number
    dayPnlPct: number
    unrealizedPnl: number
    realizedPnl: number
    usedMargin: number
    availableMargin: number
  }
}
