/* ═══════════════════════════════════════════════
   SMART TRADER — WebSocket manager
   Auto-reconnect, subscription-based feed
   Handles: dashboard, broker_accounts, broker_data,
            quote, order_update, position_update, heartbeat
   ═══════════════════════════════════════════════ */
import type { Quote } from '../types'

type WsEventType =
  | 'quote' | 'order_update' | 'position_update' | 'alert' | 'heartbeat'
  | 'dashboard' | 'broker_accounts' | 'broker_data'
  | 'broker_subscribed' | 'broker_unsubscribed' | 'pong'
  | 'risk_alerts' | 'risk_snapshot'
  | 'force_refresh'
  | 'positions_detail' | 'strategy_status' | 'broker_status'
  | 'option_chain' | 'option_chain_subscribed' | 'option_chain_unsubscribed'
  | 'market_depth' | 'market_depth_subscribed' | 'market_depth_unsubscribed'

interface WsMessage {
  type: WsEventType
  data: unknown
  ts?: number
}

type Subscriber<T = unknown> = (data: T) => void

class SmartTraderWS {
  private ws: WebSocket | null = null
  private url: string = ''
  private reconnectDelay = 1000
  private maxDelay = 15_000
  private pingTimer: ReturnType<typeof setInterval> | null = null
  private subs: Map<WsEventType, Set<Subscriber>> = new Map()
  private _open = false
  private _subscribedBroker: string | null = null

  get isOpen() { return this._open }

  connect(token: string) {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws'
    this.url = `${proto}://${location.host}/ws/feed?token=${token}`
    this._connect()
  }

  private _connect() {
    if (this.ws) { this.ws.onclose = null; this.ws.close() }
    this.ws = new WebSocket(this.url)

    this.ws.onopen = () => {
      this._open = true
      this.reconnectDelay = 1000  // reset on successful connection
      this._startPing()
      // restore broker subscription on reconnect
      if (this._subscribedBroker) {
        this._send({ action: 'subscribe_broker', config_id: this._subscribedBroker })
      }
      // Request immediate data push on connect/reconnect
      this._send({ action: 'force_refresh' })
    }

    this.ws.onmessage = (ev) => {
      try {
        const msg: WsMessage = JSON.parse(ev.data)
        // Reset reconnect delay on first real data (not just connection open)
        if (msg.type !== 'heartbeat') this.reconnectDelay = 2000
        const handlers = this.subs.get(msg.type)
        if (handlers) handlers.forEach(h => h(msg.data))
      } catch { /* ignore malformed */ }
    }

    this.ws.onclose = () => {
      this._open = false
      this._stopPing()
      setTimeout(() => {
        this.reconnectDelay = Math.min(this.reconnectDelay * 1.3, this.maxDelay)
        this._connect()
      }, this.reconnectDelay)
    }

    this.ws.onerror = () => { this.ws?.close() }
  }

  private _send(payload: unknown) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(payload))
    }
  }

  private _startPing() {
    this.pingTimer = setInterval(() => this._send({ action: 'ping' }), 15_000)
  }

  private _stopPing() {
    if (this.pingTimer) { clearInterval(this.pingTimer); this.pingTimer = null }
  }

  /** Subscribe to a specific broker's filtered data feed */
  subscribeBroker(configId: string) {
    this._subscribedBroker = configId
    this._send({ action: 'subscribe_broker', config_id: configId })
  }

  /** Unsubscribe from broker-specific feed */
  unsubscribeBroker() {
    this._subscribedBroker = null
    this._send({ action: 'unsubscribe_broker' })
  }

  /** Force immediate data refresh from backend */
  forceRefresh() {
    this._send({ action: 'force_refresh' })
  }

  /** Subscribe to option chain data for a symbol */
  subscribeOptionChain(symbol: string, expiry?: string, exchange?: string) {
    this._send({ action: 'subscribe_option_chain', symbol, expiry: expiry || '', exchange: exchange || 'NSE' })
  }

  /** Unsubscribe from option chain feed */
  unsubscribeOptionChain() {
    this._send({ action: 'unsubscribe_option_chain' })
  }

  /** Subscribe to market depth for a symbol */
  subscribeMarketDepth(symbol: string) {
    this._send({ action: 'subscribe_market_depth', symbol })
  }

  /** Unsubscribe from market depth feed */
  unsubscribeMarketDepth() {
    this._send({ action: 'unsubscribe_market_depth' })
  }

  on<T>(event: WsEventType, handler: Subscriber<T>) {
    if (!this.subs.has(event)) this.subs.set(event, new Set())
    this.subs.get(event)!.add(handler as Subscriber)
  }

  off<T>(event: WsEventType, handler: Subscriber<T>) {
    this.subs.get(event)?.delete(handler as Subscriber)
  }

  disconnect() {
    this._stopPing()
    this._subscribedBroker = null
    if (this.ws) { this.ws.onclose = null; this.ws.close(); this.ws = null }
    this._open = false
  }
}

export const ws = new SmartTraderWS()

// ── Quote cache (in-memory LTP store) ───────────────
const quoteCache = new Map<string, Quote>()

ws.on<Quote>('quote', (q) => {
  quoteCache.set(q.token, q)
})

export function getLtp(token: string): number | undefined {
  return quoteCache.get(token)?.ltp
}

export function getQuote(token: string): Quote | undefined {
  return quoteCache.get(token)
}


// ═══════════════════════════════════════════════════════
//  MarketWS — /ws/market  real-time symbol tick feed
//  No auth required. Subscibes to any symbol list.
// ═══════════════════════════════════════════════════════

export interface MarketTick {
  symbol:    string
  exchange:  string
  ltp:       number
  change:    number
  changePct: number
  open:      number
  high:      number
  low:       number
  close:     number
  volume:    number
  oi:        number
  tick_time: string
  source:    string
}

type TickHandler = (tick: MarketTick) => void
type StrategyEventHandler = (payload: { run_id: string; strategy: string; event: any }) => void
type MarketMsgType = 'connected' | 'subscribed' | 'unsubscribed' | 'tick' | 'heartbeat' | 'pong' | 'error' | 'strategy_event'

class MarketWebSocket {
  private ws: WebSocket | null = null
  private _open = false
  private reconnectDelay = 1000
  private maxDelay = 15_000
  private pingTimer: ReturnType<typeof setInterval> | null = null
  private pendingSubscribe: Set<string> = new Set()
  private subscribed: Set<string> = new Set()
  private tickHandlers: Set<TickHandler> = new Set()
  private strategyEventHandlers: Set<StrategyEventHandler> = new Set()

  connect() {
    if (this.ws && this.ws.readyState <= WebSocket.OPEN) return
    const proto = location.protocol === 'https:' ? 'wss' : 'ws'
    const url = `${proto}://${location.host}/ws/market`
    this.ws = new WebSocket(url)

    this.ws.onopen = () => {
      this._open = true
      this._startPing()
      // Re-subscribe any pending or previously subscribed symbols
      const all = new Set([...this.pendingSubscribe, ...this.subscribed])
      if (all.size > 0) {
        this._send({ action: 'subscribe', symbols: [...all] })
      }
    }

    this.ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data) as { type: MarketMsgType; data?: any; symbols?: string[]; run_id?: string; strategy?: string; event?: any }
        if (msg.type === 'tick' && msg.data) {
          this.reconnectDelay = 2000  // reset on real data
          const tick = msg.data as MarketTick
          this.tickHandlers.forEach(h => { try { h(tick) } catch {} })
        } else if (msg.type === 'subscribed') {
          this.reconnectDelay = 1000
          this.subscribed = new Set(msg.symbols ?? [])
          this.pendingSubscribe.clear()
        } else if (msg.type === 'strategy_event') {
          this.strategyEventHandlers.forEach(h => {
            try {
              h({ run_id: msg.run_id ?? '', strategy: msg.strategy ?? '', event: msg.event ?? {} })
            } catch {}
          })
        }
      } catch { /* ignore malformed */ }
    }

    this.ws.onclose = () => {
      this._open = false
      this._stopPing()
      setTimeout(() => {
        this.reconnectDelay = Math.min(this.reconnectDelay * 1.3, this.maxDelay)
        this.connect()
      }, this.reconnectDelay)
    }

    this.ws.onerror = () => { this.ws?.close() }
  }

  subscribe(symbols: string[]) {
    if (!symbols.length) return
    symbols.forEach(s => this.pendingSubscribe.add(s.toUpperCase()))
    if (this._open) {
      this._send({ action: 'subscribe', symbols })
    } else {
      this.connect()
    }
  }

  unsubscribe(symbols: string[]) {
    if (this._open && symbols.length) {
      this._send({ action: 'unsubscribe', symbols })
    }
    symbols.forEach(s => {
      this.subscribed.delete(s.toUpperCase())
      this.pendingSubscribe.delete(s.toUpperCase())
    })
  }

  onTick(handler: TickHandler): () => void {
    this.tickHandlers.add(handler)
    return () => { this.tickHandlers.delete(handler) }
  }

  onStrategyEvent(handler: StrategyEventHandler): () => void {
    this.strategyEventHandlers.add(handler)
    return () => { this.strategyEventHandlers.delete(handler) }
  }

  private _send(payload: unknown) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(payload))
    }
  }

  private _startPing() {
    this.pingTimer = setInterval(() => this._send({ action: 'ping' }), 20_000)
  }

  private _stopPing() {
    if (this.pingTimer) { clearInterval(this.pingTimer); this.pingTimer = null }
  }
}

export const marketWs = new MarketWebSocket()
