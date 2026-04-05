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

interface WsMessage {
  type: WsEventType
  data: unknown
  ts?: number
}

type Subscriber<T = unknown> = (data: T) => void

class SmartTraderWS {
  private ws: WebSocket | null = null
  private url: string = ''
  private reconnectDelay = 2000
  private maxDelay = 30_000
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
      this.reconnectDelay = 2000
      this._startPing()
      // restore broker subscription on reconnect
      if (this._subscribedBroker) {
        this._send({ action: 'subscribe_broker', config_id: this._subscribedBroker })
      }
    }

    this.ws.onmessage = (ev) => {
      try {
        const msg: WsMessage = JSON.parse(ev.data)
        const handlers = this.subs.get(msg.type)
        if (handlers) handlers.forEach(h => h(msg.data))
      } catch { /* ignore malformed */ }
    }

    this.ws.onclose = () => {
      this._open = false
      this._stopPing()
      setTimeout(() => {
        this.reconnectDelay = Math.min(this.reconnectDelay * 1.5, this.maxDelay)
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
    this.pingTimer = setInterval(() => this._send({ action: 'ping' }), 20_000)
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
