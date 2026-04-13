/* ═══════════════════════════════════════════════
   SMART TRADER — API client
   All requests go through /api — proxied to backend
   ═══════════════════════════════════════════════ */

const BASE = '/api'

class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message)
    this.name = 'ApiError'
  }
}

async function request<T>(
  path: string,
  options: RequestInit = {}
): Promise<T> {
  const token = localStorage.getItem('st_token')
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(options.headers as Record<string, string>),
  }
  if (token) headers['Authorization'] = `Bearer ${token}`

  const res = await fetch(`${BASE}${path}`, {
    ...options,
    headers,
    credentials: 'include',
  })

  if (!res.ok) {
    // Global 401 handler: clear token & redirect to login (skip for login endpoint itself)
    if (res.status === 401 && path !== '/auth/login' && token) {
      localStorage.removeItem('st_token')
      window.location.href = '/login'
      throw new ApiError(401, 'Session expired — please log in again')
    }
    const text = await res.text().catch(() => res.statusText)
    throw new ApiError(res.status, text)
  }

  if (res.status === 204) return undefined as T
  return res.json()
}

export const api = {
  get:    <T>(path: string)                       => request<T>(path),
  getNoStore: <T>(path: string)                  => request<T>(path, { cache: 'no-store' }),
  post:   <T>(path: string, body: unknown)        => request<T>(path, { method: 'POST',  body: JSON.stringify(body) }),
  put:    <T>(path: string, body: unknown)        => request<T>(path, { method: 'PUT',   body: JSON.stringify(body) }),
  patch:  <T>(path: string, body: unknown)        => request<T>(path, { method: 'PATCH', body: JSON.stringify(body) }),
  delete: <T>(path: string)                       => request<T>(path, { method: 'DELETE' }),

  // ── Auth ──
  login:  (email: string, password: string) =>
    api.post<{ access_token: string; token_type: string; user: { id: string; email: string; name: string; role: string; phone?: string } }>('/auth/login', { email, password }),

  register: (email: string, name: string, password: string, phone?: string) =>
    api.post<{ access_token: string; token_type: string; user: { id: string; email: string; name: string; role: string } }>('/auth/register', { email, name, password, phone }),

  logout: () => api.post('/auth/logout', {}),

  me: () => api.get<{ id: string; email: string; name: string; role: string; phone?: string; created_at: string }>('/auth/me'),

  changePassword: (current_password: string, new_password: string) =>
    api.post('/auth/change-password', { current_password, new_password }),

  apiKey: () => api.get<{ api_key: string; user_id: string }>('/auth/api-key'),
  regenerateApiKey: () => api.post<{ api_key: string; message: string }>('/auth/api-key/regenerate', {}),

  shoonyaConnect: () => api.post('/auth/shoonya-connect', {}),
  shoonyaDisconnect: () => api.post('/auth/shoonya-disconnect', {}),
  shoonyaStatus: () => api.get<{ loggedIn: boolean; mode: string; userId?: string; configured: boolean }>('/auth/status'),
  brokerStatus: () => api.get<{ isLive: boolean; broker: string | null; mode: string; clientId: string | null; loginAt: string | null }>('/auth/broker-status'),
  accountInfo: () => api.get<{ isLive: boolean; clientId: string | null; brokerName: string | null; loginAt: string | null; limits: Record<string, number> }>('/orders/account-info'),

  // ── Admin ──
  adminStats: () => api.get<{ users: number; admins: number; active_sessions: number; audit_entries: number; broker_configs: number }>('/admin/stats'),
  adminUsers: () => api.get<any[]>('/admin/users'),
  adminUser: (id: string) => api.get<any>(`/admin/users/${id}`),
  adminCreateUser: (data: { email: string; name: string; password: string; role?: string; phone?: string }) => api.post<any>('/admin/users', data),
  adminUpdateUser: (id: string, data: Partial<{ name: string; email: string; role: string; is_active: boolean; phone: string }>) => api.put<any>(`/admin/users/${id}`, data),
  adminDeleteUser: (id: string) => api.delete(`/admin/users/${id}`),
  adminResetPassword: (id: string, new_password: string) => api.post(`/admin/users/${id}/reset-password`, { new_password }),
  adminAuditLog: (limit = 100) => api.get<any[]>(`/admin/audit-log?limit=${limit}`),
  adminBrokerSessions: () => api.get<any[]>('/admin/broker-sessions'),

  // ── Broker configs ──
  brokerSupported: () => api.get<{ id: string; name: string; description: string }[]>('/broker/supported'),
  brokerFields: (brokerId: string) => api.get<{ id: string; label: string; placeholder?: string; required: boolean; sensitive: boolean }[]>(`/broker/fields/${brokerId}`),
  brokerConfigs: () => api.get<any[]>('/broker/configs'),
  addBrokerConfig: (broker_id: string, nickname: string, credentials: Record<string, string>) =>
    api.post<any>('/broker/configs', { broker_id, nickname, credentials }),
  updateBrokerConfig: (id: string, data: { nickname?: string; credentials?: Record<string, string> }) =>
    api.put<any>(`/broker/configs/${id}`, data),
  deleteBrokerConfig: (id: string) => api.delete(`/broker/configs/${id}`),
  connectBroker: (id: string) => api.post<any>(`/broker/configs/${id}/connect`, {}),
  disconnectBroker: (id: string) => api.post<any>(`/broker/configs/${id}/disconnect`, {}),
  brokerEnvPreview: (id: string) => api.get<{ env_content: string; warning: string }>(`/broker/configs/${id}/env-preview`),
  brokerEditCreds: (id: string) => api.get<{ fields: Record<string, string>; broker_id: string; nickname: string }>(`/broker/configs/${id}/edit-creds`),

  // ── OMS ──
  omsOrders: () => api.get<any[]>('/oms/orders'),
  omsPositions: () => api.get<any[]>('/oms/positions'),
  omsPnl: () => api.get<{ total_pnl: number; realised_pnl: number; unrealised_pnl: number; positions: number }>('/oms/pnl'),
  omsSquareOff: () => api.post('/oms/square-off', {}),

  // ── Live broker data ──
  liveDashboard: () => api.get<any>('/orders/dashboard'),
  livePositions: () => api.get<{ data: any[]; count: number }>('/orders/positions'),
  liveOrders:    () => api.get<{ data: any[]; count: number }>('/orders/book'),
  accountSummary: () => api.get<{ accounts: any[]; count: number }>('/orders/account-summary'),
  brokerAccounts: () => api.get<{ accounts: any[]; count: number }>('/orders/broker-accounts'),
  brokerData: (configId: string) => api.get<any>(`/orders/broker-data?config_id=${configId}`),

  // ── Broker diagnostics ──
  allSessions: () => api.get<any[]>('/broker/all-sessions'),
  brokerDiagnose: (configId: string, call: string) =>
    api.post<any>(`/broker/configs/${configId}/diagnose?call=${call}`, {}),

  // ── Risk ──
  riskStatus: () => api.get<any>('/risk/status'),
  riskResetDay: () => api.post('/risk/reset-day', {}),

  // ── Alerts ──
  alertHistory: () => api.get<any[]>('/alerts/history'),

  // ── Accounts ──
  accounts:       () => api.get('/accounts'),
  account:        (id: string) => api.get(`/accounts/${id}`),
  connectAccount: (data: unknown) => api.post('/accounts', data),
  deleteAccount:  (id: string) => api.delete(`/accounts/${id}`),

  // ── Dashboard ──
  dashboard: (accountId: string) => api.get(`/dashboard/${accountId}`),

  // ── Orders ──
  orders:           (accountId: string) => api.get(`/orders?account=${accountId}`),
  placeOrder:       (data: unknown)     => api.post('/orders/place', data),
  cancelOrder:      (id: string, accountId: string) => api.delete(`/orders/${encodeURIComponent(id)}?account_id=${encodeURIComponent(accountId)}`),
  cancelAllOrders:  (accountId: string) => api.delete(`/orders/cancel-all/${accountId}`),
  modifyOrder:      (id: string, data: unknown) => api.patch(`/orders/${id}`, data),

  // ── Positions ──
  positions:       (accountId: string) => api.get(`/positions?account=${accountId}`),
  squareOff:       (data: { symbol: string; exchange: string; product: string; quantity: number; side: string; accountId: string }) =>
    api.post('/orders/squareoff', data),
  squareOffAll:    (accountId: string)  => api.post('/orders/squareoff-all', { accountId }),
  setSLSettings:   (data: unknown)      => api.put('/orders/positions/sl-settings', data),
  getSLSettings:   ()                   => api.get('/orders/positions/sl-settings'),

  // ── Holdings ──
  holdings: (accountId: string) => api.get(`/orders/holdings?account_id=${accountId}`),

  // ── Trades ──
  trades: (accountId: string, date?: string) =>
    api.get(`/orders/tradebook?account_id=${accountId}${date ? `&date=${date}` : ''}`),

  // ── Market data ──
  quote:      (tokens: string[]) => api.post('/market/quote', { tokens }),
  search:     (q: string)        => api.get(`/market/search?q=${encodeURIComponent(q)}`),
  indices:        ()                 => api.get('/market/indices'),
  globalMarkets:  ()                 => api.get('/market/global'),
  fyersStatus:    ()                 => api.get('/market/fyers-status'),
  fyersReload:    ()                 => api.post('/market/fyers-reload', {}),
  optionChain:(symbol: string, expiry?: string, exchange?: string) => {
    const params = new URLSearchParams()
    if (expiry) params.set('expiry', expiry)
    if (exchange) params.set('exchange', exchange)
    const qs = params.toString()
    return api.get(`/market/option-chain/${symbol}${qs ? `?${qs}` : ''}`)
  },
  history:    (token: string, interval: string, from: string, to: string) =>
    api.get(`/market/history?token=${token}&interval=${interval}&from=${from}&to=${to}`),
  marketOhlcv: (symbol: string, timeframe = '1m', exchange = 'NSE', limit = 500) =>
    api.get<{ symbol: string; exchange: string; timeframe: string; candles: any[] }>(
      `/market/ohlcv/${encodeURIComponent(symbol)}?timeframe=${timeframe}&exchange=${exchange}&limit=${limit}`
    ),
  subscribeSymbols: (symbols: string[]) => api.post('/market/subscribe', { symbols }),
  latestTick: (symbol: string, exchange = 'NSE') =>
    api.get<{ symbol: string; tick: any; source: string }>(`/market/tick/${encodeURIComponent(symbol)}?exchange=${exchange}`),
  marketDepth: (symbol: string, exchange = 'NSE') =>
    api.get<{ symbol: string; exchange: string; bids: any[]; asks: any[]; ltp: number; volume: number; oi: number; total_buy_qty: number; total_sell_qty: number }>(`/market/depth/${encodeURIComponent(symbol)}?exchange=${exchange}`),
  screener:   (params: Record<string, string>) => {
    const qs = new URLSearchParams(params).toString()
    return api.get(`/market/screener?${qs}`)
  },

  // ── Risk ──
  riskMetrics: (accountId: string) => api.get(`/risk/${accountId}`),
  riskConfig:  (accountId: string) => api.get(`/risk/${accountId}/config`),
  updateRisk:  (accountId: string, data: unknown) => api.put(`/risk/${accountId}/config`, data),

  // ── Copy trade ──
  copyLinks:     ()                => api.get('/copy-trade/links'),
  createCopyLink:(data: unknown)   => api.post('/copy-trade/links', data),
  updateCopyLink:(id: string, d: unknown) => api.patch(`/copy-trade/links/${id}`, d),
  deleteCopyLink:(id: string)      => api.delete(`/copy-trade/links/${id}`),

  // ── Webhooks ──
  webhooks:      ()               => api.get('/webhooks'),
  createWebhook: (d: unknown)     => api.post('/webhooks', d),
  deleteWebhook: (id: string)     => api.delete(`/webhooks/${id}`),

  // ── Settings ──
  settings:      () => api.get('/settings'),
  updateSettings:(d: unknown) => api.put('/settings', d),

  // ── System ──
  health:       () => api.get('/health'),
  diagnostics:  () => api.get('/diagnostics'),

  // ── Strategy Builder — config CRUD ──────────────────────────────────────
  strategyConfigs: () =>
    api.get<any[]>('/dashboard/strategy/configs'),
  strategyConfig: (name: string) =>
    api.get<any>(`/dashboard/strategy/config/${encodeURIComponent(name)}`),
  saveStrategyConfig: (config: any) =>
    api.post<{ ok: boolean; name: string; file: string }>(
      '/dashboard/strategy/config/save-all', config),
  deleteStrategyConfig: (name: string) =>
    api.delete<{ ok: boolean; deleted: string }>(
      `/dashboard/strategy/config/${encodeURIComponent(name)}`),
  activeSymbols: () =>
    api.get<{ symbol: string; exchange: string }[]>('/dashboard/option-chain/active-symbols'),

  // ── Strategy Runner — run / stop / status ────────────────────────────────
  runStrategy:  (name: string, overrides?: { symbol?: string; exchange?: string; paper_mode?: boolean; broker_config_id?: string }) =>
    api.post<{ ok: boolean; name: string; status: string }>(`/strategy/run/${encodeURIComponent(name)}`, overrides || {}),
  stopStrategy: (name: string) =>
    api.post<{ ok: boolean; name: string; status: string }>(`/strategy/stop/${encodeURIComponent(name)}`, {}),
  strategyStatus: () =>
    api.getNoStore<any[]>('/strategy/status'),

  // ── Strategy Live Monitor ────────────────────────────────────────────────
  strategyMonitor: (name: string) =>
    api.getNoStore<any>(`/strategy/monitor/${encodeURIComponent(name)}`),
  strategyPositions: () =>
    api.getNoStore<any[]>('/strategy/monitor'),

  // ── Broker & Symbol selection ────────────────────────────────────────────
  strategyBrokers: () =>
    api.get<any[]>('/strategy/brokers'),
  availableSymbols: () =>
    api.get<any[]>('/strategy/available-symbols'),
  chainStatus: (symbol: string, exchange: string) =>
    api.getNoStore<{ symbol: string; exchange: string; available: boolean; spot: number; age_seconds: number | null; file: string | null }>(`/strategy/chain-status?symbol=${encodeURIComponent(symbol)}&exchange=${encodeURIComponent(exchange)}`),
  fetchChain: (symbol: string, exchange: string) =>
    api.post<{ ok: boolean; file?: string; detail?: string }>('/strategy/fetch-chain', { symbol, exchange }),

  // ── Strategy Run History ─────────────────────────────────────────────────
  strategyRuns: (params?: { strategy_name?: string; status?: string; limit?: number }) => {
    const qs = new URLSearchParams()
    if (params?.strategy_name) qs.set('strategy_name', params.strategy_name)
    if (params?.status) qs.set('status', params.status)
    if (params?.limit) qs.set('limit', String(params.limit))
    const q = qs.toString()
    return api.get<any[]>(`/strategy/runs${q ? '?' + q : ''}`)
  },
  strategyRunDetail: (runId: string) =>
    api.get<any>(`/strategy/runs/${encodeURIComponent(runId)}`),
  strategyRunEvents: (runId: string) =>
    api.get<any[]>(`/strategy/runs/${encodeURIComponent(runId)}/events`),
  strategyRunPnl: (runId: string) =>
    api.get<any[]>(`/strategy/runs/${encodeURIComponent(runId)}/pnl`),
}
