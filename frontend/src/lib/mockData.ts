/* ═══════════════════════════════════════════════
   SMART TRADER — Demo mock data
   Used when backend is unavailable / in dev mode
   ═══════════════════════════════════════════════ */
import type {
  Position, Holding, Order, Trade,
  IndexQuote, OptionChainData, ScreenerRow,
  RiskMetrics, BrokerAccount, DashboardData,
} from '../types'

export const DEMO_ACCOUNTS: BrokerAccount[] = [
  {
    id: 'shoonya-demo', userId: 'u-demo', broker: 'shoonya', clientId: 'DEMO_USER',
    name: 'Shoonya (Demo Mode)', status: 'connected', lastSync: new Date().toISOString(),
    availableMargin: 125840.50, usedMargin: 48200, totalBalance: 174040.50,
  },
]

export const DEMO_POSITIONS: Position[] = [
  { id:'p1', accountId:'acc-1', symbol:'NIFTY26APR24500CE', tradingsymbol:'NIFTY26APR24500CE',
    exchange:'NFO', product:'MIS', quantity:50, avgPrice:142.50, ltp:168.30,
    pnl:1290, pnlPct:18.1, dayPnl:1290, value:8415, multiplier:1, side:'BUY', type:'CE' },
  { id:'p2', accountId:'acc-1', symbol:'NIFTY26APR24400PE', tradingsymbol:'NIFTY26APR24400PE',
    exchange:'NFO', product:'MIS', quantity:50, avgPrice:98.75, ltp:72.40,
    pnl:-1317.5, pnlPct:-26.7, dayPnl:-1317.5, value:3620, multiplier:1, side:'BUY', type:'PE' },
  { id:'p3', accountId:'acc-1', symbol:'RELIANCE', tradingsymbol:'RELIANCE',
    exchange:'NSE', product:'NRML', quantity:20, avgPrice:2842.10, ltp:2918.55,
    pnl:1528.9, pnlPct:2.69, dayPnl:480, value:58371, multiplier:1, side:'BUY', type:'EQ' },
  { id:'p4', accountId:'acc-1', symbol:'BANKNIFTY26APR54000CE', tradingsymbol:'BANKNIFTY26APR54000CE',
    exchange:'NFO', product:'MIS', quantity:-25, avgPrice:385.50, ltp:312.40,
    pnl:1827.5, pnlPct:18.96, dayPnl:1827.5, value:-7810, multiplier:1, side:'SELL', type:'CE' },
  { id:'p5', accountId:'acc-1', symbol:'INFY', tradingsymbol:'INFY',
    exchange:'NSE', product:'CNC', quantity:15, avgPrice:1520.00, ltp:1498.30,
    pnl:-325.5, pnlPct:-1.43, dayPnl:-120, value:22474.5, multiplier:1, side:'BUY', type:'EQ' },
]

export const DEMO_HOLDINGS: Holding[] = [
  { id:'h1', accountId:'acc-1', symbol:'RELIANCE', exchange:'NSE', isin:'INE002A01018',
    quantity:100, avgCost:2540, ltp:2918.55, currentValue:291855, investedValue:254000,
    pnl:37855, pnlPct:14.9, dayChange:1820, dayChangePct:0.63 },
  { id:'h2', accountId:'acc-1', symbol:'TCS', exchange:'NSE', isin:'INE467B01029',
    quantity:50, avgCost:3820, ltp:4102.40, currentValue:205120, investedValue:191000,
    pnl:14120, pnlPct:7.39, dayChange:-920, dayChangePct:-0.22 },
  { id:'h3', accountId:'acc-1', symbol:'HDFC', exchange:'NSE', isin:'INE040A01034',
    quantity:75, avgCost:1680, ltp:1724.85, currentValue:129363.75, investedValue:126000,
    pnl:3363.75, pnlPct:2.67, dayChange:2280, dayChangePct:1.79 },
  { id:'h4', accountId:'acc-1', symbol:'WIPRO', exchange:'NSE', isin:'INE075A01022',
    quantity:200, avgCost:412, ltp:389.20, currentValue:77840, investedValue:82400,
    pnl:-4560, pnlPct:-5.53, dayChange:-480, dayChangePct:-0.61 },
  { id:'h5', accountId:'acc-1', symbol:'BAJFINANCE', exchange:'NSE', isin:'INE296A01024',
    quantity:10, avgCost:6820, ltp:7284.50, currentValue:72845, investedValue:68200,
    pnl:4645, pnlPct:6.81, dayChange:1240, dayChangePct:1.73 },
]

export const DEMO_ORDERS: Order[] = [
  { id:'o1', accountId:'acc-1', orderId:'BD0001', symbol:'NIFTY26APR24500CE', tradingsymbol:'NIFTY26APR24500CE',
    exchange:'NFO', type:'CE', transactionType:'BUY', orderType:'LIMIT', product:'MIS',
    quantity:50, filledQty:50, price:142.50, avgPrice:142.50, status:'COMPLETE',
    validity:'DAY', placedAt:new Date(Date.now()-3600000).toISOString(), updatedAt:new Date(Date.now()-3500000).toISOString() },
  { id:'o2', accountId:'acc-1', orderId:'BD0002', symbol:'RELIANCE', tradingsymbol:'RELIANCE',
    exchange:'NSE', type:'EQ', transactionType:'BUY', orderType:'LIMIT', product:'CNC',
    quantity:10, filledQty:0, price:2900, status:'OPEN',
    validity:'DAY', placedAt:new Date(Date.now()-900000).toISOString(), updatedAt:new Date(Date.now()-890000).toISOString() },
  { id:'o3', accountId:'acc-1', orderId:'BD0003', symbol:'BANKNIFTY26APR54000CE', tradingsymbol:'BANKNIFTY26APR54000CE',
    exchange:'NFO', type:'CE', transactionType:'SELL', orderType:'MARKET', product:'MIS',
    quantity:25, filledQty:25, price:0, avgPrice:385.50, status:'COMPLETE',
    validity:'DAY', placedAt:new Date(Date.now()-1800000).toISOString(), updatedAt:new Date(Date.now()-1790000).toISOString() },
  { id:'o4', accountId:'acc-1', orderId:'BD0004', symbol:'TCS', tradingsymbol:'TCS',
    exchange:'NSE', type:'EQ', transactionType:'SELL', orderType:'SL', product:'CNC',
    quantity:25, filledQty:0, price:4050, triggerPrice:4040, status:'TRIGGER_PENDING',
    validity:'DAY', placedAt:new Date(Date.now()-600000).toISOString(), updatedAt:new Date(Date.now()-598000).toISOString() },
]

export const DEMO_TRADES: Trade[] = [
  { id:'t1', accountId:'acc-1', orderId:'BD0001', tradeId:'TRD001', symbol:'NIFTY26APR24500CE', tradingsymbol:'NIFTY26APR24500CE',
    exchange:'NFO', transactionType:'BUY', product:'MIS', quantity:50, price:142.50, value:7125, charges:18.50, tradedAt:new Date(Date.now()-3500000).toISOString() },
  { id:'t2', accountId:'acc-1', orderId:'BD0003', tradeId:'TRD002', symbol:'BANKNIFTY26APR54000CE', tradingsymbol:'BANKNIFTY26APR54000CE',
    exchange:'NFO', transactionType:'SELL', product:'MIS', quantity:25, price:385.50, value:9637.5, charges:24.20, tradedAt:new Date(Date.now()-1790000).toISOString() },
]

export const DEMO_RISK: RiskMetrics = {
  accountId: 'acc-1',
  dailyPnl: 3328.9,
  dailyPnlLimit: -10000,
  mtmPnl: 3328.9,
  maxPositionValue: 500000,
  leverageUsed: 2.8,
  maxLeverage: 5,
  positionCount: 5,
  maxPositions: 20,
  riskStatus: 'SAFE',
  alerts: [],
}

export const DEMO_DASHBOARD: DashboardData = {
  positions: DEMO_POSITIONS,
  holdings: DEMO_HOLDINGS,
  orders: DEMO_ORDERS,
  trades: DEMO_TRADES,
  riskMetrics: DEMO_RISK,
  accountSummary: {
    totalEquity: 174040.50,
    dayPnl: 3328.9,
    dayPnlPct: 1.95,
    unrealizedPnl: 3003.4,
    realizedPnl: 325.5,
    usedMargin: 48200,
    availableMargin: 125840.50,
  },
}

export const DEMO_INDICES: IndexQuote[] = [
  { token:'NSE:NIFTY50',   symbol:'NIFTY 50',    exchange:'NSE', ltp:24387.45, open:24200, high:24450, low:24150, close:24387.45, prevClose:23980, change:407.45, changePct:1.70, volume:285000000, advances:38, declines:12, unchanged:0, updatedAt: Date.now() },
  { token:'NSE:BANKNIFTY', symbol:'BANK NIFTY',  exchange:'NSE', ltp:53842.10, open:53400, high:54100, low:53300, close:53842.10, prevClose:52980, change:862.10, changePct:1.63, volume:142000000, advances:9, declines:3, unchanged:0, updatedAt: Date.now() },
  { token:'NSE:MIDCAP',    symbol:'NIFTY MIDCAP',exchange:'NSE', ltp:53124.80, open:52800, high:53400, low:52600, close:53124.80, prevClose:52480, change:644.80, changePct:1.23, volume:48000000, advances:72, declines:28, unchanged:0, updatedAt: Date.now() },
  { token:'NSE:SMALLCAP',  symbol:'NIFTY SMALLCAP',exchange:'NSE',ltp:18420.30,open:18200,high:18550,low:18100,close:18420.30,prevClose:18200,change:220.30,changePct:1.21,volume:32000000, advances:156, declines:94, unchanged:0, updatedAt: Date.now() },
  { token:'NSE:IT',        symbol:'NIFTY IT',    exchange:'NSE', ltp:38420.50, open:38100, high:38700, low:38000, close:38420.50, prevClose:38650, change:-229.50, changePct:-0.59, volume:18000000, advances:4, declines:6, unchanged:0, updatedAt: Date.now() },
  { token:'NSE:PHARMA',    symbol:'NIFTY PHARMA',exchange:'NSE', ltp:22184.60, open:22000, high:22350, low:21900, close:22184.60, prevClose:21950, change:234.60, changePct:1.07, volume:12000000, advances:14, declines:6, unchanged:0, updatedAt: Date.now() },
  { token:'NSE:AUTO',      symbol:'NIFTY AUTO',  exchange:'NSE', ltp:24185.40, open:24100, high:24300, low:24000, close:24185.40, prevClose:24089, change:96.40, changePct:0.40, volume:8000000, advances:10, declines:5, unchanged:0, updatedAt: Date.now() },
  { token:'BSE:SENSEX',    symbol:'SENSEX',      exchange:'BSE', ltp:80248.30, open:79800, high:80500, low:79600, close:80248.30, prevClose:78990, change:1258.30, changePct:1.59, volume:520000000, advances:22, declines:8, unchanged:0, updatedAt: Date.now() },
]

export const DEMO_SCREENER: ScreenerRow[] = [
  { symbol:'RELIANCE', name:'Reliance Industries', exchange:'NSE', ltp:2918.55, change:18.55, changePct:0.64, volume:12500000, marketCap:19730000, pe:26.4, high52w:3217.90, low52w:2220.30, rsi:62.4 },
  { symbol:'TCS',  name:'Tata Consultancy', exchange:'NSE', ltp:4102.40, change:-9.20, changePct:-0.22, volume:2840000, marketCap:14990000, pe:31.2, high52w:4592.25, low52w:3311.00, rsi:48.8 },
  { symbol:'HDFCBANK', name:'HDFC Bank', exchange:'NSE', ltp:1724.85, change:30.40, changePct:1.79, volume:9800000, marketCap:13140000, pe:18.9, high52w:1794.00, low52w:1363.55, rsi:68.2 },
  { symbol:'INFY', name:'Infosys', exchange:'NSE', ltp:1498.30, change:-7.20, changePct:-0.48, volume:5200000, marketCap:6240000, pe:24.8, high52w:1906.00, low52w:1358.35, rsi:44.6 },
  { symbol:'ICICIBANK', name:'ICICI Bank', exchange:'NSE', ltp:1284.65, change:14.85, changePct:1.17, volume:11400000, marketCap:9040000, pe:19.2, high52w:1338.00, low52w:987.90, rsi:71.3 },
  { symbol:'BHARTIARTL', name:'Bharti Airtel', exchange:'NSE', ltp:1842.30, change:25.10, changePct:1.38, volume:4200000, marketCap:10980000, pe:78.4, high52w:2004.45, low52w:1214.90, rsi:65.8 },
  { symbol:'KOTAKBANK', name:'Kotak Mahindra Bank', exchange:'NSE', ltp:1948.55, change:-12.45, changePct:-0.63, volume:3800000, marketCap:3870000, pe:25.6, high52w:2199.00, low52w:1680.90, rsi:39.4 },
  { symbol:'LT', name:'Larsen & Toubro', exchange:'NSE', ltp:3542.80, change:38.20, changePct:1.09, volume:2900000, marketCap:4870000, pe:38.1, high52w:3963.70, low52w:2840.00, rsi:57.2 },
]

export function generateOptionChain(
  underlying: string, ltp: number, expiry: string
): OptionChainData {
  const atm = Math.round(ltp / 50) * 50
  const rows = []
  for (let i = -10; i <= 10; i++) {
    const strike = atm + i * 50
    const moneyness = (ltp - strike) / ltp
    const callIV = 15 + Math.abs(moneyness) * 40 + Math.random() * 2
    const putIV  = 14 + Math.abs(moneyness) * 38 + Math.random() * 2
    const callLtp = Math.max(0.05, i < 0 ? (ltp - strike) + callIV * 0.4 + Math.random() * 5 : callIV * 0.4 + Math.random() * 5)
    const putLtp  = Math.max(0.05, i > 0 ? (strike - ltp) + putIV  * 0.4 + Math.random() * 5 : putIV  * 0.4 + Math.random() * 5)
    rows.push({
      strike,
      isATM: strike === atm,
      call: {
        strike, expiry, oi: Math.floor(50000 + Math.random()*500000), oiChange: Math.floor(-10000+Math.random()*20000),
        oiChangePct: (-10+Math.random()*20), volume: Math.floor(1000+Math.random()*100000),
        iv: callIV, ltp: callLtp, bid: callLtp - 0.5, ask: callLtp + 0.5,
        delta: 0.5 - moneyness * 0.8, gamma: 0.002, theta: -5, vega: 20, rho: 0.1,
      },
      put: {
        strike, expiry, oi: Math.floor(50000 + Math.random()*500000), oiChange: Math.floor(-10000+Math.random()*20000),
        oiChangePct: (-10+Math.random()*20), volume: Math.floor(1000+Math.random()*100000),
        iv: putIV, ltp: putLtp, bid: putLtp - 0.5, ask: putLtp + 0.5,
        delta: -0.5 + moneyness * 0.8, gamma: 0.002, theta: -4.8, vega: 19, rho: -0.1,
      },
    })
  }
  return {
    underlying, underlyingLtp: ltp, expiry,
    expiries: ['10Apr2026', '17Apr2026', '24Apr2026', '01May2026', '29May2026', '26Jun2026'],
    pcr: 0.85 + Math.random() * 0.3,
    maxPainStrike: atm - 50,
    rows,
  }
}
