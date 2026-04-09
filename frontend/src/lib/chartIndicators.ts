/* ═══════════════════════════════════════════════
   Chart Indicator Computations
   All indicators computed from OHLCV candle data
   ═══════════════════════════════════════════════ */

export interface Candle {
  time: number
  open: number
  high: number
  low: number
  close: number
  volume: number
}

export interface Point { time: number; value: number }
export interface ColorPoint { time: number; value: number; color: string }

// ─── Helper: EMA on raw number array ───────────
function emaValues(src: number[], period: number): number[] {
  if (src.length === 0) return []
  const k = 2 / (period + 1)
  const out: number[] = new Array(src.length).fill(NaN)
  let sum = 0, cnt = 0
  for (let i = 0; i < Math.min(period, src.length); i++) { sum += src[i]; cnt++ }
  out[Math.min(period, src.length) - 1] = sum / cnt
  for (let i = period; i < src.length; i++) {
    out[i] = src[i] * k + out[i - 1] * (1 - k)
  }
  return out
}

// ─── SMA ───────────────────────────────────────
export function computeSMA(data: Candle[], period: number): Point[] {
  const result: Point[] = []
  for (let i = period - 1; i < data.length; i++) {
    let sum = 0
    for (let j = i - period + 1; j <= i; j++) sum += data[j].close
    result.push({ time: data[i].time, value: sum / period })
  }
  return result
}

// ─── EMA ───────────────────────────────────────
export function computeEMA(data: Candle[], period: number): Point[] {
  const vals = emaValues(data.map(c => c.close), period)
  return data
    .map((c, i) => ({ time: c.time, value: vals[i] }))
    .filter(p => !isNaN(p.value))
}

// ─── Bollinger Bands ───────────────────────────
export function computeBB(data: Candle[], period = 20, mult = 2) {
  const upper: Point[] = [], middle: Point[] = [], lower: Point[] = []
  for (let i = period - 1; i < data.length; i++) {
    let sum = 0
    for (let j = i - period + 1; j <= i; j++) sum += data[j].close
    const avg = sum / period
    let sqSum = 0
    for (let j = i - period + 1; j <= i; j++) sqSum += (data[j].close - avg) ** 2
    const std = Math.sqrt(sqSum / period)
    const t = data[i].time
    upper.push({ time: t, value: avg + mult * std })
    middle.push({ time: t, value: avg })
    lower.push({ time: t, value: avg - mult * std })
  }
  return { upper, middle, lower }
}

// ─── RSI ───────────────────────────────────────
export function computeRSI(data: Candle[], period = 14): Point[] {
  if (data.length < period + 1) return []
  const changes: number[] = []
  for (let i = 1; i < data.length; i++) changes.push(data[i].close - data[i - 1].close)

  let avgGain = 0, avgLoss = 0
  for (let i = 0; i < period; i++) {
    if (changes[i] > 0) avgGain += changes[i]; else avgLoss += Math.abs(changes[i])
  }
  avgGain /= period; avgLoss /= period

  const result: Point[] = []
  const rs0 = avgLoss === 0 ? 100 : avgGain / avgLoss
  result.push({ time: data[period].time, value: 100 - 100 / (1 + rs0) })

  for (let i = period; i < changes.length; i++) {
    const gain = changes[i] > 0 ? changes[i] : 0
    const loss = changes[i] < 0 ? Math.abs(changes[i]) : 0
    avgGain = (avgGain * (period - 1) + gain) / period
    avgLoss = (avgLoss * (period - 1) + loss) / period
    const rs = avgLoss === 0 ? 100 : avgGain / avgLoss
    result.push({ time: data[i + 1].time, value: 100 - 100 / (1 + rs) })
  }
  return result
}

// ─── MACD ──────────────────────────────────────
export function computeMACD(data: Candle[], fast = 12, slow = 26, sig = 9) {
  const closes = data.map(c => c.close)
  const fastE = emaValues(closes, fast)
  const slowE = emaValues(closes, slow)

  const macdLine: Point[] = []
  for (let i = slow - 1; i < data.length; i++) {
    if (isNaN(fastE[i]) || isNaN(slowE[i])) continue
    macdLine.push({ time: data[i].time, value: fastE[i] - slowE[i] })
  }

  const macdVals = macdLine.map(m => m.value)
  const sigE = emaValues(macdVals, sig)

  const signal: Point[] = []
  const histogram: ColorPoint[] = []
  for (let i = sig - 1; i < macdLine.length; i++) {
    if (isNaN(sigE[i])) continue
    const t = macdLine[i].time
    signal.push({ time: t, value: sigE[i] })
    const h = macdLine[i].value - sigE[i]
    histogram.push({ time: t, value: h, color: h >= 0 ? 'rgba(34,197,94,.6)' : 'rgba(244,63,94,.6)' })
  }
  return { macd: macdLine, signal, histogram }
}

// ═══════════════════════════════════════════════
//  ADVANCED INDICATORS (PineScript converted)
// ═══════════════════════════════════════════════

// ─── ZigZag — swing high/low detection ─────────
export function computeZigZag(data: Candle[], length = 8): Point[] {
  if (data.length < length * 2 + 1) return []

  const pivots: { time: number; value: number; type: 'H' | 'L' }[] = []

  for (let i = length; i < data.length - length; i++) {
    let isHi = true, isLo = true
    for (let j = 1; j <= length; j++) {
      if (data[i].high <= data[i - j].high || data[i].high <= data[i + j].high) isHi = false
      if (data[i].low >= data[i - j].low || data[i].low >= data[i + j].low) isLo = false
    }
    if (isHi) pivots.push({ time: data[i].time, value: data[i].high, type: 'H' })
    if (isLo) pivots.push({ time: data[i].time, value: data[i].low, type: 'L' })
  }

  // Remove consecutive same-type — keep extremes
  const filtered: typeof pivots = []
  for (const p of pivots) {
    if (!filtered.length) { filtered.push(p); continue }
    const last = filtered[filtered.length - 1]
    if (last.type === p.type) {
      if (p.type === 'H' && p.value > last.value) filtered[filtered.length - 1] = p
      if (p.type === 'L' && p.value < last.value) filtered[filtered.length - 1] = p
    } else {
      filtered.push(p)
    }
  }

  return filtered.map(p => ({ time: p.time, value: p.value }))
}

// ─── Universal Levels (sqrt-based price grid) ──
export interface PriceLevel { value: number; label: string }
export function computeUniversalLevels(data: Candle[], density = 12): PriceLevel[] {
  if (!data.length) return []
  const high = Math.max(...data.map(c => c.high))
  const low = Math.min(...data.map(c => c.low))
  const range = high - low
  if (range <= 0) return []

  const sqrtLo = Math.sqrt(Math.max(low - range * 0.15, 0.01))
  const sqrtHi = Math.sqrt(high + range * 0.15)
  const step = (sqrtHi - sqrtLo) / density

  const levels: PriceLevel[] = []
  for (let i = 0; i <= density; i++) {
    const sq = sqrtLo + step * i
    const price = +(sq * sq).toFixed(2)
    if (price >= low - range * 0.05 && price <= high + range * 0.05) {
      levels.push({ value: price, label: `UL ${price.toFixed(1)}` })
    }
  }
  return levels
}

// ─── Smoothed Heikin-Ashi + Bull/Bear Power (Shoonya-style) ────
// 4-level HA smoothing → Triple EMA (TMA) bull/bear with EMA length 65
// Colors: green (#00fc26) when strength > reference, red (#ff0000) when ≤
export interface HACandle { time: number; open: number; high: number; low: number; close: number; color: string }

function _emaArray(arr: number[], period: number): number[] {
  if (!arr.length) return []
  const k = 2 / (period + 1), r = [arr[0]]
  for (let i = 1; i < arr.length; i++) r.push(arr[i] * k + r[i - 1] * (1 - k))
  return r
}

function _haLevel(data: Candle[] | null, prevO: number[], prevH: number[], prevL: number[], prevC: number[]): { o: number[]; h: number[]; l: number[]; c: number[] } {
  const n = prevO.length
  const o: number[] = [], h: number[] = [], l: number[] = [], c: number[] = []
  for (let i = 0; i < n; i++) {
    const hc = (prevO[i] + prevH[i] + prevL[i] + prevC[i]) / 4
    const ho = i === 0 ? (prevO[0] + prevC[0]) / 2 : (o[i - 1] + c[i - 1]) / 2
    c.push(hc); o.push(ho)
    h.push(Math.max(prevH[i], ho, hc))
    l.push(Math.min(prevL[i], ho, hc))
  }
  return { o, h, l, c }
}

export function computeHASmooth(data: Candle[], numSmooth = 2) {
  if (data.length < 2) return { candles: [] as HACandle[], strength: [] as ColorPoint[], reference: [] as Point[] }

  // Build 4 levels of HA smoothing
  const l1 = _haLevel(null, data.map(c => c.open), data.map(c => c.high), data.map(c => c.low), data.map(c => c.close))
  const l2 = _haLevel(null, l1.o, l1.h, l1.l, l1.c)
  const l3 = _haLevel(null, l2.o, l2.h, l2.l, l2.c)
  const l4 = _haLevel(null, l3.o, l3.h, l3.l, l3.c)

  // Select HA close by smoothing level
  const haClose = numSmooth === 1 ? l1.c : numSmooth === 2 ? l2.c : numSmooth === 3 ? l3.c : l4.c

  // sr3 = (high+low+close)/3 from original data
  const sr3 = data.map(c => (c.high + c.low + c.close) / 3)

  // Triple Moving Average (TMA) with EMA length 65
  const emaLen = 65
  const ema1 = _emaArray(haClose, emaLen)
  const ema2 = _emaArray(ema1, emaLen)
  const ema3 = _emaArray(ema2, emaLen)
  const tma1 = ema1.map((v, i) => 3 * v - 3 * ema2[i] + ema3[i])
  const ema4 = _emaArray(tma1, emaLen)
  const ema5 = _emaArray(ema4, emaLen)
  const ema6 = _emaArray(ema5, emaLen)
  const tma2 = ema4.map((v, i) => 3 * v - 3 * ema5[i] + ema6[i])
  const reference = tma1.map((v, i) => v + (v - tma2[i]))

  const ema7 = _emaArray(sr3, emaLen)
  const ema8 = _emaArray(ema7, emaLen)
  const ema9 = _emaArray(ema8, emaLen)
  const tma3 = ema7.map((v, i) => 3 * v - 3 * ema8[i] + ema9[i])
  const ema10 = _emaArray(tma3, emaLen)
  const ema11 = _emaArray(ema10, emaLen)
  const ema12 = _emaArray(ema11, emaLen)
  const tma4 = ema10.map((v, i) => 3 * v - 3 * ema11[i] + ema12[i])
  const strengthArr = tma3.map((v, i) => v + (v - tma4[i]))

  // Build output with per-bar color for strength
  const strengthPts: ColorPoint[] = []
  const refPts: Point[] = []
  const candles: HACandle[] = []
  for (let i = 0; i < data.length; i++) {
    const bullish = strengthArr[i] > reference[i]
    strengthPts.push({ time: data[i].time, value: strengthArr[i], color: bullish ? '#00fc26' : '#ff0000' })
    refPts.push({ time: data[i].time, value: reference[i] })
    // Candle with Shoonya-style bull/bear color
    const selO = numSmooth === 1 ? l1.o : numSmooth === 2 ? l2.o : numSmooth === 3 ? l3.o : l4.o
    const selH = numSmooth === 1 ? l1.h : numSmooth === 2 ? l2.h : numSmooth === 3 ? l3.h : l4.h
    const selL = numSmooth === 1 ? l1.l : numSmooth === 2 ? l2.l : numSmooth === 3 ? l3.l : l4.l
    candles.push({
      time: data[i].time,
      open: selO[i], high: selH[i], low: selL[i], close: haClose[i],
      color: bullish ? '#00fc26' : '#ff0000',
    })
  }

  return { candles, strength: strengthPts, reference: refPts }
}

// ─── Support / Resistance Zones ────────────────
export interface SRBox {
  time: number
  price: number
  top: number
  bottom: number
  type: 'support' | 'resistance'
  strength: number        // 1-10  higher = more touches
}
export function computeSRBoxes(data: Candle[], pivotLen = 15, boxPct = 0.002): SRBox[] {
  if (data.length < pivotLen * 2 + 1) return []

  const raw: SRBox[] = []
  for (let i = pivotLen; i < data.length - pivotLen; i++) {
    let isHi = true, isLo = true
    for (let j = 1; j <= pivotLen; j++) {
      if (data[i].high <= data[i - j].high || data[i].high <= data[i + j].high) isHi = false
      if (data[i].low >= data[i - j].low || data[i].low >= data[i + j].low) isLo = false
    }
    if (isHi) {
      const p = data[i].high, half = p * boxPct
      let touches = 0
      for (const c of data) if (c.high >= p - half && c.high <= p + half) touches++
      raw.push({ time: data[i].time, price: p, top: p + half, bottom: p - half, type: 'resistance', strength: Math.min(touches, 10) })
    }
    if (isLo) {
      const p = data[i].low, half = p * boxPct
      let touches = 0
      for (const c of data) if (c.low >= p - half && c.low <= p + half) touches++
      raw.push({ time: data[i].time, price: p, top: p + half, bottom: p - half, type: 'support', strength: Math.min(touches, 10) })
    }
  }

  // Merge nearby same-type zones
  const merged: SRBox[] = []
  const used = new Set<number>()
  for (let i = 0; i < raw.length; i++) {
    if (used.has(i)) continue
    const box = { ...raw[i] }
    for (let j = i + 1; j < raw.length; j++) {
      if (used.has(j) || raw[j].type !== box.type) continue
      if (Math.abs(raw[j].price - box.price) / box.price < boxPct * 3) {
        box.strength = Math.max(box.strength, raw[j].strength)
        box.top = Math.max(box.top, raw[j].top)
        box.bottom = Math.min(box.bottom, raw[j].bottom)
        used.add(j)
      }
    }
    merged.push(box)
  }

  merged.sort((a, b) => b.strength - a.strength)
  return merged.slice(0, 12)
}

// ─── VWAP (Volume Weighted Average Price) ──────
export function computeVWAP(data: Candle[]): Point[] {
  if (!data.length) return []
  const result: Point[] = []
  let cumTPV = 0, cumVol = 0, lastDay = -1
  for (const c of data) {
    const d = new Date(c.time * 1000)
    const day = d.getUTCFullYear() * 10000 + (d.getUTCMonth() + 1) * 100 + d.getUTCDate()
    if (day !== lastDay) { cumTPV = 0; cumVol = 0; lastDay = day }
    const tp = (c.high + c.low + c.close) / 3
    const vol = c.volume || 1
    cumTPV += tp * vol; cumVol += vol
    result.push({ time: c.time, value: cumVol > 0 ? cumTPV / cumVol : tp })
  }
  return result
}

// ─── Supertrend ────────────────────────────────
export function computeSupertrend(data: Candle[], period = 10, mult = 3): { up: Point[]; dn: Point[] } {
  if (data.length < period + 1) return { up: [], dn: [] }
  // ATR
  const trs: number[] = [data[0].high - data[0].low]
  for (let i = 1; i < data.length; i++) {
    trs.push(Math.max(data[i].high - data[i].low, Math.abs(data[i].high - data[i - 1].close), Math.abs(data[i].low - data[i - 1].close)))
  }
  const satr: number[] = []
  let sm = 0; for (let i = 0; i < period; i++) sm += trs[i]; satr.push(sm / period)
  for (let i = period; i < data.length; i++) satr.push((satr[satr.length - 1] * (period - 1) + trs[i]) / period)
  const off = period - 1
  const uB: number[] = [], dB: number[] = [], trend: number[] = []
  for (let i = 0; i < satr.length; i++) {
    const di = i + off, hl2 = (data[di].high + data[di].low) / 2
    let ub = hl2 + mult * satr[i], lb = hl2 - mult * satr[i]
    if (i > 0) {
      ub = data[di].close > uB[i - 1] ? ub : Math.min(ub, uB[i - 1])
      lb = data[di].close < dB[i - 1] ? lb : Math.max(lb, dB[i - 1])
    }
    uB.push(ub); dB.push(lb)
    if (i === 0) { trend.push(data[di].close > ub ? -1 : 1) }
    else {
      const pt = trend[i - 1]
      if (pt === 1 && data[di].close > uB[i]) trend.push(-1)
      else if (pt === -1 && data[di].close < dB[i]) trend.push(1)
      else trend.push(pt)
    }
  }
  const up: Point[] = [], dn: Point[] = []
  for (let i = 0; i < trend.length; i++) {
    const di = i + off
    if (trend[i] === 1) dn.push({ time: data[di].time, value: uB[i] })
    else up.push({ time: data[di].time, value: dB[i] })
  }
  return { up, dn }
}

// ─── Parabolic SAR ─────────────────────────────
export function computePSAR(data: Candle[], step = 0.02, maxAf = 0.2): Point[] {
  if (data.length < 2) return []
  const result: Point[] = []
  let af = step, isUp = data[1].close > data[0].close
  let sar = isUp ? data[0].low : data[0].high
  let ep = isUp ? data[1].high : data[1].low
  result.push({ time: data[0].time, value: sar })
  for (let i = 1; i < data.length; i++) {
    sar += af * (ep - sar)
    if (isUp) {
      if (i >= 2) sar = Math.min(sar, data[i - 1].low, data[i - 2].low)
      if (data[i].low < sar) { isUp = false; sar = ep; ep = data[i].low; af = step }
      else if (data[i].high > ep) { ep = data[i].high; af = Math.min(af + step, maxAf) }
    } else {
      if (i >= 2) sar = Math.max(sar, data[i - 1].high, data[i - 2].high)
      if (data[i].high > sar) { isUp = true; sar = ep; ep = data[i].high; af = step }
      else if (data[i].low < ep) { ep = data[i].low; af = Math.min(af + step, maxAf) }
    }
    result.push({ time: data[i].time, value: sar })
  }
  return result
}

// ─── ATR (Average True Range) ──────────────────
export function computeATR(data: Candle[], period = 14): Point[] {
  if (data.length < period + 1) return []
  const trs: number[] = []
  for (let i = 1; i < data.length; i++) {
    trs.push(Math.max(data[i].high - data[i].low, Math.abs(data[i].high - data[i - 1].close), Math.abs(data[i].low - data[i - 1].close)))
  }
  let atr = trs.slice(0, period).reduce((a, b) => a + b, 0) / period
  const result: Point[] = [{ time: data[period].time, value: atr }]
  for (let i = period; i < trs.length; i++) {
    atr = (atr * (period - 1) + trs[i]) / period
    result.push({ time: data[i + 1].time, value: atr })
  }
  return result
}

// ─── Pivot Points (Classic) ────────────────────
export interface PivotSet { pivot: Point[]; s1: Point[]; r1: Point[]; s2: Point[]; r2: Point[] }
export function computePivots(data: Candle[]): PivotSet {
  const dayMap: Record<number, { high: number; low: number; close: number; candles: Candle[] }> = {}
  for (const c of data) {
    const d = new Date(c.time * 1000)
    const k = d.getUTCFullYear() * 10000 + (d.getUTCMonth() + 1) * 100 + d.getUTCDate()
    if (!dayMap[k]) dayMap[k] = { high: -Infinity, low: Infinity, close: 0, candles: [] }
    dayMap[k].high = Math.max(dayMap[k].high, c.high)
    dayMap[k].low = Math.min(dayMap[k].low, c.low)
    dayMap[k].close = c.close
    dayMap[k].candles.push(c)
  }
  const days = Object.keys(dayMap).map(Number).sort()
  const pivot: Point[] = [], s1: Point[] = [], r1: Point[] = [], s2: Point[] = [], r2: Point[] = []
  for (let i = 1; i < days.length; i++) {
    const prev = dayMap[days[i - 1]]
    const pp = (prev.high + prev.low + prev.close) / 3
    const sup1 = 2 * pp - prev.high, res1 = 2 * pp - prev.low
    const sup2 = pp - (prev.high - prev.low), res2 = pp + (prev.high - prev.low)
    for (const c of dayMap[days[i]].candles) {
      pivot.push({ time: c.time, value: pp })
      s1.push({ time: c.time, value: sup1 }); r1.push({ time: c.time, value: res1 })
      s2.push({ time: c.time, value: sup2 }); r2.push({ time: c.time, value: res2 })
    }
  }
  return { pivot, s1, r1, s2, r2 }
}

// ─── Heikin Ashi candle computation ────────────
export function computeHeikinAshi(data: Candle[]): Candle[] {
  if (!data.length) return []
  const ha: Candle[] = []
  for (let i = 0; i < data.length; i++) {
    const c = data[i]
    const haClose = (c.open + c.high + c.low + c.close) / 4
    const haOpen = i === 0 ? (c.open + c.close) / 2 : (ha[i - 1].open + ha[i - 1].close) / 2
    const haHigh = Math.max(c.high, haOpen, haClose)
    const haLow = Math.min(c.low, haOpen, haClose)
    ha.push({ time: c.time, open: haOpen, high: haHigh, low: haLow, close: haClose, volume: c.volume || 0 })
  }
  return ha
}
