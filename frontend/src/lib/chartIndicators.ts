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

// ─── Smoothed Heikin-Ashi + Bull/Bear Power ────
export interface HACandle { time: number; open: number; high: number; low: number; close: number; color: string }
export function computeHASmooth(data: Candle[], smoothPasses = 2) {
  if (!data.length) return { candles: [] as HACandle[], bullPower: [] as ColorPoint[], bearPower: [] as ColorPoint[] }

  // Standard Heikin Ashi
  let ha: { o: number; h: number; l: number; c: number }[] = []
  for (let i = 0; i < data.length; i++) {
    const d = data[i]
    if (i === 0) {
      ha.push({ o: (d.open + d.close) / 2, h: d.high, l: d.low, c: (d.open + d.high + d.low + d.close) / 4 })
    } else {
      const c = (d.open + d.high + d.low + d.close) / 4
      const o = (ha[i - 1].o + ha[i - 1].c) / 2
      ha.push({ o, h: Math.max(d.high, o, c), l: Math.min(d.low, o, c), c })
    }
  }

  // EMA-smooth multiple passes
  const smPeriod = 6
  for (let pass = 0; pass < smoothPasses; pass++) {
    const k = 2 / (smPeriod + 1)
    const sm: typeof ha = []
    for (let i = 0; i < ha.length; i++) {
      if (i === 0) { sm.push(ha[i]); continue }
      sm.push({
        o: ha[i].o * k + sm[i - 1].o * (1 - k),
        h: ha[i].h * k + sm[i - 1].h * (1 - k),
        l: ha[i].l * k + sm[i - 1].l * (1 - k),
        c: ha[i].c * k + sm[i - 1].c * (1 - k),
      })
    }
    ha = sm
  }

  const candles: HACandle[] = ha.map((h, i) => ({
    time: data[i].time,
    open: h.o,
    high: Math.max(h.o, h.c, h.h),
    low: Math.min(h.o, h.c, h.l),
    close: h.c,
    color: h.c >= h.o ? '#22c55e' : '#f43f5e',
  }))

  // Bull / Bear power  (HA close vs its EMA-13)
  const closes = ha.map(h => h.c)
  const emaK = 2 / 14
  let ema = closes[0]
  const bullPower: ColorPoint[] = []
  const bearPower: ColorPoint[] = []
  for (let i = 0; i < closes.length; i++) {
    ema = i === 0 ? closes[0] : closes[i] * emaK + ema * (1 - emaK)
    const pwr = closes[i] - ema
    bullPower.push({ time: data[i].time, value: Math.max(pwr, 0), color: '#22c55e' })
    bearPower.push({ time: data[i].time, value: Math.min(pwr, 0), color: '#f43f5e' })
  }

  return { candles, bullPower, bearPower }
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
