/* ════════════════════════════════════════════
   Advanced Chart Modal — full screen overlay
   TradingView Lightweight Charts  +  computed indicators
   ZigZag · Universal Levels · HA Smooth · S/R Boxes
   ════════════════════════════════════════════ */
import { useEffect, useRef, useState, useCallback } from 'react'
import {
  createChart, type IChartApi, type ISeriesApi, type UTCTimestamp,
  CandlestickSeries, AreaSeries, BarSeries, LineSeries, HistogramSeries,
  LineStyle,
} from 'lightweight-charts'
import { useUIStore } from '../../stores'
import { cn } from '../../lib/utils'
import { X, BarChart2, CandlestickChart, TrendingUp, Volume2, Loader2, AlertCircle } from 'lucide-react'
import { api } from '../../lib/api'
import { marketWs, type MarketTick } from '../../lib/ws'
import type { ChartInterval } from '../../types'
import {
  computeSMA, computeEMA, computeBB, computeRSI, computeMACD,
  computeZigZag, computeUniversalLevels, computeHASmooth, computeSRBoxes,
  type Candle,
} from '../../lib/chartIndicators'

/* ── Constants ─────────────────────────────── */
const INTERVALS: ChartInterval[] = ['1m','3m','5m','15m','30m','1h','4h','D','W']
type ChartType = 'candlestick' | 'line' | 'area' | 'bar'

const CLASSIC_IND  = ['MA(9)', 'MA(21)', 'EMA(50)', 'BB(20)', 'RSI(14)', 'MACD'] as const
const ADVANCED_IND = ['ZigZag', 'Levels', 'HA Smooth', 'S/R Zones'] as const
const ALL_IND = [...CLASSIC_IND, ...ADVANCED_IND]

/* ── OHLCV API response ────────────────────── */
interface OhlcvResp { symbol: string; exchange: string; timeframe: string; candles: Candle[] }

/* indicator line colours */
const IND_COLORS: Record<string, string> = {
  'MA(9)':  '#f59e0b',          // amber
  'MA(21)': '#3b82f6',          // blue
  'EMA(50)':'#a855f7',          // purple
  'BB-U':   'rgba(147,197,253,.45)',
  'BB-M':   'rgba(147,197,253,.65)',
  'BB-L':   'rgba(147,197,253,.45)',
  'MACD':   '#22d3ee',
  'MACD-S': '#f97316',
  'RSI':    '#facc15',
  'ZigZag': '#e879f9',          // fuchsia
  'HASmooth':'#6ee7b7',         // green
  'BullPwr':'#22c55e',
  'BearPwr':'#f43f5e',
}

export default function ChartModal() {
  const { chartModalOpen, chartModalToken, closeChartModal } = useUIStore()
  const containerRef = useRef<HTMLDivElement>(null)
  const subContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const subChartRef = useRef<IChartApi | null>(null)
  const unsubTickRef = useRef<(() => void) | null>(null)

  const [interval, setInterval] = useState<ChartInterval>('15m')
  const [chartType, setChartType] = useState<ChartType>('candlestick')
  const [showVolume, setShowVolume] = useState(true)
  const [activeIndicators, setActiveIndicators] = useState<string[]>(['MA(21)', 'ZigZag', 'Levels', 'S/R Zones'])
  const [crosshairData, setCrosshairData] = useState<{o:number;h:number;l:number;c:number;v:number}|null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string|null>(null)

  const toggleInd = useCallback((ind: string) => {
    setActiveIndicators(a => a.includes(ind) ? a.filter(x => x !== ind) : [...a, ind])
  }, [])

  /* Esc closes */
  useEffect(() => {
    if (!chartModalOpen) return
    const h = (e: KeyboardEvent) => { if (e.key === 'Escape') closeChartModal() }
    window.addEventListener('keydown', h)
    return () => window.removeEventListener('keydown', h)
  }, [chartModalOpen])

  /* ── Main chart effect ───────────────────── */
  useEffect(() => {
    if (!chartModalOpen || !containerRef.current) return
    let cancelled = false

    const needsSubChart = activeIndicators.includes('RSI(14)') || activeIndicators.includes('MACD')
    const container = containerRef.current

    /* Wait one frame so flex layout resolves container height */
    const rafId = requestAnimationFrame(() => {
      if (cancelled || !container.isConnected) return

    const h = container.clientHeight || window.innerHeight - 100
    const w = container.clientWidth  || window.innerWidth

    /* Create main chart */
    const chart = createChart(container, {
      autoSize: true,
      layout:    { background: { color: '#0b0e17' }, textColor: '#7b8398', fontSize: 11 },
      grid:      { vertLines: { color: '#1c2133' }, horzLines: { color: '#1c2133' } },
      crosshair: { mode: 1, vertLine: { color: '#22d3ee', labelBackgroundColor: '#22d3ee' }, horzLine: { color: '#22d3ee', labelBackgroundColor: '#22d3ee' } },
      rightPriceScale: { borderColor: '#252b3b', scaleMargins: { top: 0.08, bottom: showVolume ? 0.18 : 0.05 } },
      timeScale:       { borderColor: '#252b3b', timeVisible: true, secondsVisible: false },
      handleScroll: { mouseWheel: true, pressedMouseMove: true },
      handleScale:  { mouseWheel: true, pinch: true },
      width:  w,
      height: h,
    })
    chartRef.current = chart

    /* Sub chart for RSI / MACD */
    let subChart: IChartApi | null = null
    if (needsSubChart && subContainerRef.current) {
      subChart = createChart(subContainerRef.current, {
        autoSize: true,
        layout:    { background: { color: '#0b0e17' }, textColor: '#7b8398', fontSize: 10 },
        grid:      { vertLines: { color: '#1c2133' }, horzLines: { color: '#1c2133' } },
        crosshair: { mode: 1, vertLine: { color: '#22d3ee', labelBackgroundColor: '#22d3ee' }, horzLine: { color: '#22d3ee', labelBackgroundColor: '#22d3ee' } },
        rightPriceScale: { borderColor: '#252b3b' },
        timeScale:       { borderColor: '#252b3b', timeVisible: true, secondsVisible: false, visible: true },
        width:  subContainerRef.current.clientWidth || w,
        height: subContainerRef.current.clientHeight || Math.floor(h * 0.25),
      })
      subChartRef.current = subChart
    }

    /* Main series */
    let mainSeries: ISeriesApi<any>
    if (chartType === 'candlestick') {
      mainSeries = chart.addSeries(CandlestickSeries, { upColor: '#22c55e', downColor: '#f43f5e', borderUpColor: '#22c55e', borderDownColor: '#f43f5e', wickUpColor: '#22c55e', wickDownColor: '#f43f5e' })
    } else if (chartType === 'area') {
      mainSeries = chart.addSeries(AreaSeries, { lineColor: '#22d3ee', topColor: 'rgba(34,211,238,.25)', bottomColor: 'rgba(34,211,238,.02)', lineWidth: 2 })
    } else if (chartType === 'bar') {
      mainSeries = chart.addSeries(BarSeries, { upColor: '#22c55e', downColor: '#f43f5e' })
    } else {
      mainSeries = chart.addSeries(LineSeries, { color: '#22d3ee', lineWidth: 2 })
    }

    /* Volume sub-series */
    const volSeries = chart.addSeries(HistogramSeries, { color: '#22d3ee', priceFormat: { type: 'volume' }, priceScaleId: 'vol' })
    chart.priceScale('vol').applyOptions({ scaleMargins: { top: 0.85, bottom: 0 } })

    /* Crosshair */
    chart.subscribeCrosshairMove(param => {
      if (param.point) {
        const d = param.seriesData.get(mainSeries)
        if (d && 'open' in d) setCrosshairData({ o: (d as any).open, h: (d as any).high, l: (d as any).low, c: (d as any).close, v: 0 })
        else if (d && 'value' in d) setCrosshairData({ o: (d as any).value, h: (d as any).value, l: (d as any).value, c: (d as any).value, v: 0 })
      } else setCrosshairData(null)
    })

    /* Sync time-scales of main ↔ sub */
    if (subChart) {
      chart.timeScale().subscribeVisibleLogicalRangeChange(range => { if (range) subChart!.timeScale().setVisibleLogicalRange(range) })
      subChart.timeScale().subscribeVisibleLogicalRangeChange(range => { if (range) chart.timeScale().setVisibleLogicalRange(range) })
    }

    /* ── Fetch OHLCV & draw ─────────────────── */
    const symbol = chartModalToken ?? 'NIFTY'
    setLoading(true)
    setError(null)

    api.get<OhlcvResp>(`/market/ohlcv/${encodeURIComponent(symbol)}?timeframe=${interval}&limit=1500`)
      .then(resp => {
        if (cancelled) return
        const candles = resp.candles
        if (!candles.length) { setError('No candle data available'); setLoading(false); return }

        /* Main series data */
        if (chartType === 'area' || chartType === 'line') {
          mainSeries.setData(candles.map(c => ({ time: c.time as UTCTimestamp, value: c.close })))
        } else {
          mainSeries.setData(candles.map(c => ({ time: c.time as UTCTimestamp, open: c.open, high: c.high, low: c.low, close: c.close })))
        }

        /* Volume */
        if (showVolume) {
          volSeries.setData(candles.map(c => ({ time: c.time as UTCTimestamp, value: c.volume ?? 0, color: c.close >= c.open ? 'rgba(34,197,94,.35)' : 'rgba(244,63,94,.35)' })))
        }

        /* ── Classic Indicators ──────────────── */
        const on = (k: string) => activeIndicators.includes(k)

        if (on('MA(9)'))  chart.addSeries(LineSeries, { color: IND_COLORS['MA(9)'],  lineWidth: 1, lastValueVisible: false, priceLineVisible: false }).setData(computeSMA(candles, 9).map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
        if (on('MA(21)')) chart.addSeries(LineSeries, { color: IND_COLORS['MA(21)'], lineWidth: 1, lastValueVisible: false, priceLineVisible: false }).setData(computeSMA(candles, 21).map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
        if (on('EMA(50)'))chart.addSeries(LineSeries, { color: IND_COLORS['EMA(50)'],lineWidth: 1, lastValueVisible: false, priceLineVisible: false }).setData(computeEMA(candles, 50).map(p => ({ time: p.time as UTCTimestamp, value: p.value })))

        if (on('BB(20)')) {
          const bb = computeBB(candles, 20)
          chart.addSeries(LineSeries, { color: IND_COLORS['BB-U'], lineWidth: 1, lineStyle: LineStyle.Dashed, lastValueVisible: false, priceLineVisible: false }).setData(bb.upper.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
          chart.addSeries(LineSeries, { color: IND_COLORS['BB-M'], lineWidth: 1, lineStyle: LineStyle.Dotted,lastValueVisible: false, priceLineVisible: false }).setData(bb.middle.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
          chart.addSeries(LineSeries, { color: IND_COLORS['BB-L'], lineWidth: 1, lineStyle: LineStyle.Dashed, lastValueVisible: false, priceLineVisible: false }).setData(bb.lower.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
        }

        if (on('RSI(14)') && subChart) {
          const rsi = computeRSI(candles, 14)
          const rsiS = subChart.addSeries(LineSeries, { color: IND_COLORS['RSI'], lineWidth: 2, priceLineVisible: false, lastValueVisible: true })
          rsiS.setData(rsi.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
          rsiS.createPriceLine({ price: 70, color: 'rgba(244,63,94,.4)', lineWidth: 1, lineStyle: LineStyle.Dashed, axisLabelVisible: true, title: '70' })
          rsiS.createPriceLine({ price: 30, color: 'rgba(34,197,94,.4)', lineWidth: 1, lineStyle: LineStyle.Dashed, axisLabelVisible: true, title: '30' })
        }

        if (on('MACD') && subChart) {
          const macd = computeMACD(candles)
          subChart.addSeries(LineSeries,    { color: IND_COLORS['MACD'],   lineWidth: 2, lastValueVisible: false, priceLineVisible: false }).setData(macd.macd.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
          subChart.addSeries(LineSeries,    { color: IND_COLORS['MACD-S'], lineWidth: 1,   lastValueVisible: false, priceLineVisible: false }).setData(macd.signal.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
          subChart.addSeries(HistogramSeries,{ lastValueVisible: false, priceLineVisible: false }).setData(macd.histogram.map(p => ({ time: p.time as UTCTimestamp, value: p.value, color: p.color })))
        }

        /* ── Advanced Indicators (PineScript) ── */

        // 1) ZigZag
        if (on('ZigZag')) {
          const zz = computeZigZag(candles, 8)
          if (zz.length > 1) {
            chart.addSeries(LineSeries, { color: IND_COLORS['ZigZag'], lineWidth: 2, lineStyle: LineStyle.Solid, lastValueVisible: false, priceLineVisible: false, pointMarkersVisible: true })
              .setData(zz.map(p => ({ time: p.time as UTCTimestamp, value: p.value })))
          }
        }

        // 2) Universal Levels (sqrt-based price grid)
        if (on('Levels')) {
          const levels = computeUniversalLevels(candles, 12)
          for (const lv of levels) {
            mainSeries.createPriceLine({
              price: lv.value,
              color: 'rgba(251,191,36,.35)',
              lineWidth: 1,
              lineStyle: LineStyle.Dotted,
              axisLabelVisible: false,
              title: '',
            })
          }
        }

        // 3) HA Smooth overlay + Bull/Bear power
        if (on('HA Smooth')) {
          const ha = computeHASmooth(candles, 2)
          // Smoothed HA close as line
          chart.addSeries(LineSeries, { color: IND_COLORS['HASmooth'], lineWidth: 2, lastValueVisible: false, priceLineVisible: false })
            .setData(ha.candles.map(c => ({ time: c.time as UTCTimestamp, value: c.close })))
          // Bull / Bear power in volume area
          if (subChart || !on('RSI(14)') && !on('MACD')) {
            const target = subChart ?? chart
            const pScale = subChart ? undefined : 'power'
            const bullS = target.addSeries(HistogramSeries, { color: '#22c55e', lastValueVisible: false, priceLineVisible: false, ...(pScale ? { priceScaleId: pScale } : {}) })
            bullS.setData(ha.bullPower.map(p => ({ time: p.time as UTCTimestamp, value: p.value, color: p.color })))
            const bearS = target.addSeries(HistogramSeries, { color: '#f43f5e', lastValueVisible: false, priceLineVisible: false, ...(pScale ? { priceScaleId: pScale } : {}) })
            bearS.setData(ha.bearPower.map(p => ({ time: p.time as UTCTimestamp, value: p.value, color: p.color })))
            if (pScale) target.priceScale(pScale).applyOptions({ scaleMargins: { top: 0.9, bottom: 0 } })
          }
        }

        // 4) S/R Zones
        if (on('S/R Zones')) {
          const boxes = computeSRBoxes(candles, 15, 0.002)
          for (const box of boxes) {
            const alpha = Math.min(0.15 + box.strength * 0.03, 0.45)
            const color = box.type === 'resistance'
              ? `rgba(244,63,94,${alpha})`
              : `rgba(34,197,94,${alpha})`
            // Top line
            mainSeries.createPriceLine({ price: box.top,    color, lineWidth: 1, lineStyle: LineStyle.Dotted, axisLabelVisible: false, title: '' })
            // Bottom line
            mainSeries.createPriceLine({ price: box.bottom, color, lineWidth: 1, lineStyle: LineStyle.Dotted, axisLabelVisible: false, title: '' })
            // Centre (thicker, labelled)
            mainSeries.createPriceLine({
              price: box.price,
              color,
              lineWidth: 2,
              lineStyle: LineStyle.Solid,
              axisLabelVisible: true,
              title: `${box.type === 'resistance' ? 'R' : 'S'} ${box.price.toFixed(1)}`,
            })
          }
        }

        chart.timeScale().fitContent()
        subChart?.timeScale().fitContent()
        setLoading(false)

        /* ── Live tick streaming — update current candle in real-time ── */
        const _normSym = (s: string) => s.toUpperCase().replace(/-INDEX|-EQ|-BE/g, '').replace(/\s/g, '')
        marketWs.connect()
        marketWs.subscribe([symbol])
        const _TF_SEC: Record<string, number> = {
          '1m': 60, '3m': 180, '5m': 300, '15m': 900, '30m': 1800,
          '1h': 3600, '4h': 14400, 'D': 86400, 'W': 604800,
        }
        const barSec = _TF_SEC[interval] ?? 60
        let curBar: { time: number; open: number; high: number; low: number; close: number } | null = null

        unsubTickRef.current = marketWs.onTick((tick: MarketTick) => {
          if (_normSym(tick.symbol) !== _normSym(symbol)) return
          const ltp = tick.ltp
          if (!ltp || !chart) return

          const nowSec = Math.floor(Date.now() / 1000)
          const barTime = Math.floor(nowSec / barSec) * barSec

          if (!curBar || curBar.time !== barTime) {
            curBar = { time: barTime, open: ltp, high: ltp, low: ltp, close: ltp }
          } else {
            curBar.high = Math.max(curBar.high, ltp)
            curBar.low = Math.min(curBar.low, ltp)
            curBar.close = ltp
          }

          try {
            if (chartType === 'area' || chartType === 'line') {
              mainSeries.update({ time: curBar.time as UTCTimestamp, value: curBar.close })
            } else {
              mainSeries.update({
                time: curBar.time as UTCTimestamp,
                open: curBar.open, high: curBar.high, low: curBar.low, close: curBar.close,
              })
            }
            if (showVolume) {
              volSeries.update({
                time: curBar.time as UTCTimestamp,
                value: tick.volume ?? 0,
                color: curBar.close >= curBar.open ? 'rgba(34,197,94,.35)' : 'rgba(244,63,94,.35)',
              })
            }
          } catch { /* lightweight-charts may throw if bar already exists in wrong order */ }
        })
      })
      .catch(err => {
        if (!cancelled) { setError(err?.message ?? 'Failed to fetch chart data'); setLoading(false) }
      })

    }) // end requestAnimationFrame

    return () => {
      cancelled = true
      cancelAnimationFrame(rafId)
      if (unsubTickRef.current) { unsubTickRef.current(); unsubTickRef.current = null }
      chartRef.current?.remove()
      subChartRef.current?.remove()
      chartRef.current = null
      subChartRef.current = null
    }
  }, [chartModalOpen, interval, chartType, showVolume, activeIndicators, chartModalToken])

  if (!chartModalOpen) return null
  const symbol = chartModalToken ?? 'NIFTY'
  const needsSub = activeIndicators.includes('RSI(14)') || activeIndicators.includes('MACD')

  return (
    <div className="fixed inset-0 z-[110] flex flex-col bg-bg-base">
      {/* ── Toolbar ─────────────────────────── */}
      <div className="flex items-center gap-3 px-4 py-2.5 bg-bg-surface border-b border-border shrink-0 flex-wrap gap-y-2">
        <span className="text-[14px] font-bold text-text-bright">{symbol}</span>

        {/* Intervals */}
        <div className="flex items-center bg-bg-elevated border border-border rounded overflow-hidden">
          {INTERVALS.map(ivl => (
            <button key={ivl} onClick={() => setInterval(ivl)}
              className={cn('px-2 py-1 text-[10px] font-medium transition-colors', interval === ivl ? 'bg-brand text-bg-base' : 'text-text-muted hover:text-text-sec')}>
              {ivl}
            </button>
          ))}
        </div>

        {/* Chart type */}
        <div className="flex items-center gap-1">
          {([['candlestick', CandlestickChart], ['line', TrendingUp], ['bar', BarChart2]] as [ChartType, any][]).map(([t, Icon]) => (
            <button key={t} onClick={() => setChartType(t)}
              className={cn('btn-ghost btn-xs', chartType === t && 'text-brand bg-brand/10')} title={t}>
              <Icon className="w-3.5 h-3.5" />
            </button>
          ))}
        </div>

        {/* Volume */}
        <button onClick={() => setShowVolume(v => !v)} className={cn('btn-ghost btn-xs gap-1', showVolume && 'text-brand')}>
          <Volume2 className="w-3.5 h-3.5" /> Vol
        </button>

        {/* Classic indicators */}
        <div className="flex items-center gap-1 flex-wrap">
          {ALL_IND.map(ind => (
            <button key={ind} onClick={() => toggleInd(ind)}
              className={cn(
                'px-2 py-0.5 rounded text-[10px] font-medium transition-colors border',
                activeIndicators.includes(ind)
                  ? ADVANCED_IND.includes(ind as any) ? 'bg-fuchsia-500/20 text-fuchsia-400 border-fuchsia-500/40' : 'bg-accent/20 text-accent border-accent/40'
                  : 'text-text-muted border-transparent hover:border-border'
              )}>
              {ind}
            </button>
          ))}
        </div>

        <div className="flex-1" />

        {/* OHLC crosshair */}
        {crosshairData && (
          <div className="flex items-center gap-3 text-[10px] font-mono">
            <span>O <span className="text-text-bright">{crosshairData.o.toFixed(2)}</span></span>
            <span>H <span className="text-profit">{crosshairData.h.toFixed(2)}</span></span>
            <span>L <span className="text-loss">{crosshairData.l.toFixed(2)}</span></span>
            <span>C <span className="text-text-bright">{crosshairData.c.toFixed(2)}</span></span>
          </div>
        )}

        <button onClick={() => closeChartModal()} className="btn-ghost btn-xs ml-2"><X className="w-4 h-4" /></button>
      </div>

      {/* ── Charts area ─────────────────────── */}
      <div className="flex-1 relative flex flex-col">
        {loading && (
          <div className="absolute inset-0 z-10 flex items-center justify-center bg-bg-base/80">
            <Loader2 className="w-6 h-6 animate-spin text-brand" />
          </div>
        )}
        {error && (
          <div className="absolute inset-0 z-10 flex flex-col items-center justify-center gap-2 text-text-muted bg-bg-base/80">
            <AlertCircle className="w-5 h-5 text-loss" />
            <span className="text-xs">{error}</span>
          </div>
        )}
        <div ref={containerRef} className={needsSub ? 'flex-[3]' : 'flex-1'} />
        {needsSub && <div ref={subContainerRef} className="flex-1 border-t border-border" />}
      </div>

      {/* ── Footer ──────────────────────────── */}
      <div className="flex items-center gap-4 px-4 py-1.5 bg-bg-surface border-t border-border text-[10px] text-text-muted shrink-0">
        <span><span className="kbd">Scroll</span> zoom</span>
        <span><span className="kbd">Drag</span> pan</span>
        <span><span className="kbd">Esc</span> close</span>
      </div>
    </div>
  )
}
