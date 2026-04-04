/* ════════════════════════════════════════════
   Advanced Chart Modal — full screen overlay
   TradingView Lightweight Charts
   ════════════════════════════════════════════ */
import { useEffect, useRef, useState } from 'react'
import {
  createChart, type IChartApi, type ISeriesApi, type UTCTimestamp,
  CandlestickSeries, AreaSeries, BarSeries, LineSeries, HistogramSeries,
} from 'lightweight-charts'
import { useUIStore } from '../../stores'
import { cn } from '../../lib/utils'
import { X, BarChart2, CandlestickChart, TrendingUp, Volume2, Minus, RefreshCw } from 'lucide-react'
import type { ChartInterval } from '../../types'

const INTERVALS: ChartInterval[] = ['1m','3m','5m','15m','30m','1h','4h','D','W']

type ChartType = 'candlestick' | 'line' | 'area' | 'bar'

const INDICATORS = ['MA(9)', 'MA(21)', 'EMA(50)', 'BB(20)', 'RSI(14)', 'MACD']

export default function ChartModal() {
  const { chartModalOpen, chartModalToken, closeChartModal } = useUIStore()
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const mainSeriesRef = useRef<ISeriesApi<any> | null>(null)
  const volSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null)

  const [interval, setInterval] = useState<ChartInterval>('15m')
  const [chartType, setChartType] = useState<ChartType>('candlestick')
  const [showVolume, setShowVolume] = useState(true)
  const [activeIndicators, setActiveIndicators] = useState<string[]>(['MA(21)'])
  const [crosshairData, setCrosshairData] = useState<{o: number; h: number; l: number; c: number; v: number} | null>(null)

  // Keyboard: Escape closes
  useEffect(() => {
    if (!chartModalOpen) return
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') closeChartModal()
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [chartModalOpen])

  useEffect(() => {
    if (!chartModalOpen || !containerRef.current) return

    const chart = createChart(containerRef.current, {
      layout: {
        background: { color: '#0b0e17' },
        textColor: '#7b8398',
        fontSize: 11,
      },
      grid: {
        vertLines: { color: '#1c2133' },
        horzLines: { color: '#1c2133' },
      },
      crosshair: {
        mode: 1,
        vertLine: { color: '#22d3ee', labelBackgroundColor: '#22d3ee' },
        horzLine: { color: '#22d3ee', labelBackgroundColor: '#22d3ee' },
      },
      rightPriceScale: { borderColor: '#252b3b', scaleMargins: { top: 0.1, bottom: 0.2 } },
      timeScale: { borderColor: '#252b3b', timeVisible: true, secondsVisible: false },
      handleScroll: { mouseWheel: true, pressedMouseMove: true },
      handleScale: { mouseWheel: true, pinch: true },
      width: containerRef.current.clientWidth,
      height: containerRef.current.clientHeight,
    })
    chartRef.current = chart

    let mainSeries: ISeriesApi<any>
    if (chartType === 'candlestick') {
      mainSeries = chart.addSeries(CandlestickSeries, {
        upColor: '#22c55e', downColor: '#f43f5e',
        borderUpColor: '#22c55e', borderDownColor: '#f43f5e',
        wickUpColor: '#22c55e', wickDownColor: '#f43f5e',
      })
    } else if (chartType === 'area') {
      mainSeries = chart.addSeries(AreaSeries, {
        lineColor: '#22d3ee',
        topColor: 'rgba(34,211,238,.25)',
        bottomColor: 'rgba(34,211,238,.02)',
        lineWidth: 2,
      })
    } else if (chartType === 'bar') {
      mainSeries = chart.addSeries(BarSeries, {
        upColor: '#22c55e', downColor: '#f43f5e',
      })
    } else {
      mainSeries = chart.addSeries(LineSeries, {
        color: '#22d3ee', lineWidth: 2,
      })
    }
    mainSeriesRef.current = mainSeries

    // Volume
    const volSeries = chart.addSeries(HistogramSeries, {
      color: '#22d3ee',
      priceFormat: { type: 'volume' },
      priceScaleId: 'vol',
    })
    chart.priceScale('vol').applyOptions({ scaleMargins: { top: 0.85, bottom: 0 } })
    volSeriesRef.current = volSeries

    // Generate mock data
    const candles = generateMockCandles(interval)
    if (chartType === 'area' || chartType === 'line') {
      mainSeries.setData(candles.map(c => ({ time: c.time, value: c.close })))
    } else {
      mainSeries.setData(candles)
    }
    if (showVolume) {
      volSeries.setData(candles.map(c => ({
        time: c.time,
        value: c.volume,
        color: c.close >= c.open ? 'rgba(34,197,94,.35)' : 'rgba(244,63,94,.35)',
      })))
    }

    chart.timeScale().fitContent()

    // Crosshair listener
    chart.subscribeCrosshairMove(param => {
      if (param.point) {
        const d = param.seriesData.get(mainSeries)
        if (d && 'open' in d) {
          setCrosshairData({ o: (d as any).open, h: (d as any).high, l: (d as any).low, c: (d as any).close, v: 0 })
        } else if (d && 'value' in d) {
          setCrosshairData({ o: (d as any).value, h: (d as any).value, l: (d as any).value, c: (d as any).value, v: 0 })
        }
      } else {
        setCrosshairData(null)
      }
    })

    const ro = new ResizeObserver(() => {
      if (containerRef.current) {
        chart.resize(containerRef.current.clientWidth, containerRef.current.clientHeight)
      }
    })
    ro.observe(containerRef.current)

    return () => {
      ro.disconnect()
      chart.remove()
      chartRef.current = null
      mainSeriesRef.current = null
    }
  }, [chartModalOpen, interval, chartType, showVolume])

  if (!chartModalOpen) return null

  const symbol = chartModalToken ?? 'NIFTY'

  return (
    <div className="fixed inset-0 z-[110] flex flex-col bg-bg-base">
      {/* Toolbar */}
      <div className="flex items-center gap-3 px-4 py-2.5 bg-bg-surface border-b border-border shrink-0 flex-wrap gap-y-2">
        {/* Symbol */}
        <span className="text-[14px] font-bold text-text-bright">{symbol}</span>

        {/* Intervals */}
        <div className="flex items-center bg-bg-elevated border border-border rounded overflow-hidden">
          {INTERVALS.map(ivl => (
            <button
              key={ivl}
              onClick={() => setInterval(ivl)}
              className={cn(
                'px-2 py-1 text-[10px] font-medium transition-colors',
                interval === ivl ? 'bg-brand text-bg-base' : 'text-text-muted hover:text-text-sec'
              )}
            >
              {ivl}
            </button>
          ))}
        </div>

        {/* Chart type */}
        <div className="flex items-center gap-1">
          {([['candlestick', CandlestickChart], ['line', TrendingUp], ['bar', BarChart2]] as [ChartType, any][]).map(([t, Icon]) => (
            <button
              key={t}
              onClick={() => setChartType(t)}
              className={cn('btn-ghost btn-xs', chartType === t && 'text-brand bg-brand/10')}
              title={t}
            >
              <Icon className="w-3.5 h-3.5" />
            </button>
          ))}
        </div>

        {/* Volume toggle */}
        <button
          onClick={() => setShowVolume(v => !v)}
          className={cn('btn-ghost btn-xs gap-1', showVolume && 'text-brand')}
        >
          <Volume2 className="w-3.5 h-3.5" /> Vol
        </button>

        {/* Indicators */}
        <div className="flex items-center gap-1 flex-wrap">
          {INDICATORS.map(ind => (
            <button
              key={ind}
              onClick={() => setActiveIndicators(a => a.includes(ind) ? a.filter(x => x !== ind) : [...a, ind])}
              className={cn(
                'px-2 py-0.5 rounded text-[10px] font-medium transition-colors border',
                activeIndicators.includes(ind)
                  ? 'bg-accent/20 text-accent border-accent/40'
                  : 'text-text-muted border-transparent hover:border-border'
              )}
            >
              {ind}
            </button>
          ))}
        </div>

        <div className="flex-1" />

        {/* Crosshair OHLC */}
        {crosshairData && (
          <div className="flex items-center gap-3 text-[10px] font-mono">
            <span>O <span className="text-text-bright">{crosshairData.o.toFixed(2)}</span></span>
            <span>H <span className="text-profit">{crosshairData.h.toFixed(2)}</span></span>
            <span>L <span className="text-loss">{crosshairData.l.toFixed(2)}</span></span>
            <span>C <span className="text-text-bright">{crosshairData.c.toFixed(2)}</span></span>
          </div>
        )}

        <button onClick={() => closeChartModal()} className="btn-ghost btn-xs ml-2">
          <X className="w-4 h-4" />
        </button>
      </div>

      {/* Chart area */}
      <div ref={containerRef} className="flex-1" />

      {/* Footer hint */}
      <div className="flex items-center gap-4 px-4 py-1.5 bg-bg-surface border-t border-border text-[10px] text-text-muted shrink-0">
        <span><span className="kbd">Scroll</span> zoom</span>
        <span><span className="kbd">Drag</span> pan</span>
        <span><span className="kbd">Esc</span> close</span>
      </div>
    </div>
  )
}

function generateMockCandles(interval: ChartInterval) {
  const msPerBar: Record<ChartInterval, number> = {
    '1m': 60000, '3m': 180000, '5m': 300000, '10m': 600000,
    '15m': 900000, '30m': 1800000, '1h': 3600000, '2h': 7200000,
    '4h': 14400000, 'D': 86400000, 'W': 604800000,
  }
  const count = interval === 'D' ? 300 : interval === 'W' ? 150 : 200
  const bars: any[] = []
  let t = Date.now() - count * (msPerBar[interval] ?? 900000)
  let price = 19850 + Math.random() * 500
  for (let i = 0; i < count; i++) {
    const o = price
    const h = o + Math.abs((Math.random() - 0.3) * o * 0.01)
    const l = o - Math.abs((Math.random() - 0.3) * o * 0.01)
    const c = l + Math.random() * (h - l)
    price = c
    bars.push({
      time: Math.floor(t / 1000) as UTCTimestamp,
      open: +o.toFixed(2), high: +h.toFixed(2), low: +l.toFixed(2), close: +c.toFixed(2),
      volume: Math.floor(100000 + Math.random() * 900000),
    })
    t += msPerBar[interval] ?? 900000
  }
  return bars
}
