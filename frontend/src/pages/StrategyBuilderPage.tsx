/* ════════════════════════════════════════════════════════════════
   Strategy Builder Page
   Embeds the Universal Strategy Builder (HTML+JS) in a full-screen
   iframe.  All API calls inside the iframe are same-origin and will
   be proxied to the backend via Vite's /api proxy.
   ════════════════════════════════════════════════════════════════ */
import { useEffect, useRef } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'

export default function StrategyBuilderPage() {
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()
  const iframeRef = useRef<HTMLIFrameElement>(null)
  const strategyName = searchParams.get('name')

  // Build the src URL — optionally pre-load a saved strategy
  const src = strategyName
    ? `/strategy_builder/?name=${encodeURIComponent(strategyName)}`
    : '/strategy_builder/'

  // Listen for postMessage from the iframe (closeBuilder button)
  useEffect(() => {
    function onMessage(evt: MessageEvent) {
      if (evt.data?.type === 'ST_CLOSE_BUILDER') {
        navigate('/app/strategies')
      }
    }
    window.addEventListener('message', onMessage)
    return () => window.removeEventListener('message', onMessage)
  }, [navigate])

  return (
    <div className="fixed inset-0 z-50 bg-bg-base flex flex-col">
      {/* Thin header strip so users know they can navigate back */}
      <div className="flex items-center gap-3 px-4 h-10 shrink-0 bg-bg-surface border-b border-border">
        <button
          onClick={() => navigate('/app/strategies')}
          className="flex items-center gap-1.5 text-[11px] font-semibold text-text-muted hover:text-text-bright transition-colors"
        >
          ← Back to Strategies
        </button>
        <span className="text-text-muted text-[10px]">|</span>
        <span className="text-[11px] font-semibold text-brand">
          Strategy Builder
        </span>
        {strategyName && (
          <>
            <span className="text-text-muted text-[10px]">|</span>
            <span className="text-[11px] text-text-sec font-mono">{strategyName}</span>
          </>
        )}
      </div>

      {/* Full-screen builder iframe */}
      <iframe
        ref={iframeRef}
        src={src}
        title="Strategy Builder"
        className="flex-1 w-full border-0"
      />
    </div>
  )
}
