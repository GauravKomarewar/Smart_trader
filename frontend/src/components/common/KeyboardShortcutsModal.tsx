import { useUIStore } from '../../stores'
import { X, Command } from 'lucide-react'

const SHORTCUTS = [
  { group: 'Navigation', items: [
    { keys: ['G', 'D'],        label: 'Go to Dashboard' },
    { keys: ['G', 'M'],        label: 'Go to Market' },
    { keys: ['G', 'O'],        label: 'Go to Option Chain' },
    { keys: ['G', 'W'],        label: 'Go to Watchlist / Chart' },
    { keys: ['G', 'S'],        label: 'Go to Settings' },
  ]},
  { group: 'Trading', items: [
    { keys: ['Ctrl', 'O'],     label: 'Open Place Order modal' },
    { keys: ['Ctrl', 'Enter'], label: 'Submit order (in modal)' },
    { keys: ['Escape'],        label: 'Close modal / Cancel' },
    { keys: ['B'],             label: 'Buy selected instrument (hover)' },
    { keys: ['S'],             label: 'Sell selected instrument (hover)' },
  ]},
  { group: 'Interface', items: [
    { keys: ['Ctrl', '/'],     label: 'Open global search' },
    { keys: ['F1'],            label: 'Open keyboard shortcuts' },
    { keys: ['/'],             label: 'Search watchlist (watchlist page)' },
    { keys: ['↑ / ↓'],        label: 'Navigate watchlist items' },
    { keys: ['Enter'],         label: 'Select watchlist item' },
  ]},
  { group: 'Chart', items: [
    { keys: ['1'],             label: '1-min chart' },
    { keys: ['5'],             label: '5-min chart' },
    { keys: ['D'],             label: 'Daily chart' },
    { keys: ['Ctrl', 'F'],     label: 'Fit chart to screen' },
  ]},
]

export default function KeyboardShortcutsModal() {
  const { shortcutsOpen, setShortcutsOpen } = useUIStore()
  if (!shortcutsOpen) return null

  return (
    <div
      className="fixed inset-0 z-[120] flex items-center justify-center bg-black/60 backdrop-blur-sm"
      onClick={e => { if (e.target === e.currentTarget) setShortcutsOpen(false) }}
    >
      <div className="bg-bg-card border border-border rounded-2xl shadow-modal w-[600px] max-h-[80vh] overflow-hidden flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-border">
          <div className="flex items-center gap-2.5">
            <Command className="w-4 h-4 text-brand" />
            <h2 className="text-[14px] font-semibold text-text-bright">Keyboard Shortcuts</h2>
          </div>
          <button onClick={() => setShortcutsOpen(false)} className="btn-ghost btn-xs">
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Grid */}
        <div className="overflow-y-auto p-5 grid grid-cols-2 gap-6">
          {SHORTCUTS.map(group => (
            <div key={group.group}>
              <div className="text-[10px] font-semibold text-text-muted uppercase tracking-wider mb-2">
                {group.group}
              </div>
              <div className="space-y-1.5">
                {group.items.map(item => (
                  <div key={item.label} className="flex items-center justify-between gap-3">
                    <span className="text-[11px] text-text-sec">{item.label}</span>
                    <div className="flex items-center gap-1 shrink-0">
                      {item.keys.map((k, i) => (
                        <span key={i} className="kbd">{k}</span>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
