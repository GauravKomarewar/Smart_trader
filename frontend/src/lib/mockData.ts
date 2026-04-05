/* ═══════════════════════════════════════════════
   SMART TRADER — Empty data stubs
   Returns empty arrays / zero values only.
   NO fake prices, NO fake positions, NO fake anything.
   ═══════════════════════════════════════════════ */
import type {
  IndexQuote, OptionChainData, ScreenerRow,
} from '../types'

// All demo data has been removed to prevent false trading decisions.
// These exports remain as empty stubs so imports don't break.

export const DEMO_INDICES: IndexQuote[] = []
export const DEMO_SCREENER: ScreenerRow[] = []

export function generateOptionChain(
  underlying: string, ltp: number, expiry: string
): OptionChainData {
  return {
    underlying,
    underlyingLtp: 0,
    expiry: expiry || '',
    expiries: [],
    pcr: 0,
    maxPainStrike: 0,
    rows: [],
  }
}
