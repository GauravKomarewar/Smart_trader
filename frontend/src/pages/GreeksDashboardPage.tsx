/* ═══════════════════════════════════════════
   Greeks Calculator Page
   – BS Greeks + IV for given option params
   ═══════════════════════════════════════════ */
import { useState } from 'react'
import { api } from '../lib/api'
import { cn } from '../lib/utils'
import { Calculator, TrendingUp, Activity, ChevronRight, Loader2 } from 'lucide-react'

interface GreeksResult {
  bs_price: number
  iv:       number | null
  sigma:    number
  T:        number
  delta:    number
  gamma:    number
  theta:    number
  vega:     number
  rho:      number
}

const defaultForm = {
  spot:        '22000',
  strike:      '22000',
  expiry:      '',
  option_type: 'CE',
  risk_free:   '0.065',
  ltp:         '',
}

export default function GreeksDashboardPage() {
  const [form, setForm]         = useState(defaultForm)
  const [result, setResult]     = useState<GreeksResult | null>(null)
  const [loading, setLoading]   = useState(false)
  const [error, setError]       = useState('')

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
    setForm(f => ({ ...f, [e.target.name]: e.target.value }))
  }

  const calculate = async () => {
    if (!form.expiry) { setError('Expiry required (DDMMMYY e.g. 25JUL25)'); return }
    setError('')
    setLoading(true)
    try {
      const payload: any = {
        spot:        parseFloat(form.spot),
        strike:      parseFloat(form.strike),
        expiry:      form.expiry.toUpperCase(),
        option_type: form.option_type,
        risk_free:   parseFloat(form.risk_free),
      }
      if (form.ltp) payload.ltp = parseFloat(form.ltp)

      const res = await api.post<GreeksResult>('/greeks/calculate', payload)
      setResult(res)
    } catch (e: any) {
      setError(e.message || 'Calculation failed')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="p-6 space-y-6 max-w-5xl mx-auto">
      {/* Header */}
      <div className="flex items-center gap-3">
        <Calculator className="text-purple-400" size={24} />
        <h1 className="text-xl font-semibold text-white">Greeks Calculator</h1>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Input Form */}
        <div className="lg:col-span-1 bg-white/5 rounded-xl border border-white/10 p-5 space-y-4">
          <h2 className="text-sm font-medium text-gray-400 uppercase tracking-wider">Parameters</h2>

          <Field label="Underlying Spot" name="spot" type="number" value={form.spot} onChange={handleChange} />
          <Field label="Strike Price"    name="strike" type="number" value={form.strike} onChange={handleChange} />
          <div>
            <label className="block text-xs text-gray-400 mb-1">Expiry (DDMMMYY)</label>
            <input
              name="expiry"
              value={form.expiry}
              onChange={handleChange}
              placeholder="25JUL25"
              className="input-base w-full font-mono"
            />
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Option Type</label>
            <select
              name="option_type"
              value={form.option_type}
              onChange={handleChange}
              className="input-base w-full"
            >
              <option value="CE">CE (Call)</option>
              <option value="PE">PE (Put)</option>
            </select>
          </div>
          <Field label="Risk-Free Rate" name="risk_free" type="number" step="0.001" value={form.risk_free} onChange={handleChange} />
          <Field label="Market LTP (for IV)" name="ltp" type="number" value={form.ltp} onChange={handleChange} placeholder="Optional" />

          {error && <p className="text-xs text-red-400">{error}</p>}

          <button
            onClick={calculate}
            disabled={loading}
            className="w-full flex items-center justify-center gap-2 py-2.5 rounded-lg bg-purple-600 hover:bg-purple-500 text-white font-medium transition disabled:opacity-50"
          >
            {loading ? <Loader2 size={16} className="animate-spin" /> : <Calculator size={16} />}
            Calculate
          </button>
        </div>

        {/* Results */}
        <div className="lg:col-span-2 space-y-4">
          {result ? (
            <>
              {/* Price + IV */}
              <div className="grid grid-cols-3 gap-4">
                <ResultCard label="BS Price"  value={result.bs_price.toFixed(2)} color="blue" suffix="₹" />
                <ResultCard label="IV"        value={result.iv != null ? result.iv.toFixed(2) + '%' : '—'} color="yellow" />
                <ResultCard label="Sigma"     value={result.sigma.toFixed(2) + '%'} color="gray" />
              </div>

              {/* Greeks grid */}
              <div className="bg-white/5 rounded-xl border border-white/10 p-5">
                <h2 className="text-sm font-medium text-gray-400 uppercase tracking-wider mb-4">Greeks</h2>
                <div className="grid grid-cols-2 sm:grid-cols-5 gap-4">
                  <GreekCard label="Delta" value={result.delta} desc="Price sensitivity" />
                  <GreekCard label="Gamma" value={result.gamma} desc="Delta change rate" />
                  <GreekCard label="Theta" value={result.theta} desc="Time decay / day" color="red" />
                  <GreekCard label="Vega"  value={result.vega}  desc="Vol sensitivity / 1%" />
                  <GreekCard label="Rho"   value={result.rho}   desc="Rate sensitivity /1%" />
                </div>

                <div className="mt-4 pt-4 border-t border-white/10 text-xs text-gray-500">
                  T = {(result.T * 365).toFixed(2)} days | σ = {result.sigma.toFixed(2)}%
                </div>
              </div>

              {/* Strategy table reference */}
              <div className="bg-white/5 rounded-xl border border-white/10 p-5">
                <h2 className="text-sm font-medium text-gray-400 uppercase mb-3">Position Greeks Impact</h2>
                <div className="text-xs text-gray-500 space-y-1">
                  <p>• <span className="text-white">Delta</span>: +{result.delta.toFixed(4)} per unit LTP move</p>
                  <p>• <span className="text-white">Theta</span>: ₹{(result.theta * parseFloat(form.spot || '1')).toFixed(2)} time decay per day</p>
                  <p>• <span className="text-white">Vega</span>:  ₹{result.vega.toFixed(4)} per 1% IV change</p>
                </div>
              </div>
            </>
          ) : (
            <div className="h-full flex flex-col items-center justify-center py-20 text-gray-600">
              <Calculator size={48} className="mb-4 opacity-30" />
              <p>Enter parameters and click Calculate</p>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

function Field({ label, name, type = 'text', value, onChange, step, placeholder }: {
  label: string; name: string; type?: string; value: string;
  onChange: (e: any) => void; step?: string; placeholder?: string
}) {
  return (
    <div>
      <label className="block text-xs text-gray-400 mb-1">{label}</label>
      <input
        name={name}
        type={type}
        step={step}
        value={value}
        onChange={onChange}
        placeholder={placeholder}
        className="input-base w-full"
      />
    </div>
  )
}

function ResultCard({ label, value, color, suffix }: {
  label: string; value: string; color: 'blue' | 'yellow' | 'gray'; suffix?: string
}) {
  const colors = {
    blue:   'text-blue-400',
    yellow: 'text-yellow-400',
    gray:   'text-gray-300',
  }
  return (
    <div className="bg-white/5 rounded-xl border border-white/10 p-4 text-center">
      <p className="text-xs text-gray-500 mb-1">{label}</p>
      <p className={cn('text-lg font-semibold', colors[color])}>{suffix}{value}</p>
    </div>
  )
}

function GreekCard({ label, value, desc, color = 'green' }: {
  label: string; value: number; desc: string; color?: 'green' | 'red'
}) {
  const isNeg = value < 0
  return (
    <div className="text-center p-3 bg-white/3 rounded-lg">
      <p className="text-xs text-gray-500 mb-1">{label}</p>
      <p className={cn(
        'text-sm font-mono font-semibold',
        color === 'red' ? 'text-red-400' : isNeg ? 'text-red-300' : 'text-emerald-300'
      )}>
        {value > 0 ? '+' : ''}{value.toFixed(4)}
      </p>
      <p className="text-[10px] text-gray-600 mt-1">{desc}</p>
    </div>
  )
}
