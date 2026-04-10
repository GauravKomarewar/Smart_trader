/* ════════════════════════════════════════════
   Settings Page — 7 sub-sections
   ════════════════════════════════════════════ */
import { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useAuthStore, useSettingsStore, useToastStore } from '../stores'
import { api } from '../lib/api'
import { cn } from '../lib/utils'
import {
  User, Link2, Database, BarChart2, Palette,
  Activity, Copy, Webhook, Save, Plus, Trash2,
  Eye, EyeOff, CheckCircle, AlertTriangle, RefreshCw,
  Shield, Bell, ChevronRight, Loader2, X, Pencil,
  Terminal, XCircle,
} from 'lucide-react'

const TABS = [
  { id: 'brokers',    label: 'Broker Accounts',  icon: Link2 },
  { id: 'webhook',    label: 'Webhooks',         icon: Webhook },
  { id: 'market',     label: 'Market Data',      icon: Database },
  { id: 'optiondata', label: 'Option Chain Data', icon: BarChart2 },
  { id: 'theme',      label: 'Theme & Display',  icon: Palette },
  { id: 'copy',       label: 'Copy Trading',     icon: Copy },
  { id: 'profile',    label: 'Profile',         icon: User },
] as const
type TabId = (typeof TABS)[number]['id']

export default function SettingsPage() {
  const { tab } = useParams<{ tab?: string }>()
  const navigate = useNavigate()
  const active = (tab ?? 'brokers') as TabId

  return (
    <div className="h-full flex flex-col overflow-hidden">
      {/* Mobile horizontal scrollable tabs */}
      <div className="sm:hidden flex items-center gap-1 px-2 py-2 bg-bg-surface border-b border-border overflow-x-auto shrink-0">
        {TABS.map(t => {
          const Icon = t.icon
          return (
            <button
              key={t.id}
              onClick={() => navigate(`/app/settings/${t.id}`)}
              className={cn(
                'flex items-center gap-1.5 px-3 py-1.5 rounded text-[11px] font-medium whitespace-nowrap transition-colors shrink-0',
                active === t.id
                  ? 'bg-brand/15 text-brand'
                  : 'text-text-muted hover:text-text-sec hover:bg-bg-hover'
              )}
            >
              <Icon className="w-3 h-3 shrink-0" />
              {t.label}
            </button>
          )
        })}
      </div>

      {/* Desktop + Mobile main area */}
      <div className="flex-1 flex overflow-hidden">
        {/* Desktop sidebar */}
        <aside className="hidden sm:flex w-52 shrink-0 border-r border-border bg-bg-surface flex-col py-3">
          {TABS.map(t => {
            const Icon = t.icon
            const isActive = active === t.id
            return (
              <button
                key={t.id}
                onClick={() => navigate(`/app/settings/${t.id}`)}
                className={cn(
                  'flex items-center gap-2.5 px-4 py-2.5 text-[12px] font-medium transition-colors border-l-2 text-left',
                  isActive
                    ? 'text-brand bg-brand/10 border-brand'
                    : 'text-text-muted hover:text-text-sec hover:bg-bg-hover border-transparent'
                )}
              >
                <Icon className="w-3.5 h-3.5 shrink-0" />
                <span className="leading-tight">{t.label}</span>
              </button>
            )
          })}
        </aside>

        {/* Content */}
        <div className="flex-1 overflow-y-auto">
          <div className="max-w-3xl mx-auto p-4 sm:p-6 space-y-6">
            {active === 'profile'     && <ProfileSection />}
            {active === 'brokers'     && <BrokersSection />}
            {active === 'market'      && <MarketDataSection />}
            {active === 'optiondata'  && <OptionDataSection />}
            {active === 'theme'       && <ThemeSection />}
            {active === 'copy'        && <CopyTradeSection />}
            {active === 'webhook'     && <WebhookSection />}
          </div>
        </div>
      </div>
    </div>
  )
}

// ── Shared UI helpers ───────────────────────────
function SectionHeader({ title, description }: { title: string; description?: string }) {
  return (
    <div className="mb-6">
      <h2 className="text-[14px] font-semibold text-text-bright">{title}</h2>
      {description && <p className="text-[12px] text-text-muted mt-0.5">{description}</p>}
    </div>
  )
}

function CardBox({ children, className }: { children: React.ReactNode; className?: string }) {
  return <div className={cn('bg-bg-card border border-border rounded-xl p-5', className)}>{children}</div>
}

function FieldRow({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col sm:grid sm:grid-cols-[160px_1fr] items-start sm:items-center gap-1.5 sm:gap-4 py-2.5 border-b border-border/50 last:border-0">
      <label className="text-[12px] text-text-muted shrink-0 leading-tight">{label}</label>
      <div className="w-full">{children}</div>
    </div>
  )
}

function TextInput({ value, onChange, type = 'text', placeholder = '' }: {
  value: string; onChange: (v: string) => void; type?: string; placeholder?: string
}) {
  const [show, setShow] = useState(false)
  const t = type === 'password' ? (show ? 'text' : 'password') : type
  return (
    <div className="relative">
      <input
        type={t}
        value={value}
        onChange={e => onChange(e.target.value)}
        placeholder={placeholder}
        className="input-base w-full text-[12px]"
      />
      {type === 'password' && (
        <button
          type="button"
          className="absolute right-2.5 top-1/2 -translate-y-1/2 text-text-muted hover:text-text-sec"
          onClick={() => setShow(!show)}
        >
          {show ? <EyeOff className="w-3.5 h-3.5" /> : <Eye className="w-3.5 h-3.5" />}
        </button>
      )}
    </div>
  )
}

// ── Profile Section ─────────────────────────────
function ProfileSection() {
  const { user } = useAuthStore()
  const toast = useToastStore(s => s.toast)
  const [name, setName] = useState(user?.name ?? '')
  const [email, setEmail] = useState(user?.email ?? '')
  const [phone, setPhone] = useState(user?.phone ?? '')
  const [curPwd, setCurPwd] = useState('')
  const [newPwd, setNewPwd] = useState('')

  return (
    <>
      <SectionHeader title="Profile" description="Manage your personal information and password" />
      <CardBox>
        <FieldRow label="Full Name"><TextInput value={name} onChange={setName} /></FieldRow>
        <FieldRow label="Email"><TextInput value={email} onChange={setEmail} type="email" /></FieldRow>
        <FieldRow label="Phone"><TextInput value={phone} onChange={setPhone} placeholder="+91 9999000000" /></FieldRow>
      </CardBox>
      <CardBox className="mt-4">
        <div className="text-[12px] font-medium text-text-sec mb-3">Change Password</div>
        <FieldRow label="Current Password"><TextInput value={curPwd} onChange={setCurPwd} type="password" /></FieldRow>
        <FieldRow label="New Password"><TextInput value={newPwd} onChange={setNewPwd} type="password" /></FieldRow>
      </CardBox>
      <div className="flex justify-end mt-4">
        <button onClick={() => toast('Profile updated', 'success')} className="btn-brand btn-sm">
          <Save className="w-3.5 h-3.5" /> Save Changes
        </button>
      </div>
    </>
  )
}

// ── Brokers Section ─────────────────────────────
function BrokersSection() {
  const toast = useToastStore(s => s.toast)

  // configs list
  const [configs, setConfigs]   = useState<any[]>([])
  const [loading, setLoading]   = useState(false)

  // supported brokers list + fields
  const [supported, setSupported] = useState<any[]>([])

  // Add broker modal
  const [addOpen, setAddOpen]   = useState(false)
  const [newBroker, setNewBroker] = useState('')
  const [fields,    setFields]  = useState<any[]>([])
  const [creds,     setCreds]   = useState<Record<string, string>>({})
  const [nickname,  setNickname] = useState('')

  // Edit broker modal
  const [editConfig, setEditConfig] = useState<any | null>(null)
  const [editCreds,  setEditCreds]  = useState<Record<string, string>>({})
  const [editNickname, setEditNickname] = useState('')

  // Env preview
  const [envPreview, setEnvPreview] = useState<{ config_id: string; content: string; warning: string } | null>(null)

  // action processing
  const [actionId, setActionId] = useState<string | null>(null)

  useEffect(() => {
    loadConfigs()
    api.brokerSupported().then(setSupported).catch(() => {})
  }, [])

  async function loadConfigs() {
    setLoading(true)
    try { setConfigs(await api.brokerConfigs()) }
    catch { setConfigs([]) }
    finally { setLoading(false) }
  }

  // ── Add broker handlers ────────────────────────
  async function onNewBrokerChange(brokerId: string) {
    setNewBroker(brokerId)
    setCreds({})
    if (brokerId) {
      try {
        const res: any = await api.brokerFields(brokerId)
        const raw = Array.isArray(res) ? res : (res.fields ?? [])
        setFields(raw.map((f: any) => ({
          id: f.id ?? f.key,
          label: f.label,
          placeholder: f.placeholder ?? f.hint ?? '',
          required: !!f.required,
          sensitive: f.sensitive ?? (f.type === 'password'),
        })))
      } catch { setFields([]) }
    } else { setFields([]) }
  }

  async function handleAdd(e: React.FormEvent) {
    e.preventDefault()
    if (!newBroker || !nickname) { toast('Select broker and enter a nickname', 'error'); return }
    try {
      await api.addBrokerConfig(newBroker, nickname, creds)
      toast('Broker added successfully', 'success')
      setAddOpen(false)
      setNewBroker(''); setNickname(''); setCreds({}); setFields([])
      loadConfigs()
    } catch (err: any) { toast(err?.message ?? 'Failed to add broker', 'error') }
  }

  // ── Edit broker handlers ───────────────────────
  async function openEdit(cfg: any) {
    setEditConfig(cfg)
    setEditNickname(cfg.broker_name ?? cfg.client_id ?? '')
    try {
      const res: any = await api.brokerFields(cfg.broker_id)
      const raw = Array.isArray(res) ? res : (res.fields ?? [])
      setFields(raw.map((f: any) => ({
        id: f.id ?? f.key,
        label: f.label,
        placeholder: f.placeholder ?? f.hint ?? '',
        required: !!f.required,
        sensitive: f.sensitive ?? (f.type === 'password'),
      })))
    } catch { setFields([]) }

    // Pre-populate from saved credentials (full values returned by backend)
    try {
      const saved = await api.brokerEditCreds(cfg.id)
      const prefilled: Record<string, string> = {}
      for (const [k, v] of Object.entries(saved.fields ?? {})) {
        prefilled[k] = String(v ?? '')
      }
      setEditCreds(prefilled)
    } catch {
      setEditCreds({})
    }
  }

  async function handleEditSave(e: React.FormEvent) {
    e.preventDefault()
    if (!editConfig) return
    const payload: any = { nickname: editNickname }
    if (Object.keys(editCreds).length > 0) payload.credentials = editCreds
    try {
      await api.updateBrokerConfig(editConfig.id, payload)
      toast('Broker updated', 'success')
      setEditConfig(null)
      loadConfigs()
    } catch (err: any) { toast(err?.message ?? 'Update failed', 'error') }
  }

  // ── Connect / disconnect / delete ─────────────
  async function handleConnect(id: string) {
    setActionId(id)
    try {
      const res = await api.connectBroker(id)
      toast(res.message ?? 'Connected', 'success')
      loadConfigs()
    } catch (err: any) { toast(err?.message ?? 'Connect failed', 'error') }
    finally { setActionId(null) }
  }

  async function handleDisconnect(id: string) {
    setActionId(id)
    try {
      await api.disconnectBroker(id)
      toast('Disconnected', 'info')
      loadConfigs()
    } catch (err: any) { toast(err?.message ?? 'Disconnect failed', 'error') }
    finally { setActionId(null) }
  }

  async function handleDelete(id: string) {
    setActionId(id)
    try {
      await api.deleteBrokerConfig(id)
      toast('Broker removed', 'info')
      loadConfigs()
    } catch (err: any) { toast(err?.message ?? 'Delete failed', 'error') }
    finally { setActionId(null) }
  }

  async function handleEnvPreview(id: string) {
    try {
      const res = await api.brokerEnvPreview(id)
      setEnvPreview({ config_id: id, content: res.env_content, warning: res.warning })
    } catch (err: any) { toast(err?.message ?? 'Preview failed', 'error') }
  }

  // ── Render ─────────────────────────────────────
  return (
    <>
      <SectionHeader
        title="Broker Sessions"
        description="Add, configure and connect multiple broker accounts. Credentials are encrypted at rest."
      />

      <div className="flex justify-between items-center mb-4">
        <div className="text-[12px] text-text-muted">
          {configs.length} broker{configs.length !== 1 ? 's' : ''} configured
        </div>
        <div className="flex gap-2">
          <button onClick={loadConfigs} className="btn-ghost btn-sm gap-1">
            <RefreshCw className="w-3.5 h-3.5" />
          </button>
          <button onClick={() => setAddOpen(true)} className="btn-brand btn-sm gap-1.5">
            <Plus className="w-3.5 h-3.5" /> Add Broker
          </button>
        </div>
      </div>

      {/* Broker cards */}
      {loading ? (
        <div className="flex items-center gap-2 text-text-muted text-[12px] py-8 justify-center">
          <Loader2 className="w-4 h-4 animate-spin" /> Loading brokers…
        </div>
      ) : configs.length === 0 ? (
        <CardBox className="text-center py-10">
          <Link2 className="w-8 h-8 text-text-muted mx-auto mb-3" />
          <div className="text-[13px] font-medium text-text-sec mb-1">No brokers configured</div>
          <div className="text-[11px] text-text-muted mb-4">Add a broker to start live trading</div>
          <button onClick={() => setAddOpen(true)} className="btn-brand btn-sm gap-1.5">
            <Plus className="w-3.5 h-3.5" /> Add Broker
          </button>
        </CardBox>
      ) : (
        <div className="space-y-3">
          {configs.map((cfg: any) => {
            const busy = actionId === cfg.id
            const connected = cfg.session?.is_logged_in ?? false
            return (
              <CardBox key={cfg.id} className={connected ? 'border-profit/30' : ''}>
                <div className="flex items-start justify-between gap-4">
                  <div className="flex items-center gap-3 min-w-0">
                    <div className={`w-9 h-9 rounded-lg flex items-center justify-center shrink-0 ${
                      connected ? 'bg-profit/10 border border-profit/20' : 'bg-brand/10 border border-brand/20'
                    }`}>
                      <Shield className={`w-4 h-4 ${connected ? 'text-profit' : 'text-brand'}`} />
                    </div>
                    <div className="min-w-0">
                      <div className="text-[13px] font-semibold text-text-bright truncate">{cfg.broker_name ?? cfg.client_id}</div>
                      <div className="text-[11px] text-text-muted capitalize">{cfg.broker_id} · {cfg.client_id}</div>
                    </div>
                  </div>
                  <div className="flex items-center gap-2 shrink-0">
                    {connected
                      ? <span className="flex items-center gap-1 text-[10px] text-profit border border-profit/20 bg-profit/10 px-2 py-0.5 rounded">
                          <CheckCircle className="w-3 h-3" /> Connected
                        </span>
                      : <span className="flex items-center gap-1 text-[10px] text-text-muted border border-border bg-bg-base px-2 py-0.5 rounded">
                          <XCircle className="w-3 h-3" /> Offline
                        </span>
                    }
                  </div>
                </div>

                {cfg.session?.login_at && (
                  <div className="mt-2 text-[10px] text-text-muted">
                    Last connected: {new Date(cfg.session.login_at).toLocaleString('en-IN')}
                  </div>
                )}

                <div className="flex flex-wrap gap-2 mt-3 pt-3 border-t border-border/50">
                  {!connected ? (
                    <button
                      onClick={() => handleConnect(cfg.id)}
                      disabled={busy}
                      className="btn-brand btn-sm gap-1.5"
                    >
                      {busy ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Link2 className="w-3.5 h-3.5" />}
                      Connect
                    </button>
                  ) : (
                    <button
                      onClick={() => handleDisconnect(cfg.id)}
                      disabled={busy}
                      className="btn-ghost btn-sm border border-border gap-1.5 hover:text-loss"
                    >
                      {busy ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <XCircle className="w-3.5 h-3.5" />}
                      Disconnect
                    </button>
                  )}
                  <button
                    onClick={() => openEdit(cfg)}
                    disabled={busy}
                    className="btn-ghost btn-sm border border-border gap-1"
                  >
                    <Pencil className="w-3.5 h-3.5" /> Edit
                  </button>
                  <button
                    onClick={() => handleEnvPreview(cfg.id)}
                    disabled={busy}
                    className="btn-ghost btn-sm border border-border gap-1"
                    title="Preview .env"
                  >
                    <Terminal className="w-3.5 h-3.5" /> .env
                  </button>
                  <button
                    onClick={() => handleDelete(cfg.id)}
                    disabled={busy}
                    className="btn-ghost btn-sm border border-border gap-1 hover:text-loss ml-auto"
                  >
                    {busy ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Trash2 className="w-3.5 h-3.5" />}
                  </button>
                </div>
              </CardBox>
            )
          })}
        </div>
      )}

      {/* ── Add Broker Modal ────────────────────── */}
      {addOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm">
          <div className="bg-bg-surface border border-border rounded-2xl w-full max-w-[460px] shadow-2xl max-h-[90vh] overflow-y-auto">
            <div className="flex items-center justify-between p-5 border-b border-border sticky top-0 bg-bg-surface">
              <h2 className="text-[14px] font-semibold text-text-bright">Add Broker Account</h2>
              <button onClick={() => { setAddOpen(false); setNewBroker(''); setFields([]); setCreds({}) }} className="btn-ghost btn-xs p-1">
                <X className="w-4 h-4" />
              </button>
            </div>
            <form onSubmit={handleAdd} className="p-5 space-y-4">
              <div>
                <label className="block text-[11px] text-text-muted mb-1.5">Broker</label>
                <select
                  value={newBroker}
                  onChange={e => onNewBrokerChange(e.target.value)}
                  className="input-base w-full"
                  required
                >
                  <option value="">Select broker…</option>
                  {supported.map((b: any) => (
                    <option key={b.id} value={b.id}>{b.name}</option>
                  ))}
                </select>
              </div>
              <div>
                <label className="block text-[11px] text-text-muted mb-1.5">Nickname</label>
                <input
                  type="text"
                  value={nickname}
                  onChange={e => setNickname(e.target.value)}
                  placeholder="e.g. My Shoonya Account"
                  className="input-base w-full"
                  required
                />
              </div>
              {fields.map((f: any) => (
                <div key={f.id}>
                  <label className="block text-[11px] text-text-muted mb-1.5">
                    {f.label} {f.required && <span className="text-loss">*</span>}
                  </label>
                  <TextInput
                    value={creds[f.id] ?? ''}
                    onChange={v => setCreds(prev => ({ ...prev, [f.id]: v }))}
                    type={f.sensitive ? 'password' : 'text'}
                    placeholder={f.placeholder ?? ''}
                  />
                </div>
              ))}
              <div className="flex gap-2 pt-2">
                <button type="button" onClick={() => setAddOpen(false)} className="btn-ghost btn-sm flex-1 justify-center">Cancel</button>
                <button type="submit" className="btn-brand btn-sm flex-1 justify-center">
                  <Plus className="w-3.5 h-3.5" /> Add Broker
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* ── Edit Broker Modal ───────────────────── */}
      {editConfig && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm">
          <div className="bg-bg-surface border border-border rounded-2xl w-full max-w-[460px] shadow-2xl max-h-[90vh] overflow-y-auto">
            <div className="flex items-center justify-between p-5 border-b border-border sticky top-0 bg-bg-surface">
              <h2 className="text-[14px] font-semibold text-text-bright">Edit — {editConfig.nickname}</h2>
              <button onClick={() => setEditConfig(null)} className="btn-ghost btn-xs p-1"><X className="w-4 h-4" /></button>
            </div>
            <form onSubmit={handleEditSave} className="p-5 space-y-4">
              <div>
                <label className="block text-[11px] text-text-muted mb-1.5">Nickname</label>
                <TextInput value={editNickname} onChange={setEditNickname} />
              </div>
              <div className="text-[11px] text-text-muted">
                All credential values are shown in full. Edit as needed.
              </div>
              {fields.map((f: any) => (
                <div key={f.id}>
                  <label className="block text-[11px] text-text-muted mb-1.5">
                    {f.label}
                  </label>
                  <TextInput
                    value={editCreds[f.id] ?? ''}
                    onChange={v => setEditCreds(prev => ({ ...prev, [f.id]: v }))}
                    type="text"
                    placeholder={f.placeholder ?? ''}
                  />
                </div>
              ))}
              <div className="flex gap-2 pt-2">
                <button type="button" onClick={() => setEditConfig(null)} className="btn-ghost btn-sm flex-1 justify-center">Cancel</button>
                <button type="submit" className="btn-brand btn-sm flex-1 justify-center">
                  <Save className="w-3.5 h-3.5" /> Save
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* ── .env Preview Modal ──────────────────── */}
      {envPreview && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm">
          <div className="bg-bg-surface border border-border rounded-2xl w-full max-w-[500px] shadow-2xl">
            <div className="flex items-center justify-between p-5 border-b border-border">
              <h2 className="text-[14px] font-semibold text-text-bright">.env Preview</h2>
              <button onClick={() => setEnvPreview(null)} className="btn-ghost btn-xs p-1"><X className="w-4 h-4" /></button>
            </div>
            <div className="p-5 space-y-3">
              {envPreview.warning && (
                <div className="flex items-start gap-2 text-[11px] text-amber-400 border border-amber-400/20 bg-amber-400/5 rounded p-3">
                  <AlertTriangle className="w-3.5 h-3.5 shrink-0 mt-0.5" />
                  {envPreview.warning}
                </div>
              )}
              <pre className="bg-bg-base rounded-lg p-4 text-[11px] font-mono text-text-sec overflow-x-auto whitespace-pre-wrap">
                {envPreview.content}
              </pre>
              <div className="flex gap-2">
                <button
                  onClick={() => { navigator.clipboard.writeText(envPreview.content); toast('Copied to clipboard', 'success') }}
                  className="btn-ghost btn-sm gap-1 flex-1 justify-center"
                >
                  <Copy className="w-3.5 h-3.5" /> Copy
                </button>
                <button onClick={() => setEnvPreview(null)} className="btn-ghost btn-sm gap-1 flex-1 justify-center">
                  Close
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </>
  )
}

// ── Market Data Section ─────────────────────────
function MarketDataSection() {
  const { settings, update } = useSettingsStore()
  return (
    <>
      <SectionHeader title="Market Data" description="Configure real-time data feed and subscriptions" />
      <CardBox>
        <FieldRow label="Data Source">
          <select
            value={settings.dataSource}
            onChange={e => update({ dataSource: e.target.value as any })}
            className="input-base w-full text-[12px]"
          >
            <option value="broker">From Active Broker</option>
            <option value="fyers">Fyers Data API</option>
            <option value="websocket">Custom WebSocket</option>
          </select>
        </FieldRow>
        <FieldRow label="Refresh Rate (ms)">
          <select className="input-base w-full text-[12px]" defaultValue="500">
            {[200, 500, 1000, 2000, 5000].map(v => (
              <option key={v} value={v}>{v}ms</option>
            ))}
          </select>
        </FieldRow>
        <FieldRow label="Watchlist Max Items">
          <select className="input-base w-full text-[12px]" defaultValue="50">
            {[25, 50, 100, 200].map(v => <option key={v}>{v}</option>)}
          </select>
        </FieldRow>
        <FieldRow label="Index Data">
          <div className="flex flex-wrap gap-2">
            {['NIFTY 50', 'NIFTY BANK', 'SENSEX', 'FINNIFTY'].map(i => (
              <label key={i} className="flex items-center gap-1.5 text-[11px] text-text-sec">
                <input type="checkbox" defaultChecked className="accent-brand w-3 h-3" /> {i}
              </label>
            ))}
          </div>
        </FieldRow>
      </CardBox>
    </>
  )
}

// ── Option Chain Data Section ───────────────────
function OptionDataSection() {
  return (
    <>
      <SectionHeader title="Option Chain Data" description="Manage option chain data sources and caching" />
      <CardBox>
        <FieldRow label="Data Provider">
          <select className="input-base w-full text-[12px]" defaultValue="nse">
            <option value="nse">NSE Official API</option>
            <option value="broker">From Broker</option>
            <option value="sensibull">Sensibull API</option>
            <option value="opstra">Opstra API</option>
          </select>
        </FieldRow>
        <FieldRow label="Refresh Interval">
          <select className="input-base w-full text-[12px]" defaultValue="5000">
            {[1000, 3000, 5000, 10000, 30000].map(v => (
              <option key={v} value={v}>{v / 1000}s</option>
            ))}
          </select>
        </FieldRow>
        <FieldRow label="Strike Range">
          <select className="input-base w-full text-[12px]" defaultValue="20">
            {[10, 15, 20, 25, 30].map(v => <option key={v}>{v} strikes each side</option>)}
          </select>
        </FieldRow>
        <FieldRow label="Expiry Default">
          <select className="input-base w-full text-[12px]" defaultValue="nearest">
            <option value="nearest">Nearest Weekly</option>
            <option value="monthly">Nearest Monthly</option>
            <option value="next">Next Weekly</option>
          </select>
        </FieldRow>
        <FieldRow label="Cache Data">
          <label className="flex items-center gap-2 text-[12px] text-text-sec">
            <input type="checkbox" defaultChecked className="accent-brand w-3.5 h-3.5" />
            Cache option chain for offline view
          </label>
        </FieldRow>
      </CardBox>
    </>
  )
}

// ── Theme Section ───────────────────────────────
function ThemeSection() {
  const { settings, update } = useSettingsStore()
  const toast = useToastStore(s => s.toast)

  const THEMES: { id: string; label: string; desc: string; bg: string; surface: string; brand: string }[] = [
    {
      id: 'dark', label: 'Terminal Dark', desc: 'Default trading theme',
      bg: '#0b0e17', surface: '#111520', brand: '#22d3ee',
    },
    {
      id: 'midnight', label: 'Deep Navy', desc: 'Deeper navy blue',
      bg: '#080c16', surface: '#0d1120', brand: '#22d3ee',
    },
    {
      id: 'charcoal', label: 'Charcoal', desc: 'Warm grey theme',
      bg: '#181818', surface: '#1f1f1f', brand: '#22d3ee',
    },
    {
      id: 'light', label: 'Day Light', desc: 'Clean professional light',
      bg: '#f1f5f9', surface: '#ffffff', brand: '#0284c7',
    },
    {
      id: 'ocean', label: 'Ocean Depth', desc: 'Deep ocean dark',
      bg: '#030712', surface: '#060e20', brand: '#38bdf8',
    },
  ]

  return (
    <>
      <SectionHeader title="Theme & Display" description="Customize the look and feel of your terminal" />
      <CardBox>
        <div className="text-[11px] text-text-muted mb-3 font-medium uppercase tracking-wider">Color Theme</div>
        <div className="grid grid-cols-2 sm:grid-cols-3 gap-3 mb-4">
          {THEMES.map(t => (
            <button
              key={t.id}
              onClick={() => { update({ theme: t.id as any }); toast(`Theme: ${t.label}`, 'success') }}
              className={cn(
                'flex flex-col gap-2 p-3 rounded-xl border-2 transition-all hover:scale-[1.02]',
                settings.theme === t.id
                  ? 'border-brand shadow-brand/20 shadow-md'
                  : 'border-border hover:border-text-muted/40'
              )}
            >
              {/* Theme preview swatch */}
              <div
                className="w-full h-10 rounded-lg overflow-hidden flex"
                style={{ background: t.bg }}
              >
                <div className="flex-1 flex flex-col justify-between p-1.5">
                  <div className="h-1.5 rounded-full w-3/4" style={{ background: t.brand }} />
                  <div className="h-1 rounded-full w-1/2 opacity-40" style={{ background: t.surface === '#ffffff' ? '#475569' : '#c8cdd8' }} />
                </div>
                <div className="w-8 h-full" style={{ background: t.surface, borderLeft: `1px solid ${t.bg}` }} />
              </div>
              <div className="text-left">
                <div className="text-[11px] font-semibold text-text-bright">{t.label}</div>
                <div className="text-[10px] text-text-muted">{t.desc}</div>
              </div>
              {settings.theme === t.id && (
                <div className="self-start text-[9px] font-bold uppercase tracking-widest text-brand bg-brand/10 px-2 py-0.5 rounded">
                  Active
                </div>
              )}
            </button>
          ))}
        </div>

        <FieldRow label="Font Size">
          <select
            value={settings.fontSize}
            onChange={e => update({ fontSize: e.target.value as any })}
            className="input-base w-full text-[12px]"
          >
            <option value="small">Small (11px)</option>
            <option value="medium">Medium (13px)</option>
            <option value="large">Large (15px)</option>
          </select>
        </FieldRow>
        <FieldRow label="Density">
          <select
            value={settings.density}
            onChange={e => update({ density: e.target.value as any })}
            className="input-base w-full text-[12px]"
          >
            <option value="compact">Compact</option>
            <option value="normal">Normal</option>
            <option value="comfortable">Comfortable</option>
          </select>
        </FieldRow>
        <FieldRow label="Sound Alerts">
          <label className="flex items-center gap-2 text-[12px] text-text-sec">
            <input
              type="checkbox"
              checked={settings.soundAlerts}
              onChange={e => update({ soundAlerts: e.target.checked })}
              className="accent-brand w-3.5 h-3.5"
            />
            Play sound on order fill / rejection
          </label>
        </FieldRow>
        <FieldRow label="Notifications">
          <label className="flex items-center gap-2 text-[12px] text-text-sec">
            <input
              type="checkbox"
              checked={settings.notifications}
              onChange={e => update({ notifications: e.target.checked })}
              className="accent-brand w-3.5 h-3.5"
            />
            Browser push notifications
          </label>
        </FieldRow>
      </CardBox>
    </>
  )
}

// ── Copy Trade Section ──────────────────────────
function CopyTradeSection() {
  const { accounts } = useAuthStore()
  const toast = useToastStore(s => s.toast)
  const [links, setLinks] = useState([
    { id: '1', masterId: 'acc1', followerId: 'acc2', multiplier: 1.0, enabled: true, mode: 'fixed' as const },
  ])

  const addLink = () => {
    setLinks(l => [...l, { id: Date.now().toString(), masterId: '', followerId: '', multiplier: 1, enabled: true, mode: 'fixed' as const }])
  }

  return (
    <>
      <SectionHeader title="Copy Trading" description="Mirror trades from a master account to one or more follower accounts" />

      <div className="bg-warning/10 border border-warning/30 rounded-lg p-3 mb-4 flex items-start gap-2.5">
        <AlertTriangle className="w-4 h-4 text-warning shrink-0 mt-0.5" />
        <p className="text-[11px] text-warning/90">
          Copy trading executes orders simultaneously. Ensure follower accounts have sufficient margin before enabling.
        </p>
      </div>

      {links.map((lnk, idx) => (
        <CardBox key={lnk.id} className="mb-3">
          <div className="flex items-center justify-between mb-3">
            <span className="text-[12px] font-medium text-text-sec">Copy Link #{idx + 1}</span>
            <div className="flex items-center gap-2">
              <label className="flex items-center gap-1.5 text-[11px] text-text-sec">
                <input
                  type="checkbox"
                  checked={lnk.enabled}
                  onChange={e => setLinks(l => l.map((x, i) => i === idx ? { ...x, enabled: e.target.checked } : x))}
                  className="accent-brand"
                /> Active
              </label>
              <button
                onClick={() => setLinks(l => l.filter((_, i) => i !== idx))}
                className="btn-ghost btn-xs hover:text-loss"
              >
                <Trash2 className="w-3 h-3" />
              </button>
            </div>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div>
              <div className="text-[10px] text-text-muted mb-1">Master Account</div>
              <select className="input-base w-full text-[11px]"
                value={lnk.masterId}
                onChange={e => setLinks(l => l.map((x, i) => i === idx ? { ...x, masterId: e.target.value } : x))}
              >
                <option value="">Select master…</option>
                {accounts.map(a => <option key={a.id} value={a.id}>{a.broker} — {a.clientId}</option>)}
              </select>
            </div>
            <div>
              <div className="text-[10px] text-text-muted mb-1">Follower Account</div>
              <select className="input-base w-full text-[11px]"
                value={lnk.followerId}
                onChange={e => setLinks(l => l.map((x, i) => i === idx ? { ...x, followerId: e.target.value } : x))}
              >
                <option value="">Select follower…</option>
                {accounts.map(a => <option key={a.id} value={a.id}>{a.broker} — {a.clientId}</option>)}
              </select>
            </div>
          </div>
          <div className="grid grid-cols-2 gap-3 mt-3">
            <div>
              <div className="text-[10px] text-text-muted mb-1">Lot Multiplier</div>
              <input
                type="number"
                min={0.1}
                max={10}
                step={0.1}
                value={lnk.multiplier}
                onChange={e => setLinks(l => l.map((x, i) => i === idx ? { ...x, multiplier: +e.target.value } : x))}
                className="input-base w-full text-[11px]"
              />
            </div>
            <div>
              <div className="text-[10px] text-text-muted mb-1">Mode</div>
              <select
                value={lnk.mode}
                onChange={e => setLinks(l => l.map((x, i) => i === idx ? { ...x, mode: e.target.value as any } : x))}
                className="input-base w-full text-[11px]"
              >
                <option value="fixed">Fixed Lots</option>
                <option value="ratio">Margin Ratio</option>
                <option value="mirror">Exact Mirror</option>
              </select>
            </div>
          </div>
        </CardBox>
      ))}

      <button onClick={addLink} className="btn-ghost btn-sm border border-dashed border-border w-full justify-center mb-4">
        <Plus className="w-3.5 h-3.5" /> Add Copy Link
      </button>
      <div className="flex justify-end">
        <button onClick={() => toast('Copy trade settings saved', 'success')} className="btn-brand btn-sm">
          <Save className="w-3.5 h-3.5" /> Save
        </button>
      </div>
    </>
  )
}

// ── Webhook Section ─────────────────────────────
function WebhookSection() {
  const toast = useToastStore(s => s.toast)
  const WEBHOOK_URL = `${window.location.origin}/api/webhook/inbound`
  const TOKEN = 'wh_' + 'demo00001aabbccdd'

  return (
    <>
      <SectionHeader title="Webhooks" description="Receive trade signals from TradingView, Chartink, or your own scripts" />

      <CardBox className="mb-4">
        <div className="text-[12px] font-medium text-text-sec mb-3">Inbound Webhook URL</div>
        <div className="flex items-center gap-2">
          <code className="flex-1 bg-bg-elevated border border-border rounded px-3 py-2 text-[11px] text-brand font-mono truncate">
            {WEBHOOK_URL}
          </code>
          <button
            onClick={() => { navigator.clipboard?.writeText(WEBHOOK_URL); toast('URL copied', 'info') }}
            className="btn-ghost btn-xs"
          >Copy</button>
        </div>
        <div className="mt-3">
          <div className="text-[12px] font-medium text-text-sec mb-1">Auth Token</div>
          <div className="flex items-center gap-2">
            <code className="flex-1 bg-bg-elevated border border-border rounded px-3 py-2 text-[11px] text-accent font-mono truncate">
              {TOKEN}
            </code>
            <button
              onClick={() => { navigator.clipboard?.writeText(TOKEN); toast('Token copied', 'info') }}
              className="btn-ghost btn-xs"
            >Copy</button>
          </div>
        </div>
      </CardBox>

      <CardBox className="mb-4">
        <div className="text-[12px] font-medium text-text-sec mb-3">TradingView Alert JSON Template</div>
        <pre className="bg-bg-elevated border border-border rounded p-3 text-[10px] text-text-sec font-mono overflow-x-auto">{`{
  "token": "${TOKEN}",
  "action": "{{strategy.order.action}}",
  "symbol": "{{ticker}}",
  "qty": {{strategy.order.contracts}},
  "price": {{close}},
  "order_type": "MARKET",
  "product": "MIS"
}`}</pre>
      </CardBox>

      <CardBox>
        <div className="text-[12px] font-medium text-text-sec mb-3">Webhook Settings</div>
        <FieldRow label="Auto Execute">
          <label className="flex items-center gap-2 text-[12px] text-text-sec">
            <input type="checkbox" className="accent-brand w-3.5 h-3.5" defaultChecked />
            Automatically place orders from webhook signals
          </label>
        </FieldRow>
        <FieldRow label="Source Account">
          <select className="input-base w-full text-[12px]">
            <option>All Accounts</option>
            <option>Zerodha — KD0001</option>
          </select>
        </FieldRow>
        <FieldRow label="Risk Check">
          <label className="flex items-center gap-2 text-[12px] text-text-sec">
            <input type="checkbox" className="accent-brand w-3.5 h-3.5" defaultChecked />
            Apply risk limits to webhook orders
          </label>
        </FieldRow>
      </CardBox>
    </>
  )
}

// ── Diagnostics Section ─────────────────────────
type DiagCall = 'profile' | 'positions' | 'orderbook' | 'funds' | 'holdings' | 'tradebook'
const DIAG_CALLS: { key: DiagCall; label: string }[] = [
  { key: 'profile',    label: 'Profile' },
  { key: 'funds',      label: 'Funds' },
  { key: 'positions',  label: 'Positions' },
  { key: 'orderbook',  label: 'Order Book' },
  { key: 'holdings',   label: 'Holdings' },
  { key: 'tradebook',  label: 'Tradebook' },
]

function DiagnosticsSection() {
  const toast = useToastStore(s => s.toast)

  // System health checks
  const [health, setHealth] = useState<{ label: string; status: 'ok' | 'warn' | 'error'; detail: string }[]>([])
  const [healthLoading, setHealthLoading] = useState(false)

  // Broker accounts
  const [sessions, setSessions] = useState<any[]>([])
  const [sessionsLoading, setSessionsLoading] = useState(false)

  // Per-account diagnostic state
  const [activeConfig, setActiveConfig] = useState<string | null>(null)
  const [diagCall, setDiagCall] = useState<DiagCall>('funds')
  const [diagResult, setDiagResult] = useState<any>(null)
  const [diagRunning, setDiagRunning] = useState(false)

  // Load sessions on mount
  useEffect(() => {
    loadSessions()
    runHealthChecks()
  }, [])

  const loadSessions = async () => {
    setSessionsLoading(true)
    try {
      const data = await api.allSessions()
      setSessions(Array.isArray(data) ? data : [])
    } catch {
      setSessions([])
    } finally {
      setSessionsLoading(false)
    }
  }

  const runHealthChecks = async () => {
    setHealthLoading(true)
    const checks: typeof health = []
    const t0 = Date.now()
    try {
      const res = await fetch('/api/health')
      const ms = Date.now() - t0
      if (res.ok) {
        const body = await res.json()
        checks.push({ label: 'Backend API', status: 'ok', detail: `Responding in ${ms}ms · mode=${body.mode}` })
        checks.push({
          label: 'Broker Sessions',
          status: (body.active_accounts ?? 0) > 0 ? 'ok' : 'warn',
          detail: `${body.active_accounts ?? 0} account(s) connected`,
        })
      } else {
        checks.push({ label: 'Backend API', status: 'error', detail: `HTTP ${res.status}` })
      }
    } catch (e: any) {
      checks.push({ label: 'Backend API', status: 'error', detail: String(e) })
    }
    setHealth(checks)
    setHealthLoading(false)
  }

  const runDiagnose = async () => {
    if (!activeConfig) { toast('Select a broker account first', 'error'); return }
    setDiagRunning(true)
    setDiagResult(null)
    try {
      const res = await api.brokerDiagnose(activeConfig, diagCall)
      setDiagResult(res)
    } catch (e: any) {
      setDiagResult({ ok: false, error: String(e), data: null })
    } finally {
      setDiagRunning(false)
    }
  }

  const STATUS_ICON: Record<string, React.ReactNode> = {
    ok:    <CheckCircle className="w-4 h-4 text-profit shrink-0" />,
    warn:  <AlertTriangle className="w-4 h-4 text-warning shrink-0" />,
    error: <XCircle className="w-4 h-4 text-loss shrink-0" />,
  }
  const STATUS_BADGE: Record<string, string> = { ok: 'badge-green', warn: 'badge-yellow', error: 'badge-red' }

  const activeSess = sessions.find(s => s.config_id === activeConfig)

  return (
    <>
      <SectionHeader title="Diagnostics" description="System health and raw broker API inspector" />

      {/* ── System Health ── */}
      <CardBox>
        <div className="flex items-center justify-between mb-3">
          <span className="text-[12px] font-semibold text-text-bright">System Health</span>
          <button
            onClick={runHealthChecks}
            disabled={healthLoading}
            className="btn-ghost btn-xs flex items-center gap-1"
          >
            <RefreshCw className={cn('w-3 h-3', healthLoading && 'animate-spin')} />
            Refresh
          </button>
        </div>
        <div className="space-y-2.5">
          {health.length === 0 && healthLoading && (
            <div className="text-[11px] text-text-muted flex items-center gap-2">
              <Loader2 className="w-3.5 h-3.5 animate-spin" /> Checking…
            </div>
          )}
          {health.map(c => (
            <div key={c.label} className="flex items-center gap-3">
              {STATUS_ICON[c.status]}
              <div className="flex-1 min-w-0">
                <div className="text-[12px] font-medium text-text-sec">{c.label}</div>
                <div className="text-[10px] text-text-muted truncate">{c.detail}</div>
              </div>
              <span className={cn('badge', STATUS_BADGE[c.status])}>{c.status}</span>
            </div>
          ))}
        </div>
      </CardBox>

      {/* ── Broker API Inspector ── */}
      <CardBox className="mt-4">
        <div className="flex items-center justify-between mb-4">
          <span className="text-[12px] font-semibold text-text-bright flex items-center gap-2">
            <Terminal className="w-3.5 h-3.5 text-brand" />
            Broker API Inspector
          </span>
          <button onClick={loadSessions} disabled={sessionsLoading} className="btn-ghost btn-xs flex items-center gap-1">
            <RefreshCw className={cn('w-3 h-3', sessionsLoading && 'animate-spin')} />
            Reload
          </button>
        </div>

        {/* Account selector */}
        {sessions.length === 0 ? (
          <div className="text-[11px] text-text-muted py-4 text-center">
            {sessionsLoading ? (
              <span className="flex items-center justify-center gap-2"><Loader2 className="w-3.5 h-3.5 animate-spin" />Loading sessions…</span>
            ) : (
              <>No broker accounts connected. <a href="/app/settings/brokers" className="text-brand hover:underline">Connect one →</a></>
            )}
          </div>
        ) : (
          <div className="space-y-4">
            {/* Step 1 — choose account */}
            <div>
              <div className="text-[10px] text-text-muted mb-1.5 uppercase font-medium tracking-wide">1. Select Account</div>
              <div className="flex flex-wrap gap-2">
                {sessions.map(s => (
                  <button
                    key={s.config_id}
                    onClick={() => { setActiveConfig(s.config_id); setDiagResult(null) }}
                    className={cn(
                      'flex items-center gap-2 px-3 py-1.5 rounded-lg border text-[11px] font-medium transition-all',
                      activeConfig === s.config_id
                        ? 'bg-brand/15 border-brand text-brand'
                        : 'bg-bg-elevated border-border text-text-sec hover:border-brand/50'
                    )}
                  >
                    <span className={cn('w-2 h-2 rounded-full shrink-0', s.is_live ? 'bg-profit' : 'bg-warning')} />
                    {s.client_id}
                    <span className="text-[9px] opacity-60">{(s.broker_id || '').toUpperCase()}</span>
                  </button>
                ))}
              </div>
            </div>

            {activeSess && (
              <>
                {/* Session status */}
                <div className="flex items-center gap-3 bg-bg-elevated rounded-lg px-3 py-2 text-[11px]">
                  <span className="text-text-muted">Mode:</span>
                  <span className={cn('font-semibold', activeSess.is_live ? 'text-profit' : 'text-warning')}>{activeSess.mode?.toUpperCase()}</span>
                  {activeSess.connected_at && (
                    <><span className="text-text-muted ml-2">Connected:</span>
                    <span className="text-text-sec">{new Date(activeSess.connected_at).toLocaleString('en-IN', { dateStyle: 'short', timeStyle: 'short' })}</span></>
                  )}
                  {activeSess.error && (
                    <span className="text-loss ml-auto flex items-center gap-1"><AlertTriangle className="w-3 h-3" />{activeSess.error}</span>
                  )}
                </div>

                {/* Step 2 — choose call */}
                <div>
                  <div className="text-[10px] text-text-muted mb-1.5 uppercase font-medium tracking-wide">2. Select API Call</div>
                  <div className="flex flex-wrap gap-1.5">
                    {DIAG_CALLS.map(c => (
                      <button
                        key={c.key}
                        onClick={() => { setDiagCall(c.key); setDiagResult(null) }}
                        className={cn(
                          'px-3 py-1 rounded text-[11px] font-medium border transition-all',
                          diagCall === c.key
                            ? 'bg-brand text-white border-brand'
                            : 'bg-bg-elevated border-border text-text-sec hover:border-brand/40'
                        )}
                      >
                        {c.label}
                      </button>
                    ))}
                  </div>
                </div>

                {/* Step 3 — run */}
                <div>
                  <div className="text-[10px] text-text-muted mb-1.5 uppercase font-medium tracking-wide">3. Run</div>
                  <button
                    onClick={runDiagnose}
                    disabled={diagRunning}
                    className="btn-brand btn-sm flex items-center gap-2"
                  >
                    {diagRunning
                      ? <><Loader2 className="w-3.5 h-3.5 animate-spin" />Calling {diagCall}…</>
                      : <><Terminal className="w-3.5 h-3.5" />Call {DIAG_CALLS.find(c => c.key === diagCall)?.label}</>
                    }
                  </button>
                </div>
              </>
            )}

            {/* Output */}
            {diagResult && (
              <div className="mt-2">
                <div className="flex items-center justify-between mb-1.5">
                  <div className="text-[10px] uppercase text-text-muted font-medium tracking-wide flex items-center gap-2">
                    Raw Response
                    {diagResult.ok
                      ? <span className="badge badge-green">OK · {diagResult.elapsed_ms}ms</span>
                      : <span className="badge badge-red">ERROR</span>
                    }
                  </div>
                  <button
                    className="btn-ghost btn-xs"
                    onClick={() => {
                      navigator.clipboard.writeText(JSON.stringify(diagResult, null, 2))
                      toast('Copied to clipboard', 'success')
                    }}
                  >
                    Copy
                  </button>
                </div>
                <pre className="bg-bg-elevated border border-border rounded-lg p-3 text-[10px] text-text-sec overflow-auto max-h-96 font-mono leading-relaxed">
                  {JSON.stringify(diagResult, null, 2)}
                </pre>
              </div>
            )}
          </div>
        )}
      </CardBox>
    </>
  )
}
