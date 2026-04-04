/* ═══════════════════════════════════════════════
   Admin Page — user management only
   ═══════════════════════════════════════════════ */
import { useState, useEffect, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Users, Shield, Plus, Pencil, Trash2,
  RefreshCw, KeyRound, X, CheckCircle,
  XCircle, Loader2, Eye, EyeOff, LogOut,
  Database, Wifi, WifiOff, RotateCcw,
} from 'lucide-react'
import { useAuthStore, useToastStore } from '../stores'
import { api } from '../lib/api'

interface AdminUser {
  id: string; name: string; email: string; role: string
  is_active: boolean; phone?: string; created_at: string
}

function Badge({ role }: { role: string }) {
  const map: Record<string, string> = {
    admin:  'bg-brand/15 text-brand border-brand/20',
    user:   'bg-profit/10 text-profit border-profit/20',
    viewer: 'bg-amber-500/10 text-amber-400 border-amber-400/20',
  }
  return (
    <span className={`px-2 py-0.5 rounded text-[10px] border font-medium ${map[role] ?? 'bg-text-muted/10 text-text-muted'}`}>
      {role}
    </span>
  )
}

/* ── Add / Edit User Modal ──────────────────────── */
function UserModal({ user, onClose, onSave }: {
  user: AdminUser | null; onClose: () => void; onSave: () => void
}) {
  const { toast } = useToastStore()
  const [name,     setName]     = useState(user?.name ?? '')
  const [email,    setEmail]    = useState(user?.email ?? '')
  const [role,     setRole]     = useState(user?.role ?? 'user')
  const [phone,    setPhone]    = useState(user?.phone ?? '')
  const [password, setPassword] = useState('')
  const [showPw,   setShowPw]   = useState(false)
  const [active,   setActive]   = useState(user?.is_active ?? true)
  const [loading,  setLoading]  = useState(false)

  async function handleSave(e: React.FormEvent) {
    e.preventDefault()
    setLoading(true)
    try {
      if (user) {
        await api.adminUpdateUser(user.id, { name, email, role, is_active: active, phone: phone || undefined })
        toast('User updated', 'success')
      } else {
        if (!password) { toast('Password required', 'error'); setLoading(false); return }
        await api.adminCreateUser({ email, name, password, role, phone: phone || undefined })
        toast('User created', 'success')
      }
      onSave(); onClose()
    } catch (err: any) {
      toast(err?.message ?? 'Save failed', 'error')
    } finally { setLoading(false) }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm">
      <div className="bg-bg-surface border border-border rounded-2xl w-full max-w-[420px] shadow-2xl">
        <div className="flex items-center justify-between p-5 border-b border-border">
          <h2 className="text-[14px] font-semibold text-text-bright">{user ? 'Edit User' : 'Add User'}</h2>
          <button onClick={onClose} className="btn-ghost btn-xs p-1"><X className="w-4 h-4" /></button>
        </div>
        <form onSubmit={handleSave} className="p-5 space-y-3">
          <div>
            <label className="block text-[11px] text-text-muted mb-1">Full Name</label>
            <input type="text" value={name} onChange={e => setName(e.target.value)} className="input-base w-full" required />
          </div>
          <div>
            <label className="block text-[11px] text-text-muted mb-1">Email</label>
            <input type="email" value={email} onChange={e => setEmail(e.target.value)} className="input-base w-full" required />
          </div>
          <div>
            <label className="block text-[11px] text-text-muted mb-1">Phone (optional)</label>
            <input type="tel" value={phone} onChange={e => setPhone(e.target.value)} className="input-base w-full" placeholder="+91 9999000000" />
          </div>
          <div>
            <label className="block text-[11px] text-text-muted mb-1">Role</label>
            <select value={role} onChange={e => setRole(e.target.value)} className="input-base w-full">
              <option value="user">User</option>
              <option value="admin">Admin</option>
              <option value="viewer">Viewer (read-only)</option>
            </select>
          </div>
          {!user && (
            <div>
              <label className="block text-[11px] text-text-muted mb-1">Password</label>
              <div className="relative">
                <input type={showPw ? 'text' : 'password'} value={password}
                  onChange={e => setPassword(e.target.value)} className="input-base w-full pr-9" required />
                <button type="button" onClick={() => setShowPw(!showPw)} className="absolute right-2 top-1/2 -translate-y-1/2 text-text-muted">
                  {showPw ? <EyeOff className="w-3.5 h-3.5" /> : <Eye className="w-3.5 h-3.5" />}
                </button>
              </div>
            </div>
          )}
          {user && (
            <label className="flex items-center gap-2 text-[12px] text-text-sec cursor-pointer">
              <input type="checkbox" checked={active} onChange={e => setActive(e.target.checked)} className="accent-brand w-3.5 h-3.5" />
              Account active
            </label>
          )}
          <div className="flex gap-2 pt-2">
            <button type="button" onClick={onClose} className="btn-ghost btn-sm flex-1 justify-center">Cancel</button>
            <button type="submit" disabled={loading} className="btn-primary btn-sm flex-1 justify-center">
              {loading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : 'Save'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

/* ── Reset Password Modal ───────────────────────── */
function ResetPasswordModal({ user, onClose }: { user: AdminUser; onClose: () => void }) {
  const { toast } = useToastStore()
  const [pw, setPw]           = useState('')
  const [show, setShow]       = useState(false)
  const [loading, setLoading] = useState(false)

  async function handleReset(e: React.FormEvent) {
    e.preventDefault()
    if (pw.length < 6) { toast('Minimum 6 characters', 'error'); return }
    setLoading(true)
    try {
      await api.adminResetPassword(user.id, pw)
      toast('Password reset successfully', 'success')
      onClose()
    } catch (err: any) {
      toast(err?.message ?? 'Reset failed', 'error')
    } finally { setLoading(false) }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm">
      <div className="bg-bg-surface border border-border rounded-2xl w-full max-w-[360px] shadow-2xl p-5 space-y-4">
        <div className="flex items-center justify-between">
          <h2 className="text-[14px] font-semibold text-text-bright">Reset password — {user.name}</h2>
          <button onClick={onClose} className="btn-ghost btn-xs p-1"><X className="w-4 h-4" /></button>
        </div>
        <form onSubmit={handleReset} className="space-y-4">
          <div className="relative">
            <input type={show ? 'text' : 'password'} value={pw} onChange={e => setPw(e.target.value)}
              placeholder="New password (min 6 chars)" className="input-base w-full pr-9" required />
            <button type="button" onClick={() => setShow(!show)} className="absolute right-2 top-1/2 -translate-y-1/2 text-text-muted">
              {show ? <EyeOff className="w-3.5 h-3.5" /> : <Eye className="w-3.5 h-3.5" />}
            </button>
          </div>
          <div className="flex gap-2">
            <button type="button" onClick={onClose} className="btn-ghost btn-sm flex-1 justify-center">Cancel</button>
            <button type="submit" disabled={loading} className="btn-primary btn-sm flex-1 justify-center">
              {loading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <><KeyRound className="w-3.5 h-3.5" /> Reset</>}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

/* ════════════════════════════════════════════════
   Main AdminPage
   ════════════════════════════════════════════════ */
// ── Data Sources Section ─────────────────────────
function DataSourcesSection() {
  const { toast } = useToastStore()
  const [status, setStatus] = useState<{ live: boolean; configured: boolean; app_id: string | null } | null>(null)
  const [reloading, setReloading] = useState(false)

  const loadStatus = useCallback(async () => {
    try {
      const data = await (api as any).fyersStatus() as any
      setStatus(data)
    } catch { /* silent */ }
  }, [])

  useEffect(() => { loadStatus() }, [])

  async function handleReload() {
    setReloading(true)
    try {
      await (api as any).fyersReload()
      await loadStatus()
      toast('Fyers client reloaded', 'success')
    } catch (e: any) {
      toast(e?.message ?? 'Reload failed', 'error')
    } finally {
      setReloading(false)
    }
  }

  return (
    <div className="mt-8">
      <div className="flex items-center gap-2 mb-4">
        <Database className="w-4 h-4 text-text-sec" />
        <span className="text-[14px] font-semibold">Data Sources</span>
      </div>

      <div className="bg-bg-surface border border-border rounded-xl overflow-hidden">
        <div className="px-5 py-4 border-b border-border flex items-center justify-between">
          <div className="flex items-center gap-2">
            <span className="text-[13px] font-semibold text-text-bright">Fyers API</span>
            <span className="text-[10px] text-text-muted">Market data provider (indices, option chain, commodities)</span>
          </div>
          <button
            onClick={handleReload}
            disabled={reloading}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-border text-[12px] text-text-muted hover:text-text-sec transition-colors disabled:opacity-50"
          >
            <RotateCcw className={`w-3.5 h-3.5 ${reloading ? 'animate-spin' : ''}`} />
            Reload Token
          </button>
        </div>
        <div className="px-5 py-4 grid grid-cols-1 sm:grid-cols-3 gap-4">
          {/* Status */}
          <div className="space-y-1">
            <div className="text-[10px] text-text-muted uppercase tracking-wider">Status</div>
            {status ? (
              <div className={`flex items-center gap-1.5 text-[13px] font-semibold ${status.live ? 'text-profit' : 'text-loss'}`}>
                {status.live
                  ? <><Wifi className="w-4 h-4" /> Live</>
                  : <><WifiOff className="w-4 h-4" /> Offline</>
                }
              </div>
            ) : (
              <div className="flex items-center gap-1.5 text-text-muted text-[12px]">
                <Loader2 className="w-3.5 h-3.5 animate-spin" /> Checking…
              </div>
            )}
          </div>
          {/* App ID */}
          <div className="space-y-1">
            <div className="text-[10px] text-text-muted uppercase tracking-wider">App ID</div>
            <div className="text-[13px] font-mono text-text-bright">
              {status?.app_id ?? <span className="text-text-muted">Not configured</span>}
            </div>
          </div>
          {/* Token file */}
          <div className="space-y-1">
            <div className="text-[10px] text-text-muted uppercase tracking-wider">Token Source</div>
            <div className="text-[12px] text-text-sec font-mono">
              {status?.configured
                ? 'Auto-loaded from token file'
                : <span className="text-text-muted">Set FYERS_APP_ID in .env</span>
              }
            </div>
          </div>
        </div>
        {!status?.live && (
          <div className="px-5 py-3 border-t border-border bg-amber-500/5 text-[11px] text-amber-400">
            Fyers token may have expired. Run the Fyers auth flow and click "Reload Token" to reconnect.
          </div>
        )}
      </div>
    </div>
  )
}

export default function AdminPage() {
  const navigate  = useNavigate()
  const { user, logout } = useAuthStore()
  const { toast } = useToastStore()

  const [users,         setUsers]         = useState<AdminUser[]>([])
  const [loading,       setLoading]       = useState(false)
  const [editUser,      setEditUser]      = useState<AdminUser | null | 'new'>()
  const [resetUser,     setResetUser]     = useState<AdminUser | null>(null)
  const [deleteConfirm, setDeleteConfirm] = useState<string | null>(null)

  useEffect(() => {
    if (user && user.role !== 'admin') navigate('/', { replace: true })
  }, [user])

  const loadUsers = useCallback(async () => {
    setLoading(true)
    try { setUsers(await api.adminUsers()) }
    catch (e: any) { toast(e?.message ?? 'Failed to load users', 'error') }
    finally { setLoading(false) }
  }, [])

  useEffect(() => { loadUsers() }, [])

  async function handleDelete(id: string) {
    try {
      await api.adminDeleteUser(id)
      setDeleteConfirm(null)
      toast('User deleted', 'success')
      loadUsers()
    } catch (e: any) { toast(e?.message ?? 'Delete failed', 'error') }
  }

  function handleLogout() {
    logout()
    navigate('/', { replace: true })
  }

  return (
    <div className="min-h-screen bg-bg-base text-text-bright">

      {/* Header */}
      <div className="border-b border-border bg-bg-surface sticky top-0 z-40">
        <div className="px-4 h-14 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Shield className="w-4 h-4 text-brand" />
            <span className="text-[14px] font-semibold">Admin Panel</span>
            <span className="text-[11px] text-text-muted ml-2">Smart<span className="text-brand">Trader</span></span>
          </div>
          <div className="flex items-center gap-3">
            <span className="text-[11px] text-text-muted hidden sm:block">{user?.name}</span>
            <button
              onClick={handleLogout}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-border text-[12px] text-text-muted hover:text-loss hover:border-loss/40 transition-colors"
            >
              <LogOut className="w-3.5 h-3.5" /> Sign out
            </button>
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="px-4 py-6">

        {/* Toolbar */}
        <div className="flex items-center justify-between mb-5">
          <div className="flex items-center gap-2">
            <Users className="w-4 h-4 text-text-sec" />
            <span className="text-[14px] font-semibold">Users</span>
            <span className="text-[11px] text-text-muted ml-1">{users.length} total</span>
          </div>
          <div className="flex gap-2">
            <button onClick={loadUsers} className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-border text-[12px] text-text-muted hover:text-text-sec transition-colors">
              <RefreshCw className="w-3.5 h-3.5" />
            </button>
            <button onClick={() => setEditUser('new')} className="btn-primary btn-sm gap-1.5">
              <Plus className="w-3.5 h-3.5" /> Add User
            </button>
          </div>
        </div>

        {/* Table */}
        <div className="bg-bg-surface border border-border rounded-xl overflow-hidden">
          {loading ? (
            <div className="flex items-center justify-center gap-2 py-12 text-text-muted text-[13px]">
              <Loader2 className="w-4 h-4 animate-spin" /> Loading users…
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-[12px]">
                <thead>
                  <tr className="border-b border-border text-text-muted text-left">
                    <th className="px-4 py-3 font-medium">Name</th>
                    <th className="px-4 py-3 font-medium">Email</th>
                    <th className="px-4 py-3 font-medium">Role</th>
                    <th className="px-4 py-3 font-medium">Status</th>
                    <th className="px-4 py-3 font-medium">Created</th>
                    <th className="px-4 py-3 font-medium text-right">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {users.map(u => (
                    <tr key={u.id} className="border-b border-border/50 last:border-0 hover:bg-bg-base/50">
                      <td className="px-4 py-3 font-medium text-text-bright">{u.name}</td>
                      <td className="px-4 py-3 text-text-sec">{u.email}</td>
                      <td className="px-4 py-3"><Badge role={u.role} /></td>
                      <td className="px-4 py-3">
                        {u.is_active
                          ? <span className="flex items-center gap-1 text-profit"><CheckCircle className="w-3 h-3" /> Active</span>
                          : <span className="flex items-center gap-1 text-loss"><XCircle className="w-3 h-3" /> Inactive</span>
                        }
                      </td>
                      <td className="px-4 py-3 text-text-muted">{new Date(u.created_at).toLocaleDateString('en-IN')}</td>
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-1 justify-end">
                          <button onClick={() => setEditUser(u)} className="btn-ghost btn-xs p-1.5" title="Edit"><Pencil className="w-3.5 h-3.5" /></button>
                          <button onClick={() => setResetUser(u)} className="btn-ghost btn-xs p-1.5" title="Reset password"><KeyRound className="w-3.5 h-3.5" /></button>
                          <button onClick={() => setDeleteConfirm(u.id)} className="btn-ghost btn-xs p-1.5 hover:text-loss" title="Delete"><Trash2 className="w-3.5 h-3.5" /></button>
                        </div>
                      </td>
                    </tr>
                  ))}
                  {users.length === 0 && (
                    <tr><td colSpan={6} className="px-4 py-10 text-center text-text-muted">No users found</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          )}
        </div>

        {/* ── Data Sources ─────────────────────── */}
        <DataSourcesSection />

      </div>

      {/* Delete confirm */}
      {deleteConfirm && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm">
          <div className="bg-bg-surface border border-border rounded-2xl w-full max-w-[340px] p-6 text-center shadow-2xl">
            <Trash2 className="w-9 h-9 text-loss mx-auto mb-4" />
            <div className="text-[14px] font-semibold text-text-bright mb-1">Delete this user?</div>
            <p className="text-[12px] text-text-muted mb-5">This cannot be undone.</p>
            <div className="flex gap-3">
              <button onClick={() => setDeleteConfirm(null)} className="btn-ghost btn-sm flex-1 justify-center">Cancel</button>
              <button onClick={() => handleDelete(deleteConfirm)} className="btn-sm flex-1 justify-center bg-loss/10 border border-loss/30 text-loss hover:bg-loss/20">Delete</button>
            </div>
          </div>
        </div>
      )}

      {editUser && (
        <UserModal user={editUser === 'new' ? null : editUser} onClose={() => setEditUser(undefined)} onSave={loadUsers} />
      )}
      {resetUser && (
        <ResetPasswordModal user={resetUser} onClose={() => setResetUser(null)} />
      )}
    </div>
  )
}
