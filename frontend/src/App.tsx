import { Routes, Route, Navigate } from 'react-router-dom'
import { useEffect } from 'react'
import { useAuthStore, useToastStore, useSettingsStore } from './stores'
import { useAuthCheck, useLiveData } from './hooks'
import AppLayout from './components/layout/AppLayout'
import LandingPage from './pages/LandingPage'
import LoginPage from './pages/LoginPage'
import AdminLoginPage from './pages/AdminLoginPage'
import RegisterPage from './pages/RegisterPage'
import AdminPage from './pages/AdminPage'
import DashboardPage from './pages/DashboardPage'
import MarketPage from './pages/MarketPage'
import OptionChainPage from './pages/OptionChainPage'
import WatchlistChartPage from './pages/WatchlistChartPage'
import StrategyPage from './pages/StrategyPage'
import StrategyBuilderPage from './pages/StrategyBuilderPage'
import SettingsPage from './pages/SettingsPage'
import HistoricalAnalyticsPage from './pages/HistoricalAnalyticsPage'
import BrokerAccountsPage from './pages/BrokerAccountsPage'
import ToastContainer from './components/common/ToastContainer'

/* ── Apply theme + font globally (works on every page) ── */
function ThemeEffect() {
  const { settings } = useSettingsStore()
  useEffect(() => {
    document.documentElement.setAttribute('data-theme', settings.theme)
    document.documentElement.setAttribute('data-fontsize', settings.fontSize)
  }, [settings.theme, settings.fontSize])
  return null
}

function RequireAuth({ children }: { children: React.ReactNode }) {
  const { isAuthenticated, isChecking } = useAuthStore()

  if (isChecking) {
    return (
      <div className="h-screen w-screen flex items-center justify-center bg-bg-base">
        <div className="flex flex-col items-center gap-3">
          <div className="w-8 h-8 border-2 border-brand border-t-transparent rounded-full animate-spin" />
          <span className="text-text-sec text-sm">Loading Smart Trader…</span>
        </div>
      </div>
    )
  }

  if (!isAuthenticated) return <Navigate to="/login" replace />
  return <>{children}</>
}

// After login, navigate to /app instead of /
export const APP_ROOT = '/app'

function RequireAdmin({ children }: { children: React.ReactNode }) {
  const { isAuthenticated, isChecking, user } = useAuthStore()
  const { toast } = useToastStore()
  if (isChecking) return (
    <div className="h-screen w-screen flex items-center justify-center bg-bg-base">
      <div className="w-8 h-8 border-2 border-brand border-t-transparent rounded-full animate-spin" />
    </div>
  )
  if (!isAuthenticated) return <Navigate to="/admin-login" replace />
  if (user?.role !== 'admin') {
    toast('Admin access required — use admin credentials', 'error')
    return <Navigate to="/app" replace />
  }
  return <>{children}</>
}

export default function App() {
  useAuthCheck()
  useLiveData()

  return (
    <>
      <ThemeEffect />
      <Routes>
        {/* Public routes */}
        <Route path="/" element={<LandingPage />} />
        <Route path="/login"       element={<LoginPage />} />
        <Route path="/admin-login" element={<AdminLoginPage />} />
        <Route path="/register"    element={<RegisterPage />} />
        <Route
          path="/admin"
          element={
            <RequireAdmin>
              <AdminPage />
            </RequireAdmin>
          }
        />
        {/* App routes — require auth */}
        <Route
          path="/app"
          element={
            <RequireAuth>
              <AppLayout />
            </RequireAuth>
          }
        >
          <Route index element={<DashboardPage />} />
          <Route path="market" element={<MarketPage />} />
          <Route path="option-chain" element={<OptionChainPage />} />
          <Route path="watchlist" element={<WatchlistChartPage />} />
          <Route path="strategies" element={<StrategyPage />} />
          <Route path="strategy-builder" element={<StrategyBuilderPage />} />

          <Route path="analytics" element={<HistoricalAnalyticsPage />} />
          <Route path="broker-accounts" element={<BrokerAccountsPage />} />
          <Route path="settings" element={<SettingsPage />} />
          <Route path="settings/:tab" element={<SettingsPage />} />
        </Route>
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
      <ToastContainer />
    </>
  )
}
