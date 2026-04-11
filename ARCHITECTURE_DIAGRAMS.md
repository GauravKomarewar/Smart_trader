# Smart Trader — Complete Architecture Diagrams

> Visualize the entire codebase: system layers, data flow, broker adapters, strategy engine, API routes, and frontend structure.

---

## 1. High-Level System Architecture

```mermaid
graph TB
    subgraph CLIENTS["Clients"]
        BROWSER["Browser (React + Vite)"]
        WEBHOOK["External Webhooks\n(TradingView / Custom)"]
        MOBILE["Mobile Browser"]
    end

    subgraph FRONTEND["Frontend — React/TypeScript (port 5173)"]
        PAGES["Pages\n(Dashboard, Strategy, Option Chain,\nPositions, Broker Accounts, Greeks,\nAnalytics, Watchlist, Admin)"]
        STORES["Zustand Stores\n(Auth, Dashboard, Watchlist, Settings)"]
        HOOKS["Custom Hooks\n(useWebSocket, useMarket, etc.)"]
        COMPONENTS["Components\n(Layout, Dashboard widgets,\nModals, Common UI)"]
    end

    subgraph BACKEND["Backend — FastAPI/Python (port 8001)"]
        MAIN["main.py\n(FastAPI App)"]
        ROUTERS["API Routers\n(/api/auth, /api/market, /api/orders,\n/api/strategy, /api/risk, /api/positions,\n/api/greeks, /api/option-chain,\n/api/analytics, /api/broker, /api/admin)"]
        WSROUTER["WebSocket\n(/ws/market)"]

        subgraph CORE["core/"]
            CONFIG["config.py"]
            SECURITY["security.py\n(JWT + Encryption)"]
            DEPS["deps.py\n(DI: current_user)"]
            EVENTS["event_bus.py"]
            LOGGER["logger.py"]
        end

        subgraph TRADING["trading/"]
            BOT["trading_bot.py\n(Alert/Webhook entry)"]
            OMS["oms.py\n(Order Management System)"]
            INTENT["order_intent.py\n(OrderIntentProcessor)"]
            GUARD["execution_guard.py"]
            EXEC["execution/\n(CommandService, OrderWatcher,\nPositionExitService, Trailing)"]
            STRAT["strategy_runner/\n(StrategyExecutorService,\nEntryEngine, ExitEngine,\nAdjustmentEngine, ConditionEngine,\nMarketReader, ReconciliationEngine)"]
            SVC["services/\n(CopyTradingService,\nManualPositionManager,\nRecoveryService, Scheduler)"]
            POSSLMGR["position_sl_manager.py"]
            POSWATCHER["position_watcher.py"]
            ANALYTICS["analytics/"]
            NOTIF["notifications/"]
        end

        subgraph BROKER["broker/"]
            MULTI["multi_broker.py\n(MultiAccountRegistry)"]
            ADAPTERS["adapters/\n(Shoonya, Fyers, AngelOne,\nDhan, Paper, Groww, Kite, Upstox)"]
            OPTCHAIN["option_chain_service.py"]
            TICKSVC["live_tick_service.py"]
            ACCRISK["account_risk.py"]
            SYMNORM["symbol_normalizer.py"]
        end

        subgraph MANAGERS["managers/"]
            SUPRMGR["supreme_manager.py"]
            ACCTMGR["account_manager.py"]
            SESSIMGR["session_manager.py"]
            POSMGR["position_manager.py"]
            ORDMGR["order_manager.py"]
            HOLDMGR["holdings_manager.py"]
            TRDBMGR["tradebook_manager.py"]
            ALERTMGR["alert_manager.py"]
        end

        subgraph DB["db/"]
            DATABASE["database.py\n(SQLite + SQLAlchemy)"]
            SYMDB["symbols_db.py\n(Instruments / Script Master)"]
            TRADINGDB["trading_db.py\n(PostgreSQL)"]
        end
    end

    subgraph EXTERNAL["External Services"]
        SHOONYA["Shoonya / Finvasia API"]
        FYERS["Fyers API v3"]
        ANGEL["Angel One API"]
        DHAN["Dhan API v2"]
        PAPER["Paper Trade\n(Simulated)"]
        NSEBSE["NSE / BSE\n(Market Data)"]
    end

    BROWSER -->|HTTP/WS| FRONTEND
    WEBHOOK -->|POST /api/alert| MAIN
    FRONTEND -->|REST + WebSocket| BACKEND
    MAIN --> ROUTERS
    MAIN --> WSROUTER
    ROUTERS --> TRADING
    ROUTERS --> BROKER
    ROUTERS --> MANAGERS
    TRADING --> BROKER
    BROKER --> ADAPTERS
    ADAPTERS --> SHOONYA
    ADAPTERS --> FYERS
    ADAPTERS --> ANGEL
    ADAPTERS --> DHAN
    ADAPTERS --> PAPER
    BROKER --> NSEBSE
    TRADING --> DB
    MANAGERS --> DB
```

---

## 2. Backend Module Dependency Map

```mermaid
graph LR
    subgraph ENTRY["Entry Points"]
        MAIN["main.py"]
    end

    subgraph ROUTERS["routers/"]
        R_AUTH["auth.py"]
        R_MKT["market.py"]
        R_ORD["orders.py"]
        R_WS["websocket.py"]
        R_ADMIN["admin.py"]
        R_BROKER["broker_sessions.py"]
        R_STRAT["strategy.py"]
        R_RISK["risk.py"]
        R_POS["positions.py"]
        R_GREEK["greeks.py"]
        R_OC["option_chain.py"]
        R_ANALYTICS["analytics.py"]
    end

    subgraph TRADING_MOD["trading/"]
        T_BOT["trading_bot.py\nalert_router"]
        T_OMS["oms.py\noms_router"]
        T_INTENT["order_intent.py"]
        T_GUARD["execution_guard.py"]
        T_SR["strategy_runner/\nStrategyExecutorService"]
        T_CMD["execution/\nCommandService"]
        T_POS_SL["position_sl_manager.py"]
        T_POS_W["position_watcher.py"]
        T_SCHED["services/scheduler.py"]
    end

    subgraph BROKER_MOD["broker/"]
        B_MULTI["multi_broker.py\nMultiAccountRegistry"]
        B_BASE["adapters/base.py\nBrokerAdapter ABC"]
        B_SHOONYA["adapters/shoonya_adapter.py"]
        B_FYERS["adapters/fyers_adapter.py"]
        B_ANGEL["adapters/angelone_adapter.py"]
        B_DHAN["adapters/dhan_adapter.py"]
        B_PAPER["adapters/paper_adapter.py"]
        B_OC["option_chain_service.py"]
        B_TICK["live_tick_service.py"]
        B_RISK["account_risk.py"]
        B_NORM["symbol_normalizer.py"]
    end

    subgraph CORE_MOD["core/"]
        C_CFG["config.py"]
        C_SEC["security.py"]
        C_DEPS["deps.py"]
        C_BUS["event_bus.py"]
    end

    subgraph MGR["managers/"]
        M_SUP["supreme_manager.py"]
        M_ACC["account_manager.py"]
        M_POS["position_manager.py"]
        M_ORD["order_manager.py"]
    end

    subgraph DB_MOD["db/"]
        D_DB["database.py"]
        D_SYM["symbols_db.py"]
        D_TDB["trading_db.py"]
    end

    MAIN --> R_AUTH
    MAIN --> R_MKT
    MAIN --> R_ORD
    MAIN --> R_WS
    MAIN --> R_ADMIN
    MAIN --> R_BROKER
    MAIN --> R_STRAT
    MAIN --> R_RISK
    MAIN --> R_POS
    MAIN --> R_GREEK
    MAIN --> R_OC
    MAIN --> R_ANALYTICS
    MAIN --> T_BOT
    MAIN --> T_OMS

    R_AUTH --> C_SEC
    R_AUTH --> C_DEPS
    R_BROKER --> B_MULTI
    R_MKT --> B_OC
    R_MKT --> B_TICK
    R_OC --> B_OC
    R_STRAT --> T_SR
    R_ORD --> T_INTENT
    R_RISK --> B_RISK

    T_BOT --> T_INTENT
    T_INTENT --> T_BOT
    T_INTENT --> T_GUARD
    T_GUARD --> B_MULTI
    T_CMD --> B_MULTI
    T_SR --> T_CMD
    T_SR --> B_NORM
    T_POS_SL --> B_MULTI
    T_POS_W --> B_MULTI

    B_MULTI --> B_SHOONYA
    B_MULTI --> B_FYERS
    B_MULTI --> B_ANGEL
    B_MULTI --> B_DHAN
    B_MULTI --> B_PAPER
    B_SHOONYA --> B_BASE
    B_FYERS --> B_BASE
    B_ANGEL --> B_BASE
    B_DHAN --> B_BASE
    B_PAPER --> B_BASE

    M_SUP --> M_ACC
    M_SUP --> M_POS
    M_SUP --> M_ORD
    M_ACC --> B_MULTI

    T_SR --> D_TDB
    T_OMS --> D_TDB
    D_DB --> D_SYM
```

---

## 3. Broker Adapter Layer

```mermaid
classDiagram
    class BrokerAdapter {
        <<abstract>>
        +broker_id: str
        +connect(creds) bool
        +disconnect() void
        +get_profile() dict
        +get_funds() Funds
        +get_positions() list~Position~
        +get_holdings() list~Holding~
        +place_order(order) str
        +cancel_order(order_id) bool
        +get_order_book() list~Order~
        +get_trade_book() list~Trade~
        +subscribe_ticks(symbols, callback) void
        +unsubscribe_ticks(symbols) void
    }

    class ShoonyaAdapter {
        +broker_id = "shoonya"
        -_api: NorenApi
        +connect(creds) bool
        +place_order(order) str
        +subscribe_ticks() void
    }

    class FyersAdapter {
        +broker_id = "fyers"
        -_model: FyersModel
        -_data_socket: FyersDataSocket
        +connect(creds) bool
        +place_order(order) str
        +_handle_mcx_market_order() void
    }

    class AngelOneAdapter {
        +broker_id = "angel"
        -_session_token: str
        -BASE_URL: str
        +connect(creds) bool
        +place_order(order) str
    }

    class DhanAdapter {
        +broker_id = "dhan"
        -_access_token: str
        -BASE_URL: str
        +connect(creds) bool
        +place_order(order) str
    }

    class PaperAdapter {
        +broker_id = "paper_trade"
        -_virtual_portfolio: dict
        +connect(creds) bool
        +place_order(order) str
        +simulate_fill() void
    }

    class GrowwAdapter {
        +broker_id = "groww"
    }

    class KiteAdapter {
        +broker_id = "kite"
    }

    class UpstoxAdapter {
        +broker_id = "upstox"
    }

    class MultiAccountRegistry {
        -_sessions: dict~str, BrokerAccountSession~
        +register(session) void
        +get_session(config_id) BrokerAccountSession
        +get_all_sessions() list
        +get_sessions_for_user(user_id) list
    }

    class BrokerAccountSession {
        +config_id: str
        +broker_id: str
        +adapter: BrokerAdapter
        +is_live: bool
        +is_paper: bool
        +user_id: int
    }

    BrokerAdapter <|-- ShoonyaAdapter
    BrokerAdapter <|-- FyersAdapter
    BrokerAdapter <|-- AngelOneAdapter
    BrokerAdapter <|-- DhanAdapter
    BrokerAdapter <|-- PaperAdapter
    BrokerAdapter <|-- GrowwAdapter
    BrokerAdapter <|-- KiteAdapter
    BrokerAdapter <|-- UpstoxAdapter
    MultiAccountRegistry "1" --> "*" BrokerAccountSession
    BrokerAccountSession "1" --> "1" BrokerAdapter
```

---

## 4. Order Flow — Complete Pipeline

```mermaid
sequenceDiagram
    participant SRC as Source<br/>(Frontend / Webhook / Strategy)
    participant BOT as TradingBot<br/>(alert_router)
    participant INTENT as OrderIntentProcessor
    participant GUARD as ExecutionGuard
    participant RISK as AccountRisk
    participant CMD as CommandService
    participant ADAPTER as BrokerAdapter
    participant POSTGRES as PostgreSQL<br/>(trading_db)
    participant WATCHER as OrderWatcher

    SRC->>BOT: POST /api/alert {legs, strategy, ...}
    BOT->>BOT: Validate auth (API key / JWT)
    BOT->>INTENT: process_alert(alert_dict, user)

    INTENT->>INTENT: Resolve broker accounts for user
    INTENT->>INTENT: Normalize symbols (symbol_normalizer)

    loop For each broker account
        INTENT->>GUARD: check_execution_allowed(order)
        GUARD->>RISK: validate_risk_limits(order)
        RISK-->>GUARD: OK / REJECTED
        GUARD-->>INTENT: allowed=True/False

        alt Allowed
            INTENT->>CMD: execute_order(order, session)
            CMD->>ADAPTER: place_order(canonical_order)
            ADAPTER->>ADAPTER: Map to broker-specific format
            ADAPTER-->>CMD: broker_order_id
            CMD->>POSTGRES: persist_order(order_record)
            CMD->>WATCHER: track(broker_order_id)
            WATCHER->>ADAPTER: poll get_order_book()
            WATCHER-->>POSTGRES: update_order_status()
        else Rejected
            INTENT->>POSTGRES: persist_order(REJECTED)
        end
    end

    INTENT-->>BOT: results[]
    BOT-->>SRC: HTTP 200 {results}
```

---

## 5. Strategy Runner — Execution Flow

```mermaid
flowchart TD
    START([Strategy Config Loaded]) --> SCHED[SessionScheduler\nschedules start/stop times]
    SCHED --> SES_START[Strategy Session Start]
    SES_START --> COND{ConditionEngine\nEvaluate entry conditions}

    COND -- Conditions Not Met --> COND_WAIT[Wait & re-evaluate\non tick / time interval]
    COND_WAIT --> COND

    COND -- Conditions Met --> MKT_READ[MarketReader\nFetch ATM strike, IV, Greeks]
    MKT_READ --> STRIKE[StrikeConfig\nResolve CE/PE symbols\nATM / ITM / OTM offset]
    STRIKE --> ENTRY_ENG[EntryEngine\nBuild order legs\nQty, product, order_type]

    ENTRY_ENG --> NORM[symbol_normalizer\nMap exchange symbol to broker token]
    NORM --> EXEC_CHK{ExecutionGuard\nRisk check}

    EXEC_CHK -- Rejected --> LOG_REJ[Log rejection]
    EXEC_CHK -- Approved --> CMD_SVC[CommandService\nplace_order via BrokerAdapter]
    CMD_SVC --> PERSIST[PostgreSQL\nPersist order state]

    PERSIST --> POS_WATCH[PositionWatcher\nMonitor fills & MTM]
    POS_WATCH --> SL_MGR[PositionSLManager\nTrailing SL / Target tracking]

    SL_MGR --> ADJUST{AdjustmentEngine\nRoll / Hedge conditions}
    ADJUST -- Adjustment Triggered --> ADJ_EXEC[Place adjustment orders]
    ADJ_EXEC --> POS_WATCH

    ADJUST -- No Adjustment --> EXIT_COND{ExitEngine\nCheck exit conditions}
    EXIT_COND -- Exit Not Triggered --> SL_MGR
    EXIT_COND -- Exit Triggered --> EXIT_ORD[Place exit orders]
    EXIT_ORD --> RECONCILE[BrokerReconciliation\nVerify fills vs broker state]
    RECONCILE --> SES_END([Strategy Session End\nPersist final P&L])
```

---

## 6. API Routes Map

```mermaid
mindmap
  root((FastAPI\nport 8001))
    /api/auth
      POST /login
      GET /status
      POST /logout
      POST /refresh
    /api/market
      GET /indices
      GET /quote/:symbol
      GET /screener
      GET /option-chain/:symbol
      GET /search
    /api/orders
      POST /place
      GET /book
      GET /positions
      GET /holdings
      DELETE /cancel/:id
    /api/broker
      GET /accounts
      POST /accounts
      PUT /accounts/:id
      DELETE /accounts/:id
      POST /accounts/:id/connect
      POST /accounts/:id/disconnect
    /api/strategy
      GET /list
      POST /create
      PUT /update/:id
      DELETE /delete/:id
      POST /start/:id
      POST /stop/:id
      GET /status/:id
    /api/risk
      GET /limits
      PUT /limits
      GET /exposure
    /api/positions
      GET /open
      GET /closed
      POST /close/:id
      POST /exit-all
    /api/greeks
      GET /portfolio
      GET /live
    /api/option-chain
      GET /:symbol
      GET /live/:symbol
    /api/analytics
      GET /pnl
      GET /drawdown
      GET /win-rate
      GET /daily-summary
    /api/alert
      POST /alert
    /api/oms
      GET /orders
      GET /portfolio
    /api/admin
      GET /users
      POST /users
      PUT /users/:id
    /ws/market
      WS /ws/market
```

---

## 7. Frontend Architecture

```mermaid
graph TB
    subgraph APP["App.tsx — React Router"]
        LANDING["/ LandingPage"]
        LOGIN["/ LoginPage"]
        ADMIN_LOGIN["/admin/login AdminLoginPage"]
        ADMIN["/admin AdminPage"]
        DASH["/dashboard DashboardPage"]
        BROKER["/broker-accounts BrokerAccountsPage"]
        MARKET["/market MarketPage"]
        OPTION["/option-chain OptionChainPage"]
        STRAT["/strategy StrategyPage"]
        STRAT_BUILD["/strategy/builder StrategyBuilderPage"]
        POS["/positions PositionManagerPage"]
        GREEK["/greeks GreeksDashboardPage"]
        ANALYTICS["/analytics HistoricalAnalyticsPage"]
        WATCHLIST["/watchlist WatchlistChartPage"]
        SETTINGS["/settings SettingsPage"]
    end

    subgraph STORES["Zustand Stores (stores/index.ts)"]
        AUTH_ST["useAuthStore\n(user, accounts, isAuthenticated,\nactiveAccountId)"]
        DASH_ST["useDashboardStore\n(positions, orders, pnl)"]
        WATCH_ST["useWatchlistStore\n(watchlists, items)"]
        SETTINGS_ST["useSettingsStore\n(theme, layout, prefs)"]
        TOAST_ST["useToastStore\n(notifications queue)"]
    end

    subgraph HOOKS["Custom Hooks (hooks/)"]
        USE_WS["useWebSocket\n(live tick stream)"]
        USE_MKT["useMarketData\n(quotes, indices)"]
        USE_AUTH["useAuth\n(login/logout flow)"]
        USE_BROKER["useBrokerAccounts"]
    end

    subgraph COMPONENTS["Components (components/)"]
        LAYOUT["layout/\n(Navbar, Sidebar, Footer)"]
        DASH_COMP["dashboard/\n(PnlCard, PositionTable,\nOrderBook, RiskMeter)"]
        COMMON["common/\n(Button, Modal, Table,\nToast, Spinner, Chart)"]
        MODALS["modals/\n(PlaceOrderModal,\nBrokerConnectModal)"]
    end

    subgraph APIS["API Layer (lib/)"]
        APICLIENT["api.ts\n(axios instance + interceptors)"]
        WS_CLIENT["websocket.ts\n(ReconnectingWebSocket)"]
    end

    DASH --> DASH_ST
    DASH --> DASH_COMP
    BROKER --> AUTH_ST
    STRAT --> STORES
    OPTION --> USE_WS
    MARKET --> USE_MKT

    HOOKS --> APICLIENT
    HOOKS --> WS_CLIENT
    COMPONENTS --> STORES
    APICLIENT -->|REST| FASTAPI_BACK["FastAPI Backend\n:8001"]
    WS_CLIENT -->|WebSocket| FASTAPI_BACK
```

---

## 8. Database Layer

```mermaid
erDiagram
    USERS {
        int id PK
        string username
        string email
        string hashed_password
        string api_key
        string role
        bool is_active
        datetime created_at
    }

    BROKER_CONFIGS {
        string id PK
        int user_id FK
        string broker_id
        string display_name
        blob encrypted_credentials
        bool is_active
        bool is_paper
        datetime created_at
    }

    STRATEGIES {
        string id PK
        int user_id FK
        string name
        json config
        string status
        datetime created_at
        datetime updated_at
    }

    STRATEGY_RUNS {
        string run_id PK
        string strategy_id FK
        int user_id FK
        string broker_config_id FK
        string status
        float total_pnl
        datetime started_at
        datetime ended_at
    }

    ORDERS {
        string id PK
        string run_id FK
        int user_id FK
        string broker_config_id
        string broker_order_id
        string symbol
        string exchange
        string side
        string order_type
        string product
        int qty
        float price
        float fill_price
        string status
        datetime created_at
        datetime updated_at
    }

    POSITIONS {
        string id PK
        int user_id FK
        string broker_config_id
        string symbol
        string exchange
        int qty
        float avg_price
        float ltp
        float pnl
        bool is_open
        datetime updated_at
    }

    RISK_LIMITS {
        int id PK
        int user_id FK
        string broker_config_id
        float max_order_value
        float max_daily_loss
        float max_position_size
        int max_orders_per_day
    }

    USERS ||--o{ BROKER_CONFIGS : "has"
    USERS ||--o{ STRATEGIES : "creates"
    USERS ||--o{ ORDERS : "places"
    USERS ||--o{ POSITIONS : "holds"
    USERS ||--|| RISK_LIMITS : "has"
    STRATEGIES ||--o{ STRATEGY_RUNS : "executes"
    STRATEGY_RUNS ||--o{ ORDERS : "generates"
    BROKER_CONFIGS ||--o{ STRATEGY_RUNS : "runs on"
```

---

## 9. WebSocket / Live Data Flow

```mermaid
sequenceDiagram
    participant FE as Frontend\n(useWebSocket hook)
    participant WS as WebSocket Router\n(/ws/market)
    participant TICK as LiveTickService
    participant BUS as EventBus
    participant ADAPTER as BrokerAdapter\n(active sessions)
    participant BROKER_WS as Broker WebSocket\n(Shoonya/Fyers)

    FE->>WS: Connect ws://host/ws/market
    WS->>WS: Authenticate (token param)
    WS->>TICK: register_client(ws, user_id)

    BUS->>TICK: tick_received(symbol, ltp, oi, ...)
    TICK->>FE: push JSON tick

    FE->>WS: {"action":"subscribe","symbols":["NIFTY","BANKNIFTY"]}
    WS->>TICK: subscribe(symbols, user_id)
    TICK->>ADAPTER: subscribe_ticks(symbols, callback)
    ADAPTER->>BROKER_WS: WS subscribe frame

    BROKER_WS-->>ADAPTER: tick data stream
    ADAPTER-->>BUS: emit("tick", {symbol, data})
    BUS-->>TICK: tick handler
    TICK-->>FE: {"type":"tick","symbol":"NIFTY","ltp":22450.5,...}

    FE->>WS: {"action":"unsubscribe","symbols":["BANKNIFTY"]}
    WS->>TICK: unsubscribe(symbols, user_id)
```

---

## 10. Authentication & Security Flow

```mermaid
flowchart LR
    subgraph CLIENT["Client"]
        LOGIN_FORM["Login Form\n(username + password)"]
        JWT_STORE["JWT stored in\nlocalStorage / memory"]
        AUTH_HEADER["Authorization: Bearer token\non every request"]
    end

    subgraph AUTH_LAYER["Backend Auth Layer"]
        LOGIN_EP["POST /api/auth/login"]
        JWT_CREATE["security.py\ncreate_access_token()\n(HS256, exp=8h)"]
        DEPS_CHECK["deps.py\ncurrent_user dependency\n(verify JWT on each request)"]
        ADMIN_CHECK["Admin role check\n(role=='admin')"]
    end

    subgraph CRED_LAYER["Broker Credential Security"]
        ENCRYPT["security.py\nencrypt_credentials()\n(Fernet symmetric)"]
        DECRYPT["decrypt_credentials()"]
        KEY_SRC["ENCRYPTION_KEY env var\nor derived from JWT_SECRET"]
        CRED_DB["broker_configs table\nencrypted blob stored"]
    end

    LOGIN_FORM -->|POST credentials| LOGIN_EP
    LOGIN_EP -->|bcrypt verify| JWT_CREATE
    JWT_CREATE -->|JWT token| JWT_STORE
    JWT_STORE --> AUTH_HEADER
    AUTH_HEADER -->|every API call| DEPS_CHECK
    DEPS_CHECK --> ADMIN_CHECK

    ENCRYPT --> KEY_SRC
    KEY_SRC --> CRED_DB
    CRED_DB --> DECRYPT
    DECRYPT -->|plaintext creds| BROKER_ADAPTER["BrokerAdapter.connect()"]
```

---

## 11. Strategy Runner — Internal Modules

```mermaid
classDiagram
    class StrategyExecutorService {
        +strategy_name: str
        +run_id: str
        +state: StrategyState
        +start() void
        +stop() void
        +get_status() ExecutionState
    }

    class ConditionEngine {
        +evaluate_entry(state, market) bool
        +evaluate_exit(state, market) bool
        +evaluate_adjustment(state, market) bool
    }

    class MarketReader {
        +get_atm_strike(symbol) float
        +get_option_chain(symbol) OptionChain
        +get_iv(symbol, strike) float
        +get_greeks(symbol, strike) Greeks
    }

    class EntryEngine {
        +build_entry_legs(config, market_data) list~OrderLeg~
        +resolve_strike(config, market_data) StrikeConfig
    }

    class AdjustmentEngine {
        +check_adjustment_needed(state, market) bool
        +build_adjustment_legs(state, market) list~OrderLeg~
    }

    class ExitEngine {
        +check_exit_needed(state, market) bool
        +build_exit_legs(state) list~OrderLeg~
    }

    class BrokerReconciliation {
        +reconcile(expected, broker_state) ReconcileResult
        +detect_partial_fill(order) bool
    }

    class StatePersistence {
        +save_state(state) void
        +load_state(run_id) StrategyState
        +save_order(order) void
    }

    class StrategyState {
        +run_id: str
        +has_position: bool
        +legs: list~LegState~
        +entry_timestamp: float
        +total_pnl: float
    }

    StrategyExecutorService --> ConditionEngine
    StrategyExecutorService --> MarketReader
    StrategyExecutorService --> EntryEngine
    StrategyExecutorService --> AdjustmentEngine
    StrategyExecutorService --> ExitEngine
    StrategyExecutorService --> BrokerReconciliation
    StrategyExecutorService --> StatePersistence
    StrategyExecutorService --> StrategyState
    EntryEngine --> MarketReader
    AdjustmentEngine --> MarketReader
    ExitEngine --> MarketReader
```

---

## 12. Component & Module Count Summary

```mermaid
pie title Backend Modules by Category
    "Routers (API endpoints)" : 13
    "Trading Engine modules" : 15
    "Broker Adapters" : 8
    "Manager modules" : 9
    "Core / Config / Auth" : 6
    "DB / Persistence" : 3
    "Scripts / Utilities" : 5
```

---

## Quick Reference: Key File Paths

| Layer | File | Purpose |
|-------|------|---------|
| Entry | `backend/main.py` | FastAPI app, router registration, startup |
| Auth | `backend/core/security.py` | JWT creation/verify, credential encryption |
| DI | `backend/core/deps.py` | `current_user` FastAPI dependency |
| Config | `backend/core/config.py` | All env vars + feature flags |
| Orders | `backend/trading/trading_bot.py` | Single checkpoint for ALL orders |
| OMS | `backend/trading/oms.py` | Order lifecycle & portfolio state |
| Intent | `backend/trading/order_intent.py` | Multi-account order routing |
| Guard | `backend/trading/execution_guard.py` | Pre-trade risk validation |
| Broker | `backend/broker/multi_broker.py` | MultiAccountRegistry |
| Base | `backend/broker/adapters/base.py` | BrokerAdapter ABC |
| Strategy | `backend/trading/strategy_runner/strategy_executor_service.py` | Strategy run loop |
| DB | `backend/db/database.py` | SQLite tables (users, configs) |
| DB | `backend/db/trading_db.py` | PostgreSQL (orders, positions, runs) |
| Frontend | `frontend/src/App.tsx` | React router + page layout |
| Stores | `frontend/src/stores/index.ts` | Zustand global state |
