"""
Smart Trader — Manager Architecture
====================================

Each manager is the SINGLE WRITER for its PostgreSQL table.
Managers fetch from brokers on a periodic schedule, normalise,
and upsert into PostgreSQL.  All other code (WebSocket, REST API,
risk engine) reads ONLY from PostgreSQL — never calling broker
APIs directly for display data.

Architecture:
  SupremeManager
    ├── SessionManager      (mgr_sessions)
    ├── AccountManager      (mgr_funds)
    ├── PositionManager     (mgr_positions)
    ├── OrderManager        (mgr_orders)
    ├── HoldingsManager     (mgr_holdings)
    ├── TradebookManager    (mgr_tradebook)
    └── AlertManager        (mgr_alerts)

Data flow:
  Broker APIs  →  Manager.refresh()  →  PostgreSQL
  PostgreSQL   →  WebSocket / REST   →  Frontend
"""
