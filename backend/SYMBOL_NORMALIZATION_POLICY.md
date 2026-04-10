# Symbol Normalization Policy

## Purpose

This policy makes symbol handling stable across all brokers (Fyers, Shoonya, etc.) by enforcing one canonical format on platform surfaces and DB-driven broker resolution internally.

## Core Rules

1. Platform/UI symbols must remain normalized and broker-agnostic.
2. Any broker call must resolve symbol via Symbols DB first.
3. Conversion to broker format must happen as close as possible to broker boundary.
4. Hardcoded string formatting is allowed only as final fallback when DB resolution fails.
5. New features must not add broker-specific symbol formats to frontend state.

## Canonical Representation

Use `trading_symbol` (no exchange prefix) for UI/state/domain logic.

Examples:
- `NIFTY26APR24500CE`
- `GOLDPETAL30APR26`
- `CRUDEOILM16APR26C4850`

## Allowed Conversion APIs

Always use functions from `backend/broker/symbol_normalizer.py`:
- `lookup_by_trading_symbol`
- `lookup_instrument`
- `to_broker_symbol`
- `from_broker_symbol`
- `resolve_symbol_for_broker` (preferred for broker-facing execution/data paths)

## Required Resolution Order

For broker-facing paths:

1. `resolve_symbol_for_broker(symbol, exchange, broker)`
2. If unresolved, apply deterministic fallback
3. Log fallback usage at `debug`/`warning` level for future cleanup

## Audited Critical Paths

The following paths are required to stay DB-first:

- Live ticks: `backend/broker/live_tick_service.py`
- Market APIs: `backend/routers/market.py`
- Fyers depth/quote handling: `backend/broker/fyers_client.py`
- Order/position normalization: `backend/broker/symbol_normalizer.py`, `backend/routers/orders.py`
- Broker adapters: `backend/broker/adapters/*`

## Frontend Rules

- Watchlist/chart/option-chain pages must send normalized symbols and explicit exchange where available.
- Frontend should not build broker-specific symbol strings.

## Regression Checklist (Before Merge)

1. Quote endpoint returns non-zero for at least one equity, one index, one futures, one options contract.
2. Market depth endpoint resolves symbol for MCX/NFO contracts.
3. WS market subscription receives ticks for exchange-prefixed and non-prefixed inputs.
4. Place-order path resolves canonical symbol to broker symbol without frontend broker formatting.
5. Logs contain no repeated symbol-format errors for active instruments.

## Agent Guardrail

Any code change that introduces manual broker symbol formatting in business routes/services must be treated as a regression unless there is a documented fallback reason in the same PR/commit.
