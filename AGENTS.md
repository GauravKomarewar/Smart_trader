# Agent Instructions For Smart_trader

## Non-Negotiable: Symbol Handling

- Platform-facing state and APIs must use canonical normalized symbols.
- Do not introduce broker-specific symbol formats in frontend stores or page state.
- Before any broker call (market data, order placement, depth, history), resolve symbol via Symbols DB using `backend/broker/symbol_normalizer.py`.
- Preferred entrypoint: `resolve_symbol_for_broker(symbol, exchange, broker)`.

## Allowed Exceptions

- Deterministic hardcoded broker symbol construction is allowed only as final fallback when symbols DB lookup fails.
- Any fallback must be local to broker boundary and should preserve canonical symbol in response payloads.

## Required Touchpoints In Reviews

When changing symbol logic, review these files:
- `backend/broker/symbol_normalizer.py`
- `backend/db/symbols_db.py`
- `backend/broker/live_tick_service.py`
- `backend/broker/fyers_client.py`
- `backend/routers/market.py`
- `backend/routers/orders.py`
- `frontend/src/pages/WatchlistChartPage.tsx`

## Validation Minimum

- Run backend syntax checks for touched files.
- Run targeted symbol-resolution tests.
- Validate no new symbol-format warnings/errors in broker/websocket logs.
