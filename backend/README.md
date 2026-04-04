# Smart Trader Backend

FastAPI backend for Smart Trader — connects to Shoonya trading platform.

## Setup

```bash
cd /home/ubuntu/Smart_trader/backend

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure credentials
cp .env.example .env
# Edit .env — set SHOONYA_* credentials

# Start server
python main.py
```

Server runs on **port 8001** by default.  
Frontend dev server runs on **port 5173**.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| POST | `/api/auth/login` | Trigger Shoonya OAuth |
| GET | `/api/auth/status` | Session status |
| GET | `/api/market/indices` | Index prices |
| GET | `/api/market/quote/{symbol}` | Single quote |
| GET | `/api/market/screener` | Stock screener |
| GET | `/api/market/option-chain/{symbol}` | Option chain |
| GET | `/api/market/search?q=` | Instrument search |
| POST | `/api/orders/place` | Place order |
| GET | `/api/orders/book` | Order book |
| GET | `/api/orders/positions` | Positions |
| WS | `/ws/market` | Live market ticks |

## WebSocket Protocol

```js
const ws = new WebSocket("ws://localhost:8001/ws/market")

// Subscribe to symbols
ws.send(JSON.stringify({ action: "subscribe", symbols: ["NIFTY", "BANKNIFTY"] }))

// Receive ticks
ws.onmessage = (e) => {
  const msg = JSON.parse(e.data)
  if (msg.type === "tick") {
    console.log(msg.data)  // { symbol, ltp, change, changePct, ... }
  }
}
```

## Logging

All logs are written to `logs/` with rotation (10 MB, 5 backups):

| File | Component |
|------|-----------|
| `logs/api.log` | HTTP requests |
| `logs/auth.log` | OAuth / session events |
| `logs/broker.log` | Shoonya API calls |
| `logs/orders.log` | Order activity |
| `logs/websocket.log` | WebSocket connections |
| `logs/errors.log` | All ERROR+ across all components |

## Shoonya OAuth

The OAuth flow uses the existing `shoonya_platform` code at `/home/ubuntu/shoonya_platform/`.

Requirements:
- `SHOONYA_USER_ID`, `SHOONYA_PASSWORD`, `SHOONYA_TOTP_KEY` (base32), 
  `SHOONYA_VENDOR_CODE`, `SHOONYA_OAUTH_SECRET` in `.env`
- Chromium installed at `/snap/chromium/current/`

When credentials are absent the server runs in **DEMO mode** — all data is synthetic
but the UI works fully.

## Demo Mode

When `SHOONYA_*` credentials are not configured:
- All market data is synthetically generated
- Orders are acknowledged but not sent to broker
- WebSocket pushes simulated ticks every 1 second
- Auth status shows `mode: "demo"`
