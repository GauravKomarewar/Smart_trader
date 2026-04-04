"""
Smart Trader — Broker API Integration Test
==========================================

Tests basic API calls for both Shoonya and Fyers brokers:
  • Auth / session health
  • get_positions
  • get_order_book
  • get_holdings
  • get_funds / get_limits
  • get_tradebook
  • place_order (with deliberately wrong qty for NIFTY FUT — expects rejection)

Usage:
    # Admin token from DB
    python trading/test_broker_apis.py --via-api

    # Direct (uses env credentials)
    python trading/test_broker_apis.py --direct
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import requests
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger("broker_api_test")

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL = os.getenv("SM_API_URL", "http://localhost:8001")

# NIFTY FUT near month contract (lot size = 65)
# Using qty=1 which is invalid (not a multiple of lot size=65) → broker must reject
NIFTY_FUT_SYMBOL    = "NSE:NIFTY26APRFUT"        # Fyers format
NIFTY_FUT_SH_SYMBOL = "NIFTY26APRFUT"             # Shoonya format (no exchange prefix)
NIFTY_FUT_EXCHANGE  = "NFO"
NIFTY_LOT_SIZE      = 65
TEST_QTY_WRONG      = 1                            # deliberately wrong — not a lot multiple

# ── Helpers ───────────────────────────────────────────────────────────────────

class APIClient:
    def __init__(self, base_url: str, token: str):
        self.base = base_url.rstrip("/")
        self.hdrs = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def get(self, path: str, **params) -> dict:
        r = requests.get(f"{self.base}{path}", headers=self.hdrs, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    def post(self, path: str, body: dict) -> dict:
        r = requests.post(f"{self.base}{path}", headers=self.hdrs, json=body, timeout=30)
        return r.json()   # do NOT raise so we can inspect error

def _section(title: str) -> None:
    print(f"\n{'═'*60}")
    print(f"  {title}")
    print(f"{'═'*60}")

def _ok(label: str, val) -> None:
    print(f"  ✓  {label}: {val}")

def _warn(label: str, val) -> None:
    print(f"  ⚠  {label}: {val}")

def _err(label: str, val) -> None:
    print(f"  ✗  {label}: {val}")

# ── Login ─────────────────────────────────────────────────────────────────────

def login(email: str, password: str) -> str:
    """Login and return JWT access token."""
    r = requests.post(
        f"{BASE_URL}/api/auth/login",
        json={"email": email, "password": password},
        timeout=15,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Login failed {r.status_code}: {r.text}")
    token = r.json().get("access_token") or r.json().get("token")
    if not token:
        raise RuntimeError(f"No token in login response: {r.json()}")
    return token

# ── Test sections ─────────────────────────────────────────────────────────────

def test_health(client: APIClient) -> None:
    _section("HEALTH CHECK")
    h = client.get("/health")
    _ok("status",          h.get("status"))
    _ok("mode",            h.get("mode"))
    _ok("active_accounts", h.get("active_accounts"))
    _ok("total_sessions",  h.get("total_sessions"))


def test_broker_sessions(client: APIClient) -> None:
    _section("BROKER SESSIONS")
    try:
        sessions = client.get("/api/broker/all-sessions")
        items = sessions if isinstance(sessions, list) else sessions.get("data", [])
        if not items:
            _warn("sessions", "None connected — connect a broker first")
            return
        for s in items:
            status = "LIVE" if s.get("is_live") else ("PAPER" if s.get("mode") == "paper" else "DEMO")
            _ok(f"{s.get('broker_id','?')} [{s.get('client_id','?')}]", status)
    except Exception as e:
        _err("sessions", e)


def test_positions(client: APIClient, account_id: str = None) -> None:
    _section("POSITIONS")
    try:
        params = {"account_id": account_id} if account_id else {}
        resp = client.get("/api/orders/positions", **params)
        rows = resp.get("data", [])
        _ok("count", resp.get("count", len(rows)))
        for p in rows[:5]:
            _ok(
                p.get("symbol", "?"),
                f"qty={p.get('qty', p.get('quantity', '?'))} "
                f"pnl={p.get('pnl', '?')} "
                f"broker={p.get('broker_id', '?')}",
            )
    except Exception as e:
        _err("positions", e)


def test_order_book(client: APIClient, account_id: str = None) -> None:
    _section("ORDER BOOK")
    try:
        params = {"account_id": account_id} if account_id else {}
        resp = client.get("/api/orders/book", **params)
        rows = resp.get("data", [])
        _ok("count", resp.get("count", len(rows)))
        for o in rows[:5]:
            _ok(
                o.get("symbol", "?"),
                f"side={o.get('side', o.get('transactionType', '?'))} "
                f"status={o.get('status', '?')} "
                f"qty={o.get('qty', o.get('quantity', '?'))}",
            )
    except Exception as e:
        _err("order_book", e)


def test_holdings(client: APIClient, account_id: str = None) -> None:
    _section("HOLDINGS")
    try:
        params = {"account_id": account_id} if account_id else {}
        resp = client.get("/api/orders/holdings", **params)
        rows = resp.get("data", [])
        _ok("count", resp.get("count", len(rows)))
        for h in rows[:5]:
            _ok(h.get("symbol", "?"), f"qty={h.get('qty','?')} pnl={h.get('pnl','?')}")
    except Exception as e:
        _err("holdings", e)


def test_funds(client: APIClient, account_id: str = None) -> None:
    _section("FUNDS / MARGIN")
    try:
        params = {"account_id": account_id} if account_id else {}
        resp = client.get("/api/orders/funds", **params)
        rows = resp.get("data", [])
        _ok("accounts", len(rows))
        for f in rows:
            _ok(
                f.get("broker_id", "?"),
                f"available=₹{f.get('available_cash', f.get('cash', '?')):.2f}  "
                f"used=₹{f.get('used_margin', f.get('marginUsed', 0)):.2f}",
            )
    except Exception as e:
        _err("funds", e)


def test_tradebook(client: APIClient, account_id: str = None) -> None:
    _section("TRADEBOOK")
    try:
        params = {"account_id": account_id} if account_id else {}
        resp = client.get("/api/orders/tradebook", **params)
        rows = resp.get("data", [])
        _ok("count", resp.get("count", len(rows)))
        for t in rows[:5]:
            _ok(
                t.get("symbol", "?"),
                f"side={t.get('side','?')} qty={t.get('qty','?')} price={t.get('price','?')}",
            )
    except Exception as e:
        _err("tradebook", e)


def test_place_order_wrong_qty(client: APIClient, account_id: str = None) -> None:
    """
    Test place_order with qty=1 for NIFTY FUT (lot size=65).
    Broker MUST reject this — we confirm the error message is sensible.

    Fyers symbol:   NSE:NIFTY26APRFUT
    Shoonya symbol: NIFTY26APRFUT  exchange=NFO
    """
    _section(f"PLACE ORDER — WRONG QTY TEST (qty={TEST_QTY_WRONG}, lot_size={NIFTY_LOT_SIZE})")
    print(f"  Symbol : {NIFTY_FUT_SYMBOL} ({NIFTY_FUT_SH_SYMBOL} on Shoonya)")
    print(f"  Lot size: {NIFTY_LOT_SIZE} | Test qty: {TEST_QTY_WRONG} (NOT a lot multiple)")
    print(f"  Expected: broker rejects with lot-size error")

    order = {
        "accountId":      account_id or "",
        "symbol":         NIFTY_FUT_SH_SYMBOL,   # plain symbol; backend handles exchange prefix
        "exchange":       NIFTY_FUT_EXCHANGE,
        "transactionType": "B",
        "orderType":      "MKT",
        "productType":    "MIS",
        "quantity":       TEST_QTY_WRONG,
        "price":          0,
        "validity":       "DAY",
        "tag":            "ENTRY:test_wrong_qty",
    }
    try:
        result = client.post("/api/orders/place", order)
        success = result.get("success", False)
        msg = result.get("message", result.get("detail", str(result)))

        if not success:
            _ok("Broker rejected (expected)", msg)
        else:
            order_id = result.get("order_id", result.get("orderId", "?"))
            _warn(
                f"Order ACCEPTED (unexpected for qty={TEST_QTY_WRONG})",
                f"order_id={order_id} — broker may not enforce lot-size client-side",
            )
    except Exception as e:
        _warn("HTTP error (may indicate rejected + non-2xx)", str(e))


def test_fyers_symbol(client: APIClient, account_id: str = None) -> None:
    """Same test using Fyers-format symbol (with exchange prefix)."""
    _section(f"PLACE ORDER — FYERS FORMAT WRONG QTY TEST")
    print(f"  Symbol : {NIFTY_FUT_SYMBOL}  (Fyers format with exchange prefix)")

    order = {
        "accountId":      account_id or "",
        "symbol":         NIFTY_FUT_SYMBOL,      # Fyers format: NSE:NIFTY26APRFUT
        "exchange":       NIFTY_FUT_EXCHANGE,
        "transactionType": "B",
        "orderType":      "MKT",
        "productType":    "MIS",
        "quantity":       TEST_QTY_WRONG,
        "price":          0,
        "validity":       "DAY",
        "tag":            "ENTRY:fyers_test_wrong_qty",
    }
    try:
        result = client.post("/api/orders/place", order)
        success = result.get("success", False)
        msg = result.get("message", result.get("detail", str(result)))

        if not success:
            _ok("Broker rejected (expected)", msg)
        else:
            _warn("Order ACCEPTED", f"order_id={result.get('order_id', '?')}")
    except Exception as e:
        _warn("HTTP error", str(e))


# ── Main ─────────────────────────────────────────────────────────────────────

def run_via_api(email: str, password: str, account_id: str = None) -> None:
    print(f"\n🔐 Logging in as: {email}")
    token = login(email, password)
    print(f"   Token: {token[:20]}…")

    client = APIClient(BASE_URL, token)

    test_health(client)
    test_broker_sessions(client)
    test_positions(client, account_id)
    test_order_book(client, account_id)
    test_holdings(client, account_id)
    test_funds(client, account_id)
    test_tradebook(client, account_id)
    test_place_order_wrong_qty(client, account_id)
    test_fyers_symbol(client, account_id)

    print(f"\n{'═'*60}")
    print("  All API tests completed.")
    print(f"{'═'*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smart Trader broker API test")
    parser.add_argument("--email",    default=os.getenv("SM_TEST_EMAIL", "gktradeslog@gmail.com"))
    parser.add_argument("--password", default=os.getenv("SM_TEST_PASS", ""))
    parser.add_argument("--account",  default=None, help="config_id of specific broker account to test")
    args = parser.parse_args()

    if not args.password:
        args.password = input(f"Password for {args.email}: ")

    run_via_api(args.email, args.password, args.account)
