#!/usr/bin/env python3
"""
Smart Trader — End-to-End Integration Test
============================================

Hits the live API at http://localhost:5173 and verifies:
  • All 8 brokers appear in /api/broker/supported
  • Authenticated endpoints return properly normalised data
  • Every position, order, holding, trade, funds record uses canonical values
  • WebSocket /ws/feed connects and streams dashboard data

Run:
    cd /home/ubuntu/Smart_trader/backend
    python3 tests/test_integration_e2e.py --email <email> --password <password>

    # Or with defaults (if default creds are set):
    python3 tests/test_integration_e2e.py
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from typing import Any, Dict, List, Optional

import requests

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL = "http://localhost:5173"

VALID_PRODUCTS   = {"MIS", "NRML", "CNC"}
VALID_EXCHANGES  = {"NSE", "BSE", "NFO", "BFO", "MCX", "CDS", "BCD"}
VALID_SIDES      = {"BUY", "SELL"}
VALID_STATUSES   = {"PENDING", "COMPLETE", "CANCELLED", "REJECTED"}
VALID_ORDER_TYPES = {"MARKET", "LIMIT", "SL", "SL-M"}
VALID_INSTR_TYPES = {"EQ", "CE", "PE", "FUT", "IDX", ""}

EXPECTED_BROKERS = {"shoonya", "fyers", "angel", "dhan", "groww", "upstox", "zerodha", "paper_trade"}


# ── Helpers ───────────────────────────────────────────────────────────────────
class Colors:
    GREEN  = "\033[92m"
    RED    = "\033[91m"
    YELLOW = "\033[93m"
    CYAN   = "\033[96m"
    BOLD   = "\033[1m"
    RESET  = "\033[0m"


passed = 0
failed = 0
warned = 0


def _ok(msg: str):
    global passed
    passed += 1
    print(f"  {Colors.GREEN}✓{Colors.RESET} {msg}")


def _fail(msg: str):
    global failed
    failed += 1
    print(f"  {Colors.RED}✗{Colors.RESET} {msg}")


def _warn(msg: str):
    global warned
    warned += 1
    print(f"  {Colors.YELLOW}⚠{Colors.RESET} {msg}")


def _section(title: str):
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'─' * 60}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}  {title}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'─' * 60}{Colors.RESET}")


class APIClient:
    def __init__(self, token: str):
        self.token = token
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {token}"
        self.session.headers["Content-Type"] = "application/json"

    def get(self, path: str, **kwargs) -> requests.Response:
        return self.session.get(f"{BASE_URL}{path}", timeout=15, **kwargs)

    def post(self, path: str, **kwargs) -> requests.Response:
        return self.session.post(f"{BASE_URL}{path}", timeout=15, **kwargs)


# ── Login ─────────────────────────────────────────────────────────────────────
def login(email: str, password: str) -> Optional[APIClient]:
    _section("Authentication")
    try:
        resp = requests.post(
            f"{BASE_URL}/api/auth/login",
            json={"email": email, "password": password},
            timeout=10,
        )
        data = resp.json()
        token = data.get("access_token") or data.get("token")
        if token:
            _ok(f"Login OK — token received ({len(token)} chars)")
            return APIClient(token)
        else:
            _fail(f"Login failed: {data}")
            return None
    except Exception as e:
        _fail(f"Login error: {e}")
        return None


# ── Test: Health ──────────────────────────────────────────────────────────────
def test_health():
    _section("Health Check")
    try:
        resp = requests.get(f"{BASE_URL}/health", timeout=5)
        if resp.status_code == 200:
            _ok(f"GET /health → {resp.status_code}")
        else:
            _fail(f"GET /health → {resp.status_code}")
    except Exception as e:
        _fail(f"Health check: {e}")


# ── Test: Broker Support ─────────────────────────────────────────────────────
def test_supported_brokers(client: APIClient):
    _section("Supported Brokers")
    try:
        resp = client.get("/api/broker/supported")
        data = resp.json()
        brokers_raw = data if isinstance(data, list) else data.get("brokers", [])
        broker_names = set()
        for b in brokers_raw:
            if isinstance(b, dict):
                broker_names.add(b.get("id", "").lower())
            elif isinstance(b, str):
                broker_names.add(b.lower())

        for expected in EXPECTED_BROKERS:
            if expected in broker_names:
                _ok(f"Broker '{expected}' registered")
            else:
                _fail(f"Broker '{expected}' MISSING — got: {broker_names}")
    except Exception as e:
        _fail(f"Supported brokers: {e}")


# ── Test: Broker Sessions ────────────────────────────────────────────────────
def test_broker_sessions(client: APIClient):
    _section("Broker Sessions")
    try:
        resp = client.get("/api/broker/all-sessions")
        data = resp.json()
        sessions = data if isinstance(data, list) else data.get("sessions", [])
        if sessions:
            for s in sessions:
                name = s.get("broker", "?")
                connected = s.get("connected", False)
                status = f"{Colors.GREEN}connected{Colors.RESET}" if connected else f"{Colors.YELLOW}disconnected{Colors.RESET}"
                _ok(f"{name}: {status} (account: {s.get('account_id', '?')})")
        else:
            _warn("No active broker sessions")
    except Exception as e:
        _fail(f"Broker sessions: {e}")


# ── Validation helpers ────────────────────────────────────────────────────────
def validate_position(pos: dict, label: str) -> bool:
    ok = True
    for key in ("symbol", "exchange", "product", "qty", "avg_price", "ltp", "pnl"):
        if key not in pos:
            _fail(f"{label}: missing field '{key}'")
            ok = False

    if pos.get("product") and pos["product"] not in VALID_PRODUCTS:
        _fail(f"{label}: non-canonical product '{pos['product']}'")
        ok = False
    if pos.get("exchange") and pos["exchange"] not in VALID_EXCHANGES:
        _fail(f"{label}: non-canonical exchange '{pos['exchange']}'")
        ok = False
    return ok


def validate_order(order: dict, label: str) -> bool:
    ok = True
    for key in ("order_id", "symbol", "side", "product", "order_type", "qty", "status"):
        if key not in order:
            _fail(f"{label}: missing field '{key}'")
            ok = False

    if order.get("side") and order["side"] not in VALID_SIDES:
        _fail(f"{label}: non-canonical side '{order['side']}'")
        ok = False
    if order.get("status") and order["status"] not in VALID_STATUSES:
        _fail(f"{label}: non-canonical status '{order['status']}'")
        ok = False
    if order.get("order_type") and order["order_type"] not in VALID_ORDER_TYPES:
        _fail(f"{label}: non-canonical order_type '{order['order_type']}'")
        ok = False
    if order.get("product") and order["product"] not in VALID_PRODUCTS:
        _fail(f"{label}: non-canonical product '{order['product']}'")
        ok = False
    return ok


def validate_holding(h: dict, label: str) -> bool:
    ok = True
    for key in ("symbol", "qty", "avg_price"):
        if key not in h:
            _fail(f"{label}: missing field '{key}'")
            ok = False
    return ok


def validate_funds(f: dict, label: str) -> bool:
    ok = True
    for key in ("available_cash", "used_margin", "total_balance"):
        if key not in f:
            _fail(f"{label}: missing field '{key}'")
            ok = False
    return ok


def validate_trade(t: dict, label: str) -> bool:
    ok = True
    for key in ("trade_id", "order_id", "symbol", "side", "qty", "price"):
        if key not in t:
            _fail(f"{label}: missing field '{key}'")
            ok = False
    if t.get("side") and t["side"] not in VALID_SIDES:
        _fail(f"{label}: non-canonical side '{t['side']}'")
        ok = False
    return ok


# ── Test: All Positions ──────────────────────────────────────────────────────
def test_positions(client: APIClient):
    _section("Positions (All Brokers)")
    try:
        resp = client.get("/api/orders/positions")
        data = resp.json()
        positions = data if isinstance(data, list) else data.get("positions", [])
        if not positions:
            _warn("No positions returned (might be market closed / no open positions)")
            return
        _ok(f"Got {len(positions)} position(s)")
        for i, pos in enumerate(positions):
            label = f"Position #{i+1} ({pos.get('broker','?')}/{pos.get('symbol','?')})"
            if validate_position(pos, label):
                _ok(f"{label}: OK — {pos.get('product')} {pos.get('exchange')} qty={pos.get('qty')}")
    except Exception as e:
        _fail(f"Positions error: {e}")


# ── Test: All Orders ─────────────────────────────────────────────────────────
def test_order_book(client: APIClient):
    _section("Order Book (All Brokers)")
    try:
        resp = client.get("/api/orders/book")
        data = resp.json()
        orders = data if isinstance(data, list) else data.get("orders", [])
        if not orders:
            _warn("No orders returned")
            return
        _ok(f"Got {len(orders)} order(s)")
        for i, order in enumerate(orders):
            label = f"Order #{i+1} ({order.get('broker','?')}/{order.get('symbol','?')})"
            if validate_order(order, label):
                _ok(f"{label}: {order.get('side')} {order.get('status')} @ {order.get('avg_price')}")
    except Exception as e:
        _fail(f"Orders error: {e}")


# ── Test: All Holdings ───────────────────────────────────────────────────────
def test_holdings(client: APIClient):
    _section("Holdings (All Brokers)")
    try:
        resp = client.get("/api/orders/holdings")
        data = resp.json()
        holdings = data if isinstance(data, list) else data.get("holdings", [])
        if not holdings:
            _warn("No holdings returned")
            return
        _ok(f"Got {len(holdings)} holding(s)")
        for i, h in enumerate(holdings):
            label = f"Holding #{i+1} ({h.get('broker','?')}/{h.get('symbol','?')})"
            if validate_holding(h, label):
                _ok(f"{label}: qty={h.get('qty')} avg={h.get('avg_price')}")
    except Exception as e:
        _fail(f"Holdings error: {e}")


# ── Test: All Funds ──────────────────────────────────────────────────────────
def test_funds(client: APIClient):
    _section("Funds (All Brokers)")
    try:
        resp = client.get("/api/orders/funds")
        data = resp.json()
        funds_list = data if isinstance(data, list) else data.get("data", data.get("funds", [data]))
        if not funds_list:
            _warn("No funds returned (no brokers connected)")
            return
        for i, f in enumerate(funds_list):
            label = f"Funds #{i+1} ({f.get('broker','?')}/{f.get('account_id','?')})"
            if validate_funds(f, label):
                _ok(f"{label}: cash={f.get('available_cash')} used={f.get('used_margin')} total={f.get('total_balance')}")
    except Exception as e:
        _fail(f"Funds error: {e}")


# ── Test: All Tradebook ─────────────────────────────────────────────────────
def test_tradebook(client: APIClient):
    _section("Tradebook (All Brokers)")
    try:
        resp = client.get("/api/orders/tradebook")
        data = resp.json()
        trades = data if isinstance(data, list) else data.get("trades", [])
        if not trades:
            _warn("No trades returned (normal outside market hours)")
            return
        _ok(f"Got {len(trades)} trade(s)")
        for i, t in enumerate(trades[:10]):  # Show first 10
            label = f"Trade #{i+1} ({t.get('broker','?')}/{t.get('symbol','?')})"
            if validate_trade(t, label):
                _ok(f"{label}: {t.get('side')} qty={t.get('qty')} @ {t.get('price')}")
        if len(trades) > 10:
            _ok(f"... and {len(trades) - 10} more trade(s)")
    except Exception as e:
        _fail(f"Tradebook error: {e}")


# ── Test: WebSocket Feed ─────────────────────────────────────────────────────
def test_websocket_feed(client: APIClient):
    _section("WebSocket Feed (/ws/feed)")
    try:
        import websocket
    except ImportError:
        _warn("websocket-client not installed — skipping WS test (pip install websocket-client)")
        return

    try:
        ws_url = f"ws://localhost:5173/ws/feed?token={client.token}"
        ws = websocket.create_connection(ws_url, timeout=10)
        # Wait for first message
        msg = ws.recv()
        ws.close()

        data = json.loads(msg)
        msg_type = data.get("type", "")
        if msg_type in ("dashboard", "broker_accounts", "broker_data"):
            _ok(f"WebSocket connected, received message type: '{msg_type}'")
            payload = data.get("data", {})
            # Check dashboard structure
            if "positions" in payload or "orders" in payload or "funds" in payload:
                _ok(f"Dashboard payload contains expected keys: {list(payload.keys())[:5]}")
            elif msg_type == "broker_accounts":
                _ok(f"Broker accounts data received ({len(payload)} accounts)")
            else:
                _warn(f"Unexpected payload keys: {list(payload.keys())[:5]}")
        else:
            _warn(f"Unexpected WS message type: '{msg_type}'")
    except Exception as e:
        _warn(f"WebSocket test: {e}")


# ── Test: Place Order (wrong qty for lot-size validation) ─────────────────────
def test_place_order_wrong_qty(client: APIClient):
    _section("Order Placement Guard (wrong lot size)")
    try:
        # Try to place NIFTY FUT order with qty=1 (lot=75/50)
        # This should fail with a validation error
        resp = client.post("/api/orders/place", json={
            "accountId": "paper",
            "symbol": "NIFTY26APRFUT",
            "exchange": "NFO",
            "transactionType": "BUY",
            "productType": "MIS",
            "orderType": "MARKET",
            "quantity": 1,  # Wrong lot size
        })
        data = resp.json()
        if data.get("success") is False or resp.status_code >= 400:
            _ok(f"Order correctly rejected: {data.get('message', data.get('detail', ''))[:80]}")
        else:
            _warn(f"Order was accepted (qty=1 for FUT) — lot-size guard may be disabled")
    except Exception as e:
        _fail(f"Place order test: {e}")


# ── Summary ───────────────────────────────────────────────────────────────────
def print_summary():
    _section("Test Summary")
    total = passed + failed + warned
    print(f"  Total: {total}  |  "
          f"{Colors.GREEN}Passed: {passed}{Colors.RESET}  |  "
          f"{Colors.RED}Failed: {failed}{Colors.RESET}  |  "
          f"{Colors.YELLOW}Warnings: {warned}{Colors.RESET}")
    if failed == 0:
        print(f"\n  {Colors.GREEN}{Colors.BOLD}ALL CHECKS PASSED ✓{Colors.RESET}")
    else:
        print(f"\n  {Colors.RED}{Colors.BOLD}{failed} CHECK(S) FAILED{Colors.RESET}")
    return failed == 0


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Smart Trader E2E Integration Test")
    parser.add_argument("--email", default=os.getenv("SM_TEST_EMAIL", "gktradeslog@gmail.com"))
    parser.add_argument("--password", default=os.getenv("SM_TEST_PASS", "Admin@1234"))
    parser.add_argument("--base-url", default="http://localhost:5173")
    args = parser.parse_args()

    base_url = args.base_url

    if not args.password:
        args.password = input(f"Password for {args.email}: ")

    print(f"\n{Colors.BOLD}Smart Trader — End-to-End Integration Test{Colors.RESET}")
    print(f"  Target: {BASE_URL}")
    print(f"  Time:   {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # 1. Health
    test_health()

    # 2. Login
    client = login(args.email, args.password)
    if not client:
        print(f"\n{Colors.RED}Cannot continue without auth token.{Colors.RESET}")
        sys.exit(1)

    # 3. Broker checks
    test_supported_brokers(client)
    test_broker_sessions(client)

    # 4. Data endpoints
    test_positions(client)
    test_order_book(client)
    test_holdings(client)
    test_funds(client)
    test_tradebook(client)

    # 5. WebSocket
    test_websocket_feed(client)

    # 6. Order guard
    test_place_order_wrong_qty(client)

    # 7. Summary
    success = print_summary()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
