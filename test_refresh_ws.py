#!/usr/bin/env python3
"""
Direct WebSocket + REST test for Smart Trader data refresh.
No browser needed - tests backend data flow directly.
"""
import asyncio
import json
import time
import sys
import aiohttp

BASE = "http://localhost:5173"
WS_BASE = "ws://localhost:5173"

async def test_rest_endpoints():
    """Test REST API response times and data freshness."""
    print("\n" + "="*60)
    print("REST API RESPONSE TIME TEST")
    print("="*60)
    
    async with aiohttp.ClientSession() as session:
        # Login first
        try:
            resp = await session.post(f"{BASE}/api/auth/login", json={
                "email": "komarewargaurav@gmail.com",
                "password": "Gaurav@123"
            })
            data = await resp.json()
            token = data.get("token") or data.get("access_token")
            if not token:
                print(f"  Login response: {json.dumps(data)[:200]}")
                return None
            print(f"  ✓ Logged in, token: {token[:20]}...")
        except Exception as e:
            print(f"  Login failed: {e}")
            return None
        
        headers = {"Authorization": f"Bearer {token}"}
        
        # Test key endpoints
        endpoints = [
            ("/api/orders/dashboard", "Dashboard"),
            ("/api/orders/broker-accounts", "Broker Accounts"),
            ("/api/market/indices", "Market Indices"),
            ("/api/market/screener", "Screener"),
            ("/api/positions", "Positions"),
        ]
        
        for path, name in endpoints:
            try:
                start = time.time()
                resp = await session.get(f"{BASE}{path}", headers=headers)
                elapsed = (time.time() - start) * 1000
                data = await resp.json()
                
                # Extract key values
                summary = ""
                if isinstance(data, dict):
                    if "positions" in data:
                        summary = f"positions={len(data['positions'])}"
                    if "accountSummary" in data:
                        summary += f" equity={data['accountSummary'].get('totalEquity', 0)}"
                elif isinstance(data, list):
                    summary = f"items={len(data)}"
                
                print(f"  {name}: {elapsed:.0f}ms | {summary}")
            except Exception as e:
                print(f"  {name}: ERROR - {e}")
        
        return token


async def test_ws_feed(token):
    """Test WebSocket /ws/feed data push frequency."""
    print("\n" + "="*60)
    print("WEBSOCKET /ws/feed DATA PUSH TEST (15 seconds)")
    print("="*60)
    
    messages = []
    type_counts = {}
    
    async with aiohttp.ClientSession() as session:
        try:
            ws = await session.ws_connect(
                f"{WS_BASE}/ws/feed?token={token}",
                timeout=aiohttp.ClientWSCloseTimeout(ws_close=5)
            )
            
            start = time.time()
            deadline = start + 15  # 15 second test
            
            while time.time() < deadline:
                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=2.0)
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        msg_type = data.get("type", "unknown")
                        elapsed = time.time() - start
                        
                        type_counts[msg_type] = type_counts.get(msg_type, 0) + 1
                        messages.append({
                            "type": msg_type,
                            "elapsed": round(elapsed, 2),
                            "data_size": len(msg.data),
                        })
                        
                        # Print first few of each type
                        if type_counts[msg_type] <= 2:
                            preview = json.dumps(data.get("data", {}), default=str)[:100]
                            print(f"  [{elapsed:.1f}s] {msg_type} ({len(msg.data)}B): {preview}")
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        print(f"  WS closed/error: {msg}")
                        break
                except asyncio.TimeoutError:
                    print(f"  [{time.time()-start:.1f}s] No message for 2s!")
                    continue
            
            await ws.close()
            
        except Exception as e:
            print(f"  WS connection error: {e}")
            return
    
    # Summary
    print(f"\n  Total messages in 15s: {len(messages)}")
    print(f"  Message types:")
    for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"    {t}: {c} ({c/15:.1f}/sec)")
    
    # Check dashboard update frequency
    dash_msgs = [m for m in messages if m["type"] == "dashboard"]
    if dash_msgs:
        intervals = [dash_msgs[i]["elapsed"] - dash_msgs[i-1]["elapsed"] for i in range(1, len(dash_msgs))]
        if intervals:
            avg = sum(intervals) / len(intervals)
            print(f"\n  Dashboard update interval: avg={avg:.2f}s, min={min(intervals):.2f}s, max={max(intervals):.2f}s")
    
    pos_msgs = [m for m in messages if m["type"] == "positions_detail"]
    if pos_msgs:
        intervals = [pos_msgs[i]["elapsed"] - pos_msgs[i-1]["elapsed"] for i in range(1, len(pos_msgs))]
        if intervals:
            avg = sum(intervals) / len(intervals)
            print(f"  Positions update interval: avg={avg:.2f}s, min={min(intervals):.2f}s, max={max(intervals):.2f}s")
    
    acct_msgs = [m for m in messages if m["type"] == "broker_accounts"]
    if acct_msgs:
        intervals = [acct_msgs[i]["elapsed"] - acct_msgs[i-1]["elapsed"] for i in range(1, len(acct_msgs))]
        if intervals:
            avg = sum(intervals) / len(intervals)
            print(f"  Broker accounts interval: avg={avg:.2f}s, min={min(intervals):.2f}s, max={max(intervals):.2f}s")
    
    return messages


async def test_ws_market():
    """Test WebSocket /ws/market tick frequency."""
    print("\n" + "="*60)
    print("WEBSOCKET /ws/market TICK TEST (10 seconds)")
    print("="*60)
    
    messages = []
    symbol_counts = {}
    
    async with aiohttp.ClientSession() as session:
        try:
            ws = await session.ws_connect(f"{WS_BASE}/ws/market")
            
            # Subscribe to key symbols
            await ws.send_json({
                "action": "subscribe",
                "symbols": ["NIFTY", "BANKNIFTY", "NIFTY 50", "NIFTY BANK", "RELIANCE", "TCS"]
            })
            
            start = time.time()
            deadline = start + 10
            
            while time.time() < deadline:
                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=2.0)
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        msg_type = data.get("type", "unknown")
                        elapsed = time.time() - start
                        
                        if msg_type == "tick":
                            tick = data.get("data", {})
                            sym = tick.get("symbol", "?")
                            ltp = tick.get("ltp", 0)
                            symbol_counts[sym] = symbol_counts.get(sym, 0) + 1
                            messages.append({"sym": sym, "ltp": ltp, "elapsed": round(elapsed, 2)})
                            
                            if symbol_counts[sym] <= 3:
                                print(f"  [{elapsed:.1f}s] TICK {sym}: {ltp} (change={tick.get('change', '?')}, src={tick.get('source', '?')})")
                        elif msg_type == "subscribed":
                            print(f"  [{elapsed:.1f}s] Subscribed: {data.get('symbols', [])}")
                        elif msg_type == "connected":
                            print(f"  [{elapsed:.1f}s] Connected: mode={data.get('mode', '?')}")
                        elif msg_type != "heartbeat" and msg_type != "pong":
                            print(f"  [{elapsed:.1f}s] {msg_type}")
                except asyncio.TimeoutError:
                    print(f"  [{time.time()-start:.1f}s] No tick for 2s!")
            
            await ws.close()
            
        except Exception as e:
            print(f"  WS market error: {e}")
            return
    
    print(f"\n  Total ticks in 10s: {len(messages)}")
    print(f"  Per-symbol tick counts:")
    for s, c in sorted(symbol_counts.items(), key=lambda x: -x[1]):
        print(f"    {s}: {c} ticks ({c/10:.1f}/sec)")
    
    # Check for symbols with NO ticks
    if not symbol_counts:
        print("  ⚠️ NO TICKS RECEIVED AT ALL!")
    
    return messages


async def test_force_refresh(token):
    """Test force_refresh action - should trigger immediate data push."""
    print("\n" + "="*60)
    print("FORCE REFRESH TEST")
    print("="*60)
    
    async with aiohttp.ClientSession() as session:
        try:
            ws = await session.ws_connect(f"{WS_BASE}/ws/feed?token={token}")
            
            # Wait for initial data
            initial_msgs = []
            start = time.time()
            while time.time() - start < 3:
                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=1.0)
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        initial_msgs.append(data.get("type"))
                except asyncio.TimeoutError:
                    break
            
            print(f"  Initial messages (3s): {initial_msgs}")
            
            # Send force_refresh
            print(f"  Sending force_refresh...")
            t0 = time.time()
            await ws.send_json({"action": "force_refresh"})
            
            # Measure how fast response comes
            refresh_msgs = []
            while time.time() - t0 < 3:
                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=0.5)
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        elapsed_ms = (time.time() - t0) * 1000
                        refresh_msgs.append(f"{data.get('type')} @ {elapsed_ms:.0f}ms")
                except asyncio.TimeoutError:
                    break
            
            print(f"  Response after force_refresh: {refresh_msgs}")
            
            await ws.close()
            
        except Exception as e:
            print(f"  Force refresh test error: {e}")


async def main():
    print("Smart Trader Data Refresh Diagnostic")
    print(f"Target: {BASE}")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. REST endpoints
    token = await test_rest_endpoints()
    if not token:
        print("\n❌ Cannot proceed without auth token")
        return
    
    # 2. WS feed test
    await test_ws_feed(token)
    
    # 3. WS market test  
    await test_ws_market()
    
    # 4. Force refresh test
    await test_force_refresh(token)
    
    print("\n" + "="*60)
    print("DIAGNOSTIC COMPLETE")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())
