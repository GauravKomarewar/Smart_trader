#!/usr/bin/env python3
"""
Selenium test to measure data refresh timing on Smart Trader pages.
Uses Firefox headless to login and check each page's data update timing.
"""
import json
import time
import sys
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

BASE = "http://localhost:5173"

def setup_driver():
    opts = Options()
    opts.add_argument("--headless")
    opts.add_argument("--width=1440")
    opts.add_argument("--height=900")
    from selenium.webdriver.firefox.service import Service
    service = Service(executable_path="/usr/local/bin/geckodriver")
    driver = webdriver.Firefox(options=opts, service=service)
    driver.implicitly_wait(5)
    return driver


def login(driver):
    """Login to Smart Trader."""
    driver.get(f"{BASE}/login")
    time.sleep(2)
    
    # Check current page
    print(f"  Current URL after loading login: {driver.current_url}")
    
    # Try to find login form
    try:
        email_input = driver.find_element(By.CSS_SELECTOR, 'input[type="email"], input[name="email"], input[placeholder*="email" i], input[placeholder*="Email" i]')
        pass_input = driver.find_element(By.CSS_SELECTOR, 'input[type="password"]')
        print(f"  Found login form. Email field: {email_input.get_attribute('placeholder')}")
        
        email_input.clear()
        email_input.send_keys("komarewargaurav@gmail.com")
        pass_input.clear()
        pass_input.send_keys("Gaurav@123")
        
        # Find and click submit  
        submit = driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]')
        submit.click()
        time.sleep(3)
        print(f"  After login URL: {driver.current_url}")
    except Exception as e:
        print(f"  Login form error: {e}")
        # Maybe already logged in via token
        pass
    
    return driver


def check_websocket_state(driver):
    """Check if WebSocket connections are active."""
    ws_state = driver.execute_script("""
        // Check SmartTraderWS state  
        let result = {};
        try {
            // Check for ws object in the app
            result.localStorage_token = !!localStorage.getItem('st_token');
        } catch(e) { result.localStorage_error = e.message; }
        return result;
    """)
    return ws_state


def measure_ws_messages(driver, duration_sec=10):
    """Inject a listener to count WS messages and measure update frequency."""
    setup_script = """
    window.__ws_monitor = {
        messages: [],
        types: {},
        startTime: Date.now(),
        intercepted: false
    };
    
    // Intercept WebSocket messages
    const origWS = WebSocket.prototype.addEventListener;
    const origOnMsg = Object.getOwnPropertyDescriptor(WebSocket.prototype, 'onmessage');
    
    // Monkey-patch all existing WebSocket instances
    // We can't easily access existing instances, so let's use the Performance API
    // and also track new messages through PerformanceObserver
    
    // Instead, let's poll the stores directly
    return "monitor_setup";
    """
    driver.execute_script(setup_script)


def test_page_data_freshness(driver, page_name, page_url, check_script, duration=15):
    """
    Navigate to a page, then monitor how often data actually changes in the DOM.
    """
    print(f"\n{'='*60}")
    print(f"TESTING: {page_name}")
    print(f"{'='*60}")
    
    driver.get(page_url)
    time.sleep(3)  # Let page load
    
    print(f"  URL: {driver.current_url}")
    print(f"  Title: {driver.title}")
    
    # Take snapshots of key data elements over time
    snapshots = []
    for i in range(duration):
        try:
            data = driver.execute_script(check_script)
            snapshots.append({
                "time": time.time(),
                "elapsed": i,
                "data": data
            })
        except Exception as e:
            snapshots.append({
                "time": time.time(), 
                "elapsed": i,
                "error": str(e)
            })
        time.sleep(1)
    
    # Analyze: how often did data change?
    changes = 0
    last_data = None
    change_times = []
    for snap in snapshots:
        if "error" in snap:
            continue
        current = json.dumps(snap["data"], sort_keys=True)
        if last_data is not None and current != last_data:
            changes += 1
            change_times.append(snap["elapsed"])
        last_data = current
    
    total_samples = len([s for s in snapshots if "error" not in s])
    
    print(f"\n  Results over {duration}s:")
    print(f"  - Total samples: {total_samples}")
    print(f"  - Data changes: {changes}")
    if changes > 0:
        avg_interval = duration / changes
        print(f"  - Avg update interval: {avg_interval:.1f}s")
        print(f"  - Change at seconds: {change_times}")
    else:
        print(f"  - ⚠️  NO DATA CHANGES IN {duration}s!")
    
    # Print first and last data snapshot
    valid = [s for s in snapshots if "error" not in s]
    if valid:
        print(f"  - First data: {json.dumps(valid[0]['data'], default=str)[:200]}")
        print(f"  - Last data:  {json.dumps(valid[-1]['data'], default=str)[:200]}")
    
    return {
        "page": page_name,
        "changes": changes,
        "samples": total_samples,
        "duration": duration,
    }


def main():
    print("Starting Smart Trader refresh test with Selenium + Firefox headless")
    print(f"Target: {BASE}")
    
    driver = setup_driver()
    
    try:
        # 1. Login
        print("\n--- LOGGING IN ---")
        login(driver)
        
        # Check WS state
        ws_state = check_websocket_state(driver)
        print(f"  WS state: {ws_state}")
        
        # Wait for app to fully load after login
        time.sleep(3)
        print(f"  Current URL: {driver.current_url}")
        
        # Take screenshot
        driver.save_screenshot("/tmp/st_login_result.png")
        print("  Screenshot saved: /tmp/st_login_result.png")
        
        # Check page source for app state
        page_source = driver.page_source
        if "dashboard" in driver.current_url.lower() or "/app" in driver.current_url:
            print("  ✓ Login appears successful - on app pages")
        else:
            print(f"  ⚠️ May not be logged in. URL: {driver.current_url}")
            # Try navigating directly
            driver.get(f"{BASE}/app")
            time.sleep(2)
            print(f"  After nav to /app: {driver.current_url}")
            driver.save_screenshot("/tmp/st_app_page.png")
        
        # 2. Test Dashboard page
        results = []
        
        results.append(test_page_data_freshness(
            driver, "Dashboard", f"{BASE}/app",
            """
            // Check all visible dynamic data on dashboard
            let data = {};
            
            // Check Zustand store state
            try {
                // Look for any timestamp/update indicators in the DOM
                const timeEl = document.querySelector('[class*="text-text-muted"]');
                data.updateText = timeEl ? timeEl.textContent : 'none';
            } catch(e) {}
            
            // Get all numeric values visible on page (KPIs, balances, etc)
            const allNums = [];
            document.querySelectorAll('[class*="pnl"], [class*="kpi"], [class*="font-mono"], [class*="tabular"]').forEach(el => {
                if (el.textContent.trim()) allNums.push(el.textContent.trim().substring(0, 30));
            });
            data.numbers = allNums.slice(0, 20);
            
            // Check broker cards
            const cards = document.querySelectorAll('[class*="card"]');
            data.cardCount = cards.length;
            
            // Get all text content of key data areas
            const mainContent = document.querySelector('.overflow-y-auto, main, [class*="Dashboard"]');
            if (mainContent) {
                data.contentHash = mainContent.textContent.length;
                // Extract just the dynamic numbers
                const nums = mainContent.textContent.match(/₹[\\d,]+\\.?\\d*|[+-]?\\d+\\.\\d{2}%?/g);
                data.dynamicNums = nums ? nums.slice(0, 15) : [];
            }
            
            return data;
            """,
            duration=12
        ))
        
        # 3. Test Market page  
        results.append(test_page_data_freshness(
            driver, "Market", f"{BASE}/app/market",
            """
            let data = {};
            // Get all LTP values and change percentages on market page
            const prices = [];
            document.querySelectorAll('[class*="font-mono"], [class*="tabular"], [class*="ltp"]').forEach(el => {
                const t = el.textContent.trim();
                if (t && (t.includes('.') || t.includes('₹') || t.includes('%'))) {
                    prices.push(t.substring(0, 20));
                }
            });
            data.prices = prices.slice(0, 20);
            
            // Check index cards specifically
            const indexCards = document.querySelectorAll('[class*="grid"] > div');
            data.indexCardCount = indexCards.length;
            
            // Get all visible numbers
            const mainEl = document.querySelector('.overflow-y-auto, main');
            if (mainEl) {
                const nums = mainEl.textContent.match(/[\\d,]+\\.\\d{1,2}/g);
                data.allNums = nums ? nums.slice(0, 20) : [];
            }
            
            return data;
            """,
            duration=12
        ))
        
        # 4. Test Option Chain page
        results.append(test_page_data_freshness(
            driver, "Option Chain", f"{BASE}/app/options",
            """
            let data = {};
            // Get LTP values from option chain table
            const ltps = [];
            document.querySelectorAll('td, [class*="font-mono"]').forEach(el => {
                const t = el.textContent.trim();
                if (/^\\d+\\.\\d{1,2}$/.test(t)) ltps.push(t);
            });
            data.ltps = ltps.slice(0, 30);
            
            // Check underlying price
            const spotEl = document.querySelector('[class*="spot"], [class*="underlying"]');
            if (spotEl) data.spot = spotEl.textContent.trim();
            
            // Get any price that looks dynamic
            const mainEl = document.querySelector('.overflow-y-auto, main');
            if (mainEl) {
                const nums = mainEl.textContent.match(/\\d{3,6}\\.\\d{1,2}/g);
                data.allNums = nums ? nums.slice(0, 30) : [];
            }
            
            return data;
            """,
            duration=12
        ))
        
        # 5. Test Positions page
        results.append(test_page_data_freshness(
            driver, "Position Manager", f"{BASE}/app/positions",
            """
            let data = {};
            const mainEl = document.querySelector('.overflow-y-auto, main');
            if (mainEl) {
                data.content = mainEl.textContent.substring(0, 500);
                const nums = mainEl.textContent.match(/[+-]?[\\d,]+\\.\\d{1,2}/g);
                data.nums = nums ? nums.slice(0, 20) : [];
            }
            return data;
            """,
            duration=10
        ))
        
        # 6. Test Broker Accounts page
        results.append(test_page_data_freshness(
            driver, "Broker Accounts", f"{BASE}/app/brokers",
            """
            let data = {};
            const mainEl = document.querySelector('.overflow-y-auto, main');
            if (mainEl) {
                const nums = mainEl.textContent.match(/[+-]?[\\d,]+\\.\\d{1,2}/g);
                data.nums = nums ? nums.slice(0, 20) : [];
                data.contentLen = mainEl.textContent.length;
            }
            return data;
            """,
            duration=10
        ))
        
        # 7. Check WebSocket connections in browser console
        print(f"\n{'='*60}")
        print("WEBSOCKET CONNECTION CHECK")
        print(f"{'='*60}")
        
        ws_check = driver.execute_script("""
            let result = {};
            // Check performance entries for WebSocket
            const entries = performance.getEntriesByType('resource');
            const wsEntries = entries.filter(e => e.name.includes('ws/'));
            result.wsResourceCount = wsEntries.length;
            result.wsUrls = wsEntries.map(e => e.name).slice(0, 5);
            
            // Check console errors
            result.hasToken = !!localStorage.getItem('st_token');
            
            return result;
        """)
        print(f"  WS check: {json.dumps(ws_check, indent=2)}")
        
        # 8. Specifically check if WS messages are arriving
        print(f"\n{'='*60}")
        print("WEBSOCKET MESSAGE FLOW TEST")
        print(f"{'='*60}")
        
        # Inject WS message counter
        driver.execute_script("""
            window.__wsMessages = [];
            window.__origWsSend = WebSocket.prototype.send;
            
            // Patch all future WebSocket instances
            const OrigWebSocket = window.WebSocket;
            window.WebSocket = function(...args) {
                const ws = new OrigWebSocket(...args);
                const origOnMessage = null;
                
                ws.addEventListener('message', function(event) {
                    try {
                        const msg = JSON.parse(event.data);
                        window.__wsMessages.push({
                            type: msg.type,
                            ts: Date.now(),
                            url: args[0],
                            dataKeys: msg.data ? Object.keys(msg.data).slice(0, 5) : []
                        });
                        // Keep only last 200
                        if (window.__wsMessages.length > 200) {
                            window.__wsMessages = window.__wsMessages.slice(-200);
                        }
                    } catch(e) {}
                });
                
                return ws;
            };
            window.WebSocket.prototype = OrigWebSocket.prototype;
            window.WebSocket.CONNECTING = OrigWebSocket.CONNECTING;
            window.WebSocket.OPEN = OrigWebSocket.OPEN;
            window.WebSocket.CLOSING = OrigWebSocket.CLOSING;
            window.WebSocket.CLOSED = OrigWebSocket.CLOSED;
        """)
        
        # Navigate to dashboard to trigger fresh WS connections
        driver.get(f"{BASE}/app")
        time.sleep(8)
        
        # Check messages received
        ws_msgs = driver.execute_script("return window.__wsMessages || [];")
        print(f"  Messages captured in 8s: {len(ws_msgs)}")
        if ws_msgs:
            # Group by type
            type_counts = {}
            for m in ws_msgs:
                t = m.get("type", "unknown")
                type_counts[t] = type_counts.get(t, 0) + 1
            print(f"  Message types: {json.dumps(type_counts, indent=2)}")
            
            # Check timing
            if len(ws_msgs) >= 2:
                intervals = []
                for i in range(1, len(ws_msgs)):
                    intervals.append(ws_msgs[i]["ts"] - ws_msgs[i-1]["ts"])
                avg_int = sum(intervals) / len(intervals)
                print(f"  Avg interval between messages: {avg_int:.0f}ms")
                print(f"  Min interval: {min(intervals)}ms, Max: {max(intervals)}ms")
        else:
            print("  ⚠️ NO WS MESSAGES CAPTURED - WS might be using existing connections")
            
            # Try checking existing connections via browser internals
            print("\n  Checking existing WS connections via navigator...")
        
        # Summary
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        for r in results:
            status = "✓ OK" if r["changes"] >= r["duration"] // 3 else "⚠️ SLOW" if r["changes"] > 0 else "❌ NO UPDATES"
            print(f"  {status} | {r['page']}: {r['changes']} changes in {r['duration']}s")
        
    finally:
        driver.quit()
        print("\nTest complete.")


if __name__ == "__main__":
    main()
