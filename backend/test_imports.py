import os
import sys

def check_imports():
    paths = [
        "broker.account_risk",
        "trading.bot_core",
        "managers.supreme_manager",
        "broker.adapters.fyers_adapter",
        "broker.adapters.shoonya_adapter"
    ]
    for path in paths:
        try:
            __import__(path)
            print(f"PASS: {path}")
        except ImportError as e:
            print(f"FAIL: {path} - {e}")
        except Exception as e:
            print(f"LOAD ERROR: {path} - {e}")

if __name__ == "__main__":
    check_imports()
