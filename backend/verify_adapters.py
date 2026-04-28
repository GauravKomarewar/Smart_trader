import sys
import os
from broker.adapters.fyers_adapter import FyersAdapter
from broker.adapters.shoonya_adapter import ShoonyaAdapter
from broker.adapters.dhan_adapter import DhanAdapter
from broker.adapters.paper_adapter import PaperAdapter

def test_init():
    try:
        # Paper adapter
        paper = PaperAdapter(account_id="TEST_AC", client_id="TEST_CLIENT")
        print("PaperAdapter initialized")
        
        creds = {"api_key": "test", "token": "test", "user_id": "test", "password": "test", "client_id": "test", "secret_key": "test"}
        
        # Fyers
        try:
            # Check FyersAdapter __init__ signature if possible, but let's try common patterns
            fyers = FyersAdapter(credentials=creds)
            print("FyersAdapter instantiated")
        except Exception as e:
            print(f"FyersAdapter instantiation failed: {e}")

        # Shoonya
        try:
            shoonya = ShoonyaAdapter(credentials=creds)
            print("ShoonyaAdapter instantiated")
        except Exception as e:
            print(f"ShoonyaAdapter instantiation failed: {e}")

    except Exception as e:
        print(f"General error: {e}")

if __name__ == "__main__":
    test_init()
