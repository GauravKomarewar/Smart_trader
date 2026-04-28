import os
from scripts import shoonya_scriptmaster, fyers_scriptmaster, kite_scriptmaster, angelone_scriptmaster, upstox_scriptmaster, dhan_scriptmaster, groww_scriptmaster

output_dir = "/home/ubuntu/Smart_trader/backend/scripts/cleaned_exports"

brokers = {
    "shoonya": (shoonya_scriptmaster, "shoonya_cleaned.csv"),
    "fyers": (fyers_scriptmaster, "fyers_cleaned.csv"),
    "kite": (kite_scriptmaster, "kite_cleaned.csv"),
    "angelone": (angelone_scriptmaster, "angelone_cleaned.csv"),
    "upstox": (upstox_scriptmaster, "upstox_cleaned.csv"),
    "dhan": (dhan_scriptmaster, "dhan_cleaned.csv"),
    "groww": (groww_scriptmaster, "groww_cleaned.csv")
}

for name, (module, filename) in brokers.items():
    try:
        path = os.path.join(output_dir, filename)
        module.export_cleaned_csv(path)
        print(f"{name}: Success")
    except Exception as e:
        print(f"{name}: Failed - {e}")

