import os
import sys

# Ensure backend directory is in sys.path
sys.path.append('/home/ubuntu/Smart_trader/backend')

from scripts import (
    shoonya_scriptmaster, 
    fyers_scriptmaster, 
    kite_scriptmaster, 
    angelone_scriptmaster, 
    upstox_scriptmaster, 
    dhan_scriptmaster, 
    groww_scriptmaster
)

brokers = {
    "shoonya": shoonya_scriptmaster,
    "fyers": fyers_scriptmaster,
    "kite": kite_scriptmaster,
    "angelone": angelone_scriptmaster,
    "upstox": upstox_scriptmaster,
    "dhan": dhan_scriptmaster,
    "groww": groww_scriptmaster
}

output_dir = "/home/ubuntu/Smart_trader/backend/scripts/cleaned_exports"
results = []

for name, module in brokers.items():
    output_path = os.path.join(output_dir, f"{name}_cleaned.csv")
    try:
        module.export_cleaned_csv(output_path)
        results.append((name, "Success", output_path))
    except Exception as e:
        results.append((name, "Failed", str(e)))

print(f"{'Broker':<12} | {'Status':<8} | {'Output Path or Error'}")
print("-" * 60)
for broker, status, detail in results:
    print(f"{broker:<12} | {status:<8} | {detail}")
