#!/usr/bin/env python3
"""
Simulation harness for testing strategies.
"""

import json
import time
import logging
from trading.strategy_runner.executor import StrategyExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    # Example strategy configuration (short straddle)
    config = {
        "schema_version": "4.0",
        "name": "Test Straddle",
        "identity": {
            "exchange": "NFO",
            "underlying": "NIFTY",
            "lots": 1
        },
        "timing": {
            "entry_window_start": "09:15",
            "entry_window_end": "14:00"
        },
        "schedule": {
            "active_days": ["mon", "tue", "wed", "thu", "fri"],
            "expiry_mode": "weekly_current"
        },
        "entry": {
            "global_conditions": [],
            "legs": [
                {
                    "tag": "LEG@1",
                    "instrument": "OPT",
                    "side": "SELL",
                    "option_type": "CE",
                    "lots": 1,
                    "strike_mode": "standard",
                    "strike_selection": "atm",
                    "conditions": []
                },
                {
                    "tag": "LEG@2",
                    "instrument": "OPT",
                    "side": "SELL",
                    "option_type": "PE",
                    "lots": 1,
                    "strike_mode": "standard",
                    "strike_selection": "atm",
                    "conditions": []
                }
            ]
        },
        "adjustment": {
            "rules": []
        },
        "exit": {
            "profit_target": {"amount": 500, "action": "exit_all"},
            "stop_loss": {"amount": 300, "action": "exit_all"},
            "time": {"strategy_exit_time": "15:20"}
        }
    }

    with open("test_strategy.json", "w") as f:
        json.dump(config, f, indent=2)

    executor = StrategyExecutor("test_strategy.json", "test_state.pkl")
    try:
        executor.run(interval_sec=2)
    except KeyboardInterrupt:
        print("Simulation stopped.")