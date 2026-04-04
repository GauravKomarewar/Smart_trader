"""
fresh_strategy — Self-contained JSON-driven strategy engine
============================================================

Reads strategy rules from JSON configs built by dashboard strategy_builder.html.
Reads live option chain data from SQLite databases.
Generates alerts and sends to process_alert() via OMS — never executes directly.

Completely isolated from the old strategies/ folder.
"""
