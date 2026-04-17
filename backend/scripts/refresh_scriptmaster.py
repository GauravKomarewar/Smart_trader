#!/usr/bin/env python3
"""
Smart Trader — Daily Scriptmaster Refresh (one-shot script)
============================================================

Downloads fresh scriptmaster CSVs from ALL broker APIs and writes
normalised data into the PostgreSQL symbols table.

Designed to be run once per day at 08:45 IST, either via:
  • systemd timer  (see scripts/systemd/smart-trader-refresh.timer)
  • cron:          45 3 * * * /home/ubuntu/.venv/bin/python3
                       /home/ubuntu/Smart_trader/backend/scripts/refresh_scriptmaster.py

Usage:
  python3 refresh_scriptmaster.py              # force-refresh (default)
  python3 refresh_scriptmaster.py --no-force   # skip if already current today

The script adds backend/ to sys.path so it can be invoked directly without
activating any virtual environment wrapper.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

# ── Resolve backend root so this script works when run directly ──────────────
_HERE = Path(__file__).resolve().parent      # …/backend/scripts/
_BACKEND = _HERE.parent                      # …/backend/
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("refresh_scriptmaster")


# ── Main pipeline ─────────────────────────────────────────────────────────────

def main(force: bool = True) -> int:
    """Run the two-step scriptmaster refresh pipeline.

    Returns:
        0  on success
        1  on error
    """
    t0 = time.monotonic()
    log.info("=" * 64)
    log.info("Smart Trader — Scriptmaster Daily Refresh")
    log.info("=" * 64)

    # ── Step 1: Download fresh CSVs from all broker APIs ─────────────────────
    log.info("Step 1/2: Downloading broker scriptmasters …")
    try:
        from scripts.unified_scriptmaster import refresh_all  # noqa: PLC0415

        counts = refresh_all(force=force)
        total_downloaded = 0
        for broker, count in sorted((counts or {}).items()):
            log.info("  %-14s  %6d symbols", broker, count or 0)
            total_downloaded += count or 0
        log.info(
            "  ─── %d symbols downloaded across %d brokers",
            total_downloaded,
            len(counts or {}),
        )
    except Exception as exc:
        log.error("Scriptmaster download FAILED: %s", exc, exc_info=True)
        return 1

    # ── Step 2: Sanitise + write to PostgreSQL symbols table ─────────────────
    log.info("Step 2/2: Populating symbols DB …")
    try:
        from db.symbols_db import populate_symbols_db  # noqa: PLC0415

        db_result = populate_symbols_db(force=force)
        if isinstance(db_result, dict):
            db_total = 0
            for broker, cnt in sorted(db_result.items()):
                log.info("  %-14s  %6d rows", broker, cnt or 0)
                db_total += cnt or 0
            log.info("  ─── %d total rows inserted/updated", db_total)
        else:
            log.info("  Total rows: %s", db_result)
    except Exception as exc:
        log.error("symbols DB populate FAILED: %s", exc, exc_info=True)
        return 1

    # ── Invalidate normalizer LRU cache so the new data is picked up ─────────
    try:
        from broker.symbol_normalizer import get_normalizer  # noqa: PLC0415

        get_normalizer().invalidate_cache()
        log.info("Symbol normalizer cache cleared")
    except Exception:
        pass  # normalizer may not be running in-process when this is a standalone run

    elapsed = time.monotonic() - t0
    log.info("=" * 64)
    log.info("Refresh completed in %.1f s", elapsed)
    log.info("=" * 64)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Refresh all broker scriptmasters and update the Smart Trader symbols DB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 refresh_scriptmaster.py             # force-refresh all broker data
  python3 refresh_scriptmaster.py --no-force  # skip if already fetched today
        """,
    )
    parser.add_argument(
        "--no-force",
        dest="force",
        action="store_false",
        default=True,
        help="Skip download if scriptmaster data is already current for today",
    )
    args = parser.parse_args()
    sys.exit(main(force=args.force))
