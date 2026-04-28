#!/usr/bin/env python3
"""
Interactive inspector for:
1) symbols table storage layout + sample rows
2) interactive symbol search
3) live strategy run diagnostics

Usage:
  /home/ubuntu/.venv/bin/python backend/scripts/inspect_symbols_and_strategy.py
  /home/ubuntu/.venv/bin/python backend/scripts/inspect_symbols_and_strategy.py --run-id <run_id>
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, time as dt_time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

# Ensure backend/ is importable when script is run directly.
_HERE = Path(__file__).resolve().parent
_BACKEND = _HERE.parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from db.trading_db import get_trading_conn
from db.symbols_db import search_symbols

try:
    from tabulate import tabulate
except Exception:
    tabulate = None


def _fmt_val(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.2f}"
    if isinstance(value, (dict, list)):
        text = json.dumps(value, default=str)
    else:
        text = str(value)
    return text.replace("\n", " ")


def _render_table(rows: List[Dict[str, Any]], columns: List[str], max_col_width: int = 42) -> str:
    if not rows:
        return "(no rows)"

    if tabulate is not None:
        clipped_rows: List[List[str]] = []
        for row in rows:
            vals: List[str] = []
            for col in columns:
                txt = _fmt_val(row.get(col, ""))
                if len(txt) > max_col_width:
                    txt = txt[: max_col_width - 3] + "..."
                vals.append(txt)
            clipped_rows.append(vals)
        return tabulate(clipped_rows, headers=columns, tablefmt="github")

    widths: List[int] = []
    for col in columns:
        width = len(col)
        for row in rows:
            width = max(width, len(_fmt_val(row.get(col, ""))))
        widths.append(min(width, max_col_width))

    def _clip(text: str, width: int) -> str:
        if len(text) <= width:
            return text
        if width <= 3:
            return text[:width]
        return text[: width - 3] + "..."

    header = " | ".join(_clip(c, w).ljust(w) for c, w in zip(columns, widths))
    sep = "-+-".join("-" * w for w in widths)

    body_lines: List[str] = []
    for row in rows:
        line = " | ".join(
            _clip(_fmt_val(row.get(c, "")), w).ljust(w)
            for c, w in zip(columns, widths)
        )
        body_lines.append(line)

    return "\n".join([header, sep] + body_lines)


def _print_section(title: str) -> None:
    print("\n" + "=" * 96)
    print(title)
    print("=" * 96)


def _fetch_rows(sql: str, params: Optional[Iterable[Any]] = None) -> List[Dict[str, Any]]:
    conn = get_trading_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, tuple(params or ()))
        columns = [d[0] for d in cur.description]
        rows = [dict(zip(columns, rec)) for rec in cur.fetchall()]
        return rows
    finally:
        conn.close()


def _show_symbols_storage(sample_limit: int = 12) -> None:
    _print_section("1) SYMBOLS TABLE STORAGE OVERVIEW")

    summary = _fetch_rows(
        """
        SELECT COUNT(*) AS total_rows,
               COUNT(*) FILTER (WHERE exchange = 'NFO') AS nfo_rows,
               COUNT(*) FILTER (WHERE instrument_type = 'OPT') AS opt_rows,
               MAX(broker_coverage) AS max_broker_coverage
        FROM symbols
        """
    )
    if summary:
        row = summary[0]
        print(
            f"Total rows: {row['total_rows']} | "
            f"NFO: {row['nfo_rows']} | "
            f"Options: {row['opt_rows']} | "
            f"Max broker_coverage: {row['max_broker_coverage']}"
        )

    sample_rows = _fetch_rows(
        """
        SELECT exchange,
               symbol,
               trading_symbol,
               normalized_trading_symbol,
               instrument_type,
               expiry,
               strike,
               option_type,
               lot_size,
               tick_size,
               fyers_symbol,
               shoonya_tsym,
               shoonya_token,
               angelone_tsym,
               dhan_security_id,
               kite_instrument_token,
               upstox_ikey,
               groww_token,
               broker_coverage
        FROM symbols
        ORDER BY broker_coverage DESC, exchange, symbol, expiry NULLS LAST, strike NULLS LAST
        LIMIT %s
        """,
        (sample_limit,),
    )

    columns = [
        "exchange",
        "symbol",
        "trading_symbol",
        "normalized_trading_symbol",
        "instrument_type",
        "expiry",
        "strike",
        "option_type",
        "lot_size",
        "tick_size",
        "fyers_symbol",
        "shoonya_tsym",
        "shoonya_token",
        "angelone_tsym",
        "dhan_security_id",
        "kite_instrument_token",
        "upstox_ikey",
        "groww_token",
        "broker_coverage",
    ]
    print("\nSample rows:")
    print(_render_table(sample_rows, columns))


def _interactive_symbol_search(default_limit: int = 10) -> None:
    _print_section("2) INTERACTIVE SYMBOL SEARCH")
    print("Enter a search query (example: NIFTY24400CE, BANKNIFTY, RELIANCE).")
    print("Leave query empty to skip this section.")

    while True:
        query = input("\nSearch query: ").strip()
        if not query:
            print("Search skipped.")
            return

        exch = input("Exchange filter (optional, e.g. NFO/NSE): ").strip().upper()
        lim_raw = input(f"Result limit [{default_limit}]: ").strip()
        try:
            limit = int(lim_raw) if lim_raw else default_limit
        except ValueError:
            limit = default_limit
        mode_raw = input("Match mode [exact/prefix/fuzzy] (default: exact): ").strip().lower()
        match_mode = mode_raw if mode_raw in ("exact", "prefix", "fuzzy") else "exact"

        results = _search_symbols_interactive(
            query=query,
            exchange=exch,
            limit=limit,
            match_mode=match_mode,
        )
        if not results:
            print("No results found.")
        else:
            cols = [
                "exchange",
                "symbol",
                "trading_symbol",
                "normalized_trading_symbol",
                "instrument_type",
                "expiry",
                "strike",
                "option_type",
                "lot_size",
                "tick_size",
                "fyers_symbol",
                "shoonya_tsym",
                "shoonya_token",
                "angelone_tsym",
                "angelone_token",
                "dhan_security_id",
                "kite_instrument_token",
                "upstox_ikey",
                "groww_token",
                "broker_coverage",
            ]
            print(f"\nFound {len(results)} row(s):")
            print(_render_table(results, cols))

        again = input("\nSearch again? [y/N]: ").strip().lower()
        if again != "y":
            return


def _parse_hhmm(value: str) -> Optional[dt_time]:
    try:
        return datetime.strptime(str(value), "%H:%M").time()
    except Exception:
        return None


def _search_symbols_interactive(
    query: str,
    exchange: str,
    limit: int,
    match_mode: str,
) -> List[Dict[str, Any]]:
    q = str(query or "").strip().upper()
    exch = str(exchange or "").strip().upper()
    lim = max(1, int(limit or 10))

    if match_mode == "fuzzy":
        return search_symbols(query=q, exchange=exch, limit=lim)

    conn = get_trading_conn()
    try:
        cur = conn.cursor()

        if match_mode == "exact":
            # Exact mode is strict-first:
            # 1) exact trading_symbol
            # 2) fallback to normalized_trading_symbol only if #1 yields no rows
            where_ts = ["UPPER(trading_symbol) = %s"]
            params_ts: List[Any] = [q]
            if exch:
                where_ts.append("exchange = %s")
                params_ts.append(exch)

            cur.execute(
                f"""
                SELECT *
                FROM symbols
                WHERE {' AND '.join(where_ts)}
                ORDER BY broker_coverage DESC, exchange, trading_symbol
                LIMIT %s
                """,
                params_ts + [lim],
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, rec)) for rec in cur.fetchall()]
            if rows:
                return rows

            where_norm = ["UPPER(normalized_trading_symbol) = %s"]
            params_norm: List[Any] = [q]
            if exch:
                where_norm.append("exchange = %s")
                params_norm.append(exch)

            cur.execute(
                f"""
                SELECT *
                FROM symbols
                WHERE {' AND '.join(where_norm)}
                ORDER BY broker_coverage DESC, exchange, trading_symbol
                LIMIT %s
                """,
                params_norm + [lim],
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, rec)) for rec in cur.fetchall()]
            if rows:
                return rows

            # Fallback for users searching raw symbol or broker-native exact values.
            where_fallback = [
                "(" \
                "UPPER(symbol) = %s OR "
                "UPPER(fyers_symbol) = %s OR "
                "UPPER(shoonya_tsym) = %s OR "
                "UPPER(angelone_tsym) = %s"
                ")",
            ]
            params_fallback: List[Any] = [q, q, q, q]
            if exch:
                where_fallback.append("exchange = %s")
                params_fallback.append(exch)

            cur.execute(
                f"""
                SELECT *
                FROM symbols
                WHERE {' AND '.join(where_fallback)}
                ORDER BY broker_coverage DESC, exchange, symbol
                LIMIT %s
                """,
                params_fallback + [lim],
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, rec)) for rec in cur.fetchall()]

        # Prefix mode
        where_prefix = [
            "(" \
            "UPPER(trading_symbol) LIKE %s OR "
            "UPPER(normalized_trading_symbol) LIKE %s OR "
            "UPPER(symbol) LIKE %s"
            ")",
        ]
        like_q = q + "%"
        params_prefix: List[Any] = [like_q, like_q, like_q]
        if exch:
            where_prefix.append("exchange = %s")
            params_prefix.append(exch)

        cur.execute(
            f"""
            SELECT *
            FROM symbols
            WHERE {' AND '.join(where_prefix)}
            ORDER BY
                CASE WHEN UPPER(trading_symbol) = %s THEN 0 ELSE 1 END,
                broker_coverage DESC,
                exchange,
                trading_symbol
            LIMIT %s
            """,
            params_prefix + [q, lim],
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, rec)) for rec in cur.fetchall()]
    finally:
        conn.close()


def _analyse_run(run_id: str) -> None:
    run_rows = _fetch_rows("SELECT * FROM strategy_runs WHERE run_id = %s", (run_id,))
    if not run_rows:
        print(f"Run not found: {run_id}")
        return

    run = run_rows[0]
    legs = _fetch_rows(
        """
        SELECT tag, side, option_type, strike, expiry, qty, lot_size,
               entry_price, ltp, exit_price, is_active, order_status,
               trading_symbol, order_id, created_at, updated_at
        FROM strategy_legs
        WHERE run_id = %s
        ORDER BY created_at, tag
        """,
        (run_id,),
    )

    event_counts = _fetch_rows(
        """
        SELECT event_type, COUNT(*) AS cnt
        FROM strategy_events
        WHERE run_id = %s
        GROUP BY event_type
        ORDER BY event_type
        """,
        (run_id,),
    )

    recent_events = _fetch_rows(
        """
        SELECT event_type, reason, details, created_at
        FROM strategy_events
        WHERE run_id = %s
        ORDER BY created_at DESC
        LIMIT 20
        """,
        (run_id,),
    )

    print("\nRun details:")
    run_view = {
        "run_id": run.get("run_id"),
        "strategy_name": run.get("strategy_name"),
        "symbol": run.get("symbol"),
        "exchange": run.get("exchange"),
        "paper_mode": run.get("paper_mode"),
        "status": run.get("status"),
        "entered_today": run.get("entered_today"),
        "total_trades_today": run.get("total_trades_today"),
        "cumulative_daily_pnl": run.get("cumulative_daily_pnl"),
        "started_at": run.get("started_at"),
        "last_tick_at": run.get("last_tick_at"),
        "stopped_at": run.get("stopped_at"),
    }
    print(_render_table([run_view], list(run_view.keys())))

    print("\nLegs:")
    leg_cols = [
        "tag",
        "side",
        "option_type",
        "strike",
        "expiry",
        "qty",
        "lot_size",
        "entry_price",
        "ltp",
        "exit_price",
        "is_active",
        "order_status",
        "trading_symbol",
        "order_id",
    ]
    print(_render_table(legs, leg_cols))

    print("\nEvent counts:")
    print(_render_table(event_counts, ["event_type", "cnt"]))

    print("\nLatest events (20):")
    print(_render_table(recent_events, ["event_type", "reason", "created_at", "details"]))

    issues: List[str] = []
    counts = {r["event_type"]: int(r["cnt"]) for r in event_counts}

    exit_count = counts.get("EXIT", 0)
    if exit_count > 100:
        issues.append(
            f"High EXIT event volume detected: {exit_count} EXIT events (possible repeated exit trigger loop)."
        )

    active_legs = sum(1 for l in legs if l.get("is_active"))
    if str(run.get("status", "")).upper() == "RUNNING" and active_legs == 0:
        issues.append("Run status is RUNNING but all legs are inactive (likely missing auto-stop).")

    prefixed = [l.get("trading_symbol", "") for l in legs if ":" in str(l.get("trading_symbol", ""))]
    if prefixed:
        issues.append(
            "Canonical symbol issue: one or more strategy_legs.trading_symbol values contain exchange prefixes: "
            + ", ".join(prefixed)
        )

    cfg = run.get("config_snapshot") or {}
    if isinstance(cfg, str):
        try:
            cfg = json.loads(cfg)
        except Exception:
            cfg = {}

    strategy_exit = _parse_hhmm(
        ((cfg.get("exit") or {}).get("time") or {}).get("strategy_exit_time", "")
    )
    eod_exit = _parse_hhmm((cfg.get("timing") or {}).get("eod_exit_time", ""))
    cutoff = min([t for t in [strategy_exit, eod_exit] if t is not None], default=None)
    if cutoff and run.get("last_tick_at"):
        try:
            last_tick_time = run["last_tick_at"].time()
            if last_tick_time > cutoff and str(run.get("status", "")).upper() == "RUNNING":
                issues.append(
                    f"Run kept ticking past cutoff {cutoff.strftime('%H:%M')} (last_tick_at={run['last_tick_at']})."
                )
        except Exception:
            pass

    print("\nDetected issues:")
    if not issues:
        print("No obvious issues detected from DB snapshots.")
    else:
        for idx, msg in enumerate(issues, 1):
            print(f"{idx}. {msg}")


def _strategy_analysis(default_run_id: Optional[str]) -> None:
    _print_section("3) STRATEGY RUN ANALYSIS")

    latest_runs = _fetch_rows(
        """
        SELECT run_id,
               strategy_name,
               symbol,
               exchange,
               paper_mode,
               status,
               entered_today,
               total_trades_today,
               cumulative_daily_pnl,
               started_at,
               last_tick_at
        FROM strategy_runs
        ORDER BY started_at DESC
        LIMIT 10
        """
    )

    print("Latest strategy runs:")
    print(
        _render_table(
            latest_runs,
            [
                "run_id",
                "strategy_name",
                "symbol",
                "exchange",
                "paper_mode",
                "status",
                "entered_today",
                "total_trades_today",
                "cumulative_daily_pnl",
                "started_at",
                "last_tick_at",
            ],
        )
    )

    pick_default = default_run_id or (latest_runs[0]["run_id"] if latest_runs else "")
    run_id = input(f"\nRun ID to inspect [{pick_default}]: ").strip() or pick_default
    if not run_id:
        print("Run analysis skipped (no run id provided).")
        return

    _analyse_run(run_id)


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect symbols storage, search symbols, and analyze strategy runs")
    parser.add_argument("--run-id", default="", help="Run ID to deep-analyze (if omitted, prompts interactively)")
    parser.add_argument("--sample-limit", type=int, default=12, help="Symbols table sample rows to display")
    parser.add_argument("--search-limit", type=int, default=10, help="Default result limit for interactive search")
    args = parser.parse_args()

    _show_symbols_storage(sample_limit=max(1, args.sample_limit))
    _interactive_symbol_search(default_limit=max(1, args.search_limit))
    _strategy_analysis(default_run_id=args.run_id.strip() or None)

    print("\nInspection complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
