#!/usr/bin/env python3
"""
market_reader.py — Live SQLite Option Chain Reader (Production‑Ready)
======================================================================

Reads live option chain data from SQLite databases in:
    market_data/option_chain/data/{EXCHANGE}_{SYMBOL}_{DD-MMM-YYYY}.sqlite

Supports multiple expiries, strike step auto‑detection, snapshot freshness checks,
and robust match_leg attribute handling. Fully compatible with the Universal
Multi‑Leg Strategy Execution Engine.
"""

import glob
import logging
import re
import sqlite3
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from .models import StrikeConfig, OptionType, StrikeMode
from scripts.scriptmaster import get_future, universal_symbol_search

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_FOLDER = _PROJECT_ROOT / "data" / "option_chain"

# Allowed match parameters for strike selection (map to SQL expressions)
MATCH_PARAM_TO_SQL = {
    "delta": "delta",
    "abs_delta": "ABS(delta)",
    "gamma": "gamma",
    "theta": "theta",
    "abs_theta": "ABS(theta)",
    "vega": "vega",
    "iv": "iv",
    "ltp": "ltp",
    "oi": "oi",
    "volume": "volume",
    "strike": "strike",
    # Proxy mapping; exact moneyness matching is handled in resolve_strike().
    "moneyness": "strike",
}

# Adaptive tolerance for find_option_by_criteria depending on the Greek/attribute.
# None → proportional: max(base, |target| * fraction).  Keeps match_leg reliable
# for ANY parameter, not just delta.
def _adaptive_tolerance(param: str, target_value: float) -> float:
    _FIXED = {
        "delta": 0.15, "abs_delta": 0.15,
        "gamma": 0.0005, "theta": 5.0, "abs_theta": 5.0,
        "vega": 2.0, "iv": 10.0,
    }
    if param in _FIXED:
        return _FIXED[param]
    # Proportional for ltp, oi, volume, strike, moneyness
    return max(50.0, abs(target_value) * 0.25)

# Fallback lot sizes when DB and ScriptMaster lookups fail.
# This dict should be extended as needed; final fallback is 1.
DEFAULT_LOT_SIZES = {
    "NIFTY": 50,
    "BANKNIFTY": 25,
    "FINNIFTY": 40,
    "MIDCPNIFTY": 75,
    "SENSEX": 10,
    "CRUDEOIL": 100,
    "CRUDEOILM": 100,
    "GOLD": 1,
    "GOLDM": 1,
    "SILVER": 1,
    "SILVERM": 1,
}


class MarketReader:
    """
    Production‑ready market reader with connection pooling, freshness checks,
    and dynamic strike step detection.
    """

    def __init__(self, exchange: str, symbol: str, max_stale_seconds: int = 120):
        """
        Args:
            exchange: NFO, MCX, etc.
            symbol: NIFTY, BANKNIFTY, etc.
            max_stale_seconds: Maximum allowed age of snapshot (seconds) before
                                warning in data retrieval (default 120s).
        """
        self.exchange = exchange.upper()
        self.symbol = symbol.upper()
        self.max_stale_seconds = max_stale_seconds
        # Store connection info per expiry key: {key: {'conn': conn, 'path': str, 'mtime': float}}
        self._conn_info: Dict[str, Dict] = {}
        self._strike_step_cache: Dict[str, float] = {}  # expiry -> strike step
        self._prev_total_oi: Dict[str, Dict[str, float]] = {}
    # ----------------------------------------------------------------------
    # Internal helpers
    # ----------------------------------------------------------------------
    def _resolve_db_path(self, expiry: Optional[str] = None) -> Optional[str]:
        """Resolve full path to SQLite file; if expiry None, pick nearest future."""
        folder = DB_FOLDER
        if not folder.exists():
            logger.error(f"DB folder not found: {folder}")
            return None

        if expiry is not None:
            filename = f"{self.exchange}_{self.symbol}_{expiry}.sqlite"
            file_path = folder / filename
            return str(file_path) if file_path.exists() else None

        # No expiry: auto-resolve to nearest future expiry
        pattern = f"{self.exchange}_{self.symbol}_*.sqlite"
        matches = list(folder.glob(pattern))
        if not matches:
            logger.error(f"No database files matching {pattern}")
            return None

        today = date.today()
        candidates = []
        for f in matches:
            name = f.stem
            parts = name.split("_", 2)
            if len(parts) < 3:
                continue
            date_str = parts[2]
            try:
                expiry_date = datetime.strptime(date_str, "%d-%b-%Y").date()
                candidates.append((f, expiry_date))
            except ValueError:
                continue

        if not candidates:
            logger.warning(f"Using first match: {matches[0].name}")
            return str(matches[0])

        future = [(f, d) for f, d in candidates if d >= today]
        if future:
            future.sort(key=lambda x: x[1])
            chosen = future[0]
        else:
            candidates.sort(key=lambda x: x[1], reverse=True)
            chosen = candidates[0]

        logger.info(f"Resolved default DB: {chosen[0].name} (expiry: {chosen[1]})")
        return str(chosen[0])

    def _get_connection(self, expiry: Optional[str] = None) -> Optional[sqlite3.Connection]:
        key = expiry or "default"
        # Resolve the expected database path
        path = self._resolve_db_path(expiry)
        if not path:
            return None

        # Check if we have a cached connection for this key with the same path
        if key in self._conn_info:
            info = self._conn_info[key]
            if info['path'] == path:
                # Same path – check if file modification time changed
                try:
                    current_mtime = Path(path).stat().st_mtime
                    if current_mtime == info['mtime']:
                        # Also verify connection is alive
                        info['conn'].execute("SELECT 1")
                        return info['conn']
                except Exception:
                    # File missing, stat error, or connection dead – will close and reopen
                    pass
            # If we reach here, the cached connection is stale – close it
            try:
                info['conn'].close()
            except Exception:
                pass
            del self._conn_info[key]

        # Open a new connection
        path_obj = Path(path)
        if not path_obj.exists() or path_obj.stat().st_size < 1024:
            logger.error(f"DB file missing or too small: {path}")
            return None

        try:
            uri = f"file:{path}?mode=ro"
            conn = sqlite3.connect(uri, uri=True, timeout=5)
            conn.row_factory = sqlite3.Row

            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            table_names = {row[0] for row in tables}
            required_tables = {"option_chain", "meta"}
            if not required_tables.issubset(table_names):
                logger.error(f"Missing tables: {required_tables - table_names}")
                conn.close()
                return None

            row_count = conn.execute("SELECT COUNT(*) FROM option_chain").fetchone()[0]
            if row_count == 0:
                logger.error(f"option_chain table empty in {path}")
                conn.close()
                return None

            # Get file modification time
            mtime = path_obj.stat().st_mtime

            logger.info(f"Connected to DB: {path_obj.name} ({row_count} rows)")
            self._conn_info[key] = {'conn': conn, 'path': path, 'mtime': mtime}
            return conn
        except Exception as e:
            logger.error(f"Connection failed: {path} | {e}")
            return None

    def _close_connection(self, key: str):
        if key in self._conn_info:
            try:
                self._conn_info[key]['conn'].close()
            except Exception:
                pass
            del self._conn_info[key]

    def close_all(self):
        for key in list(self._conn_info.keys()):
            self._close_connection(key)

    def __del__(self):
        self.close_all()

    def _get_strike_step(self, expiry: Optional[str] = None) -> float:
        """
        Determine the strike price step (minimum difference between consecutive strikes)
        from the option chain. Caches the result per expiry.
        """
        key = expiry or "default"
        if key in self._strike_step_cache:
            return self._strike_step_cache[key]

        conn = self._get_connection(expiry)
        if not conn:
            return 50.0  # fallback
        assert conn is not None  # for type checker

        try:
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT strike FROM option_chain ORDER BY strike")
            strikes = [row[0] for row in cur.fetchall()]
            if len(strikes) < 2:
                return 50.0

            diffs = [strikes[i+1] - strikes[i] for i in range(len(strikes)-1)]
            # Filter out zero (shouldn't happen, but safe)
            diffs = [d for d in diffs if d > 0]
            if not diffs:
                return 50.0

            # Use most common difference (mode)
            step = Counter(diffs).most_common(1)[0][0]
            self._strike_step_cache[key] = step
            logger.debug(f"Strike step for {key}: {step}")
            return step
        except Exception as e:
            logger.error(f"Failed to compute strike step: {e}")
            return 50.0

    def _check_freshness(self, expiry: Optional[str] = None):
        """Warn (but don't crash) if snapshot is older than max_stale_seconds.
        Uses time-of-day awareness: pre/post market allows 5x staler data."""
        age = self.get_snapshot_age_seconds(expiry)
        now = datetime.now()
        market_start = now.replace(hour=9, minute=15, second=0, microsecond=0)
        market_end = now.replace(hour=15, minute=30, second=0, microsecond=0)
        # Allow staler data outside market hours
        if now < market_start or now > market_end:
            effective_max = self.max_stale_seconds * 5
        else:
            effective_max = self.max_stale_seconds
        if age > effective_max:
            logger.warning(
                "Option chain data stale: %.1fs > %.1fs "
                "(continuing with stale data)",
                age, effective_max,
            )

    # ----------------------------------------------------------------------
    # Public data retrieval methods (with optional expiry)
    # ----------------------------------------------------------------------
    def get_meta(self, expiry: Optional[str] = None) -> Dict[str, str]:
        conn = self._get_connection(expiry)
        if not conn:
            return {}
        assert conn is not None
        try:
            cur = conn.cursor()
            cur.execute("SELECT key, value FROM meta")
            return {row["key"]: row["value"] for row in cur.fetchall()}
        except Exception as e:
            logger.error(f"get_meta error: {e}")
            return {}

    def get_spot_price(self, expiry: Optional[str] = None) -> float:
        meta = self.get_meta(expiry)
        try:
            return float(meta.get("spot_ltp", 0))
        except (ValueError, TypeError):
            return 0.0

    def get_atm_strike(self, expiry: Optional[str] = None) -> float:
        meta = self.get_meta(expiry)
        try:
            return float(meta.get("atm", 0))
        except (ValueError, TypeError):
            return 0.0

    def get_fut_ltp(self, expiry: Optional[str] = None) -> float:
        meta = self.get_meta(expiry)
        try:
            return float(meta.get("fut_ltp", 0))
        except (ValueError, TypeError):
            return 0.0

    def get_snapshot_age_seconds(self, expiry: Optional[str] = None) -> float:
        meta = self.get_meta(expiry)
        try:
            ts = float(meta.get("snapshot_ts", 0))
            return datetime.now().timestamp() - ts
        except (ValueError, TypeError):
            return 99999.0

    def get_lot_size(self, expiry: Optional[str] = None) -> int:
        conn = self._get_connection(expiry)
        if not conn:
            conn_lot = None
        else:
            conn_lot = conn
        if conn_lot:
            assert conn_lot is not None
            try:
                cur = conn_lot.cursor()
                cur.execute("SELECT lot_size FROM option_chain WHERE lot_size IS NOT NULL LIMIT 1")
                row = cur.fetchone()
                if row and row["lot_size"]:
                    return int(row["lot_size"])
            except Exception:
                pass

        # Fallback to ScriptMaster (authoritative contract metadata).
        try:
            fut = get_future(self.symbol, self.exchange, result=0)
            if isinstance(fut, dict):
                lot = fut.get("LotSize")
                if lot:
                    return int(lot)
        except Exception:
            pass

        try:
            rows = universal_symbol_search(self.symbol, self.exchange) or []
            if expiry:
                rows = sorted(
                    rows,
                    key=lambda r: 0 if str(r.get("Expiry", "")).strip().upper() == str(expiry).strip().upper() else 1,
                )
            for rec in rows:
                lot = rec.get("LotSize")
                if lot:
                    return int(lot)
        except Exception:
            pass

        # Final static fallback.
        return DEFAULT_LOT_SIZES.get(self.symbol, 1)

    def get_full_chain(self, expiry: Optional[str] = None) -> List[Dict[str, Any]]:
        self._check_freshness(expiry)   # optional safety
        conn = self._get_connection(expiry)
        if not conn:
            return []
        assert conn is not None
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM option_chain WHERE ltp > 0 ORDER BY strike, option_type")
            return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"get_full_chain error: {e}")
            return []

    def get_option_at_strike(
        self, strike: float, option_type: Union[str, OptionType], expiry: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        self._check_freshness(expiry)
        if isinstance(option_type, OptionType):
            option_type = option_type.value
        conn = self._get_connection(expiry)
        if not conn:
            return None
        assert conn is not None
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT * FROM option_chain WHERE strike = ? AND option_type = ?",
                (strike, option_type.upper())
            )
            row = cur.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"get_option_at_strike error: {e}")
            return None

    def find_option_by_delta(
        self,
        option_type: Union[str, OptionType],
        target_delta: float,
        tolerance: float = 0.05,
        expiry: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Find the option with delta closest to target_delta for the given option_type.

        Delta sign convention:
        - CE deltas are positive (0 to +1)
        - PE deltas are negative (-1 to 0)

        The search compares ABS(delta) against target_delta so that callers
        can simply pass e.g. 0.5 for both CE and PE.  Within a single
        option_type the ABS comparison yields a correct, monotonic ranking.
        """
        self._check_freshness(expiry)
        if isinstance(option_type, OptionType):
            option_type = option_type.value
        conn = self._get_connection(expiry)
        if not conn:
            return None
        assert conn is not None
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT * FROM option_chain
                WHERE option_type = ?
                  AND delta IS NOT NULL
                  AND ltp > 0
                  AND ABS(ABS(delta) - ?) <= ?
                ORDER BY ABS(ABS(delta) - ?) ASC
                LIMIT 1
            """, (option_type.upper(), target_delta, tolerance, target_delta))
            row = cur.fetchone()
            if row:
                return dict(row)

            # Fallback: if strict tolerance has no hit, choose nearest available
            # non-null delta — but enforce a hard maximum deviation to prevent
            # entering positions with wildly wrong risk profiles (e.g. on gap days
            # where the option chain is truncated).
            MAX_DELTA_FALLBACK = 0.15  # never accept delta off by more than 0.15 from target
            cur.execute(
                """
                SELECT * FROM option_chain
                WHERE option_type = ?
                  AND delta IS NOT NULL
                  AND ltp > 0
                  AND ABS(ABS(delta) - ?) <= ?
                ORDER BY ABS(ABS(delta) - ?) ASC
                LIMIT 1
                """,
                (option_type.upper(), target_delta, MAX_DELTA_FALLBACK, target_delta),
            )
            row = cur.fetchone()
            if row:
                best = dict(row)
                logger.warning(
                    "Delta target %.4f not found within tolerance %.4f for %s; "
                    "using nearest strike=%s delta=%s (within max fallback %.4f)",
                    target_delta,
                    tolerance,
                    option_type.upper(),
                    best.get("strike"),
                    best.get("delta"),
                    MAX_DELTA_FALLBACK,
                )
                return best

            # Nothing within max fallback — log critical and reject
            logger.critical(
                "DELTA MATCH REJECTED: target=%.4f tolerance=%.4f max_fallback=%.4f "
                "for %s — no suitable strike in chain. Chain may need re-centering.",
                target_delta, tolerance, MAX_DELTA_FALLBACK, option_type.upper(),
            )
            return None
        except Exception as e:
            logger.error(f"find_option_by_delta error: {e}")
            return None

    def find_straddle_strike_by_delta(
        self,
        option_type: Union[str, OptionType],
        target_delta: float,
        expiry: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        For straddle/strangle strategies, find the best strike where BOTH CE and
        PE deltas are optimally balanced around the target delta at the SAME strike.

        For a proper straddle with target_delta=0.5:
        - CE delta should be ≈ +0.5
        - PE delta should be ≈ -0.5

        The method reads all strikes that have both CE and PE data, computes a
        combined delta error for each, and returns the option data for the
        requested ``option_type`` at the strike with the smallest combined error.
        """
        self._check_freshness(expiry)
        if isinstance(option_type, OptionType):
            opt_type = option_type.value
        else:
            opt_type = option_type.upper()

        conn = self._get_connection(expiry)
        if not conn:
            return None

        try:
            cur = conn.cursor()
            # Step 1: Fetch CE rows with delta
            cur.execute("""
                SELECT strike, delta FROM option_chain
                WHERE option_type = 'CE' AND delta IS NOT NULL AND ltp > 0
            """)
            ce_map = {row["strike"]: row["delta"] for row in cur.fetchall()}

            # Step 2: Fetch PE rows with delta
            cur.execute("""
                SELECT strike, delta FROM option_chain
                WHERE option_type = 'PE' AND delta IS NOT NULL AND ltp > 0
            """)
            pe_map = {row["strike"]: row["delta"] for row in cur.fetchall()}

            # Step 3: Find strikes present in both
            common_strikes = set(ce_map.keys()) & set(pe_map.keys())
            if not common_strikes:
                logger.warning("Straddle delta matching: no strikes with both CE and PE data")
                return None

            # Step 4: Score each common strike
            best_strike: Optional[float] = None
            best_error = float("inf")

            for strike in sorted(common_strikes):
                ce_delta = ce_map[strike]
                pe_delta = pe_map[strike]

                # For a balanced straddle:
                # CE should be at +target_delta, PE should be at -target_delta
                ce_error = abs(abs(ce_delta) - target_delta)
                pe_error = abs(abs(pe_delta) - target_delta)
                combined_error = ce_error + pe_error

                if combined_error < best_error:
                    best_error = combined_error
                    best_strike = strike

            if best_strike is None:
                return None

            logger.info(
                "Straddle delta=%.4f: Selected strike %s (combined_error=%.4f) for %s",
                target_delta, best_strike, best_error, opt_type,
            )

            # Step 5: Fetch full option data for the requested type at that strike
            cur.execute("""
                SELECT * FROM option_chain
                WHERE strike = ? AND option_type = ? AND delta IS NOT NULL AND ltp > 0
            """, (best_strike, opt_type))
            result = cur.fetchone()
            return dict(result) if result else None

        except Exception as e:
            logger.error(f"find_straddle_strike_by_delta error: {e}")
            return None

    def find_option_by_premium(
        self,
        option_type: Union[str, OptionType],
        target_premium: float,
        tolerance: float = 10.0,
        expiry: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        self._check_freshness(expiry)
        if isinstance(option_type, OptionType):
            option_type = option_type.value
        conn = self._get_connection(expiry)
        if not conn:
            return None
        assert conn is not None
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT * FROM option_chain
                WHERE option_type = ?
                  AND ltp > 0
                  AND ABS(ltp - ?) <= ?
                ORDER BY ABS(ltp - ?) ASC
                LIMIT 1
            """, (option_type.upper(), target_premium, tolerance, target_premium))
            row = cur.fetchone()
            if row:
                return dict(row)

            # Fallback: nearest premium without tolerance filter
            cur.execute("""
                SELECT * FROM option_chain
                WHERE option_type = ? AND ltp > 0
                ORDER BY ABS(ltp - ?) ASC
                LIMIT 1
            """, (option_type.upper(), target_premium))
            row = cur.fetchone()
            if row:
                best = dict(row)
                logger.warning(
                    "Premium target %.2f not within tolerance %.2f for %s; "
                    "using nearest strike=%s ltp=%s",
                    target_premium, tolerance, option_type.upper(),
                    best.get("strike"), best.get("ltp"),
                )
                return best
            return None
        except Exception as e:
            logger.error(f"find_option_by_premium error: {e}")
            return None

    def find_option_by_iv(
        self,
        option_type: Union[str, OptionType],
        target_iv: float,
        tolerance: float = 5.0,
        expiry: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        self._check_freshness(expiry)
        if isinstance(option_type, OptionType):
            option_type = option_type.value
        conn = self._get_connection(expiry)
        if not conn:
            return None
        assert conn is not None
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT * FROM option_chain
                WHERE option_type = ?
                  AND iv IS NOT NULL AND iv > 0
                  AND ltp > 0
                  AND ABS(iv - ?) <= ?
                ORDER BY ABS(iv - ?) ASC
                LIMIT 1
            """, (option_type.upper(), target_iv, tolerance, target_iv))
            row = cur.fetchone()
            if row:
                return dict(row)

            # Fallback: nearest IV without tolerance filter
            cur.execute("""
                SELECT * FROM option_chain
                WHERE option_type = ? AND iv IS NOT NULL AND iv > 0 AND ltp > 0
                ORDER BY ABS(iv - ?) ASC
                LIMIT 1
            """, (option_type.upper(), target_iv))
            row = cur.fetchone()
            if row:
                best = dict(row)
                logger.warning(
                    "IV target %.2f not within tolerance %.2f for %s; "
                    "using nearest strike=%s iv=%s",
                    target_iv, tolerance, option_type.upper(),
                    best.get("strike"), best.get("iv"),
                )
                return best
            return None
        except Exception as e:
            logger.error(f"find_option_by_iv error: {e}")
            return None

    def find_option_by_criteria(
        self,
        option_type: Union[str, OptionType],
        target_attr: str,
        target_value: float,
        tolerance: float = 0.01,
        expiry: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Find option where an attribute (delta, abs_delta, iv, etc.) is closest to target.
        Uses a mapping to convert attribute name to SQL expression.
        """
        self._check_freshness(expiry)
        if isinstance(option_type, OptionType):
            option_type = option_type.value

        if target_attr not in MATCH_PARAM_TO_SQL:
            raise ValueError(f"Unsupported attribute for match: {target_attr}")

        sql_expr = MATCH_PARAM_TO_SQL[target_attr]
        conn = self._get_connection(expiry)
        if not conn:
            return None
        assert conn is not None
        try:
            cur = conn.cursor()
            # Safe because sql_expr comes from our controlled mapping
            query = f"""
                SELECT * FROM option_chain
                WHERE option_type = ?
                  AND ltp > 0
                  AND ABS({sql_expr} - ?) <= ?
                ORDER BY ABS({sql_expr} - ?) ASC
                LIMIT 1
            """
            cur.execute(query, (option_type.upper(), target_value, tolerance, target_value))
            row = cur.fetchone()
            if row:
                return dict(row)

            # Fallback: nearest match with a hard max deviation limit.
            # Prevents entering positions with wildly wrong attributes on
            # truncated/stale chains.
            max_fallback = _adaptive_tolerance(target_attr, target_value)
            fallback_query = f"""
                SELECT * FROM option_chain
                WHERE option_type = ?
                  AND {sql_expr} IS NOT NULL
                  AND ltp > 0
                  AND ABS({sql_expr} - ?) <= ?
                ORDER BY ABS({sql_expr} - ?) ASC
                LIMIT 1
            """
            cur.execute(fallback_query, (option_type.upper(), target_value, max_fallback, target_value))
            row = cur.fetchone()
            if row:
                best = dict(row)
                logger.warning(
                    "%s target %.6f not within tolerance %.4f for %s; "
                    "using nearest strike=%s value=%s (within max fallback %.4f)",
                    target_attr, target_value, tolerance, option_type.upper(),
                    best.get("strike"), best.get(target_attr, best.get("strike")),
                    max_fallback,
                )
                return best

            # Nothing within max fallback — reject
            logger.critical(
                "CRITERIA MATCH REJECTED: %s target=%.6f tolerance=%.4f max_fallback=%.4f "
                "for %s — no suitable strike in chain. Chain may need re-centering.",
                target_attr, target_value, tolerance, max_fallback, option_type.upper(),
            )
            return None
        except Exception as e:
            logger.error(f"find_option_by_criteria error: {e}")
            return None

    def get_atm_options(
        self, expiry: Optional[str] = None
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        atm = self.get_atm_strike(expiry)
        if atm <= 0:
            return None, None
        ce = self.get_option_at_strike(atm, "CE", expiry)
        pe = self.get_option_at_strike(atm, "PE", expiry)
        return ce, pe

    # ----------------------------------------------------------------------
    # High-level strike resolution (used by EntryEngine / AdjustmentEngine)
    # ----------------------------------------------------------------------
    def resolve_strike(
        self,
        config: StrikeConfig,
        symbol: str,  # kept for interface compatibility
        expiry: Optional[str] = None,
        reference_leg_state=None,
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Resolve strike and option data from StrikeConfig.

        Each leg is resolved independently per its own strike_selection config.
        If a user explicitly sets strike_selection="straddle_delta" in the JSON,
        the straddle-aware matching is used.  Otherwise, plain delta search applies.

        Args:
            config: Strike configuration
            symbol: Symbol (for compatibility)
            expiry: Expiry date
            reference_leg_state: Reference leg for match_leg mode

        Raises:
            RuntimeError if snapshot too old or data not found.
        """
        # Freshness check
        self._check_freshness(expiry)

        mode = config.mode
        opt_type = config.option_type
        # Read ATM strike once for efficiency
        atm = self.get_atm_strike(expiry)

        opt_data: Optional[Dict[str, Any]] = None
        strike: float = 0.0  # will be assigned in each branch

        if mode == StrikeMode.STANDARD:
            sel = config.strike_selection
            if sel is None:
                raise ValueError("Standard strike mode requires 'strike_selection'")
            sel = sel.lower().strip()

            val = config.strike_value

            # Handle atm±n with arbitrary offset
            if sel == "atm":
                strike = atm
            elif sel.startswith("atm+") or sel.startswith("atm-"):
                match = re.match(r"atm([+-]\d+)", sel)
                if match:
                    offset = int(match.group(1))
                    step = self._get_strike_step(expiry)
                    strike = atm + offset * step
                else:
                    raise ValueError(f"Malformed atm offset: {sel}")
            elif sel in ("delta", "straddle_delta"):
                target = float(val) if val else 0.3
                # Only use straddle-aware matching when user EXPLICITLY sets
                # strike_selection="straddle_delta" in the JSON config.
                # Plain "delta" always resolves each leg independently.
                if sel == "straddle_delta":
                    opt_data = self.find_straddle_strike_by_delta(opt_type, target, expiry=expiry)
                    if opt_data is None:
                        logger.warning(
                            f"Straddle delta matching failed for {opt_type} delta={target}; "
                            f"falling back to independent delta search"
                        )
                        opt_data = self.find_option_by_delta(opt_type, target, expiry=expiry)
                else:
                    opt_data = self.find_option_by_delta(opt_type, target, expiry=expiry)
                
                if opt_data is None:
                    raise ValueError(f"No option found for delta={target}")
                strike = opt_data["strike"]
            elif sel == "theta":
                # Theta is always negative for options. Users naturally enter
                # positive values (e.g. "15" meaning "theta around -15").  We
                # search by ABS(theta) so the sign doesn't matter.
                target = abs(float(val)) if val else 10.0
                opt_data = self.find_option_by_criteria(
                    opt_type, "abs_theta", target,
                    tolerance=_adaptive_tolerance("abs_theta", target),
                    expiry=expiry,
                )
                if opt_data is None:
                    raise ValueError(f"No option found for theta≈{target}")
                strike = opt_data["strike"]
            elif sel == "vega":
                target = float(val) if val else 5.0
                opt_data = self.find_option_by_criteria(
                    opt_type, "vega", target,
                    tolerance=_adaptive_tolerance("vega", target),
                    expiry=expiry,
                )
                if opt_data is None:
                    raise ValueError(f"No option found for vega={target}")
                strike = opt_data["strike"]
            elif sel == "gamma":
                target = float(val) if val else 0.0003
                opt_data = self.find_option_by_criteria(
                    opt_type, "gamma", target,
                    tolerance=_adaptive_tolerance("gamma", target),
                    expiry=expiry,
                )
                if opt_data is None:
                    raise ValueError(f"No option found for gamma={target}")
                strike = opt_data["strike"]
            elif sel == "premium":
                target = float(val) if val else 50
                opt_data = self.find_option_by_premium(opt_type, target, expiry=expiry)
                if opt_data is None:
                    raise ValueError(f"No option found for premium={target}")
                strike = opt_data["strike"]
            elif sel == "oi":
                target = float(val) if val else 0.0
                opt_data = self.find_option_by_criteria(opt_type, "oi", target, tolerance=max(1000.0, target * 0.25), expiry=expiry)
                if opt_data is None:
                    raise ValueError(f"No option found for oi={target}")
                strike = opt_data["strike"]
            elif sel == "volume":
                target = float(val) if val else 0.0
                opt_data = self.find_option_by_criteria(opt_type, "volume", target, tolerance=max(1000.0, target * 0.25), expiry=expiry)
                if opt_data is None:
                    raise ValueError(f"No option found for volume={target}")
                strike = opt_data["strike"]
            elif sel == "iv":
                target = float(val) if val else 15.0
                opt_data = self.find_option_by_iv(opt_type, target, expiry=expiry)
                if opt_data is None:
                    raise ValueError(f"No option found for IV={target}")
                strike = opt_data["strike"]
            elif sel == "otm_pct":
                target_pct = float(val) if val else 0.5
                if opt_type == OptionType.CE:
                    strike = atm * (1 + target_pct / 100.0)
                else:
                    strike = atm * (1 - target_pct / 100.0)
                step = self._get_strike_step(expiry)
                strike = round(strike / step) * step
            elif sel == "exact_strike":
                if val is None or str(val).strip() == "":
                    raise ValueError("exact_strike selection requires strike_value")
                strike = float(val)
            elif sel == "atm_points":
                points = float(val) if val else 0.0
                strike = atm + points
                step = self._get_strike_step(expiry)
                strike = round(strike / step) * step
            elif sel == "atm_pct":
                pct = float(val) if val else 0.0
                strike = atm * (1 + pct / 100.0)
                step = self._get_strike_step(expiry)
                strike = round(strike / step) * step
            elif sel in ("max_pain", "pcr_inflection"):
                strike = self.get_max_pain_strike(expiry) if sel == "max_pain" else self.get_pcr_inflection_strike(expiry)
                if strike <= 0:
                    strike = atm
            else:
                raise ValueError(f"Unsupported standard selection: {sel}")

            if sel not in ["delta", "straddle_delta", "theta", "vega", "gamma", "premium", "oi", "volume", "iv"]:
                # For simple strike selections, get option data now
                opt_data = self.get_option_at_strike(strike, opt_type, expiry)
                if opt_data is None:
                    raise ValueError(f"No option data at strike {strike}")

        elif mode == StrikeMode.EXACT:
            exact = config.exact_strike
            if exact is None:
                raise ValueError("Exact strike mode requires 'exact_strike'")
            strike = exact
            if config.rounding:
                strike = round(strike / config.rounding) * config.rounding
            opt_data = self.get_option_at_strike(strike, opt_type, expiry)
            if opt_data is None:
                raise ValueError(f"No option data at exact strike {strike}")

        elif mode == StrikeMode.ATM_POINTS:
            offset = config.atm_offset_points or 0
            strike = atm + offset
            if config.rounding:
                strike = round(strike / config.rounding) * config.rounding
            opt_data = self.get_option_at_strike(strike, opt_type, expiry)
            if opt_data is None:
                raise ValueError(f"No option data at ATM+{offset} (strike {strike})")

        elif mode == StrikeMode.ATM_PCT:
            pct = config.atm_offset_pct or 0
            strike = atm * (1 + pct / 100)
            if config.rounding:
                strike = round(strike / config.rounding) * config.rounding
            opt_data = self.get_option_at_strike(strike, opt_type, expiry)
            if opt_data is None:
                raise ValueError(f"No option data at ATM+{pct}% (strike {strike})")

        elif mode == StrikeMode.MATCH_LEG:
            if reference_leg_state is None:
                raise ValueError("Match leg mode requires a reference leg state")
            match_param = config.match_param
            if match_param is None:
                raise ValueError("Match leg mode requires 'match_param'")
            if match_param == "moneyness":
                if reference_leg_state.strike is None:
                    raise ValueError("Reference leg has no strike for moneyness matching")
                offset = reference_leg_state.strike - atm
                strike = atm + offset
                step = self._get_strike_step(expiry)
                strike = round(strike / step) * step
                opt_data = self.get_option_at_strike(strike, opt_type, expiry)
                if opt_data is None:
                    raise ValueError(f"No option found for moneyness-preserved strike {strike}")
                return strike, opt_data
            if not hasattr(reference_leg_state, match_param):
                raise ValueError(
                    f"Reference leg has no attribute '{match_param}'. "
                    f"Available: delta, abs_delta, iv, theta, abs_theta, vega, gamma, pnl, pnl_pct, ltp, strike"
                )
            attr_value = getattr(reference_leg_state, match_param)
            target = attr_value * config.match_multiplier + config.match_offset
            tol = _adaptive_tolerance(match_param, target)
            opt_data = self.find_option_by_criteria(
                opt_type,
                match_param,
                target,
                tolerance=tol,
                expiry=expiry
            )
            if opt_data is None:
                raise ValueError(f"No option found matching {match_param}={target} (tol={tol:.4f})")
            strike = opt_data["strike"]

        else:
            raise ValueError(f"Unknown strike mode: {mode}")

        # At this point, strike must be a numeric value and opt_data must be set.
        if not isinstance(strike, (int, float)):
            raise ValueError(f"Strike must be numeric, got {type(strike).__name__}: {strike}")
        if opt_data is None:
            raise ValueError(f"Internal error: opt_data not assigned for strike {strike}")
        return strike, opt_data

    def get_max_pain_strike(self, expiry: Optional[str] = None) -> float:
        """
        Compute max-pain strike (minimum aggregate option writer payout at expiry).
        """
        self._check_freshness(expiry)
        conn = self._get_connection(expiry)
        if not conn:
            return 0.0
        assert conn is not None
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT strike, option_type, oi
                FROM option_chain
                WHERE strike IS NOT NULL AND option_type IN ('CE','PE')
                """
            )
            rows = cur.fetchall()
            if not rows:
                return 0.0

            strikes: List[float] = sorted({float(r["strike"]) for r in rows if r["strike"] is not None})
            ce_oi: Dict[float, float] = {}
            pe_oi: Dict[float, float] = {}
            for r in rows:
                k = float(r["strike"])
                oi = float(r["oi"] or 0.0)
                if r["option_type"] == "CE":
                    ce_oi[k] = ce_oi.get(k, 0.0) + oi
                else:
                    pe_oi[k] = pe_oi.get(k, 0.0) + oi

            best_strike = 0.0
            best_pain = float("inf")
            for s in strikes:
                pain = 0.0
                for k, oi in ce_oi.items():
                    pain += max(0.0, s - k) * oi
                for k, oi in pe_oi.items():
                    pain += max(0.0, k - s) * oi
                if pain < best_pain:
                    best_pain = pain
                    best_strike = s
            return best_strike
        except Exception as e:
            logger.error(f"get_max_pain_strike error: {e}")
            return 0.0

    def get_pcr_inflection_strike(self, expiry: Optional[str] = None) -> float:
        """
        Find strike where PCR (PE_OI / CE_OI) is closest to 1, preferring near-ATM.
        """
        self._check_freshness(expiry)
        conn = self._get_connection(expiry)
        if not conn:
            return 0.0
        assert conn is not None
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT strike,
                       SUM(CASE WHEN option_type='CE' THEN COALESCE(oi,0) ELSE 0 END) AS ce_oi,
                       SUM(CASE WHEN option_type='PE' THEN COALESCE(oi,0) ELSE 0 END) AS pe_oi
                FROM option_chain
                WHERE strike IS NOT NULL
                GROUP BY strike
                """
            )
            rows = cur.fetchall()
            if not rows:
                return 0.0
            atm = self.get_atm_strike(expiry)
            best_strike = 0.0
            best_score = float("inf")
            for r in rows:
                strike = float(r["strike"])
                ce_oi = float(r["ce_oi"] or 0.0)
                pe_oi = float(r["pe_oi"] or 0.0)
                if ce_oi <= 0:
                    continue
                pcr = pe_oi / ce_oi
                score = abs(pcr - 1.0) + (abs(strike - atm) / max(1.0, atm))
                if score < best_score:
                    best_score = score
                    best_strike = strike
            if best_strike > 0:
                return best_strike
            return atm
        except Exception as e:
            logger.error(f"get_pcr_inflection_strike error: {e}")
            return 0.0

    def get_chain_metrics(self, expiry: Optional[str] = None) -> Dict[str, float]:
        """
        Return chain-level aggregate metrics used by strategy conditions.
        """
        self._check_freshness(expiry)
        conn = self._get_connection(expiry)
        if not conn:
            return {
                "pcr": 0.0,
                "pcr_volume": 0.0,
                "total_oi_ce": 0.0,
                "total_oi_pe": 0.0,
                "oi_buildup_ce": 0.0,
                "oi_buildup_pe": 0.0,
                "max_pain_strike": 0.0,
            }
        assert conn is not None
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT
                  SUM(CASE WHEN option_type='CE' THEN COALESCE(oi,0) ELSE 0 END) AS ce_oi,
                  SUM(CASE WHEN option_type='PE' THEN COALESCE(oi,0) ELSE 0 END) AS pe_oi,
                  SUM(CASE WHEN option_type='CE' THEN COALESCE(volume,0) ELSE 0 END) AS ce_vol,
                  SUM(CASE WHEN option_type='PE' THEN COALESCE(volume,0) ELSE 0 END) AS pe_vol
                FROM option_chain
                """
            )
            row = cur.fetchone()
            if not row:
                return {
                    "pcr": 0.0,
                    "pcr_volume": 0.0,
                    "total_oi_ce": 0.0,
                    "total_oi_pe": 0.0,
                    "oi_buildup_ce": 0.0,
                    "oi_buildup_pe": 0.0,
                    "max_pain_strike": 0.0,
                    "prev_total_oi_ce": 0.0,
                    "prev_total_oi_pe": 0.0,
                }

            ce_oi = float(row["ce_oi"] or 0.0)
            pe_oi = float(row["pe_oi"] or 0.0)
            ce_vol = float(row["ce_vol"] or 0.0)
            pe_vol = float(row["pe_vol"] or 0.0)

            # Use a safe key: if expiry is None, store under "default"
            cache_key = expiry if expiry is not None else "default"
            prev = self._prev_total_oi.get(cache_key, {})
            oi_buildup_ce = ce_oi - prev.get('CE', ce_oi)
            oi_buildup_pe = pe_oi - prev.get('PE', pe_oi)
            self._prev_total_oi[cache_key] = {'CE': ce_oi, 'PE': pe_oi}

            pcr = (pe_oi / ce_oi) if ce_oi > 0 else 0.0
            pcr_volume = (pe_vol / ce_vol) if ce_vol > 0 else 0.0

            return {
                "pcr": pcr,
                "pcr_volume": pcr_volume,
                "total_oi_ce": ce_oi,
                "total_oi_pe": pe_oi,
                "prev_total_oi_ce": prev.get('CE', ce_oi),
                "prev_total_oi_pe": prev.get('PE', pe_oi),
                "oi_buildup_ce": oi_buildup_ce,
                "oi_buildup_pe": oi_buildup_pe,
                "max_pain_strike": self.get_max_pain_strike(expiry),
            }
        except Exception as e:
            logger.error(f"get_chain_metrics error: {e}")
            return {
                "pcr": 0.0,
                "pcr_volume": 0.0,
                "total_oi_ce": 0.0,
                "total_oi_pe": 0.0,
                "prev_total_oi_ce": 0.0,
                "prev_total_oi_pe": 0.0,
                "oi_buildup_ce": 0.0,
                "oi_buildup_pe": 0.0,
                "max_pain_strike": 0.0,
            }

    def resolve_expiry_mode(self, mode: str) -> str:
        """
        Convert an expiry mode string (e.g., 'weekly_current', 'weekly_next')
        into an actual expiry date string (format DD-MMM-YYYY) by scanning
        the available DB files.

        Raises ValueError if mode cannot be resolved.
        """
        # If it's already a date-like string (contains digits and hyphens), assume it's a date.
        if re.match(r'\d{1,2}-[A-Za-z]{3}-\d{4}', mode):
            return mode

        # Determine target based on mode
        today = date.today()
        pattern = f"{self.exchange}_{self.symbol}_*.sqlite"
        folder = DB_FOLDER
        if not folder.exists():
            raise ValueError(f"DB folder not found: {folder}")

        matches = list(folder.glob(pattern))
        if not matches:
            raise ValueError(f"No database files for {self.exchange}_{self.symbol}")

        # Parse expiry dates from filenames
        expiries = []
        for f in matches:
            name = f.stem
            parts = name.split("_", 2)
            if len(parts) < 3:
                continue
            date_str = parts[2]
            try:
                exp_date = datetime.strptime(date_str, "%d-%b-%Y").date()
                expiries.append((exp_date, date_str))
            except ValueError:
                continue

        if not expiries:
            raise ValueError("No valid expiry dates found in filenames")

        expiries.sort(key=lambda x: x[0])  # sort by date

        if mode == "weekly_current" or mode == "monthly_current":
            # Find the nearest future expiry (>= today)
            future = [d for d in expiries if d[0] >= today]
            if future:
                return future[0][1]
            # If none future, return the latest (last of the year)
            return expiries[-1][1]

        elif mode == "weekly_auto":
            # Rule:
            # - if nearest expiry day is today -> use next expiry
            # - otherwise -> use nearest expiry
            current = self.resolve_expiry_mode("weekly_current")
            cur_date = datetime.strptime(current, "%d-%b-%Y").date()
            if cur_date != today:
                return current
            for exp_date, exp_str in expiries:
                if exp_date > cur_date:
                    return exp_str
            # Wrap if only one expiry exists
            return expiries[0][1]

        elif mode == "weekly_next":
            # Find the first expiry after the current weekly (which we take as the first future expiry)
            current = self.resolve_expiry_mode("weekly_current")  # recursive but safe
            cur_date = datetime.strptime(current, "%d-%b-%Y").date()
            for exp_date, exp_str in expiries:
                if exp_date > cur_date:
                    return exp_str
            # Wrap to first of next year? Or raise? We'll return first.
            return expiries[0][1]

        elif mode == "monthly_next":
            current = self.resolve_expiry_mode("monthly_current")
            cur_date = datetime.strptime(current, "%d-%b-%Y").date()
            for exp_date, exp_str in expiries:
                if exp_date > cur_date:
                    return exp_str
            return expiries[0][1]

        else:
            raise ValueError(f"Unsupported expiry mode: {mode}")

    def get_next_expiry(self, current_expiry: str, mode: str = "weekly_next") -> str:
        """
        Given a current expiry string (format DD-MMM-YYYY) and a mode
        ('weekly_next', 'monthly_next', 'weekly_current', 'monthly_current'),
        return the next available expiry date string from the DB folder.
        """
        # If mode is *_current, return current_expiry (assumed to be a valid date)
        if mode in ("weekly_current", "monthly_current"):
            return current_expiry

        # For next modes, find the next expiry after current_expiry
        pattern = f"{self.exchange}_{self.symbol}_*.sqlite"
        folder = DB_FOLDER
        if not folder.exists():
            raise RuntimeError(f"DB folder not found: {folder}")

        matches = list(folder.glob(pattern))
        if not matches:
            raise RuntimeError(f"No database files for {self.exchange}_{self.symbol}")

        # Parse expiry dates from filenames
        expiries = []
        for f in matches:
            name = f.stem
            parts = name.split("_", 2)
            if len(parts) < 3:
                continue
            date_str = parts[2]
            try:
                exp_date = datetime.strptime(date_str, "%d-%b-%Y").date()
                expiries.append((exp_date, date_str))
            except ValueError:
                continue

        if not expiries:
            raise RuntimeError("No valid expiry dates found in filenames")

        expiries.sort(key=lambda x: x[0])  # sort by date

        # Parse current_expiry
        try:
            cur_date = datetime.strptime(current_expiry, "%d-%b-%Y").date()
        except ValueError:
            # Fallback: return the first future expiry
            today = datetime.now().date()
            future = [d for d in expiries if d[0] >= today]
            if future:
                return future[0][1]
            return expiries[-1][1]

        # Find the next expiry after cur_date
        for exp_date, exp_str in expiries:
            if exp_date > cur_date:
                return exp_str

        # If none after, wrap to the first (next year)
        return expiries[0][1]


# ==============================================================================
# The following mock class is used in unit tests to simulate the MarketReader
# behavior without needing actual database files. It provides hardcoded responses
# for all methods, with signatures matching the base class exactly.
# ==============================================================================

class MockMarketReader(MarketReader):
    """Simple mock for testing."""

    def __init__(self, exchange: str = "NFO", symbol: str = "NIFTY", **kwargs):
        super().__init__(exchange, symbol)
        self._spot = 25000.0
        self._atm = 25000.0
        self._fut = 25050.0
        self._chain_cache = {}

    def get_spot_price(self, expiry: Optional[str] = None) -> float:
        return self._spot

    def get_atm_strike(self, expiry: Optional[str] = None) -> float:
        return self._atm

    def get_fut_ltp(self, expiry: Optional[str] = None) -> float:
        return self._fut

    def get_option_at_strike(
        self, strike: float, option_type: Union[str, OptionType], expiry: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        if isinstance(option_type, OptionType):
            opt_type = option_type.value
        else:
            opt_type = option_type
        return {
            "strike": strike,
            "option_type": opt_type.upper(),
            "ltp": 100.0,
            "delta": 0.5 if opt_type.upper() == "CE" else -0.5,
            "gamma": 0.005,
            "theta": -10,
            "vega": 20,
            "iv": 15.0,
            "oi": 10000,
            "volume": 500,
        }

    def find_option_by_delta(
        self,
        option_type: Union[str, OptionType],
        target_delta: float,
        tolerance: float = 0.05,
        expiry: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        if isinstance(option_type, OptionType):
            opt_type = option_type.value
        else:
            opt_type = option_type
        # Mock: for delta=0.5 both CE and PE return ATM
        # Higher delta → deeper ITM → lower CE strike / higher PE strike
        step = 50  # default step
        delta_offset = (target_delta - 0.5) * 10  # how many steps from ATM
        if opt_type.upper() == "CE":
            strike = self._atm - (delta_offset * step)
        else:
            strike = self._atm + (delta_offset * step)
        return self.get_option_at_strike(strike, opt_type)

    def find_straddle_strike_by_delta(
        self,
        option_type: Union[str, OptionType],
        target_delta: float,
        expiry: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        if isinstance(option_type, OptionType):
            opt_type = option_type.value
        else:
            opt_type = option_type
        # For straddle: both legs at ATM when delta=0.5
        return self.get_option_at_strike(self._atm, opt_type)

    def find_option_by_premium(
        self,
        option_type: Union[str, OptionType],
        target_premium: float,
        tolerance: float = 10.0,
        expiry: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        if isinstance(option_type, OptionType):
            opt_type = option_type.value
        else:
            opt_type = option_type
        strike = self._atm - (target_premium * 5) if opt_type.upper() == "CE" else self._atm + (target_premium * 5)
        return self.get_option_at_strike(strike, opt_type)

    def find_option_by_iv(
        self,
        option_type: Union[str, OptionType],
        target_iv: float,
        tolerance: float = 5.0,
        expiry: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        if isinstance(option_type, OptionType):
            opt_type = option_type.value
        else:
            opt_type = option_type
        return self.get_option_at_strike(self._atm, opt_type)

    def find_option_by_criteria(
        self,
        option_type: Union[str, OptionType],
        target_attr: str,
        target_value: float,
        tolerance: float = 0.01,
        expiry: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        if isinstance(option_type, OptionType):
            opt_type = option_type.value
        else:
            opt_type = option_type
        return self.get_option_at_strike(self._atm, opt_type)

    def resolve_strike(
        self,
        config: StrikeConfig,
        symbol: str,
        expiry: Optional[str] = None,
        reference_leg_state=None,
    ) -> Tuple[float, Dict[str, Any]]:
        if config.mode == StrikeMode.STANDARD:
            sel = (config.strike_selection or "atm").lower()
            val = config.strike_value
            if sel == "atm":
                strike = self._atm
            elif sel.startswith("atm+") or sel.startswith("atm-"):
                m = re.match(r"atm([+-]\d+)", sel)
                step = float(config.rounding or 50)
                off = int(m.group(1)) if m else 0
                strike = self._atm + (off * step)
            elif sel in ("delta", "straddle_delta"):
                target = float(val) if val else 0.3
                if sel == "straddle_delta":
                    opt_data = self.find_straddle_strike_by_delta(config.option_type, target)
                else:
                    opt_data = self.find_option_by_delta(config.option_type, target)
                if opt_data:
                    return opt_data["strike"], opt_data
                strike = self._atm
            elif sel == "theta":
                target = abs(float(val)) if val else 10.0
                opt_data = self.find_option_by_criteria(config.option_type, "abs_theta", target)
                if opt_data:
                    return opt_data["strike"], opt_data
                strike = self._atm
            elif sel in ("vega", "gamma", "premium", "iv", "oi", "volume"):
                opt_data = self.find_option_by_criteria(config.option_type, sel, float(val) if val else 0.0)
                if opt_data:
                    return opt_data["strike"], opt_data
                strike = self._atm
            else:
                strike = self._atm
        elif config.mode == StrikeMode.EXACT:
            strike = float(config.exact_strike if config.exact_strike is not None else self._atm)
            if config.rounding:
                strike = round(strike / config.rounding) * config.rounding
        elif config.mode == StrikeMode.ATM_POINTS:
            strike = self._atm + float(config.atm_offset_points or 0)
            if config.rounding:
                strike = round(strike / config.rounding) * config.rounding
        elif config.mode == StrikeMode.ATM_PCT:
            strike = self._atm * (1 + float(config.atm_offset_pct or 0.0) / 100.0)
            if config.rounding:
                strike = round(strike / config.rounding) * config.rounding
        else:
            strike = self._atm
        opt_data = self.get_option_at_strike(strike, config.option_type)
        if opt_data is None:
            raise ValueError(f"No option found at strike {strike} for {config.option_type}")
        return strike, opt_data
