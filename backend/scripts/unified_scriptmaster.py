"""
Smart Trader — Unified ScriptMaster
=====================================

Merges symbols from ALL broker-specific scriptmasters into a single interface
used throughout the system.

Every component — search, option chain, watchlist, order placement —
uses this module. Symbols are returned in a canonical NormalizedInstrument
format with per-broker trading_symbol mappings.

Architecture:
    scripts/fyers_scriptmaster.py     → Fyers symbols
    scripts/shoonya_scriptmaster.py   → Shoonya symbols
    scripts/angelone_scriptmaster.py  → Angel One symbols
    scripts/dhan_scriptmaster.py      → Dhan symbols
    scripts/kite_scriptmaster.py      → Zerodha Kite symbols
    scripts/upstox_scriptmaster.py    → Upstox symbols
    scripts/groww_scriptmaster.py     → Groww symbols
    scripts/unified_scriptmaster.py   → THIS FILE: combines, dedup, provides single API

Usage:
    from scripts.unified_scriptmaster import (
        refresh_all, search_all, get_expiries, get_options,
        resolve_broker_symbol, get_lot_size, requires_limit_order,
    )
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger("smart_trader.unified_scriptmaster")

_LOCK = threading.Lock()
_initialized = False

# ── Fallback constants ─────────────────────────────────────────────────────────

_STRIKE_GAPS: Dict[str, int] = {
    "NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50,
    "MIDCPNIFTY": 25, "SENSEX": 100, "BANKEX": 100,
    "CRUDEOIL": 50, "GOLD": 100, "SILVER": 1000,
}

_INDEX_SYMBOLS: Dict[str, str] = {
    "NIFTY":      "NSE:NIFTY50-INDEX",
    "BANKNIFTY":  "NSE:NIFTYBANK-INDEX",
    "FINNIFTY":   "NSE:FINNIFTY-INDEX",
    "MIDCPNIFTY": "NSE:MIDCPNIFTY-INDEX",
    "SENSEX":     "BSE:SENSEX-INDEX",
    "BANKEX":     "BSE:BANKEX-INDEX",
    "CRUDEOIL":   "MCX:CRUDEOIL-INDEX",
    "GOLD":       "MCX:GOLD-INDEX",
    "SILVER":     "MCX:SILVER-INDEX",
    "NATURALGAS": "MCX:NATURALGAS-INDEX",
    "COPPER":     "MCX:COPPER-INDEX",
    "USDINR":     "NSE:USDINR-INDEX",
}

# Exchange normalisation for cross-broker dedup
_EXCHANGE_NORM: Dict[str, str] = {
    "NSE": "NSE", "NFO": "NSE", "BSE": "BSE", "BFO": "BSE", "MCX": "MCX",
    "CDS": "CDS", "BCD": "BCD",
}


def _ensure_loaded() -> None:
    """Ensure both scriptmasters are loaded (lazy, once per process)."""
    global _initialized
    if _initialized:
        return
    with _LOCK:
        if _initialized:
            return
        refresh_all(force=False)
        _initialized = True


def refresh_all(force: bool = False) -> Dict[str, int]:
    """
    Download/reload all broker scriptmasters.
    Returns dict of counts: {"fyers": N, "shoonya": M, "angelone": ..., ...}
    """
    global _initialized
    counts = {}

    try:
        from scripts.fyers_scriptmaster import refresh as fyers_refresh, FYERS_SCRIPTMASTER
        fyers_refresh(force=force)
        counts["fyers"] = len(FYERS_SCRIPTMASTER)
        logger.info("Fyers scriptmaster: %d symbols", counts["fyers"])
    except Exception as exc:
        logger.error("Fyers scriptmaster refresh failed: %s", exc)
        counts["fyers"] = 0

    try:
        from scripts.shoonya_scriptmaster import refresh as shoonya_refresh, SHOONYA_UNIVERSAL
        shoonya_refresh(force=force)
        counts["shoonya"] = len(SHOONYA_UNIVERSAL)
        logger.info("Shoonya scriptmaster: %d symbols", counts["shoonya"])
    except Exception as exc:
        logger.error("Shoonya scriptmaster refresh failed: %s", exc)
        counts["shoonya"] = 0

    try:
        from scripts.angelone_scriptmaster import refresh as angelone_refresh, ANGELONE_SCRIPTMASTER
        angelone_refresh(force=force)
        counts["angelone"] = len(ANGELONE_SCRIPTMASTER)
        logger.info("Angel One scriptmaster: %d symbols", counts["angelone"])
    except Exception as exc:
        logger.error("Angel One scriptmaster refresh failed: %s", exc)
        counts["angelone"] = 0

    try:
        from scripts.dhan_scriptmaster import refresh as dhan_refresh, DHAN_SCRIPTMASTER
        dhan_refresh(force=force)
        counts["dhan"] = len(DHAN_SCRIPTMASTER)
        logger.info("Dhan scriptmaster: %d symbols", counts["dhan"])
    except Exception as exc:
        logger.error("Dhan scriptmaster refresh failed: %s", exc)
        counts["dhan"] = 0

    try:
        from scripts.kite_scriptmaster import refresh as kite_refresh, KITE_SCRIPTMASTER
        kite_refresh(force=force)
        counts["kite"] = len(KITE_SCRIPTMASTER)
        logger.info("Kite scriptmaster: %d symbols", counts["kite"])
    except Exception as exc:
        logger.error("Kite scriptmaster refresh failed: %s", exc)
        counts["kite"] = 0

    try:
        from scripts.upstox_scriptmaster import refresh as upstox_refresh, UPSTOX_SCRIPTMASTER
        upstox_refresh(force=force)
        counts["upstox"] = len(UPSTOX_SCRIPTMASTER)
        logger.info("Upstox scriptmaster: %d symbols", counts["upstox"])
    except Exception as exc:
        logger.error("Upstox scriptmaster refresh failed: %s", exc)
        counts["upstox"] = 0

    try:
        from scripts.groww_scriptmaster import refresh as groww_refresh, GROWW_SCRIPTMASTER
        groww_refresh(force=force)
        counts["groww"] = len(GROWW_SCRIPTMASTER)
        logger.info("Groww scriptmaster: %d symbols", counts["groww"])
    except Exception as exc:
        logger.error("Groww scriptmaster refresh failed: %s", exc)
        counts["groww"] = 0

    _initialized = True
    return counts


# ── Unified Search ────────────────────────────────────────────────────────────

def search_all(
    query: str,
    exchange: str = "",
    instrument_type: str = "",
    limit: int = 30,
) -> List[Dict[str, Any]]:
    """
    Search instruments across all broker scriptmasters.

    Returns unified records with both Fyers and Shoonya trading symbols
    where available. Results sorted by relevance.

    Each result has:
        symbol, exchange, instrument_type, trading_symbol (canonical, no prefix),
        fyers_symbol ("EXCH:SYM"), shoonya_symbol (plain "SYM"),
        angelone_symbol, dhan_symbol, kite_symbol, upstox_symbol, groww_symbol,
        lot_size, tick_size, expiry, strike, option_type, token
    """
    _ensure_loaded()
    q = query.upper().strip()
    if not q:
        return []

    # Collect from Fyers
    fyers_results = _search_fyers(q, exchange, instrument_type, limit * 2)
    # Collect from Shoonya
    shoonya_results = _search_shoonya(q, exchange, instrument_type, limit * 2)
    # Collect from other brokers
    other_results = _search_other_brokers(q, exchange, instrument_type, limit * 2)

    # Merge: prefer Fyers records (richer data), augment with other broker symbols
    merged = _merge_results(fyers_results, shoonya_results, other_results)

    # Sort by relevance
    merged.sort(key=lambda r: (
        _relevance_score(r, q),
        {"IDX": 0, "FUT": 1, "EQ": 2, "OPT": 3}.get(r.get("instrument_type", ""), 4),
    ))

    return merged[:limit]


def _search_fyers(query: str, exchange: str, instrument_type: str, limit: int) -> List[Dict[str, Any]]:
    """Search Fyers scriptmaster and return unified format."""
    try:
        from scripts.fyers_scriptmaster import FYERS_SCRIPTMASTER
    except ImportError:
        return []

    results = []
    for key, rec in FYERS_SCRIPTMASTER.items():
        if exchange:
            exch_upper = exchange.upper()
            rec_exch = rec.get("Exchange", "").upper()
            if rec_exch != exch_upper:
                # Allow cross-matching between equivalent exchange names
                equiv_groups = [
                    {"NSE", "NFO"},
                    {"BSE", "BFO"},
                    {"CDS", "BCD"},
                ]
                matched = any(
                    exch_upper in grp and rec_exch in grp
                    for grp in equiv_groups
                )
                if not matched:
                    continue

        instr = rec.get("Instrument", "")
        if instrument_type and instr.upper() != instrument_type.upper():
            continue

        underlying = (rec.get("Underlying") or "").upper()
        fyers_sym = (rec.get("FyersSymbol") or "").upper()
        desc = (rec.get("Description") or "").upper()

        if query not in underlying and query not in fyers_sym and query not in desc:
            continue

        fyers_sym_full = rec.get("FyersSymbol", "")
        tsym = fyers_sym_full.split(":", 1)[-1] if ":" in fyers_sym_full else fyers_sym_full
        raw_expiry = rec.get("Expiry", "")

        results.append({
            "symbol":          rec.get("Underlying", "") or rec.get("Symbol", ""),
            "trading_symbol":  tsym,
            "fyers_symbol":    fyers_sym_full,
            "shoonya_symbol":  "",  # filled in merge
            "exchange":        rec.get("Exchange", ""),
            "instrument_type": instr,
            "lot_size":        int(rec.get("LotSize", 1) or 1),
            "tick_size":       float(rec.get("TickSize", 0.05) or 0.05),
            "expiry":          _to_iso(raw_expiry),
            "strike":          float(rec.get("StrikePrice") or 0),
            "option_type":     rec.get("OptionType") or "",
            "token":           str(rec.get("Token", "")),
            "description":     rec.get("Description", ""),
            "_source":         "fyers",
        })
        if len(results) >= limit:
            break
    return results


def _search_shoonya(query: str, exchange: str, instrument_type: str, limit: int) -> List[Dict[str, Any]]:
    """Search Shoonya scriptmaster and return unified format."""
    try:
        from scripts.shoonya_scriptmaster import SHOONYA_SCRIPTMASTER
    except ImportError:
        return []

    # Map Shoonya instrument names to canonical
    _instr_map = {
        "FUTIDX": "FUT", "FUTSTK": "FUT", "FUTCOM": "FUT",
        "FUTCUR": "FUT", "FUTIRC": "FUT",
        "OPTIDX": "OPT", "OPTSTK": "OPT", "OPTFUT": "OPT",
        "OPTCOM": "OPT", "OPTCUR": "OPT",
        "EQ": "EQ", "": "EQ",
    }

    results = []
    exchanges = [exchange.upper()] if exchange else list(SHOONYA_SCRIPTMASTER.keys())
    for exch in exchanges:
        for rec in SHOONYA_SCRIPTMASTER.get(exch, {}).values():
            raw_instr = rec.get("Instrument", "")
            canonical_instr = _instr_map.get(raw_instr, raw_instr)
            if instrument_type and canonical_instr != instrument_type.upper():
                continue

            sym = (rec.get("Symbol") or "").upper()
            underlying = (rec.get("Underlying") or "").upper()
            tsym = (rec.get("TradingSymbol") or "").upper()

            if query not in sym and query not in underlying and query not in tsym:
                continue

            raw_expiry = rec.get("Expiry", "")

            results.append({
                "symbol":          rec.get("Underlying", "") or rec.get("Symbol", ""),
                "trading_symbol":  rec.get("TradingSymbol", ""),
                "fyers_symbol":    "",  # filled in merge
                "shoonya_symbol":  rec.get("TradingSymbol", ""),
                "exchange":        exch,
                "instrument_type": canonical_instr,
                "lot_size":        int(rec.get("LotSize") or 1),
                "tick_size":       float(rec.get("TickSize") or 0.05),
                "expiry":          _to_iso(raw_expiry),
                "strike":          float(rec.get("StrikePrice") or 0),
                "option_type":     rec.get("OptionType") or "",
                "token":           str(rec.get("Token", "")),
                "description":     "",
                "shoonya_instrument": raw_instr,
                "_source":         "shoonya",
            })
            if len(results) >= limit:
                return results
    return results


def _search_other_brokers(query: str, exchange: str, instrument_type: str, limit: int) -> List[Dict[str, Any]]:
    """Search Angel One, Dhan, Kite, Upstox, Groww scriptmasters and return unified format."""
    results = []
    _instr_map = {
        "FUTIDX": "FUT", "FUTSTK": "FUT", "FUTCOM": "FUT",
        "FUTCUR": "FUT", "FUTIRC": "FUT",
        "OPTIDX": "OPT", "OPTSTK": "OPT", "OPTFUT": "OPT",
        "OPTCOM": "OPT", "OPTCUR": "OPT",
        "EQ": "EQ", "": "EQ", "IDX": "IDX",
    }

    # Helper to convert a generic broker record to unified format
    def _to_unified(rec: Dict, source: str) -> Dict[str, Any]:
        raw_instr = rec.get("Instrument", "") or rec.get("InstrumentType", "")
        canonical_instr = _instr_map.get(raw_instr, raw_instr) if raw_instr else "EQ"
        # Also check the canonical Instrument field directly
        if rec.get("Instrument") in ("FUT", "OPT", "EQ", "IDX"):
            canonical_instr = rec["Instrument"]

        raw_expiry = rec.get("Expiry", "")
        return {
            "symbol":          rec.get("Underlying", "") or rec.get("Symbol", ""),
            "trading_symbol":  rec.get("TradingSymbol", "") or rec.get("Symbol", ""),
            "fyers_symbol":    "",
            "shoonya_symbol":  "",
            "exchange":        rec.get("Exchange", ""),
            "instrument_type": canonical_instr,
            "lot_size":        int(rec.get("LotSize", 1) or 1),
            "tick_size":       float(rec.get("TickSize", 0.05) or 0.05),
            "expiry":          _to_iso(raw_expiry),
            "strike":          float(rec.get("StrikePrice") or 0),
            "option_type":     rec.get("OptionType") or "",
            "token":           str(rec.get("Token", "")),
            "description":     rec.get("Name", "") or rec.get("Description", ""),
            "_source":         source,
        }

    # Angel One
    try:
        from scripts.angelone_scriptmaster import ANGELONE_SCRIPTMASTER
        for rec in ANGELONE_SCRIPTMASTER.values():
            if exchange and rec.get("Exchange", "").upper() != exchange.upper():
                continue
            instr = rec.get("Instrument", "")
            if instrument_type and instr != instrument_type.upper():
                continue
            underlying = (rec.get("Underlying") or "").upper()
            sym = (rec.get("Symbol") or "").upper()
            if query not in underlying and query not in sym:
                continue
            results.append(_to_unified(rec, "angelone"))
            if len(results) >= limit:
                return results
    except Exception:
        pass

    # Dhan
    try:
        from scripts.dhan_scriptmaster import DHAN_SCRIPTMASTER
        for rec in DHAN_SCRIPTMASTER.values():
            if exchange and rec.get("Exchange", "").upper() != exchange.upper():
                continue
            instr = rec.get("Instrument", "")
            if instrument_type and instr != instrument_type.upper():
                continue
            underlying = (rec.get("Underlying") or "").upper()
            sym = (rec.get("Symbol") or "").upper()
            display = (rec.get("DisplayName") or "").upper()
            if query not in underlying and query not in sym and query not in display:
                continue
            results.append(_to_unified(rec, "dhan"))
            if len(results) >= limit:
                return results
    except Exception:
        pass

    # Kite
    try:
        from scripts.kite_scriptmaster import KITE_SCRIPTMASTER
        for rec in KITE_SCRIPTMASTER.values():
            if exchange and rec.get("Exchange", "").upper() != exchange.upper():
                continue
            instr = rec.get("Instrument", "")
            if instrument_type and instr != instrument_type.upper():
                continue
            underlying = (rec.get("Underlying") or "").upper()
            tsym = (rec.get("TradingSymbol") or "").upper()
            if query not in underlying and query not in tsym:
                continue
            results.append(_to_unified(rec, "kite"))
            if len(results) >= limit:
                return results
    except Exception:
        pass

    # Upstox
    try:
        from scripts.upstox_scriptmaster import UPSTOX_SCRIPTMASTER
        for rec in UPSTOX_SCRIPTMASTER.values():
            if exchange and rec.get("Exchange", "").upper() != exchange.upper():
                continue
            instr = rec.get("Instrument", "")
            if instrument_type and instr != instrument_type.upper():
                continue
            underlying = (rec.get("Underlying") or "").upper()
            tsym = (rec.get("TradingSymbol") or "").upper()
            if query not in underlying and query not in tsym:
                continue
            results.append(_to_unified(rec, "upstox"))
            if len(results) >= limit:
                return results
    except Exception:
        pass

    # Groww
    try:
        from scripts.groww_scriptmaster import GROWW_SCRIPTMASTER
        for rec in GROWW_SCRIPTMASTER.values():
            if exchange and rec.get("Exchange", "").upper() != exchange.upper():
                continue
            instr = rec.get("Instrument", "")
            if instrument_type and instr != instrument_type.upper():
                continue
            underlying = (rec.get("Underlying") or "").upper()
            tsym = (rec.get("TradingSymbol") or "").upper()
            if query not in underlying and query not in tsym:
                continue
            results.append(_to_unified(rec, "groww"))
            if len(results) >= limit:
                return results
    except Exception:
        pass

    return results


def _merge_results(fyers: List[Dict], shoonya: List[Dict], others: Optional[List[Dict]] = None) -> List[Dict]:
    """
    Merge Fyers, Shoonya, and other broker results, cross-linking broker symbols.

    Matching is by (exchange, underlying, instrument_type, expiry, strike, option_type).
    """
    # Index Shoonya records by matching key for cross-reference
    shoonya_idx: Dict[str, Dict] = {}
    for rec in shoonya:
        key = _match_key(rec)
        if key not in shoonya_idx:
            shoonya_idx[key] = rec

    # Index other broker records
    other_idx: Dict[str, List[Dict]] = {}
    for rec in (others or []):
        key = _match_key(rec)
        other_idx.setdefault(key, []).append(rec)

    merged = []
    seen_keys = set()

    # Process Fyers records first (they have richer data)
    for rec in fyers:
        key = _match_key(rec)
        seen_keys.add(key)
        # Cross-link Shoonya symbol
        shoonya_rec = shoonya_idx.get(key)
        if shoonya_rec:
            rec["shoonya_symbol"] = shoonya_rec.get("shoonya_symbol", "")
            if shoonya_rec.get("shoonya_instrument"):
                rec["shoonya_instrument"] = shoonya_rec["shoonya_instrument"]
        # Cross-link other broker symbols
        _cross_link_other(rec, other_idx.get(key, []))
        rec.pop("_source", None)
        merged.append(rec)

    # Add Shoonya-only records (MCX, etc.)
    for rec in shoonya:
        key = _match_key(rec)
        if key not in seen_keys:
            seen_keys.add(key)
            _cross_link_other(rec, other_idx.get(key, []))
            rec.pop("_source", None)
            merged.append(rec)

    # Add records from other brokers that weren't in Fyers or Shoonya
    for rec in (others or []):
        key = _match_key(rec)
        if key not in seen_keys:
            seen_keys.add(key)
            rec.pop("_source", None)
            merged.append(rec)

    return merged


def _cross_link_other(rec: Dict, other_recs: List[Dict]) -> None:
    """Cross-link other broker symbols into a merged record."""
    for orec in other_recs:
        source = orec.get("_source", "")
        if source == "angelone":
            rec["angelone_symbol"] = orec.get("trading_symbol", "")
            rec["angelone_token"] = orec.get("token", "")
        elif source == "dhan":
            rec["dhan_symbol"] = orec.get("trading_symbol", "")
            rec["dhan_security_id"] = orec.get("token", "")
        elif source == "kite":
            rec["kite_symbol"] = orec.get("trading_symbol", "")
            rec["kite_token"] = orec.get("token", "")
        elif source == "upstox":
            rec["upstox_symbol"] = orec.get("trading_symbol", "")
            rec["upstox_instrument_key"] = orec.get("token", "")
        elif source == "groww":
            rec["groww_symbol"] = orec.get("trading_symbol", "")
            rec["groww_token"] = orec.get("token", "")


def _match_key(rec: Dict) -> str:
    """Generate matching key for cross-broker dedup (exchange-normalised)."""
    raw_exch = rec.get("exchange", "").upper()
    norm_exch = _EXCHANGE_NORM.get(raw_exch, raw_exch)
    return (
        f"{norm_exch}|{rec.get('symbol', '').upper()}|"
        f"{rec.get('instrument_type', '')}|{rec.get('expiry', '')}|"
        f"{rec.get('strike', 0)}|{rec.get('option_type', '').upper()}"
    )


def _relevance_score(rec: Dict, query: str) -> int:
    """Lower = more relevant."""
    sym = (rec.get("symbol") or "").upper()
    if sym == query:
        return 0
    if sym.startswith(query):
        return 1
    if query in sym:
        return 2
    return 3


# ── Expiry and Lot Size ──────────────────────────────────────────────────────

def get_expiries(
    symbol: str,
    exchange: str = "NFO",
    kind: str = "OPTION",
) -> List[str]:
    """
    Get available expiry dates (ISO format) from all broker data.
    Returns sorted list of ISO date strings.
    """
    _ensure_loaded()
    all_expiries = set()
    sym = symbol.upper()

    # Fyers
    try:
        from scripts.fyers_scriptmaster import get_expiry_list
        for exp in get_expiry_list(sym, exchange, kind):
            iso = _to_iso(exp)
            if iso:
                all_expiries.add(iso)
    except Exception:
        pass

    # Shoonya
    try:
        from scripts.shoonya_scriptmaster import get_expiry_list as shoo_exp
        for exp in shoo_exp(sym, exchange, kind):
            iso = _to_iso(exp)
            if iso:
                all_expiries.add(iso)
    except Exception:
        pass

    # Additional brokers (Angel One, Dhan, Kite, Upstox, Groww)
    for mod_name in (
        "scripts.angelone_scriptmaster",
        "scripts.dhan_scriptmaster",
        "scripts.kite_scriptmaster",
        "scripts.upstox_scriptmaster",
        "scripts.groww_scriptmaster",
    ):
        try:
            import importlib
            mod = importlib.import_module(mod_name)
            for exp in mod.get_expiry_list(sym, exchange, kind):
                iso = _to_iso(exp)
                if iso:
                    all_expiries.add(iso)
        except Exception:
            pass

    today = datetime.now().strftime("%Y-%m-%d")
    return sorted(e for e in all_expiries if e >= today)


def get_options(
    underlying: str,
    exchange: str = "NFO",
    expiry: Optional[str] = None,
    strike: Optional[float] = None,
    option_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Get options from all scriptmasters, returning unified records.
    Expiry can be ISO or DD-Mon-YYYY format.
    """
    _ensure_loaded()
    results = []

    # Convert ISO expiry to broker format if needed
    broker_expiry = _iso_to_broker(expiry) if expiry else None

    # Fyers options
    try:
        from scripts.fyers_scriptmaster import get_options as fy_opts
        for rec in fy_opts(underlying, exchange, broker_expiry, strike, option_type):
            fyers_sym = rec.get("FyersSymbol", "")
            tsym = fyers_sym.split(":", 1)[-1] if ":" in fyers_sym else fyers_sym
            results.append({
                "symbol": rec.get("Underlying", ""),
                "trading_symbol": tsym,
                "fyers_symbol": fyers_sym,
                "shoonya_symbol": "",
                "exchange": rec.get("Exchange", ""),
                "instrument_type": "OPT",
                "lot_size": int(rec.get("LotSize", 1) or 1),
                "tick_size": float(rec.get("TickSize", 0.05) or 0.05),
                "expiry": _to_iso(rec.get("Expiry", "")),
                "strike": float(rec.get("StrikePrice") or 0),
                "option_type": rec.get("OptionType") or "",
                "token": str(rec.get("Token", "")),
            })
    except Exception:
        pass

    # Shoonya options — cross-link
    try:
        from scripts.shoonya_scriptmaster import get_options as sh_opts
        for rec in sh_opts(underlying, exchange, broker_expiry, strike, option_type):
            # Try to find matching Fyers record
            strike_val = float(rec.get("StrikePrice") or 0)
            otype_val = (rec.get("OptionType") or "").upper()
            matched = False
            for r in results:
                if (abs(r["strike"] - strike_val) < 0.01
                        and r["option_type"].upper() == otype_val
                        and r["expiry"] == _to_iso(rec.get("Expiry", ""))):
                    r["shoonya_symbol"] = rec.get("TradingSymbol", "")
                    matched = True
                    break
            if not matched:
                results.append({
                    "symbol": rec.get("Underlying", "") or rec.get("Symbol", ""),
                    "trading_symbol": rec.get("TradingSymbol", ""),
                    "fyers_symbol": "",
                    "shoonya_symbol": rec.get("TradingSymbol", ""),
                    "exchange": exchange,
                    "instrument_type": "OPT",
                    "lot_size": int(rec.get("LotSize") or 1),
                    "tick_size": float(rec.get("TickSize") or 0.05),
                    "expiry": _to_iso(rec.get("Expiry", "")),
                    "strike": strike_val,
                    "option_type": otype_val,
                    "token": str(rec.get("Token", "")),
                })
    except Exception:
        pass

    return sorted(results, key=lambda r: (r.get("expiry", ""), r.get("strike", 0)))


def get_lot_size(symbol: str, exchange: str = "NFO") -> int:
    """Get lot size from any available scriptmaster."""
    _ensure_loaded()
    sym = symbol.upper()

    try:
        from scripts.fyers_scriptmaster import get_near_futures
        rec = get_near_futures(sym, exchange)
        if rec and rec.get("LotSize", 0) > 0:
            return rec["LotSize"]
    except Exception:
        pass

    try:
        from scripts.shoonya_scriptmaster import get_futures as sh_futs
        futs = sh_futs(sym, exchange)
        if futs and futs[0].get("LotSize", 0) > 0:
            return futs[0]["LotSize"]
    except Exception:
        pass

    return 1


def requires_limit_order(exchange: str = "", trading_symbol: str = "",
                          tradingsymbol: str = "", token: str = "",
                          broker: str = "", **_kw) -> bool:
    """
    Check if LIMIT order is required (stock options / MCX options).
    Accepts both trading_symbol and tradingsymbol for backwards compatibility.
    """
    _ensure_loaded()
    ts = trading_symbol or tradingsymbol

    # Try Shoonya (has granular instrument types: OPTSTK, OPTFUT, OPTIDX)
    try:
        from scripts.shoonya_scriptmaster import requires_limit_order as shoo_rlo
        if shoo_rlo(exchange, tradingsymbol=ts):
            return True
    except Exception:
        pass

    # Fallback: Fyers lookup
    try:
        from scripts.fyers_scriptmaster import FYERS_SCRIPTMASTER
        rec = FYERS_SCRIPTMASTER.get(ts) or FYERS_SCRIPTMASTER.get(f"{exchange}:{ts}")
        if rec:
            instr = rec.get("Instrument", "")
            return instr == "OPT" and rec.get("Exchange", "") in ("NFO", "BFO", "MCX")
    except Exception:
        pass

    # Conservative default: options (CE/PE suffix) on F&O exchanges need LIMIT
    if ts:
        ts_upper = ts.upper()
        if "OPT" in ts_upper:
            return True
        if ts_upper.endswith("CE") or ts_upper.endswith("PE"):
            return exchange.upper() in ("NFO", "BFO", "MCX")
    return False


# ── Broker Symbol Resolution ─────────────────────────────────────────────────

def resolve_broker_symbol(
    trading_symbol: str,
    exchange: str,
    broker: str,
) -> str:
    """
    Convert canonical trading_symbol to broker-specific format.

    Args:
        trading_symbol: "NIFTY26APR24500CE" (no exchange prefix)
        exchange:       "NFO"
        broker:         "fyers" | "shoonya" | "angelone" | "dhan" | "kite" |
                        "upstox" | "groww" | "zerodha" | "paper"

    Returns:
        Fyers:    "NFO:NIFTY26APR24500CE"
        Kite:     "NIFTY26APR24500CE" (Kite uses exchange:tradingsymbol in API)
        Shoonya/Angel/Dhan/Paper: "NIFTY26APR24500CE"
        Upstox:   instrument_key if found, else trading_symbol
        Groww:    trading_symbol
    """
    clean = trading_symbol.split(":", 1)[-1] if ":" in trading_symbol else trading_symbol

    if broker == "fyers":
        return f"{exchange}:{clean}"

    if broker == "upstox":
        # Upstox uses instrument_key (e.g. NSE_FO|43919)
        try:
            from scripts.upstox_scriptmaster import UPSTOX_UNIVERSAL
            rec = UPSTOX_UNIVERSAL.get(f"{exchange}:{clean}")
            if rec:
                return rec.get("InstrumentKey", clean)
        except Exception:
            pass
        return clean

    # Shoonya, Angel One, Dhan, Kite, Groww, Paper use plain symbols
    return clean


def resolve_from_broker(broker_symbol: str, broker: str) -> tuple:
    """
    Parse broker-specific symbol to (trading_symbol, exchange).
    """
    if ":" in broker_symbol:
        parts = broker_symbol.split(":", 1)
        return parts[1], parts[0]
    return broker_symbol, ""


# ── Cross-broker symbol lookup ────────────────────────────────────────────────

def get_broker_symbols(
    trading_symbol: str,
    exchange: str,
) -> Dict[str, str]:
    """
    Given a canonical trading_symbol, find the equivalent symbol for each broker.

    Returns:
        {"fyers": "NFO:NIFTY26APR24500CE", "shoonya": "NIFTY26APR24500CE",
         "angelone": "NIFTY26APR24500CE", "dhan": "NIFTY26APR24500CE",
         "kite": "NIFTY26APR24500CE", "upstox": "NSE_FO|...", "groww": "NIFTY26APR24500CE"}
    """
    _ensure_loaded()
    result = {}

    # Fyers: try direct lookup
    try:
        from scripts.fyers_scriptmaster import FYERS_SCRIPTMASTER
        fyers_key = f"{exchange}:{trading_symbol}"
        if fyers_key in FYERS_SCRIPTMASTER:
            result["fyers"] = fyers_key
    except Exception:
        pass

    # Shoonya: try lookup by TradingSymbol
    try:
        from scripts.shoonya_scriptmaster import get_record_by_tsym
        rec = get_record_by_tsym(trading_symbol, exchange)
        if rec:
            result["shoonya"] = rec["TradingSymbol"]
    except Exception:
        pass

    # Angel One
    try:
        from scripts.angelone_scriptmaster import get_record_by_symbol
        rec = get_record_by_symbol(trading_symbol)
        if rec and rec.get("Exchange", "").upper() == exchange.upper():
            result["angelone"] = rec["Symbol"]
    except Exception:
        pass

    # Dhan
    try:
        from scripts.dhan_scriptmaster import SYMBOL_INDEX as DHAN_SYM_IDX
        rec = DHAN_SYM_IDX.get(trading_symbol.upper())
        if rec and rec.get("Exchange", "").upper() == exchange.upper():
            result["dhan"] = rec.get("SecurityId", trading_symbol)
    except Exception:
        pass

    # Kite
    try:
        from scripts.kite_scriptmaster import KITE_SCRIPTMASTER
        kite_key = f"{exchange}:{trading_symbol}"
        if kite_key in KITE_SCRIPTMASTER:
            result["kite"] = trading_symbol
    except Exception:
        pass

    # Upstox
    try:
        from scripts.upstox_scriptmaster import UPSTOX_UNIVERSAL
        upstox_key = f"{exchange}:{trading_symbol}"
        rec = UPSTOX_UNIVERSAL.get(upstox_key)
        if rec:
            result["upstox"] = rec.get("InstrumentKey", trading_symbol)
    except Exception:
        pass

    # Groww
    try:
        from scripts.groww_scriptmaster import GROWW_SCRIPTMASTER
        # Groww uses exchange:trading_symbol as key with raw exchange (NSE not NFO)
        for key, rec in GROWW_SCRIPTMASTER.items():
            if rec.get("TradingSymbol", "").upper() == trading_symbol.upper():
                if rec.get("Exchange", "").upper() == exchange.upper():
                    result["groww"] = rec["TradingSymbol"]
                    break
    except Exception:
        pass

    # Fallbacks
    if "fyers" not in result:
        result["fyers"] = f"{exchange}:{trading_symbol}"
    if "shoonya" not in result:
        result["shoonya"] = trading_symbol
    for broker in ("angelone", "dhan", "kite", "groww"):
        if broker not in result:
            result[broker] = trading_symbol
    if "upstox" not in result:
        result["upstox"] = trading_symbol

    return result


# ══════════════════════════════════════════════════════════════════════════════
# COMPATIBILITY API — same interface as old scriptmaster.py
# Used by: market_reader, entry_engine, strategy_executor_service, symbol_normalizer
# ══════════════════════════════════════════════════════════════════════════════

def _fyers_to_standard(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a fyers_scriptmaster record to the standard interface.

    TradingSymbol is the PLAIN symbol WITHOUT exchange prefix (broker-agnostic).
    FyersSymbol retains the full 'EXCHANGE:SYMBOL' format for Fyers-specific use.
    """
    fyers_sym = rec.get("FyersSymbol", "")
    # Strip exchange prefix for TradingSymbol (e.g. 'NSE:NIFTY26APRFUT' → 'NIFTY26APRFUT')
    plain_sym = fyers_sym.split(":", 1)[-1] if ":" in fyers_sym else fyers_sym
    return {
        "TradingSymbol": plain_sym,
        "FyersSymbol":   fyers_sym,
        "Symbol":        rec.get("Underlying", ""),
        "Underlying":    rec.get("Underlying", ""),
        "Exchange":      rec.get("Exchange", ""),
        "Token":         rec.get("Token", ""),
        "FyToken":       rec.get("FyToken", ""),
        "LotSize":       rec.get("LotSize", 1),
        "TickSize":      rec.get("TickSize", 0.05),
        "Expiry":        rec.get("Expiry", ""),
        "Instrument":    rec.get("Instrument", ""),
        "OptionType":    rec.get("OptionType"),
        "StrikePrice":   rec.get("StrikePrice"),
        "Description":   rec.get("Description", ""),
        "tsym":          plain_sym,
        "exch":          rec.get("Exchange", ""),
        "ls":            str(rec.get("LotSize", 1)),
    }


def _parse_expiry_date(expiry_str: str) -> Optional[Any]:
    """Parse DD-Mon-YYYY expiry string to date, or None."""
    if not expiry_str:
        return None
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(expiry_str, fmt).date()
        except ValueError:
            continue
    return None


def refresh(force: bool = False) -> int:
    """Download / reload instrument master data. Returns total symbol count."""
    counts = refresh_all(force=force)
    return sum(counts.values())


def get_future(
    symbol: str,
    exchange: str = "NFO",
    result: int = 0,
) -> Optional[Dict[str, Any]]:
    """
    Look up futures contract for a given underlying.
    Returns dict with TradingSymbol, LotSize, Expiry, Exchange, Token, etc.
    """
    _ensure_loaded()
    try:
        from scripts.fyers_scriptmaster import get_futures
        futs = get_futures(symbol.upper(), exchange.upper())
        if futs and len(futs) > result:
            return _fyers_to_standard(futs[result])
    except Exception as exc:
        logger.debug("get_future(%s, %s) fyers: %s", symbol, exchange, exc)

    # Try Shoonya
    try:
        from scripts.shoonya_scriptmaster import get_futures as sh_futs
        futs = sh_futs(symbol.upper(), exchange.upper())
        if futs and len(futs) > result:
            rec = futs[result]
            return {
                "TradingSymbol": rec.get("TradingSymbol", ""),
                "Symbol":        rec.get("Underlying", "") or rec.get("Symbol", ""),
                "Underlying":    rec.get("Underlying", "") or rec.get("Symbol", ""),
                "Exchange":      rec.get("Exchange", exchange),
                "LotSize":       int(rec.get("LotSize", 1)),
                "Expiry":        rec.get("Expiry", ""),
                "Instrument":    "FUT",
                "Token":         rec.get("Token", ""),
                "tsym":          rec.get("TradingSymbol", ""),
                "exch":          rec.get("Exchange", exchange),
                "ls":            str(rec.get("LotSize", 1)),
            }
    except Exception as exc:
        logger.debug("get_future(%s, %s) shoonya: %s", symbol, exchange, exc)

    # Fallback: synthetic record
    lot = get_lot_size(symbol, exchange)
    return {
        "TradingSymbol": f"{symbol.upper()}FUT",
        "Symbol": symbol.upper(),
        "Underlying": symbol.upper(),
        "Exchange": exchange.upper(),
        "LotSize": lot,
        "Expiry": "",
        "Instrument": "FUT",
        "Token": "",
        "FyersSymbol": "",
        "tsym": f"{symbol.upper()}FUT",
        "exch": exchange.upper(),
        "ls": str(lot),
    }


def get_futures_list(
    symbol: str,
    exchange: str = "NFO",
) -> List[Dict[str, Any]]:
    """Return all futures contracts for a symbol, sorted by expiry."""
    _ensure_loaded()
    try:
        from scripts.fyers_scriptmaster import get_futures
        return [_fyers_to_standard(r) for r in get_futures(symbol.upper(), exchange.upper())]
    except Exception:
        fut = get_future(symbol, exchange)
        return [fut] if fut else []


def get_option(
    symbol: str,
    exchange: str = "NFO",
    expiry: Optional[str] = None,
    strike: Optional[float] = None,
    option_type: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Look up a specific option contract. Returns first match or None."""
    results = get_options_list(symbol, exchange, expiry, strike, option_type)
    return results[0] if results else None


def get_options_list(
    symbol: str,
    exchange: str = "NFO",
    expiry: Optional[str] = None,
    strike: Optional[float] = None,
    option_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return options matching filters, sorted by (expiry, strike)."""
    _ensure_loaded()
    try:
        from scripts.fyers_scriptmaster import get_options as fy_opts
        raw = fy_opts(
            underlying=symbol.upper(),
            exchange=exchange.upper(),
            expiry=expiry,
            strike=strike,
            option_type=option_type.upper() if option_type else None,
        )
        return [_fyers_to_standard(r) for r in raw]
    except Exception as exc:
        logger.debug("get_options_list error: %s", exc)
        return []


def universal_symbol_search(
    symbol: str,
    exchange: str = "NFO",
) -> List[Dict[str, Any]]:
    """
    Search instrument master for matching symbols/underlyings.
    Returns list of dicts with TradingSymbol, LotSize, Expiry, etc.
    """
    _ensure_loaded()
    try:
        from scripts.fyers_scriptmaster import search
        raw = search(query=symbol, exchange=exchange)
        return [_fyers_to_standard(r) for r in raw]
    except Exception as exc:
        logger.debug("universal_symbol_search error: %s", exc)
        fut = get_future(symbol, exchange)
        return [fut] if fut else []


def options_expiry(
    symbol: str,
    exchange: str = "NFO",
    result: Optional[int] = None,
) -> Any:
    """
    Return option expiry dates for the symbol.
    result=None: full list, result=0: nearest, result=1: next, etc.
    Returns list[str] (DD-Mon-YYYY) or str.
    """
    _ensure_loaded()
    try:
        from scripts.fyers_scriptmaster import get_expiry_list
        expiries = get_expiry_list(symbol.upper(), exchange.upper(), "OPTION")
        if expiries:
            today = datetime.now().date()
            expiries = [e for e in expiries if _parse_expiry_date(e) and _parse_expiry_date(e) >= today]
        if result is not None:
            return expiries[result] if len(expiries) > result else ""
        return expiries
    except Exception as exc:
        logger.debug("options_expiry error: %s", exc)
        return "" if result is not None else []


def fut_expiry(
    symbol: str,
    exchange: str = "NFO",
    result: Optional[int] = None,
) -> Any:
    """Return futures expiry dates for the symbol."""
    _ensure_loaded()
    try:
        from scripts.fyers_scriptmaster import get_expiry_list
        expiries = get_expiry_list(symbol.upper(), exchange.upper(), "FUTURE")
        if expiries:
            today = datetime.now().date()
            expiries = [e for e in expiries if _parse_expiry_date(e) and _parse_expiry_date(e) >= today]
        if result is not None:
            return expiries[result] if len(expiries) > result else ""
        return expiries
    except Exception as exc:
        logger.debug("fut_expiry error: %s", exc)
        return "" if result is not None else []


def get_strike_gap(symbol: str, exchange: str = "NFO") -> int:
    """Return strike gap for the underlying."""
    return _STRIKE_GAPS.get(symbol.upper(), 50)


def get_spot_symbol(symbol: str) -> Optional[str]:
    """Return the Fyers spot/index symbol for an underlying (for quotes)."""
    return _INDEX_SYMBOLS.get(symbol.upper())


def build_option_trading_symbol(
    symbol: str,
    expiry: str,
    strike: float,
    option_type: str,
    exchange: str = "NFO",
) -> str:
    """Build a Fyers-format option trading symbol using real scriptmaster data."""
    _ensure_loaded()
    try:
        from scripts.fyers_scriptmaster import get_options as fy_opts
        matches = fy_opts(
            underlying=symbol.upper(),
            exchange=exchange.upper(),
            expiry=expiry,
            strike=strike,
            option_type=option_type.upper(),
        )
        if matches:
            return matches[0].get("FyersSymbol", "")
    except Exception:
        pass

    # Fallback: construct from convention
    try:
        dt = _parse_expiry_date(expiry)
        if dt:
            yymm = dt.strftime("%y%m")
            dd = dt.strftime("%d")
            prefix = "NSE" if exchange in ("NFO", "NSE") else "BSE"
            return f"{prefix}:{symbol.upper()}{yymm}{dd}{int(strike)}{option_type.upper()}"
    except Exception:
        pass
    return f"{symbol.upper()}{expiry}{int(strike)}{option_type.upper()}"


def get_instrument_count() -> int:
    """Return total loaded instrument count across all brokers."""
    total = 0
    try:
        from scripts.fyers_scriptmaster import FYERS_SCRIPTMASTER
        total += len(FYERS_SCRIPTMASTER)
    except Exception:
        pass
    try:
        from scripts.shoonya_scriptmaster import SHOONYA_UNIVERSAL
        total += len(SHOONYA_UNIVERSAL)
    except Exception:
        pass
    try:
        from scripts.angelone_scriptmaster import ANGELONE_SCRIPTMASTER
        total += len(ANGELONE_SCRIPTMASTER)
    except Exception:
        pass
    try:
        from scripts.dhan_scriptmaster import DHAN_SCRIPTMASTER
        total += len(DHAN_SCRIPTMASTER)
    except Exception:
        pass
    try:
        from scripts.kite_scriptmaster import KITE_SCRIPTMASTER
        total += len(KITE_SCRIPTMASTER)
    except Exception:
        pass
    try:
        from scripts.upstox_scriptmaster import UPSTOX_SCRIPTMASTER
        total += len(UPSTOX_SCRIPTMASTER)
    except Exception:
        pass
    try:
        from scripts.groww_scriptmaster import GROWW_SCRIPTMASTER
        total += len(GROWW_SCRIPTMASTER)
    except Exception:
        pass
    return total


# ── Date helpers ──────────────────────────────────────────────────────────────

_MONTH_MAP = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
    "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
    "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
}


def _to_iso(raw: str) -> str:
    """Convert any expiry string to ISO YYYY-MM-DD."""
    if not raw:
        return ""
    raw = raw.strip()
    if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
        return raw  # already ISO
    try:
        dt = datetime.strptime(raw, "%d-%b-%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass
    try:
        dt = datetime.strptime(raw, "%d-%B-%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return raw


def _iso_to_broker(iso: str) -> Optional[str]:
    """Convert ISO date to DD-Mon-YYYY for broker API."""
    if not iso:
        return None
    try:
        dt = datetime.strptime(iso, "%Y-%m-%d")
        return dt.strftime("%-d-%b-%Y")
    except ValueError:
        return iso
