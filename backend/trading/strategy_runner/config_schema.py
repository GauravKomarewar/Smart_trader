#!/usr/bin/env python3
"""
config_schema.py — JSON Strategy Config Validator (v4)
========================================================

Validates every possible field and combination in the v4.0 strategy JSON schema.
Ensures configs built by dashboard strategy_builder.html (or edited by hand) are correct
before the runner processes them.

All validation errors are collected (not fail-fast) so the user sees every problem at once.
"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

# =============================================================================
# VALID VALUE SETS (based on strategy_builder.html and engine models)
# =============================================================================

VALID_EXCHANGES = {"NFO", "MCX", "BFO", "NSE", "BSE", "CDS", "NCDEX"}

VALID_UNDERLYINGS = {
    "NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY",
    "SENSEX", "BANKEX",
    "CRUDEOIL", "CRUDEOILM", "GOLD", "GOLDM", "SILVER", "SILVERM",
    "NATURALGAS", "COPPER", "ZINC", "LEAD", "ALUMINIUM", "NICKEL",
}

VALID_PRODUCT_TYPES = {"NRML", "MIS", "CNC"}
VALID_ORDER_TYPES = {"MARKET", "LIMIT", "SL", "SLM"}
VALID_INSTRUMENT_TYPES = {"OPT", "FUT"}
VALID_OPTION_TYPES = {"CE", "PE"}
VALID_SIDES = {"BUY", "SELL"}

VALID_EXPIRY_MODES = {
    "strategy_default", "weekly_current", "weekly_next", "weekly_auto", "monthly_current", "monthly_next", "custom"
}
VALID_ENTRY_SEQUENCES = {"parallel", "sequential"}

VALID_STRIKE_MODES = {"standard", "exact", "atm_points", "atm_pct", "match_leg"}
VALID_STRIKE_SELECTIONS = {
    "atm", "atm+1", "atm-1", "atm+2", "atm-2", "atm+3", "atm-3",
    "atm+4", "atm-4", "atm+5", "atm-5",
    "atm_points", "atm_pct", "exact_strike",
    "delta", "straddle_delta", "premium", "iv", "theta", "vega", "gamma", "oi", "volume", "otm_pct",
    "max_pain", "pcr_inflection",
}

VALID_COMPARATORS = {
    ">", ">=", "<", "<=", "==", "!=", "~=",
    "between", "not_between",
    "crosses_above", "crosses_below",
    "is_true", "is_false",
}
VALID_JOIN_OPERATORS = {"AND", "OR"}

VALID_RULE_TYPES = {"if_then", "if_then_else", "if_any", "always"}

VALID_ENTRY_ACTIONS = {
    "simple_close_open_new",  # actually not used in entry, but builder includes it in action types
    "open_hedge", "close_leg", "partial_close_lots", "reduce_by_pct",
    "roll_to_next_expiry", "convert_to_spread",
}
# Entry actions are more specific; we'll validate based on the builder's output.
# In v4 entry, the action is defined per leg in its execution block, not as a top-level action.
# So we only need to validate that each leg has a valid side/option_type etc.

VALID_ADJUSTMENT_ACTIONS = {
    "simple_close_open_new", "open_hedge", "close_leg",
    "partial_close_lots", "reduce_by_pct",
    "roll_to_next_expiry", "convert_to_spread",
}

VALID_EXIT_ACTIONS = {"exit_all", "trail", "partial_50", "partial_lots", "lock_trail"}

# All parameters that can appear in conditions (from builder's BASE_PARAMS)
# This list is comprehensive but may be extended; we'll do a warning for unknown.
KNOWN_PARAMETERS = {
    # Option Legs
    "ce_ltp", "pe_ltp", "ce_strike", "pe_strike", "ce_oi", "pe_oi",
    "ce_oi_change", "pe_oi_change", "ce_volume", "pe_volume",
    "ce_bid_ask_spread", "pe_bid_ask_spread",
    "ce_moneyness", "pe_moneyness",

    # Greeks Signed
    "ce_delta", "pe_delta", "ce_gamma", "pe_gamma",
    "ce_theta", "pe_theta", "ce_vega", "pe_vega",

    # Greeks Absolute
    "abs(ce_delta)", "abs(pe_delta)", "abs(ce_gamma)", "abs(pe_gamma)",
    "abs(ce_theta)", "abs(pe_theta)", "abs(ce_vega)", "abs(pe_vega)",
    "abs(net_delta)", "abs(delta_diff)",

    # Portfolio Greeks
    "portfolio_delta", "portfolio_gamma", "portfolio_theta", "portfolio_vega",
    "abs(portfolio_delta)",

    # Volatility
    "ce_iv", "pe_iv", "iv_skew", "india_vix", "atm_iv",

    # Premium & Cost
    "total_premium", "premium_collected", "total_cost_basis",
    "ce_premium_decay_pct", "pe_premium_decay_pct", "total_premium_decay_pct",
    "max_profit_potential",

    # Strategy P&L
    "net_delta", "delta_diff", "combined_pnl", "combined_pnl_pct",
    "unrealised_pnl", "realised_pnl", "profit_step",

    # Breakeven
    "breakeven_upper", "breakeven_lower", "breakeven_distance",
    "spot_vs_upper_be", "spot_vs_lower_be",

    # OI / Market Data
    "pcr", "pcr_volume", "max_pain_strike", "spot_vs_max_pain",
    "total_oi_ce", "total_oi_pe", "oi_buildup_ce", "oi_buildup_pe",

    # Leg Status
    "active_legs_count", "closed_legs_count", "any_leg_active", "all_legs_active",
    "adjustment_count", "adj_count_today",

    # Time
    "time_current", "time_in_position_sec", "time_since_last_adj_sec",
    "session_type", "minutes_to_exit", "is_expiry_day", "days_to_expiry",

    # Index/Spot
    "spot_price", "spot_change", "spot_change_pct", "atm_strike", "fut_ltp",
    "spot_ltp",
    "index_NIFTY_ltp", "index_NIFTY_change_pct",
    "index_BANKNIFTY_ltp", "index_BANKNIFTY_change_pct",
    "index_SENSEX_ltp", "index_SENSEX_change_pct",
    "index_FINNIFTY_ltp", "index_FINNIFTY_change_pct",
    "index_MIDCPNIFTY_ltp",
    "index_CRUDEOIL_ltp", "index_CRUDEOIL_change_pct",
    "index_GOLDPETAL_ltp", "index_SILVERMIC_ltp", "index_NATGASMINI_ltp",

    # Dynamic leg refs
    "higher_delta_leg", "lower_delta_leg",
    "most_profitable_leg", "least_profitable_leg",
    "max_leg_delta", "min_leg_delta",
    "any_leg_delta_above", "all_legs_delta_below",
}

# Tag parameters are dynamic; they are validated by pattern.
TAG_PARAM_PATTERN = re.compile(
    r"^tag\.[^.]+\.(?:delta|abs_delta|gamma|abs_gamma|theta|abs_theta|vega|abs_vega|iv|pnl|pnl_pct|ltp|oi|oi_change|volume|strike|is_active|is_itm|moneyness)$"
)
INDEX_PARAM_PATTERN = re.compile(r"^index_[A-Za-z0-9]+_(?:ltp|pc|change|change_pct|open|high|low|close)$")

# =============================================================================
# VALIDATION ERROR CLASS
# =============================================================================
class ValidationError:
    """Single validation error with path context."""
    def __init__(self, path: str, message: str, severity: str = "error"):
        self.path = path
        self.message = message
        self.severity = severity  # "error" or "warning"

    def __repr__(self):
        return f"[{self.severity.upper()}] {self.path}: {self.message}"

# =============================================================================
# MAIN VALIDATION ENTRY POINT
# =============================================================================
def validate_config(config: Dict[str, Any]) -> Tuple[bool, List[ValidationError]]:
    """
    Validate a complete strategy JSON config (v4).

    Returns:
        (is_valid, errors_list)
        is_valid is True only if there are zero errors (warnings OK).
    """
    errors: List[ValidationError] = []

    if not isinstance(config, dict):
        errors.append(ValidationError("root", "Config must be a JSON object"))
        return False, errors

    # Top level fields
    _validate_top_level(config, errors)

    # identity section
    if "identity" in config:
        _validate_identity(config["identity"], errors, prefix="identity")
    else:
        errors.append(ValidationError("identity", "Missing required section 'identity'"))

    # timing section
    if "timing" in config:
        _validate_timing(config["timing"], errors, prefix="timing")
    else:
        errors.append(ValidationError("timing", "Missing required section 'timing'"))

    # schedule section
    if "schedule" in config:
        _validate_schedule(config["schedule"], errors, prefix="schedule")
    else:
        errors.append(ValidationError("schedule", "Missing required section 'schedule'"))

    # market_data section (optional)
    if "market_data" in config:
        _validate_market_data(config["market_data"], errors, prefix="market_data")

    # entry section
    if "entry" in config:
        _validate_entry(config["entry"], errors, prefix="entry")
    else:
        errors.append(ValidationError("entry", "Missing required section 'entry'"))

    # adjustment section (optional)
    if "adjustment" in config:
        _validate_adjustment(config["adjustment"], errors, prefix="adjustment")

    # exit section
    if "exit" in config:
        _validate_exit(config["exit"], errors, prefix="exit")
    else:
        errors.append(ValidationError("exit", "Missing required section 'exit'"))

    # rms section (optional)
    if "rms" in config:
        _validate_rms(config["rms"], errors, prefix="rms")

    is_valid = not any(e.severity == "error" for e in errors)
    return is_valid, errors


def validate_config_file(path: str) -> Tuple[bool, List[ValidationError], Optional[Dict]]:
    """
    Load and validate a JSON config file.

    Returns:
        (is_valid, errors, parsed_config_or_None)
    """
    try:
        with open(path, "r") as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        return False, [ValidationError("file", f"Invalid JSON: {e}")], None
    except FileNotFoundError:
        return False, [ValidationError("file", f"File not found: {path}")], None
    except Exception as e:
        return False, [ValidationError("file", f"Cannot read file: {e}")], None

    is_valid, errors = validate_config(config)
    return is_valid, errors, config


def coerce_config_numerics(config: Any) -> Any:
    """
    Recursively convert numeric strings to appropriate int/float.
    Useful after loading JSON where numbers may be strings.
    """
    if isinstance(config, dict):
        coerced = {k: coerce_config_numerics(v) for k, v in config.items()}
        # Preserve int semantics for known count/time fields.
        int_keys = {
            "max_trades_per_day", "cooldown_seconds", "max_reentries_per_day",
            "max_per_day", "max_total", "adjustments", "lots", "qty",
        }
        for k in int_keys:
            if k in coerced and isinstance(coerced[k], float):
                coerced[k] = int(coerced[k])
        return coerced
    if isinstance(config, list):
        return [coerce_config_numerics(item) for item in config]
    if isinstance(config, str):
        # Try int first, then float, else keep string
        try:
            return int(config)
        except ValueError:
            try:
                return float(config)
            except ValueError:
                return config
    return config

# =============================================================================
# SECTION VALIDATORS
# =============================================================================
def _validate_top_level(config: Dict, errors: List[ValidationError]):
    """Validate top-level fields."""
    if "schema_version" not in config:
        errors.append(ValidationError("schema_version", "Missing schema_version"))
    elif config["schema_version"] != "4.0":
        errors.append(ValidationError("schema_version", f"Expected '4.0', got '{config['schema_version']}'", "warning"))

    if "name" not in config:
        errors.append(ValidationError("name", "Missing strategy name"))
    elif not isinstance(config["name"], str) or not config["name"].strip():
        errors.append(ValidationError("name", "Strategy name must be a non-empty string"))

    if "id" in config and not isinstance(config["id"], str):
        errors.append(ValidationError("id", "id must be a string"))

    if "description" in config and not isinstance(config["description"], str):
        errors.append(ValidationError("description", "description must be a string"))

    if "type" in config and config["type"] not in {
        "neutral", "directional", "spread", "calendar", "volatility", "gamma_scalp", "custom"
    }:
        errors.append(ValidationError("type", f"Unknown strategy type '{config.get('type')}'", "warning"))

    if "enabled" in config and not isinstance(config["enabled"], bool):
        errors.append(ValidationError("enabled", "enabled must be a boolean"))


def _validate_identity(identity: Dict, errors: List[ValidationError], prefix: str):
    if not isinstance(identity, dict):
        errors.append(ValidationError(prefix, "identity must be an object"))
        return

    # exchange
    exchange = identity.get("exchange")
    if not exchange:
        errors.append(ValidationError(f"{prefix}.exchange", "Missing exchange"))
    elif exchange.upper() not in VALID_EXCHANGES:
        errors.append(ValidationError(f"{prefix}.exchange", f"Invalid exchange '{exchange}'. Valid: {sorted(VALID_EXCHANGES)}"))

    # underlying
    underlying = identity.get("underlying")
    if not underlying:
        errors.append(ValidationError(f"{prefix}.underlying", "Missing underlying"))
    elif underlying.upper() not in VALID_UNDERLYINGS:
        errors.append(ValidationError(f"{prefix}.underlying", f"Unknown underlying '{underlying}'. Valid: {sorted(VALID_UNDERLYINGS)}", "warning"))

    # instrument_type (optional, but if present must be valid)
    if "instrument_type" in identity and identity["instrument_type"] not in {"OPTIDX", "OPTSTK", "FUTIDX", "FUTSTK", "MCX", "CASH"}:
        errors.append(ValidationError(f"{prefix}.instrument_type", f"Invalid instrument_type '{identity['instrument_type']}'", "warning"))

    # product_type
    product = identity.get("product_type")
    if product and product.upper() not in VALID_PRODUCT_TYPES:
        errors.append(ValidationError(f"{prefix}.product_type", f"Invalid product_type '{product}'. Valid: {sorted(VALID_PRODUCT_TYPES)}"))

    # order_type
    order_type = identity.get("order_type")
    if order_type and order_type.upper() not in VALID_ORDER_TYPES:
        errors.append(ValidationError(f"{prefix}.order_type", f"Invalid order_type '{order_type}'. Valid: {sorted(VALID_ORDER_TYPES)}"))

    # lots
    lots = identity.get("lots")
    if lots is not None:
        try:
            lots_val = float(lots)
            if lots_val < 1:
                errors.append(ValidationError(f"{prefix}.lots", "lots must be >= 1"))
        except (TypeError, ValueError):
            errors.append(ValidationError(f"{prefix}.lots", "lots must be a number"))

    # NOTE: db_file/db_path deprecated – auto-resolved from exchange+symbol+expiry_mode
    if "db_file" in identity:
        errors.append(
            ValidationError(
                f"{prefix}.db_file",
                "db_file is deprecated – auto-resolved from exchange+symbol+expiry_mode",
                "warning",
            )
        )
    if "db_path" in identity:
        errors.append(
            ValidationError(
                f"{prefix}.db_path",
                "db_path is deprecated – auto-resolved from exchange+symbol+expiry_mode",
                "warning",
            )
        )
    test_mode = identity.get("test_mode")
    if test_mode is not None and not isinstance(test_mode, bool):
        errors.append(ValidationError(f"{prefix}.test_mode", "test_mode must be a boolean"))


def _validate_timing(timing: Dict, errors: List[ValidationError], prefix: str):
    if not isinstance(timing, dict):
        errors.append(ValidationError(prefix, "timing must be an object"))
        return

    for key in ("entry_window_start", "entry_window_end", "eod_exit_time"):
        val = timing.get(key)
        if not val:
            errors.append(ValidationError(f"{prefix}.{key}", f"Missing {key}"))
        elif not _is_valid_time(val):
            errors.append(ValidationError(f"{prefix}.{key}", f"Invalid time format '{val}'. Expected HH:MM"))

    # Optionally check that entry_start < entry_end
    start = timing.get("entry_window_start")
    end = timing.get("entry_window_end")
    if start and end and _is_valid_time(start) and _is_valid_time(end):
        if _time_to_minutes(start) >= _time_to_minutes(end):
            errors.append(ValidationError(prefix, "entry_window_start must be before entry_window_end", "warning"))


def _validate_schedule(schedule: Dict, errors: List[ValidationError], prefix: str):
    if not isinstance(schedule, dict):
        errors.append(ValidationError(prefix, "schedule must be an object"))
        return

    # frequency
    freq = schedule.get("frequency")
    if freq and freq not in {"daily", "weekly", "monthly", "event", "manual"}:
        errors.append(ValidationError(f"{prefix}.frequency", f"Invalid frequency '{freq}'", "warning"))

    # active_days
    active_days = schedule.get("active_days")
    if active_days is not None:
        if not isinstance(active_days, list):
            errors.append(ValidationError(f"{prefix}.active_days", "active_days must be an array"))
        else:
            valid_days = {"mon", "tue", "wed", "thu", "fri", "sat"}
            for day in active_days:
                if day not in valid_days:
                    errors.append(ValidationError(f"{prefix}.active_days", f"Invalid day '{day}'. Valid: {sorted(valid_days)}", "warning"))

    # expiry_mode
    exp_mode = schedule.get("expiry_mode")
    if exp_mode and exp_mode not in VALID_EXPIRY_MODES:
        errors.append(ValidationError(f"{prefix}.expiry_mode", f"Invalid expiry_mode '{exp_mode}'. Valid: {sorted(VALID_EXPIRY_MODES)}"))

    # dte_min / dte_max (optional numeric)
    for key in ("dte_min", "dte_max", "square_off_before_min", "max_reentries_per_day"):
        val = schedule.get(key)
        if val is not None:
            try:
                float_val = float(val)
                if float_val < 0:
                    errors.append(ValidationError(f"{prefix}.{key}", f"{key} must be >= 0"))
            except (TypeError, ValueError):
                errors.append(ValidationError(f"{prefix}.{key}", f"{key} must be a number"))

    # entry_on_expiry_day (boolean)
    entry_on_exp = schedule.get("entry_on_expiry_day")
    if entry_on_exp is not None and not isinstance(entry_on_exp, bool):
        errors.append(ValidationError(f"{prefix}.entry_on_expiry_day", "entry_on_expiry_day must be a boolean"))


def _validate_market_data(md: Dict, errors: List[ValidationError], prefix: str):
    if not isinstance(md, dict):
        errors.append(ValidationError(prefix, "market_data must be an object"))
        return

    source = md.get("source", "sqlite")
    if source not in {"sqlite", "database"}:
        errors.append(ValidationError(f"{prefix}.source", f"Invalid source '{source}'. Valid: sqlite, database"))

    # NOTE: db_file deprecated – auto-resolved from exchange+symbol+expiry_mode
    if "db_file" in md:
        errors.append(
            ValidationError(
                f"{prefix}.db_file",
                "db_file is deprecated – auto-resolved from exchange+symbol+expiry_mode",
                "warning",
            )
        )


def _validate_entry(entry: Dict, errors: List[ValidationError], prefix: str):
    if not isinstance(entry, dict):
        errors.append(ValidationError(prefix, "entry must be an object"))
        return

    # entry_sequence
    seq = entry.get("entry_sequence")
    if seq and seq not in VALID_ENTRY_SEQUENCES:
        errors.append(ValidationError(f"{prefix}.entry_sequence", f"Invalid entry_sequence '{seq}'. Valid: {sorted(VALID_ENTRY_SEQUENCES)}"))

    # global_conditions
    global_conds = entry.get("global_conditions")
    if global_conds is not None:
        if not isinstance(global_conds, list):
            errors.append(ValidationError(f"{prefix}.global_conditions", "global_conditions must be an array"))
        else:
            for i, cond in enumerate(global_conds):
                _validate_condition(cond, f"{prefix}.global_conditions[{i}]", errors)

    # legs
    legs = entry.get("legs")
    if not legs:
        errors.append(ValidationError(f"{prefix}.legs", "Missing entry.legs"))
    elif not isinstance(legs, list):
        errors.append(ValidationError(f"{prefix}.legs", "legs must be an array"))
    else:
        for i, leg in enumerate(legs):
            _validate_entry_leg(leg, f"{prefix}.legs[{i}]", errors)


def _validate_entry_leg(leg: Dict, path: str, errors: List[ValidationError]):
    if not isinstance(leg, dict):
        errors.append(ValidationError(path, "leg must be an object"))
        return

    # tag (required)
    tag = leg.get("tag")
    if not tag:
        errors.append(ValidationError(f"{path}.tag", "Missing leg tag"))
    elif not isinstance(tag, str):
        errors.append(ValidationError(f"{path}.tag", "tag must be a string"))

    # instrument (optional, default OPT)
    instrument = leg.get("instrument", "OPT")
    if instrument not in VALID_INSTRUMENT_TYPES:
        errors.append(ValidationError(f"{path}.instrument", f"Invalid instrument '{instrument}'. Valid: {sorted(VALID_INSTRUMENT_TYPES)}"))

    # side (required)
    side = leg.get("side")
    if not side:
        errors.append(ValidationError(f"{path}.side", "Missing side"))
    elif side not in VALID_SIDES:
        errors.append(ValidationError(f"{path}.side", f"Invalid side '{side}'. Valid: {sorted(VALID_SIDES)}"))

    # option_type (required if instrument OPT, not allowed if FUT)
    opt_type = leg.get("option_type")
    if instrument == "OPT":
        if not opt_type:
            errors.append(ValidationError(f"{path}.option_type", "Missing option_type for option leg"))
        elif opt_type not in VALID_OPTION_TYPES:
            errors.append(ValidationError(f"{path}.option_type", f"Invalid option_type '{opt_type}'. Valid: {sorted(VALID_OPTION_TYPES)}"))
    else:  # FUT
        if opt_type is not None:
            errors.append(ValidationError(f"{path}.option_type", "option_type must not be present for futures leg"))

    # lots (required)
    lots = leg.get("lots")
    if lots is None:
        errors.append(ValidationError(f"{path}.lots", "Missing lots"))
    else:
        try:
            lots_val = int(lots)
            if lots_val < 1:
                errors.append(ValidationError(f"{path}.lots", "lots must be >= 1"))
        except (TypeError, ValueError):
            errors.append(ValidationError(f"{path}.lots", "lots must be an integer"))

    # order_type (optional)
    order_type = leg.get("order_type")
    if order_type and order_type not in VALID_ORDER_TYPES:
        errors.append(ValidationError(f"{path}.order_type", f"Invalid order_type '{order_type}'. Valid: {sorted(VALID_ORDER_TYPES)}", "warning"))

    # expiry (optional, default strategy_default)
    expiry = leg.get("expiry")
    if expiry and expiry not in VALID_EXPIRY_MODES and expiry != "strategy_default":
        errors.append(ValidationError(f"{path}.expiry", f"Invalid expiry '{expiry}'. Valid: {sorted(VALID_EXPIRY_MODES)}", "warning"))

    # label (optional)
    label = leg.get("label")
    if label is not None and not isinstance(label, str):
        errors.append(ValidationError(f"{path}.label", "label must be a string"))

    # group (optional)
    group = leg.get("group")
    if group is not None and not isinstance(group, str):
        errors.append(ValidationError(f"{path}.group", "group must be a string"))

    # IF conditions (array)
    if_conds = leg.get("conditions", [])
    if not isinstance(if_conds, list):
        errors.append(ValidationError(f"{path}.conditions", "conditions must be an array"))
    else:
        for i, cond in enumerate(if_conds):
            _validate_condition(cond, f"{path}.conditions[{i}]", errors)

    # else_enabled (boolean)
    else_enabled = leg.get("else_enabled", False)
    if not isinstance(else_enabled, bool):
        errors.append(ValidationError(f"{path}.else_enabled", "else_enabled must be a boolean"))

    # else_conditions (array, present only if else_enabled)
    else_conds = leg.get("else_conditions", [])
    if else_enabled:
        if not isinstance(else_conds, list):
            errors.append(ValidationError(f"{path}.else_conditions", "else_conditions must be an array"))
        else:
            for i, cond in enumerate(else_conds):
                _validate_condition(cond, f"{path}.else_conditions[{i}]", errors)

    # else_action (object, present only if else_enabled)
    else_action = leg.get("else_action")
    if else_enabled:
        if not isinstance(else_action, dict):
            errors.append(ValidationError(f"{path}.else_action", "else_action must be an object"))
        else:
            _validate_leg_execution_config(else_action, f"{path}.else_action", errors, instrument)

    # IF branch execution config is the leg itself (already validated above)
    # We need to validate that the leg contains the necessary fields for execution.
    # Those are: side, lots, order_type (already done), plus strike-related fields for options.
    if instrument == "OPT":
        _validate_leg_strike_config(leg, f"{path}", errors)


def _validate_leg_execution_config(
    cfg: Dict,
    path: str,
    errors: List[ValidationError],
    instrument: str,
    allow_dynamic_option_types: bool = False,
):
    """Validate the execution part of a leg (side, lots, strike_mode, etc.)."""
    # side (required)
    side = cfg.get("side")
    if not side:
        errors.append(ValidationError(f"{path}.side", "Missing side"))
    elif side not in VALID_SIDES:
        errors.append(ValidationError(f"{path}.side", f"Invalid side '{side}'. Valid: {sorted(VALID_SIDES)}"))

    # lots (required)
    lots = cfg.get("lots")
    if lots is not None:
        try:
            lots_val = int(lots)
            if lots_val < 1:
                errors.append(ValidationError(f"{path}.lots", "lots must be >= 1"))
        except (TypeError, ValueError):
            errors.append(ValidationError(f"{path}.lots", "lots must be an integer"))

    # order_type (optional)
    order_type = cfg.get("order_type")
    if order_type and order_type not in VALID_ORDER_TYPES:
        errors.append(ValidationError(f"{path}.order_type", f"Invalid order_type '{order_type}'. Valid: {sorted(VALID_ORDER_TYPES)}", "warning"))

    if instrument == "OPT":
        _validate_leg_strike_config(cfg, path, errors, allow_dynamic_option_types=allow_dynamic_option_types)


def _validate_leg_strike_config(
    cfg: Dict,
    path: str,
    errors: List[ValidationError],
    allow_dynamic_option_types: bool = False,
):
    """Validate strike-related fields for an option leg."""
    # option_type (required)
    opt_type = cfg.get("option_type")
    if not opt_type:
        errors.append(ValidationError(f"{path}.option_type", "Missing option_type"))
    else:
        valid_option_types = set(VALID_OPTION_TYPES)
        if allow_dynamic_option_types:
            valid_option_types.update({"MATCH_CLOSING", "MATCH_OPPOSITE"})
        if opt_type not in valid_option_types:
            errors.append(ValidationError(f"{path}.option_type", f"Invalid option_type '{opt_type}'. Valid: {sorted(valid_option_types)}"))

    # strike_mode (required)
    strike_mode = cfg.get("strike_mode")
    if not strike_mode:
        errors.append(ValidationError(f"{path}.strike_mode", "Missing strike_mode"))
    elif strike_mode not in VALID_STRIKE_MODES:
        errors.append(ValidationError(f"{path}.strike_mode", f"Invalid strike_mode '{strike_mode}'. Valid: {sorted(VALID_STRIKE_MODES)}"))

    # Now validate based on strike_mode
    if strike_mode == "standard":
        sel = cfg.get("strike_selection")
        if not sel:
            errors.append(ValidationError(f"{path}.strike_selection", "Missing strike_selection for standard mode"))
        elif sel not in VALID_STRIKE_SELECTIONS:
            errors.append(ValidationError(f"{path}.strike_selection", f"Unknown strike_selection '{sel}'. Valid subset of {sorted(VALID_STRIKE_SELECTIONS)}", "warning"))
        # strike_value is optional, but if present should be numeric
        val = cfg.get("strike_value")
        if val is not None and str(val).strip() != "":
            try:
                float(val)
            except (TypeError, ValueError):
                errors.append(ValidationError(f"{path}.strike_value", "strike_value must be a number"))

    elif strike_mode == "exact":
        exact = cfg.get("exact_strike")
        if exact is None:
            errors.append(ValidationError(f"{path}.exact_strike", "Missing exact_strike for exact mode"))
        else:
            try:
                float(exact)
            except (TypeError, ValueError):
                errors.append(ValidationError(f"{path}.exact_strike", "exact_strike must be a number"))
        rounding = cfg.get("rounding")
        if rounding is not None:
            try:
                float(rounding)
            except (TypeError, ValueError):
                errors.append(ValidationError(f"{path}.rounding", "rounding must be a number"))

    elif strike_mode == "atm_points":
        offset = cfg.get("atm_offset_points")
        if offset is None:
            errors.append(ValidationError(f"{path}.atm_offset_points", "Missing atm_offset_points for atm_points mode"))
        else:
            try:
                float(offset)
            except (TypeError, ValueError):
                errors.append(ValidationError(f"{path}.atm_offset_points", "atm_offset_points must be a number"))
        rounding = cfg.get("rounding")
        if rounding is not None:
            try:
                float(rounding)
            except (TypeError, ValueError):
                errors.append(ValidationError(f"{path}.rounding", "rounding must be a number"))

    elif strike_mode == "atm_pct":
        pct = cfg.get("atm_offset_pct")
        if pct is None:
            errors.append(ValidationError(f"{path}.atm_offset_pct", "Missing atm_offset_pct for atm_pct mode"))
        else:
            try:
                float(pct)
            except (TypeError, ValueError):
                errors.append(ValidationError(f"{path}.atm_offset_pct", "atm_offset_pct must be a number"))
        rounding = cfg.get("rounding")
        if rounding is not None:
            try:
                float(rounding)
            except (TypeError, ValueError):
                errors.append(ValidationError(f"{path}.rounding", "rounding must be a number"))

    elif strike_mode == "match_leg":
        match_leg = cfg.get("match_leg")
        if not match_leg:
            errors.append(ValidationError(f"{path}.match_leg", "Missing match_leg for match_leg mode"))
        match_param = cfg.get("match_param")
        if not match_param:
            errors.append(ValidationError(f"{path}.match_param", "Missing match_param for match_leg mode"))
        elif match_param not in {"delta", "abs_delta", "iv", "theta", "abs_theta", "vega", "gamma", "ltp", "oi", "volume", "strike", "moneyness"}:
            errors.append(ValidationError(f"{path}.match_param", f"Invalid match_param '{match_param}'", "warning"))
        offset = cfg.get("match_offset", 0)
        mult = cfg.get("match_multiplier", 1)
        try:
            float(offset)
            float(mult)
        except (TypeError, ValueError):
            errors.append(ValidationError(f"{path}.match_offset", "match_offset and match_multiplier must be numbers"))


def _validate_condition(cond: Dict, path: str, errors: List[ValidationError]):
    """Validate a single condition dict (could be simple or compound)."""
    if not isinstance(cond, dict):
        errors.append(ValidationError(path, "condition must be an object"))
        return

    # Check if it's a compound condition (has "operator" and "rules")
    if "operator" in cond and "rules" in cond:
        # Compound condition
        op = cond["operator"]
        if op not in VALID_JOIN_OPERATORS:
            errors.append(ValidationError(f"{path}.operator", f"Invalid operator '{op}'. Valid: {sorted(VALID_JOIN_OPERATORS)}"))
        rules = cond["rules"]
        if not isinstance(rules, list):
            errors.append(ValidationError(f"{path}.rules", "rules must be an array"))
        else:
            for i, rule in enumerate(rules):
                _validate_condition(rule, f"{path}.rules[{i}]", errors)
        return

    # Simple condition
    param = cond.get("parameter")
    if not param:
        errors.append(ValidationError(f"{path}.parameter", "Missing parameter"))
    elif not _is_valid_parameter(param):
        errors.append(ValidationError(f"{path}.parameter", f"Unknown parameter '{param}'", "warning"))

    comp = cond.get("comparator")
    if not comp:
        errors.append(ValidationError(f"{path}.comparator", "Missing comparator"))
    elif comp not in VALID_COMPARATORS:
        errors.append(ValidationError(f"{path}.comparator", f"Invalid comparator '{comp}'. Valid: {sorted(VALID_COMPARATORS)}"))

    # value is required for most comparators, except is_true/is_false
    if comp in ("is_true", "is_false"):
        if "value" in cond:
            errors.append(ValidationError(f"{path}.value", f"value should not be present for comparator {comp}", "warning"))
    else:
        if "value" not in cond:
            errors.append(ValidationError(f"{path}.value", "Missing value"))

    # For between/not_between, runtime uses separate 'value' and 'value2' fields.
    # ✅ BUG FIX: Schema was validating value as [min, max] list, but _dict_to_condition
    # reads d["value"] and d.get("value2") as separate fields. Accept both formats.
    if comp in ("between", "not_between"):
        val = cond.get("value")
        val2 = cond.get("value2")
        if isinstance(val, (list, tuple)):
            # Legacy array format [min, max] — accepted but runtime needs value+value2
            if len(val) != 2:
                errors.append(ValidationError(f"{path}.value", f"Comparator '{comp}' array must have exactly 2 elements"))
            else:
                try:
                    if isinstance(val, list):
                        cond["value"] = [float(val[0]), float(val[1])]
                    else:
                        float(val[0])
                        float(val[1])
                except (TypeError, ValueError):
                    errors.append(ValidationError(f"{path}.value", "Both elements of value must be numbers"))
        else:
            # Preferred format: value + value2 as separate fields
            if val is None:
                errors.append(ValidationError(f"{path}.value", f"Comparator '{comp}' requires value"))
            else:
                try:
                    float(val)
                except (TypeError, ValueError):
                    errors.append(ValidationError(f"{path}.value", "value must be a number"))
            if val2 is None:
                errors.append(ValidationError(f"{path}.value2", f"Comparator '{comp}' requires value2"))
            else:
                try:
                    float(val2)
                except (TypeError, ValueError):
                    errors.append(ValidationError(f"{path}.value2", "value2 must be a number"))

    # For ~=, tolerance is optional but if present should be number
    if comp == "~=" and "tolerance" in cond:
        tol = cond["tolerance"]
        try:
            float(tol)
        except (TypeError, ValueError):
            errors.append(ValidationError(f"{path}.tolerance", "tolerance must be a number"))

    # join may be present in a list context; if present, must be valid
    join = cond.get("join")
    if join and join not in VALID_JOIN_OPERATORS:
        errors.append(ValidationError(f"{path}.join", f"Invalid join '{join}'. Valid: {sorted(VALID_JOIN_OPERATORS)}"))


def _validate_adjustment(adj: Dict, errors: List[ValidationError], prefix: str):
    if not isinstance(adj, dict):
        errors.append(ValidationError(prefix, "adjustment must be an object"))
        return

    enabled = adj.get("enabled", False)
    if not isinstance(enabled, bool):
        errors.append(ValidationError(f"{prefix}.enabled", "enabled must be a boolean"))

    rules = adj.get("rules", [])
    if not isinstance(rules, list):
        errors.append(ValidationError(f"{prefix}.rules", "rules must be an array"))
        return

    priorities_seen = set()
    for i, rule in enumerate(rules):
        _validate_adjustment_rule(rule, f"{prefix}.rules[{i}]", errors, priorities_seen)


def _validate_adjustment_rule(rule: Dict, path: str, errors: List[ValidationError], priorities_seen: Set[int]):
    if not isinstance(rule, dict):
        errors.append(ValidationError(path, "adjustment rule must be an object"))
        return

    # name (optional but recommended)
    name = rule.get("name")
    if name is not None and not isinstance(name, str):
        errors.append(ValidationError(f"{path}.name", "name must be a string"))

    # priority
    pri = rule.get("priority")
    if pri is None:
        errors.append(ValidationError(f"{path}.priority", "Missing priority"))
    else:
        try:
            pri_val = int(pri)
            if pri_val < 1:
                errors.append(ValidationError(f"{path}.priority", "priority must be >= 1"))
            if pri_val in priorities_seen:
                errors.append(ValidationError(f"{path}.priority", f"Duplicate priority {pri_val}", "warning"))
            else:
                priorities_seen.add(pri_val)
        except (TypeError, ValueError):
            errors.append(ValidationError(f"{path}.priority", "priority must be an integer"))

    # max_per_day (optional)
    max_day = rule.get("max_per_day")
    if max_day is not None:
        try:
            maxd = int(max_day)
            if maxd < 1:
                errors.append(ValidationError(f"{path}.max_per_day", "max_per_day must be >= 1"))
        except (TypeError, ValueError):
            errors.append(ValidationError(f"{path}.max_per_day", "max_per_day must be an integer"))

    # max_total (optional)
    max_total = rule.get("max_total")
    if max_total is not None:
        try:
            mt = int(max_total)
            if mt < 1:
                errors.append(ValidationError(f"{path}.max_total", "max_total must be >= 1", "warning"))
        except (TypeError, ValueError):
            errors.append(ValidationError(f"{path}.max_total", "max_total must be an integer"))

    # cooldown_sec (optional)
    cooldown = rule.get("cooldown_sec")
    if cooldown is not None:
        try:
            cd = float(cooldown)
            if cd < 0:
                errors.append(ValidationError(f"{path}.cooldown_sec", "cooldown_sec must be >= 0"))
        except (TypeError, ValueError):
            errors.append(ValidationError(f"{path}.cooldown_sec", "cooldown_sec must be a number"))

    # retrigger (optional, bool)
    if "retriger" in rule and "retrigger" not in rule:
        errors.append(ValidationError(f"{path}.retriger", "retriger is deprecated; use retrigger", "warning"))
    key = "retrigger"
    if "retriger" in rule and "retrigger" not in rule:
        key = "retriger"
    retrig = rule.get("retrigger", rule.get("retriger"))
    if retrig is not None and not isinstance(retrig, bool):
        errors.append(ValidationError(f"{path}.{key}", "retrigger must be a boolean"))

    # leg_guard (optional) – should be a tag string
    leg_guard = rule.get("leg_guard")
    if leg_guard is not None and not isinstance(leg_guard, str):
        errors.append(ValidationError(f"{path}.leg_guard", "leg_guard must be a string"))

    # conditions (compound or array)
    conds = rule.get("conditions")
    if conds is not None:
        if isinstance(conds, dict):
            _validate_condition(conds, f"{path}.conditions", errors)
        elif isinstance(conds, list):
            for i, c in enumerate(conds):
                _validate_condition(c, f"{path}.conditions[{i}]", errors)
        else:
            errors.append(ValidationError(f"{path}.conditions", "conditions must be an object or array"))

    # action (required)
    action = rule.get("action")
    if not action:
        errors.append(ValidationError(f"{path}.action", "Missing action"))
    else:
        _validate_adjustment_action(action, f"{path}.action", errors)

    # else_enabled (bool)
    else_enabled = rule.get("else_enabled", False)
    if not isinstance(else_enabled, bool):
        errors.append(ValidationError(f"{path}.else_enabled", "else_enabled must be a boolean"))

    # else_conditions (optional)
    else_conds = rule.get("else_conditions")
    if else_enabled:
        if else_conds is None:
            errors.append(ValidationError(f"{path}.else_conditions", "Missing else_conditions when else_enabled is true"))
        elif isinstance(else_conds, dict):
            _validate_condition(else_conds, f"{path}.else_conditions", errors)
        elif isinstance(else_conds, list):
            for i, c in enumerate(else_conds):
                _validate_condition(c, f"{path}.else_conditions[{i}]", errors)
        else:
            errors.append(ValidationError(f"{path}.else_conditions", "else_conditions must be an object or array"))

    # else_action (optional)
    else_action = rule.get("else_action")
    if else_enabled and else_action is None:
        errors.append(ValidationError(f"{path}.else_action", "Missing else_action when else_enabled is true"))
    elif else_action is not None:
        _validate_adjustment_action(else_action, f"{path}.else_action", errors)


def _validate_adjustment_action(action: Dict, path: str, errors: List[ValidationError]):
    if not isinstance(action, dict):
        errors.append(ValidationError(path, "action must be an object"))
        return

    atype = action.get("type")
    if not atype:
        errors.append(ValidationError(f"{path}.type", "Missing action type"))
    elif atype not in VALID_ADJUSTMENT_ACTIONS:
        errors.append(ValidationError(f"{path}.type", f"Invalid action type '{atype}'. Valid: {sorted(VALID_ADJUSTMENT_ACTIONS)}"))

    # Validate based on type
    if atype == "close_leg":
        close_tag = action.get("close_tag")
        if not close_tag:
            errors.append(ValidationError(f"{path}.close_tag", "Missing close_tag for close_leg"))
        elif not isinstance(close_tag, str):
            errors.append(ValidationError(f"{path}.close_tag", "close_tag must be a string"))

    elif atype == "partial_close_lots":
        close_tag = action.get("close_tag")
        if not close_tag:
            errors.append(ValidationError(f"{path}.close_tag", "Missing close_tag for partial_close_lots"))
        lots = action.get("lots")
        if lots is None:
            errors.append(ValidationError(f"{path}.lots", "Missing lots for partial_close_lots"))
        else:
            try:
                int(lots)
            except (TypeError, ValueError):
                errors.append(ValidationError(f"{path}.lots", "lots must be an integer"))

    elif atype == "reduce_by_pct":
        close_tag = action.get("close_tag")
        if not close_tag:
            errors.append(ValidationError(f"{path}.close_tag", "Missing close_tag for reduce_by_pct"))
        pct = action.get("reduce_pct")
        if pct is None:
            errors.append(ValidationError(f"{path}.reduce_pct", "Missing reduce_pct for reduce_by_pct"))
        else:
            try:
                p = float(pct)
                if not (0 <= p <= 100):
                    errors.append(ValidationError(f"{path}.reduce_pct", "reduce_pct must be between 0 and 100"))
            except (TypeError, ValueError):
                errors.append(ValidationError(f"{path}.reduce_pct", "reduce_pct must be a number"))

    elif atype == "open_hedge":
        new_leg = action.get("new_leg")
        if not new_leg:
            errors.append(ValidationError(f"{path}.new_leg", "Missing new_leg for open_hedge"))
        elif not isinstance(new_leg, dict):
            errors.append(ValidationError(f"{path}.new_leg", "new_leg must be an object"))
        else:
            # Validate new_leg as a strike config (it should have side, option_type, strike_mode, etc.)
            _validate_leg_strike_config(new_leg, f"{path}.new_leg", errors, allow_dynamic_option_types=True)

    elif atype == "roll_to_next_expiry":
        leg = action.get("leg")
        if not leg:
            errors.append(ValidationError(f"{path}.leg", "Missing leg for roll_to_next_expiry"))
        target_expiry = action.get("target_expiry")
        if target_expiry and target_expiry not in {"weekly_next", "monthly_next", "weekly_current"}:
            errors.append(ValidationError(f"{path}.target_expiry", f"Invalid target_expiry '{target_expiry}'", "warning"))
        same_strike = action.get("same_strike")
        if same_strike and same_strike not in {"yes", "atm", "delta"}:
            errors.append(ValidationError(f"{path}.same_strike", f"Invalid same_strike '{same_strike}'", "warning"))

    elif atype == "convert_to_spread":
        wing = action.get("wing_leg")
        if not wing:
            errors.append(ValidationError(f"{path}.wing_leg", "Missing wing_leg for convert_to_spread"))
        elif not isinstance(wing, dict):
            errors.append(ValidationError(f"{path}.wing_leg", "wing_leg must be an object"))
        else:
            _validate_leg_strike_config(wing, f"{path}.wing_leg", errors, allow_dynamic_option_types=True)

    elif atype == "simple_close_open_new":
        swaps = action.get("leg_swaps")
        if not swaps:
            errors.append(ValidationError(f"{path}.leg_swaps", "Missing leg_swaps for simple_close_open_new"))
        elif not isinstance(swaps, list):
            errors.append(ValidationError(f"{path}.leg_swaps", "leg_swaps must be an array"))
        else:
            for i, swap in enumerate(swaps):
                _validate_leg_swap(swap, f"{path}.leg_swaps[{i}]", errors)


def _validate_leg_swap(swap: Dict, path: str, errors: List[ValidationError]):
    if not isinstance(swap, dict):
        errors.append(ValidationError(path, "swap must be an object"))
        return

    close_tag = swap.get("close_tag")
    if not close_tag:
        errors.append(ValidationError(f"{path}.close_tag", "Missing close_tag in swap"))
    elif not isinstance(close_tag, str):
        errors.append(ValidationError(f"{path}.close_tag", "close_tag must be a string"))

    new_leg = swap.get("new_leg")
    if not new_leg:
        errors.append(ValidationError(f"{path}.new_leg", "Missing new_leg in swap"))
    elif not isinstance(new_leg, dict):
        errors.append(ValidationError(f"{path}.new_leg", "new_leg must be an object"))
    else:
        _validate_leg_strike_config(new_leg, f"{path}.new_leg", errors, allow_dynamic_option_types=True)


def _validate_exit(exit_cfg: Dict, errors: List[ValidationError], prefix: str):
    if not isinstance(exit_cfg, dict):
        errors.append(ValidationError(prefix, "exit must be an object"))
        return

    # profit_target
    pt = exit_cfg.get("profit_target")
    if pt is not None:
        if not isinstance(pt, dict):
            errors.append(ValidationError(f"{prefix}.profit_target", "profit_target must be an object"))
        else:
            _validate_profit_target(pt, f"{prefix}.profit_target", errors)

    # stop_loss
    sl = exit_cfg.get("stop_loss")
    if sl is not None:
        if not isinstance(sl, dict):
            errors.append(ValidationError(f"{prefix}.stop_loss", "stop_loss must be an object"))
        else:
            _validate_stop_loss(sl, f"{prefix}.stop_loss", errors)

    # trailing
    tr = exit_cfg.get("trailing")
    if tr is not None:
        if not isinstance(tr, dict):
            errors.append(ValidationError(f"{prefix}.trailing", "trailing must be an object"))
        else:
            _validate_trailing(tr, f"{prefix}.trailing", errors)

    # profit_steps
    ps = exit_cfg.get("profit_steps")
    if ps is not None:
        if not isinstance(ps, dict):
            errors.append(ValidationError(f"{prefix}.profit_steps", "profit_steps must be an object"))
        else:
            _validate_profit_steps(ps, f"{prefix}.profit_steps", errors)

    # risk
    risk = exit_cfg.get("risk")
    if risk is not None:
        if not isinstance(risk, dict):
            errors.append(ValidationError(f"{prefix}.risk", "risk must be an object"))
        else:
            _validate_risk_subsection(risk, f"{prefix}.risk", errors)

    # time
    time_cfg = exit_cfg.get("time")
    if time_cfg is not None:
        if not isinstance(time_cfg, dict):
            errors.append(ValidationError(f"{prefix}.time", "time must be an object"))
        else:
            _validate_time_exit(time_cfg, f"{prefix}.time", errors)

    # combined_conditions
    combined = exit_cfg.get("combined_conditions")
    if combined is not None:
        if not isinstance(combined, dict):
            errors.append(ValidationError(f"{prefix}.combined_conditions", "combined_conditions must be an object"))
        else:
            # It should have operator and rules
            op = combined.get("operator")
            if op not in VALID_JOIN_OPERATORS:
                errors.append(ValidationError(f"{prefix}.combined_conditions.operator", f"Invalid operator '{op}'. Valid: {sorted(VALID_JOIN_OPERATORS)}"))
            rules = combined.get("rules", [])
            if not isinstance(rules, list):
                errors.append(ValidationError(f"{prefix}.combined_conditions.rules", "rules must be an array"))
            else:
                for i, rule in enumerate(rules):
                    _validate_condition(rule, f"{prefix}.combined_conditions.rules[{i}]", errors)

    # leg_rules
    leg_rules = exit_cfg.get("leg_rules")
    if leg_rules is not None:
        if not isinstance(leg_rules, list):
            errors.append(ValidationError(f"{prefix}.leg_rules", "leg_rules must be an array"))
        else:
            for i, rule in enumerate(leg_rules):
                _validate_leg_exit_rule(rule, f"{prefix}.leg_rules[{i}]", errors)


def _validate_profit_target(pt: Dict, path: str, errors: List[ValidationError]):
    # amount (optional)
    amt = pt.get("amount")
    if amt is not None:
        try:
            float(amt)
        except (TypeError, ValueError):
            errors.append(ValidationError(f"{path}.amount", "amount must be a number"))
    pct = pt.get("pct")
    if pct is not None:
        try:
            p = float(pct)
            if p < 0:
                errors.append(ValidationError(f"{path}.pct", "pct must be >= 0"))
        except (TypeError, ValueError):
            errors.append(ValidationError(f"{path}.pct", "pct must be a number"))
    action = pt.get("action")
    if action and action not in VALID_EXIT_ACTIONS:
        errors.append(ValidationError(f"{path}.action", f"Invalid action '{action}'. Valid: {sorted(VALID_EXIT_ACTIONS)}"))
    lots = pt.get("lots")
    if lots is not None:
        try:
            int(lots)
        except (TypeError, ValueError):
            errors.append(ValidationError(f"{path}.lots", "lots must be an integer"))


def _validate_stop_loss(sl: Dict, path: str, errors: List[ValidationError]):
    amt = sl.get("amount")
    if amt is not None:
        try:
            float(amt)
        except (TypeError, ValueError):
            errors.append(ValidationError(f"{path}.amount", "amount must be a number"))
    pct = sl.get("pct")
    if pct is not None:
        try:
            p = float(pct)
            if p < 0:
                errors.append(ValidationError(f"{path}.pct", "pct must be >= 0"))
        except (TypeError, ValueError):
            errors.append(ValidationError(f"{path}.pct", "pct must be a number"))
    action = sl.get("action")
    if action and action not in {"exit_all", "adjust", "partial_50"}:
        errors.append(ValidationError(f"{path}.action", f"Invalid action '{action}'", "warning"))
    allow_reentry = sl.get("allow_reentry")
    if allow_reentry is not None and not isinstance(allow_reentry, bool):
        errors.append(ValidationError(f"{path}.allow_reentry", "allow_reentry must be a boolean"))


def _validate_trailing(tr: Dict, path: str, errors: List[ValidationError]):
    for key in ("trail_amount", "lock_in_at", "trail_step", "step_trigger"):
        val = tr.get(key)
        if val is not None:
            try:
                float(val)
            except (TypeError, ValueError):
                errors.append(ValidationError(f"{path}.{key}", f"{key} must be a number"))


def _validate_profit_steps(ps: Dict, path: str, errors: List[ValidationError]):
    step_size = ps.get("step_size")
    if step_size is not None:
        try:
            float(step_size)
        except (TypeError, ValueError):
            errors.append(ValidationError(f"{path}.step_size", "step_size must be a number"))
    max_steps = ps.get("max_steps")
    if max_steps is not None:
        try:
            int(max_steps)
        except (TypeError, ValueError):
            errors.append(ValidationError(f"{path}.max_steps", "max_steps must be an integer"))
    action = ps.get("action")
    if action and action not in {"adj", "trail", "partial"}:
        errors.append(ValidationError(f"{path}.action", f"Invalid action '{action}'", "warning"))


def _validate_risk_subsection(risk: Dict, path: str, errors: List[ValidationError]):
    # max_loss_per_day
    loss = risk.get("max_loss_per_day")
    if loss is not None:
        try:
            float(loss)
        except (TypeError, ValueError):
            errors.append(ValidationError(f"{path}.max_loss_per_day", "must be a number"))
    # max_delta
    md = risk.get("max_delta")
    if md is not None:
        try:
            float(md)
        except (TypeError, ValueError):
            errors.append(ValidationError(f"{path}.max_delta", "must be a number"))
    # delta_breach_action
    dba = risk.get("delta_breach_action")
    if dba and dba not in {"hedge", "adjust", "alert"}:
        errors.append(ValidationError(f"{path}.delta_breach_action", f"Invalid action '{dba}'", "warning"))
    # max_iv, min_iv
    for key in ("max_iv", "min_iv"):
        val = risk.get(key)
        if val is not None:
            try:
                float(val)
            except (TypeError, ValueError):
                errors.append(ValidationError(f"{path}.{key}", "must be a number"))
    # max_lots
    ml = risk.get("max_lots")
    if ml is not None:
        try:
            int(ml)
        except (TypeError, ValueError):
            errors.append(ValidationError(f"{path}.max_lots", "must be an integer"))
    # breakeven_buffer
    buf = risk.get("breakeven_buffer")
    if buf is not None:
        try:
            float(buf)
        except (TypeError, ValueError):
            errors.append(ValidationError(f"{path}.breakeven_buffer", "must be a number"))


def _validate_time_exit(tm: Dict, path: str, errors: List[ValidationError]):
    exit_time = tm.get("strategy_exit_time")
    if exit_time and not _is_valid_time(exit_time):
        errors.append(ValidationError(f"{path}.strategy_exit_time", "Invalid time format. Expected HH:MM"))
    expiry_action = tm.get("expiry_day_action")
    if expiry_action and expiry_action not in {"none", "time", "open", "roll"}:
        errors.append(ValidationError(f"{path}.expiry_day_action", f"Invalid action '{expiry_action}'", "warning"))
    expiry_time = tm.get("expiry_day_time")
    if expiry_time and not _is_valid_time(expiry_time):
        errors.append(ValidationError(f"{path}.expiry_day_time", "Invalid time format. Expected HH:MM"))
    roll_target = tm.get("roll_target")
    if roll_target and roll_target not in {"weekly_next", "monthly_next", "custom"}:
        errors.append(ValidationError(f"{path}.roll_target", f"Invalid roll_target '{roll_target}'", "warning"))


def _validate_leg_exit_rule(rule: Dict, path: str, errors: List[ValidationError]):
    if not isinstance(rule, dict):
        errors.append(ValidationError(path, "leg_exit_rule must be an object"))
        return

    ref = rule.get("exit_leg_ref")
    if not ref:
        errors.append(ValidationError(f"{path}.exit_leg_ref", "Missing exit_leg_ref"))
    elif not isinstance(ref, str):
        errors.append(ValidationError(f"{path}.exit_leg_ref", "exit_leg_ref must be a string"))

    action = rule.get("action")
    if action and action not in {"close_leg", "close_all", "reduce_50pct", "partial_lots", "roll_next"}:
        errors.append(ValidationError(f"{path}.action", f"Invalid action '{action}'", "warning"))

    group = rule.get("group")
    if group is not None and not isinstance(group, str):
        errors.append(ValidationError(f"{path}.group", "group must be a string"))

    conds = rule.get("conditions", [])
    if not isinstance(conds, list):
        errors.append(ValidationError(f"{path}.conditions", "conditions must be an array"))
    else:
        for i, cond in enumerate(conds):
            _validate_condition(cond, f"{path}.conditions[{i}]", errors)


def _validate_rms(rms: Dict, errors: List[ValidationError], prefix: str):
    if not isinstance(rms, dict):
        errors.append(ValidationError(prefix, "rms must be an object"))
        return
    # rms may contain daily and position limits; we can validate numeric fields.
    daily = rms.get("daily")
    if daily and isinstance(daily, dict):
        loss_limit = daily.get("loss_limit")
        if loss_limit is not None:
            try:
                val = float(loss_limit)
                if val < 0:
                    errors.append(ValidationError(f"{prefix}.daily.loss_limit", "loss_limit must be >= 0"))
            except (TypeError, ValueError):
                errors.append(ValidationError(f"{prefix}.daily.loss_limit", "loss_limit must be a number"))
        max_trades = daily.get("max_trades")
        if max_trades is not None:
            try:
                int(max_trades)
            except (TypeError, ValueError):
                errors.append(ValidationError(f"{prefix}.daily.max_trades", "max_trades must be an integer"))

    position = rms.get("position")
    if position and isinstance(position, dict):
        max_lots = position.get("max_lots")
        if max_lots is not None:
            try:
                int(max_lots)
            except (TypeError, ValueError):
                errors.append(ValidationError(f"{prefix}.position.max_lots", "max_lots must be an integer"))


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================
def _is_valid_time(t: str) -> bool:
    """Check if string matches HH:MM format (24-hour)."""
    if not isinstance(t, str):
        return False
    parts = t.split(":")
    if len(parts) != 2:
        return False
    try:
        h = int(parts[0])
        m = int(parts[1])
        return 0 <= h <= 23 and 0 <= m <= 59
    except ValueError:
        return False


def _time_to_minutes(t: str) -> int:
    h, m = map(int, t.split(":"))
    return h * 60 + m


def _is_valid_parameter(param: str) -> bool:
    """Check if a parameter name is known or matches a valid pattern."""
    if param in KNOWN_PARAMETERS:
        return True
    if TAG_PARAM_PATTERN.match(param):
        return True
    if INDEX_PARAM_PATTERN.match(param):
        return True
    return False
