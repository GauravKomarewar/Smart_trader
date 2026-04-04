from typing import Any, Optional


def _fmt_num(v: Any) -> str:
    try:
        return f"{float(v):.2f}"
    except Exception:
        return "—"


def build_strategy_report(strategy: Any, market_adapter: Optional[Any] = None) -> Optional[str]:
    """
    Build a Telegram-friendly strategy status report.
    """
    state = getattr(strategy, "state", None)
    if state is None or not getattr(state, "active", True):
        return None

    ce_leg = getattr(state, "ce_leg", None)
    pe_leg = getattr(state, "pe_leg", None)

    spot = "—"
    if market_adapter is not None:
        try:
            snap = market_adapter.get_market_snapshot()
            if isinstance(snap, dict) and snap.get("spot") is not None:
                spot = str(snap.get("spot"))
        except Exception:
            spot = "—"

    unrealized = state.total_unrealized_pnl() if hasattr(state, "total_unrealized_pnl") else 0.0
    realized = getattr(state, "realized_pnl", 0.0)
    net_delta = state.total_delta() if hasattr(state, "total_delta") else 0.0

    lines = [
        "*DELTA NEUTRAL* `LIVE STATUS`",
        f"Spot: `{spot}`",
        "—",
        "*CALL LEG*",
        f"Symbol: `{getattr(ce_leg, 'symbol', '—')}`",
        f"Delta: `{_fmt_num(getattr(ce_leg, 'delta', 0.0))}`",
        "*PUT LEG*",
        f"Symbol: `{getattr(pe_leg, 'symbol', '—')}`",
        f"Delta: `{_fmt_num(getattr(pe_leg, 'delta', 0.0))}`",
        "—",
        f"Net Delta: `{_fmt_num(net_delta)}`",
        f"Unrealized: `{_fmt_num(unrealized)}`",
        f"Realized: `{_fmt_num(realized)}`",
    ]

    if getattr(state, "adjustment_phase", None):
        lines.append(f"Adjustment In Progress: `{state.adjustment_phase}`")
    else:
        lines.append("Adjustment Rules: Profit Target / Delta rebalance")
        lines.append("Profit Target: Active")

    return "\n".join(lines)

