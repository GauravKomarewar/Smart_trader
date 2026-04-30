from __future__ import annotations

import os
import sys
from dataclasses import dataclass

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from broker.account_risk import AccountRiskConfig, AccountRiskManager


@dataclass
class _DummySession:
    positions: list
    limits: dict | None = None

    def get_positions(self):
        return self.positions

    def get_limits(self):
        return self.limits or {}


@pytest.fixture
def risk_manager(tmp_path):
    cfg = AccountRiskConfig(
        max_daily_loss=-2500.0,
        state_dir=str(tmp_path / "state"),
        history_dir=str(tmp_path / "history"),
    )
    return AccountRiskManager(
        user_id="u1",
        config_id="cfg1",
        broker_id="shoonya",
        client_id="FA14667",
        config=cfg,
    )


def test_heartbeat_does_not_double_count_when_unrealized_zero(risk_manager):
    # Broker payload shape that previously over-counted:
    # rpnl present, unrealized is 0, total pnl also present.
    # Old logic treated 0 as missing and added rpnl + pnl (double count).
    sess = _DummySession(
        positions=[
            {
                "symbol": "NIFTY05MAY26C24200",
                "rpnl": -1288.75,
                "urmtom": 0,
                "pnl": -1288.75,
                "netqty": -65,
            }
        ]
    )

    hb = risk_manager.heartbeat(sess)

    assert hb["pnl"] == pytest.approx(-1288.75)
    assert risk_manager.get_status()["daily_loss_hit"] is False


def test_heartbeat_uses_sum_when_realized_and_unrealized_present(risk_manager):
    sess = _DummySession(
        positions=[
            {
                "symbol": "NIFTY05MAY26P23700",
                "realised_pnl": -900.0,
                "unrealised_pnl": -450.0,
                "pnl": -1350.0,
                "net_qty": -50,
            }
        ]
    )

    hb = risk_manager.heartbeat(sess)

    assert hb["pnl"] == pytest.approx(-1350.0)
    assert risk_manager.get_status()["daily_loss_hit"] is False


def test_heartbeat_uses_total_pnl_if_components_missing(risk_manager):
    sess = _DummySession(
        positions=[
            {
                "symbol": "NIFTY05MAY26P23700",
                "pnl": -1700.0,
                "net_qty": -25,
            }
        ]
    )

    hb = risk_manager.heartbeat(sess)

    assert hb["pnl"] == pytest.approx(-1700.0)
    assert risk_manager.get_status()["daily_loss_hit"] is False
