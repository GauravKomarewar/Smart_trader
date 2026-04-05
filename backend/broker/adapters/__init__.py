"""Broker Adapters — common interface for all supported brokers."""
from broker.adapters.base import BrokerAdapter, Funds, Holding, Order, Position, Trade
from broker.adapters.shoonya_adapter import ShoonyaAdapter
from broker.adapters.fyers_adapter import FyersAdapter
from broker.adapters.paper_adapter import PaperAdapter
from broker.adapters.angelone_adapter import AngelOneAdapter
from broker.adapters.dhan_adapter import DhanAdapter
from broker.adapters.groww_adapter import GrowwAdapter
from broker.adapters.upstox_adapter import UpstoxAdapter
from broker.adapters.kite_adapter import KiteAdapter

__all__ = [
    "BrokerAdapter",
    "Position", "Order", "Holding", "Funds", "Trade",
    "ShoonyaAdapter",
    "FyersAdapter",
    "PaperAdapter",
    "AngelOneAdapter",
    "DhanAdapter",
    "GrowwAdapter",
    "UpstoxAdapter",
    "KiteAdapter",
]
