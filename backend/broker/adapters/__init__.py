"""Broker Adapters — common interface for all supported brokers."""
from broker.adapters.base import BrokerAdapter, Funds, Holding, Order, Position, Trade
from broker.adapters.shoonya_adapter import ShoonyaAdapter
from broker.adapters.fyers_adapter import FyersAdapter
from broker.adapters.paper_adapter import PaperAdapter

__all__ = [
    "BrokerAdapter",
    "Position", "Order", "Holding", "Funds", "Trade",
    "ShoonyaAdapter",
    "FyersAdapter",
    "PaperAdapter",
]
