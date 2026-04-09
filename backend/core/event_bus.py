"""
Smart Trader — Event Bus
=========================
Lightweight in-process event bus for instant notifications.

Used to wake WebSocket feed loop immediately when orders or positions change,
instead of waiting for the next 2-second manager poll cycle.

Events:
  • order_update   — order placed / filled / cancelled / rejected
  • position_update — position opened / closed / P&L change
  • force_refresh   — force immediate dashboard push

Usage:
    from core.event_bus import event_bus
    event_bus.emit("order_update", {"user_id": "...", "order_id": "...", ...})

    # In WS feed loop:
    event = await event_bus.wait(user_id, timeout=1.0)
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from typing import Any, Dict, Optional

logger = logging.getLogger("smart_trader.event_bus")


class EventBus:
    """Simple user-scoped async event bus for instant WS push."""

    def __init__(self) -> None:
        # user_id → asyncio.Event (signal that new data is available)
        self._user_events: Dict[str, asyncio.Event] = {}
        # user_id → list of pending event payloads
        self._pending: Dict[str, list] = defaultdict(list)
        # Keep max 50 events per user to prevent memory leak
        self._MAX_PENDING = 50

    def _get_event(self, user_id: str) -> asyncio.Event:
        if user_id not in self._user_events:
            self._user_events[user_id] = asyncio.Event()
        return self._user_events[user_id]

    def emit(self, event_type: str, data: Dict[str, Any]) -> None:
        """
        Emit an event. Can be called from any thread.
        data must include 'user_id'.
        """
        user_id = data.get("user_id", "")
        if not user_id:
            return

        payload = {"type": event_type, "data": data, "ts": int(time.time())}
        pending = self._pending[user_id]
        if len(pending) >= self._MAX_PENDING:
            pending.pop(0)
        pending.append(payload)

        # Signal the asyncio event (thread-safe)
        event = self._get_event(user_id)
        try:
            # If running from a non-asyncio thread, need to find the loop
            loop = asyncio.get_running_loop()
            event.set()
        except RuntimeError:
            # Called from sync thread — set directly (Event.set is thread-safe)
            event.set()

    async def wait(self, user_id: str, timeout: float = 1.0) -> bool:
        """
        Wait for an event for this user. Returns True if an event arrived.
        Resets the signal after wake.
        """
        event = self._get_event(user_id)
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            event.clear()
            return True
        except asyncio.TimeoutError:
            return False

    def drain(self, user_id: str) -> list:
        """Drain all pending event payloads for a user."""
        events = list(self._pending.get(user_id, []))
        self._pending[user_id] = []
        return events

    def cleanup(self, user_id: str) -> None:
        """Clean up resources for a disconnected user."""
        self._user_events.pop(user_id, None)
        self._pending.pop(user_id, None)


# Singleton
event_bus = EventBus()
