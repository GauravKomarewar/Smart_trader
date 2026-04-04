"""
Smart Trader — Telegram Notifier
=================================

Ported from shoonya_platform/notifications/telegram.py.
All shoonya_platform-specific imports removed.
Category-based preferences, HTTP session kill/restore, JSONL local logging.
"""

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Literal, Optional

import requests

logger = logging.getLogger("smart_trader.telegram")


def _sanitize(text: str) -> str:
    """Remove HTML-unsafe chars and strip excessive whitespace."""
    if not text:
        return ""
    # Replace & < > that could break HTML parse_mode
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return text.strip()


class TelegramNotifier:
    """Send Telegram notifications with per-category preferences."""

    _CATEGORY_MAP = {
        "send_startup": "system",
        "send_ready":   "system",
        "send_login":   "system",
        "send_error":   "system",
        "send_test":    "system",
        "send_alert":   "strategy",
        "send_order":   "strategy",
        "send_daily":   "reports",
        "send_status":  "reports",
        "send_heartbeat": "reports",
    }

    def __init__(self, bot_token: str, chat_id: str, log_dir: str = "logs"):
        self.bot_token  = bot_token
        self.chat_id    = chat_id
        self.base_url   = f"https://api.telegram.org/bot{bot_token}"
        self.session    = requests.Session()
        self.is_connected = False
        self._log_path  = Path(log_dir) / "telegram_messages.jsonl"
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        self._prefs = {"all": True, "system": True, "strategy": True, "reports": True}

        try:
            self.test_connection()
        except Exception as e:
            logger.warning("Telegram initial connection test failed: %s", e)

    # -- Prefs ----------------------------------------------------------------

    def set_preferences(self, prefs: dict) -> None:
        old_all = self._prefs.get("all", True)
        for k in ("all", "system", "strategy", "reports"):
            if k in prefs:
                self._prefs[k] = bool(prefs[k])
        new_all = self._prefs.get("all", True)
        if old_all and not new_all:
            self._kill_session()
        elif not old_all and new_all:
            self._restore_session()

    def _kill_session(self) -> None:
        if self.session is not None:
            try:
                self.session.close()
            except Exception:
                pass
            self.session = None
            logger.warning("Telegram HTTP session destroyed — notifications OFF")

    def _restore_session(self) -> None:
        if self.session is None:
            self.session = requests.Session()
            logger.info("Telegram HTTP session restored — notifications ON")

    def is_category_enabled(self, category: str) -> bool:
        if not self._prefs.get("all", True):
            return False
        return self._prefs.get(category, True)

    def _should_send(self, method_name: str, *, category: str = "") -> bool:
        if not self._prefs.get("all", True):
            return False
        if category in ("system", "strategy", "reports"):
            return self._prefs.get(category, True)
        for prefix, cat in self._CATEGORY_MAP.items():
            if method_name.startswith(prefix):
                return self._prefs.get(cat, True)
        return False

    # -- Connection -----------------------------------------------------------

    def test_connection(self) -> bool:
        try:
            session = self.session or requests.Session()
            resp = session.get(f"{self.base_url}/getMe", timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("ok"):
                    logger.info("Telegram connected: %s", data["result"]["first_name"])
                    self.is_connected = True
                    return True
            self.is_connected = False
            return False
        except Exception as e:
            logger.error("Telegram connection test failed: %s", e)
            self.is_connected = False
            return False

    # -- Core send ------------------------------------------------------------

    def send_message(
        self,
        message: str,
        parse_mode: Literal["HTML", "MarkdownV2"] = "HTML",
        _skip_log: bool = False,
        _force: bool = False,
    ) -> bool:
        if not _force and not self._prefs.get("all", True):
            return True

        if self.session is None:
            if _force:
                self.session = requests.Session()
            else:
                return True

        if not _skip_log:
            self._log_to_file(message)

        if not self.is_connected:
            if not self.test_connection():
                return False

        try:
            safe = _sanitize(message)
            resp = self.session.post(
                f"{self.base_url}/sendMessage",
                json={"chat_id": self.chat_id, "text": safe, "parse_mode": parse_mode},
                timeout=10,
            )
            if resp.status_code == 200 and resp.json().get("ok"):
                return True
            logger.warning("Telegram send failed: %s %s", resp.status_code, resp.text[:200])
            return False
        except Exception as e:
            logger.error("send_message error: %s", e)
            self.is_connected = False
            return False

    def _log_to_file(self, message: str):
        try:
            entry = {"ts": datetime.utcnow().isoformat(), "msg": message}
            with open(self._log_path, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception:
            pass

    # -- Typed senders --------------------------------------------------------

    def send_startup(self, client_id: str, version: str = ""):
        if not self._should_send("send_startup"):
            return
        self.send_message(
            f"<b>Bot Started</b>\nClient: <code>{client_id}</code>\nVersion: {version}",
            _skip_log=True)

    def send_ready(self, client_id: str):
        if not self._should_send("send_ready"):
            return
        self.send_message(f"<b>Bot Ready</b> | <code>{client_id}</code>", _skip_log=True)

    def send_error(self, error: str, context: str = ""):
        self._log_to_file(f"ERROR: {error}")
        if not self._should_send("send_error"):
            return
        msg = f"<b>ERROR</b>\n{_sanitize(error)}"
        if context:
            msg += f"\n<i>{_sanitize(context)}</i>"
        self.send_message(msg, _skip_log=True)

    def send_order(self, symbol: str, side: str, qty: int, price: float,
                   strategy: str = "", status: str = "FILLED"):
        self._log_to_file(f"ORDER {side} {symbol} x{qty} @ {price} [{strategy}] {status}")
        if not self._should_send("send_order"):
            return
        icon = "🟢" if side == "BUY" else "🔴"
        self.send_message(
            f"{icon} <b>{status}</b> | {side} {qty}x <code>{symbol}</code>"
            f"\nPrice: {price:.2f}"
            f"{chr(10) + 'Strategy: ' + strategy if strategy else ''}",
            _skip_log=True)

    def send_alert(self, text: str, title: str = "Alert"):
        self._log_to_file(f"ALERT {title}: {text}")
        if not self._should_send("send_alert"):
            return
        self.send_message(f"<b>{_sanitize(title)}</b>\n{_sanitize(text)}", _skip_log=True)

    def send_daily(self, pnl: float, trades: int, date_str: str = ""):
        self._log_to_file(f"DAILY pnl={pnl} trades={trades}")
        if not self._should_send("send_daily"):
            return
        icon = "🟢" if pnl >= 0 else "🔴"
        self.send_message(
            f"{icon} <b>Daily Report</b>{' | ' + date_str if date_str else ''}"
            f"\nPnL: ₹{pnl:.2f} | Trades: {trades}",
            _skip_log=True)

    def send_heartbeat(self, client_id: str, pnl: float, open_positions: int):
        self._log_to_file(f"HB client={client_id} pnl={pnl} pos={open_positions}")
        if not self._should_send("send_heartbeat"):
            return
        icon = "🟢" if pnl >= 0 else "🔴"
        self.send_message(
            f"{icon} <b>Heartbeat</b> | <code>{client_id}</code>"
            f"\nPnL: ₹{pnl:.2f} | Open: {open_positions}",
            _skip_log=True)

    def send_status(self, status_text: str):
        self._log_to_file(f"STATUS: {status_text}")
        if not self._should_send("send_status"):
            return
        self.send_message(f"<b>Status</b>\n{_sanitize(status_text)}", _skip_log=True)

    def get_recent_messages(self, n: int = 50) -> list:
        """Read last N messages from the JSONL log file."""
        if not self._log_path.exists():
            return []
        try:
            lines = self._log_path.read_text().splitlines()
            return [json.loads(l) for l in lines[-n:] if l.strip()]
        except Exception:
            return []
