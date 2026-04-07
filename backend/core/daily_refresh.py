from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Optional

IST = timezone(timedelta(hours=5, minutes=30))
DAILY_REFRESH_HOUR = 8
DAILY_REFRESH_MINUTE = 45


def now_ist() -> datetime:
    return datetime.now(IST)


def current_refresh_cycle_start(now: Optional[datetime] = None) -> datetime:
    current = _coerce_ist(now)
    target = current.replace(
        hour=DAILY_REFRESH_HOUR,
        minute=DAILY_REFRESH_MINUTE,
        second=0,
        microsecond=0,
    )
    if current < target:
        target -= timedelta(days=1)
    return target


def next_refresh_time(now: Optional[datetime] = None) -> datetime:
    current = _coerce_ist(now)
    target = current.replace(
        hour=DAILY_REFRESH_HOUR,
        minute=DAILY_REFRESH_MINUTE,
        second=0,
        microsecond=0,
    )
    if current >= target:
        target += timedelta(days=1)
    return target


def parse_refresh_timestamp(meta: Mapping[str, Any] | None) -> Optional[datetime]:
    if not meta:
        return None

    refreshed_at = meta.get("refreshed_at")
    if refreshed_at:
        try:
            parsed = datetime.fromisoformat(str(refreshed_at).replace("Z", "+00:00"))
            return _coerce_ist(parsed)
        except ValueError:
            pass

    legacy_date = meta.get("date")
    if legacy_date:
        for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
            try:
                parsed = datetime.strptime(str(legacy_date), fmt)
                return parsed.replace(
                    hour=DAILY_REFRESH_HOUR,
                    minute=DAILY_REFRESH_MINUTE,
                    second=0,
                    microsecond=0,
                    tzinfo=IST,
                )
            except ValueError:
                continue

    return None


def read_refresh_metadata(meta_file: Path) -> dict[str, Any]:
    if not meta_file.exists():
        return {}
    try:
        return json.loads(meta_file.read_text())
    except Exception:
        return {}


def is_daily_refresh_due(
    meta_file: Path,
    last_refresh: Optional[datetime] = None,
    now: Optional[datetime] = None,
) -> tuple[bool, Optional[datetime]]:
    cycle_start = current_refresh_cycle_start(now)
    candidate = _coerce_ist(last_refresh) if last_refresh else None
    if candidate and candidate >= cycle_start:
        return False, candidate

    meta_ts = parse_refresh_timestamp(read_refresh_metadata(meta_file))
    if meta_ts and meta_ts >= cycle_start:
        return False, meta_ts

    return True, meta_ts


def build_refresh_metadata(version: str, **extra: Any) -> dict[str, Any]:
    refreshed_at = now_ist()
    payload: dict[str, Any] = {
        "date": refreshed_at.strftime("%Y-%m-%d"),
        "refreshed_at": refreshed_at.isoformat(),
        "version": version,
    }
    payload.update(extra)
    return payload


def _coerce_ist(value: Optional[datetime]) -> datetime:
    if value is None:
        return now_ist()
    if value.tzinfo is None:
        return value.replace(tzinfo=IST)
    return value.astimezone(IST)
