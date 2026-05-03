from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

Direction = Literal["up", "down", "flat"]


@dataclass(frozen=True, slots=True)
class Tick:
    ticker: str
    price: float
    prev_price: float | None  # None on the very first tick for this ticker
    timestamp: datetime       # always tz-aware UTC
    direction: Direction      # derived from price vs prev_price

    def to_sse_dict(self) -> dict:
        return {
            "t": self.ticker,
            "p": round(self.price, 4),
            "pp": round(self.prev_price, 4) if self.prev_price is not None else None,
            "ts": self.timestamp.isoformat(),
            "d": self.direction,
        }


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
