from __future__ import annotations

import asyncio

from .types import Direction, Tick, utcnow


class PriceCache:
    """Thread-safe in-memory map of ticker -> latest Tick.

    Single source of truth for current prices; read by SSE routes and
    portfolio valuation. No history — sparklines are accumulated client-side.
    """

    def __init__(self) -> None:
        self._data: dict[str, Tick] = {}
        self._lock = asyncio.Lock()

    async def update(self, ticker: str, price: float) -> Tick:
        """Insert or replace the latest tick. Returns the new Tick."""
        async with self._lock:
            prev = self._data.get(ticker)
            prev_price = prev.price if prev is not None else None
            direction: Direction
            if prev_price is None or price == prev_price:
                direction = "flat"
            elif price > prev_price:
                direction = "up"
            else:
                direction = "down"
            tick = Tick(
                ticker=ticker,
                price=price,
                prev_price=prev_price,
                timestamp=utcnow(),
                direction=direction,
            )
            self._data[ticker] = tick
            return tick

    async def get(self, ticker: str) -> Tick | None:
        async with self._lock:
            return self._data.get(ticker)

    async def snapshot(self) -> dict[str, Tick]:
        """Return a copy of the current cache for the SSE snapshot burst."""
        async with self._lock:
            return dict(self._data)
