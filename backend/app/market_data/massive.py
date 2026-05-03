from __future__ import annotations

import asyncio
import logging
from typing import AsyncIterator

import httpx

from ..config import MASSIVE_TIER
from .provider import MarketDataProvider, WatchlistSource

log = logging.getLogger(__name__)

BASE_URL = (
    "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
)

# Conservative defaults; user can override via MASSIVE_TIER env var.
TIER_INTERVALS: dict[str, float] = {
    "free":      15.0,  # 4 req/min — headroom under the 5/min free cap
    "starter":    5.0,
    "developer":  2.0,
    "advanced":   1.0,
}


class MassiveProvider(MarketDataProvider):
    """Polygon.io REST snapshot client.

    One HTTP call per poll cycle (all tickers in one request) to stay
    within the free-tier rate limit. Errors are logged and swallowed so
    the cache keeps its last good values.
    """

    def __init__(
        self,
        api_key: str,
        watchlist: WatchlistSource,
        *,
        poll_interval: float | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._api_key = api_key
        self._watchlist = watchlist
        self._interval = poll_interval or TIER_INTERVALS[MASSIVE_TIER]
        self._client = client
        self._owns_client = client is None

    async def __aenter__(self) -> "MassiveProvider":
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=10.0)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _poll_once(self, tickers: list[str]) -> list[tuple[str, float]]:
        if not tickers:
            return []
        assert self._client is not None, "must be used as async context manager"
        params = {
            "tickers": ",".join(sorted(set(t.upper() for t in tickers))),
            "apiKey": self._api_key,
        }
        try:
            resp = await self._client.get(BASE_URL, params=params)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            log.warning("massive poll failed: %s", exc)
            return []
        return self._parse(resp.json())

    @staticmethod
    def _parse(payload: dict) -> list[tuple[str, float]]:
        """Extract (ticker, price) pairs from a Polygon snapshot response.

        Price preference order: lastTrade.p → day.c → prevDay.c
        Entries with price <= 0 are dropped.
        """
        out: list[tuple[str, float]] = []
        for entry in payload.get("tickers", []):
            sym: str | None = entry.get("ticker")
            if not sym:
                continue
            # The `or` chain treats 0 as missing (Python falsiness). That is
            # intentional: a zero last-trade is a stale/garbage value and we
            # prefer the day close. The post-filter below also drops <= 0.
            price = (
                (entry.get("lastTrade") or {}).get("p")
                or (entry.get("day") or {}).get("c")
                or (entry.get("prevDay") or {}).get("c")
            )
            if price is None or price <= 0:
                continue
            out.append((sym, float(price)))
        return out

    async def stream(self) -> AsyncIterator[tuple[str, float]]:
        while True:
            tickers = await self._watchlist.current_tickers()
            for pair in await self._poll_once(tickers):
                yield pair
            await asyncio.sleep(self._interval)
