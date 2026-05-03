from __future__ import annotations

import os

from .massive import MassiveProvider
from .provider import MarketDataProvider, WatchlistSource
from .simulator import SimulatorProvider


def build_provider(watchlist: WatchlistSource) -> MarketDataProvider:
    """Select the market data provider based on environment variables.

    If MASSIVE_API_KEY is set and non-empty, uses MassiveProvider (Polygon.io).
    Otherwise, falls back to the built-in GBM simulator.
    """
    key = (os.environ.get("MASSIVE_API_KEY") or "").strip()
    if key:
        return MassiveProvider(api_key=key, watchlist=watchlist)
    return SimulatorProvider(watchlist=watchlist)
