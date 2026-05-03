from __future__ import annotations

from ..config import MASSIVE_API_KEY
from .massive import MassiveProvider
from .provider import MarketDataProvider, WatchlistSource
from .simulator import SimulatorProvider


def build_provider(watchlist: WatchlistSource) -> MarketDataProvider:
    """Select the market data provider based on configuration.

    If MASSIVE_API_KEY is set and non-empty, uses MassiveProvider (Polygon.io).
    Otherwise, falls back to the built-in GBM simulator.
    """
    if MASSIVE_API_KEY:
        return MassiveProvider(api_key=MASSIVE_API_KEY, watchlist=watchlist)
    return SimulatorProvider(watchlist=watchlist)
