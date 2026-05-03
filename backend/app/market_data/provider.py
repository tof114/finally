from __future__ import annotations

from abc import ABC, abstractmethod
from typing import AsyncIterator, Protocol


class WatchlistSource(Protocol):
    """Anything that can tell us which tickers we currently care about."""

    async def current_tickers(self) -> list[str]: ...


class MarketDataProvider(ABC):
    """Common interface for the simulator and Massive (Polygon.io) client.

    Implementations must be cancellation-safe: when the consumer stops
    iterating, background work (HTTP sessions, tasks) must be cleaned up
    via __aexit__.
    """

    @abstractmethod
    async def __aenter__(self) -> "MarketDataProvider": ...

    @abstractmethod
    async def __aexit__(self, exc_type, exc, tb) -> None: ...

    @abstractmethod
    def stream(self) -> AsyncIterator[tuple[str, float]]:
        """Async generator yielding (ticker, price) pairs forever."""
