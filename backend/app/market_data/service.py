from __future__ import annotations

import asyncio
import logging

from .broadcaster import PriceBroadcaster
from .cache import PriceCache
from .provider import MarketDataProvider

log = logging.getLogger(__name__)

_BACKOFF_MAX = 30.0


class MarketDataService:
    """Composition root: owns the cache, broadcaster, provider, and producer task.

    Attach to ``app.state.market_data`` via the FastAPI lifespan.
    """

    def __init__(self, provider: MarketDataProvider) -> None:
        self.cache = PriceCache()
        self.broadcaster = PriceBroadcaster()
        self._provider = provider
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        await self._provider.__aenter__()
        self._task = asyncio.create_task(
            self._supervised_run(), name="market-data-producer"
        )

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self._provider.__aexit__(None, None, None)

    async def _run(self) -> None:
        async for ticker, price in self._provider.stream():
            tick = await self.cache.update(ticker, price)
            await self.broadcaster.publish(tick)

    async def _supervised_run(self) -> None:
        """Restart the producer on unexpected errors with exponential backoff."""
        backoff = 1.0
        while True:
            try:
                await self._run()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception(
                    "market data producer crashed; restarting in %.1fs", backoff
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _BACKOFF_MAX)
            else:
                # stream() exited cleanly (shouldn't normally happen)
                break
