from __future__ import annotations

import asyncio
import random

import pytest

from app.market_data.service import MarketDataService
from app.market_data.simulator import SimulatorProvider

from .conftest import FakeWatchlist


@pytest.mark.asyncio
async def test_service_populates_cache():
    wl = FakeWatchlist(["AAPL"])
    provider = SimulatorProvider(wl, rng=random.Random(0))
    service = MarketDataService(provider)
    await service.start()

    # Give the producer one cycle.
    await asyncio.sleep(0.6)
    tick = await service.cache.get("AAPL")
    assert tick is not None
    assert tick.price > 0

    await service.stop()


@pytest.mark.asyncio
async def test_service_broadcasts_ticks():
    wl = FakeWatchlist(["AAPL"])
    provider = SimulatorProvider(wl, rng=random.Random(1))
    service = MarketDataService(provider)
    await service.start()

    async with service.broadcaster.subscribe() as q:
        tick = await asyncio.wait_for(q.get(), timeout=2.0)
    assert tick.ticker == "AAPL"

    await service.stop()


@pytest.mark.asyncio
async def test_service_stop_is_idempotent():
    wl = FakeWatchlist(["AAPL"])
    provider = SimulatorProvider(wl, rng=random.Random(2))
    service = MarketDataService(provider)
    await service.start()
    await service.stop()
    await service.stop()  # second stop should not raise
