from __future__ import annotations

import pytest

from app.market_data.cache import PriceCache


@pytest.mark.asyncio
async def test_first_tick_has_no_prev_price():
    cache = PriceCache()
    tick = await cache.update("AAPL", 190.0)
    assert tick.ticker == "AAPL"
    assert tick.price == 190.0
    assert tick.prev_price is None
    assert tick.direction == "flat"


@pytest.mark.asyncio
async def test_direction_up():
    cache = PriceCache()
    await cache.update("AAPL", 190.0)
    tick = await cache.update("AAPL", 191.0)
    assert tick.direction == "up"
    assert tick.prev_price == 190.0


@pytest.mark.asyncio
async def test_direction_down():
    cache = PriceCache()
    await cache.update("AAPL", 190.0)
    tick = await cache.update("AAPL", 189.0)
    assert tick.direction == "down"
    assert tick.prev_price == 190.0


@pytest.mark.asyncio
async def test_direction_flat_when_price_unchanged():
    cache = PriceCache()
    await cache.update("AAPL", 190.0)
    tick = await cache.update("AAPL", 190.0)
    assert tick.direction == "flat"
    assert tick.prev_price == 190.0


@pytest.mark.asyncio
async def test_get_returns_none_for_unknown_ticker():
    cache = PriceCache()
    assert await cache.get("UNKNOWN") is None


@pytest.mark.asyncio
async def test_get_returns_latest_tick():
    cache = PriceCache()
    await cache.update("MSFT", 400.0)
    tick = await cache.update("MSFT", 401.0)
    stored = await cache.get("MSFT")
    assert stored == tick


@pytest.mark.asyncio
async def test_snapshot_returns_all_tickers():
    cache = PriceCache()
    await cache.update("AAPL", 190.0)
    await cache.update("GOOGL", 175.0)
    snap = await cache.snapshot()
    assert set(snap.keys()) == {"AAPL", "GOOGL"}


@pytest.mark.asyncio
async def test_snapshot_is_independent_copy():
    cache = PriceCache()
    await cache.update("AAPL", 190.0)
    snap = await cache.snapshot()
    snap["AAPL"] = None  # mutate the copy
    # Original cache should be unchanged
    assert (await cache.get("AAPL")).price == 190.0


@pytest.mark.asyncio
async def test_multiple_tickers_independent():
    cache = PriceCache()
    await cache.update("AAPL", 190.0)
    await cache.update("TSLA", 250.0)
    await cache.update("AAPL", 192.0)
    aapl = await cache.get("AAPL")
    tsla = await cache.get("TSLA")
    assert aapl.price == 192.0
    assert aapl.prev_price == 190.0
    assert tsla.price == 250.0
    assert tsla.prev_price is None
