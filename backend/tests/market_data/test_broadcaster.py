from __future__ import annotations

import asyncio

import pytest

from app.market_data.broadcaster import PriceBroadcaster, _QUEUE_SIZE
from app.market_data.types import Tick, utcnow


def _make_tick(ticker: str = "AAPL", price: float = 190.0) -> Tick:
    return Tick(ticker=ticker, price=price, prev_price=None, timestamp=utcnow(), direction="flat")


@pytest.mark.asyncio
async def test_publish_reaches_subscriber():
    bc = PriceBroadcaster()
    tick = _make_tick()
    async with bc.subscribe() as q:
        await bc.publish(tick)
        received = await asyncio.wait_for(q.get(), timeout=1.0)
    assert received == tick


@pytest.mark.asyncio
async def test_fan_out_to_multiple_subscribers():
    bc = PriceBroadcaster()
    tick = _make_tick()
    async with bc.subscribe() as q1, bc.subscribe() as q2:
        await bc.publish(tick)
        r1 = await asyncio.wait_for(q1.get(), timeout=1.0)
        r2 = await asyncio.wait_for(q2.get(), timeout=1.0)
    assert r1 == tick
    assert r2 == tick


@pytest.mark.asyncio
async def test_no_subscribers_publish_is_noop():
    bc = PriceBroadcaster()
    # Should complete without error even with no subscribers.
    await bc.publish(_make_tick())


@pytest.mark.asyncio
async def test_subscriber_removed_after_context_exit():
    bc = PriceBroadcaster()
    async with bc.subscribe():
        assert bc.subscriber_count == 1
    assert bc.subscriber_count == 0


@pytest.mark.asyncio
async def test_drop_oldest_when_queue_full():
    """When a subscriber's queue is full, the oldest tick is dropped."""
    bc = PriceBroadcaster()
    async with bc.subscribe() as q:
        # Fill the queue to capacity with distinct ticks.
        ticks = [_make_tick(price=float(i)) for i in range(_QUEUE_SIZE)]
        for t in ticks:
            await bc.publish(t)

        # Now publish one more — should evict the oldest and insert newest.
        overflow_tick = _make_tick(price=9999.0)
        await bc.publish(overflow_tick)

        # Drain the queue.
        received = []
        while not q.empty():
            received.append(q.get_nowait())

    # The queue should be full again (or close) and the overflow tick present.
    prices = [t.price for t in received]
    assert 9999.0 in prices
    # The very first tick (price=0.0) should have been dropped.
    assert 0.0 not in prices


@pytest.mark.asyncio
async def test_publish_multiple_tickers():
    bc = PriceBroadcaster()
    ticks = [_make_tick("AAPL", 190.0), _make_tick("MSFT", 420.0)]
    async with bc.subscribe() as q:
        for t in ticks:
            await bc.publish(t)
        received = [await asyncio.wait_for(q.get(), timeout=1.0) for _ in ticks]
    assert {t.ticker for t in received} == {"AAPL", "MSFT"}
