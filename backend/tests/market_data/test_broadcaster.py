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
        # Fill the queue to capacity with distinct ticks (prices 0.0 .. N-1).
        ticks = [_make_tick(price=float(i)) for i in range(_QUEUE_SIZE)]
        for t in ticks:
            await bc.publish(t)

        # Publish one more — should evict the oldest (price=0.0) and append.
        overflow_tick = _make_tick(price=9999.0)
        await bc.publish(overflow_tick)

        received = []
        while not q.empty():
            received.append(q.get_nowait())

    prices = [t.price for t in received]
    # FIFO ordering preserved: head is now the second-oldest, tail is overflow.
    assert prices[0] == 1.0, "drop should evict the oldest, not an arbitrary item"
    assert prices[-1] == 9999.0, "overflow tick should be at the tail"
    assert 0.0 not in prices, "originally-oldest tick should be evicted"
    assert len(prices) == _QUEUE_SIZE


@pytest.mark.asyncio
async def test_publish_multiple_tickers():
    bc = PriceBroadcaster()
    ticks = [_make_tick("AAPL", 190.0), _make_tick("MSFT", 420.0)]
    async with bc.subscribe() as q:
        for t in ticks:
            await bc.publish(t)
        received = [await asyncio.wait_for(q.get(), timeout=1.0) for _ in ticks]
    assert {t.ticker for t in received} == {"AAPL", "MSFT"}
