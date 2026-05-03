from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from .types import Tick

log = logging.getLogger(__name__)

# ~500ms cadence × 30 tickers ≈ 60 ticks/sec. 256 entries ≈ 4s backlog.
_QUEUE_SIZE = 256


class PriceBroadcaster:
    """Fan-out: one producer pushes Ticks; N SSE clients each have a queue.

    Slow clients are handled with a bounded queue + drop-oldest policy so a
    stuck browser cannot back-pressure the producer or leak memory.
    """

    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue[Tick]] = set()
        self._lock = asyncio.Lock()

    async def publish(self, tick: Tick) -> None:
        async with self._lock:
            queues = list(self._subscribers)
        for q in queues:
            try:
                q.put_nowait(tick)
            except asyncio.QueueFull:
                # Drop oldest to make room; better to lose a tick than stall.
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    q.put_nowait(tick)
                except asyncio.QueueFull:
                    log.warning("dropped tick for slow subscriber: %s", tick.ticker)

    @asynccontextmanager
    async def subscribe(self) -> AsyncIterator[asyncio.Queue[Tick]]:
        q: asyncio.Queue[Tick] = asyncio.Queue(maxsize=_QUEUE_SIZE)
        async with self._lock:
            self._subscribers.add(q)
        try:
            yield q
        finally:
            async with self._lock:
                self._subscribers.discard(q)

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)
