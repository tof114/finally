from __future__ import annotations

import asyncio
import json
import random
from contextlib import asynccontextmanager

import httpx
import pytest
from fastapi import FastAPI

from app.market_data.cache import PriceCache
from app.market_data.broadcaster import PriceBroadcaster
from app.market_data.routes import router
from app.market_data.simulator import SimulatorProvider
from app.market_data.service import MarketDataService
from app.market_data.types import Tick, utcnow


class FakeWatchlist:
    def __init__(self, tickers: list[str]) -> None:
        self._t = tickers

    async def current_tickers(self) -> list[str]:
        return list(self._t)


def _make_tick(ticker: str = "AAPL", price: float = 190.0) -> Tick:
    return Tick(
        ticker=ticker,
        price=price,
        prev_price=None,
        timestamp=utcnow(),
        direction="flat",
    )


def _make_app(service: MarketDataService) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.market_data = service
        yield

    app = FastAPI(lifespan=lifespan)
    app.include_router(router)
    return app


# ---------------------------------------------------------------------------
# Snapshot burst
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_snapshot_burst_on_connect():
    """Client immediately receives all cached tickers on SSE connect."""
    wl = FakeWatchlist(["AAPL", "MSFT"])
    provider = SimulatorProvider(wl, rng=random.Random(0))
    service = MarketDataService(provider)
    await service.start()
    await asyncio.sleep(0.7)  # let cache fill

    app = _make_app(service)
    async with httpx.AsyncClient(app=app, base_url="http://test") as client:
        async with client.stream("GET", "/api/stream/prices") as resp:
            assert resp.status_code == 200
            assert "text/event-stream" in resp.headers["content-type"]

            received_tickers: set[str] = set()
            async for line in resp.aiter_lines():
                if line.startswith("data:"):
                    data = json.loads(line[5:])
                    received_tickers.add(data["t"])
                if len(received_tickers) >= 2:
                    break

    await service.stop()
    assert {"AAPL", "MSFT"}.issubset(received_tickers)


@pytest.mark.asyncio
async def test_sse_event_format():
    """Each price event has the expected JSON keys."""
    wl = FakeWatchlist(["AAPL"])
    provider = SimulatorProvider(wl, rng=random.Random(5))
    service = MarketDataService(provider)
    await service.start()
    await asyncio.sleep(0.7)

    app = _make_app(service)
    async with httpx.AsyncClient(app=app, base_url="http://test") as client:
        async with client.stream("GET", "/api/stream/prices") as resp:
            async for line in resp.aiter_lines():
                if line.startswith("data:"):
                    data = json.loads(line[5:])
                    assert "t" in data
                    assert "p" in data
                    assert "ts" in data
                    assert "d" in data
                    assert data["d"] in ("up", "down", "flat")
                    break

    await service.stop()


@pytest.mark.asyncio
async def test_response_headers():
    """SSE response should have correct cache-control headers."""
    wl = FakeWatchlist([])
    provider = SimulatorProvider(wl, rng=random.Random(0))
    service = MarketDataService(provider)
    await service.start()

    app = _make_app(service)
    async with httpx.AsyncClient(app=app, base_url="http://test") as client:
        async with client.stream("GET", "/api/stream/prices") as resp:
            assert resp.headers.get("cache-control") == "no-cache"
            assert resp.headers.get("x-accel-buffering") == "no"

    await service.stop()
