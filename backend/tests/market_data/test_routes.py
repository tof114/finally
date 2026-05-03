from __future__ import annotations

import asyncio
import json
import random

import httpx
import pytest
from fastapi import FastAPI

from app.market_data.routes import router
from app.market_data.service import MarketDataService
from app.market_data.simulator import SimulatorProvider

from .conftest import FakeWatchlist


def _make_app(service: MarketDataService) -> FastAPI:
    # No lifespan needed: each test starts/stops the service explicitly.
    app = FastAPI()
    app.state.market_data = service
    app.include_router(router)
    return app


@pytest.mark.asyncio
async def test_snapshot_burst_on_connect(live_server):
    """Client immediately receives all cached tickers on SSE connect."""
    wl = FakeWatchlist(["AAPL", "MSFT"])
    provider = SimulatorProvider(wl, rng=random.Random(0))
    service = MarketDataService(provider)
    await service.start()
    await asyncio.sleep(0.7)  # let cache fill

    received_tickers: set[str] = set()
    try:
        base_url = await live_server(_make_app(service))
        async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as client:
            async with client.stream("GET", "/api/stream/prices") as resp:
                assert resp.status_code == 200
                assert "text/event-stream" in resp.headers["content-type"]

                async for line in resp.aiter_lines():
                    if line.startswith("data:"):
                        data = json.loads(line[5:])
                        received_tickers.add(data["t"])
                    if len(received_tickers) >= 2:
                        break
    finally:
        await service.stop()

    assert {"AAPL", "MSFT"}.issubset(received_tickers)


@pytest.mark.asyncio
async def test_sse_event_format(live_server):
    """Each price event has the expected JSON keys."""
    wl = FakeWatchlist(["AAPL"])
    provider = SimulatorProvider(wl, rng=random.Random(5))
    service = MarketDataService(provider)
    await service.start()
    await asyncio.sleep(0.7)

    try:
        base_url = await live_server(_make_app(service))
        async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as client:
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
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_response_headers(live_server):
    """SSE response should have correct cache-control headers."""
    wl = FakeWatchlist([])
    provider = SimulatorProvider(wl, rng=random.Random(0))
    service = MarketDataService(provider)
    await service.start()

    try:
        base_url = await live_server(_make_app(service))
        async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as client:
            async with client.stream("GET", "/api/stream/prices") as resp:
                assert resp.headers.get("cache-control") == "no-cache"
                assert resp.headers.get("x-accel-buffering") == "no"
    finally:
        await service.stop()
