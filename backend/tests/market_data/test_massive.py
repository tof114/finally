from __future__ import annotations

import asyncio

import httpx
import pytest

from app.market_data.massive import MassiveProvider, TIER_INTERVALS


class FakeWatchlist:
    def __init__(self, tickers: list[str]) -> None:
        self._t = tickers

    async def current_tickers(self) -> list[str]:
        return list(self._t)


SAMPLE_PAYLOAD = {
    "status": "OK",
    "tickers": [
        {"ticker": "AAPL",     "lastTrade": {"p": 192.34}},
        {"ticker": "GOOGL",    "lastTrade": {"p": 175.00}},
        {"ticker": "BAD",      "lastTrade": {"p": 0}},        # filtered out (price <= 0)
        {"ticker": "NODATA",   "lastTrade": None, "day": None, "prevDay": None},  # filtered
        {"ticker": "FALLBACK", "lastTrade": None, "day": {"c": 50.0}},            # day close
        {"ticker": "PREVDAY",  "lastTrade": None, "day": {"c": 0}, "prevDay": {"c": 30.0}},
    ],
}


def _make_client(payload: dict, captured: list | None = None):
    async def handler(request):
        if captured is not None:
            captured.append(request)
        return httpx.Response(200, json=payload)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _make_error_client(status: int):
    async def handler(_):
        return httpx.Response(status)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def test_parse_filters_zero_and_none_prices():
    result = MassiveProvider._parse(SAMPLE_PAYLOAD)
    syms = {s for s, _ in result}
    assert "AAPL" in syms
    assert "GOOGL" in syms
    assert "BAD" not in syms
    assert "NODATA" not in syms


def test_parse_fallback_to_day_close():
    result = MassiveProvider._parse(SAMPLE_PAYLOAD)
    by_sym = dict(result)
    assert by_sym["FALLBACK"] == 50.0


def test_parse_fallback_to_prevday():
    result = MassiveProvider._parse(SAMPLE_PAYLOAD)
    by_sym = dict(result)
    assert by_sym["PREVDAY"] == 30.0


def test_parse_prefers_last_trade():
    result = MassiveProvider._parse(SAMPLE_PAYLOAD)
    by_sym = dict(result)
    assert by_sym["AAPL"] == 192.34


def test_parse_empty_payload():
    assert MassiveProvider._parse({}) == []
    assert MassiveProvider._parse({"tickers": []}) == []


def test_parse_missing_ticker_field():
    payload = {"tickers": [{"lastTrade": {"p": 100.0}}]}  # no "ticker" key
    assert MassiveProvider._parse(payload) == []


# ---------------------------------------------------------------------------
# Streaming
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stream_yields_parsed_prices():
    captured: list = []
    client = _make_client(SAMPLE_PAYLOAD, captured)
    p = MassiveProvider(
        api_key="testkey",
        watchlist=FakeWatchlist(["AAPL", "GOOGL"]),
        poll_interval=0.01,
        client=client,
    )
    async with p:
        gen = p.stream()
        out = []
        for _ in range(2):
            out.append(await gen.__anext__())
    syms = {s for s, _ in out}
    assert "AAPL" in syms or "GOOGL" in syms


@pytest.mark.asyncio
async def test_stream_url_contains_sorted_deduplicated_tickers():
    captured: list = []
    client = _make_client(SAMPLE_PAYLOAD, captured)
    p = MassiveProvider(
        api_key="k",
        watchlist=FakeWatchlist(["GOOGL", "AAPL", "AAPL"]),
        poll_interval=0.01,
        client=client,
    )
    async with p:
        gen = p.stream()
        # Drain first poll cycle
        for entry in SAMPLE_PAYLOAD["tickers"]:
            try:
                await asyncio.wait_for(gen.__anext__(), timeout=0.5)
            except (StopAsyncIteration, asyncio.TimeoutError):
                break

    assert len(captured) >= 1
    url_str = str(captured[0].url)
    # tickers param should be sorted and deduplicated
    assert "AAPL%2CGOOGL" in url_str or "AAPL,GOOGL" in url_str


@pytest.mark.asyncio
async def test_swallows_http_500_error():
    client = _make_error_client(500)
    p = MassiveProvider(
        "k", FakeWatchlist(["AAPL"]), poll_interval=0.01, client=client
    )
    async with p:
        gen = p.stream()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(gen.__anext__(), timeout=0.05)


@pytest.mark.asyncio
async def test_swallows_http_429_error():
    client = _make_error_client(429)
    p = MassiveProvider(
        "k", FakeWatchlist(["AAPL"]), poll_interval=0.01, client=client
    )
    async with p:
        gen = p.stream()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(gen.__anext__(), timeout=0.05)


@pytest.mark.asyncio
async def test_empty_watchlist_yields_nothing_then_sleeps():
    client = _make_client(SAMPLE_PAYLOAD)
    p = MassiveProvider(
        "k", FakeWatchlist([]), poll_interval=0.01, client=client
    )
    async with p:
        gen = p.stream()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(gen.__anext__(), timeout=0.05)


# ---------------------------------------------------------------------------
# Context manager / resource management
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_context_manager_returns_self():
    client = _make_client({})
    p = MassiveProvider("k", FakeWatchlist([]), client=client)
    result = await p.__aenter__()
    assert result is p
    await p.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_injected_client_not_closed_on_exit():
    """When a client is injected, the provider must NOT close it."""
    client = _make_client({})
    p = MassiveProvider("k", FakeWatchlist([]), client=client)
    async with p:
        pass
    # Client should still be open (not raise on use).
    assert not client.is_closed


# ---------------------------------------------------------------------------
# Tier intervals
# ---------------------------------------------------------------------------

def test_tier_intervals_defined_for_all_tiers():
    for tier in ("free", "starter", "developer", "advanced"):
        assert tier in TIER_INTERVALS
        assert TIER_INTERVALS[tier] > 0


# ---------------------------------------------------------------------------
# Conformance test: both providers emit positive prices for watchlist members
# ---------------------------------------------------------------------------

# Minimal payload: only the two tickers in the conformance watchlist.
_CONFORMANCE_PAYLOAD = {
    "status": "OK",
    "tickers": [
        {"ticker": "AAPL",  "lastTrade": {"p": 192.34}},
        {"ticker": "GOOGL", "lastTrade": {"p": 175.00}},
    ],
}


def _conformance_massive_factory(wl):
    return MassiveProvider(
        "k",
        wl,
        poll_interval=0.01,
        client=_make_client(_CONFORMANCE_PAYLOAD),
    )


def _conformance_simulator_factory(wl):
    import random
    from app.market_data.simulator import SimulatorProvider
    return SimulatorProvider(wl, rng=random.Random(0))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "factory",
    [
        pytest.param(_conformance_simulator_factory, id="simulator"),
        pytest.param(_conformance_massive_factory, id="massive"),
    ],
)
async def test_provider_emits_positive_prices_for_watchlist_members(factory):
    wl = FakeWatchlist(["AAPL", "GOOGL"])
    p = factory(wl)
    async with p:
        gen = p.stream()
        for _ in range(4):
            t, price = await gen.__anext__()
            assert t in {"AAPL", "GOOGL"}
            assert price > 0
