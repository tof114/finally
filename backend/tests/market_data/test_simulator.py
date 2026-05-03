from __future__ import annotations

import random

import pytest

from app.market_data.simulator import DEFAULT_SPEC, SPECS, SimulatorProvider


class FakeWatchlist:
    def __init__(self, tickers: list[str]) -> None:
        self._t = tickers

    async def current_tickers(self) -> list[str]:
        return list(self._t)


# ---------------------------------------------------------------------------
# Basic correctness
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_seeds_known_ticker_near_spec_price():
    sim = SimulatorProvider(FakeWatchlist(["AAPL"]), rng=random.Random(0))
    async with sim:
        ticker, price = await sim.stream().__anext__()
    assert ticker == "AAPL"
    # First tick should be within 5% of the seed price.
    assert abs(price - SPECS["AAPL"].seed_price) / SPECS["AAPL"].seed_price < 0.05


@pytest.mark.asyncio
async def test_unknown_ticker_uses_default_spec_and_is_positive():
    sim = SimulatorProvider(FakeWatchlist(["WEIRD"]), rng=random.Random(0))
    async with sim:
        ticker, price = await sim.stream().__anext__()
    assert ticker == "WEIRD"
    assert price > 0


@pytest.mark.asyncio
async def test_price_is_always_positive():
    sim = SimulatorProvider(FakeWatchlist(["TSLA"]), rng=random.Random(7))
    async with sim:
        gen = sim.stream()
        for _ in range(50):
            _, price = await gen.__anext__()
            assert price > 0


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_deterministic_with_same_seed():
    wl = FakeWatchlist(["AAPL", "GOOGL"])
    sim_a = SimulatorProvider(wl, rng=random.Random(42))
    sim_b = SimulatorProvider(FakeWatchlist(["AAPL", "GOOGL"]), rng=random.Random(42))
    async with sim_a, sim_b:
        ga, gb = sim_a.stream(), sim_b.stream()
        for _ in range(20):
            a = await ga.__anext__()
            b = await gb.__anext__()
            assert a == b


@pytest.mark.asyncio
async def test_different_seeds_produce_different_results():
    wl = FakeWatchlist(["AAPL"])
    sim_a = SimulatorProvider(wl, rng=random.Random(1))
    sim_b = SimulatorProvider(FakeWatchlist(["AAPL"]), rng=random.Random(2))
    async with sim_a, sim_b:
        results_a = [await sim_a.stream().__anext__() for _ in range(5)]
        results_b = [await sim_b.stream().__anext__() for _ in range(5)]
    prices_a = [p for _, p in results_a]
    prices_b = [p for _, p in results_b]
    assert prices_a != prices_b


# ---------------------------------------------------------------------------
# Hot-reload: watchlist changes picked up during streaming
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_picks_up_added_ticker():
    wl = FakeWatchlist(["AAPL"])
    sim = SimulatorProvider(wl, rng=random.Random(1))
    async with sim:
        gen = sim.stream()
        await gen.__anext__()  # consume AAPL tick
        wl._t.append("MSFT")  # add MSFT mid-stream
        seen: set[str] = set()
        for _ in range(6):
            t, _ = await gen.__anext__()
            seen.add(t)
    assert "MSFT" in seen


@pytest.mark.asyncio
async def test_removed_ticker_stops_streaming():
    wl = FakeWatchlist(["AAPL", "MSFT"])
    sim = SimulatorProvider(wl, rng=random.Random(3))
    async with sim:
        gen = sim.stream()
        # Consume a few ticks, then remove MSFT.
        for _ in range(4):
            await gen.__anext__()
        wl._t = ["AAPL"]  # remove MSFT
        seen_after: set[str] = set()
        for _ in range(6):
            t, _ = await gen.__anext__()
            seen_after.add(t)
    assert "MSFT" not in seen_after


# ---------------------------------------------------------------------------
# Multiple tickers: each ticker appears in the stream
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_all_watchlist_tickers_appear():
    tickers = ["AAPL", "GOOGL", "MSFT", "NVDA"]
    sim = SimulatorProvider(FakeWatchlist(tickers), rng=random.Random(10))
    async with sim:
        gen = sim.stream()
        seen: set[str] = set()
        for _ in range(len(tickers) * 2):
            t, _ = await gen.__anext__()
            seen.add(t)
    assert seen == set(tickers)


# ---------------------------------------------------------------------------
# Conformance: all known SPECS tickers
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_all_spec_tickers_produce_positive_prices():
    tickers = list(SPECS.keys())
    sim = SimulatorProvider(FakeWatchlist(tickers), rng=random.Random(99))
    async with sim:
        gen = sim.stream()
        for _ in range(len(tickers)):
            _, price = await gen.__anext__()
            assert price > 0


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_context_manager_enter_returns_self():
    sim = SimulatorProvider(FakeWatchlist(["AAPL"]))
    result = await sim.__aenter__()
    assert result is sim
    await sim.__aexit__(None, None, None)
