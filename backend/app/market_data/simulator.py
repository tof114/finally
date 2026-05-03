from __future__ import annotations

import asyncio
import math
import os
import random
from dataclasses import dataclass
from typing import AsyncIterator

from .provider import MarketDataProvider, WatchlistSource

# ---------------------------------------------------------------------------
# Ticker specs — "realistic as of Jan 2026" seed prices.
# mu = annualized drift; sigma = annualized volatility.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TickerSpec:
    seed_price: float
    mu: float    # annualized drift  (e.g. 0.08 = +8%/yr)
    sigma: float # annualized vol    (e.g. 0.30 = 30%/yr)
    sector: str
    rho: float = 0.7  # sector factor loading


DEFAULT_SPEC = TickerSpec(seed_price=100.0, mu=0.05, sigma=0.30, sector="other")

SPECS: dict[str, TickerSpec] = {
    "AAPL":  TickerSpec(190.0, 0.10, 0.28, "tech"),
    "GOOGL": TickerSpec(175.0, 0.09, 0.30, "tech"),
    "MSFT":  TickerSpec(420.0, 0.11, 0.26, "tech"),
    "AMZN":  TickerSpec(180.0, 0.10, 0.32, "tech"),
    "TSLA":  TickerSpec(245.0, 0.05, 0.55, "tech"),
    "NVDA":  TickerSpec(870.0, 0.18, 0.45, "tech"),
    "META":  TickerSpec(495.0, 0.12, 0.34, "tech"),
    "JPM":   TickerSpec(195.0, 0.06, 0.22, "finance"),
    "V":     TickerSpec(280.0, 0.08, 0.20, "finance"),
    "NFLX":  TickerSpec(615.0, 0.09, 0.36, "consumer"),
}

STEP_SECONDS: float = float(os.environ.get("SIM_STEP_SECONDS", "0.5"))
SECONDS_PER_YEAR: float = 365.25 * 24 * 3600
EVENT_PROBABILITY_PER_STEP: float = 1.0 / 600.0  # ~once every 5 min per ticker
EVENT_MAGNITUDE_RANGE: tuple[float, float] = (0.02, 0.05)  # 2–5% jump


class SimulatorProvider(MarketDataProvider):
    """GBM-based price simulator with one-factor sector correlation.

    Deterministic when a seeded ``random.Random`` is injected — useful for
    unit tests (``rng=random.Random(42)``).
    """

    def __init__(
        self,
        watchlist: WatchlistSource,
        *,
        rng: random.Random | None = None,
    ) -> None:
        self._watchlist = watchlist
        self._rng = rng or random.Random()
        self._prices: dict[str, float] = {}

    async def __aenter__(self) -> "SimulatorProvider":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _spec(self, ticker: str) -> TickerSpec:
        return SPECS.get(ticker, DEFAULT_SPEC)

    def _ensure_seed(self, ticker: str) -> None:
        if ticker not in self._prices:
            self._prices[ticker] = self._spec(ticker).seed_price

    def _step_price(self, ticker: str, sector_factor: float) -> float:
        """Advance one GBM step and return the new price."""
        spec = self._spec(ticker)
        s = self._prices[ticker]
        dt = STEP_SECONDS / SECONDS_PER_YEAR

        # Correlated normal: Z_i = rho * F_sector + sqrt(1-rho^2) * eps_i
        eps = self._rng.gauss(0.0, 1.0)
        z = spec.rho * sector_factor + math.sqrt(1.0 - spec.rho ** 2) * eps

        drift = (spec.mu - 0.5 * spec.sigma ** 2) * dt
        diffusion = spec.sigma * math.sqrt(dt) * z
        new_price = s * math.exp(drift + diffusion)

        # Occasional random event (2–5% sudden move)
        if self._rng.random() < EVENT_PROBABILITY_PER_STEP:
            mag = self._rng.uniform(*EVENT_MAGNITUDE_RANGE)
            sign = 1.0 if self._rng.random() < 0.5 else -1.0
            new_price *= 1.0 + sign * mag

        # Floor — never let a ticker hit zero
        new_price = max(new_price, 0.01)
        self._prices[ticker] = new_price
        return new_price

    # ------------------------------------------------------------------
    # MarketDataProvider interface
    # ------------------------------------------------------------------

    async def stream(self) -> AsyncIterator[tuple[str, float]]:
        while True:
            tickers = await self._watchlist.current_tickers()
            for t in tickers:
                self._ensure_seed(t)

            # Draw one common factor per sector for this step.
            sectors = {self._spec(t).sector for t in tickers}
            factors = {s: self._rng.gauss(0.0, 1.0) for s in sectors}

            for t in tickers:
                price = self._step_price(t, factors[self._spec(t).sector])
                yield (t, price)

            await asyncio.sleep(STEP_SECONDS)
