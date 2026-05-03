from __future__ import annotations

import random
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from .config import SIM_RANDOM_SEED
from .market_data.factory import build_provider
from .market_data.routes import router as market_router
from .market_data.service import MarketDataService


class _SimpleWatchlist:
    """Temporary stub until the watchlist module is implemented.

    Returns the default seed tickers so the simulator has something to
    work with before the database layer exists.
    """

    DEFAULT_TICKERS = [
        "AAPL", "GOOGL", "MSFT", "AMZN", "TSLA",
        "NVDA", "META", "JPM", "V", "NFLX",
    ]

    async def current_tickers(self) -> list[str]:
        return list(self.DEFAULT_TICKERS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    watchlist = _SimpleWatchlist()

    # Allow deterministic simulator via env var (useful for demos).
    from .market_data.simulator import SimulatorProvider
    from .market_data.factory import build_provider as _build

    provider = _build(watchlist)
    if isinstance(provider, SimulatorProvider) and SIM_RANDOM_SEED is not None:
        provider = SimulatorProvider(watchlist, rng=random.Random(SIM_RANDOM_SEED))

    service = MarketDataService(provider)
    await service.start()
    app.state.market_data = service
    app.state.watchlist = watchlist
    try:
        yield
    finally:
        await service.stop()


app = FastAPI(title="FinAlly Backend", lifespan=lifespan)
app.include_router(market_router)
