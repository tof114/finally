# Market Data Backend — Detailed Design

This document specifies the implementation of the market data subsystem for FinAlly.
It covers the unified provider interface, the GBM-based simulator, the Massive
(Polygon.io) REST client, the in-memory price cache, the SSE fan-out broadcaster,
and the FastAPI wiring.

---

## 1. Goals & Non-Goals

### Goals

- One **abstract provider interface** with two interchangeable implementations
  (`SimulatorProvider`, `MassiveProvider`).
- A **single in-process price cache** that both providers write to and the SSE
  layer reads from.
- A single **SSE endpoint** (`GET /api/stream/prices`) that fans-out price
  updates to any number of connected browsers from the cache.
- Provider selection is purely a function of environment variables — the rest
  of the backend is provider-agnostic.
- Designed so the same architecture would work for multiple users in the
  future (cache and broadcaster are global, not per-connection).

### Non-Goals

- No order book, no level-2 data, no historical OHLC fetch (sparklines are
  accumulated client-side from the SSE stream).
- No WebSocket support from Massive — REST polling only (works on every tier).
- No persistence of intraday tick data (only `portfolio_snapshots` are
  persisted, by a separate background task in the portfolio module).

---

## 2. Module Layout

All code lives under `backend/app/market_data/`.

```
backend/
├── pyproject.toml
└── app/
    ├── main.py                          # FastAPI app + lifespan wiring
    ├── config.py                        # env var loading
    └── market_data/
        ├── __init__.py
        ├── types.py                     # Tick, dataclasses
        ├── cache.py                     # PriceCache
        ├── broadcaster.py               # PriceBroadcaster (SSE fan-out)
        ├── provider.py                  # MarketDataProvider ABC
        ├── simulator.py                 # SimulatorProvider (GBM)
        ├── massive.py                   # MassiveProvider (Polygon.io REST)
        ├── service.py                   # MarketDataService (composition)
        ├── factory.py                   # build_provider() — env-driven
        └── routes.py                    # /api/stream/prices SSE route
```

The `WatchlistRegistry` lives in the watchlist module, but the market data
service depends on it through a small interface (see §11).

---

## 3. Shared Types

All providers emit the same `Tick` shape. The cache and SSE layer never
need to know which provider produced a tick.

```python
# app/market_data/types.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

Direction = Literal["up", "down", "flat"]

@dataclass(frozen=True, slots=True)
class Tick:
    ticker: str
    price: float
    prev_price: float | None     # None on the very first tick we see
    timestamp: datetime          # always tz-aware UTC
    direction: Direction         # derived from price vs prev_price

    def to_sse_dict(self) -> dict:
        # Compact JSON shipped over SSE. Keep keys short — 500ms cadence.
        return {
            "t": self.ticker,
            "p": round(self.price, 4),
            "pp": round(self.prev_price, 4) if self.prev_price is not None else None,
            "ts": self.timestamp.isoformat(),
            "d": self.direction,
        }


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
```

---

## 4. PriceCache

A thread-safe in-memory map of `ticker -> Tick`. The cache is the **single
source of truth** for the latest price; both the SSE endpoint and the
portfolio valuation logic read from it.

The cache is intentionally tiny — no history, no aggregation. History lives
on the client (sparklines) and in the `portfolio_snapshots` table.

```python
# app/market_data/cache.py
from __future__ import annotations
import asyncio
from .types import Tick, Direction, utcnow


class PriceCache:
    def __init__(self) -> None:
        self._data: dict[str, Tick] = {}
        self._lock = asyncio.Lock()

    async def update(self, ticker: str, price: float) -> Tick:
        """Insert or replace the latest tick. Returns the new Tick."""
        async with self._lock:
            prev = self._data.get(ticker)
            prev_price = prev.price if prev is not None else None
            direction: Direction
            if prev_price is None or price == prev_price:
                direction = "flat"
            elif price > prev_price:
                direction = "up"
            else:
                direction = "down"
            tick = Tick(
                ticker=ticker,
                price=price,
                prev_price=prev_price,
                timestamp=utcnow(),
                direction=direction,
            )
            self._data[ticker] = tick
            return tick

    async def get(self, ticker: str) -> Tick | None:
        async with self._lock:
            return self._data.get(ticker)

    async def snapshot(self) -> dict[str, Tick]:
        """Copy of the current cache. Used to send the initial SSE burst."""
        async with self._lock:
            return dict(self._data)
```

**Why a lock at all under asyncio?** Strictly speaking, `dict` mutations
are atomic on CPython. The lock is for *logical* atomicity — `prev = get;
compute direction; put` must not interleave with another `update` for the
same ticker, otherwise two ticks could compute the same `prev_price` and
report the wrong direction. The lock is uncontended in practice (one
producer task per cache).

---

## 5. Broadcaster (SSE Fan-Out)

Pattern: each connected SSE client owns an `asyncio.Queue`. The broadcaster
keeps the set of queues and pushes every new tick into each one. Slow
clients are handled with a bounded queue + drop-oldest policy so a stuck
browser cannot leak memory or back-pressure the producer.

```python
# app/market_data/broadcaster.py
from __future__ import annotations
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator
from .types import Tick

log = logging.getLogger(__name__)

# Per-subscriber queue size. ~500ms cadence × 30 tickers => ~60 ticks/sec.
# A queue of 256 holds ~4s of backlog before we start dropping.
_QUEUE_SIZE = 256


class PriceBroadcaster:
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
                # Drop oldest to make room. Slow client; better to lose a
                # tick than to stall the producer.
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
```

---

## 6. Provider Interface

The abstract base class. Both implementations expose a single method
`stream()` that yields `(ticker, price)` pairs forever. The service
layer wraps each yield into a cache update + broadcast.

```python
# app/market_data/provider.py
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import AsyncIterator, Protocol


class WatchlistSource(Protocol):
    """Anything that can tell us which tickers we currently care about."""
    async def current_tickers(self) -> list[str]: ...


class MarketDataProvider(ABC):
    """Common interface for the simulator and Massive client.

    Implementations must be cancellation-safe — when the consumer stops
    iterating, any background work (HTTP sessions, tasks) must be cleaned
    up via __aexit__.
    """

    @abstractmethod
    async def __aenter__(self) -> "MarketDataProvider": ...

    @abstractmethod
    async def __aexit__(self, exc_type, exc, tb) -> None: ...

    @abstractmethod
    def stream(self) -> AsyncIterator[tuple[str, float]]:
        """Async generator yielding (ticker, price) forever."""
```

Both providers consult the same `WatchlistSource` rather than holding
their own list — when the user adds a ticker, the change is picked up on
the next iteration of the producer loop.

---

## 7. SimulatorProvider (default)

### 7.1 Math: Geometric Brownian Motion

GBM step over time `dt` for asset `i`:

```
S_{i, t+dt} = S_{i, t} * exp((mu_i - 0.5 * sigma_i^2) * dt + sigma_i * sqrt(dt) * Z_i)
```

`Z_i` is the *correlated* standard normal for ticker `i`. We get correlation
by drawing an i.i.d. vector `eps ~ N(0, I)` and multiplying by the Cholesky
factor `L` of the desired correlation matrix `C`:

```
Z = L @ eps,  where  C = L @ L.T
```

For FinAlly we don't need a full covariance matrix — a simple "all tech
stocks share a common factor" model works fine. We split tickers into
sectors (`tech`, `finance`, `consumer`, `other`) and use a one-factor
model per sector:

```
Z_i = rho_i * F_{sector(i)} + sqrt(1 - rho_i^2) * eps_i
```

with `F_sector ~ N(0, 1)` shared by all members of the sector and
`rho_i ≈ 0.7`. This gives visually believable sector co-movement without
linear-algebra dependencies.

### 7.2 Seed Prices & Parameters

Hardcoded table of "realistic looking as of Jan 2026" seeds. Drift `mu`
is annualized; volatility `sigma` is annualized — both converted to a
500ms step inside the loop.

```python
# app/market_data/simulator.py — top of file
from __future__ import annotations
import asyncio
import math
import random
from dataclasses import dataclass
from typing import AsyncIterator

from .provider import MarketDataProvider, WatchlistSource


@dataclass(frozen=True)
class TickerSpec:
    seed_price: float
    mu: float          # annualized drift (e.g. 0.08 = +8%/yr)
    sigma: float       # annualized vol   (e.g. 0.30 = 30%/yr)
    sector: str
    rho: float = 0.7   # sector loading


# Defaults cover the seed watchlist plus a long tail of well-known tickers
# the LLM might add. Anything not listed gets DEFAULT_SPEC.
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

STEP_SECONDS = 0.5
SECONDS_PER_YEAR = 365.25 * 24 * 3600
EVENT_PROBABILITY_PER_STEP = 1.0 / 600.0  # ~ once every 5 min per ticker
EVENT_MAGNITUDE_RANGE = (0.02, 0.05)      # 2–5% jump
```

### 7.3 The Producer Loop

```python
class SimulatorProvider(MarketDataProvider):
    def __init__(self, watchlist: WatchlistSource, *, rng: random.Random | None = None) -> None:
        self._watchlist = watchlist
        self._rng = rng or random.Random()
        self._prices: dict[str, float] = {}   # last simulated price

    async def __aenter__(self) -> "SimulatorProvider":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    def _spec(self, ticker: str) -> TickerSpec:
        return SPECS.get(ticker, DEFAULT_SPEC)

    def _ensure_seed(self, ticker: str) -> None:
        if ticker not in self._prices:
            self._prices[ticker] = self._spec(ticker).seed_price

    def _step_price(self, ticker: str, sector_factor: float) -> float:
        spec = self._spec(ticker)
        s = self._prices[ticker]
        dt = STEP_SECONDS / SECONDS_PER_YEAR
        eps = self._rng.gauss(0.0, 1.0)
        z = spec.rho * sector_factor + math.sqrt(1.0 - spec.rho ** 2) * eps
        drift = (spec.mu - 0.5 * spec.sigma ** 2) * dt
        diffusion = spec.sigma * math.sqrt(dt) * z
        new_price = s * math.exp(drift + diffusion)

        # Occasional dramatic event
        if self._rng.random() < EVENT_PROBABILITY_PER_STEP:
            mag = self._rng.uniform(*EVENT_MAGNITUDE_RANGE)
            sign = 1.0 if self._rng.random() < 0.5 else -1.0
            new_price *= (1.0 + sign * mag)

        # Floor — never let a ticker go to (or below) zero
        new_price = max(new_price, 0.01)
        self._prices[ticker] = new_price
        return new_price

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
```

### 7.4 Properties Worth Calling Out

- **Pure async**: no threads, no numpy.
- **Deterministic when seeded**: tests pass `rng=random.Random(42)`.
- **Watchlist hot reload**: `current_tickers()` is consulted on every step,
  so newly-added tickers begin streaming on the very next 500ms tick.
- **Removed tickers**: silently stop being yielded. The cache keeps the
  last value, which is the desired behavior — a removed-then-re-added
  ticker resumes from where the simulator left it.

---

## 8. MassiveProvider (Polygon.io REST)

### 8.1 Endpoint

We use the **snapshot** endpoint, which returns the latest trade for many
tickers in one HTTP call:

```
GET https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers
    ?tickers=AAPL,GOOGL,MSFT,...
    &apiKey=$MASSIVE_API_KEY
```

One HTTP request per poll cycle, regardless of watchlist size — important
for free tier (5 req/min).

### 8.2 Tier-Aware Polling Cadence

```python
# app/market_data/massive.py — top
from __future__ import annotations
import asyncio
import logging
import os
from typing import AsyncIterator
import httpx

from .provider import MarketDataProvider, WatchlistSource

log = logging.getLogger(__name__)

BASE_URL = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"

# Conservative defaults; user can override via env.
TIER_INTERVALS = {
    "free":     15.0,   # 4 req/min, headroom under 5/min cap
    "starter":  5.0,
    "developer": 2.0,
    "advanced": 1.0,
}
```

### 8.3 The Provider

```python
class MassiveProvider(MarketDataProvider):
    def __init__(
        self,
        api_key: str,
        watchlist: WatchlistSource,
        *,
        poll_interval: float | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._api_key = api_key
        self._watchlist = watchlist
        self._interval = poll_interval or TIER_INTERVALS[
            os.environ.get("MASSIVE_TIER", "free").lower()
        ]
        self._client = client          # if injected, we won't close it
        self._owns_client = client is None

    async def __aenter__(self) -> "MassiveProvider":
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=10.0)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _poll_once(self, tickers: list[str]) -> list[tuple[str, float]]:
        if not tickers:
            return []
        params = {
            "tickers": ",".join(sorted(set(tickers))),
            "apiKey": self._api_key,
        }
        assert self._client is not None
        try:
            resp = await self._client.get(BASE_URL, params=params)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            log.warning("massive poll failed: %s", e)
            return []
        return self._parse(resp.json())

    @staticmethod
    def _parse(payload: dict) -> list[tuple[str, float]]:
        """Extract (ticker, price) pairs from a Polygon snapshot response.

        Schema (abridged):
          {
            "status": "OK",
            "tickers": [
              {
                "ticker": "AAPL",
                "lastTrade": {"p": 192.34, "t": 1700000000000000000, ...},
                "day":       {"c": 192.10, ...},
                "prevDay":   {"c": 191.80, ...}
              }, ...
            ]
          }
        """
        out: list[tuple[str, float]] = []
        for entry in payload.get("tickers", []):
            sym = entry.get("ticker")
            if not sym:
                continue
            # Prefer last trade; fall back to day close, then prev close.
            price = (
                (entry.get("lastTrade") or {}).get("p")
                or (entry.get("day") or {}).get("c")
                or (entry.get("prevDay") or {}).get("c")
            )
            if price is None or price <= 0:
                continue
            out.append((sym, float(price)))
        return out

    async def stream(self) -> AsyncIterator[tuple[str, float]]:
        while True:
            tickers = await self._watchlist.current_tickers()
            for pair in await self._poll_once(tickers):
                yield pair
            await asyncio.sleep(self._interval)
```

### 8.4 Robustness Notes

- **Single HTTP call per cycle** — preserves rate-limit headroom.
- **Degrade silently on errors**: a failed poll logs and yields nothing;
  the cache keeps its last good value, so the SSE clients see a brief
  flat period rather than a crash.
- **Fallback price chain** (`lastTrade.p` → `day.c` → `prevDay.c`)
  handles outside-RTH and recently-listed tickers.
- **Symbol normalization**: Polygon expects upper-case; we sort+dedupe
  before joining so tests have a stable URL.

---

## 9. MarketDataService

The composition root: owns the cache, the broadcaster, the provider, and
runs the producer task. The rest of the app receives this object via
`request.app.state.market_data`.

```python
# app/market_data/service.py
from __future__ import annotations
import asyncio
import logging
from .broadcaster import PriceBroadcaster
from .cache import PriceCache
from .provider import MarketDataProvider

log = logging.getLogger(__name__)


class MarketDataService:
    def __init__(self, provider: MarketDataProvider) -> None:
        self.cache = PriceCache()
        self.broadcaster = PriceBroadcaster()
        self._provider = provider
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        await self._provider.__aenter__()
        self._task = asyncio.create_task(self._run(), name="market-data-producer")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self._provider.__aexit__(None, None, None)

    async def _run(self) -> None:
        try:
            async for ticker, price in self._provider.stream():
                tick = await self.cache.update(ticker, price)
                await self.broadcaster.publish(tick)
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - defensive
            log.exception("market data producer crashed; restarting")
            # Re-raise so the lifespan can restart us cleanly. In practice
            # we wrap this in an outer supervisor (see §10).
            raise
```

A tiny supervisor wraps `_run` so a crashing provider auto-restarts with
exponential backoff (1s, 2s, 4s, capped at 30s). Implementation is left
to the engineer — pattern is `while True: try: await _run(); except:
sleep(backoff)`.

---

## 10. FastAPI Wiring

### 10.1 Factory

```python
# app/market_data/factory.py
from __future__ import annotations
import os
from .provider import MarketDataProvider, WatchlistSource
from .simulator import SimulatorProvider
from .massive import MassiveProvider


def build_provider(watchlist: WatchlistSource) -> MarketDataProvider:
    key = (os.environ.get("MASSIVE_API_KEY") or "").strip()
    if key:
        return MassiveProvider(api_key=key, watchlist=watchlist)
    return SimulatorProvider(watchlist=watchlist)
```

### 10.2 Lifespan

```python
# app/main.py (excerpt)
from contextlib import asynccontextmanager
from fastapi import FastAPI

from .market_data.factory import build_provider
from .market_data.service import MarketDataService
from .market_data.routes import router as market_router
from .watchlist.registry import WatchlistRegistry  # supplied by watchlist module


@asynccontextmanager
async def lifespan(app: FastAPI):
    watchlist = WatchlistRegistry()  # reads from SQLite
    await watchlist.load()
    provider = build_provider(watchlist)
    service = MarketDataService(provider)
    await service.start()
    app.state.market_data = service
    app.state.watchlist = watchlist
    try:
        yield
    finally:
        await service.stop()


app = FastAPI(lifespan=lifespan)
app.include_router(market_router)
```

### 10.3 SSE Route

`StreamingResponse` with `text/event-stream`. We send an immediate
**snapshot burst** so a fresh client gets the current price for every
ticker before waiting for the next 500ms tick.

```python
# app/market_data/routes.py
from __future__ import annotations
import asyncio
import json
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

router = APIRouter(prefix="/api/stream", tags=["market-data"])

# Heartbeat keeps proxies / load balancers from idle-timing out the SSE
# connection during low-volume periods.
HEARTBEAT_SECONDS = 15


def _sse(event: str, data: dict) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data, separators=(',', ':'))}\n\n".encode()


@router.get("/prices")
async def stream_prices(request: Request) -> StreamingResponse:
    service = request.app.state.market_data

    async def gen():
        # 1. Snapshot burst: every ticker we know about, right now.
        snap = await service.cache.snapshot()
        for tick in snap.values():
            yield _sse("price", tick.to_sse_dict())

        # 2. Live updates via the broadcaster.
        async with service.broadcaster.subscribe() as queue:
            while True:
                if await request.is_disconnected():
                    return
                try:
                    tick = await asyncio.wait_for(queue.get(), timeout=HEARTBEAT_SECONDS)
                    yield _sse("price", tick.to_sse_dict())
                except asyncio.TimeoutError:
                    yield b": heartbeat\n\n"   # SSE comment, ignored by client

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # disable nginx buffering if proxied
            "Connection": "keep-alive",
        },
    )
```

Frontend code (reference):

```ts
const es = new EventSource("/api/stream/prices");
es.addEventListener("price", (e) => {
  const { t, p, pp, ts, d } = JSON.parse(e.data);
  store.update(t, { price: p, prevPrice: pp, ts, direction: d });
});
```

---

## 11. WatchlistSource Contract

The market data module **does not** import the watchlist module directly.
It depends only on the `WatchlistSource` Protocol:

```python
class WatchlistSource(Protocol):
    async def current_tickers(self) -> list[str]: ...
```

The watchlist module supplies an implementation that reads from SQLite
and caches in memory, invalidating its cache when the `/api/watchlist`
mutation endpoints fire. This keeps the module boundary clean and makes
the simulator/Massive trivial to unit-test against a fake.

```python
# Test fake — exemplifies the contract.
class FakeWatchlist:
    def __init__(self, tickers: list[str]) -> None:
        self._t = tickers
    async def current_tickers(self) -> list[str]:
        return list(self._t)
```

---

## 12. Configuration Summary

| Env var               | Default     | Effect                                              |
| --------------------- | ----------- | --------------------------------------------------- |
| `MASSIVE_API_KEY`     | unset       | If set + non-empty → `MassiveProvider`, else simulator |
| `MASSIVE_TIER`        | `free`      | Selects polling cadence (15s / 5s / 2s / 1s)        |
| `SIM_STEP_SECONDS`    | `0.5`       | (Optional override) simulator tick cadence          |
| `SIM_RANDOM_SEED`     | unset       | (Optional) deterministic simulator for demos        |

`config.py` reads these once at startup; nothing else touches `os.environ`.

---

## 13. Testing Strategy

### 13.1 Unit Tests

```
backend/tests/market_data/
├── test_cache.py
├── test_simulator.py
├── test_massive.py
├── test_broadcaster.py
└── test_routes.py
```

**`test_simulator.py`** — exemplary cases:

```python
import random
import pytest
from app.market_data.simulator import SimulatorProvider, SPECS


class FakeWatchlist:
    def __init__(self, t): self._t = t
    async def current_tickers(self): return list(self._t)


@pytest.mark.asyncio
async def test_simulator_seeds_known_ticker_at_spec_price():
    sim = SimulatorProvider(FakeWatchlist(["AAPL"]), rng=random.Random(0))
    async with sim:
        gen = sim.stream()
        ticker, price = await anext(gen)
        assert ticker == "AAPL"
        # First tick is one GBM step away from seed_price; should be close.
        assert abs(price - SPECS["AAPL"].seed_price) / SPECS["AAPL"].seed_price < 0.05


@pytest.mark.asyncio
async def test_simulator_is_deterministic_with_seed():
    sim_a = SimulatorProvider(FakeWatchlist(["AAPL", "GOOGL"]), rng=random.Random(42))
    sim_b = SimulatorProvider(FakeWatchlist(["AAPL", "GOOGL"]), rng=random.Random(42))
    async with sim_a, sim_b:
        ga, gb = sim_a.stream(), sim_b.stream()
        for _ in range(20):
            assert await anext(ga) == await anext(gb)


@pytest.mark.asyncio
async def test_simulator_picks_up_added_tickers():
    wl = FakeWatchlist(["AAPL"])
    sim = SimulatorProvider(wl, rng=random.Random(1))
    async with sim:
        gen = sim.stream()
        await anext(gen)               # AAPL
        wl._t.append("MSFT")           # mutate while streaming
        seen = set()
        for _ in range(4):
            t, _ = await anext(gen)
            seen.add(t)
        assert "MSFT" in seen


@pytest.mark.asyncio
async def test_simulator_unknown_ticker_uses_default_spec():
    sim = SimulatorProvider(FakeWatchlist(["WEIRD"]), rng=random.Random(0))
    async with sim:
        ticker, price = await anext(sim.stream())
        assert ticker == "WEIRD"
        assert price > 0
```

**`test_massive.py`** — uses `httpx.MockTransport` so no network call is
ever made:

```python
import httpx
import pytest
from app.market_data.massive import MassiveProvider


SAMPLE = {
    "status": "OK",
    "tickers": [
        {"ticker": "AAPL",  "lastTrade": {"p": 192.34}},
        {"ticker": "GOOGL", "lastTrade": {"p": 175.00}},
        {"ticker": "BAD",   "lastTrade": {"p": 0}},        # filtered out
        {"ticker": "FALLBACK",
         "lastTrade": None, "day": {"c": 50.0}},           # uses day close
    ],
}


def make_client(captured):
    async def handler(request):
        captured.append(request)
        return httpx.Response(200, json=SAMPLE)
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


class FakeWatchlist:
    def __init__(self, t): self._t = t
    async def current_tickers(self): return list(self._t)


@pytest.mark.asyncio
async def test_massive_parses_snapshot_and_filters_bad_prices():
    captured = []
    client = make_client(captured)
    p = MassiveProvider(api_key="k", watchlist=FakeWatchlist(["AAPL", "GOOGL", "BAD", "FALLBACK"]),
                        poll_interval=0.01, client=client)
    async with p:
        gen = p.stream()
        out = []
        for _ in range(3):
            out.append(await anext(gen))
    syms = {s for s, _ in out}
    assert "AAPL" in syms and "GOOGL" in syms and "FALLBACK" in syms
    assert "BAD" not in syms
    assert "AAPL,BAD,FALLBACK,GOOGL" in str(captured[0].url)


@pytest.mark.asyncio
async def test_massive_swallows_http_errors():
    async def handler(_):
        return httpx.Response(500)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    p = MassiveProvider("k", FakeWatchlist(["AAPL"]), poll_interval=0.01, client=client)
    async with p:
        gen = p.stream()
        # No yields, but no exception either — wait one cycle then cancel.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(anext(gen), timeout=0.05)
```

**`test_cache.py`** asserts direction logic (`up`/`down`/`flat`) and
that `prev_price` is `None` on the first tick.

**`test_broadcaster.py`** verifies fan-out to multiple subscribers and
the drop-oldest behavior on a full queue.

**`test_routes.py`** uses `httpx.AsyncClient(app=app)` with
`stream("GET", "/api/stream/prices")` to confirm the snapshot burst
arrives, then a live tick arrives after publishing one to the
broadcaster.

### 13.2 Conformance Tests

A single parametrized test runs both providers through the same
assertions:

```python
@pytest.mark.parametrize("factory", [
    lambda wl: SimulatorProvider(wl, rng=random.Random(0)),
    lambda wl: MassiveProvider("k", wl, poll_interval=0.01, client=make_client([])),
])
@pytest.mark.asyncio
async def test_provider_emits_positive_prices_for_watchlist_members(factory):
    wl = FakeWatchlist(["AAPL", "GOOGL"])
    p = factory(wl)
    async with p:
        gen = p.stream()
        for _ in range(4):
            t, price = await anext(gen)
            assert t in {"AAPL", "GOOGL"}
            assert price > 0
```

This catches drift between implementations (e.g. someone forgets to
upper-case a symbol on one side).

---

## 14. Edge Cases & Decisions

| Scenario | Behavior |
|---|---|
| Watchlist becomes empty | Producer yields nothing; cache & broadcaster idle; SSE clients receive heartbeats only. |
| Newly added ticker | Simulator: streams next tick (≤500ms). Massive: streams on next poll cycle. |
| Removed ticker | Producer stops yielding; cache keeps last value (used for any open positions until re-added). |
| Massive 429 / 5xx | Logged, swallowed. Cache keeps last good price. SSE keeps heartbeating. |
| `MASSIVE_API_KEY=""` | Counts as unset — simulator selected. |
| Slow SSE client | Per-subscriber queue caps at 256; oldest tick dropped to keep producer unblocked. |
| Lifespan shutdown mid-stream | `service.stop()` cancels the producer task; provider `__aexit__` closes the httpx client; in-flight SSE connections see disconnect. |
| Test mode | Provide a custom `httpx.AsyncClient` (Massive) or seed the `Random` (simulator) via constructor injection — no patching required. |

---

## 15. Why This Design Hits the Requirements

- **One interface, two implementations** — §6 + §7 + §8.
- **Provider-agnostic downstream** — cache, broadcaster, SSE route never
  reference simulator or Massive types (§4, §5, §10).
- **Env-var selection only** — `factory.build_provider` is the only
  branch on `MASSIVE_API_KEY` (§10.1).
- **Future multi-user ready** — cache is keyed by ticker (not user);
  broadcaster fans out to N subscribers; the producer feeds the union
  of all watchlists. To support multiple users we'd swap
  `WatchlistRegistry.current_tickers()` for a union across all users —
  no other change.
- **Clean shutdown / cancellation** — every long-lived resource is
  managed by `__aenter__`/`__aexit__` and the FastAPI `lifespan`.
- **Testable without network or sleeps** — `httpx.MockTransport` for
  Massive, seeded `random.Random` for the simulator, and a tiny
  `FakeWatchlist` that satisfies the Protocol.
