# Market Data Backend — Summary

**Status**: ✅ Implemented and tested. See `backend/app/market_data/`.

This document is the single, current reference for the market data subsystem. The full historical design and review docs live in `planning/archive/` if deeper context is needed.

---

## 1. Goals

- One **abstract provider interface** with two interchangeable implementations: `SimulatorProvider` (default) and `MassiveProvider` (Polygon.io REST).
- A single **in-process price cache** that the provider writes to and the SSE layer reads from.
- A single SSE endpoint `GET /api/stream/prices` fanning out updates to any number of connected clients.
- Provider selection is purely env-driven (`MASSIVE_API_KEY`) — the rest of the backend is provider-agnostic.

**Non-goals**: order book, historical OHLC fetch, WebSocket from Massive, intraday tick persistence.

---

## 2. Architecture

```
backend/app/market_data/
├── types.py          # Tick dataclass, Direction, utcnow
├── cache.py          # PriceCache (asyncio-locked dict)
├── broadcaster.py    # PriceBroadcaster (per-subscriber Queue, drop-oldest)
├── provider.py       # MarketDataProvider ABC + WatchlistSource Protocol
├── simulator.py      # SimulatorProvider (GBM, sector-correlated)
├── massive.py        # MassiveProvider (Polygon.io snapshot endpoint)
├── service.py        # MarketDataService — composition root + supervisor
├── factory.py        # build_provider() — env-driven selection
└── routes.py         # /api/stream/prices SSE route
```

**Data flow**: `provider.stream() → service → cache.update() → broadcaster.publish() → SSE subscribers`.

### Key types

`Tick`: `frozen=True, slots=True` dataclass with `ticker, price, prev_price, timestamp (tz-aware UTC), direction (up/down/flat)`. Serialized to compact SSE JSON via `to_sse_dict()` (short keys: `t, p, pp, ts, d`).

### Cache

Thread-safe `dict[ticker, Tick]`. The lock guards the read-modify-write (compute direction). Single producer task writes; SSE handlers read via `snapshot()`.

### Broadcaster

Each SSE client gets an `asyncio.Queue(maxsize=256)` (~4s of backlog at 500ms cadence). On overflow, oldest tick is dropped — slow clients never back-pressure the producer. `subscribe()` is an async context manager for automatic cleanup.

---

## 3. Simulator (default)

Geometric Brownian Motion with sector-correlated normals:

```
S_{t+dt} = S_t · exp((μ − ½σ²)dt + σ√dt · Z)
Z = ρ·F_sector + √(1−ρ²)·ε
```

- One common factor `F` per sector per step (tech / finance / consumer / other), `ρ ≈ 0.7`.
- 500ms tick cadence; μ and σ annualized.
- Hardcoded `SPECS` table for the 10 default tickers (AAPL, GOOGL, MSFT, AMZN, TSLA, NVDA, META, JPM, V, NFLX). Unknown tickers fall back to `DEFAULT_SPEC`.
- Occasional 2–5% event jumps (~once per 5 min per ticker).
- Hard floor at $0.01.
- Fully deterministic when seeded (`rng=random.Random(42)`).
- Hot-reload: reads watchlist on every step, picks up additions on the next tick.

---

## 4. Massive (Polygon.io)

Snapshot endpoint, one HTTP call per poll cycle:

```
GET https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?tickers=...&apiKey=...
```

- Tier-aware cadence via `MASSIVE_TIER`: `free=15s` (default), `starter=5s`, `developer=2s`, `advanced=1s`.
- Fallback price chain: `lastTrade.p → day.c → prevDay.c`. Zero/null filtered out.
- Symbols normalized: `sorted(set(t.upper() ...))`.
- HTTP errors (`httpx.HTTPError`) logged and swallowed — cache keeps last good price; SSE keeps heartbeating.
- Constructor injection of `httpx.AsyncClient` for tests; lifecycle owned via `_owns_client` flag.

---

## 5. SSE Route

`GET /api/stream/prices` returns a `text/event-stream` `StreamingResponse`.

1. **Snapshot burst** on connect: every ticker currently in the cache is sent immediately.
2. **Live updates** via `broadcaster.subscribe()`.
3. **Heartbeat** every 15s (SSE comment) to keep idle proxies from timing out.
4. Detects client disconnect via `request.is_disconnected()`.

Headers: `Cache-Control: no-cache`, `X-Accel-Buffering: no`, `Connection: keep-alive`.

Frontend usage:
```ts
const es = new EventSource("/api/stream/prices");
es.addEventListener("price", (e) => { /* {t,p,pp,ts,d} */ });
```

---

## 6. Wiring

`MarketDataService` owns the cache, broadcaster, and producer task. Started/stopped from FastAPI's `lifespan` context. A supervisor wraps `_run` with exponential backoff (1s → 30s) on provider crashes.

`build_provider(watchlist)` in `factory.py`:
- `MASSIVE_API_KEY` set & non-empty → `MassiveProvider`
- otherwise → `SimulatorProvider`

The watchlist is passed in via the `WatchlistSource` Protocol (`async current_tickers() -> list[str]`) — market data does not import from the watchlist module.

---

## 7. Configuration

| Env var            | Default | Effect                                                      |
| ------------------ | ------- | ----------------------------------------------------------- |
| `MASSIVE_API_KEY`  | unset   | If set & non-empty → `MassiveProvider`, else simulator      |
| `MASSIVE_TIER`     | `free`  | Polling cadence for Massive (15s / 5s / 2s / 1s)            |
| `SIM_STEP_SECONDS` | `0.5`   | Simulator tick cadence override                             |
| `SIM_RANDOM_SEED`  | unset   | Deterministic simulator for demos / reproducible runs       |

---

## 8. Testing

`backend/tests/market_data/` — 51 tests covering:

- **types** — SSE serialization, rounding, tz-awareness
- **cache** — direction logic, snapshot independence, multi-ticker isolation
- **broadcaster** — fan-out, drop-oldest, subscriber lifecycle
- **simulator** — determinism (seeded), hot-reload, default-spec fallback, positivity floor
- **massive** — `httpx.MockTransport` based; fallback chain, URL canonicalization, error swallowing
- **service** — cache fill, broadcast wiring, idempotent stop
- **routes** — snapshot burst, SSE event format, response headers (uses `ASGITransport(app=app)`)
- **conformance** — single parametrized test running both providers through the same assertions, catches drift between implementations

Run with `uv run pytest` from `backend/`.

---

## 9. Known Edge Cases & Decisions

| Scenario                       | Behavior                                                                |
| ------------------------------ | ----------------------------------------------------------------------- |
| Empty watchlist                | Producer yields nothing; cache idle; SSE clients receive heartbeats.    |
| New ticker added               | Simulator: next 500ms tick. Massive: next poll cycle.                   |
| Removed ticker                 | Stops being yielded; cache keeps last value (used by open positions).   |
| Massive 429 / 5xx              | Logged, swallowed. Cache keeps last good. SSE keeps heartbeating.       |
| `MASSIVE_API_KEY=""`           | Counts as unset → simulator selected.                                   |
| Slow SSE client                | Per-subscriber queue caps at 256; oldest tick dropped first.            |
| Lifespan shutdown mid-stream   | `service.stop()` cancels producer; provider `__aexit__` closes httpx.   |
| Disconnect during `queue.get()`| Detected on next tick or within 15s heartbeat — bounded cleanup latency.|

---

## 10. Future Multi-User Path

The cache and broadcaster are global and keyed by ticker only — they're already multi-user-ready. To extend:
- Replace the single `WatchlistRegistry.current_tickers()` with a union of every user's watchlist.
- Add per-user filtering at the SSE layer (clients subscribe with a user id; broadcaster filters before publishing).

No schema changes, no provider changes.
