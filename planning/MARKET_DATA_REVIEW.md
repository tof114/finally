# Market Data Backend — Comprehensive Code Review

**Date**: 2026-05-03
**Reviewer**: Claude Code
**Scope**: All files under `backend/app/market_data/`, `backend/app/main.py`, `backend/app/config.py`, and `backend/tests/market_data/`
**Status**: ⚠️ **APPROVED WITH FIXES REQUIRED** — implementation is solid; route tests are broken by a transitive `httpx` upgrade and a lifespan-state bug.

---

## 1. Executive Summary

The market data backend implementation is a faithful and clean realization of the design in `planning/MARKET_DATA_DESIGN.md`. Module layout, type signatures, GBM math, SSE wiring, and the provider-abstraction boundary all match the spec. Unit-test coverage is strong (51 tests collected, 48 pass). However:

- **3 of 51 tests fail** — all in `tests/market_data/test_routes.py`, all due to the same root cause: `httpx 0.28.1` removed the `app=` keyword from `AsyncClient`. A lifespan-state bug in the test helper compounds the issue.
- **One small dead-code redundancy** in `app/main.py` (a re-import that does nothing) — cosmetic.
- **One env-var inconsistency**: `STEP_SECONDS` is read at *module-import* time in `simulator.py` rather than via `config.py`.
- **One latent bug** in the Massive parser when the upstream returns a snapshot for a ticker not currently in the watchlist (no functional regression, but the cache may grow unboundedly over a long session).

Everything else is in great shape and ready to build on.

---

## 2. Test Run Results

```
tests run:    51
passed:       48
failed:        3   (all in test_routes.py)
warnings:      0
duration:    ~43s
```

### 2.1 Failing tests

```
FAILED tests/market_data/test_routes.py::test_snapshot_burst_on_connect
FAILED tests/market_data/test_routes.py::test_sse_event_format
FAILED tests/market_data/test_routes.py::test_response_headers
```

All three fail with:

```
TypeError: AsyncClient.__init__() got an unexpected keyword argument 'app'
```

### 2.2 Root cause #1 — `httpx` 0.28+ API break

`httpx.AsyncClient(app=app, base_url=...)` was removed in httpx 0.28.0; the in-process ASGI transport now requires explicit construction:

```python
from httpx import AsyncClient, ASGITransport
async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
    ...
```

`backend/pyproject.toml` pins `httpx>=0.27.0`, which resolves to `0.28.1` in `uv sync` today. Either pin tighter (`httpx>=0.27,<0.28`) or — preferably — update the test helper to use `ASGITransport`.

### 2.3 Root cause #2 — lifespan never runs in `_make_app`

After fixing the `app=` issue, the tests still fail because `_make_app` puts `service` into `app.state.market_data` **inside** a `lifespan` coroutine, and `httpx.ASGITransport` does **not** drive the ASGI lifespan protocol. The route then raises `AttributeError: 'State' object has no attribute 'market_data'`. Verified empirically.

Two equivalent fixes:

```python
# Option A — set state directly, no lifespan needed:
def _make_app(service):
    app = FastAPI()
    app.state.market_data = service
    app.include_router(router)
    return app
```

```python
# Option B — drive the lifespan with asgi-lifespan (extra dep):
from asgi_lifespan import LifespanManager
async with LifespanManager(app):
    async with AsyncClient(transport=ASGITransport(app=app), ...) as client:
        ...
```

Recommendation: **Option A** — these tests don't need the lifespan; the service is already started/stopped explicitly in each test.

### 2.4 What passes

Every other module is well-covered and green:
- **Cache** (`test_cache.py`, 9/9) — direction logic, snapshot independence, multi-ticker independence.
- **Broadcaster** (`test_broadcaster.py`, 6/6) — fan-out, drop-oldest, subscriber lifecycle.
- **Simulator** (`test_simulator.py`, 10/10) — determinism, hot-reload, default-spec fallback, positivity floor.
- **Massive** (`test_massive.py`, 16/16) — parsing fallback chain, URL canonicalization, error swallowing, conformance with simulator.
- **Service** (`test_service.py`, 3/3) — cache population, broadcast wiring, idempotent stop.
- **Types** (`test_types.py`, 4/4) — SSE serialization, rounding, tz-awareness.

---

## 3. Implementation Review (file by file)

### 3.1 `app/market_data/types.py` ✅

Matches the spec exactly. `Tick` is `frozen=True, slots=True` for memory efficiency and immutability. `to_sse_dict` keeps SSE payload compact. `utcnow()` correctly returns tz-aware UTC.

Minor nit: `to_sse_dict` returns `dict` rather than a typed `TypedDict`. Not a defect — just a missed opportunity for editor autocomplete on the JS-style short keys.

### 3.2 `app/market_data/cache.py` ✅

Faithful to the design. Lock semantics are correct: the guarded section is the read-modify-write (`get → derive direction → put`), which is the *logical* atomicity the design called out. The lock is uncontended in practice (single producer task).

Observation (not a defect): the cache has no eviction policy. Over a long-running session, if the watchlist (or LLM-driven additions) churns through many symbols, memory grows monotonically. For a single-user demo this is fine; flag for future consideration.

### 3.3 `app/market_data/broadcaster.py` ✅

Correct fan-out + drop-oldest pattern. `_QUEUE_SIZE=256` ≈ 4s of backlog, matching the design rationale. `subscribe()` is an `asynccontextmanager` so cleanup is automatic on disconnect/exception. Adds a useful `subscriber_count` property (not in design — a welcome addition).

Subtle correctness note: `publish()` snapshots the subscriber set under the lock and then iterates without it. This is the right call — releases the lock during `put_nowait()` so a slow `subscribe()/unsubscribe()` doesn't stall the producer. The window where a freshly-added subscriber misses one published tick is negligible (worst case it gets the very next one, ~500ms later, plus the snapshot burst on `routes.py` connect).

### 3.4 `app/market_data/provider.py` ✅

ABC + `WatchlistSource` Protocol exactly as specified. The Protocol enables the trivially-substitutable `FakeWatchlist` used throughout the test suite. Good separation.

### 3.5 `app/market_data/simulator.py` ✅ (one inconsistency)

GBM math is correct: `S_{t+dt} = S_t · exp((μ − ½σ²)dt + σ√dt · Z)`. Sector-correlated normal `Z = ρ·F + √(1−ρ²)·ε` with one factor per sector — visually believable, no linear-algebra dependency, exactly as specified.

Other strengths:
- Hot-reload via `current_tickers()` per loop iteration — verified by `test_picks_up_added_ticker`.
- Removed-then-re-added tickers resume from last simulated price (verified behavior, intended).
- Hard floor at `0.01` prevents pathological collapse.
- Event probability and magnitude match spec values.

**Issue (minor)**: `STEP_SECONDS` is read directly from `os.environ` at module-import time (line 41), not via `config.py`. The design (§12) explicitly says: *"`config.py` reads these once at startup; nothing else touches `os.environ`"*. Two consequences:

1. `config.SIM_STEP_SECONDS` is defined but never consumed by the simulator.
2. Integration tests that try to override step cadence at runtime cannot, because the value is frozen at import.

Suggested fix: import `SIM_STEP_SECONDS` from `app.config` (or accept it as a constructor parameter to enable per-test overrides).

### 3.6 `app/market_data/massive.py` ✅ (one latent issue)

Implementation matches the design closely:
- Single HTTP call per cycle (good for free-tier rate limits).
- Symbol normalization: `sorted(set(t.upper() for t in tickers))` — the design only said `sorted(set(...))`; **the `.upper()` is a real improvement** because Polygon expects uppercase.
- Fallback chain `lastTrade.p → day.c → prevDay.c` correctly implemented.
- `httpx.HTTPError` (the broad base class) is caught, covering connect/read/timeout/HTTPStatusError variants.
- Constructor injection of `client` with `_owns_client` ownership flag — clean lifecycle.
- `MASSIVE_TIER` is read from `os.environ` (line 46) — same `config.py` inconsistency as `simulator.py`.

**Latent issue**: `_parse()` accepts every ticker the Polygon endpoint returns. In practice Polygon will only return what was asked for, so this is benign today. But because the watchlist may shrink between the request and the response, the cache can pick up *stale* entries for tickers no longer in the watchlist (those entries then never refresh). Not a regression vs. simulator (which has the same "removed tickers keep last value" property — that's intended), but worth a one-line filter against the watchlist if the cache size ever becomes a concern.

**Subtle bug**: the fallback chain uses Python truthiness:

```python
price = (
    (entry.get("lastTrade") or {}).get("p")
    or (entry.get("day") or {}).get("c")
    or (entry.get("prevDay") or {}).get("c")
)
```

`0` and `0.0` are falsy in Python, so a real `lastTrade.p == 0` will silently fall through to `day.c`. The subsequent `if price is None or price <= 0` filter discards zero prices anyway, so the **net behavior is correct** (a zero last-trade is treated as missing), and the existing `test_parse_fallback_to_prevday` verifies the case `day.c=0 → prevDay.c=30.0`. Worth a comment so a future reader understands the implicit "0 means missing" semantics.

### 3.7 `app/market_data/service.py` ✅

Composition root. `_supervised_run` adds the exponential backoff wrapper the design requested. Good handling of `CancelledError` (re-raised, not swallowed). `stop()` is idempotent (checked by `test_service_stop_is_idempotent`).

One subtle property: on a clean exit from `stream()` (which the design says shouldn't normally happen) the supervisor `break`s without restarting. Good — prevents a tight loop if a provider is misbehaved enough to return.

`__aexit__` is called with `(None, None, None)` regardless of whether the producer crashed — this is fine for both implementations since neither uses the exception info.

### 3.8 `app/market_data/factory.py` ✅

Three-line function, exactly as designed. Reads `MASSIVE_API_KEY` via `os.environ` directly rather than via `config.py` — same minor inconsistency.

### 3.9 `app/market_data/routes.py` ✅

Implements the `/api/stream/prices` SSE endpoint per spec. Snapshot burst → broadcaster subscription → heartbeat-on-timeout. `request.is_disconnected()` polled each loop iteration so client disconnect is detected promptly. Headers (`Cache-Control: no-cache`, `X-Accel-Buffering: no`, `Connection: keep-alive`) all correct.

Minor robustness note: `await request.is_disconnected()` is checked *before* the `wait_for(queue.get(), …)` call. On a disconnect that arrives while the consumer is waiting on `queue.get()`, the loop won't notice until either a tick arrives (then notices on the next iteration) or 15s passes (heartbeat timeout, then notices). This is fine in practice but is a 15s upper bound on cleanup latency for an idle, disconnected client. Not worth fixing unless many clients churn.

### 3.10 `app/main.py` ⚠️ (cosmetic)

The lifespan has a redundant inner import:

```python
from .market_data.simulator import SimulatorProvider
from .market_data.factory import build_provider as _build  # already imported at top of file
```

`build_provider` is already imported as a top-level symbol; the local re-bind to `_build` accomplishes nothing. Either delete it and use `build_provider(watchlist)`, or move the entire `SIM_RANDOM_SEED` branch into the factory.

The `_SimpleWatchlist` stub is appropriately marked as temporary, and is sensible until the watchlist module lands.

### 3.11 `app/config.py` ✅

Loads `.env` via `python-dotenv`, parses all five env vars at import time. `MASSIVE_API_KEY` strip + truthiness logic matches the design. `SIM_RANDOM_SEED` is correctly parsed as `int | None`.

The unfortunate gap is that **nothing in `market_data/` actually imports from `config.py`**. The `MASSIVE_API_KEY`, `MASSIVE_TIER`, and `SIM_STEP_SECONDS` values are re-read directly from `os.environ` in `factory.py`, `massive.py`, and `simulator.py`. Net result: the file exists but is mostly aspirational. Either centralize on `config.py` (preferred — matches the design) or delete the redundant fields.

---

## 4. Test Suite Review

### 4.1 Coverage assessment

| Module | Tests | Notable strengths | Gaps |
|---|---|---|---|
| types | 4 | tz-awareness, rounding | none |
| cache | 9 | direction logic, copy independence, multi-ticker | no concurrent-update test (would exercise the lock) |
| broadcaster | 6 | drop-oldest, subscriber lifecycle | no test that a subscriber added *during* `publish()` is safe |
| simulator | 10 | determinism, hot-reload, conformance | no explicit assertion that sector correlation actually exists |
| massive | 16 | mock transport, fallback chain, conformance | no test for `httpx.RequestError` (only HTTP-status errors) |
| service | 3 | cache fill, broadcast, idempotent stop | no test of `_supervised_run` backoff behavior |
| routes | 3 | snapshot burst, headers, event format | **all three currently broken** |

The conformance test parametrized over both simulator and Massive is a particularly nice touch — exactly what §13.2 of the design called for.

### 4.2 Style observations

- Tests prefer `gen.__anext__()` rather than `anext(gen)`. Both work; `anext` is the modern (Python 3.10+) idiom and is slightly more readable.
- `FakeWatchlist` is duplicated across four test files. Worth extracting to `tests/market_data/conftest.py` as a fixture if more test files appear.
- `tests/conftest.py` modifies `sys.path`. Reasonable for now, but configuring `pytest`'s `pythonpath` in `pyproject.toml` (`tool.pytest.ini_options.pythonpath = ["."]`) is cleaner.

### 4.3 The drop-oldest test isn't quite testing what it claims

`test_drop_oldest_when_queue_full` fills the queue, publishes one more, then drains. It asserts `0.0 not in prices` — but prices `1.0..255.0` plus `9999.0` are all present, which is consistent with "the very first (price=0.0) was evicted to make room". Good. However, the test would silently pass even if the broadcaster always evicted the *most recent* tick (because the overflow tick is published last and the assertion only checks that `0.0` is gone). Tightening the assertion to `assert prices[0] == 1.0` (i.e., the new oldest is `1.0`, the originally-second tick) would catch eviction-from-the-wrong-end bugs.

---

## 5. Conformance with `MARKET_DATA_DESIGN.md`

| Section | Status | Notes |
|---|---|---|
| §2 Module Layout | ✅ | All 9 files present with the specified names. |
| §3 Shared Types | ✅ | `Tick`, `Direction`, `utcnow` exactly as spec. |
| §4 PriceCache | ✅ | API and lock semantics match. |
| §5 Broadcaster | ✅ | Adds `subscriber_count` (helpful extra). |
| §6 Provider ABC | ✅ | Matches; Protocol used for `WatchlistSource`. |
| §7 Simulator | ✅ | Math, SPECS table, event probability — all match. ⚠ STEP_SECONDS env-read bypasses config.py. |
| §8 Massive | ✅ | Matches; adds `.upper()` on symbol normalization (improvement). ⚠ MASSIVE_TIER env-read bypasses config.py. |
| §9 Service | ✅ | `_supervised_run` implements the §9 supervisor with backoff. |
| §10 Wiring | ⚠ | Lifespan slightly overcomplicated by inline `_build` rename; otherwise correct. |
| §11 WatchlistSource | ✅ | `_SimpleWatchlist` stub satisfies Protocol. |
| §12 Configuration | ⚠ | `config.py` exists but is bypassed by 3 of the 5 env vars it defines. |
| §13 Tests | ✅ (mostly) | All test files specified in §13.1 exist, plus a `test_service.py` and `test_types.py` not specified. Routes tests are broken (see §2). |

---

## 6. Risks & Recommendations

### 6.1 Must-fix before downstream modules build on this

1. **Repair the route tests.** Replace `_make_app` with the lifespan-free version (Option A in §2.3) and switch `httpx.AsyncClient(app=...)` to `AsyncClient(transport=ASGITransport(app=app), ...)`. Three call sites.
2. **Pin `httpx` in `pyproject.toml`.** Either `httpx>=0.27,<0.28` (deferred decision) or update tests for the new API and bump the floor (`httpx>=0.28`).

### 6.2 Should-fix soon

3. **Centralize env reads in `config.py`.** `simulator.STEP_SECONDS`, `massive.TIER_INTERVALS` lookup, and `factory.build_provider` should all import from `config.py` so the file is the single source of truth.
4. **Remove the inner re-import in `app/main.py`** — use the top-level `build_provider`.

### 6.3 Nice-to-have

5. **Extract `FakeWatchlist` to `tests/market_data/conftest.py`** as a fixture.
6. **Add a comment to the Massive parser** explaining that `0` is intentionally treated as missing (relies on Python falsiness).
7. **Tighten `test_drop_oldest_when_queue_full`** to assert the new head, not just the absence of the evicted item.
8. **Add a `pyproject` entry for `pythonpath`** so `tests/conftest.py` no longer needs to mutate `sys.path`.

---

## 7. Final Verdict

The implementation is **well-structured, faithful to the design, and tested broadly**. The producer/cache/broadcaster/SSE pipeline is correctly wired. The single biggest issue — the failing route tests — is a 5-minute fix unrelated to the design's correctness; it stems from an `httpx` upgrade and a lifespan-state bug in test scaffolding, not from logic in the production code.

After the fixes in §6.1, this module is ready to act as the foundation for the portfolio module, the watchlist module, and the LLM chat layer.

---

**Reviewer**: Claude Code
**Date**: 2026-05-03
**Status**: ⚠️ **APPROVED WITH FIXES REQUIRED** (route-test repair, `httpx` pin)
