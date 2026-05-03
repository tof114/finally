# Backend — FinAlly

Self-contained FastAPI app managed with `uv`. Serves all `/api/*` routes plus the static Next.js export.

## Layout

```
backend/
├── pyproject.toml
├── uv.lock
├── app/
│   ├── main.py              # FastAPI + lifespan wiring
│   ├── config.py            # env var parsing (single source of truth)
│   ├── market_data/         # implemented — see planning/MARKET_DATA_SUMMARY.md
│   └── market/              # legacy / WIP — not the production module
└── tests/
    ├── conftest.py
    └── market_data/         # 51 tests, all passing
```

## Running

```bash
uv sync                              # install deps from uv.lock
uv run uvicorn app.main:app --reload # dev server on :8000
uv run pytest                        # run all tests
uv run pytest tests/market_data      # subset
```

## Conventions

- **Python 3.12+**: use `|` unions, `from __future__ import annotations`, dataclasses with `slots=True` where it fits.
- **Async only**: no threads. Long-lived resources (HTTP clients, background tasks) live behind `async with` / lifespan.
- **Env vars are read once** in `app/config.py`. New code must import from there — never call `os.environ` directly elsewhere.
- **Module boundaries**: cross-module dependencies go through `Protocol`s (e.g. `WatchlistSource`), not direct imports. Keeps tests trivial to fake.
- **Tests do not hit the network**: use `httpx.MockTransport` for HTTP, seeded `random.Random(...)` for the simulator.
- **No comments restating code.** Comment only non-obvious *why* (a constraint, an invariant, a workaround).

## Key references

- `planning/PLAN.md` — overall project spec (the contract).
- `planning/MARKET_DATA_SUMMARY.md` — current state of the market data subsystem.
- `planning/archive/` — historical design + review docs (deeper context).

## Status

| Module       | Status         |
| ------------ | -------------- |
| market_data  | ✅ implemented |
| portfolio    | ⏳ to do       |
| watchlist    | ⏳ to do       |
| chat (LLM)   | ⏳ to do       |
| db / schema  | ⏳ to do       |

## When adding a new module

1. Create `app/<module>/` with the same shape as `market_data/` (types, service, routes, factory).
2. Expose only what other modules need; depend on others via `Protocol`s.
3. Wire it into `app/main.py` lifespan.
4. Add tests under `tests/<module>/`. Aim for fakes over mocks — easier to read.
