"""Tiny demo of the market data pipeline.

Spins up the SimulatorProvider + PriceCache + Broadcaster end-to-end and
prints a live, color-coded table to the terminal so you can watch ticks
arrive. No server, no SSE — same code path as production minus the HTTP
layer.

Run from backend/:
    uv run python scripts/demo_market_data.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.market_data.service import MarketDataService
from app.market_data.simulator import SimulatorProvider
from app.market_data.types import Tick

GREEN = "\033[32m"
RED = "\033[31m"
DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"
CLEAR = "\033[2J\033[H"

DURATION_SECONDS = 10


class DemoWatchlist:
    TICKERS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA",
               "NVDA", "META", "JPM", "V", "NFLX"]

    async def current_tickers(self) -> list[str]:
        return list(self.TICKERS)


def render(latest: dict[str, Tick], tick_count: int) -> str:
    lines = [
        f"{BOLD}FinAlly market data — live simulator demo{RESET}",
        f"{DIM}ticks received: {tick_count}{RESET}",
        "",
        f"  {'TICKER':<8} {'PRICE':>10} {'PREV':>10} {'Δ':>9}  DIR",
        f"  {'-' * 8} {'-' * 10} {'-' * 10} {'-' * 9}  ---",
    ]
    for sym in DemoWatchlist.TICKERS:
        t = latest.get(sym)
        if t is None:
            lines.append(f"  {sym:<8} {'…':>10} {'…':>10} {'…':>9}  ...")
            continue
        prev = t.prev_price if t.prev_price is not None else t.price
        delta = t.price - prev
        color = GREEN if t.direction == "up" else RED if t.direction == "down" else DIM
        arrow = "▲" if t.direction == "up" else "▼" if t.direction == "down" else "·"
        lines.append(
            f"  {sym:<8} {color}{t.price:>10.2f}{RESET} "
            f"{DIM}{prev:>10.2f}{RESET} "
            f"{color}{delta:>+9.4f}{RESET}  {color}{arrow} {t.direction}{RESET}"
        )
    return "\n".join(lines)


async def main() -> None:
    watchlist = DemoWatchlist()
    provider = SimulatorProvider(watchlist)
    service = MarketDataService(provider)

    latest: dict[str, Tick] = {}
    tick_count = 0

    await service.start()
    try:
        async with service.broadcaster.subscribe() as queue:
            print(CLEAR, end="")
            print(render(latest, tick_count))
            deadline = asyncio.get_event_loop().time() + DURATION_SECONDS
            while True:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    break
                try:
                    tick = await asyncio.wait_for(queue.get(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                latest[tick.ticker] = tick
                tick_count += 1
                print(CLEAR, end="")
                print(render(latest, tick_count))
    finally:
        await service.stop()

    print()
    print(f"{BOLD}Done.{RESET} Streamed {tick_count} ticks across "
          f"{len(latest)} tickers in {DURATION_SECONDS}s.")


if __name__ == "__main__":
    asyncio.run(main())
