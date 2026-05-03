"""Live demo of the market data pipeline.

Spins up the SimulatorProvider + PriceCache + Broadcaster end-to-end and
prints ticks to the terminal so you can watch the pipeline work. Same
code path as production minus the HTTP layer.

Two display modes, picked automatically:
- TTY (interactive terminal): in-place updating table, color-coded.
- Not a TTY (piped, redirected, captured): one line per tick, stream-style.

Run from the project root or backend/:
    uv run --project backend python backend/scripts/demo_market_data.py
    # or
    cd backend && uv run python scripts/demo_market_data.py

Press Ctrl+C to stop.
"""

from __future__ import annotations

import argparse
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


class DemoWatchlist:
    TICKERS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA",
               "NVDA", "META", "JPM", "V", "NFLX"]

    async def current_tickers(self) -> list[str]:
        return list(self.TICKERS)


def color_for(direction: str) -> str:
    return GREEN if direction == "up" else RED if direction == "down" else DIM


def arrow_for(direction: str) -> str:
    return "▲" if direction == "up" else "▼" if direction == "down" else "·"


def render_table(latest: dict[str, Tick], tick_count: int, use_color: bool) -> str:
    g = GREEN if use_color else ""
    r = RED if use_color else ""
    d = DIM if use_color else ""
    b = BOLD if use_color else ""
    x = RESET if use_color else ""

    lines = [
        f"{b}FinAlly market data — live simulator demo{x}",
        f"{d}ticks received: {tick_count}   (Ctrl+C to stop){x}",
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
        c = (g if t.direction == "up" else r if t.direction == "down" else d) if use_color else ""
        lines.append(
            f"  {sym:<8} {c}{t.price:>10.2f}{x} "
            f"{d}{prev:>10.2f}{x} "
            f"{c}{delta:>+9.4f}{x}  {c}{arrow_for(t.direction)} {t.direction}{x}"
        )
    return "\n".join(lines)


def render_stream_line(t: Tick, use_color: bool) -> str:
    if use_color:
        c = color_for(t.direction)
        x = RESET
    else:
        c = ""
        x = ""
    prev = t.prev_price if t.prev_price is not None else t.price
    delta = t.price - prev
    return (
        f"{t.timestamp.isoformat(timespec='milliseconds')}  "
        f"{c}{t.ticker:<6}{x} "
        f"{c}{t.price:>10.2f}{x}  "
        f"Δ {c}{delta:>+8.4f}{x}  "
        f"{c}{arrow_for(t.direction)} {t.direction}{x}"
    )


async def run(duration: float | None, force_stream: bool) -> None:
    watchlist = DemoWatchlist()
    provider = SimulatorProvider(watchlist)
    service = MarketDataService(provider)

    use_tty = sys.stdout.isatty() and not force_stream
    use_color = sys.stdout.isatty()

    latest: dict[str, Tick] = {}
    tick_count = 0

    await service.start()
    try:
        async with service.broadcaster.subscribe() as queue:
            if use_tty:
                print(CLEAR, end="", flush=True)
                print(render_table(latest, tick_count, use_color), flush=True)

            loop = asyncio.get_event_loop()
            deadline = (loop.time() + duration) if duration is not None else None

            while True:
                if deadline is not None:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        break
                    timeout = remaining
                else:
                    timeout = None

                try:
                    tick = await (asyncio.wait_for(queue.get(), timeout=timeout)
                                  if timeout is not None else queue.get())
                except asyncio.TimeoutError:
                    break

                latest[tick.ticker] = tick
                tick_count += 1

                if use_tty:
                    print(CLEAR, end="", flush=True)
                    print(render_table(latest, tick_count, use_color), flush=True)
                else:
                    print(render_stream_line(tick, use_color), flush=True)
    finally:
        await service.stop()

    if use_tty:
        print()
    print(f"Done. Streamed {tick_count} ticks across {len(latest)} tickers.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--duration", type=float, default=None,
        help="Stop after N seconds (default: run until Ctrl+C)",
    )
    parser.add_argument(
        "--stream", action="store_true",
        help="Force stream-style output (one line per tick) even on a TTY",
    )
    args = parser.parse_args()
    try:
        asyncio.run(run(args.duration, args.stream))
    except KeyboardInterrupt:
        print()


if __name__ == "__main__":
    main()
