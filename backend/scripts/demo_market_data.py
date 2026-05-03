"""Live TUI demo of the market data pipeline.

Spins up the SimulatorProvider + PriceCache + Broadcaster end-to-end and
renders a rich terminal UI: live price table with sparklines, a recent
events panel for notable moves, and a header with elapsed/remaining time.

Same code path as production minus the HTTP layer.

Run from backend/:
    uv run python scripts/demo_market_data.py
    uv run python scripts/demo_market_data.py --duration 30
    uv run python scripts/demo_market_data.py --plain  # no Rich, line-per-tick

Press Ctrl+C to stop.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from collections import deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.market_data.service import MarketDataService
from app.market_data.simulator import SimulatorProvider
from app.market_data.types import Tick

DEFAULT_DURATION = 60.0
SPARK_HISTORY = 40
EVENTS_HISTORY = 12
NOTABLE_MOVE_PCT = 2.0  # show in events panel when |move| >= this percent

SPARK_CHARS = "▁▂▃▄▅▆▇█"


class DemoWatchlist:
    TICKERS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA",
               "NVDA", "META", "JPM", "V", "NFLX"]

    async def current_tickers(self) -> list[str]:
        return list(self.TICKERS)


def sparkline(values: list[float]) -> str:
    if len(values) < 2:
        return ""
    lo, hi = min(values), max(values)
    if hi - lo < 1e-9:
        return SPARK_CHARS[len(SPARK_CHARS) // 2] * len(values)
    span = hi - lo
    n = len(SPARK_CHARS) - 1
    return "".join(SPARK_CHARS[int((v - lo) / span * n)] for v in values)


def run_rich(duration: float) -> int:
    from rich.console import Console
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text

    console = Console()
    history: dict[str, deque[float]] = {
        t: deque(maxlen=SPARK_HISTORY) for t in DemoWatchlist.TICKERS
    }
    seed: dict[str, float] = {}
    latest: dict[str, Tick] = {}
    events: deque[tuple[str, str, float]] = deque(maxlen=EVENTS_HISTORY)
    started = time.monotonic()
    tick_count = 0

    def header() -> Panel:
        elapsed = time.monotonic() - started
        remaining = max(0.0, duration - elapsed)
        line = Text.assemble(
            ("FinAlly Market Data Simulator", "bold yellow"),
            ("   │   ", "dim"),
            (f"{elapsed:6.1f}s elapsed", "cyan"),
            ("   │   ", "dim"),
            (f"{remaining:5.1f}s remaining", "magenta"),
            ("   │   ", "dim"),
            (f"{len(latest)} tickers", "green"),
            ("   │   ", "dim"),
            ("Ctrl+C to exit", "bold"),
        )
        return Panel(line, border_style="bright_blue", padding=(0, 1))

    def price_table() -> Panel:
        t = Table(
            show_header=True,
            header_style="bold bright_white on grey15",
            border_style="grey30",
            row_styles=["", "on grey7"],
            expand=True,
        )
        t.add_column("Ticker", style="bold", width=8)
        t.add_column("Price", justify="right", width=12)
        t.add_column("Change", justify="right", width=11)
        t.add_column("Chg %", justify="right", width=9)
        t.add_column("Dir", justify="center", width=5)
        t.add_column("Sparkline", style="bright_cyan")

        for sym in DemoWatchlist.TICKERS:
            tk = latest.get(sym)
            if tk is None:
                t.add_row(sym, "…", "…", "…", "…", "")
                continue
            prev = tk.prev_price if tk.prev_price is not None else tk.price
            delta = tk.price - prev
            seed_p = seed.get(sym, tk.price)
            chg_pct = (tk.price / seed_p - 1.0) * 100.0 if seed_p else 0.0
            color = ("bright_green" if tk.direction == "up"
                     else "bright_red" if tk.direction == "down"
                     else "white")
            arrow = "▲" if tk.direction == "up" else "▼" if tk.direction == "down" else "·"
            t.add_row(
                sym,
                Text(f"${tk.price:>9.2f}", style=color),
                Text(f"{delta:+8.4f}", style=color),
                Text(f"{chg_pct:+6.2f}%", style=color),
                Text(arrow, style=color),
                sparkline(list(history[sym])),
            )
        return Panel(t, title="[bold]Live Prices[/bold]",
                     border_style="bright_blue", padding=(0, 1))

    def events_panel() -> Panel:
        if not events:
            body: Text = Text(
                f"Watching for notable moves (≥{NOTABLE_MOVE_PCT:.0f}% from session open)…",
                style="dim italic",
            )
        else:
            body = Text()
            for sym, direction, pct in reversed(events):
                color = "bright_green" if direction == "up" else "bright_red"
                arrow = "▲" if direction == "up" else "▼"
                body.append(f"  {arrow}  ", style=color)
                body.append(f"{sym:<6} ", style="bold")
                body.append(f"{pct:+.2f}%\n", style=color)
        return Panel(body, title="[bold]Recent Events[/bold]",
                     border_style="bright_blue", padding=(0, 1),
                     subtitle=f"[dim]ticks: {tick_count}[/dim]")

    layout = Layout()
    layout.split_column(
        Layout(header(), name="header", size=3),
        Layout(price_table(), name="prices"),
        Layout(events_panel(), name="events", size=EVENTS_HISTORY + 4),
    )

    def refresh() -> None:
        layout["header"].update(header())
        layout["prices"].update(price_table())
        layout["events"].update(events_panel())

    async def producer() -> None:
        nonlocal tick_count
        watchlist = DemoWatchlist()
        provider = SimulatorProvider(watchlist)
        service = MarketDataService(provider)
        await service.start()
        try:
            async with service.broadcaster.subscribe() as queue:
                deadline = asyncio.get_event_loop().time() + duration
                with Live(layout, console=console, refresh_per_second=8,
                          screen=True):
                    while True:
                        remaining = deadline - asyncio.get_event_loop().time()
                        if remaining <= 0:
                            break
                        try:
                            tk = await asyncio.wait_for(queue.get(), timeout=remaining)
                        except asyncio.TimeoutError:
                            break
                        latest[tk.ticker] = tk
                        history[tk.ticker].append(tk.price)
                        seed.setdefault(tk.ticker, tk.price)
                        tick_count += 1

                        seed_p = seed[tk.ticker]
                        if seed_p:
                            pct = (tk.price / seed_p - 1.0) * 100.0
                            if abs(pct) >= NOTABLE_MOVE_PCT:
                                last = next(
                                    (e for e in reversed(events) if e[0] == tk.ticker),
                                    None,
                                )
                                if last is None or abs(last[2] - pct) >= 0.5:
                                    events.append(
                                        (tk.ticker,
                                         "up" if pct > 0 else "down",
                                         pct)
                                    )
                        refresh()
        finally:
            await service.stop()

    try:
        asyncio.run(producer())
    except KeyboardInterrupt:
        pass

    console.print(
        f"\n[bold]Done.[/bold] Streamed [cyan]{tick_count}[/cyan] ticks "
        f"across [green]{len(latest)}[/green] tickers."
    )
    return 0


def run_plain(duration: float | None) -> int:
    async def producer() -> int:
        watchlist = DemoWatchlist()
        provider = SimulatorProvider(watchlist)
        service = MarketDataService(provider)
        count = 0
        await service.start()
        try:
            async with service.broadcaster.subscribe() as queue:
                loop = asyncio.get_event_loop()
                deadline = (loop.time() + duration) if duration else None
                while True:
                    timeout = (deadline - loop.time()) if deadline else None
                    if timeout is not None and timeout <= 0:
                        break
                    try:
                        tk = (await asyncio.wait_for(queue.get(), timeout=timeout)
                              if timeout else await queue.get())
                    except asyncio.TimeoutError:
                        break
                    prev = tk.prev_price if tk.prev_price is not None else tk.price
                    delta = tk.price - prev
                    print(
                        f"{tk.timestamp.isoformat(timespec='milliseconds')}  "
                        f"{tk.ticker:<6} {tk.price:>10.2f}  "
                        f"Δ {delta:>+8.4f}  {tk.direction}",
                        flush=True,
                    )
                    count += 1
        finally:
            await service.stop()
        return count

    try:
        count = asyncio.run(producer())
    except KeyboardInterrupt:
        count = 0
    print(f"Done. {count} ticks.")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--duration", type=float, default=DEFAULT_DURATION,
                   help=f"Stop after N seconds (default: {DEFAULT_DURATION:g})")
    p.add_argument("--plain", action="store_true",
                   help="Use plain stream output (no Rich TUI)")
    args = p.parse_args()

    if args.plain or not sys.stdout.isatty():
        return run_plain(args.duration)
    return run_rich(args.duration)


if __name__ == "__main__":
    sys.exit(main())
