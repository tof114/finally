from __future__ import annotations

import asyncio
import socket
from typing import AsyncIterator

import pytest_asyncio
import uvicorn
from fastapi import FastAPI


class FakeWatchlist:
    """Minimal WatchlistSource for tests: mutate `_t` to drive add/remove."""

    def __init__(self, tickers: list[str]) -> None:
        self._t = list(tickers)

    async def current_tickers(self) -> list[str]:
        return list(self._t)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest_asyncio.fixture
async def live_server() -> AsyncIterator[callable]:
    """Yield a callable that starts a uvicorn server for a given FastAPI app
    and returns its base URL. Server is shut down on test exit.

    httpx.ASGITransport buffers infinite streaming responses, so SSE routes
    must be tested against a real HTTP server.
    """
    server: uvicorn.Server | None = None
    task: asyncio.Task | None = None

    async def _start(app: FastAPI) -> str:
        nonlocal server, task
        port = _free_port()
        config = uvicorn.Config(
            app, host="127.0.0.1", port=port, log_level="warning", lifespan="off"
        )
        server = uvicorn.Server(config)
        task = asyncio.create_task(server.serve())
        # Wait for startup (server.started is set after the listen socket is up).
        for _ in range(200):
            if server.started:
                break
            await asyncio.sleep(0.02)
        else:
            raise RuntimeError("uvicorn did not start within 4s")
        return f"http://127.0.0.1:{port}"

    try:
        yield _start
    finally:
        if server is not None:
            server.should_exit = True
        if task is not None:
            try:
                await asyncio.wait_for(task, timeout=5.0)
            except asyncio.TimeoutError:
                task.cancel()
