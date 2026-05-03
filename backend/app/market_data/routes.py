from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

router = APIRouter(prefix="/api/stream", tags=["market-data"])

# Heartbeat keeps proxies / load balancers from idle-timing out the SSE
# connection during low-volume periods (e.g., empty watchlist).
HEARTBEAT_SECONDS = 15


def _sse(event: str, data: dict) -> bytes:
    return (
        f"event: {event}\ndata: {json.dumps(data, separators=(',', ':'))}\n\n"
    ).encode()


@router.get("/prices")
async def stream_prices(request: Request) -> StreamingResponse:
    """SSE endpoint: sends a snapshot burst, then live price updates."""
    service = request.app.state.market_data

    async def gen():
        # 1. Snapshot burst so the client has current prices immediately.
        snap = await service.cache.snapshot()
        for tick in snap.values():
            yield _sse("price", tick.to_sse_dict())

        # 2. Live updates via the broadcaster. Starlette cancels this generator
        # when the client disconnects, so an explicit disconnect check is not
        # needed — CancelledError unwinds through the broadcaster ctx manager.
        async with service.broadcaster.subscribe() as queue:
            while True:
                try:
                    tick = await asyncio.wait_for(
                        queue.get(), timeout=HEARTBEAT_SECONDS
                    )
                    yield _sse("price", tick.to_sse_dict())
                except asyncio.TimeoutError:
                    yield b": heartbeat\n\n"  # SSE comment, ignored by client

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # disable nginx buffering if proxied
            "Connection": "keep-alive",
        },
    )
