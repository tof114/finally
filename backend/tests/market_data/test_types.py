from __future__ import annotations

from datetime import timezone

from app.market_data.types import Tick, utcnow


def test_tick_to_sse_dict_all_fields():
    ts = utcnow()
    tick = Tick(ticker="AAPL", price=192.3456, prev_price=191.0, timestamp=ts, direction="up")
    d = tick.to_sse_dict()
    assert d["t"] == "AAPL"
    assert d["p"] == 192.3456
    assert d["pp"] == 191.0
    assert d["d"] == "up"
    assert d["ts"] == ts.isoformat()


def test_tick_to_sse_dict_no_prev_price():
    tick = Tick(ticker="TSLA", price=250.0, prev_price=None, timestamp=utcnow(), direction="flat")
    d = tick.to_sse_dict()
    assert d["pp"] is None


def test_tick_rounds_to_4_decimal_places():
    tick = Tick(ticker="X", price=1.23456789, prev_price=1.11111111, timestamp=utcnow(), direction="up")
    d = tick.to_sse_dict()
    assert d["p"] == 1.2346
    assert d["pp"] == 1.1111


def test_utcnow_is_tz_aware():
    ts = utcnow()
    assert ts.tzinfo is not None
    assert ts.tzinfo == timezone.utc
