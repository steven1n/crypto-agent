import json
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from exchange.base import StreamLifecycleEvent, StreamLifecycleKind
from exchange.binance.market_ws import (
    BinanceMarketWebSocket,
    WebSocketConnection,
    calculate_backoff,
    normalize_binance_ws_payload,
)
from market.events import (
    AggressorSide,
    BestBidAskEvent,
    BookUpdateEvent,
    NormalizedMarketEvent,
    TradeEvent,
    TradeSource,
)
from market.normalization import ReceiveContext

EVENT_TIME = 1_725_000_000_000


def agg_trade() -> dict[str, object]:
    return {
        "e": "aggTrade",
        "E": EVENT_TIME,
        "s": "BTCUSDC",
        "a": 100,
        "p": "60000.10",
        "q": "0.25",
        "f": 200,
        "l": 202,
        "T": EVENT_TIME - 1,
        "m": False,
    }


def individual_trade() -> dict[str, object]:
    return {
        "e": "trade",
        "E": EVENT_TIME,
        "T": EVENT_TIME - 1,
        "s": "BTCUSDC",
        "t": 201,
        "p": "60000.10",
        "q": "0.05",
        "m": True,
    }


def book_ticker() -> dict[str, object]:
    return {
        "e": "bookTicker",
        "E": EVENT_TIME,
        "T": EVENT_TIME - 1,
        "s": "BTCUSDC",
        "u": 300,
        "b": "60000.10",
        "B": "1.25",
        "a": "60000.20",
        "A": "0.75",
    }


def depth_update() -> dict[str, object]:
    return {
        "e": "depthUpdate",
        "E": EVENT_TIME,
        "T": EVENT_TIME - 1,
        "s": "BTCUSDC",
        "U": 301,
        "u": 302,
        "pu": 300,
        "b": [["60000.10", "2.0"]],
        "a": [["60000.30", "0"]],
    }


def receive_context() -> ReceiveContext:
    return ReceiveContext(
        wall_time=datetime.fromtimestamp(EVENT_TIME / 1_000, tz=UTC),
        monotonic_ns=5_000_000_000,
    )


def test_aggregate_trade_is_normalized_with_aggressor_side() -> None:
    event = normalize_binance_ws_payload(agg_trade(), receive_context())

    assert isinstance(event, TradeEvent)
    assert event.price == Decimal("60000.10")
    assert event.quantity == Decimal("0.25")
    assert event.aggressor_side is AggressorSide.BUY
    assert event.estimated_latency_ms == 0


def test_buyer_maker_means_sell_aggressor() -> None:
    payload = agg_trade()
    payload["m"] = True

    event = normalize_binance_ws_payload(payload, receive_context())

    assert isinstance(event, TradeEvent)
    assert event.buyer_is_maker
    assert event.aggressor_side is AggressorSide.SELL


def test_individual_trade_is_normalized_as_a_single_fill_trade_event() -> None:
    event = normalize_binance_ws_payload(individual_trade(), receive_context())

    assert isinstance(event, TradeEvent)
    assert event.source is TradeSource.INDIVIDUAL
    assert event.aggregate_trade_id == event.first_trade_id == event.last_trade_id == 201
    assert event.aggressor_side is AggressorSide.SELL


def test_zero_price_individual_trade_marker_is_rejected() -> None:
    payload = individual_trade()
    payload["p"] = "0"
    payload["q"] = "0"

    with pytest.raises(ValueError, match="marker"):
        normalize_binance_ws_payload(payload, receive_context())


def test_book_ticker_is_normalized() -> None:
    event = normalize_binance_ws_payload(book_ticker(), receive_context())

    assert isinstance(event, BestBidAskEvent)
    assert event.update_id == 300
    assert event.bid_price == Decimal("60000.10")
    assert event.ask_quantity == Decimal("0.75")


def test_depth_update_preserves_sequence_and_decimal_levels() -> None:
    event = normalize_binance_ws_payload(depth_update(), receive_context())

    assert isinstance(event, BookUpdateEvent)
    assert (event.first_update_id, event.final_update_id, event.previous_final_update_id) == (
        301,
        302,
        300,
    )
    assert event.bids == ((Decimal("60000.10"), Decimal("2.0")),)


def test_backoff_is_exponential_and_bounded() -> None:
    assert [calculate_backoff(1.0, 5.0, attempt) for attempt in range(1, 6)] == [
        1.0,
        2.0,
        4.0,
        5.0,
        5.0,
    ]


class FakeWebSocket:
    def __init__(self, messages: list[str | bytes | Exception]) -> None:
        self.messages = iter(messages)
        self.closed = False

    async def recv(self) -> str | bytes:
        item = next(self.messages)
        if isinstance(item, Exception):
            raise item
        return item

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.closed = True


class FakeContext(AbstractAsyncContextManager[WebSocketConnection]):
    def __init__(self, websocket: FakeWebSocket) -> None:
        self.websocket = websocket

    async def __aenter__(self) -> WebSocketConnection:
        return self.websocket

    async def __aexit__(self, *args: object) -> None:
        return None


def stream_message(payload: dict[str, object]) -> str:
    return json.dumps({"stream": "test", "data": payload})


async def test_client_tracks_feed_health_and_stops_cleanly() -> None:
    websocket = FakeWebSocket(
        [
            stream_message(agg_trade()),
            stream_message(book_ticker()),
            stream_message(depth_update()),
        ]
    )
    client = BinanceMarketWebSocket(
        "btcusdc",
        stale_after_seconds=5,
        connect_factory=lambda _: FakeContext(websocket),
    )
    events: list[NormalizedMarketEvent] = []

    async def handler(event: NormalizedMarketEvent) -> None:
        events.append(event)
        if len(events) == 3:
            health = client.health()
            assert health.connected
            assert not health.stale
            await client.stop()

    await client.run(handler)

    assert len(events) == 3
    assert websocket.closed
    assert not client.health().connected


async def test_client_reconnects_after_transport_failure() -> None:
    contexts = iter(
        [
            FakeContext(FakeWebSocket([ConnectionError("network down")])),
            FakeContext(FakeWebSocket([stream_message(agg_trade())])),
        ]
    )
    client = BinanceMarketWebSocket(
        "BTCUSDC",
        reconnect_initial_seconds=0.001,
        reconnect_max_seconds=0.001,
        connect_factory=lambda _: next(contexts),
    )
    received: list[NormalizedMarketEvent] = []

    async def handler(event: NormalizedMarketEvent) -> None:
        received.append(event)
        await client.stop()

    await client.run(handler)

    assert len(received) == 1
    assert client.health().reconnect_count == 1
    assert client.health().last_error is None


async def test_lifecycle_events_identify_connection_generation() -> None:
    websocket = FakeWebSocket([stream_message(agg_trade())])
    client = BinanceMarketWebSocket("BTCUSDC", connect_factory=lambda _: FakeContext(websocket))
    lifecycle: list[StreamLifecycleEvent] = []

    async def handler(event: NormalizedMarketEvent) -> None:
        await client.stop()

    async def lifecycle_handler(event: StreamLifecycleEvent) -> None:
        lifecycle.append(event)

    await client.run(handler, lifecycle_handler=lifecycle_handler)

    assert [event.kind for event in lifecycle] == [
        StreamLifecycleKind.CONNECTED,
        StreamLifecycleKind.DISCONNECTED,
    ]
    assert {event.connection_generation for event in lifecycle} == {1}


def test_stream_url_contains_only_public_market_streams() -> None:
    client = BinanceMarketWebSocket("BTCUSDC")

    assert client.stream_url == (
        "wss://fstream.binance.com/stream?streams="
        "btcusdc@trade/btcusdc@bookTicker/btcusdc@depth@100ms"
    )
    assert "listenKey" not in client.stream_url


async def test_malformed_market_message_fails_closed_instead_of_skipping():
    from exchange.binance.market_ws import MalformedMarketMessageError

    client = BinanceMarketWebSocket(
        "BTCUSDC", connect_factory=lambda _: FakeContext(FakeWebSocket(["{invalid json"]))
    )

    async def handler(event):
        pytest.fail("malformed event reached consumer")

    with pytest.raises(MalformedMarketMessageError):
        await client.run(handler)


async def test_idle_socket_timeout_reconnects_without_waiting_for_keepalive():
    import asyncio

    class SilentSocket(FakeWebSocket):
        async def recv(self):
            await asyncio.Event().wait()

    contexts = iter(
        [
            FakeContext(SilentSocket([])),
            FakeContext(FakeWebSocket([stream_message(individual_trade())])),
        ]
    )
    client = BinanceMarketWebSocket(
        "BTCUSDC",
        stale_after_seconds=0.005,
        reconnect_initial_seconds=0.001,
        reconnect_max_seconds=0.001,
        connect_factory=lambda _: next(contexts),
    )

    async def handler(event):
        await client.stop()

    await asyncio.wait_for(client.run(handler), timeout=1)
    assert client.health().reconnect_count == 1
