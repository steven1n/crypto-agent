import asyncio
import json
from contextlib import AbstractAsyncContextManager
from decimal import Decimal

from exchange.base import (
    StreamLifecycleEvent,
    StreamLifecycleKind,
)
from exchange.binance.market_ws import BinanceMarketWebSocket, WebSocketConnection
from exchange.binance.orderbook_sync import BinanceBookSynchronizer
from features.engine import FeatureEngine
from features.models import FeatureSnapshot
from market.events import BookUpdateEvent, MarketSnapshot, NormalizedMarketEvent
from market.orderbook import TrustedBookSnapshot
from tests.helpers import market_snapshot

D = Decimal


def raw_depth(
    *,
    first_update_id: int,
    final_update_id: int,
    previous_final_update_id: int,
    bids: list[list[str]],
    asks: list[list[str]],
) -> str:
    return json.dumps(
        {
            "stream": "btcusdc@depth@100ms",
            "data": {
                "e": "depthUpdate",
                "E": 1_725_000_000_000,
                "T": 1_725_000_000_000,
                "s": "BTCUSDC",
                "U": first_update_id,
                "u": final_update_id,
                "pu": previous_final_update_id,
                "b": bids,
                "a": asks,
            },
        }
    )


class BlockingSnapshotProvider:
    def __init__(self, snapshot: MarketSnapshot) -> None:
        self.snapshot = snapshot
        self.requested = asyncio.Event()
        self.release = asyncio.Event()

    async def book_snapshot(self, symbol: str, *, limit: int = 1_000) -> MarketSnapshot:
        assert symbol == "BTCUSDC"
        assert limit == 1_000
        self.requested.set()
        await self.release.wait()
        return self.snapshot


class FakeWebSocket:
    def __init__(self, messages: list[str]) -> None:
        self.messages = messages
        self.closed = False
        self._closed = asyncio.Event()

    async def recv(self) -> str | bytes:
        if self.messages:
            return self.messages.pop(0)
        await self._closed.wait()
        raise ConnectionError("closed")

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.closed = True
        self._closed.set()


class FakeContext(AbstractAsyncContextManager[WebSocketConnection]):
    def __init__(self, websocket: FakeWebSocket) -> None:
        self.websocket = websocket

    async def __aenter__(self) -> WebSocketConnection:
        return self.websocket

    async def __aexit__(self, *args: object) -> None:
        return None


async def test_mock_websocket_and_rest_reconstruct_then_revoke_trusted_book() -> None:
    provider = BlockingSnapshotProvider(market_snapshot(last_update_id=100))
    sync = BinanceBookSynchronizer("BTCUSDC", provider, stale_after_ms=10_000)
    websocket = FakeWebSocket(
        [
            raw_depth(
                first_update_id=99,
                final_update_id=101,
                previous_final_update_id=98,
                bids=[["100", "2"]],
                asks=[],
            ),
            raw_depth(
                first_update_id=102,
                final_update_id=103,
                previous_final_update_id=101,
                bids=[["101", "4"]],
                asks=[["102", "0"], ["104", "3"]],
            ),
        ]
    )
    stream = BinanceMarketWebSocket("BTCUSDC", connect_factory=lambda _: FakeContext(websocket))
    captured: list[TrustedBookSnapshot] = []
    captured_features: list[FeatureSnapshot] = []
    feature_engine = FeatureEngine("BTCUSDC")
    depth_count = 0

    async def handle_event(event: NormalizedMarketEvent) -> None:
        nonlocal depth_count
        feature_engine.on_market_activity(event.local_receive_monotonic_ns)
        if not isinstance(event, BookUpdateEvent):
            return
        depth_count += 1
        await sync.on_depth_event(event)
        if depth_count == 2:
            provider.release.set()
            await sync.wait_until_synced(1)
            snapshot = sync.trusted_snapshot()
            assert snapshot is not None
            captured.append(snapshot)
            feature_engine.on_book_snapshot(snapshot)
            feature = feature_engine.build_snapshot(now_ns=snapshot.timestamp_monotonic_ns)
            assert feature is not None
            captured_features.append(feature)
            await stream.stop()

    async def handle_lifecycle(event: StreamLifecycleEvent) -> None:
        if event.kind is StreamLifecycleKind.CONNECTED:
            await sync.on_connected()
            feature_engine.on_connection(True, monotonic_timestamp_ns=event.monotonic_ns)
        else:
            await sync.on_disconnected(event.reason or "disconnected")
            feature_engine.on_connection(False, monotonic_timestamp_ns=event.monotonic_ns)

    await stream.run(handle_event, lifecycle_handler=handle_lifecycle)

    assert websocket.closed
    assert len(captured) == 1
    snapshot = captured[0]
    assert snapshot.last_update_id == 103
    assert snapshot.best_bid_price == D("101")
    assert snapshot.best_bid_quantity == D("4")
    assert snapshot.best_ask_price == D("104")
    assert sync.diagnostics().events_applied == 2
    assert len(captured_features) == 1
    assert captured_features[0].feature_valid
    assert captured_features[0].book.mid_price == D("102.5")
    # The stream's DISCONNECTED lifecycle event revokes the just-captured view.
    assert sync.trusted_snapshot() is None
    await sync.stop()
