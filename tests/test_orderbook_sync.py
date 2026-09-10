import asyncio
from decimal import Decimal

import pytest

from exchange.binance.orderbook_sync import (
    BinanceBookSynchronizer,
    SnapshotBridgeError,
    is_snapshot_bridge,
)
from market.events import MarketSnapshot
from market.orderbook import OrderBookState
from tests.helpers import depth_event, market_snapshot

D = Decimal


class FakeClock:
    def __init__(self, now_ns: int = 10_000_000_000) -> None:
        self.now_ns = now_ns

    def __call__(self) -> int:
        return self.now_ns

    def advance_ms(self, milliseconds: int) -> None:
        self.now_ns += milliseconds * 1_000_000


class ControlledSnapshotProvider:
    def __init__(self) -> None:
        self.responses: asyncio.Queue[MarketSnapshot | Exception] = asyncio.Queue()
        self.calls = 0
        self._call_condition = asyncio.Condition()

    async def book_snapshot(self, symbol: str, *, limit: int = 1_000) -> MarketSnapshot:
        assert symbol == "BTCUSDC"
        assert limit == 1_000
        async with self._call_condition:
            self.calls += 1
            self._call_condition.notify_all()
        response = await self.responses.get()
        if isinstance(response, Exception):
            raise response
        return response

    def respond(self, response: MarketSnapshot | Exception) -> None:
        self.responses.put_nowait(response)

    async def wait_for_calls(self, expected: int) -> None:
        async def wait() -> None:
            async with self._call_condition:
                await self._call_condition.wait_for(lambda: self.calls >= expected)

        await asyncio.wait_for(wait(), timeout=1)


def synchronizer(
    provider: ControlledSnapshotProvider,
    clock: FakeClock,
    *,
    max_buffered_events: int = 20,
    stale_after_ms: int = 1_000,
) -> BinanceBookSynchronizer:
    return BinanceBookSynchronizer(
        "BTCUSDC",
        provider,
        max_buffered_events=max_buffered_events,
        stale_after_ms=stale_after_ms,
        resync_initial_seconds=0.001,
        resync_max_seconds=0.002,
        clock_ns=clock,
    )


async def make_synced(
    *,
    bridge_final_u: int = 101,
    stale_after_ms: int = 1_000,
) -> tuple[BinanceBookSynchronizer, ControlledSnapshotProvider, FakeClock]:
    provider = ControlledSnapshotProvider()
    clock = FakeClock()
    sync = synchronizer(provider, clock, stale_after_ms=stale_after_ms)
    provider.respond(market_snapshot(last_update_id=100))
    await sync.start()
    await sync.on_depth_event(
        depth_event(
            first_update_id=99,
            final_update_id=bridge_final_u,
            previous_final_update_id=98,
            bids=((D("100"), D("2")),),
            monotonic_ns=clock.now_ns,
        )
    )
    await sync.wait_until_synced(1)
    return sync, provider, clock


@pytest.mark.parametrize(
    ("first_u", "final_u", "expected"),
    [
        (99, 99, False),  # u < lastUpdateId
        (99, 100, True),  # U < and u == lastUpdateId
        (99, 101, True),  # U < and u > lastUpdateId
        (100, 100, True),  # U == and u == lastUpdateId
        (100, 101, True),  # U == and u > lastUpdateId
        (101, 101, False),  # U > lastUpdateId
    ],
)
def test_usdm_snapshot_bridge_boundaries(first_u: int, final_u: int, expected: bool) -> None:
    event = depth_event(
        first_update_id=first_u,
        final_update_id=final_u,
        previous_final_update_id=98,
    )
    assert is_snapshot_bridge(event, 100) is expected


async def test_bootstrap_discards_old_event_and_replays_bridge_continuously() -> None:
    provider = ControlledSnapshotProvider()
    clock = FakeClock()
    sync = synchronizer(provider, clock)

    await sync.start()
    await provider.wait_for_calls(1)
    await sync.on_depth_event(
        depth_event(
            first_update_id=90,
            final_update_id=99,
            previous_final_update_id=89,
            bids=((D("97"), D("5")),),
            monotonic_ns=clock.now_ns,
        )
    )
    await sync.on_depth_event(
        depth_event(
            first_update_id=99,
            final_update_id=101,
            previous_final_update_id=98,
            bids=((D("101"), D("2")),),
            monotonic_ns=clock.now_ns,
        )
    )
    await sync.on_depth_event(
        depth_event(
            first_update_id=102,
            final_update_id=105,
            previous_final_update_id=101,
            asks=((D("102"), D("0")), (D("104"), D("3"))),
            monotonic_ns=clock.now_ns,
        )
    )
    provider.respond(market_snapshot(last_update_id=100))
    await sync.wait_until_synced(1)

    view = sync.trusted_snapshot()
    assert view is not None
    assert view.last_update_id == 105
    assert view.best_bid_price == D("101")
    assert view.best_ask_price == D("104")
    diagnostics = sync.diagnostics()
    assert diagnostics.events_applied == 2
    assert diagnostics.events_discarded_as_old == 1
    assert diagnostics.sync_success_count == 1
    await sync.stop()


async def test_bridge_equal_to_snapshot_id_is_applied_without_plus_one() -> None:
    provider = ControlledSnapshotProvider()
    clock = FakeClock()
    sync = synchronizer(provider, clock)
    provider.respond(market_snapshot(last_update_id=100))

    await sync.start()
    await sync.on_depth_event(
        depth_event(
            first_update_id=99,
            final_update_id=100,
            previous_final_update_id=98,
            bids=((D("100"), D("7")),),
            monotonic_ns=clock.now_ns,
        )
    )
    await sync.wait_until_synced(1)

    view = sync.trusted_snapshot()
    assert view is not None
    assert view.last_update_id == 100
    assert view.best_bid_quantity == D("7")
    await sync.stop()


async def test_missing_bridge_retries_with_a_newer_snapshot() -> None:
    provider = ControlledSnapshotProvider()
    clock = FakeClock()
    sync = synchronizer(provider, clock)
    provider.respond(market_snapshot(last_update_id=100))
    provider.respond(market_snapshot(last_update_id=101))

    await sync.start()
    await sync.on_depth_event(
        depth_event(
            first_update_id=101,
            final_update_id=102,
            previous_final_update_id=100,
            monotonic_ns=clock.now_ns,
        )
    )
    await sync.wait_until_synced(1)

    assert provider.calls == 2
    assert sync.last_update_id == 102
    assert sync.diagnostics().sync_failure_count == 1
    assert isinstance(SnapshotBridgeError("test"), SnapshotBridgeError)
    await sync.stop()


async def test_pu_gap_invalidates_book_and_requests_resync() -> None:
    sync, provider, clock = await make_synced(bridge_final_u=105)
    provider.respond(market_snapshot(last_update_id=106))

    await sync.on_depth_event(
        depth_event(
            first_update_id=106,
            final_update_id=107,
            previous_final_update_id=103,
            monotonic_ns=clock.now_ns,
        )
    )

    assert sync.trusted_snapshot() is None
    diagnostics = sync.diagnostics()
    assert diagnostics.state in {OrderBookState.RESYNCING, OrderBookState.SNAPSHOT_LOADING}
    assert diagnostics.sequence_gap_count == 1
    assert diagnostics.resync_count == 1
    await sync.stop()


async def test_exact_duplicate_is_ignored_without_corruption() -> None:
    sync, _, clock = await make_synced()
    duplicate = depth_event(
        first_update_id=99,
        final_update_id=101,
        previous_final_update_id=98,
        bids=((D("100"), D("2")),),
        monotonic_ns=clock.now_ns,
    )

    await sync.on_depth_event(duplicate)

    view = sync.trusted_snapshot()
    assert view is not None
    assert view.best_bid_quantity == D("2")
    assert sync.diagnostics().events_discarded_as_old == 1
    await sync.stop()


async def test_conflicting_payload_with_same_ids_fails_closed() -> None:
    sync, provider, clock = await make_synced()
    provider.respond(market_snapshot(last_update_id=101))
    conflicting = depth_event(
        first_update_id=99,
        final_update_id=101,
        previous_final_update_id=98,
        bids=((D("100"), D("999")),),
        monotonic_ns=clock.now_ns,
    )

    await sync.on_depth_event(conflicting)

    assert sync.trusted_snapshot() is None
    assert sync.diagnostics().resync_count == 1
    await sync.stop()


async def test_nonduplicate_stale_or_out_of_order_event_fails_closed() -> None:
    sync, provider, clock = await make_synced(bridge_final_u=105)
    provider.respond(market_snapshot(last_update_id=105))

    await sync.on_depth_event(
        depth_event(
            first_update_id=102,
            final_update_id=104,
            previous_final_update_id=101,
            monotonic_ns=clock.now_ns,
        )
    )

    assert sync.trusted_snapshot() is None
    assert sync.diagnostics().resync_count == 1
    await sync.stop()


async def test_crossed_live_update_invalidates_and_resyncs() -> None:
    sync, provider, clock = await make_synced()
    provider.respond(market_snapshot(last_update_id=101))

    await sync.on_depth_event(
        depth_event(
            first_update_id=102,
            final_update_id=102,
            previous_final_update_id=101,
            bids=((D("102"), D("1")),),
            monotonic_ns=clock.now_ns,
        )
    )

    assert sync.trusted_snapshot() is None
    assert sync.diagnostics().crossed_book_count == 1
    assert sync.diagnostics().resync_count == 1
    await sync.stop()


async def test_empty_side_live_update_invalidates_and_resyncs() -> None:
    sync, provider, clock = await make_synced()
    provider.respond(market_snapshot(last_update_id=101))

    await sync.on_depth_event(
        depth_event(
            first_update_id=102,
            final_update_id=102,
            previous_final_update_id=101,
            asks=((D("102"), D("0")),),
            monotonic_ns=clock.now_ns,
        )
    )

    assert sync.trusted_snapshot() is None
    assert sync.diagnostics().resync_count == 1
    await sync.stop()


async def test_buffer_overflow_abandons_attempt_and_starts_fresh_sync() -> None:
    provider = ControlledSnapshotProvider()
    clock = FakeClock()
    sync = synchronizer(provider, clock, max_buffered_events=1)

    await sync.start()
    await provider.wait_for_calls(1)
    await sync.on_depth_event(
        depth_event(
            first_update_id=99,
            final_update_id=100,
            previous_final_update_id=98,
            monotonic_ns=clock.now_ns,
        )
    )
    await sync.on_depth_event(
        depth_event(
            first_update_id=101,
            final_update_id=101,
            previous_final_update_id=100,
            monotonic_ns=clock.now_ns,
        )
    )
    await provider.wait_for_calls(2)

    diagnostics = sync.diagnostics()
    assert diagnostics.buffer_overflow_count == 1
    assert diagnostics.resync_count == 1
    assert diagnostics.current_buffer_size == 1
    assert sync.trusted_snapshot() is None
    await sync.stop()


async def test_snapshot_race_does_not_lose_event() -> None:
    provider = ControlledSnapshotProvider()
    clock = FakeClock()
    sync = synchronizer(provider, clock)

    await sync.start()
    await provider.wait_for_calls(1)
    await sync.on_depth_event(
        depth_event(
            first_update_id=99,
            final_update_id=101,
            previous_final_update_id=98,
            bids=((D("100"), D("2")),),
            monotonic_ns=clock.now_ns,
        )
    )
    provider.respond(market_snapshot(last_update_id=100))
    await sync.on_depth_event(
        depth_event(
            first_update_id=102,
            final_update_id=103,
            previous_final_update_id=101,
            bids=((D("101"), D("4")),),
            monotonic_ns=clock.now_ns,
        )
    )
    await sync.wait_until_synced(1)

    view = sync.trusted_snapshot()
    assert view is not None
    assert view.last_update_id == 103
    assert view.best_bid_price == D("101")
    assert view.best_bid_quantity == D("4")
    await sync.stop()


async def test_disconnect_revokes_trust_and_reconnect_requires_fresh_snapshot() -> None:
    sync, provider, clock = await make_synced()

    await sync.on_disconnected("test disconnect")
    assert sync.state is OrderBookState.DESYNCED
    assert sync.trusted_snapshot() is None

    provider.respond(market_snapshot(last_update_id=200))
    await sync.on_connected()
    await sync.on_depth_event(
        depth_event(
            first_update_id=199,
            final_update_id=201,
            previous_final_update_id=198,
            bids=((D("101"), D("3")),),
            monotonic_ns=clock.now_ns,
        )
    )
    await sync.wait_until_synced(1)

    assert sync.last_update_id == 201
    assert sync.diagnostics().resync_count == 1
    await sync.stop()


async def test_staleness_uses_fake_monotonic_clock_and_recovers_on_continuity() -> None:
    sync, _, clock = await make_synced(stale_after_ms=50)

    clock.advance_ms(51)
    assert sync.trusted_snapshot() is None
    assert sync.state is OrderBookState.STALE
    assert sync.diagnostics().sequence_gap_count == 0

    await sync.on_depth_event(
        depth_event(
            first_update_id=102,
            final_update_id=102,
            previous_final_update_id=101,
            bids=((D("100"), D("3")),),
            monotonic_ns=clock.now_ns,
        )
    )

    assert sync.state is OrderBookState.SYNCED
    assert sync.trusted_snapshot() is not None
    await sync.stop()


async def test_invalid_snapshot_retries_without_exposing_candidate() -> None:
    provider = ControlledSnapshotProvider()
    clock = FakeClock()
    sync = synchronizer(provider, clock)
    provider.respond(
        market_snapshot(
            last_update_id=100,
            bids=((D("102"), D("1")),),
            asks=((D("101"), D("1")),),
        )
    )
    provider.respond(market_snapshot(last_update_id=100))

    await sync.start()
    await sync.on_depth_event(
        depth_event(
            first_update_id=99,
            final_update_id=101,
            previous_final_update_id=98,
            monotonic_ns=clock.now_ns,
        )
    )
    await sync.wait_until_synced(1)

    diagnostics = sync.diagnostics()
    assert diagnostics.sync_failure_count == 1
    assert diagnostics.crossed_book_count == 1
    assert diagnostics.sync_success_count == 1
    await sync.stop()


async def test_snapshot_transport_failure_retries_with_backoff() -> None:
    provider = ControlledSnapshotProvider()
    clock = FakeClock()
    sync = synchronizer(provider, clock)
    provider.respond(ConnectionError("REST unavailable"))
    provider.respond(market_snapshot(last_update_id=100))

    await sync.start()
    await sync.on_depth_event(
        depth_event(
            first_update_id=99,
            final_update_id=101,
            previous_final_update_id=98,
            monotonic_ns=clock.now_ns,
        )
    )
    await sync.wait_until_synced(1)

    assert provider.calls == 2
    assert sync.diagnostics().sync_failure_count == 1
    await sync.stop()
