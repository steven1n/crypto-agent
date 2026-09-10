from collections.abc import Iterator
from datetime import UTC, datetime

from market.clock import ExchangeClockOffsetEstimator


def wall_time(milliseconds: int) -> datetime:
    return datetime.fromtimestamp(milliseconds / 1_000, tz=UTC)


class ServerTime:
    def __init__(self, values: list[int | Exception]) -> None:
        self.values = iter(values)

    async def server_time_ms(self) -> int:
        value = next(self.values)
        if isinstance(value, Exception):
            raise value
        return value


class CallableClock[T]:
    def __init__(self, values: Iterator[T]) -> None:
        self.values = values

    def __call__(self) -> T:
        return next(self.values)


async def test_midpoint_offset_has_explicit_positive_sign() -> None:
    wall = CallableClock(iter([wall_time(1_000), wall_time(1_100)]))
    mono = CallableClock(iter([0, 100_000_000]))
    estimator = ExchangeClockOffsetEstimator(
        ServerTime([1_070]),
        sample_count=1,
        max_rtt_ms=200,
        wall_clock=wall,
        monotonic_clock_ns=mono,
    )

    sample = await estimator.sample_once()

    assert sample is not None
    assert sample.offset_ms == 20
    assert sample.rtt_ms == 100
    assert estimator.status().offset_ms == 20
    # Event exchange=1070 corresponds to local=1050; received local=1100 => 50 ms.
    assert (
        estimator.corrected_event_lag_ms(
            local_receive_time=wall_time(1_100),
            exchange_event_time=wall_time(1_070),
        )
        == 50
    )


async def test_negative_offset_and_corrected_lag_sign() -> None:
    estimator = ExchangeClockOffsetEstimator(
        ServerTime([1_030]),
        sample_count=1,
        wall_clock=CallableClock(iter([wall_time(1_000), wall_time(1_100)])),
        monotonic_clock_ns=CallableClock(iter([0, 20_000_000])),
    )
    await estimator.sample_once()

    assert estimator.status().offset_ms == -20
    # Event exchange=1030 corresponds to local=1050; received local=1100 => 50 ms.
    assert (
        estimator.corrected_event_lag_ms(
            local_receive_time=wall_time(1_100),
            exchange_event_time=wall_time(1_030),
        )
        == 50
    )


async def test_high_rtt_sample_is_rejected() -> None:
    estimator = ExchangeClockOffsetEstimator(
        ServerTime([1_070]),
        sample_count=1,
        max_rtt_ms=50,
        wall_clock=CallableClock(iter([wall_time(1_000), wall_time(1_100)])),
        monotonic_clock_ns=CallableClock(iter([0, 100_000_000])),
    )

    assert await estimator.sample_once() is None
    assert estimator.status().offset_ms is None
    assert estimator.status().failures == 1


async def test_unavailable_server_time_does_not_fabricate_offset() -> None:
    estimator = ExchangeClockOffsetEstimator(
        ServerTime([ConnectionError("offline")]),
        sample_count=1,
        wall_clock=CallableClock(iter([wall_time(1_000)])),
        monotonic_clock_ns=CallableClock(iter([0])),
    )

    assert await estimator.sample_once() is None
    assert not estimator.status().healthy
    assert (
        estimator.corrected_event_lag_ms(
            local_receive_time=wall_time(1_100),
            exchange_event_time=wall_time(1_000),
        )
        is None
    )


async def test_rolling_median_deprioritizes_offset_outlier() -> None:
    estimator = ExchangeClockOffsetEstimator(
        ServerTime([1_060, 2_060, 3_500]),
        sample_count=3,
        wall_clock=CallableClock(
            iter(
                [
                    wall_time(1_000),
                    wall_time(1_100),
                    wall_time(2_000),
                    wall_time(2_100),
                    wall_time(3_000),
                    wall_time(3_100),
                ]
            )
        ),
        monotonic_clock_ns=CallableClock(
            iter([0, 10_000_000, 20_000_000, 30_000_000, 40_000_000, 50_000_000])
        ),
    )

    await estimator.synchronize()

    assert estimator.status().offset_ms == 10
    assert estimator.status().sample_count == 3
