"""Forward-looking labels isolated from all real-time strategy modules."""

from __future__ import annotations

import bisect
import math
from dataclasses import dataclass

from features.models import FeatureSnapshot


@dataclass(frozen=True, slots=True)
class LabelSnapshot:
    symbol: str
    monotonic_ns: int
    future_return_250ms: float | None
    future_return_500ms: float | None
    future_return_1s: float | None
    future_return_2s: float | None
    future_return_5s: float | None
    future_return_10s: float | None
    future_max_up_bps_1s: float | None
    future_max_down_bps_1s: float | None
    future_max_up_bps_2s: float | None
    future_max_down_bps_2s: float | None
    future_max_up_bps_5s: float | None
    future_max_down_bps_5s: float | None


RETURN_HORIZONS_MS = (250, 500, 1_000, 2_000, 5_000, 10_000)
EXCURSION_HORIZONS_MS = (1_000, 2_000, 5_000)


class LabelGenerator:
    """Uses the first observation at or after each future boundary."""

    def generate(
        self,
        features: tuple[FeatureSnapshot, ...],
        *,
        integrity_boundaries_ns: tuple[int, ...] = (),
    ) -> tuple[LabelSnapshot, ...]:
        if not features:
            return ()
        timestamps = [item.monotonic_ns for item in features]
        if any(
            current <= previous
            for previous, current in zip(timestamps, timestamps[1:], strict=False)
        ):
            raise ValueError("label input timestamps must be strictly increasing")
        if len({item.symbol for item in features}) != 1:
            raise ValueError("labels must be generated for exactly one symbol")
        if tuple(sorted(integrity_boundaries_ns)) != integrity_boundaries_ns:
            raise ValueError("integrity boundaries must be sorted")

        invalid_prefix = [0]
        for index, item in enumerate(features):
            gap = index > 0 and item.monotonic_ns - features[index - 1].monotonic_ns > 3_000_000_000
            invalid_prefix.append(invalid_prefix[-1] + int(not item.feature_valid or gap))

        labels: list[LabelSnapshot] = []
        for index, feature in enumerate(features):
            current_price = float(feature.book.mid_price)
            returns: dict[int, float | None] = {}
            target_indices: dict[int, int | None] = {}
            for horizon_ms in RETURN_HORIZONS_MS:
                target = feature.monotonic_ns + horizon_ms * 1_000_000
                future_index = bisect.bisect_left(timestamps, target, lo=index + 1)
                if future_index < len(features) and (
                    invalid_prefix[future_index + 1] > invalid_prefix[index]
                    or bisect.bisect_right(integrity_boundaries_ns, timestamps[future_index])
                    > bisect.bisect_right(integrity_boundaries_ns, feature.monotonic_ns)
                ):
                    future_index = len(features)
                target_indices[horizon_ms] = None if future_index == len(features) else future_index
                returns[horizon_ms] = (
                    None
                    if future_index == len(features)
                    else math.log(float(features[future_index].book.mid_price) / current_price)
                )

            excursions: dict[int, tuple[float | None, float | None]] = {}
            for horizon_ms in EXCURSION_HORIZONS_MS:
                end_index = target_indices[horizon_ms]
                if end_index is None:
                    excursions[horizon_ms] = (None, None)
                    continue
                path_returns_bps = [
                    math.log(float(item.book.mid_price) / current_price) * 10_000
                    for item in features[index + 1 : end_index + 1]
                ]
                excursions[horizon_ms] = (
                    max(path_returns_bps, default=0.0),
                    min(path_returns_bps, default=0.0),
                )

            labels.append(
                LabelSnapshot(
                    symbol=feature.symbol,
                    monotonic_ns=feature.monotonic_ns,
                    future_return_250ms=returns[250],
                    future_return_500ms=returns[500],
                    future_return_1s=returns[1_000],
                    future_return_2s=returns[2_000],
                    future_return_5s=returns[5_000],
                    future_return_10s=returns[10_000],
                    future_max_up_bps_1s=excursions[1_000][0],
                    future_max_down_bps_1s=excursions[1_000][1],
                    future_max_up_bps_2s=excursions[2_000][0],
                    future_max_down_bps_2s=excursions[2_000][1],
                    future_max_up_bps_5s=excursions[5_000][0],
                    future_max_down_bps_5s=excursions[5_000][1],
                )
            )
        return tuple(labels)
