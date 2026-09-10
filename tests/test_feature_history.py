import math
from decimal import Decimal

import pytest

from features.history import MidPriceHistory


def test_return_uses_observation_at_or_before_boundary_without_future_leakage() -> None:
    history = MidPriceHistory()
    history.add(0, Decimal("100"))
    history.add(200_000_000, Decimal("105"))
    history.add(1_100_000_000, Decimal("110"))

    assert history.log_return(1_000, now_ns=1_100_000_000) == pytest.approx(math.log(1.1))


def test_missing_history_is_none_not_zero() -> None:
    history = MidPriceHistory()
    history.add(1_000_000_000, Decimal("100"))

    assert history.log_return(1_000, now_ns=1_000_000_000) is None
    assert history.realized_volatility(1_000, now_ns=1_000_000_000) is None


def test_realized_volatility_is_nonannualized_observed_update_sum() -> None:
    history = MidPriceHistory()
    history.add(0, Decimal("100"))
    history.add(500_000_000, Decimal("110"))
    history.add(1_000_000_000, Decimal("99"))

    expected = math.sqrt(math.log(1.1) ** 2 + math.log(0.9) ** 2)
    assert history.realized_volatility(1_000, now_ns=1_000_000_000) == pytest.approx(expected)


def test_constant_price_has_known_zero_volatility() -> None:
    history = MidPriceHistory()
    history.add(0, Decimal("100"))
    history.add(1_000_000_000, Decimal("100"))

    assert history.realized_volatility(1_000, now_ns=1_000_000_000) == 0


def test_history_retention_is_bounded() -> None:
    history = MidPriceHistory(retention_seconds=10)
    for second in range(31):
        history.add(second * 1_000_000_000, Decimal("100"))

    assert len(history) == 11
    assert history.historical_at_or_before(19_999_999_999) is None
