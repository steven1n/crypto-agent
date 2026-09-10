import random
from decimal import Decimal

import pytest

from features.book import calculate_book_features, calculate_microprice
from tests.helpers import trusted_book


def test_known_spread_example() -> None:
    snapshot = trusted_book(
        bids=((Decimal("100"), Decimal("1")),),
        asks=((Decimal("101"), Decimal("1")),),
    )
    features = calculate_book_features(snapshot)

    assert features.spread == Decimal("1")
    assert features.mid_price == Decimal("100.5")
    assert features.spread_bps == float(Decimal("1") / Decimal("100.5") * 10_000)


def test_spread_mid_microprice_and_depth_are_exact() -> None:
    snapshot = trusted_book()
    features = calculate_book_features(snapshot)

    assert features.spread == Decimal("2")
    assert features.mid_price == Decimal("101")
    assert features.microprice == Decimal("101.3333333333333333333333333")
    assert features.microprice_offset == Decimal("0.3333333333333333333333333")
    assert features.at_depth(1).bid_volume == Decimal("2")
    assert features.at_depth(1).ask_volume == Decimal("1")
    assert features.at_depth(5).bid_volume == Decimal("20")
    assert features.at_depth(5).ask_volume == Decimal("15")
    assert features.at_depth(10).bid_volume == Decimal("20")
    assert features.at_depth(10).ask_volume == Decimal("15")


def test_microprice_moves_toward_side_with_less_top_quantity() -> None:
    equal = trusted_book(
        bids=((Decimal("100"), Decimal("1")),),
        asks=((Decimal("102"), Decimal("1")),),
    )
    bid_heavy = trusted_book(
        bids=((Decimal("100"), Decimal("9")),),
        asks=((Decimal("102"), Decimal("1")),),
    )
    ask_heavy = trusted_book(
        bids=((Decimal("100"), Decimal("1")),),
        asks=((Decimal("102"), Decimal("9")),),
    )

    assert calculate_microprice(equal) == Decimal("101")
    assert calculate_microprice(bid_heavy) == Decimal("101.8")
    assert calculate_microprice(ask_heavy) == Decimal("100.2")


def test_random_valid_books_preserve_feature_bounds() -> None:
    random_generator = random.Random(20260904)
    for _ in range(250):
        bid = Decimal(random_generator.randrange(10_000, 20_000)) / 100
        spread = Decimal(random_generator.randrange(1, 100)) / 100
        ask = bid + spread
        bid_quantity = Decimal(random_generator.randrange(1, 10_000)) / 100
        ask_quantity = Decimal(random_generator.randrange(1, 10_000)) / 100
        snapshot = trusted_book(bids=((bid, bid_quantity),), asks=((ask, ask_quantity),))
        features = calculate_book_features(snapshot)

        assert features.microprice is not None
        assert bid <= features.microprice <= ask
        assert features.spread_bps > 0
        assert features.at_depth(1).imbalance is not None
        assert -1 <= features.at_depth(1).imbalance <= 1


def test_known_l10_depth_and_near_one_imbalance_boundaries() -> None:
    bids = tuple((Decimal(100 - level), Decimal(level + 1)) for level in range(10))
    asks = tuple((Decimal(101 + level), Decimal("0.0001")) for level in range(10))
    bid_heavy = calculate_book_features(trusted_book(bids=bids, asks=asks))
    bid_depth = bid_heavy.at_depth(10)

    assert bid_depth.bid_volume == Decimal("55")
    assert bid_depth.ask_volume == Decimal("0.0010")
    assert bid_depth.imbalance is not None
    assert 0 < bid_depth.imbalance < 1

    ask_heavy = calculate_book_features(
        trusted_book(
            bids=tuple((price, Decimal("0.0001")) for price, _ in bids),
            asks=tuple((price, Decimal(level + 1)) for level, (price, _) in enumerate(asks)),
        )
    ).at_depth(10)
    assert ask_heavy.imbalance is not None
    assert -1 < ask_heavy.imbalance < 0


def test_unknown_depth_is_explicit() -> None:
    with pytest.raises(KeyError):
        calculate_book_features(trusted_book()).at_depth(20)
