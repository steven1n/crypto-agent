import random
from decimal import Decimal

import pytest

from market.orderbook import (
    BookSide,
    CrossedBookError,
    EmptyBookSideError,
    InvalidBookDataError,
    LocalOrderBook,
    SequenceGapError,
)
from tests.helpers import depth_event, market_snapshot

D = Decimal


def loaded_book() -> LocalOrderBook:
    book = LocalOrderBook("BTCUSDC")
    book.load_snapshot(
        market_snapshot(
            bids=((D("100"), D("1")), (D("99"), D("2"))),
            asks=((D("102"), D("1.5")), (D("103"), D("3"))),
        )
    )
    return book


def test_insert_level_is_sorted_by_price() -> None:
    book = loaded_book()
    book.apply_bid(D("101"), D("2"))

    assert book.depth(BookSide.BID) == (
        (D("101"), D("2")),
        (D("100"), D("1")),
        (D("99"), D("2")),
    )


def test_replace_uses_absolute_quantity() -> None:
    book = loaded_book()
    book.apply_bid(D("100"), D("3"))

    assert book.depth(BookSide.BID, 1) == ((D("100"), D("3")),)


def test_zero_quantity_deletes_level() -> None:
    book = loaded_book()
    book.apply_bid(D("100"), D("0"))

    assert book.best_bid() == (D("99"), D("2"))


def test_deleting_nonexistent_level_is_valid() -> None:
    book = loaded_book()
    before = book.snapshot()

    book.apply_bid(D("98"), D("0"))

    after = book.snapshot()
    assert after.bids == before.bids
    assert after.asks == before.asks


def test_snapshot_loads_decimal_levels_and_metadata() -> None:
    book = loaded_book()
    view = book.snapshot()

    assert view.last_update_id == 100
    assert view.best_bid_price == D("100")
    assert view.best_ask_price == D("102")
    assert view.bids == ((D("100"), D("1")), (D("99"), D("2")))
    assert view.asks == ((D("102"), D("1.5")), (D("103"), D("3")))
    assert view.spread == D("2")
    assert view.mid_price == D("101")


def test_apply_event_validates_pu_and_updates_id() -> None:
    book = loaded_book()
    event = depth_event(
        first_update_id=101,
        final_update_id=105,
        previous_final_update_id=100,
        bids=((D("100"), D("4")),),
    )

    book.apply_event(event, expected_previous_update_id=100)

    assert book.last_update_id == 105
    assert book.best_bid() == (D("100"), D("4"))

    with pytest.raises(SequenceGapError):
        book.apply_event(
            depth_event(
                first_update_id=106,
                final_update_id=106,
                previous_final_update_id=103,
            ),
            expected_previous_update_id=105,
        )


def test_crossed_update_is_rejected_atomically() -> None:
    book = loaded_book()

    with pytest.raises(CrossedBookError):
        book.apply_bid(D("102"), D("1"))

    assert book.best_bid() == (D("100"), D("1"))
    assert book.best_ask() == (D("102"), D("1.5"))


def test_deleting_last_side_is_rejected_atomically() -> None:
    book = LocalOrderBook("BTCUSDC")
    book.load_snapshot(market_snapshot())

    with pytest.raises(EmptyBookSideError):
        book.apply_ask(D("102"), D("0"))

    assert book.best_ask() == (D("102"), D("1"))


@pytest.mark.parametrize(
    ("bids", "asks"),
    [
        (((D("0"), D("1")),), ((D("2"), D("1")),)),
        (((D("1"), D("0")),), ((D("2"), D("1")),)),
        (((D("1"), D("-1")),), ((D("2"), D("1")),)),
    ],
)
def test_invalid_snapshot_levels_are_rejected(
    bids: tuple[tuple[Decimal, Decimal], ...],
    asks: tuple[tuple[Decimal, Decimal], ...],
) -> None:
    book = LocalOrderBook("BTCUSDC")
    with pytest.raises(InvalidBookDataError):
        book.load_snapshot(market_snapshot(bids=bids, asks=asks))


def test_configured_storage_limit_retains_best_known_levels() -> None:
    book = LocalOrderBook("BTCUSDC", max_stored_levels=2)
    book.load_snapshot(
        market_snapshot(
            bids=((D("100"), D("1")), (D("99"), D("1")), (D("98"), D("1"))),
            asks=((D("101"), D("1")), (D("102"), D("1")), (D("103"), D("1"))),
        )
    )

    assert book.depth(BookSide.BID) == ((D("100"), D("1")), (D("99"), D("1")))
    assert book.depth(BookSide.ASK) == ((D("101"), D("1")), (D("102"), D("1")))


def test_seeded_random_operations_match_reference_model() -> None:
    rng = random.Random(84721)
    book = LocalOrderBook("BTCUSDC")
    book.load_snapshot(
        market_snapshot(
            bids=((D("90"), D("1")),),
            asks=((D("110"), D("1")),),
        )
    )
    reference_bids = {D("90"): D("1")}
    reference_asks = {D("110"): D("1")}

    for _ in range(500):
        is_bid = rng.choice((True, False))
        price = D(rng.randint(91, 99) if is_bid else rng.randint(101, 109))
        quantity = D(rng.choice((0, 1, 2, 3)))
        reference = reference_bids if is_bid else reference_asks
        if quantity == 0:
            reference.pop(price, None)
        else:
            reference[price] = quantity
        if is_bid:
            book.apply_bid(price, quantity)
        else:
            book.apply_ask(price, quantity)

    assert book.depth(BookSide.BID) == tuple(sorted(reference_bids.items(), reverse=True))
    assert book.depth(BookSide.ASK) == tuple(sorted(reference_asks.items()))
