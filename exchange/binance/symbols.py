"""Precision-safe extraction of Binance symbol trading rules."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from exchange.binance.models import BinanceSymbolFilter, BinanceSymbolInfo


class SymbolValidationError(ValueError):
    """Raised when a configured symbol cannot be used for Phase 1 research."""


@dataclass(frozen=True, slots=True)
class SymbolRules:
    symbol: str
    base_asset: str
    quote_asset: str
    margin_asset: str
    tick_size: Decimal
    step_size: Decimal
    min_quantity: Decimal
    min_notional: Decimal | None


def _filter(filters: tuple[BinanceSymbolFilter, ...], name: str) -> BinanceSymbolFilter:
    try:
        return next(item for item in filters if item.filter_type == name)
    except StopIteration as exc:
        raise SymbolValidationError(f"required {name} filter is missing") from exc


def extract_symbol_rules(symbol: BinanceSymbolInfo) -> SymbolRules:
    if symbol.status != "TRADING":
        raise SymbolValidationError(f"{symbol.symbol} is not trading (status={symbol.status})")
    if symbol.contract_type != "PERPETUAL":
        raise SymbolValidationError(
            f"{symbol.symbol} is not perpetual (contractType={symbol.contract_type})"
        )

    price_filter = _filter(symbol.filters, "PRICE_FILTER")
    lot_filter = _filter(symbol.filters, "LOT_SIZE")
    if price_filter.tick_size is None or price_filter.tick_size <= 0:
        raise SymbolValidationError("PRICE_FILTER has no positive tickSize")
    if lot_filter.step_size is None or lot_filter.step_size <= 0:
        raise SymbolValidationError("LOT_SIZE has no positive stepSize")
    if lot_filter.min_qty is None or lot_filter.min_qty < 0:
        raise SymbolValidationError("LOT_SIZE has no valid minQty")

    notional_filter = next(
        (item for item in symbol.filters if item.filter_type in {"MIN_NOTIONAL", "NOTIONAL"}),
        None,
    )
    min_notional = None
    if notional_filter is not None:
        min_notional = notional_filter.notional or notional_filter.min_notional

    return SymbolRules(
        symbol=symbol.symbol,
        base_asset=symbol.base_asset,
        quote_asset=symbol.quote_asset,
        margin_asset=symbol.margin_asset,
        tick_size=price_filter.tick_size,
        step_size=lot_filter.step_size,
        min_quantity=lot_filter.min_qty,
        min_notional=min_notional,
    )
