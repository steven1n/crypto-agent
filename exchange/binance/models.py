"""Typed models for Binance public REST and WebSocket payloads."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class BinanceModel(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class BinanceSymbolFilter(BinanceModel):
    filter_type: str = Field(alias="filterType")
    min_price: Decimal | None = Field(default=None, alias="minPrice")
    max_price: Decimal | None = Field(default=None, alias="maxPrice")
    tick_size: Decimal | None = Field(default=None, alias="tickSize")
    min_qty: Decimal | None = Field(default=None, alias="minQty")
    max_qty: Decimal | None = Field(default=None, alias="maxQty")
    step_size: Decimal | None = Field(default=None, alias="stepSize")
    notional: Decimal | None = None
    min_notional: Decimal | None = Field(default=None, alias="minNotional")


class BinanceSymbolInfo(BinanceModel):
    symbol: str
    pair: str
    contract_type: str = Field(alias="contractType")
    status: str
    base_asset: str = Field(alias="baseAsset")
    quote_asset: str = Field(alias="quoteAsset")
    margin_asset: str = Field(alias="marginAsset")
    price_precision: int = Field(alias="pricePrecision")
    quantity_precision: int = Field(alias="quantityPrecision")
    filters: tuple[BinanceSymbolFilter, ...]


class BinanceExchangeInfo(BinanceModel):
    timezone: str
    server_time: int = Field(alias="serverTime")
    symbols: tuple[BinanceSymbolInfo, ...]


class BinanceServerTime(BinanceModel):
    server_time: int = Field(alias="serverTime")


class BinanceDepthSnapshot(BinanceModel):
    last_update_id: int = Field(alias="lastUpdateId")
    message_output_time: int | None = Field(default=None, alias="E")
    transaction_time: int | None = Field(default=None, alias="T")
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]


class BinanceAggTrade(BinanceModel):
    event_type: Literal["aggTrade"] = Field(alias="e")
    event_time_ms: int = Field(alias="E")
    symbol: str = Field(alias="s")
    aggregate_trade_id: int = Field(alias="a")
    price: Decimal = Field(alias="p")
    quantity: Decimal = Field(alias="q")
    first_trade_id: int = Field(alias="f")
    last_trade_id: int = Field(alias="l")
    trade_time_ms: int = Field(alias="T")
    buyer_is_maker: bool = Field(alias="m")


class BinanceTrade(BinanceModel):
    event_type: Literal["trade"] = Field(alias="e")
    event_time_ms: int = Field(alias="E")
    transaction_time_ms: int = Field(alias="T")
    symbol: str = Field(alias="s")
    trade_id: int = Field(alias="t")
    price: Decimal = Field(alias="p")
    quantity: Decimal = Field(alias="q")
    buyer_is_maker: bool = Field(alias="m")


class BinanceBookTicker(BinanceModel):
    event_type: Literal["bookTicker"] = Field(alias="e")
    event_time_ms: int = Field(alias="E")
    transaction_time_ms: int = Field(alias="T")
    symbol: str = Field(alias="s")
    update_id: int = Field(alias="u")
    bid_price: Decimal = Field(alias="b")
    bid_quantity: Decimal = Field(alias="B")
    ask_price: Decimal = Field(alias="a")
    ask_quantity: Decimal = Field(alias="A")


class BinanceDepthUpdate(BinanceModel):
    event_type: Literal["depthUpdate"] = Field(alias="e")
    event_time_ms: int = Field(alias="E")
    transaction_time_ms: int = Field(alias="T")
    symbol: str = Field(alias="s")
    first_update_id: int = Field(alias="U")
    final_update_id: int = Field(alias="u")
    previous_final_update_id: int = Field(alias="pu")
    bids: tuple[tuple[Decimal, Decimal], ...] = Field(alias="b")
    asks: tuple[tuple[Decimal, Decimal], ...] = Field(alias="a")


BinanceWsPayload = BinanceAggTrade | BinanceTrade | BinanceBookTicker | BinanceDepthUpdate


def parse_ws_payload(payload: dict[str, Any]) -> BinanceWsPayload:
    """Validate a supported Binance event, rejecting unknown payloads explicitly."""

    event_type = payload.get("e")
    model: (
        type[BinanceAggTrade]
        | type[BinanceTrade]
        | type[BinanceBookTicker]
        | type[BinanceDepthUpdate]
    )
    if event_type == "aggTrade":
        model = BinanceAggTrade
    elif event_type == "trade":
        model = BinanceTrade
    elif event_type == "bookTicker":
        model = BinanceBookTicker
    elif event_type == "depthUpdate":
        model = BinanceDepthUpdate
    else:
        raise ValueError(f"unsupported Binance event type: {event_type!r}")
    return model.model_validate(payload)
