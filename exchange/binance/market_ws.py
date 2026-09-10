"""Reconnect-capable Binance USD-M public WebSocket market-data client."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Any, Protocol, cast

import structlog
from pydantic import ValidationError
from websockets.asyncio.client import connect

from exchange.base import (
    MarketEventHandler,
    MarketLifecycleHandler,
    StreamLifecycleEvent,
    StreamLifecycleKind,
)
from exchange.binance.models import (
    BinanceAggTrade,
    BinanceBookTicker,
    BinanceTrade,
    parse_ws_payload,
)
from market.events import (
    AggressorSide,
    BestBidAskEvent,
    BookUpdateEvent,
    NormalizedMarketEvent,
    TradeEvent,
    TradeSource,
    utc_from_milliseconds,
)
from market.normalization import ReceiveContext

logger = structlog.get_logger(__name__)


class NonMarketTradeMarker(ValueError):
    """Transport marker from the individual trade feed, not a real fill."""


class MalformedMarketMessageError(RuntimeError):
    """A malformed message cannot be skipped while claiming intact raw capture."""


class WebSocketConnection(Protocol):
    async def recv(self) -> str | bytes: ...

    async def close(self, code: int = 1000, reason: str = "") -> None: ...


ConnectFactory = Callable[[str], AbstractAsyncContextManager[WebSocketConnection]]


@dataclass(frozen=True, slots=True)
class ConnectionHealth:
    connected: bool
    stale: bool
    last_message_age_seconds: float | None
    aggregate_trade_age_seconds: float | None
    book_ticker_age_seconds: float | None
    depth_age_seconds: float | None
    reconnect_count: int
    last_error: str | None


def normalize_binance_ws_payload(
    payload: dict[str, Any], receive_context: ReceiveContext
) -> NormalizedMarketEvent:
    raw = parse_ws_payload(payload)
    exchange_event_time = utc_from_milliseconds(raw.event_time_ms)
    estimated_latency_ms = receive_context.estimated_latency_ms(raw.event_time_ms)
    if isinstance(raw, BinanceAggTrade):
        return TradeEvent(
            symbol=raw.symbol,
            exchange_event_time=exchange_event_time,
            local_receive_time=receive_context.wall_time,
            local_receive_monotonic_ns=receive_context.monotonic_ns,
            estimated_latency_ms=estimated_latency_ms,
            aggregate_trade_id=raw.aggregate_trade_id,
            price=raw.price,
            quantity=raw.quantity,
            first_trade_id=raw.first_trade_id,
            last_trade_id=raw.last_trade_id,
            trade_time=utc_from_milliseconds(raw.trade_time_ms),
            # Binance `m` means "was the buyer the maker?" A maker buyer is
            # passive, so the seller was the aggressing taker (and vice versa).
            aggressor_side=(AggressorSide.SELL if raw.buyer_is_maker else AggressorSide.BUY),
            buyer_is_maker=raw.buyer_is_maker,
            source=TradeSource.AGGREGATE,
        )
    if isinstance(raw, BinanceTrade):
        # The individual-trade feed periodically emits an ``X=NA`` zero-price,
        # zero-quantity marker. It is transport activity, not a market trade.
        if raw.price <= 0 or raw.quantity <= 0:
            raise NonMarketTradeMarker("non-market individual trade marker")
        return TradeEvent(
            symbol=raw.symbol,
            exchange_event_time=exchange_event_time,
            local_receive_time=receive_context.wall_time,
            local_receive_monotonic_ns=receive_context.monotonic_ns,
            estimated_latency_ms=estimated_latency_ms,
            aggregate_trade_id=raw.trade_id,
            price=raw.price,
            quantity=raw.quantity,
            first_trade_id=raw.trade_id,
            last_trade_id=raw.trade_id,
            trade_time=utc_from_milliseconds(raw.transaction_time_ms),
            aggressor_side=(AggressorSide.SELL if raw.buyer_is_maker else AggressorSide.BUY),
            buyer_is_maker=raw.buyer_is_maker,
            source=TradeSource.INDIVIDUAL,
        )
    if isinstance(raw, BinanceBookTicker):
        return BestBidAskEvent(
            symbol=raw.symbol,
            exchange_event_time=exchange_event_time,
            local_receive_time=receive_context.wall_time,
            local_receive_monotonic_ns=receive_context.monotonic_ns,
            estimated_latency_ms=estimated_latency_ms,
            transaction_time=utc_from_milliseconds(raw.transaction_time_ms),
            update_id=raw.update_id,
            bid_price=raw.bid_price,
            bid_quantity=raw.bid_quantity,
            ask_price=raw.ask_price,
            ask_quantity=raw.ask_quantity,
        )
    return BookUpdateEvent(
        symbol=raw.symbol,
        exchange_event_time=exchange_event_time,
        local_receive_time=receive_context.wall_time,
        local_receive_monotonic_ns=receive_context.monotonic_ns,
        estimated_latency_ms=estimated_latency_ms,
        transaction_time=utc_from_milliseconds(raw.transaction_time_ms),
        first_update_id=raw.first_update_id,
        final_update_id=raw.final_update_id,
        previous_final_update_id=raw.previous_final_update_id,
        bids=raw.bids,
        asks=raw.asks,
    )


def calculate_backoff(initial_seconds: float, maximum_seconds: float, attempt: int) -> float:
    """Return deterministic bounded exponential delay for a one-based attempt."""

    if attempt < 1:
        raise ValueError("attempt must be at least 1")
    return min(maximum_seconds, initial_seconds * (2.0 ** (attempt - 1)))


class BinanceMarketWebSocket:
    """Streams normalized trades, BBO and diff-depth without authenticated APIs."""

    def __init__(
        self,
        symbol: str,
        *,
        base_url: str = "wss://fstream.binance.com",
        stale_after_seconds: float = 5.0,
        reconnect_initial_seconds: float = 1.0,
        reconnect_max_seconds: float = 30.0,
        connect_factory: ConnectFactory | None = None,
    ) -> None:
        self.symbol = symbol.strip().upper()
        stream_symbol = self.symbol.lower()
        streams = (
            f"{stream_symbol}@trade",
            f"{stream_symbol}@bookTicker",
            f"{stream_symbol}@depth@100ms",
        )
        self.stream_url = f"{base_url.rstrip('/')}/stream?streams={'/'.join(streams)}"
        self._stale_after_seconds = stale_after_seconds
        self._reconnect_initial_seconds = reconnect_initial_seconds
        self._reconnect_max_seconds = reconnect_max_seconds
        self._connect_factory = connect_factory or self._default_connect
        self._stop_event = asyncio.Event()
        self._connection: WebSocketConnection | None = None
        self._connected = False
        self._last_message_ns: int | None = None
        self._last_trade_ns: int | None = None
        self._last_book_ticker_ns: int | None = None
        self._last_depth_ns: int | None = None
        self._reconnect_count = 0
        self._last_error: str | None = None
        self._connection_generation = 0

    @staticmethod
    def _default_connect(url: str) -> AbstractAsyncContextManager[WebSocketConnection]:
        connection = connect(
            url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=10,
            max_queue=64,
            max_size=1_048_576,
        )
        return cast(AbstractAsyncContextManager[WebSocketConnection], connection)

    def health(self, *, now_monotonic_ns: int | None = None) -> ConnectionHealth:
        now_ns = (
            now_monotonic_ns if now_monotonic_ns is not None else ReceiveContext.now().monotonic_ns
        )

        def age(last_ns: int | None) -> float | None:
            return None if last_ns is None else max(0.0, (now_ns - last_ns) / 1_000_000_000)

        message_age = age(self._last_message_ns)
        trade_age = age(self._last_trade_ns)
        ticker_age = age(self._last_book_ticker_ns)
        depth_age = age(self._last_depth_ns)
        required_ages = (trade_age, ticker_age, depth_age)
        stale = (not self._connected) or any(
            item is None or item > self._stale_after_seconds for item in required_ages
        )
        return ConnectionHealth(
            connected=self._connected,
            stale=stale,
            last_message_age_seconds=message_age,
            aggregate_trade_age_seconds=trade_age,
            book_ticker_age_seconds=ticker_age,
            depth_age_seconds=depth_age,
            reconnect_count=self._reconnect_count,
            last_error=self._last_error,
        )

    async def reconnect(self, reason: str = "controlled public-data reconnect") -> None:
        """Close this symbol's socket without stopping its reconnect supervisor."""
        if self._connection is not None:
            await self._connection.close(reason=reason)

    async def stop(self) -> None:
        self._stop_event.set()
        if self._connection is not None:
            await self._connection.close(reason="application shutdown")

    async def run(
        self,
        handler: MarketEventHandler,
        *,
        lifecycle_handler: MarketLifecycleHandler | None = None,
    ) -> None:
        attempt = 0
        while not self._stop_event.is_set():
            disconnect_reason = "WebSocket connection closed"
            lifecycle_connected = False
            try:
                async with self._connect_factory(self.stream_url) as websocket:
                    self._connection = websocket
                    self._connected = True
                    self._last_error = None
                    self._connection_generation += 1
                    lifecycle_connected = True
                    logger.info("websocket_connected", symbol=self.symbol, url=self.stream_url)
                    if lifecycle_handler is not None:
                        await lifecycle_handler(
                            StreamLifecycleEvent(
                                kind=StreamLifecycleKind.CONNECTED,
                                connection_generation=self._connection_generation,
                                monotonic_ns=ReceiveContext.now().monotonic_ns,
                            )
                        )
                    received_valid_event = await self._consume(websocket, handler)
                    if received_valid_event:
                        attempt = 0
            except asyncio.CancelledError:
                raise
            except MalformedMarketMessageError:
                disconnect_reason = "malformed market message; capture integrity not guaranteed"
                raise
            except Exception as exc:
                if not self._stop_event.is_set():
                    self._last_error = f"{type(exc).__name__}: {exc}"
                    disconnect_reason = self._last_error
                    logger.warning(
                        "websocket_disconnected", symbol=self.symbol, error=self._last_error
                    )
            finally:
                self._connected = False
                self._connection = None
                if lifecycle_connected and lifecycle_handler is not None:
                    try:
                        await lifecycle_handler(
                            StreamLifecycleEvent(
                                kind=StreamLifecycleKind.DISCONNECTED,
                                connection_generation=self._connection_generation,
                                monotonic_ns=ReceiveContext.now().monotonic_ns,
                                reason=disconnect_reason,
                            )
                        )
                    except Exception as exc:
                        logger.exception(
                            "websocket_lifecycle_handler_failed",
                            symbol=self.symbol,
                            error=f"{type(exc).__name__}: {exc}",
                        )

            if self._stop_event.is_set():
                break
            attempt += 1
            self._reconnect_count += 1
            delay = calculate_backoff(
                self._reconnect_initial_seconds, self._reconnect_max_seconds, attempt
            )
            logger.info(
                "websocket_reconnect_attempt",
                symbol=self.symbol,
                attempt=attempt,
                delay_seconds=delay,
            )
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
            except TimeoutError:
                pass

    async def _consume(self, websocket: WebSocketConnection, handler: MarketEventHandler) -> bool:
        received_valid_event = False
        while not self._stop_event.is_set():
            # A quiet/broken socket must not wait many minutes for transport keepalive.
            raw_message = await asyncio.wait_for(
                websocket.recv(), timeout=self._stale_after_seconds
            )
            receive_context = ReceiveContext.now()
            try:
                decoded: Any = json.loads(raw_message)
                if not isinstance(decoded, dict):
                    raise ValueError("WebSocket message is not an object")
                payload = decoded.get("data", decoded)
                if not isinstance(payload, dict):
                    raise ValueError("combined-stream data is not an object")
                event = normalize_binance_ws_payload(payload, receive_context)
                if event.symbol != self.symbol:
                    raise ValueError(f"received symbol {event.symbol!r}, expected {self.symbol!r}")
            except NonMarketTradeMarker:
                logger.debug("websocket_non_market_marker", symbol=self.symbol)
                continue
            except (json.JSONDecodeError, UnicodeDecodeError, ValidationError, ValueError) as exc:
                logger.warning("websocket_message_rejected", symbol=self.symbol, error=str(exc))
                raise MalformedMarketMessageError(str(exc)) from exc

            received_valid_event = True
            self._last_message_ns = receive_context.monotonic_ns
            if isinstance(event, TradeEvent):
                self._last_trade_ns = receive_context.monotonic_ns
            elif isinstance(event, BestBidAskEvent):
                self._last_book_ticker_ns = receive_context.monotonic_ns
            elif isinstance(event, BookUpdateEvent):
                self._last_depth_ns = receive_context.monotonic_ns
            await handler(event)
        return received_valid_event
