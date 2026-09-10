"""Unauthenticated Binance USD-M Futures REST client."""

from __future__ import annotations

from datetime import UTC, datetime
from time import monotonic_ns
from typing import Any

import httpx
from pydantic import ValidationError

from exchange.binance.models import (
    BinanceDepthSnapshot,
    BinanceExchangeInfo,
    BinanceServerTime,
    BinanceSymbolInfo,
)
from exchange.binance.symbols import SymbolRules, SymbolValidationError, extract_symbol_rules
from market.events import MarketSnapshot, utc_from_milliseconds


class BinancePublicApiError(RuntimeError):
    """A transport, HTTP, exchange, or schema failure from a public endpoint."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        exchange_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.exchange_code = exchange_code


class BinancePublicRestClient:
    """Minimal public-only REST surface needed by the market-data pipeline."""

    _DEPTH_LIMITS = frozenset({5, 10, 20, 50, 100, 500, 1_000})

    def __init__(
        self,
        *,
        base_url: str = "https://fapi.binance.com",
        timeout_seconds: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=timeout_seconds,
            headers={"User-Agent": "crypto-agent/0.1 public-market-data"},
        )

    async def __aenter__(self) -> BinancePublicRestClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def _get(self, path: str, *, params: dict[str, str | int] | None = None) -> Any:
        try:
            response = await self._client.get(f"{self._base_url}{path}", params=params)
        except httpx.HTTPError as exc:
            raise BinancePublicApiError(f"Binance REST transport failure: {exc}") from exc

        if response.is_error:
            exchange_code: int | None = None
            message = response.text
            try:
                error_payload = response.json()
                if isinstance(error_payload, dict):
                    raw_code = error_payload.get("code")
                    if isinstance(raw_code, int):
                        exchange_code = raw_code
                    raw_message = error_payload.get("msg")
                    if isinstance(raw_message, str):
                        message = raw_message
            except ValueError:
                pass
            raise BinancePublicApiError(
                f"Binance REST returned HTTP {response.status_code}: {message}",
                status_code=response.status_code,
                exchange_code=exchange_code,
            )

        try:
            return response.json()
        except ValueError as exc:
            raise BinancePublicApiError("Binance REST returned invalid JSON") from exc

    async def server_time_ms(self) -> int:
        payload = await self._get("/fapi/v1/time")
        try:
            return BinanceServerTime.model_validate(payload).server_time
        except ValidationError as exc:
            raise BinancePublicApiError("invalid Binance server-time response") from exc

    async def exchange_info(self) -> BinanceExchangeInfo:
        payload = await self._get("/fapi/v1/exchangeInfo")
        try:
            return BinanceExchangeInfo.model_validate(payload)
        except ValidationError as exc:
            raise BinancePublicApiError("invalid Binance exchange-info response") from exc

    async def symbol_info(self, symbol: str) -> BinanceSymbolInfo:
        normalized = symbol.strip().upper()
        info = await self.exchange_info()
        match = next((item for item in info.symbols if item.symbol == normalized), None)
        if match is None:
            raise SymbolValidationError(f"symbol {normalized!r} is not listed on Binance USD-M")
        return match

    async def validate_symbol(self, symbol: str) -> SymbolRules:
        return extract_symbol_rules(await self.symbol_info(symbol))

    async def book_snapshot(self, symbol: str, *, limit: int = 1_000) -> MarketSnapshot:
        if limit not in self._DEPTH_LIMITS:
            allowed = ", ".join(str(item) for item in sorted(self._DEPTH_LIMITS))
            raise ValueError(f"invalid Binance depth limit {limit}; expected one of {allowed}")

        payload = await self._get(
            "/fapi/v1/depth",
            params={"symbol": symbol.strip().upper(), "limit": limit},
        )
        received_monotonic_ns = monotonic_ns()
        received_at = datetime.now(UTC)
        try:
            snapshot = BinanceDepthSnapshot.model_validate(payload)
        except ValidationError as exc:
            raise BinancePublicApiError("invalid Binance depth response") from exc

        return MarketSnapshot(
            symbol=symbol.strip().upper(),
            last_update_id=snapshot.last_update_id,
            bids=snapshot.bids,
            asks=snapshot.asks,
            created_at=received_at,
            created_monotonic_ns=received_monotonic_ns,
            exchange_event_time=(
                None
                if snapshot.message_output_time is None
                else utc_from_milliseconds(snapshot.message_output_time)
            ),
            exchange_transaction_time=(
                None
                if snapshot.transaction_time is None
                else utc_from_milliseconds(snapshot.transaction_time)
            ),
        )
