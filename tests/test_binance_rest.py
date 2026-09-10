from decimal import Decimal

import httpx
import pytest

from exchange.binance.rest import BinancePublicApiError, BinancePublicRestClient
from exchange.binance.symbols import SymbolValidationError


def exchange_info_payload() -> dict[str, object]:
    return {
        "timezone": "UTC",
        "serverTime": 1_725_000_000_000,
        "symbols": [
            {
                "symbol": "BTCUSDC",
                "pair": "BTCUSDC",
                "contractType": "PERPETUAL",
                "status": "TRADING",
                "baseAsset": "BTC",
                "quoteAsset": "USDC",
                "marginAsset": "USDC",
                "pricePrecision": 1,
                "quantityPrecision": 3,
                "filters": [
                    {
                        "filterType": "PRICE_FILTER",
                        "minPrice": "0.10",
                        "maxPrice": "1000000",
                        "tickSize": "0.10",
                    },
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.001",
                        "maxQty": "1000",
                        "stepSize": "0.001",
                    },
                    {"filterType": "MIN_NOTIONAL", "notional": "5"},
                ],
            }
        ],
    }


@pytest.fixture
def rest_client() -> BinancePublicRestClient:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/fapi/v1/time":
            return httpx.Response(200, json={"serverTime": 1_725_000_000_123})
        if request.url.path == "/fapi/v1/exchangeInfo":
            return httpx.Response(200, json=exchange_info_payload())
        if request.url.path == "/fapi/v1/depth":
            assert request.url.params["symbol"] == "BTCUSDC"
            assert request.url.params["limit"] == "1000"
            return httpx.Response(
                200,
                json={
                    "lastUpdateId": 123,
                    "E": 1_725_000_000_100,
                    "T": 1_725_000_000_099,
                    "bids": [["60000.10", "1.250"]],
                    "asks": [["60000.20", "0.750"]],
                },
            )
        return httpx.Response(404, json={"code": -1000, "msg": "unknown endpoint"})

    transport = httpx.MockTransport(handler)
    return BinancePublicRestClient(client=httpx.AsyncClient(transport=transport))


async def test_server_time(rest_client: BinancePublicRestClient) -> None:
    assert await rest_client.server_time_ms() == 1_725_000_000_123


async def test_validate_symbol_extracts_decimal_rules(
    rest_client: BinancePublicRestClient,
) -> None:
    rules = await rest_client.validate_symbol("btcusdc")

    assert rules.tick_size == Decimal("0.10")
    assert rules.step_size == Decimal("0.001")
    assert rules.min_quantity == Decimal("0.001")
    assert rules.min_notional == Decimal("5")
    assert isinstance(rules.tick_size, Decimal)


async def test_missing_symbol_is_rejected(rest_client: BinancePublicRestClient) -> None:
    with pytest.raises(SymbolValidationError, match="not listed"):
        await rest_client.validate_symbol("NOTREAL")


async def test_depth_snapshot_is_normalized(rest_client: BinancePublicRestClient) -> None:
    snapshot = await rest_client.book_snapshot("btcusdc")

    assert snapshot.symbol == "BTCUSDC"
    assert snapshot.last_update_id == 123
    assert snapshot.bids == ((Decimal("60000.10"), Decimal("1.250")),)
    assert snapshot.asks == ((Decimal("60000.20"), Decimal("0.750")),)


async def test_binance_error_details_are_preserved() -> None:
    transport = httpx.MockTransport(
        lambda _: httpx.Response(400, json={"code": -1121, "msg": "Invalid symbol."})
    )
    client = BinancePublicRestClient(client=httpx.AsyncClient(transport=transport))

    with pytest.raises(BinancePublicApiError) as exc_info:
        await client.server_time_ms()

    assert exc_info.value.status_code == 400
    assert exc_info.value.exchange_code == -1121


def test_invalid_depth_limit_is_rejected_before_network(
    rest_client: BinancePublicRestClient,
) -> None:
    async def call() -> None:
        await rest_client.book_snapshot("BTCUSDC", limit=42)

    with pytest.raises(ValueError, match="invalid Binance depth limit"):
        import asyncio

        asyncio.run(call())
