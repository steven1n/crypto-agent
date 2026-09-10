from pathlib import Path

import pytest
from pydantic import ValidationError

from config.settings import Exchange, MarketType, Settings


def test_settings_have_safe_phase_one_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRADING_ENABLED", raising=False)
    settings = Settings(_env_file=None)

    assert settings.exchange is Exchange.BINANCE
    assert settings.symbol == "BTCUSDC"
    assert settings.market_type is MarketType.PERPETUAL
    assert settings.data_directory == Path("data")
    assert settings.trading_enabled is False
    assert settings.orderbook_stale_after_ms == 3_000
    assert settings.orderbook_max_buffered_events == 50_000
    assert settings.orderbook_max_stored_levels is None
    assert settings.feature_interval_ms == 100
    assert settings.feature_imbalance_levels == (1, 5, 10)
    assert settings.feature_recorder_batch_size == 6_000
    assert settings.symbols == ("BTCUSDT", "BTCUSDC")


def test_symbol_is_normalized(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYMBOL", " btcusdc ")
    assert Settings(_env_file=None).symbol == "BTCUSDC"


def test_live_trading_cannot_be_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRADING_ENABLED", "true")
    with pytest.raises(ValidationError, match="forbidden in Phase 1"):
        Settings(_env_file=None)


def test_reconnect_bounds_are_validated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WS_RECONNECT_INITIAL_SECONDS", "10")
    monkeypatch.setenv("WS_RECONNECT_MAX_SECONDS", "5")
    with pytest.raises(ValidationError, match="WS_RECONNECT_MAX_SECONDS"):
        Settings(_env_file=None)


def test_empty_optional_level_limit_uses_unbounded_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ORDERBOOK_MAX_STORED_LEVELS", "")
    assert Settings(_env_file=None).orderbook_max_stored_levels is None


def test_imbalance_levels_are_typed_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FEATURE_IMBALANCE_LEVELS", "1,5,10,20")
    assert Settings(_env_file=None).feature_imbalance_levels == (1, 5, 10, 20)
