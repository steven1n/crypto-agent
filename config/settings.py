"""Typed, environment-driven application settings."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Annotated

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Exchange(StrEnum):
    BINANCE = "binance"


class MarketType(StrEnum):
    PERPETUAL = "perpetual"


class LogLevel(StrEnum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class Settings(BaseSettings):
    """Runtime configuration loaded from environment variables or ``.env``."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        case_sensitive=False,
        extra="ignore",
    )

    exchange: Exchange = Exchange.BINANCE
    symbol: str = "BTCUSDC"
    symbols: Annotated[tuple[str, ...], NoDecode] = ("BTCUSDT", "BTCUSDC")
    market_type: MarketType = MarketType.PERPETUAL
    data_directory: Path = Path("data")
    log_level: LogLevel = LogLevel.INFO
    trading_enabled: bool = False

    binance_rest_base_url: str = "https://fapi.binance.com"
    binance_ws_base_url: str = "wss://fstream.binance.com"
    rest_timeout_seconds: float = Field(default=10.0, gt=0)
    ws_stale_after_seconds: float = Field(default=5.0, gt=0)
    ws_reconnect_initial_seconds: float = Field(default=1.0, gt=0)
    ws_reconnect_max_seconds: float = Field(default=30.0, gt=0)
    orderbook_stale_after_ms: int = Field(default=3_000, gt=0)
    orderbook_max_buffered_events: int = Field(default=50_000, gt=0)
    orderbook_max_stored_levels: int | None = Field(default=None, gt=0)
    orderbook_resync_initial_seconds: float = Field(default=0.5, gt=0)
    orderbook_resync_max_seconds: float = Field(default=10.0, gt=0)
    feature_interval_ms: int = Field(default=100, gt=0)
    trade_stale_after_ms: int = Field(default=5_000, gt=0)
    clock_sync_interval_seconds: float = Field(default=300.0, gt=0)
    clock_sync_sample_count: int = Field(default=5, ge=1, le=100)
    clock_sync_max_rtt_ms: float = Field(default=1_000.0, gt=0)
    mid_history_seconds: float = Field(default=15.0, ge=10.0)
    trade_history_seconds: float = Field(default=15.0, ge=10.0)
    feature_imbalance_levels: Annotated[tuple[int, ...], NoDecode] = (1, 5, 10)
    feature_recorder_batch_size: int = Field(default=6_000, gt=0)
    feature_recorder_flush_interval_seconds: float = Field(default=60.0, gt=0)
    feature_recorder_queue_size: int = Field(default=10_000, gt=0)
    raw_recorder_batch_size: int = Field(default=20_000, gt=0)
    raw_recorder_flush_interval_seconds: float = Field(default=30.0, gt=0)
    raw_recorder_queue_size: int = Field(default=100_000, gt=0)
    min_free_disk_gb: float = Field(default=2.0, ge=0)
    health_report_interval_seconds: float = Field(default=60.0, gt=0)
    capture_duration_seconds: float | None = Field(default=None, gt=0)
    orderbook_hard_level_limit: int = Field(default=100_000, ge=1_000)

    @field_validator("symbols", mode="before")
    @classmethod
    def parse_symbols(cls, value: object) -> object:
        if isinstance(value, str):
            return tuple(item.strip().upper() for item in value.split(",") if item.strip())
        return value

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        symbol = value.strip().upper()
        if not symbol or not symbol.isalnum():
            raise ValueError("SYMBOL must contain only letters and numbers")
        return symbol

    @field_validator("binance_rest_base_url", "binance_ws_base_url")
    @classmethod
    def strip_url_suffix(cls, value: str) -> str:
        return value.rstrip("/")

    @field_validator("feature_imbalance_levels", mode="before")
    @classmethod
    def parse_imbalance_levels(cls, value: object) -> object:
        if isinstance(value, str):
            return tuple(int(item.strip()) for item in value.split(",") if item.strip())
        return value

    @model_validator(mode="after")
    def enforce_phase_one_safety(self) -> Settings:
        if not self.symbols or len(set(self.symbols)) != len(self.symbols):
            raise ValueError("SYMBOLS must be nonempty and unique")
        if not set(self.symbols).issubset({"BTCUSDT", "BTCUSDC"}):
            raise ValueError("this capture milestone supports BTCUSDT and BTCUSDC")
        if self.trading_enabled:
            raise ValueError(
                "TRADING_ENABLED=true is forbidden in Phase 1; no live execution engine exists"
            )
        if self.ws_reconnect_max_seconds < self.ws_reconnect_initial_seconds:
            raise ValueError(
                "WS_RECONNECT_MAX_SECONDS must be greater than or equal to "
                "WS_RECONNECT_INITIAL_SECONDS"
            )
        if self.orderbook_resync_max_seconds < self.orderbook_resync_initial_seconds:
            raise ValueError(
                "ORDERBOOK_RESYNC_MAX_SECONDS must be greater than or equal to "
                "ORDERBOOK_RESYNC_INITIAL_SECONDS"
            )
        levels = self.feature_imbalance_levels
        if not levels or any(level <= 0 for level in levels):
            raise ValueError("FEATURE_IMBALANCE_LEVELS must contain positive integers")
        if tuple(sorted(set(levels))) != levels:
            raise ValueError("FEATURE_IMBALANCE_LEVELS must be sorted and unique")
        if not {1, 5, 10}.issubset(levels):
            raise ValueError("FEATURE_IMBALANCE_LEVELS must include 1, 5, and 10")
        if self.orderbook_max_stored_levels is not None and self.orderbook_max_stored_levels < max(
            levels
        ):
            raise ValueError("ORDERBOOK_MAX_STORED_LEVELS must cover FEATURE_IMBALANCE_LEVELS")
        if self.mid_history_seconds < 10 + self.feature_interval_ms / 1_000:
            raise ValueError("MID_HISTORY_SECONDS must cover the 10-second return horizon")
        if self.trade_history_seconds < 10:
            raise ValueError("TRADE_HISTORY_SECONDS must cover the 10-second trade window")
        return self
