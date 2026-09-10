"""Incremental trade-event flow windows using monotonic expiry."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from decimal import Decimal

from features.models import TradeFlowMetrics
from market.events import AggressorSide, TradeEvent


@dataclass(frozen=True, slots=True)
class TradeObservation:
    monotonic_ns: int
    side: AggressorSide
    quantity: Decimal
    price: Decimal


class RollingTradeWindow:
    def __init__(self, window_ms: int, *, max_observations: int = 200_000) -> None:
        if window_ms <= 0:
            raise ValueError("window_ms must be positive")
        self.window_ms = window_ms
        self._max_observations = max_observations
        self._window_ns = window_ms * 1_000_000
        self._trades: deque[TradeObservation] = deque()
        self._buy_volume = Decimal(0)
        self._sell_volume = Decimal(0)
        self._notional = Decimal(0)

    def __len__(self) -> int:
        return len(self._trades)

    def clear(self) -> None:
        self._trades.clear()
        self._buy_volume = Decimal(0)
        self._sell_volume = Decimal(0)
        self._notional = Decimal(0)

    def add(self, trade: TradeEvent) -> None:
        if trade.quantity < 0 or trade.price <= 0:
            raise ValueError("trade price must be positive and quantity non-negative")
        if self._trades and trade.local_receive_monotonic_ns < self._trades[-1].monotonic_ns:
            raise ValueError("trade events must arrive in monotonic order")
        observation = TradeObservation(
            monotonic_ns=trade.local_receive_monotonic_ns,
            side=trade.aggressor_side,
            quantity=trade.quantity,
            price=trade.price,
        )
        self._trades.append(observation)
        if observation.side is AggressorSide.BUY:
            self._buy_volume += observation.quantity
        else:
            self._sell_volume += observation.quantity
        self._notional += observation.price * observation.quantity
        self._evict(trade.local_receive_monotonic_ns)
        if len(self._trades) > self._max_observations:
            raise RuntimeError("trade window exceeded bounded capacity")

    def _evict(self, now_ns: int) -> None:
        cutoff = now_ns - self._window_ns
        while self._trades and self._trades[0].monotonic_ns < cutoff:
            expired = self._trades.popleft()
            if expired.side is AggressorSide.BUY:
                self._buy_volume -= expired.quantity
            else:
                self._sell_volume -= expired.quantity
            self._notional -= expired.price * expired.quantity

    def snapshot(self, *, now_ns: int, available: bool) -> TradeFlowMetrics:
        self._evict(now_ns)
        if not available:
            return TradeFlowMetrics(
                window_ms=self.window_ms,
                available=False,
                aggressive_buy_volume=None,
                aggressive_sell_volume=None,
                signed_volume=None,
                total_volume=None,
                trade_event_count=None,
                trade_imbalance=None,
                buy_ratio=None,
                volume_weighted_price=None,
            )
        total = self._buy_volume + self._sell_volume
        signed = self._buy_volume - self._sell_volume
        return TradeFlowMetrics(
            window_ms=self.window_ms,
            available=True,
            aggressive_buy_volume=self._buy_volume,
            aggressive_sell_volume=self._sell_volume,
            signed_volume=signed,
            total_volume=total,
            trade_event_count=len(self._trades),
            trade_imbalance=None if total == 0 else float(signed / total),
            buy_ratio=None if total == 0 else float(self._buy_volume / total),
            volume_weighted_price=None if total == 0 else self._notional / total,
        )


class TradeFlowTracker:
    def __init__(self, windows_ms: tuple[int, ...] = (1_000, 5_000, 10_000)) -> None:
        if not windows_ms or tuple(sorted(set(windows_ms))) != windows_ms:
            raise ValueError("windows_ms must be sorted and unique")
        self._windows = tuple(RollingTradeWindow(window) for window in windows_ms)

    @property
    def history_size(self) -> int:
        return max((len(window) for window in self._windows), default=0)

    def clear(self) -> None:
        for window in self._windows:
            window.clear()

    def add(self, trade: TradeEvent) -> None:
        for window in self._windows:
            window.add(trade)

    def snapshot(self, *, now_ns: int, available: bool) -> tuple[TradeFlowMetrics, ...]:
        return tuple(
            window.snapshot(now_ns=now_ns, available=available) for window in self._windows
        )
