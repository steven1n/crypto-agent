"""Replay-only one-position shadow execution with explicit costs."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from execution.costs import BPS, ExecutionAssumption, TransactionCostModel
from features.models import FeatureSnapshot
from signals.models import Signal, SignalSide


class ExitReason(StrEnum):
    TIME_STOP = "TIME_STOP"
    END_OF_DATA = "END_OF_DATA"


@dataclass(frozen=True, slots=True)
class ShadowTrade:
    symbol: str
    strategy_name: str
    side: SignalSide
    entry_time: datetime
    exit_time: datetime
    entry_monotonic_ns: int
    exit_monotonic_ns: int
    notional: Decimal
    quantity: Decimal
    entry_mid: Decimal
    simulated_entry_price: Decimal
    exit_mid: Decimal
    simulated_exit_price: Decimal
    gross_pnl: Decimal
    fees: Decimal
    slippage_cost: Decimal
    net_pnl: Decimal
    gross_edge_bps: float
    net_edge_bps: float
    holding_time_ms: float
    mfe_bps: float
    mae_bps: float
    exit_reason: ExitReason


@dataclass(slots=True)
class _OpenPosition:
    signal: Signal
    feature: FeatureSnapshot
    notional: Decimal
    exit_due_ns: int
    path: list[Decimal] = field(default_factory=list)


class ShadowExecutionSimulator:
    def __init__(
        self,
        cost_model: TransactionCostModel,
        *,
        fixed_notional: Decimal,
        holding_period_ms: int,
    ) -> None:
        if fixed_notional <= 0 or holding_period_ms <= 0:
            raise ValueError("notional and holding period must be positive")
        self.cost_model = cost_model
        self.fixed_notional = fixed_notional
        self.holding_period_ms = holding_period_ms
        self._position: _OpenPosition | None = None
        self._trades: list[ShadowTrade] = []

    @property
    def has_position(self) -> bool:
        return self._position is not None

    @property
    def trades(self) -> tuple[ShadowTrade, ...]:
        return tuple(self._trades)

    def enter(
        self,
        signal: Signal,
        feature: FeatureSnapshot,
        *,
        maker_filled: bool = False,
    ) -> bool:
        if self._position is not None:
            return False
        if signal.symbol != feature.symbol or signal.monotonic_ns != feature.monotonic_ns:
            raise ValueError("signal and entry feature are not aligned")
        if self.cost_model.assumption is ExecutionAssumption.MAKER_TAKER and not maker_filled:
            return False
        self._position = _OpenPosition(
            signal=signal,
            feature=feature,
            notional=self.fixed_notional,
            exit_due_ns=feature.monotonic_ns + self.holding_period_ms * 1_000_000,
            path=[feature.book.mid_price],
        )
        return True

    def observe(self, feature: FeatureSnapshot) -> ShadowTrade | None:
        position = self._position
        if position is None:
            return None
        if feature.symbol != position.signal.symbol:
            raise ValueError("position and feature symbols differ")
        position.path.append(feature.book.mid_price)
        if feature.monotonic_ns < position.exit_due_ns:
            return None
        return self._close(feature, ExitReason.TIME_STOP)

    def close_end_of_data(self, feature: FeatureSnapshot) -> ShadowTrade | None:
        if self._position is None:
            return None
        return self._close(feature, ExitReason.END_OF_DATA)

    def _close(self, exit_feature: FeatureSnapshot, reason: ExitReason) -> ShadowTrade:
        position = self._position
        assert position is not None
        entry = position.feature
        side = position.signal.side
        quantity = position.notional / entry.book.mid_price
        simulated_entry, simulated_exit = self.cost_model.executable_prices(
            side=side,
            entry_mid=entry.book.mid_price,
            exit_mid=exit_feature.book.mid_price,
            entry_spread_bps=entry.book.spread_bps,
            exit_spread_bps=exit_feature.book.spread_bps,
        )
        sign = Decimal(1) if side is SignalSide.LONG else Decimal(-1)
        gross_pnl = sign * quantity * (exit_feature.book.mid_price - entry.book.mid_price)
        simulated_pnl_before_fees = sign * quantity * (simulated_exit - simulated_entry)
        slippage_cost = gross_pnl - simulated_pnl_before_fees
        fees = quantity * (
            simulated_entry * self.cost_model.entry_fee_bps / BPS
            + simulated_exit * self.cost_model.exit_fee_bps / BPS
        )
        net_pnl = simulated_pnl_before_fees - fees
        path_bps = [
            math.log(float(price / entry.book.mid_price))
            * 10_000
            * (1 if side is SignalSide.LONG else -1)
            for price in position.path
        ]
        trade = ShadowTrade(
            symbol=entry.symbol,
            strategy_name=position.signal.strategy_name,
            side=side,
            entry_time=entry.created_at,
            exit_time=exit_feature.created_at,
            entry_monotonic_ns=entry.monotonic_ns,
            exit_monotonic_ns=exit_feature.monotonic_ns,
            notional=position.notional,
            quantity=quantity,
            entry_mid=entry.book.mid_price,
            simulated_entry_price=simulated_entry,
            exit_mid=exit_feature.book.mid_price,
            simulated_exit_price=simulated_exit,
            gross_pnl=gross_pnl,
            fees=fees,
            slippage_cost=slippage_cost,
            net_pnl=net_pnl,
            gross_edge_bps=float(gross_pnl / position.notional * BPS),
            net_edge_bps=float(net_pnl / position.notional * BPS),
            holding_time_ms=(exit_feature.monotonic_ns - entry.monotonic_ns) / 1_000_000,
            mfe_bps=max(path_bps),
            mae_bps=min(path_bps),
            exit_reason=reason,
        )
        self._trades.append(trade)
        self._position = None
        return trade
