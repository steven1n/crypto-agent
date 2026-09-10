"""Explicit basis-point transaction-cost assumptions."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from signals.models import SignalSide

BPS = Decimal(10_000)


class ExecutionAssumption(StrEnum):
    TAKER_TAKER = "TAKER_TAKER"
    MAKER_TAKER = "MAKER_TAKER"


@dataclass(frozen=True, slots=True)
class TransactionCostModel:
    maker_fee_bps: Decimal
    taker_fee_bps: Decimal
    entry_slippage_bps: Decimal
    exit_slippage_bps: Decimal
    assumption: ExecutionAssumption = ExecutionAssumption.TAKER_TAKER
    cross_spread: bool = True

    def __post_init__(self) -> None:
        values = (
            self.maker_fee_bps,
            self.taker_fee_bps,
            self.entry_slippage_bps,
            self.exit_slippage_bps,
        )
        if any(value < 0 for value in values):
            raise ValueError("fees and slippage cannot be negative")

    @property
    def entry_fee_bps(self) -> Decimal:
        if self.assumption is ExecutionAssumption.MAKER_TAKER:
            return self.maker_fee_bps
        return self.taker_fee_bps

    @property
    def exit_fee_bps(self) -> Decimal:
        return self.taker_fee_bps

    def executable_prices(
        self,
        *,
        side: SignalSide,
        entry_mid: Decimal,
        exit_mid: Decimal,
        entry_spread_bps: float,
        exit_spread_bps: float,
    ) -> tuple[Decimal, Decimal]:
        entry_cross = (
            Decimal(str(entry_spread_bps)) / 2
            if self.cross_spread and self.assumption is ExecutionAssumption.TAKER_TAKER
            else Decimal(0)
        )
        exit_cross = Decimal(str(exit_spread_bps)) / 2 if self.cross_spread else Decimal(0)
        entry_bps = self.entry_slippage_bps + entry_cross
        exit_bps = self.exit_slippage_bps + exit_cross
        if side is SignalSide.LONG:
            return (
                entry_mid * (Decimal(1) + entry_bps / BPS),
                exit_mid * (Decimal(1) - exit_bps / BPS),
            )
        return (
            entry_mid * (Decimal(1) - entry_bps / BPS),
            exit_mid * (Decimal(1) + exit_bps / BPS),
        )

    def round_trip_configured_cost_bps(self) -> Decimal:
        return (
            self.entry_fee_bps
            + self.exit_fee_bps
            + self.entry_slippage_bps
            + self.exit_slippage_bps
        )
