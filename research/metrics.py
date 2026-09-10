"""Cost-aware summary metrics for non-overlapping shadow trades."""

from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal
from statistics import fmean, median

from execution.shadow import ShadowTrade
from signals.models import SignalSide


@dataclass(frozen=True, slots=True)
class StrategyMetrics:
    signal_count: int
    trade_count: int
    long_count: int
    short_count: int
    gross_pnl: Decimal
    fees: Decimal
    slippage_cost: Decimal
    net_pnl: Decimal
    average_net_pnl: float | None
    median_net_pnl: float | None
    win_rate: float | None
    average_win: float | None
    average_loss: float | None
    profit_factor: float | None
    maximum_drawdown: float
    maximum_consecutive_losses: int
    average_holding_time_ms: float | None
    average_mfe_bps: float | None
    average_mae_bps: float | None
    turnover: Decimal
    cost_as_percentage_of_gross_edge: float | None
    gross_edge_per_trade_bps: float | None
    break_even_round_trip_cost_bps: float | None


def calculate_strategy_metrics(
    trades: tuple[ShadowTrade, ...], *, signal_count: int
) -> StrategyMetrics:
    net_values = [float(trade.net_pnl) for trade in trades]
    wins = [value for value in net_values if value > 0]
    losses = [value for value in net_values if value < 0]
    cumulative = 0.0
    peak = 0.0
    maximum_drawdown = 0.0
    consecutive = 0
    maximum_consecutive = 0
    for value in net_values:
        cumulative += value
        peak = max(peak, cumulative)
        maximum_drawdown = max(maximum_drawdown, peak - cumulative)
        if value < 0:
            consecutive += 1
            maximum_consecutive = max(maximum_consecutive, consecutive)
        else:
            consecutive = 0
    gross = sum((trade.gross_pnl for trade in trades), Decimal(0))
    fees = sum((trade.fees for trade in trades), Decimal(0))
    slippage = sum((trade.slippage_cost for trade in trades), Decimal(0))
    net = sum((trade.net_pnl for trade in trades), Decimal(0))
    gross_edges = [trade.gross_edge_bps for trade in trades]
    average_gross_edge = None if not gross_edges else fmean(gross_edges)
    total_cost = float(fees + slippage)
    return StrategyMetrics(
        signal_count=signal_count,
        trade_count=len(trades),
        long_count=sum(trade.side is SignalSide.LONG for trade in trades),
        short_count=sum(trade.side is SignalSide.SHORT for trade in trades),
        gross_pnl=gross,
        fees=fees,
        slippage_cost=slippage,
        net_pnl=net,
        average_net_pnl=None if not net_values else fmean(net_values),
        median_net_pnl=None if not net_values else median(net_values),
        win_rate=None if not net_values else len(wins) / len(net_values),
        average_win=None if not wins else fmean(wins),
        average_loss=None if not losses else fmean(losses),
        profit_factor=(
            (math.inf if wins else None) if not losses else sum(wins) / abs(sum(losses))
        ),
        maximum_drawdown=maximum_drawdown,
        maximum_consecutive_losses=maximum_consecutive,
        average_holding_time_ms=(
            None if not trades else fmean(trade.holding_time_ms for trade in trades)
        ),
        average_mfe_bps=None if not trades else fmean(trade.mfe_bps for trade in trades),
        average_mae_bps=None if not trades else fmean(trade.mae_bps for trade in trades),
        turnover=sum((trade.notional * 2 for trade in trades), Decimal(0)),
        cost_as_percentage_of_gross_edge=(None if gross <= 0 else total_cost / float(gross) * 100),
        gross_edge_per_trade_bps=average_gross_edge,
        break_even_round_trip_cost_bps=average_gross_edge,
    )
