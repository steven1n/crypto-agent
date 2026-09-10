"""Fixed-parameter research baselines; deliberately no fitting or optimization."""

from __future__ import annotations

from dataclasses import dataclass

from features.models import FeatureSnapshot
from signals.models import Signal, SignalEvidence, SignalSide


def _signal(
    feature: FeatureSnapshot,
    *,
    side: SignalSide,
    name: str,
    score: float,
    evidence: tuple[SignalEvidence, ...],
    horizon_ms: int,
) -> Signal:
    return Signal(
        symbol=feature.symbol,
        side=side,
        strategy_name=name,
        created_time=feature.created_at,
        monotonic_ns=feature.monotonic_ns,
        score=score,
        evidence=evidence,
        expected_horizon_ms=horizon_ms,
    )


@dataclass(frozen=True, slots=True)
class MicropriceMomentumStrategy:
    microprice_threshold_bps: float = 0.05
    imbalance_l1_threshold: float = 0.2
    expected_horizon_ms: int = 1_000
    name: str = "microprice_momentum"

    def evaluate(self, feature: FeatureSnapshot) -> Signal | None:
        if not feature.feature_valid or feature.book.microprice_offset_bps is None:
            return None
        microprice = feature.book.microprice_offset_bps
        imbalance = feature.book.at_depth(1).imbalance
        if imbalance is None:
            return None
        if microprice >= self.microprice_threshold_bps and imbalance >= self.imbalance_l1_threshold:
            side = SignalSide.LONG
        elif (
            microprice <= -self.microprice_threshold_bps
            and imbalance <= -self.imbalance_l1_threshold
        ):
            side = SignalSide.SHORT
        else:
            return None
        return _signal(
            feature,
            side=side,
            name=self.name,
            score=abs(microprice),
            evidence=(
                SignalEvidence("microprice_offset_bps", microprice),
                SignalEvidence("imbalance_l1", imbalance),
            ),
            horizon_ms=self.expected_horizon_ms,
        )


@dataclass(frozen=True, slots=True)
class BookImbalanceStrategy:
    threshold: float = 0.6
    expected_horizon_ms: int = 1_000
    name: str = "book_imbalance"

    def evaluate(self, feature: FeatureSnapshot) -> Signal | None:
        imbalance = feature.book.at_depth(5).imbalance
        if not feature.feature_valid or imbalance is None or abs(imbalance) < self.threshold:
            return None
        return _signal(
            feature,
            side=SignalSide.LONG if imbalance > 0 else SignalSide.SHORT,
            name=self.name,
            score=abs(imbalance),
            evidence=(SignalEvidence("imbalance_l5", imbalance),),
            horizon_ms=self.expected_horizon_ms,
        )


@dataclass(frozen=True, slots=True)
class TradeFlowStrategy:
    threshold: float = 0.6
    expected_horizon_ms: int = 1_000
    name: str = "trade_flow"

    def evaluate(self, feature: FeatureSnapshot) -> Signal | None:
        imbalance = feature.trade_flow_window(1_000).trade_imbalance
        if not feature.feature_valid or imbalance is None or abs(imbalance) < self.threshold:
            return None
        return _signal(
            feature,
            side=SignalSide.LONG if imbalance > 0 else SignalSide.SHORT,
            name=self.name,
            score=abs(imbalance),
            evidence=(SignalEvidence("trade_imbalance_1s", imbalance),),
            horizon_ms=self.expected_horizon_ms,
        )


@dataclass(frozen=True, slots=True)
class CombinedBaselineStrategy:
    microprice_threshold_bps: float = 0.03
    book_threshold: float = 0.4
    trade_threshold: float = 0.4
    expected_horizon_ms: int = 1_000
    name: str = "combined_baseline"

    def evaluate(self, feature: FeatureSnapshot) -> Signal | None:
        microprice = feature.book.microprice_offset_bps
        book = feature.book.at_depth(5).imbalance
        trade = feature.trade_flow_window(1_000).trade_imbalance
        if not feature.feature_valid or None in (microprice, book, trade):
            return None
        assert microprice is not None and book is not None and trade is not None
        if (
            microprice >= self.microprice_threshold_bps
            and book >= self.book_threshold
            and trade >= self.trade_threshold
        ):
            side = SignalSide.LONG
        elif (
            microprice <= -self.microprice_threshold_bps
            and book <= -self.book_threshold
            and trade <= -self.trade_threshold
        ):
            side = SignalSide.SHORT
        else:
            return None
        score = min(
            abs(microprice) / self.microprice_threshold_bps,
            abs(book) / self.book_threshold,
            abs(trade) / self.trade_threshold,
        )
        return _signal(
            feature,
            side=side,
            name=self.name,
            score=score,
            evidence=(
                SignalEvidence("microprice_offset_bps", microprice),
                SignalEvidence("imbalance_l5", book),
                SignalEvidence("trade_imbalance_1s", trade),
            ),
            horizon_ms=self.expected_horizon_ms,
        )
