"""Strategy output models with no execution or future-label access."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class SignalSide(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


@dataclass(frozen=True, slots=True)
class SignalEvidence:
    name: str
    value: float


@dataclass(frozen=True, slots=True)
class Signal:
    symbol: str
    side: SignalSide
    strategy_name: str
    created_time: datetime
    monotonic_ns: int
    score: float
    evidence: tuple[SignalEvidence, ...]
    expected_horizon_ms: int
