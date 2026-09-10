"""Narrow strategy interface over present-time features only."""

from __future__ import annotations

from typing import Protocol

from features.models import FeatureSnapshot
from signals.models import Signal


class Strategy(Protocol):
    name: str

    def evaluate(self, feature: FeatureSnapshot) -> Signal | None: ...
