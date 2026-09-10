"""Narrow public-market interfaces used by the application."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from market.events import MarketSnapshot, NormalizedMarketEvent

MarketEventHandler = Callable[[NormalizedMarketEvent], Awaitable[None]]


class StreamLifecycleKind(StrEnum):
    CONNECTED = "CONNECTED"
    DISCONNECTED = "DISCONNECTED"


@dataclass(frozen=True, slots=True)
class StreamLifecycleEvent:
    kind: StreamLifecycleKind
    connection_generation: int
    monotonic_ns: int
    reason: str | None = None


MarketLifecycleHandler = Callable[[StreamLifecycleEvent], Awaitable[None]]


class PublicMarketRestClient(Protocol):
    async def server_time_ms(self) -> int: ...

    async def book_snapshot(self, symbol: str, *, limit: int = 1_000) -> MarketSnapshot: ...

    async def aclose(self) -> None: ...


class PublicMarketStream(Protocol):
    async def run(
        self,
        handler: MarketEventHandler,
        *,
        lifecycle_handler: MarketLifecycleHandler | None = None,
    ) -> None: ...

    async def stop(self) -> None: ...
