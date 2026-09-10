"""Exact REST response used by one synchronization attempt."""

from dataclasses import dataclass
from datetime import datetime

from market.events import MarketSnapshot


@dataclass(frozen=True, slots=True)
class BootstrapSnapshot:
    snapshot: MarketSnapshot
    connection_generation: int
    sync_generation: int
    request_start_time: datetime
    request_start_monotonic_ns: int
    response_receive_time: datetime
    response_receive_monotonic_ns: int
    snapshot_limit: int
    resync_reason: str | None
    clock_offset_ms: float | None
    # IDs identify which already-recorded diff events were buffered when the
    # response was installed. No prices/quantities are duplicated in this list.
    buffered_final_update_ids: tuple[int, ...]
