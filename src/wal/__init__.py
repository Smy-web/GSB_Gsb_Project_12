"""wal: an append-only WAL key-value ledger."""

from .compact import CompactStats, compact
from .frame import (
    CorruptionError,
    InjectedCrash,
    LockedError,
    SeqGapError,
    WALError,
)
from .log import WAL, Record, get, iter_records, snapshot_at
from .recover import RecoveryStats, recover

__all__ = [
    "WAL",
    "Record",
    "iter_records",
    "snapshot_at",
    "get",
    "recover",
    "RecoveryStats",
    "compact",
    "CompactStats",
    "WALError",
    "CorruptionError",
    "SeqGapError",
    "LockedError",
    "InjectedCrash",
]
