"""wal -- append-only write-ahead-log key-value ledger."""

from .compact import CompactionHooks, CompactStats, compact
from .frame import CorruptionError, Record, SeqGapError
from .log import Hooks, WAL, iter_records, snapshot_at
from .recover import RecoverStats, recover

__all__ = [
    "WAL",
    "Hooks",
    "Record",
    "CorruptionError",
    "SeqGapError",
    "recover",
    "RecoverStats",
    "compact",
    "CompactionHooks",
    "CompactStats",
    "iter_records",
    "snapshot_at",
]
