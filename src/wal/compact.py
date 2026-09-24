"""Log compaction.

Rewrites the log keeping only the latest op per key (tombstones are kept, so
the compacted log still replays to the exact same final state).  Retained
records are renumbered to a contiguous sequence starting at 1.

Crash-safe protocol (single writer, file states):

    main.log  --(hardlink)-->  main.log + main.log.old      [hook: before_rename]
    main.log.old --(rename)--> wal.log.compacting -> main.log
                                                              [hook: after_rename]
    fsync(dir)  ->  unlink(main.log.old)  ->  fsync(dir)      [hooks: before/after_delete_old]

At any crash point, recover()'s finish_pending_compaction() restores a
consistent single-log state, and the visible state is always either fully the
old log or fully the new log -- never a mix.
"""

from __future__ import annotations

import fcntl
import os
from dataclasses import dataclass

from .frame import (
    COMPACT_OLD,
    COMPACT_TMP,
    MAIN_LOG,
    Record,
    encode_record,
)
from .recover import finish_pending_compaction, scan_strict
from .recover import _fsync_dir  # shared helper, no circular import


class CompactionHooks:
    """Crash-injection points for compact(). Override in tests."""

    def before_rename(self) -> None:  # tmp written+fsynced, old link created
        pass

    def after_rename(self) -> None:  # tmp renamed onto main, dir fsynced
        pass

    def before_delete_old(self) -> None:  # about to unlink the old segment
        pass

    def after_delete_old(self) -> None:  # old segment unlinked, dir fsynced
        pass


@dataclass
class CompactStats:
    records_before: int
    records_after: int
    bytes_before: int
    bytes_after: int


def compact(dir_path: str, hooks: CompactionHooks | None = None) -> CompactStats:
    """Compact the log in ``dir_path``. See module docstring for the protocol."""
    hooks = hooks or CompactionHooks()
    lock_path = os.path.join(dir_path, "wal.lock")
    os.makedirs(dir_path, exist_ok=True)
    lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        return _compact_locked(dir_path, hooks)
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)


def _compact_locked(dir_path: str, hooks: CompactionHooks) -> CompactStats:
    finish_pending_compaction(dir_path)
    main = os.path.join(dir_path, MAIN_LOG)
    tmp = os.path.join(dir_path, COMPACT_TMP)
    old = os.path.join(dir_path, COMPACT_OLD)

    records: list[Record] = []
    bytes_before = 0
    if os.path.exists(main):
        scan = scan_strict(main)
        if scan.discarded_bytes > 0:
            # Torn tail present: truncate first so we never compact garbage in.
            with open(main, "r+b") as fh:
                fh.truncate(scan.valid_bytes)
                fh.flush()
                os.fsync(fh.fileno())
            _fsync_dir(dir_path)
        records = scan.records
        bytes_before = scan.valid_bytes

    latest: dict[str, Record] = {}
    for record in records:
        latest[record.key] = record
    kept = sorted(latest.values(), key=lambda record: record.seq)

    # Renumber retained records to a contiguous sequence starting at 1.
    blob = b"".join(
        encode_record(new_seq, record.op, record.key, record.value)
        for new_seq, record in enumerate(kept, start=1)
    )

    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        _write_all(fd, blob)
        os.fsync(fd)
    finally:
        os.close(fd)
    _fsync_dir(dir_path)  # make the tmp file durable before the rename dance

    hooks.before_rename()

    # Hardlink the current log to .old so a main.log name always exists
    # (or is restorable) at every instant; then atomically swap in the new log.
    if os.path.exists(main):
        if os.path.exists(old):
            os.unlink(old)
        os.link(main, old)
    os.rename(tmp, main)
    _fsync_dir(dir_path)  # rename is durable from here on

    hooks.after_rename()
    hooks.before_delete_old()

    if os.path.exists(old):
        os.unlink(old)  # old segment removed only after the rename persisted
        _fsync_dir(dir_path)

    hooks.after_delete_old()

    return CompactStats(
        records_before=len(records),
        records_after=len(kept),
        bytes_before=bytes_before,
        bytes_after=len(blob),
    )


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        view = view[written:]
