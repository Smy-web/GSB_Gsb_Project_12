"""Append-only WAL writer/reader.

Single writer: the append file descriptor holds an exclusive fcntl.flock for
its whole lifetime.  Readers open the file read-only and may observe any
fsync'ed prefix (a partially-written tail is simply not visible to them).
"""

from __future__ import annotations

import fcntl
import os
import time
from collections.abc import Iterator

from .frame import MAIN_LOG, Record, SeqGapError, encode_record
from .recover import finish_pending_compaction, recover, scan_strict
from .recover import _fsync_dir

DURABILITY_MODES = ("none", "batch", "every")


class Hooks:
    """Fault-injection seam for durability tests. Override in tests."""

    def before_fsync(self, fd: int) -> None:
        pass

    def after_write(self, fd: int) -> None:
        pass


class WAL:
    """Append-only write-ahead log in ``dir_path``.

    durability:
      "none"  -- never fsync on append (data may vanish on crash).
      "batch" -- fsync every ``batch_n`` appends or every ``batch_ms``
                 milliseconds, whichever comes first.
      "every" -- fsync before every append() returns: nothing acknowledged
                 can be lost.
    """

    def __init__(
        self,
        dir_path: str,
        durability: str = "batch",
        batch_n: int = 1000,
        batch_ms: float = 50.0,
        hooks: Hooks | None = None,
        lock_timeout: float | None = None,
    ) -> None:
        if durability not in DURABILITY_MODES:
            raise ValueError(f"unknown durability: {durability!r}")
        self.dir = dir_path
        self.durability = durability
        self.batch_n = batch_n
        self.batch_ms = batch_ms
        self.hooks = hooks or Hooks()

        os.makedirs(dir_path, exist_ok=True)
        self._lock_fd = os.open(
            os.path.join(dir_path, "wal.lock"), os.O_CREAT | os.O_RDWR, 0o644
        )
        try:
            self._acquire_lock(lock_timeout)
        except BaseException:
            os.close(self._lock_fd)
            raise

        # Recover before appending: clear interrupted compactions, truncate
        # torn tails, and learn the last committed seq.
        stats = recover(dir_path)
        self._last_seq = stats.last_seq

        created = not os.path.exists(os.path.join(dir_path, MAIN_LOG))
        self._fd = os.open(
            os.path.join(dir_path, MAIN_LOG),
            os.O_WRONLY | os.O_CREAT | os.O_APPEND,
            0o644,
        )
        if created:
            _fsync_dir(dir_path)  # make the new file's directory entry durable
        self._size = os.path.getsize(os.path.join(dir_path, MAIN_LOG))
        self._durable_offset = self._size  # everything recovered is durable
        self._pending = 0
        self._last_flush = time.monotonic()
        self._closed = False

    # -- locking -----------------------------------------------------------

    def _acquire_lock(self, timeout: float | None) -> None:
        if timeout is None:
            fcntl.flock(self._lock_fd, fcntl.LOCK_EX)
            return
        deadline = time.monotonic() + timeout
        while True:
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        "another writer holds the WAL lock"
                    ) from None
                time.sleep(0.01)

    # -- properties used by tests / crash simulation -----------------------

    @property
    def last_seq(self) -> int:
        return self._last_seq

    @property
    def durable_offset(self) -> int:
        """Bytes known to be fsync'ed; a crash loses everything past this."""
        return self._durable_offset

    # -- writing -----------------------------------------------------------

    def append(self, op: str, key: str, value=None) -> int:
        """Append one record; returns its sequence number."""
        if self._closed:
            raise ValueError("WAL is closed")
        seq = self._last_seq + 1
        frame = encode_record(seq, op, key, value)
        self._write_all(frame)
        self.hooks.after_write(self._fd)
        self._size += len(frame)
        self._last_seq = seq
        self._pending += 1
        if self.durability == "every":
            self._fsync()
        elif self.durability == "batch":
            elapsed_ms = (time.monotonic() - self._last_flush) * 1000.0
            if self._pending >= self.batch_n or elapsed_ms >= self.batch_ms:
                self._fsync()
        return seq

    def flush(self) -> None:
        """fsync any pending appends (no-op for durability="none")."""
        if self.durability != "none" and self._pending > 0:
            self._fsync()

    def _fsync(self) -> None:
        self.hooks.before_fsync(self._fd)
        os.fsync(self._fd)
        self._durable_offset = self._size
        self._pending = 0
        self._last_flush = time.monotonic()

    def _write_all(self, data: bytes) -> None:
        view = memoryview(data)
        while view:
            written = os.write(self._fd, view)
            view = view[written:]

    def close(self) -> None:
        if self._closed:
            return
        self.flush()
        os.close(self._fd)
        fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
        os.close(self._lock_fd)
        self._closed = True

    def __enter__(self) -> "WAL":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -- reading -----------------------------------------------------------

    def iter_records(self, from_seq: int = 1) -> Iterator[Record]:
        """Yield records with seq >= from_seq, validating the seq chain.

        A torn tail (crash mid-append) ends iteration silently; a corrupt
        frame or a sequence hole raises CorruptionError/SeqGapError.
        """
        for record in iter_records(self.dir, from_seq):
            yield record

    def snapshot_at(self, seq: int) -> dict:
        """Key/value view exactly after record ``seq`` was applied."""
        return snapshot_at(self.dir, seq)


def _scan_valid_prefix(dir_path: str) -> list[Record]:
    finish_pending_compaction(dir_path)
    path = os.path.join(dir_path, MAIN_LOG)
    if not os.path.exists(path):
        return []
    return scan_strict(path).records


def iter_records(dir_path: str, from_seq: int = 1) -> Iterator[Record]:
    """Read-only iteration over the valid record prefix of the log."""
    for record in _scan_valid_prefix(dir_path):
        if record.seq >= from_seq:
            yield record


def snapshot_at(dir_path: str, seq: int) -> dict:
    """Key/value view exactly after record ``seq`` was applied.

    Raises SeqGapError if the log's sequence chain is broken at or before
    ``seq`` (holes are never silently skipped), and ValueError if ``seq`` is
    beyond the last record.
    """
    records = _scan_valid_prefix(dir_path)
    if seq < 0:
        raise ValueError("seq must be >= 0")
    if records and seq > records[-1].seq:
        raise ValueError(
            f"seq {seq} is beyond the last record (seq {records[-1].seq})"
        )
    view: dict = {}
    for record in records:
        if record.seq > seq:
            break
        if record.op == "put":
            view[record.key] = record.value
        else:
            view.pop(record.key, None)
    return view
