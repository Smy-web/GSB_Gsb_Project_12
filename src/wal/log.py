"""Append-only write-ahead log: single-writer locking, durability levels, reads."""

from __future__ import annotations

import fcntl
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from . import frame
from .frame import CorruptionError, InjectedCrash, LockedError, SeqGapError, WALError
from .recover import recover_segment, scan_segment

LOCK_FILE_NAME = "LOCK"
_SEGMENT_PREFIX = "wal-"
_SEGMENT_SUFFIX = ".log"

DURABILITY_NONE = "none"
DURABILITY_BATCH = "batch"
DURABILITY_EVERY = "every"

_MISSING = object()


@dataclass(frozen=True)
class Record:
    seq: int
    op: str
    key: object
    value: object = None


def segment_name(generation):
    return f"{_SEGMENT_PREFIX}{generation:06d}{_SEGMENT_SUFFIX}"


def list_segments(dir):
    """Return [(generation, path), ...] sorted by generation."""
    found = []
    for path in Path(dir).glob(f"{_SEGMENT_PREFIX}*{_SEGMENT_SUFFIX}"):
        try:
            generation = int(path.name[len(_SEGMENT_PREFIX):-len(_SEGMENT_SUFFIX)])
        except ValueError:
            continue
        found.append((generation, path))
    return sorted(found)


def current_segment_path(dir):
    segments = list_segments(dir)
    return segments[-1][1] if segments else None


def fsync_dir(dir):
    fd = os.open(dir, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _acquire_lock(dir, blocking):
    fd = os.open(Path(dir) / LOCK_FILE_NAME, os.O_RDWR | os.O_CREAT, 0o644)
    flags = fcntl.LOCK_EX if blocking else fcntl.LOCK_EX | fcntl.LOCK_NB
    try:
        fcntl.flock(fd, flags)
    except BlockingIOError:
        os.close(fd)
        raise LockedError(f"another writer holds the lock in {dir}") from None
    return fd


@contextmanager
def lock_directory(dir, blocking=True):
    fd = _acquire_lock(dir, blocking)
    try:
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


class WAL:
    """Single-writer append-only log. One instance per directory per process."""

    def __init__(self, dir, durability=DURABILITY_EVERY, batch_n=1000,
                 batch_ms=100, hooks=None):
        if durability not in (DURABILITY_NONE, DURABILITY_BATCH, DURABILITY_EVERY):
            raise ValueError(f"unknown durability: {durability!r}")
        self.dir = Path(dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.durability = durability
        self.batch_n = int(batch_n)
        self.batch_ms = int(batch_ms)
        self.hooks = dict(hooks or {})
        self._lock_fd = _acquire_lock(self.dir, blocking=False)
        self._fd = None
        try:
            self._open_segment()
        except Exception:
            os.close(self._lock_fd)
            raise

    def _open_segment(self):
        cleaned = False
        for stale in self.dir.glob(f"{_SEGMENT_PREFIX}*{_SEGMENT_SUFFIX}.tmp"):
            stale.unlink()
            cleaned = True
        segments = list_segments(self.dir)
        for _, old in segments[:-1]:
            # Superseded by a completed compaction; safe to drop (see compact.py).
            old.unlink()
            cleaned = True
        if segments:
            seg_path = segments[-1][1]
        else:
            seg_path = self.dir / segment_name(1)
            seg_path.touch()
            cleaned = True
        if cleaned:
            fsync_dir(self.dir)
        stats = recover_segment(seg_path, salvage=False)
        self._segment_path = seg_path
        self._next_seq = stats.last_seq + 1
        self._fd = os.open(seg_path, os.O_WRONLY | os.O_APPEND | os.O_CREAT)
        self._durable_end = os.lseek(self._fd, 0, os.SEEK_END)
        self._since_sync = 0
        self._last_sync = time.monotonic()

    def _check_open(self):
        if self._fd is None:
            raise WALError("WAL is closed")

    def append(self, op, key, value=_MISSING):
        """Append one record and return its seq. See sync() for durability."""
        self._check_open()
        if op == "put":
            payload = frame.encode_payload(op, key, value)
        else:
            payload = frame.encode_payload(op, key)
        data = frame.encode_frame(self._next_seq, payload)
        view = memoryview(data)
        while view:
            written = os.write(self._fd, view)
            view = view[written:]
        seq = self._next_seq
        self._next_seq += 1
        self._since_sync += 1
        self._maybe_sync()
        return seq

    def put(self, key, value):
        return self.append("put", key, value)

    def delete(self, key):
        return self.append("del", key)

    def _maybe_sync(self):
        if self.durability == DURABILITY_EVERY:
            self.sync()
        elif self.durability == DURABILITY_BATCH:
            elapsed_ms = (time.monotonic() - self._last_sync) * 1000
            if self._since_sync >= self.batch_n or elapsed_ms >= self.batch_ms:
                self.sync()

    def sync(self):
        """fsync the segment. The before_fsync hook may raise InjectedCrash."""
        self._check_open()
        hook = self.hooks.get("before_fsync")
        if hook is not None:
            try:
                hook(self)
            except InjectedCrash:
                self._crash_now()
                raise
        os.fsync(self._fd)
        self._durable_end = os.lseek(self._fd, 0, os.SEEK_END)
        self._since_sync = 0
        self._last_sync = time.monotonic()

    def simulate_crash(self):
        """Simulate sudden process death: everything not yet fsynced is lost."""
        self._crash_now()

    def _crash_now(self):
        if self._fd is None:
            return
        os.ftruncate(self._fd, self._durable_end)
        os.close(self._fd)
        self._fd = None
        fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
        os.close(self._lock_fd)
        self._lock_fd = None

    def close(self):
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        if self._lock_fd is not None:
            fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            os.close(self._lock_fd)
            self._lock_fd = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def iter_records(dir, from_seq=1):
    """Yield Records in seq order, tolerating a torn tail.

    Raises CorruptionError if nothing valid can be read from a non-empty
    segment, and SeqGapError if valid frames have non-contiguous seqs.
    """
    dir = Path(dir)
    if not dir.is_dir():
        raise WALError(f"no such directory: {dir}")
    seg = current_segment_path(dir)
    if seg is None:
        return
    res = scan_segment(seg)
    if res.error is not None and not res.frames:
        raise CorruptionError(f"{seg}: {res.error}")
    expected = 1
    for fi in res.frames:
        if fi.seq != expected:
            raise SeqGapError(f"{seg}: expected seq {expected}, found {fi.seq}")
        expected += 1
        if fi.seq < from_seq:
            continue
        obj = frame.decode_payload(fi.payload)
        yield Record(seq=fi.seq, op=obj["op"], key=obj["key"], value=obj.get("value"))


def snapshot_at(dir, seq=None):
    """Return the key->value view as of the given seq (None = latest)."""
    view = {}
    for rec in iter_records(dir):
        if seq is not None and rec.seq > seq:
            break
        if rec.op == "put":
            view[rec.key] = rec.value
        else:
            view.pop(rec.key, None)
    return view


def get(dir, key, default=None):
    return snapshot_at(dir).get(key, default)
