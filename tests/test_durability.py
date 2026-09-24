"""Durability-level crash semantics via fault-injection hooks."""

import os

import pytest

from wal.frame import MAIN_LOG
from wal.log import WAL, Hooks
from wal.recover import recover


class CrashSimulated(Exception):
    pass


class CrashOnFsync(Hooks):
    """Raises CrashSimulated instead of the ``count``-th fsync."""

    def __init__(self, count):
        self.count = count
        self.seen = 0

    def before_fsync(self, fd):
        self.seen += 1
        if self.seen == self.count:
            raise CrashSimulated


def simulate_crash(wal: WAL) -> None:
    """Kill the 'process': drop fds without flushing, then discard everything
    past the last fsync'ed offset (what a real crash would lose)."""
    os.close(wal._fd)
    os.close(wal._lock_fd)
    wal._closed = True
    path = os.path.join(wal.dir, MAIN_LOG)
    with open(path, "r+b") as fh:
        fh.truncate(wal.durable_offset)


def committed_seqs(dir_path):
    stats = recover(dir_path)
    assert stats.complete_records == stats.last_seq  # contiguous from 1
    return stats.last_seq


def test_durability_none_may_lose_everything(tmp_path):
    d = str(tmp_path / "w")
    wal = WAL(d, durability="none")
    for i in range(5):
        assert wal.append("put", f"k{i}", i) == i + 1
    assert wal.durable_offset == 0  # nothing ever fsync'ed
    simulate_crash(wal)

    assert committed_seqs(d) == 0  # the whole batch is lost: allowed
    with WAL(d, durability="every") as wal2:
        assert wal2.append("put", "after", 1) == 1  # seq restarts cleanly


def test_durability_every_loses_nothing_acknowledged(tmp_path):
    d = str(tmp_path / "w")
    hooks = CrashOnFsync(count=6)
    wal = WAL(d, durability="every", hooks=hooks)
    for i in range(5):
        assert wal.append("put", f"k{i}", i) == i + 1
    with pytest.raises(CrashSimulated):
        wal.append("put", "k5", 5)  # 6th append: process dies inside fsync
    simulate_crash(wal)

    # every record whose append() returned successfully survives
    assert committed_seqs(d) == 5
    with WAL(d, durability="every") as wal2:
        assert wal2.append("put", "after", 1) == 6
        assert [r.seq for r in wal2.iter_records()] == [1, 2, 3, 4, 5, 6]


def test_durability_batch_loses_only_the_last_batch(tmp_path):
    d = str(tmp_path / "w")
    wal = WAL(d, durability="batch", batch_n=10, batch_ms=60_000)
    for i in range(25):
        wal.append("put", f"k{i}", i)
    # fsyncs happened after records 10 and 20; records 21..25 are in limbo
    assert wal.durable_offset > 0
    simulate_crash(wal)

    assert committed_seqs(d) == 20  # exactly the last fsync'ed prefix
    with WAL(d, durability="every") as wal2:
        assert wal2.append("put", "after", 1) == 21  # no hole, no dupe
        assert [r.seq for r in wal2.iter_records()] == list(range(1, 22))


def test_durability_batch_time_trigger(tmp_path):
    d = str(tmp_path / "w")
    wal = WAL(d, durability="batch", batch_n=10**9, batch_ms=0)
    wal.append("put", "a", 1)  # elapsed >= 0ms triggers the time-based fsync
    assert wal.durable_offset > 0
    wal.close()


def test_flush_makes_pending_durable(tmp_path):
    d = str(tmp_path / "w")
    wal = WAL(d, durability="batch", batch_n=10**9, batch_ms=60_000)
    wal.append("put", "a", 1)
    pending_offset = wal.durable_offset
    wal.flush()
    assert wal.durable_offset > pending_offset
    wal.close()
