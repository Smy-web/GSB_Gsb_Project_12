"""Single-writer mutual exclusion across processes."""

import multiprocessing as mp
import time

import pytest

from wal.log import WAL


def _hold_lock(dir_path, acquired, release):
    wal = WAL(dir_path, durability="every")
    wal.append("put", "holder", 1)
    acquired.set()
    release.wait(10)
    wal.close()


def _try_open(dir_path, result):
    try:
        WAL(dir_path, durability="every", lock_timeout=0.2)
    except TimeoutError:
        result.value = 3  # lock correctly denied
        return
    result.value = 0  # got the lock: mutual exclusion broken


def test_second_writer_blocked_while_first_holds_lock(tmp_path):
    d = str(tmp_path / "w")
    ctx = mp.get_context("fork")
    acquired = ctx.Event()
    release = ctx.Event()
    holder = ctx.Process(target=_hold_lock, args=(d, acquired, release))
    holder.start()
    try:
        assert acquired.wait(10)
        t0 = time.monotonic()
        with pytest.raises(TimeoutError):
            WAL(d, durability="every", lock_timeout=0.3)
        assert time.monotonic() - t0 >= 0.3
    finally:
        release.set()
        holder.join(10)
    assert holder.exitcode == 0
    # once the holder released, a new writer can open and continue the chain
    with WAL(d, durability="every") as wal:
        assert wal.append("put", "second", 2) == 2


def test_child_process_denied_while_parent_holds_lock(tmp_path):
    d = str(tmp_path / "w")
    wal = WAL(d, durability="every")
    try:
        ctx = mp.get_context("fork")
        result = ctx.Value("i", 0)
        child = ctx.Process(target=_try_open, args=(d, result))
        child.start()
        child.join(10)
        assert child.exitcode == 0
        assert result.value == 3  # child could not take the write lock
    finally:
        wal.close()
