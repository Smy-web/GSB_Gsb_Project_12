"""Single-writer mutual exclusion via flock, verified across processes."""

import multiprocessing

import pytest

from wal import frame
from wal.log import WAL


def _child_try_open(dir, conn):
    try:
        wal = WAL(dir)
    except frame.LockedError:
        conn.send("locked")
    except Exception as exc:  # pragma: no cover - diagnostic path
        conn.send(f"error: {exc!r}")
    else:
        wal.close()
        conn.send("opened")
    finally:
        conn.close()


def test_lock_same_process(tmp_path):
    wal = WAL(tmp_path)
    with pytest.raises(frame.LockedError):
        WAL(tmp_path)
    wal.close()
    # lock released on close
    with WAL(tmp_path) as wal2:
        wal2.put("k", 1)


def test_lock_across_processes(tmp_path):
    wal = WAL(tmp_path)
    ctx = multiprocessing.get_context("fork")
    parent, child = ctx.Pipe()
    proc = ctx.Process(target=_child_try_open, args=(str(tmp_path), child))
    proc.start()
    assert parent.recv() == "locked"
    proc.join()
    assert proc.exitcode == 0
    wal.close()

    # after the writer exits, another process may take the lock
    parent, child = ctx.Pipe()
    proc = ctx.Process(target=_child_try_open, args=(str(tmp_path), child))
    proc.start()
    assert parent.recv() == "opened"
    proc.join()
