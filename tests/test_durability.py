"""Durability levels under simulated crashes (process death before fsync)."""

import time

import pytest

from wal import frame
from wal.log import WAL, iter_records


def keys(dir):
    return [r.key for r in iter_records(dir)]


def test_none_loses_unsynced_records(tmp_path):
    wal = WAL(tmp_path, durability="none")
    for i in range(10):
        wal.put(f"k{i}", i)
    wal.simulate_crash()
    assert keys(tmp_path) == []


def test_none_keeps_explicitly_synced_prefix(tmp_path):
    wal = WAL(tmp_path, durability="none")
    wal.put("a", 1)
    wal.sync()
    wal.put("b", 2)
    wal.simulate_crash()
    assert keys(tmp_path) == ["a"]


def test_batch_may_lose_the_latest_batch(tmp_path):
    wal = WAL(tmp_path, durability="batch", batch_n=5, batch_ms=60_000)
    for i in range(7):
        wal.put(f"k{i}", i)
    wal.simulate_crash()
    assert keys(tmp_path) == [f"k{i}" for i in range(5)]


def test_batch_time_trigger(tmp_path):
    wal = WAL(tmp_path, durability="batch", batch_n=10**9, batch_ms=50)
    wal.put("a", 1)
    time.sleep(0.2)
    wal.put("b", 2)  # T elapsed -> fsyncs both
    wal.simulate_crash()
    assert keys(tmp_path) == ["a", "b"]


def test_every_loses_no_successful_append(tmp_path):
    wal = WAL(tmp_path, durability="every")
    for i in range(7):
        wal.put(f"k{i}", i)
    wal.simulate_crash()
    assert keys(tmp_path) == [f"k{i}" for i in range(7)]


def test_hook_crash_before_fsync_every_keeps_returned_appends(tmp_path):
    calls = []

    def hook(wal):
        calls.append(1)
        if len(calls) == 3:
            raise frame.InjectedCrash("process died before fsync")

    wal = WAL(tmp_path, durability="every", hooks={"before_fsync": hook})
    wal.put("a", 1)
    wal.put("b", 2)
    with pytest.raises(frame.InjectedCrash):
        wal.put("c", 3)  # never returned success -> may be lost
    assert keys(tmp_path) == ["a", "b"]


def test_hook_crash_before_fsync_batch_keeps_previous_batches(tmp_path):
    state = {"n": 0}

    def hook(wal):
        state["n"] += 1
        if state["n"] == 2:
            raise frame.InjectedCrash("process died before fsync")

    wal = WAL(tmp_path, durability="batch", batch_n=2, batch_ms=60_000,
              hooks={"before_fsync": hook})
    wal.put("a", 1)
    wal.put("b", 2)  # sync #1 succeeds
    wal.put("c", 3)
    with pytest.raises(frame.InjectedCrash):
        wal.put("d", 4)  # sync #2 dies before fsync
    assert keys(tmp_path) == ["a", "b"]


def test_wal_is_unusable_after_simulated_crash(tmp_path):
    wal = WAL(tmp_path, durability="none")
    wal.put("a", 1)
    wal.simulate_crash()
    with pytest.raises(frame.WALError):
        wal.put("b", 2)


def test_unknown_durability_rejected(tmp_path):
    with pytest.raises(ValueError):
        WAL(tmp_path, durability="sometimes")
