"""Compaction correctness and crash-injection at all four hook points."""

import os

import pytest

from wal.compact import CompactionHooks, compact
from wal.frame import COMPACT_OLD, COMPACT_TMP, MAIN_LOG
from wal.log import WAL, snapshot_at
from wal.recover import finish_pending_compaction, recover


class CrashSimulated(Exception):
    pass


def build_log(dir_path) -> dict:
    with WAL(dir_path, durability="every") as wal:
        wal.append("put", "a", 1)
        wal.append("put", "b", 2)
        wal.append("put", "a", 3)  # supersedes a=1
        wal.append("del", "b")
        wal.append("put", "c", 4)
    return {"a": 3, "c": 4}  # expected final view


def last_seq(dir_path) -> int:
    return recover(dir_path).last_seq


def view(dir_path) -> dict:
    return snapshot_at(dir_path, last_seq(dir_path))


def test_compact_keeps_latest_op_per_key(tmp_path):
    d = str(tmp_path / "w")
    expected = build_log(d)
    stats = compact(d)
    assert stats.records_before == 5
    assert stats.records_after == 3  # a(put 3), b(del), c(put 4)
    assert stats.bytes_after < stats.bytes_before
    assert view(d) == expected
    # tombstone for b is retained so replay still deletes it
    with WAL(d, durability="every") as wal:
        ops = {r.key: r.op for r in wal.iter_records()}
    assert ops == {"a": "put", "b": "del", "c": "put"}
    # seq renumbered contiguously, appends continue from there
    with WAL(d, durability="every") as wal:
        assert wal.append("put", "d", 5) == 4
        assert [r.seq for r in wal.iter_records()] == [1, 2, 3, 4]


def test_compact_empty_dir(tmp_path):
    d = str(tmp_path / "w")
    stats = compact(d)
    assert stats.records_before == 0 and stats.records_after == 0
    assert view(d) == {}


class CrashAt(CompactionHooks):
    def __init__(self, point):
        self.point = point

    def _maybe(self, point):
        if point == self.point:
            raise CrashSimulated(point)

    def before_rename(self):
        self._maybe("before_rename")

    def after_rename(self):
        self._maybe("after_rename")

    def before_delete_old(self):
        self._maybe("before_delete_old")

    def after_delete_old(self):
        self._maybe("after_delete_old")


@pytest.mark.parametrize(
    "point",
    ["before_rename", "after_rename", "before_delete_old", "after_delete_old"],
)
def test_compact_crash_at_injection_points(tmp_path, point):
    d = str(tmp_path / "w")
    expected = build_log(d)

    with pytest.raises(CrashSimulated):
        compact(d, hooks=CrashAt(point))

    # Whatever the crash point, recovery restores a single consistent log
    # whose replayed state equals the pre-compact state (never half/half).
    finish_pending_compaction(d)
    assert view(d) == expected
    names = set(os.listdir(d)) - {"wal.lock"}
    assert MAIN_LOG in names
    assert not ({COMPACT_TMP, COMPACT_OLD} & names), names

    # And a subsequent compact runs cleanly to completion.
    compact(d)
    assert view(d) == expected
    assert set(os.listdir(d)) == {"wal.lock", MAIN_LOG}


def test_compact_intermediate_states_are_never_mixed(tmp_path):
    """After a crash, the visible log is either fully old or fully new."""
    d = str(tmp_path / "w")
    expected = build_log(d)
    pre_compact_dump = None
    with WAL(d, durability="every") as wal:
        pre_compact_dump = [(r.seq, r.op, r.key, r.value) for r in wal.iter_records()]

    with pytest.raises(CrashSimulated):
        compact(d, hooks=CrashAt("after_rename"))

    # before recovery: main.log is either the old log or the new log
    stats = recover(d)
    with WAL(d, durability="every") as wal:
        dump = [(r.seq, r.op, r.key, r.value) for r in wal.iter_records()]
    is_old = dump == pre_compact_dump
    is_new = [t[1:] for t in dump] == [
        ("put", "a", 3),
        ("del", "b", None),
        ("put", "c", 4),
    ] and [t[0] for t in dump] == [1, 2, 3]
    assert is_old or is_new
    assert view(d) == expected
