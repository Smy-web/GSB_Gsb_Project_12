"""Compaction: latest-op-per-key rewrite + crash injection at 4 points."""

import pytest

from wal import frame
from wal.compact import compact
from wal.log import WAL, iter_records, list_segments, snapshot_at

EXPECTED_VIEW = {"a": 3, "c": 4}


def populate(dir):
    with WAL(dir) as wal:
        wal.put("a", 1)
        wal.put("b", 2)
        wal.put("a", 3)
        wal.delete("b")
        wal.put("c", 4)


def test_compact_keeps_latest_op_per_key(tmp_path):
    populate(tmp_path)
    stats = compact(tmp_path)
    assert stats.records_in == 5
    assert stats.records_out == 3
    assert stats.bytes_after < stats.bytes_before

    records = list(iter_records(tmp_path))
    assert [r.seq for r in records] == [1, 2, 3]  # renumbered, no holes
    assert {r.key: r.op for r in records} == {"a": "put", "b": "del", "c": "put"}
    assert snapshot_at(tmp_path) == EXPECTED_VIEW

    # appends continue after the renumbered seqs
    with WAL(tmp_path) as wal:
        assert wal.put("d", 5) == 4
    assert snapshot_at(tmp_path) == {**EXPECTED_VIEW, "d": 5}


def test_compact_empty_dir(tmp_path):
    stats = compact(tmp_path)
    assert stats.records_in == 0 and stats.records_out == 0


@pytest.mark.parametrize(
    "point", ["before_rename", "after_rename", "before_delete_old", "after_delete_old"]
)
def test_compact_crash_injection(tmp_path, point):
    populate(tmp_path)

    def hook():
        raise frame.InjectedCrash(f"crash at {point}")

    with pytest.raises(frame.InjectedCrash):
        compact(tmp_path, hooks={point: hook})

    # No half-new/half-old state: readers see either the old generation
    # (5 records) or the new one (3 records), both fully consistent.
    records = list(iter_records(tmp_path))
    if point == "before_rename":
        assert [r.seq for r in records] == [1, 2, 3, 4, 5]
    else:
        assert [r.seq for r in records] == [1, 2, 3]
    assert snapshot_at(tmp_path) == EXPECTED_VIEW

    # The log keeps accepting appends with contiguous seqs, and the next
    # open cleans up any leftover generations/tmp files.
    with WAL(tmp_path) as wal:
        wal.put("z", 26)
    assert len(list_segments(tmp_path)) == 1
    assert snapshot_at(tmp_path) == {**EXPECTED_VIEW, "z": 26}


def test_compact_after_crash_recovers_consistently(tmp_path):
    populate(tmp_path)
    with pytest.raises(frame.InjectedCrash):
        compact(tmp_path, hooks={"before_delete_old": lambda: (_ for _ in ()).throw(
            frame.InjectedCrash("boom"))})
    # a second compact finishes the job
    stats = compact(tmp_path)
    assert stats.records_out == 3
    assert snapshot_at(tmp_path) == EXPECTED_VIEW
