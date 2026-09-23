"""seq holes must raise SeqGapError, never be silently skipped."""

import pytest

from wal import frame
from wal.frame import SeqGapError
from wal.log import WAL, iter_records, segment_name, snapshot_at
from wal.recover import recover


def make_log_with_hole(dir):
    """5 valid records, record 3 crc-corrupted, then salvage -> hole at seq 3."""
    frames = [frame.encode_frame(i + 1, frame.encode_payload("put", f"k{i}", i))
              for i in range(5)]
    blob = bytearray(b"".join(frames))
    offset = sum(len(f) for f in frames[:2])
    blob[offset + frame.HEADER_SIZE] ^= 0xFF
    (dir / segment_name(1)).write_bytes(bytes(blob))
    recover(dir, salvage=True)


def test_iter_records_raises_on_gap(tmp_path):
    make_log_with_hole(tmp_path)
    with pytest.raises(SeqGapError, match="expected seq 3"):
        list(iter_records(tmp_path))


def test_snapshot_at_raises_on_gap(tmp_path):
    make_log_with_hole(tmp_path)
    with pytest.raises(SeqGapError):
        snapshot_at(tmp_path)


def test_snapshot_at_seq_views(tmp_path):
    with WAL(tmp_path) as wal:
        wal.put("a", 1)
        wal.put("b", 2)
        wal.delete("a")
        wal.put("a", 3)
    assert snapshot_at(tmp_path, 1) == {"a": 1}
    assert snapshot_at(tmp_path, 2) == {"a": 1, "b": 2}
    assert snapshot_at(tmp_path, 3) == {"b": 2}
    assert snapshot_at(tmp_path, 4) == {"a": 3, "b": 2}
    assert snapshot_at(tmp_path) == {"a": 3, "b": 2}


def test_iter_records_from_seq(tmp_path):
    with WAL(tmp_path) as wal:
        for i in range(5):
            wal.put(f"k{i}", i)
    records = list(iter_records(tmp_path, from_seq=3))
    assert [r.seq for r in records] == [3, 4, 5]
    assert list(iter_records(tmp_path, from_seq=99)) == []


def test_compact_heals_salvage_holes(tmp_path):
    make_log_with_hole(tmp_path)
    from wal.compact import compact
    compact(tmp_path)
    records = list(iter_records(tmp_path))
    assert [r.seq for r in records] == [1, 2, 3, 4]
