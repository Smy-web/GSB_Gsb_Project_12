"""Salvage mode: skip bad records, keep later readable ones (manual rescue)."""

import pytest

from wal import frame
from wal.frame import SeqGapError
from wal.log import iter_records, segment_name
from wal.recover import recover

FRAMES = [frame.encode_frame(i + 1, frame.encode_payload("put", f"k{i}", i))
          for i in range(5)]
ENDS = []
_pos = 0
for _f in FRAMES:
    _pos += len(_f)
    ENDS.append(_pos)


def corrupt_record(tmp_path, index):
    """Flip one payload byte of record `index` (0-based) -> crc mismatch."""
    blob = bytearray(b"".join(FRAMES))
    start = ENDS[index - 1] if index else 0
    blob[start + frame.HEADER_SIZE] ^= 0xFF
    seg = tmp_path / segment_name(1)
    seg.write_bytes(bytes(blob))
    return seg


def test_strict_recovery_drops_everything_after_bad_record(tmp_path):
    corrupt_record(tmp_path, 2)
    stats = recover(tmp_path)
    assert stats.complete_records == 2
    assert stats.last_seq == 2
    assert stats.dropped_bytes == ENDS[4] - ENDS[1]
    assert [r.key for r in iter_records(tmp_path)] == ["k0", "k1"]


def test_salvage_keeps_later_valid_records(tmp_path):
    corrupt_record(tmp_path, 2)
    stats = recover(tmp_path, salvage=True)
    assert stats.salvaged
    assert stats.complete_records == 4
    assert stats.last_seq == 5
    assert stats.dropped_bytes == len(FRAMES[2])
    # the hole is explicit, not silently skipped
    with pytest.raises(SeqGapError):
        list(iter_records(tmp_path))


def test_salvage_with_destroyed_header_resyncs_on_magic(tmp_path):
    # wipe record 3's header entirely (magic included)
    blob = bytearray(b"".join(FRAMES))
    start = ENDS[1]
    for i in range(start, start + frame.HEADER_SIZE):
        blob[i] = 0x00
    seg = tmp_path / segment_name(1)
    seg.write_bytes(bytes(blob))
    stats = recover(tmp_path, salvage=True)
    assert stats.complete_records == 4
    assert stats.last_seq == 5
    assert stats.dropped_bytes == len(FRAMES[2])


def test_salvage_idempotent_when_clean(tmp_path):
    seg = tmp_path / segment_name(1)
    seg.write_bytes(b"".join(FRAMES))
    stats = recover(tmp_path, salvage=True)
    assert stats.complete_records == 5
    assert stats.dropped_bytes == 0
    assert seg.stat().st_size == ENDS[4]
