"""Recovery invariant: truncating at ANY byte offset must yield a prefix of
the complete record set, and appending afterwards must continue the seq
without gaps or reuse."""

import pytest

from wal import frame
from wal.log import WAL, iter_records, segment_name
from wal.recover import recover

N_RECORDS = 12
FRAMES = [frame.encode_frame(i + 1, frame.encode_payload("put", f"k{i}", i))
          for i in range(N_RECORDS)]
FULL = b"".join(FRAMES)
ENDS = []  # cumulative end offset of each frame
_pos = 0
for _f in FRAMES:
    _pos += len(_f)
    ENDS.append(_pos)


def expected_prefix(cut):
    count = 0
    for end in ENDS:
        if end <= cut:
            count += 1
        else:
            break
    return count, ENDS[count - 1] if count else 0


@pytest.mark.parametrize("cut", range(len(FULL) + 1))
def test_recover_invariant_any_offset(tmp_path, cut):
    seg = tmp_path / segment_name(1)
    seg.write_bytes(FULL[:cut])

    stats = recover(tmp_path)
    count, good_end = expected_prefix(cut)

    # recovery yields exactly a prefix of the complete record set
    assert stats.complete_records == count
    assert stats.last_seq == count
    assert stats.dropped_bytes == cut - good_end
    assert seg.stat().st_size == good_end

    # appending again continues the seq: contiguous, no reuse
    with WAL(tmp_path) as wal:
        seq = wal.put("after", "recovery")
    assert seq == count + 1
    records = list(iter_records(tmp_path))
    assert [r.seq for r in records] == list(range(1, count + 2))
    assert [r.key for r in records[:count]] == [f"k{i}" for i in range(count)]


def test_torn_tail_stats(tmp_path):
    seg = tmp_path / segment_name(1)
    seg.write_bytes(FULL + b"\x00\x01\x02")
    stats = recover(tmp_path)
    assert stats.complete_records == N_RECORDS
    assert stats.dropped_bytes == 3
    assert stats.last_seq == N_RECORDS
    # recover is idempotent
    assert recover(tmp_path).dropped_bytes == 0


def test_crc_corruption_truncates_at_first_bad_frame(tmp_path):
    blob = bytearray(FULL)
    blob[ENDS[2] + frame.HEADER_SIZE] ^= 0xFF  # flip a payload byte of record 4
    seg = tmp_path / segment_name(1)
    seg.write_bytes(bytes(blob))
    stats = recover(tmp_path)
    assert stats.complete_records == 3
    assert stats.last_seq == 3
    assert stats.dropped_bytes == len(FULL) - ENDS[2]


def test_recover_missing_dir():
    with pytest.raises(Exception):
        recover("/nonexistent-wal-dir-xyz")


def test_recover_empty_dir(tmp_path):
    stats = recover(tmp_path)
    assert stats.complete_records == 0
    assert stats.last_seq == 0
