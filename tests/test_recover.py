"""Crash-recovery invariants, including the brute-force truncation sweep."""

import os

import pytest

from wal.frame import MAIN_LOG, CorruptionError, encode_record
from wal.log import WAL
from wal.recover import recover

N_RECORDS = 12
FRAMES = [encode_record(i, "put", f"k{i}", i) for i in range(1, N_RECORDS + 1)]
BLOB = b"".join(FRAMES)
# cumulative end offset of each frame within the blob
ENDS = []
_off = 0
for _f in FRAMES:
    _off += len(_f)
    ENDS.append(_off)


def complete_frames(cut: int) -> int:
    return sum(1 for end in ENDS if end <= cut)


def valid_bytes(cut: int) -> int:
    n = complete_frames(cut)
    return ENDS[n - 1] if n else 0


def write_blob(dir_path: str, data: bytes) -> None:
    os.makedirs(dir_path, exist_ok=True)
    with open(os.path.join(dir_path, MAIN_LOG), "wb") as fh:
        fh.write(data)


@pytest.mark.parametrize("cut", range(len(BLOB) + 1))
def test_truncation_at_every_offset(tmp_path, cut):
    """Invariant: truncating at ANY byte offset, recover() yields exactly the
    complete-record prefix, and the next append continues the seq chain."""
    d = str(tmp_path / "w")
    write_blob(d, BLOB[:cut])

    stats = recover(d)
    expected = complete_frames(cut)
    assert stats.complete_records == expected
    assert stats.discarded_bytes == cut - valid_bytes(cut)
    assert stats.last_seq == expected

    # recover is idempotent
    again = recover(d)
    assert again.discarded_bytes == 0
    assert again.complete_records == expected

    # appending after recovery continues the sequence with no holes/dupes
    with WAL(d, durability="every") as wal:
        seq = wal.append("put", "post-crash", True)
        assert seq == expected + 1
        seqs = [r.seq for r in wal.iter_records()]
    assert seqs == list(range(1, expected + 2))


def test_recover_missing_dir(tmp_path):
    stats = recover(str(tmp_path / "nope"))
    assert stats.complete_records == 0
    assert stats.last_seq == 0


def _build_log_with_mid_corruption(dir_path: str) -> bytes:
    good_head = b"".join(encode_record(i, "put", f"k{i}", i) for i in (1, 2, 3))
    bad = bytearray(encode_record(4, "put", "k4", 4))
    bad[-1] ^= 0xFF  # corrupt crc of record 4
    good_tail = b"".join(encode_record(i, "put", f"k{i}", i) for i in (5, 6))
    blob = good_head + bytes(bad) + good_tail
    write_blob(dir_path, blob)
    return blob


def test_strict_mode_rejects_mid_log_corruption(tmp_path):
    d = str(tmp_path / "w")
    _build_log_with_mid_corruption(d)
    with pytest.raises(CorruptionError):
        recover(d, mode="strict")


def test_salvage_skips_bad_records_and_reports_gaps(tmp_path):
    d = str(tmp_path / "w")
    blob = _build_log_with_mid_corruption(d)
    before = open(os.path.join(d, MAIN_LOG), "rb").read()

    stats = recover(d, mode="salvage")
    assert stats.complete_records == 5  # 1,2,3,5,6 -- record 4 unreadable
    assert stats.last_seq == 6
    assert stats.seq_gaps == [(4, 5)]  # hole reported, not silently skipped
    assert stats.truncated is False
    # salvage is read-only: file untouched
    assert open(os.path.join(d, MAIN_LOG), "rb").read() == before == blob


def test_salvage_resynchronizes_on_garbage_bytes(tmp_path):
    d = str(tmp_path / "w")
    blob = (
        encode_record(1, "put", "a", 1)
        + b"\x00\xffgarbage-not-a-frame\x13\x37"
        + encode_record(2, "put", "b", 2)
    )
    write_blob(d, blob)
    stats = recover(d, mode="salvage")
    assert stats.complete_records == 2
    # seq 1 then seq 2: contiguous, no gap reported
    assert stats.seq_gaps == []
