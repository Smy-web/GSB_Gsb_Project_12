import struct

import pytest

from wal.frame import (
    HEADER_SIZE,
    MAGIC,
    VERSION,
    CorruptionError,
    SeqGapError,
    decode_frame,
    encode_record,
)


@pytest.mark.parametrize(
    "op,key,value",
    [
        ("put", "a", 1),
        ("put", "k", "hello"),
        ("put", "unicode-key-键", {"nested": [1, 2, {"x": None}], "s": "值"}),
        ("put", "n", None),
        ("put", "f", 3.25),
        ("put", "empty", ""),
        ("del", "a", None),
        ("del", "键", None),
    ],
)
def test_frame_roundtrip(op, key, value):
    frame = encode_record(7, op, key, value)
    record = decode_frame(frame)
    assert record.seq == 7
    assert record.op == op
    assert record.key == key
    if op == "put":
        assert record.value == value


def test_frame_layout():
    frame = encode_record(0x0102030405060708, "put", "k", "v")
    assert frame[:2] == MAGIC
    assert frame[2] == VERSION
    seq = struct.unpack(">Q", frame[3:11])[0]
    assert seq == 0x0102030405060708  # big-endian
    payload_len = struct.unpack(">I", frame[11:15])[0]
    assert len(frame) == HEADER_SIZE + payload_len


def test_crc_mismatch_detected():
    frame = bytearray(encode_record(1, "put", "k", "v"))
    frame[-1] ^= 0xFF
    with pytest.raises(CorruptionError):
        decode_frame(bytes(frame))


def test_bad_magic_detected():
    frame = bytearray(encode_record(1, "put", "k", "v"))
    frame[0] ^= 0xFF
    with pytest.raises(CorruptionError):
        decode_frame(bytes(frame))


def test_expected_seq_gap():
    frame = encode_record(5, "put", "k", "v")
    with pytest.raises(SeqGapError):
        decode_frame(frame, expected_seq=4)
