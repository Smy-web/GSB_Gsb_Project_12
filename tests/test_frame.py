import pytest

from wal import frame


def test_header_layout_size():
    assert frame.HEADER_SIZE == 19
    assert frame.MAGIC == b"WL"
    assert frame.VERSION == 1


def test_payload_roundtrip_put():
    data = frame.encode_payload("put", "k", {"a": [1, 2]})
    assert frame.decode_payload(data) == {"op": "put", "key": "k", "value": {"a": [1, 2]}}


def test_payload_roundtrip_del_has_no_value():
    obj = frame.decode_payload(frame.encode_payload("del", "k"))
    assert obj == {"op": "del", "key": "k"}
    assert "value" not in obj


def test_frame_roundtrip():
    payload = frame.encode_payload("put", "key", [1, 2, 3])
    blob = frame.encode_frame(42, payload)
    assert len(blob) == frame.HEADER_SIZE + len(payload)
    seq, payload_len, crc = frame.decode_header(blob[:frame.HEADER_SIZE])
    assert (seq, payload_len) == (42, len(payload))
    assert frame.check_crc(blob[frame.HEADER_SIZE:], crc)


def test_non_ascii_roundtrip():
    payload = frame.encode_payload("put", "键", "值")
    blob = frame.encode_frame(1, payload)
    obj = frame.decode_payload(blob[frame.HEADER_SIZE:])
    assert obj == {"op": "put", "key": "键", "value": "值"}


def test_bad_magic_rejected():
    blob = bytearray(frame.encode_frame(1, frame.encode_payload("put", "k", 1)))
    blob[0] ^= 0xFF
    with pytest.raises(frame.CorruptionError):
        frame.decode_header(bytes(blob[:frame.HEADER_SIZE]))


def test_crc_detects_payload_corruption():
    payload = frame.encode_payload("put", "k", "v")
    blob = bytearray(frame.encode_frame(1, payload))
    blob[-1] ^= 0x01
    _, _, crc = frame.decode_header(bytes(blob[:frame.HEADER_SIZE]))
    assert not frame.check_crc(bytes(blob[frame.HEADER_SIZE:]), crc)


def test_short_header_rejected():
    with pytest.raises(frame.CorruptionError):
        frame.decode_header(b"WL\x01")


def test_bad_op_rejected():
    with pytest.raises(ValueError):
        frame.encode_payload("set", "k", 1)
