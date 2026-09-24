"""Binary frame encoding/decoding for the WAL.

Frame layout (all integers big-endian):

    +--------+---------+-----------+-------------+----------------+----------+
    | magic  | version |   seq     | payload_len | crc32(payload) | payload  |
    | 2 B    | 1 B     | 8 B       | 4 B         | 4 B            | N B      |
    +--------+---------+-----------+-------------+----------------+----------+

magic = b"WL", version = 1.  Payload is UTF-8 JSON:
{"op": "put"|"del", "key": <str>, "value": <any JSON> (put only)}.
"""

from __future__ import annotations

import json
import struct
import zlib
from dataclasses import dataclass
from typing import Any

MAGIC = b"WL"
VERSION = 1
# magic(2) + version(1) + seq(8) + payload_len(4) + crc32(4)
HEADER_SIZE = 19
HEADER_STRUCT = struct.Struct(">2sBQI I".replace(" ", ""))
MAX_PAYLOAD = 64 * 1024 * 1024

MAIN_LOG = "wal.log"
COMPACT_TMP = "wal.log.compacting"
COMPACT_OLD = "wal.log.old"


class CorruptionError(Exception):
    """A record on disk is corrupt in a way strict mode cannot skip."""


class SeqGapError(CorruptionError):
    """Sequence numbers are not contiguous (a hole was detected)."""


@dataclass(frozen=True)
class Record:
    seq: int
    op: str  # "put" | "del"
    key: str
    value: Any = None  # meaningful only for "put"


def encode_record(seq: int, op: str, key: str, value: Any = None) -> bytes:
    """Encode one record into its binary frame."""
    if op == "put":
        payload_obj = {"op": "put", "key": key, "value": value}
    elif op == "del":
        payload_obj = {"op": "del", "key": key}
    else:
        raise ValueError(f"unknown op: {op!r}")
    payload = json.dumps(
        payload_obj, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    if len(payload) > MAX_PAYLOAD:
        raise ValueError("payload too large")
    header = HEADER_STRUCT.pack(MAGIC, VERSION, seq, len(payload), zlib.crc32(payload))
    return header + payload


def decode_frame(frame: bytes, expected_seq: int | None = None) -> Record:
    """Decode a complete frame. Raises CorruptionError on any mismatch."""
    if len(frame) < HEADER_SIZE:
        raise CorruptionError("frame shorter than header")
    magic, version, seq, payload_len, crc = HEADER_STRUCT.unpack(
        frame[:HEADER_SIZE]
    )
    if magic != MAGIC:
        raise CorruptionError(f"bad magic: {magic!r}")
    if version != VERSION:
        raise CorruptionError(f"unsupported version: {version}")
    payload = frame[HEADER_SIZE:]
    if len(payload) != payload_len:
        raise CorruptionError("payload length mismatch")
    if zlib.crc32(payload) != crc:
        raise CorruptionError("crc32 mismatch")
    record = _decode_payload(seq, payload)
    if expected_seq is not None and record.seq != expected_seq:
        raise SeqGapError(
            f"sequence hole: expected seq {expected_seq}, found {record.seq}"
        )
    return record


def _decode_payload(seq: int, payload: bytes) -> Record:
    try:
        obj = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CorruptionError(f"payload is not valid JSON: {exc}") from exc
    if not isinstance(obj, dict):
        raise CorruptionError("payload is not a JSON object")
    op = obj.get("op")
    key = obj.get("key")
    if op not in ("put", "del") or not isinstance(key, str):
        raise CorruptionError(f"invalid payload fields: {obj!r}")
    value = obj.get("value") if op == "put" else None
    return Record(seq=seq, op=op, key=key, value=value)
