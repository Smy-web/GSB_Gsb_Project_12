"""Binary record frame format.

Layout (all integers big-endian):

    offset  size  field
    ------  ----  -----
    0       2     magic   = b"WL"
    2       1     version = 1
    3       8     seq     (uint64)
    11      4     payload_len (uint32)
    15      4     crc32(payload) (uint32)
    19      N     payload (JSON UTF-8): {"op": "put"|"del", "key": ..., "value": ...}
"""

from __future__ import annotations

import json
import struct
import zlib

MAGIC = b"WL"
VERSION = 1
HEADER = struct.Struct(">2sBQII")
HEADER_SIZE = HEADER.size  # 19
MAX_PAYLOAD_SIZE = 64 * 1024 * 1024

_MISSING = object()


class WALError(Exception):
    """Base class for all wal errors."""


class CorruptionError(WALError):
    """The log is corrupt in a way that is not a clean torn tail."""


class SeqGapError(WALError):
    """Record sequence numbers are not contiguous."""


class LockedError(WALError):
    """Another writer holds the exclusive lock."""


class InjectedCrash(WALError):
    """Raised by fault-injection hooks to simulate a process crash."""


def encode_payload(op, key, value=_MISSING):
    if op not in ("put", "del"):
        raise ValueError(f"bad op: {op!r}")
    if not isinstance(key, (str, int, float, bool, type(None))):
        raise ValueError("key must be a JSON scalar (str/number/bool/null)")
    obj = {"op": op, "key": key}
    if op == "put":
        if value is _MISSING:
            raise ValueError("put requires a value")
        obj["value"] = value
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def decode_payload(data):
    try:
        obj = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CorruptionError(f"payload is not valid JSON: {exc}") from exc
    if not isinstance(obj, dict) or obj.get("op") not in ("put", "del") or "key" not in obj:
        raise CorruptionError("payload is not a wal operation")
    return obj


def encode_frame(seq, payload):
    crc = zlib.crc32(payload) & 0xFFFFFFFF
    return HEADER.pack(MAGIC, VERSION, seq, len(payload), crc) + payload


def decode_header(buf):
    """Parse a header. Raises CorruptionError on bad magic/version/length."""
    if len(buf) < HEADER_SIZE:
        raise CorruptionError("short header")
    magic, version, seq, payload_len, crc = HEADER.unpack(buf)
    if magic != MAGIC:
        raise CorruptionError("bad magic")
    if version != VERSION:
        raise CorruptionError(f"unsupported version {version}")
    if payload_len > MAX_PAYLOAD_SIZE:
        raise CorruptionError(f"payload_len {payload_len} exceeds limit")
    return seq, payload_len, crc


def check_crc(payload, crc):
    return (zlib.crc32(payload) & 0xFFFFFFFF) == crc
