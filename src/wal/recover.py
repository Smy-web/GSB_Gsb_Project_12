"""Log scanning and crash recovery.

Two scan modes:

* strict  -- records must form one contiguous, uncorrupted prefix starting at
             seq 1.  The first bad/torn frame terminates the valid prefix and
             everything after it is discarded (truncated away by recover()).
* salvage -- after a bad frame, keep scanning byte-by-byte for the next magic
             and try to resynchronize, collecting later readable records.
             Read-only: never modifies the log.  For manual rescue only.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from .frame import (
    COMPACT_OLD,
    COMPACT_TMP,
    HEADER_STRUCT,
    HEADER_SIZE,
    MAGIC,
    MAIN_LOG,
    MAX_PAYLOAD,
    CorruptionError,
    Record,
    SeqGapError,
    decode_frame,
)


@dataclass
class ScanResult:
    records: list[Record]
    valid_bytes: int  # bytes belonging to the valid prefix
    total_bytes: int  # total file size scanned
    torn: bool  # stopped because of a torn/corrupt tail
    stop_reason: str = "eof"  # "eof" | "torn-tail" | "corrupt"
    seq_gaps: list[tuple[int, int]] = field(default_factory=list)  # salvage only

    @property
    def discarded_bytes(self) -> int:
        return self.total_bytes - self.valid_bytes


@dataclass
class RecoverStats:
    mode: str
    complete_records: int
    discarded_bytes: int
    last_seq: int
    truncated: bool  # whether the log file was modified
    seq_gaps: list[tuple[int, int]] = field(default_factory=list)


def _read_valid_frame(buf: bytes, pos: int, file_size: int) -> tuple[Record, int]:
    """Decode the frame at ``pos``. Raises CorruptionError; callers decide
    whether the failure is a torn tail (frame extends past EOF) or genuine
    mid-log corruption."""
    header = buf[pos : pos + HEADER_SIZE]
    if len(header) < HEADER_SIZE:
        raise CorruptionError("short header")
    magic, version, seq, payload_len, crc = HEADER_STRUCT.unpack(header)
    if magic != MAGIC:
        raise CorruptionError(f"bad magic at offset {pos}")
    if version != 1:
        raise CorruptionError(f"unsupported version {version} at offset {pos}")
    if payload_len > MAX_PAYLOAD:
        raise CorruptionError(f"absurd payload_len {payload_len} at offset {pos}")
    frame = buf[pos : pos + HEADER_SIZE + payload_len]
    if len(frame) < HEADER_SIZE + payload_len:
        raise CorruptionError("short payload")
    return decode_frame(frame), HEADER_SIZE + payload_len


def scan_strict(path: str) -> ScanResult:
    """Scan ``path`` requiring a contiguous, uncorrupted record prefix."""
    with open(path, "rb") as fh:
        buf = fh.read()
    file_size = len(buf)
    records: list[Record] = []
    pos = 0
    expected_seq = 1
    torn = False
    reason = "eof"
    while pos < file_size:
        try:
            record, frame_len = _read_valid_frame(buf, pos, file_size)
        except CorruptionError as exc:
            torn = True
            # A frame that runs past EOF is a torn tail (crash during append);
            # anything else is mid-log corruption.
            reason = "torn-tail" if _extends_past_eof(buf, pos, file_size) else "corrupt"
            if reason == "corrupt":
                raise CorruptionError(
                    f"corrupt record at offset {pos}: {exc}"
                ) from exc
            break
        if record.seq != expected_seq:
            raise SeqGapError(
                f"sequence hole at offset {pos}: expected seq {expected_seq}, "
                f"found {record.seq}"
            )
        records.append(record)
        pos += frame_len
        expected_seq += 1
    return ScanResult(
        records=records,
        valid_bytes=pos,
        total_bytes=file_size,
        torn=torn,
        stop_reason=reason,
    )


def _extends_past_eof(buf: bytes, pos: int, file_size: int) -> bool:
    """True if the frame header at ``pos`` is well-formed but the frame would
    extend beyond EOF (i.e. a plausible torn write)."""
    header = buf[pos : pos + HEADER_SIZE]
    if len(header) < HEADER_SIZE:
        return True  # truncated header: torn
    magic, version, seq, payload_len, crc = HEADER_STRUCT.unpack(header)
    if magic != MAGIC or version != 1 or payload_len > MAX_PAYLOAD:
        return False
    return pos + HEADER_SIZE + payload_len > file_size


def scan_salvage(path: str) -> ScanResult:
    """Best-effort scan: skip bad frames and resynchronize on the next magic.

    Never raises on corrupt data; never modifies the file.
    """
    with open(path, "rb") as fh:
        buf = fh.read()
    file_size = len(buf)
    records: list[Record] = []
    gaps: list[tuple[int, int]] = []
    pos = 0
    expected_seq = 1
    while pos < file_size:
        try:
            record, frame_len = _read_valid_frame(buf, pos, file_size)
        except CorruptionError:
            pos += 1  # slide forward, look for the next magic
            continue
        if record.seq != expected_seq:
            gaps.append((expected_seq, record.seq))
        expected_seq = record.seq + 1
        records.append(record)
        pos += frame_len
    return ScanResult(
        records=records,
        valid_bytes=file_size,
        total_bytes=file_size,
        torn=False,
        stop_reason="eof",
        seq_gaps=gaps,
    )


def finish_pending_compaction(dir_path: str) -> None:
    """Bring the directory to a single-log state after an interrupted compact().

    Compact protocol states (see compact.py):
      main + tmp  -> tmp not yet renamed: discard tmp, keep main.
      old + tmp   -> rename already durable: rename tmp -> main, delete old.
      old + main  -> rename durable, old not yet deleted: delete old.
      old only    -> crash between unlink(main) and rename: rename old -> main.
    """
    main = os.path.join(dir_path, MAIN_LOG)
    tmp = os.path.join(dir_path, COMPACT_TMP)
    old = os.path.join(dir_path, COMPACT_OLD)
    have_main = os.path.exists(main)
    have_tmp = os.path.exists(tmp)
    have_old = os.path.exists(old)

    if have_main and have_tmp:
        os.unlink(tmp)
    elif have_old and have_tmp and not have_main:
        os.rename(tmp, main)
        os.unlink(old)
        _fsync_dir(dir_path)
    elif have_old and have_main:
        os.unlink(old)
        _fsync_dir(dir_path)
    elif have_old and not have_main:
        os.rename(old, main)
        _fsync_dir(dir_path)


def recover(dir_path: str, mode: str = "strict") -> RecoverStats:
    """Recover the log in ``dir_path``.

    strict  (default): truncate the log to the last complete record so the
            valid prefix is exactly the set of complete records.
    salvage: read-only; scan past bad frames and report what is readable.
    """
    if mode not in ("strict", "salvage"):
        raise ValueError(f"unknown recover mode: {mode!r}")
    finish_pending_compaction(dir_path)
    path = os.path.join(dir_path, MAIN_LOG)
    if not os.path.exists(path):
        return RecoverStats(
            mode=mode, complete_records=0, discarded_bytes=0,
            last_seq=0, truncated=False,
        )
    if mode == "salvage":
        result = scan_salvage(path)
        return RecoverStats(
            mode=mode,
            complete_records=len(result.records),
            discarded_bytes=0,
            last_seq=result.records[-1].seq if result.records else 0,
            truncated=False,
            seq_gaps=result.seq_gaps,
        )
    result = scan_strict(path)
    truncated = False
    if result.discarded_bytes > 0:
        with open(path, "r+b") as fh:
            fh.truncate(result.valid_bytes)
            fh.flush()
            os.fsync(fh.fileno())
        _fsync_dir(dir_path)
        truncated = True
    return RecoverStats(
        mode=mode,
        complete_records=len(result.records),
        discarded_bytes=result.discarded_bytes,
        last_seq=result.records[-1].seq if result.records else 0,
        truncated=truncated,
    )


def _fsync_dir(dir_path: str) -> None:
    fd = os.open(dir_path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
