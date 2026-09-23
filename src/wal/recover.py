"""Crash recovery: validate frames, truncate torn tails, optional salvage."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from . import frame
from .frame import CorruptionError, WALError


@dataclass(frozen=True)
class FrameInfo:
    seq: int
    offset: int
    end: int
    payload: bytes


@dataclass(frozen=True)
class ScanResult:
    frames: tuple
    good_end: int      # offset just past the last valid frame
    file_size: int
    error: str | None  # why scanning stopped; None on clean EOF


@dataclass(frozen=True)
class RecoveryStats:
    complete_records: int
    dropped_bytes: int
    last_seq: int
    salvaged: bool = False


def _frame_at(data, pos):
    """Return (seq, end, payload) if a valid frame starts at pos, else None."""
    if pos + frame.HEADER_SIZE > len(data):
        return None
    try:
        seq, payload_len, crc = frame.decode_header(data[pos:pos + frame.HEADER_SIZE])
    except CorruptionError:
        return None
    end = pos + frame.HEADER_SIZE + payload_len
    if end > len(data):
        return None
    payload = data[pos + frame.HEADER_SIZE:end]
    if not frame.check_crc(payload, crc):
        return None
    return seq, end, payload


def _diagnose(data, pos):
    remaining = len(data) - pos
    if remaining < frame.HEADER_SIZE:
        return f"torn header at offset {pos} ({remaining} of {frame.HEADER_SIZE} bytes)"
    try:
        _, payload_len, _ = frame.decode_header(data[pos:pos + frame.HEADER_SIZE])
    except CorruptionError as exc:
        return f"bad frame header at offset {pos}: {exc}"
    if pos + frame.HEADER_SIZE + payload_len > len(data):
        return f"torn payload at offset {pos}"
    return f"crc mismatch at offset {pos}"


def scan_segment(path):
    """Scan a segment file, returning the valid frame prefix."""
    data = Path(path).read_bytes()
    frames = []
    pos = 0
    error = None
    while pos < len(data):
        parsed = _frame_at(data, pos)
        if parsed is None:
            error = _diagnose(data, pos)
            break
        seq, end, payload = parsed
        frames.append(FrameInfo(seq=seq, offset=pos, end=end, payload=payload))
        pos = end
    return ScanResult(tuple(frames), pos, len(data), error)


def recover_segment(path, salvage=False):
    """Recover a single segment file in place. Caller must hold the dir lock."""
    path = Path(path)
    if salvage:
        return _salvage_segment(path)
    res = scan_segment(path)
    if res.error is not None:
        with open(path, "r+b") as fh:
            fh.truncate(res.good_end)
    return RecoveryStats(
        complete_records=len(res.frames),
        dropped_bytes=res.file_size - res.good_end,
        last_seq=res.frames[-1].seq if res.frames else 0,
    )


def _find_next_frame(data, start):
    i = data.find(frame.MAGIC, start)
    while i != -1:
        if _frame_at(data, i) is not None:
            return i
        i = data.find(frame.MAGIC, i + 1)
    return None


def _salvage_segment(path):
    """Skip unreadable records and keep every valid frame found after them.

    Original sequence numbers are preserved, so skipped records leave
    visible seq holes (readers report them as SeqGapError).
    """
    from .log import fsync_dir  # lazy import, avoids a module cycle

    data = path.read_bytes()
    good = []
    pos = 0
    skipped = 0
    last_seq = 0
    while pos < len(data):
        parsed = _frame_at(data, pos)
        if parsed is not None:
            seq, end, _ = parsed
            good.append(data[pos:end])
            last_seq = seq
            pos = end
            continue
        nxt = _find_next_frame(data, pos + 1)
        if nxt is None:
            skipped += len(data) - pos
            break
        skipped += nxt - pos
        pos = nxt
    if skipped:
        tmp = path.with_name(path.name + ".tmp")
        with open(tmp, "wb") as fh:
            for raw in good:
                fh.write(raw)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        fsync_dir(path.parent)
    return RecoveryStats(len(good), skipped, last_seq, salvaged=True)


def recover(dir, salvage=False):
    """Recover the current segment of a WAL directory.

    Default mode truncates at the first invalid/torn frame. Salvage mode
    additionally skips bad records and keeps later readable ones.
    """
    from .log import current_segment_path, lock_directory  # lazy, avoids cycle

    dir = Path(dir)
    if not dir.is_dir():
        raise WALError(f"no such directory: {dir}")
    with lock_directory(dir):
        seg = current_segment_path(dir)
        if seg is None:
            return RecoveryStats(0, 0, 0, salvaged=salvage)
        return recover_segment(seg, salvage=salvage)
