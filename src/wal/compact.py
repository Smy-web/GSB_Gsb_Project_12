"""Log compaction: keep only the latest op per key, atomically swap segments.

Crash-safety protocol (generation-gated segment swap):

    1. write wal-<g+1>.log.tmp with the compacted records, fsync the file
    2. [hook: before_rename]  os.replace(tmp, wal-<g+1>.log)  (atomic rename)
    3. [hook: after_rename]   fsync the directory
    4. [hook: before_delete_old]  delete segments with generation < g+1
    5. [hook: after_delete_old]   fsync the directory again

A crash at any point leaves either the old generation or the fully fsynced
new generation readable; readers always pick the highest generation, so a
half-new/half-old state is impossible. Leftover .tmp files are ignored and
cleaned up on the next open.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from . import frame
from .frame import WALError
from .log import (
    current_segment_path,
    fsync_dir,
    list_segments,
    lock_directory,
    segment_name,
)
from .recover import scan_segment

HOOK_POINTS = ("before_rename", "after_rename", "before_delete_old", "after_delete_old")


@dataclass(frozen=True)
class CompactStats:
    records_in: int
    records_out: int
    bytes_before: int
    bytes_after: int
    generation: int


def _fire(hooks, name):
    hook = hooks.get(name)
    if hook is not None:
        hook()


def compact(dir, hooks=None):
    """Compact the current segment. hooks: name -> callable, see HOOK_POINTS."""
    hooks = dict(hooks or {})
    unknown = set(hooks) - set(HOOK_POINTS)
    if unknown:
        raise ValueError(f"unknown compact hook(s): {sorted(unknown)}")
    dir = Path(dir)
    if not dir.is_dir():
        raise WALError(f"no such directory: {dir}")
    with lock_directory(dir):
        return _compact_locked(dir, hooks)


def _compact_locked(dir, hooks):
    seg = current_segment_path(dir)
    if seg is None:
        return CompactStats(0, 0, 0, 0, 0)
    generation = int(seg.name[len("wal-"):-len(".log")])
    res = scan_segment(seg)  # valid prefix only; a torn tail is discarded

    latest = {}  # key -> payload dict, ordered by seq of the key's last op
    for fi in res.frames:
        obj = frame.decode_payload(fi.payload)
        key = obj["key"]
        if key in latest:
            del latest[key]
        latest[key] = obj

    new_generation = generation + 1
    tmp = dir / (segment_name(new_generation) + ".tmp")
    final = dir / segment_name(new_generation)
    seq = 0
    with open(tmp, "wb") as out:
        for obj in latest.values():
            seq += 1
            if obj["op"] == "put":
                payload = frame.encode_payload("put", obj["key"], obj.get("value"))
            else:
                payload = frame.encode_payload("del", obj["key"])
            out.write(frame.encode_frame(seq, payload))
        out.flush()
        os.fsync(out.fileno())

    _fire(hooks, "before_rename")
    os.replace(tmp, final)
    _fire(hooks, "after_rename")
    fsync_dir(dir)

    _fire(hooks, "before_delete_old")
    for old_generation, old_path in list_segments(dir):
        if old_generation < new_generation:
            old_path.unlink()
    _fire(hooks, "after_delete_old")
    fsync_dir(dir)

    return CompactStats(
        records_in=len(res.frames),
        records_out=seq,
        bytes_before=res.file_size,
        bytes_after=final.stat().st_size,
        generation=new_generation,
    )
