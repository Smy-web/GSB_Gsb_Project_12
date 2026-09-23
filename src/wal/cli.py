"""Command line interface: python3 -m wal <command> [options].

Exit codes: 0 = success, 2 = usage error or unrecoverable corruption.
"""

from __future__ import annotations

import argparse
import json
import sys

from .compact import compact
from .frame import WALError
from .log import WAL, iter_records, snapshot_at
from .recover import recover


def _parse_value(text):
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _add_dir(parser):
    parser.add_argument("--dir", default=".", help="WAL directory (default: cwd)")


def build_parser():
    parser = argparse.ArgumentParser(prog="wal", description="append-only WAL key-value ledger")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("append", help="append a put record")
    _add_dir(p)
    p.add_argument("--key", required=True)
    p.add_argument("--value", required=True, help="JSON if parseable, else a plain string")
    p.add_argument("--durability", choices=["none", "batch", "every"], default="every")

    p = sub.add_parser("del", help="append a delete (tombstone) record")
    _add_dir(p)
    p.add_argument("--key", required=True)

    p = sub.add_parser("get", help="print the current value of a key")
    _add_dir(p)
    p.add_argument("--key", required=True)

    p = sub.add_parser("dump", help="print all records as JSONL, ordered by seq")
    _add_dir(p)

    p = sub.add_parser("recover", help="truncate torn tail (or salvage readable records)")
    _add_dir(p)
    p.add_argument("--salvage", action="store_true", help="skip bad records, keep later valid ones")

    p = sub.add_parser("compact", help="rewrite the log keeping only the latest op per key")
    _add_dir(p)

    return parser


def _print_json(obj):
    print(json.dumps(obj, ensure_ascii=False, sort_keys=True))


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        if args.command == "append":
            with WAL(args.dir, durability=args.durability) as wal:
                seq = wal.put(args.key, _parse_value(args.value))
            _print_json({"seq": seq})
        elif args.command == "del":
            with WAL(args.dir) as wal:
                seq = wal.delete(args.key)
            _print_json({"seq": seq})
        elif args.command == "get":
            view = snapshot_at(args.dir)
            if args.key not in view:
                print(f"wal: key not found: {args.key}", file=sys.stderr)
                return 2
            print(json.dumps(view[args.key], ensure_ascii=False))
        elif args.command == "dump":
            for rec in iter_records(args.dir):
                obj = {"seq": rec.seq, "op": rec.op, "key": rec.key}
                if rec.op == "put":
                    obj["value"] = rec.value
                _print_json(obj)
        elif args.command == "recover":
            stats = recover(args.dir, salvage=args.salvage)
            _print_json({
                "complete_records": stats.complete_records,
                "dropped_bytes": stats.dropped_bytes,
                "last_seq": stats.last_seq,
                "salvaged": stats.salvaged,
            })
        elif args.command == "compact":
            stats = compact(args.dir)
            _print_json({
                "records_in": stats.records_in,
                "records_out": stats.records_out,
                "bytes_before": stats.bytes_before,
                "bytes_after": stats.bytes_after,
                "generation": stats.generation,
            })
    except WALError as exc:
        print(f"wal: error: {exc}", file=sys.stderr)
        return 2
    return 0
