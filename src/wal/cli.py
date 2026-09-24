"""Command line interface: python3 -m wal <command> ...

Exit codes: 0 = success, 2 = usage error or unrecoverable corruption.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from .compact import compact as compact_dir
from .frame import CorruptionError
from .log import WAL, iter_records, snapshot_at
from .recover import recover


def _json_line(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wal", description="append-only WAL key-value ledger"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def add_dir(p, default=None):
        p.add_argument(
            "--dir",
            default=default or os.environ.get("WAL_DIR", "./wal_data"),
            help="log directory (default: $WAL_DIR or ./wal_data)",
        )

    p = sub.add_parser("append", help="append a put record")
    add_dir(p)
    p.add_argument("--key", required=True)
    p.add_argument("--value", required=True, help="JSON value")

    p = sub.add_parser("del", help="append a tombstone")
    add_dir(p)
    p.add_argument("--key", required=True)

    p = sub.add_parser("get", help="print the current value of a key")
    add_dir(p)
    p.add_argument("--key", required=True)

    p = sub.add_parser("dump", help="print all records as JSONL, ordered by seq")
    add_dir(p)

    p = sub.add_parser("recover", help="recover the log (strict truncates torn tails)")
    add_dir(p)
    p.add_argument(
        "--mode",
        choices=("strict", "salvage"),
        default="strict",
        help="salvage is read-only and skips bad records",
    )

    p = sub.add_parser("compact", help="rewrite the log keeping latest op per key")
    add_dir(p)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "append":
            try:
                value = json.loads(args.value)
            except json.JSONDecodeError:
                value = args.value  # treat non-JSON as a plain string
            with WAL(args.dir, durability="every") as wal:
                seq = wal.append("put", args.key, value)
            print(_json_line({"seq": seq, "op": "put", "key": args.key}))
        elif args.command == "del":
            with WAL(args.dir, durability="every") as wal:
                seq = wal.append("del", args.key)
            print(_json_line({"seq": seq, "op": "del", "key": args.key}))
        elif args.command == "get":
            with WAL(args.dir, durability="every") as wal:
                view = wal.snapshot_at(wal.last_seq)
            if args.key not in view:
                return 1
            print(_json_line(view[args.key]))
        elif args.command == "dump":
            for record in iter_records(args.dir):
                obj = {"seq": record.seq, "op": record.op, "key": record.key}
                if record.op == "put":
                    obj["value"] = record.value
                print(_json_line(obj))
        elif args.command == "recover":
            stats = recover(args.dir, mode=args.mode)
            print(
                _json_line(
                    {
                        "mode": stats.mode,
                        "complete_records": stats.complete_records,
                        "discarded_bytes": stats.discarded_bytes,
                        "last_seq": stats.last_seq,
                        "truncated": stats.truncated,
                        "seq_gaps": [list(g) for g in stats.seq_gaps],
                    }
                )
            )
        elif args.command == "compact":
            stats = compact_dir(args.dir)
            print(
                _json_line(
                    {
                        "records_before": stats.records_before,
                        "records_after": stats.records_after,
                        "bytes_before": stats.bytes_before,
                        "bytes_after": stats.bytes_after,
                    }
                )
            )
    except CorruptionError as exc:
        print(f"wal: unrecoverable corruption: {exc}", file=sys.stderr)
        return 2
    except (OSError, ValueError, TimeoutError) as exc:
        print(f"wal: error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
