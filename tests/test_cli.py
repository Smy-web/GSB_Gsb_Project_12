"""CLI end-to-end tests: python3 -m wal ..."""

import json
import os
import subprocess
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent / "src"


def run_cli(*args):
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SRC) + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.run(
        [sys.executable, "-m", "wal", *args],
        capture_output=True, text=True, env=env,
    )


def test_append_get_del_roundtrip(tmp_path):
    d = str(tmp_path)
    r = run_cli("append", "--dir", d, "--key", "a", "--value", "1")
    assert r.returncode == 0, r.stderr
    assert json.loads(r.stdout) == {"seq": 1}

    r = run_cli("append", "--dir", d, "--key", "b", "--value", '{"x": [1, 2]}')
    assert r.returncode == 0
    assert json.loads(r.stdout) == {"seq": 2}

    r = run_cli("get", "--dir", d, "--key", "a")
    assert r.returncode == 0 and r.stdout.strip() == "1"
    r = run_cli("get", "--dir", d, "--key", "b")
    assert json.loads(r.stdout) == {"x": [1, 2]}

    r = run_cli("del", "--dir", d, "--key", "a")
    assert r.returncode == 0
    r = run_cli("get", "--dir", d, "--key", "a")
    assert r.returncode == 2  # key not found


def test_dump_is_byte_identical_across_runs(tmp_path):
    from wal.log import WAL

    with WAL(tmp_path) as wal:
        for i in range(100):
            wal.put(f"k{i}", {"n": i, "s": f"v{i}"})
        wal.delete("k7")

    first = run_cli("dump", "--dir", str(tmp_path))
    second = run_cli("dump", "--dir", str(tmp_path))
    assert first.returncode == second.returncode == 0
    assert first.stdout == second.stdout  # byte-identical
    lines = first.stdout.splitlines()
    assert len(lines) == 101
    assert json.loads(lines[0]) == {"key": "k0", "op": "put",
                                    "seq": 1, "value": {"n": 0, "s": "v0"}}
    assert json.loads(lines[-1]) == {"key": "k7", "op": "del", "seq": 101}


def test_recover_and_compact_commands(tmp_path):
    from wal import frame
    from wal.log import WAL, segment_name

    with WAL(tmp_path) as wal:
        for i in range(5):
            wal.put(f"k{i}", i)
    # append a torn tail by hand
    seg = tmp_path / segment_name(1)
    with open(seg, "ab") as fh:
        fh.write(frame.encode_frame(6, frame.encode_payload("put", "x", 1))[:7])

    r = run_cli("recover", "--dir", str(tmp_path))
    assert r.returncode == 0, r.stderr
    stats = json.loads(r.stdout)
    assert stats["complete_records"] == 5
    assert stats["dropped_bytes"] == 7

    r = run_cli("compact", "--dir", str(tmp_path))
    assert r.returncode == 0, r.stderr
    stats = json.loads(r.stdout)
    assert stats["records_out"] == 5

    r = run_cli("dump", "--dir", str(tmp_path))
    assert r.returncode == 0
    assert len(r.stdout.splitlines()) == 5


def test_dump_on_unrecoverable_corruption_exits_2(tmp_path):
    from wal.log import segment_name

    (tmp_path / segment_name(1)).write_bytes(b"this is not a wal segment at all")
    r = run_cli("dump", "--dir", str(tmp_path))
    assert r.returncode == 2
    assert "error" in r.stderr.lower()


def test_usage_error_exits_2(tmp_path):
    r = run_cli("append", "--dir", str(tmp_path))  # missing --key/--value
    assert r.returncode == 2


def test_get_on_missing_dir_exits_2(tmp_path):
    r = run_cli("get", "--dir", str(tmp_path / "nope"), "--key", "a")
    assert r.returncode == 2
