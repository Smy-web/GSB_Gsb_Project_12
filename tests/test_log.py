"""Read API, seq-hole errors, CLI behaviour and dump determinism."""

import os

import pytest

from wal.cli import main as cli_main
from wal.frame import MAIN_LOG, CorruptionError, SeqGapError, encode_record
from wal.log import WAL, iter_records, snapshot_at


@pytest.fixture
def log_dir(tmp_path):
    d = str(tmp_path / "w")
    with WAL(d, durability="every") as wal:
        wal.append("put", "a", 1)
        wal.append("put", "b", 2)
        wal.append("put", "a", 3)
        wal.append("del", "b")
        wal.append("put", "c", {"x": [1, 2]})
    return d


def test_iter_records_from_seq(log_dir):
    assert [r.seq for r in iter_records(log_dir)] == [1, 2, 3, 4, 5]
    assert [r.seq for r in iter_records(log_dir, from_seq=3)] == [3, 4, 5]
    assert list(iter_records(log_dir, from_seq=6)) == []


def test_snapshot_at(log_dir):
    assert snapshot_at(log_dir, 0) == {}
    assert snapshot_at(log_dir, 1) == {"a": 1}
    assert snapshot_at(log_dir, 3) == {"a": 3, "b": 2}
    assert snapshot_at(log_dir, 4) == {"a": 3}  # b deleted
    assert snapshot_at(log_dir, 5) == {"a": 3, "c": {"x": [1, 2]}}


def test_snapshot_beyond_last_seq(log_dir):
    with pytest.raises(ValueError):
        snapshot_at(log_dir, 6)


def test_seq_hole_raises_not_skipped(tmp_path):
    d = str(tmp_path / "w")
    os.makedirs(d)
    blob = b"".join(
        encode_record(i, "put", f"k{i}", i) for i in (1, 2, 4)  # seq 3 missing
    )
    with open(os.path.join(d, MAIN_LOG), "wb") as fh:
        fh.write(blob)
    with pytest.raises(SeqGapError):
        list(iter_records(d))
    with pytest.raises(SeqGapError):
        snapshot_at(d, 2)


def test_mid_log_corruption_raises(tmp_path):
    d = str(tmp_path / "w")
    os.makedirs(d)
    bad = bytearray(encode_record(2, "put", "k2", 2))
    bad[-1] ^= 0xFF
    blob = encode_record(1, "put", "k1", 1) + bytes(bad) + encode_record(3, "put", "k3", 3)
    with open(os.path.join(d, MAIN_LOG), "wb") as fh:
        fh.write(blob)
    with pytest.raises(CorruptionError):
        list(iter_records(d))


def test_reader_tolerates_torn_tail(log_dir):
    path = os.path.join(log_dir, MAIN_LOG)
    size = os.path.getsize(path)
    with open(path, "r+b") as fh:
        fh.truncate(size - 3)  # partial last frame, as after a crash
    assert [r.seq for r in iter_records(log_dir)] == [1, 2, 3, 4]


def test_dump_is_byte_identical_across_runs(log_dir, capsys):
    assert cli_main(["dump", "--dir", log_dir]) == 0
    first = capsys.readouterr().out
    assert cli_main(["dump", "--dir", log_dir]) == 0
    second = capsys.readouterr().out
    assert first == second
    assert first.count("\n") == 5


def test_cli_roundtrip(tmp_path, capsys):
    d = str(tmp_path / "w")
    assert cli_main(["append", "--dir", d, "--key", "x", "--value", '{"n": 1}']) == 0
    assert cli_main(["append", "--dir", d, "--key", "y", "--value", "hi"]) == 0
    assert cli_main(["del", "--dir", d, "--key", "x"]) == 0
    capsys.readouterr()

    assert cli_main(["get", "--dir", d, "--key", "y"]) == 0
    assert capsys.readouterr().out == '"hi"\n'
    assert cli_main(["get", "--dir", d, "--key", "x"]) == 1  # deleted
    assert cli_main(["get", "--dir", d, "--key", "zz"]) == 1  # never existed

    assert cli_main(["recover", "--dir", d]) == 0
    out = capsys.readouterr().out
    assert '"complete_records":3' in out
    assert cli_main(["recover", "--dir", d, "--mode", "salvage"]) == 0
    capsys.readouterr()
    assert cli_main(["compact", "--dir", d]) == 0
    assert '"records_after":2' in capsys.readouterr().out  # y=put + x=tombstone retained


def test_cli_exit_code_2_on_unrecoverable_corruption(tmp_path, capsys):
    d = str(tmp_path / "w")
    os.makedirs(d)
    bad = bytearray(encode_record(1, "put", "k1", 1))
    bad[-1] ^= 0xFF
    good = encode_record(2, "put", "k2", 2)
    with open(os.path.join(d, MAIN_LOG), "wb") as fh:
        fh.write(bytes(bad) + good)  # mid-log corruption (not a torn tail)
    assert cli_main(["dump", "--dir", d]) == 2
    assert cli_main(["recover", "--dir", d]) == 2
    capsys.readouterr()


def test_cli_usage_error_exit_2():
    with pytest.raises(SystemExit) as exc:
        cli_main(["append", "--key", "x"])  # missing --value
    assert exc.value.code == 2
