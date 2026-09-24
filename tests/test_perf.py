import time

from wal.log import WAL


def test_batch_append_200k_under_3_seconds(tmp_path):
    d = str(tmp_path / "w")
    wal = WAL(d, durability="batch", batch_n=1000, batch_ms=50)
    t0 = time.monotonic()
    for i in range(200_000):
        wal.append("put", f"key-{i % 1000}", i)
    wal.close()
    elapsed = time.monotonic() - t0
    assert wal.last_seq == 200_000
    assert elapsed < 3.0, f"200k batch appends took {elapsed:.2f}s"
