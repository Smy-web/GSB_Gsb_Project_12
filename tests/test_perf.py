"""Performance: 200k appends in batch mode must finish in under 3 seconds."""

import time

from wal.log import WAL, iter_records


def test_batch_append_200k_under_3s(tmp_path):
    wal = WAL(tmp_path, durability="batch", batch_n=4096, batch_ms=1000)
    start = time.perf_counter()
    for i in range(200_000):
        wal.put(f"key-{i}", i)
    wal.close()
    elapsed = time.perf_counter() - start
    assert elapsed < 3.0, f"200k batch appends took {elapsed:.2f}s"
    assert sum(1 for _ in iter_records(tmp_path)) == 200_000
