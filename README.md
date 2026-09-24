# wal — append-only WAL 键值账本

纯标准库实现的 append-only 预写日志键值账本（Python 3.10+，开发验证环境
WSL2 Ubuntu / Python 3.14 / pytest 9）。

## 运行

```bash
cd B
PYTHONPATH=src python3 -m wal append --dir ./data --key foo --value '{"n":1}'
PYTHONPATH=src python3 -m wal del     --dir ./data --key foo
PYTHONPATH=src python3 -m wal get     --dir ./data --key foo
PYTHONPATH=src python3 -m wal dump    --dir ./data          # 按 seq 输出 JSONL
PYTHONPATH=src python3 -m wal recover --dir ./data          # 默认 strict
PYTHONPATH=src python3 -m wal recover --dir ./data --mode salvage
PYTHONPATH=src python3 -m wal compact --dir ./data
python3 -m pytest                                          # 测试（conftest 自动加 src 到 path）
```

退出码：`0` 成功；`1` 仅用于 `get` 未命中；`2` 用法错误或损坏不可恢复。
`--dir` 缺省取 `$WAL_DIR`，再缺省 `./wal_data`。

## 一、帧格式

```
 0       2       3              11      15          19
 +-------+-------+--------------+-------+-----------+============+
 | magic | ver   | seq          | payln | crc32     | payload    |
 | "WL"  | 0x01  | u64 大端     | u32   | u32       | JSON UTF-8 |
 +-------+-------+--------------+-------+-----------+============+
 |<----------- 19 字节定长头 ----------->|<-- payln 字节 -->|
```

- 所有整数大端；`crc32` 只覆盖 payload（`zlib.crc32`）。
- payload：`{"op":"put","key":...,"value":...}` 或 `{"op":"del","key":...}`，
  `key` 必须是字符串，`value` 为任意 JSON 值。
- seq 从 1 开始严格连续（compact 会重编号，见下）。
- 单写者：写进程对 `wal.lock` 持 `fcntl.flock` 排他锁（可配 `lock_timeout`）；
  读者只读打开，只能看到已 fsync 的前缀——写了一半的尾部对读者不可见。

## 二、崩溃恢复

### strict（默认）

顺序扫描：逐帧校验 magic/version/长度/crc/seq 连续性。第一个校验失败或
长度不足的帧即"撕裂写"边界，`recover()` 把文件**截断到最后一条完整记录**，
并返回统计：完整记录数、丢弃字节数、最后完整 seq。截断后 fsync 文件与目录。

注意区分两种坏帧：

- **撕裂尾**（帧头合法但帧体越过 EOF，或头部本身被截短）：视为崩溃现场，
  截断即可，属正常恢复路径；
- **中段损坏**（帧完整但 crc 错、magic 错、seq 空洞）：说明磁盘/数据本身
  已坏，strict 直接抛 `CorruptionError`（CLI 退出码 2），绝不静默截掉
  "中间一段"后继续——那会造成数据语义的静默分叉。

### salvage（人工抢救，默认关闭）

逐字节滑动寻找下一个 magic 重新同步，跳过坏帧继续收集后面可读记录。
**只读**，不修改文件；返回可读记录与 seq 空洞清单（`seq_gaps`）。
与 strict 的差异：strict 保证"结果一定是某个完整前缀"，salvage 不保证
任何前缀性质，只保证"把还能解码的记录挑出来"，供人工导出后重建。

### 恢复不变量及证明思路

**不变量**：对任意合法日志 L（帧序列 F1..Fn 的字节拼接）与任意字节偏移
`0 ≤ c ≤ |L|`，令 `k = max{ j : |F1..Fj| ≤ c }`，则对 `L[:c]` 执行
`recover()` 后：

1. 日志内容恰好为 `F1..Fk`（完整记录前缀）；
2. 统计为 `complete_records=k`、`discarded_bytes=c-|F1..Fk|`、`last_seq=k`；
3. 之后第一次 append 的 seq 为 `k+1`，链路 `1..k+1` 连续不重号。

**证明思路**：

- (1) 帧是自定界的：帧头含 `payload_len`，帧 `Fi` 占据字节区间
  `[off_i, off_i + 19 + len_i)`，各区间互不重叠且顺序相接。扫描器从偏移 0
  开始，每读一帧就跳到 `off_i + 19 + len_i`，因此扫描位置永远落在帧边界上。
  对截断文件 `L[:c]`：若 `c` 落在某帧 `F_{k+1}` 内部，则扫描到 `off_{k+1}`
  时头部或帧体不足（或 crc 不符——截断必然破坏该帧的 crc 校验），判定为
  撕裂尾，截断到 `off_{k+1} = |F1..Fk|`。若 `c` 恰在帧边界，则扫描到 EOF
  自然停止，无需截断。两种情形结果都是 `F1..Fk`。
- (2) 由扫描器直接计数可得；且 recover 幂等（对已恢复文件再跑一次，
  `discarded_bytes=0`）。
- (3) seq 不变式：日志在任意时刻都是 `seq = 1..m` 的连续前缀（归纳：空日志
  m=0 成立；append 只写 `seq = last_seq + 1`；recover 只删尾部不改内容；
  compact 整体重编号为 `1..m'`）。因此恢复后 `last_seq = m = k`，下一条
  append 取 `k+1`，连续性保持。

`tests/test_recover.py` 把一条 12 帧日志在**每一个字节偏移**（含 0 和全长）
截断一次做参数化暴力验证（600+ 个用例），逐一断言上述三条。

## 三、持久化档位与丢数据边界

`WAL(dir, durability=..., batch_n=N, batch_ms=T)`：

| 档位 | fsync 时机 | 崩溃时丢数据边界 |
|------|-----------|------------------|
| `none` | 从不（`flush()`/`close()` 也不刷） | 可能丢**全部**已返回成功的记录 |
| `batch` | 每 N 条或每 T 毫秒（先到先触发） | 最多丢**最近一个批次**（自上次 fsync 以来的记录） |
| `every` | 每次 append 返回前 | **不丢**任何已返回成功的记录 |

语义要点：丢的永远是**尾部的一段连续记录**，绝不出现"中间丢一条"。恢复后
seq 从最后一个幸存 seq 续号，因此重放方看到的永远是某个前缀。
测试用 `Hooks.before_fsync` 注入 `CrashSimulated` 模拟"fsync 前进程死"，
再把文件截到 `wal.durable_offset`（最后一次 fsync 的位置）模拟掉电，
分别验证三档的边界（`tests/test_durability.py`）。

## 四、压缩 compact()

只保留每个 key 的最新 op（**含 tombstone**），保留的记录重编号为
`1..m` 连续 seq。协议（单写者锁内执行）：

```
1. 写 wal.log.compacting（重编号后的新日志）+ fsync 文件 + fsync 目录
2. link(wal.log, wal.log.old)            # 硬链接保留旧段
3. rename(wal.log.compacting, wal.log)   # 原子替换
4. fsync(目录)                            # rename 在此持久化
5. unlink(wal.log.old) + fsync(目录)     # 旧段只在 rename 持久化后删除
```

`recover()` 开头的 `finish_pending_compaction()` 识别四种中间态
（main+tmp / old+tmp / old+main / 仅 old）并收敛到单一日志。因为
rename 是原子的，且 tmp 在 rename 前已 fsync、目录项在 rename 后已 fsync，
任意时刻崩溃后可见状态**要么完全是旧日志、要么完全是新日志**，两者重放
出的键值视图相同，绝不出现半新半旧。四个注入点
（`before_rename` / `after_rename` / `before_delete_old` /
`after_delete_old`）由 `CompactionHooks` 暴露，测试逐点注入崩溃验证
（`tests/test_compact.py`）。

### 取舍：tombstone 在 compact 后**保留**

我选择保留 tombstone（`del` 记录作为该 key 的"最新 op"留在压缩后日志里）。
理由与影响：

- **正确性简单**：压缩前后日志重放出的任意 `snapshot_at(seq)` 视图完全
  一致，不需要额外的"已删除集合"元数据；物理删除 tombstone 则需要引入
  压缩水位/代际元数据才能保证与旧快照语义一致。
- **对读者的影响**：`snapshot_at` / `get` 视图无任何差异（tombstone 本来就
  不产生可见键）；代价是**空间**——被删 key 会永久占用一条小记录，反复
  compact 也无法回收。若某天写入以"删多于存"为主，需要再加一代际标记
  （例如压缩时把"早于某 seq 的 tombstone"物理丢弃），当前版本刻意不做。

## 五、读接口

- `iter_records(dir, from_seq=1)` / `WAL.iter_records(...)`：按 seq 顺序产出
  记录；遇到 seq 空洞抛 `SeqGapError`（显式报错，绝不静默跳过），中段损坏
  抛 `CorruptionError`，撕裂尾对读者透明（迭代到完整前缀为止）。
- `snapshot_at(dir, seq)` / `WAL.snapshot_at(seq)`：返回恰好应用完第 `seq`
  条记录后的键值视图；`seq` 超过最后一条记录抛 `ValueError`。

## 目录结构

```
src/wal/frame.py    帧编解码、Record、异常
src/wal/log.py      WAL（单写锁、三档持久化、Hooks）、iter_records、snapshot_at
src/wal/recover.py  strict/salvage 扫描器、recover()、压缩中间态收敛
src/wal/compact.py  compact() 与四个崩溃注入点
src/wal/cli.py      命令行（__main__.py 入口）
tests/              帧往返、任意偏移截断不变量、salvage 差异、三档崩溃语义、
                    压缩四注入点、seq 空洞、多进程写锁互斥、20 万条 < 3s、
                    dump 逐字节确定性
```
