# wal — append-only WAL 键值账本

只依赖 Python 标准库（3.14）实现的追加式写前日志键值存储。单写者（`flock`
排他锁）+ 多读者（只读打开，可读到已 fsync 的前缀）。

## 目录结构

```
src/wal/
  frame.py     # 二进制帧编解码、异常体系
  log.py       # WAL 写入器（三档持久化）、iter_records / snapshot_at
  recover.py   # 崩溃恢复（默认截断模式 + salvage 抢救模式）
  compact.py   # 压缩：最新 op 重写 + 原子换段
  cli.py       # 命令行入口
  __main__.py  # python3 -m wal
tests/         # pytest 套件
```

## 快速开始

```bash
export PYTHONPATH=src          # 或 cd src
python3 -m wal append --dir data --key foo --value '{"n": 1}'
python3 -m wal append --dir data --key bar --value 2 --durability batch
python3 -m wal del    --dir data --key bar
python3 -m wal get    --dir data --key foo
python3 -m wal dump   --dir data          # 按 seq 输出 JSONL
python3 -m wal recover --dir data         # 可加 --salvage
python3 -m wal compact --dir data
python3 -m pytest tests/ -q               # 运行测试
```

退出码：`0` 成功；`2` 用法错误（argparse）或损坏不可恢复 / key 不存在 /
目录不存在 / 写锁被占用。

## 一、记录帧格式

所有整数大端。帧头定长 19 字节：

```
 offset  size  字段            说明
 ------  ----  --------------  --------------------------------
  0      2     magic           固定 b"WL" (0x57 0x4C)
  2      1     version         固定 1
  3      8     seq             uint64，全局单调递增，从 1 开始
 11      4     payload_len     uint32，payload 字节数（上限 64 MiB）
 15      4     crc32           uint32，zlib.crc32(payload)
 19      N     payload         JSON UTF-8
```

payload 形如 `{"op":"put","key":K,"value":V}` 或 `{"op":"del","key":K}`
（del 无 value 字段）。key 限 JSON 标量，value 任意可 JSON 序列化值。

日志目录内文件：

- `wal-000001.log`、`wal-000002.log` …：代际（generation）编号的段文件，
  读者永远只读最高代；
- `LOCK`：写者 / recover / compact 的 `fcntl.flock` 排他锁文件；
- `*.tmp`：压缩 / salvage 的临时文件，打开时被清理。

## 二、崩溃恢复

### 默认模式（截断）

`recover(dir)` 从偏移 0 顺序扫描：解析帧头 → 按 `payload_len` 取 payload →
校验 crc32。第一处「帧头不足 19B / payload 长度不足 / magic、version、
长度上限非法 / crc 不符」即判定为撕裂写（torn write），把段文件**截断到最后
一个完整帧的末尾**，返回统计：

- `complete_records`：完整记录数
- `dropped_bytes`：丢弃字节数
- `last_seq`：最后一条完整记录的 seq（空日志为 0）

### 恢复不变量与证明思路

**不变量**：对任意日志，在任意字节偏移截断（模拟进程崩溃）后 `recover`，
得到的必然是「原完整记录序列的某个前缀」，且再次 append 时 seq 连续、不重号。

证明思路：

1. 写者是单写者（flock 排他），只以 `O_APPEND` 顺序追加，因此文件内容在任意
   时刻都是「已写字节流的某个前缀」——崩溃只能让尾部变短，不会让中间出现
   空洞或乱序。
2. 扫描器从偏移 0 起逐帧验证：帧 i 完整且 crc 正确才被接受，且帧 i+1 的起点
   由帧 i 的长度字段唯一确定。因此「被接受帧的集合」关于帧序列构成前缀：
   一旦帧 k 无效，扫描立即停止，不存在「跳过 k 接受 k+1」。
3. 截断把文件物理截到第 k 帧末尾，被丢弃的尾部字节永久消失。下次 append 的
   seq = `last_seq + 1` = k+1：被丢弃记录占用的 seq 随记录本身一起消失，不会
   重号；新记录紧接第 k 条，不会跳号。
4. 掉电（非进程崩溃）可能让未 fsync 的页缓存以非前缀形式丢失，此时中间帧的
   crc 会不符，同样在第 2 步被截断点捕获——不变量退化为「已持久化前缀」，
   仍成立。

`tests/test_recover.py` 把 12 条记录的日志在**每一个字节偏移**（0..len）
各截断一次做参数化暴力验证（591 个用例），逐一断言前缀性、统计值、文件
长度和续写 seq 连续性。

### salvage 模式（默认关闭，人工抢救用）

`recover(dir, salvage=True)` / `python3 -m wal recover --dir data --salvage`：

| | 默认模式 | salvage 模式 |
|---|---|---|
| 遇到坏记录 | 截断，**丢弃其后所有记录** | 跳过坏记录，按 magic 重新同步，保留后面可读记录 |
| 结果 seq | 一定连续 | 可能有**空洞**（保留原 seq） |
| 读者表现 | 正常 | 空洞处显式抛 `SeqGapError`，绝不静默跳过 |
| 用途 | 常规崩溃恢复 | 中间记录损坏时的人工抢救 |

salvage 用「写临时文件 + fsync + 原子 rename + fsync 目录」的方式落盘。
抢救出的日志如有 seq 空洞，可用一次 `compact` 重编号消除。

## 三、持久化档位

`WAL(dir, durability=..., batch_n=N, batch_ms=T)`，或 CLI `--durability`：

| 档位 | fsync 时机 | 崩溃时丢数据边界 |
|---|---|---|
| `none` | 从不（除非手动 `sync()`） | 可能丢失**全部**未显式 sync 的已返回记录 |
| `batch` | 每 N 条 **或** 距上次 fsync ≥ T 毫秒（在 append 时检查） | 可能丢失**最近一批**（自上次 fsync 以来）已返回记录 |
| `every` | 每条 append 返回前 | **不丢**任何已返回成功的记录 |

`batch` 的 T 只在 append 路径上检查（无后台线程），空闲期间不触发 fsync。

故障注入：`WAL(..., hooks={"before_fsync": fn})`，hook 抛 `InjectedCrash`
即模拟「fsync 前进程死亡」——未 fsync 的尾部被丢弃，写者关闭。
`wal.simulate_crash()` 可在任意时刻模拟进程死亡。测试见
`tests/test_durability.py`：none 全丢 / batch 只保留已 fsync 前缀 /
every 一条不丢。

## 四、压缩 compact(dir)

只保留每个 key 的最新 op（含 tombstone），流程：

1. 取排他锁，扫描当前段的有效前缀；
2. 把存活记录**重编号为连续 seq 1..M** 写入 `wal-<g+1>.log.tmp`，fsync 文件；
3. `[hook: before_rename]` → `os.replace` 原子 rename 为 `wal-<g+1>.log`
   → `[hook: after_rename]` → fsync 目录；
4. `[hook: before_delete_old]` → 删除所有更低代的旧段
   → `[hook: after_delete_old]` → 再 fsync 目录。

崩溃安全性：读者与写者永远只认**最高代**段文件。

- rename 前崩溃：只有旧代，完整可用；残留 `.tmp` 下次打开时清理；
- rename 后、删旧段前崩溃：新旧两代并存，新代在 rename 前已 fsync，读者选
  新代，旧代下次打开时清理；
- 删旧段后崩溃：只剩新代。

任何注入点都不会出现「半新半旧」。`tests/test_compact.py` 在四个注入点
分别崩溃后验证：逻辑视图完整、seq 连续、可继续 append。

**取舍：tombstone 在 compact 后保留（不物理删除）。** 理由：

- 压缩的语义是「每个 key 保留最新 op」，del 就是某些 key 的最新 op；保留它
  让 `iter_records`/`dump` 的输出在压缩前后信息等价（只是删掉了被覆盖的
  历史），读者无需区分「这个 key 从没存在过」和「这个 key 被删过」；
- 若物理删除 tombstone，被删 key 会彻底从日志消失：省一点空间，但一旦将来
  引入多段合并或副本同步，就无法区分「未同步到」与「已删除」，容易复活
  旧值。
- 代价：长期大量删除的日志会积累 tombstone。缓解：可定期全量重建（当前
  未实现）。

注意：compact 会**重编号 seq**，历史 `snapshot_at(旧seq)` 视图随之失效——
压缩即放弃时间点查询能力，这是 append-only 账本做空间回收的标准代价。

## 五、读接口

```python
from wal import iter_records, snapshot_at
iter_records(dir, from_seq=1)   # 按 seq 顺序产出 Record(seq, op, key, value)
snapshot_at(dir, seq)           # 该 seq 时刻的键值视图；seq=None 为最新
```

- 读者只读打开，容忍未 fsync 的撕裂尾（读到已提交前缀为止）；
- 有效帧 seq 不连续（如 salvage 后）→ 显式抛 `SeqGapError`，绝不静默跳过；
- 文件从第 0 字节就不可读（非 WAL 文件）→ `CorruptionError`。

## 六、测试

`python3 -m pytest tests/ -q`（约 680 个用例）覆盖：

- 帧往返、坏 magic / 短帧头 / crc 校验（`test_frame.py`）
- 任意字节偏移截断的恢复不变量（参数化暴力，`test_recover.py`）
- salvage 与默认模式差异（`test_salvage.py`）
- 三档持久化崩溃语义 + before_fsync 故障注入（`test_durability.py`）
- 压缩四注入点崩溃一致性（`test_compact.py`）
- seq 空洞显式报错、`snapshot_at` 历史视图（`test_seq_gap.py`）
- 单写锁互斥（同进程 + 多进程，`test_lock.py`）
- 20 万条 batch 追加 < 3 秒（`test_perf.py`，实测约 1.9s）
- CLI 往返、dump 两次逐字节相同、退出码（`test_cli.py`）
