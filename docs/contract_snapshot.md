# 离线库快照契约（snapshot v1）

本契约描述第四类功能：对现有三套业务库（无环批次库、稳态回料库、称量小票采集库）
做**只读**的离线内容寻址快照，以及在库外恢复出可继续使用的完整库文件。
它不改变任何旧库的表结构、统计口径或入口，也不属于任何旧库格式的替代。

快照 = **对象存储 + 不可变清单**：

- 源库被当作一个整像（image）读取，按固定 **65536 字节**块切分，每块以
  SHA-256 为名存为对象；相同字节的块（即使跨快照、跨库族）只存一份。
- 清单记录版本、库族、整像长度与 SHA-256、块长，以及**有序**的块摘要与
  块长。清单字节的 SHA-256 **就是快照 ID**。

## 1. 源库读取（固定已提交快照）

- 源库以只读方式打开；若打开的读事务已经建立，其后的读取看到的是**固定
  的已提交快照**，并发写者未提交的改动不可见。
- 复制在同一个读事务内通过 SQLite 备份接口完成，因此即使写者在复制过程中
  提交，快照内容也不会改变（以固定那一刻的已提交状态为准）。写者不必等待、
  可以继续提交。
- 不改动源库：不写主文件、不执行 checkpoint、不替换、不复制活库文件后当
  快照用、不忽略 WAL。已经提交在 WAL 中的数据属于该快照，会一并进入镜像；
  未提交的 WAL 帧任何读者都看不到，因此也不会进入镜像。
- 只读打开 WAL 库时 SQLite 可能在库目录旁建立自己的 `-wal`/`-shm` 缓存文件；
  若这两个文件在打开前**不存在**，capture 结束后删除**本次打开新建的**空
  缓存文件（按设备号/inode 确认是本次建立），使源库目录保持原样。打开前已
  存在的 sidecar、以及非空（可能带有其他进程已提交帧）的 `-wal` 及其 `-shm`
  一律不动。
- 源库必须能被识别为三族之一：批次库（`lots`+`transforms`+`inputs`+
  `origin_mix`）、稳态库（`recycle_source`+`recycle_process`+`recycle_sink`
  等）、采集库（`intake_campaign`+`intake_ticket`）。其他库不猜测、不按族
  存快照，直接拒绝。

## 2. 对象与清单

### 2.1 分块

- 块长固定 65536 字节。除**最后一块**外每块恰为 65536 字节；最后一块可以较
  短；整像长度不是块长整数倍时才有短尾块。
- 块**有序**；相同摘要可重复出现（例如 SQLite 页内容相同的相邻块），出现
  次数与位置都必须保留；实现不会据此做逻辑去重，只复用相同字节对象。
- 对象文件名 = 块的 SHA-256（小写十六进制）。对象内容不可变；写入时先写
  临时文件、`fsync`，再原子改名到 `objects/<前两位>/<摘要>`。

### 2.2 清单

清单是 UTF-8 JSON，**规范编码**：键排序、无多余空白、**无末尾换行**。
顶层精确为：

```json
{"chunk_size":65536,"chunks":[{"length":36864,"sha256":"..."}],"image_length":36864,"image_sha256":"...","library_family":"lineage","snapshot_version":1}
```

| 字段 | 规则 |
| --- | --- |
| `snapshot_version` | 整数 `1`；`true`/`false` 不是整数，拒绝 |
| `library_family` | `lineage`、`recycle`、`intake_tickets` 之一 |
| `image_length` | 正整数；等于所有块长之和 |
| `image_sha256` | 64 位小写十六进制，整像摘要 |
| `chunk_size` | 恰为 `65536` |
| `chunks` | 有序数组，元素恰含 `sha256`、`length` |

解析规则：

- 拒绝**额外字段**与缺失字段；拒绝**重复键**（解码时检测）；拒绝 `NaN`/
  `Infinity`；
- 字节必须已经是规范编码：解析后重新序列化必须**逐字节相同**，因此缩进、
  空格、换行、键序改动都会被拒绝；
- 块数必须等于 `ceil(image_length / chunk_size)`，短尾块只能是最后一块，块长
  必须与位置匹配。

**快照 ID = 清单 SHA-256**。仓库中清单文件名为 `<ID>.json`，读取时同时核对
"文件名 = 清单字节摘要"与"清单内容摘要 = ID"。

## 3. 提交与失败边界

- **对象先于清单**：任何对象都在被清单引用之前完整写入并 `fsync`；清单用
  一次原子改名发布。清单一旦可见，该快照即提交。
- 失败时清理本次运行的临时文件（镜像临时文件、对象/清单临时文件）；已经
  留下的、无人引用的对象成为**孤块**，由 `gc` 回收。
- 发布前失败：不出现新清单，旧快照不受影响。发布后失败（如 stdout 写失败）：
  快照已提交，CLI 以退出码 3 说明"已完成但 stdout 失败"，不谎称回滚。
- 捕获同一库同一状态得到相同清单字节与相同 ID，重复 capture 幂等（不新增
  对象、不改写清单）。

## 4. 校验（verify）

对指定清单或全部清单：

1. 逐块读取对象：文件必存在、必为普通文件（拒绝符号链接）、长度等于清单
   记录的块长、SHA-256 等于块摘要；
2. 按序拼接并计算**整像 SHA-256**，必须等于 `image_sha256`，累计长度必须
   等于 `image_length`；
3. 在临时文件中重建整像并只读打开：`PRAGMA integrity_check` 必须为 `ok`，
   `PRAGMA foreign_key_check` 必须为空，识别出的库族必须与清单声明一致。

任一步失败即整次失败，不返回部分成功。

## 5. 恢复（restore）

- 只把快照整像原样发布为**库外新文件**：先在目标同目录写临时文件、校验整像
  摘要与库完整性，再 `os.replace` 原子替换目标。
- **拒绝覆盖已有文件**（包括符号链接目标）、拒绝输出落在快照存储目录内、
  拒绝输出是源库本体或其 `-wal`/`-shm`/`-journal` sidecar（按解析路径与
  设备号/inode 双重判断）、拒绝源库路径本身是符号链接。
- 恢复出的文件与**快照整像**逐字节一致（其 SHA-256 等于清单的
  `image_sha256`），因此保留全部数据、SQLite 类型与模式（表、视图、索引、
  外键、CHECK 约束）。整像由 SQLite 从固定已提交快照复制而成，与源库
  当前磁盘文件不保证逐字节相同（例如 WAL 已合并进主文件、页布局不同），
  但查询到的数据与约束完全一致。可以直接交给旧入口继续使用：批次库可查
  `origin_mix`/`available_origin_estimate`，稳态库保留精确分数与原始 JSON，
  采集库可以继续 `status`/`export`；在副本上还可以继续 ingest 新 campaign、
  seal、export——都作用在副本上，不改动源库。已封账 campaign 在副本上仍
  拒绝 ingest。
- 恢复失败保持旧目标完整并清理临时文件，可重跑。

## 6. forget 与 gc

- `forget` **只删除清单**，不删除对象；删除后可 `gc`。未知 ID 拒绝。
- `gc` 的**全引用预验**：先从所有现存清单重建被引用对象集合，再逐个校验
  每个被引用对象的长度与摘要；只要有一个被引用对象缺失或损坏，就整体拒绝，
  **不删除任何对象**（包括孤块），避免在引用关系不可信时误删。
- 预验通过后，才删除不被任何清单引用的对象（孤块），并清理 `tmp/` 中残留的
  临时项；完成后移除空的分片目录。
- 删除过程是**部分删除**：单个对象因 I/O 错误删不掉时记录到 `errors` 并继续
  处理其余对象。CLI 仍打印完整回执（含已删/保留/错误），随后以非零退出码
  如实报告部分删除。若 `objects/` 中出现无法按内容寻址文件规则解释的条目
  （非法分片名、非对象文件名、分片错位），同样整体拒绝且不删除。
- `tmp/` 只清理由其余步骤留下的进行中临时项；`manifests/` 中出现临时文件或
  异常条目属于存储损坏，`list`/`verify`/`gc` 一律拒绝而不是猜测。
- 同一对象被多个快照共享时，只要还有清单引用就保留。

## 7. 存储布局

```
store.json                     # {"schema_family":"lot_lineage_snapshots","schema_version":1}
objects/<aa>/<sha256>          # 不可变块对象
manifests/<sha256>.json       # 不可变清单，文件名为其摘要
tmp/                          # 进行中的临时文件；gc 可清理
```

既有非空目录缺少 `store.json` 时拒绝初始化（不误认作快照库）；`store.json`
声明其他族或版本时拒绝。

## 8. 命令

```sh
PYTHONPATH=src python3 -m lot_lineage.snapshot capture \
  --database run/lineage.sqlite --store run/snapshots [--family lineage]
PYTHONPATH=src python3 -m lot_lineage.snapshot list --store run/snapshots
PYTHONPATH=src python3 -m lot_lineage.snapshot verify \
  --store run/snapshots [--snapshot ID] [--family lineage]
PYTHONPATH=src python3 -m lot_lineage.snapshot restore \
  --store run/snapshots --snapshot ID --output run/restored.sqlite \
  [--family lineage] [--library run/lineage.sqlite]
PYTHONPATH=src python3 -m lot_lineage.snapshot manifest \
  --store run/snapshots --snapshot ID
PYTHONPATH=src python3 -m lot_lineage.snapshot forget --store run/snapshots --snapshot ID
PYTHONPATH=src python3 -m lot_lineage.snapshot gc --store run/snapshots
```

所有命令打印确定性 JSON（键排序）回执；错误打印到 stderr 并返回非零；
已提交/已发布但 stdout 失败返回 3。

## 9. 明确不保证

- **非逻辑去重**：只按固定字节块复用相同对象，不解析 SQL 行、不做业务级
  去重或压缩比较；两份快照的差异总以块粒度体现。
- **不签名**：清单与对象没有签名、证书或时间戳；摘要只用于内容完整性，不
  证明来源或作者。
- **不承诺跨版本确定性**：本契约只固定版本 1 的字段与编码；未来版本的清单
  不保证与本版本字节相同或可互相恢复。
- **不保证断电恢复**：`fsync` + 原子改名只提供进程崩溃时的"旧或新"边界，
  不对物理断电后的文件系统持久性、介质损坏或存储自身可靠性做承诺。
- 不提供加密、访问控制、压缩、增量快照（除对象复用外的传输优化）或在线
  调度。
