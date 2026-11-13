# 稳态闭路回料核算契约（schema_version 2）

本契约描述与 `lots/recipes` 批次模型完全独立的第二类数据：同一核算时段内
库存没有净变化的稳态平均物流。它**不是**某个实际批次的历史路径，也不模拟
初始库存或达到稳态的过程；结果是稳态平均来源估计。批次模型继续保持无环、
单产出、`build(source,target)` 与 `lot_lineage.cli` 行为不变。

## 1. 输入结构

UTF-8 JSON，顶层精确包含五个键：`schema_version`、`processes`、`feeds`、
`transfers`、`sinks`。`schema_version` 只接受整数 `2`（字符串与布尔拒绝）。

- `processes`：`process_id`。
- `feeds`：`feed_id`、`origin_id`、`to_process`、`mass_g`，系统外部投入。
- `transfers`：`transfer_id`、`from_process`、`to_process`、`mass_g`，
  系统内转移；允许自回流（`from_process == to_process`）及不同 ID 的平行物流。
- `sinks`：`sink_id`、`from_process`、`kind`、`mass_g`，
  `kind` 仅 `product` 或 `loss`，表示真正离开系统的产品或损耗。

`processes`、`feeds`、`sinks` 非空；`transfers` 可以为空。零流量通过省略
记录表示，不存在质量为 0 的记录。

### 1.1 标识与质量规则

- 各类 ID 在各自数组内唯一；重复记录即使内容完全相同也拒绝。
- 标识必须是非空、无首尾空白的字符串，按原文区分大小写。
- `origin_id` 可以在多笔 feed 中重复，代表同一种外部来源。
- 所有过程引用必须存在。
- `mass_g` 是 1 至 10^12 的整数克；`bool` 不是整数（`True`/`False` 拒绝）。
- 结构、引用或数值错误定位到数组记录的 JSON Pointer（如 `/feeds/0`），
  并尽量附带业务键，如 `unknown process reference 'GHOST'`。

### 1.2 节点收支与可达性

对每个过程 `v`，总入流 `M_v` = 外部 feed + 所有进入 `v` 的 transfer
（自回流同时计入收支两侧）。要求：

1. `M_v > 0`；
2. `M_v` 精确等于全部出 transfer 与出 sink 之和（整数克精确相等，差 1 g 也拒绝）；
3. 每个过程都能从某笔外部 feed 沿零条或多条 transfer 到达；
4. 每个过程都能沿零条或多条 transfer 到达某个 sink。

全图都检查，包括与主要生产线无关的连通分量。只有内部循环而没有外部来源
（或没有出口）的单元，即使局部收支平衡，来源也无法由这份数据确定：整次
拒绝并在错误信息中列出相关单元，不忽略、不任意赋值、不补造初始来源。

## 2. 来源方程与稳态假定

设备完全均匀混合：产品、损耗及返回上游的物料都带走与混合单元相同的来源比例。
对每个外部来源 `o` 与过程 `v`：

```
M_v * x[v,o] - sum(m[u,v] * x[u,o]) = feed[v,o]
```

求和覆盖所有进入 `v` 的内部 transfer（含自回流）；`feed[v,o]` 是来源 `o`
直接投入 `v` 的质量之和（同一来源的多笔 feed 先聚合，拆分一笔 feed 不改变
任何比例或终端归属）。每条内部出流及 sink 使用其 `from_process` 的来源比例，
归属质量 = 该流 `mass_g` × 对应比例。

### 2.1 唯一可识别性

校验条件保证每个过程都有外部 feed 可达且都能到达 sink。把图按强连通分量
（SCC）收缩成 DAG 后，每个 SCC 沿凝聚图拓扑序只依赖已求解的上游 SCC。
对一个 SCC 联立其全部节点方程：系数矩阵对角元为正的总入流 `M_v`，非对角元
为内部转移质量的负值。每个节点至少有一条"离开本 SCC 或出系统"的净出口
（否则不可达 sink），该矩阵严格对角占优且不可约，因而非奇异；结合非负的
右端项，解存在、唯一且非负。无外部来源的纯内部循环在输入阶段即被拒绝，
因此不会出现零空间导致的多解。若精确求解仍发现奇异、负比例或比例和不为 1，
明确失败，不返回猜测值。

### 2.2 生产算法边界

- 按内部转移图的 SCC 组织计算，沿凝聚图依赖顺序处理；
- 仅在存在循环依赖的 SCC（自环也算循环）内共同求解方程，无环节点退化为
  一次加权平均；
- 多个来源共享同一系数矩阵，只做一次高斯-若尔当消元，各来源作为不同的
  右端列同时求解；
- 全程 `fractions.Fraction` 精确有理数，方程残差与守恒检查同样精确；
- 不枚举回流路径、不按克数循环、不切断回流、不以浮点传播到某容差作为答案。

## 3. 三层守恒口径

1. **节点逐来源层**：每个节点、每种来源的逐来源入流精确等于逐来源出流。
2. **物流层**：每条 transfer/sink 的各来源归属质量之和精确等于该流质量。
3. **外部来源层**：对每种 origin，全部 feed 投入精确等于全部 product 与
   loss 中的归属质量之和；内部 transfer 既不计入投入也不计入产出，
   内部转移总量（如 180 g）不会放大外部投入（100 g）、产品（90 g）或
   损耗（10 g）。

## 4. SQLite 分析库（独立格式）

格式标识 `recycle_steady`，版本 2，与批次库的表名完全不重叠；旧库的表与
返回结构保持兼容。表（均开启外键）：

| 表 | 主键 | 关联与说明 |
| --- | --- | --- |
| `recycle_source` | `source_id`（恒为 1） | 格式族、版本、SHA256、原始 JSON BLOB |
| `recycle_meta` | `key` | 计数、来源摘要、凝聚顺序等键值元数据 |
| `recycle_record_pointer` | `(record_kind,business_key)` | 业务键到 JSON Pointer（如 `/feeds/0`） |
| `recycle_scc` | `scc_id` | 是否循环分量、成员数 |
| `recycle_scc_member` | `(scc_id,process_id)` | SCC 成员及稳定序号 |
| `recycle_process` | `process_id` | 所属 SCC、整数入/出吞吐量 |
| `recycle_feed` / `recycle_transfer` / `recycle_sink` | 各自业务 ID | 原始事实外键到过程，附 Pointer |
| `recycle_process_origin` | `(process_id,origin_id)` | 节点来源比例分子/分母文本 |
| `recycle_transfer_origin` | `(transfer_id,origin_id)` | 逐 transfer 来源质量分子/分母文本 |
| `recycle_sink_origin` | `(sink_id,origin_id)` | 逐 sink 来源质量分子/分母文本 |
| `recycle_origin_terminal` | `origin_id` | 外部投入（十进制文本）、产品/损耗分子分母 |
| `recycle_global_total` | `metric` | 外部投入、内部转移、产品、损耗、节点吞吐总量（十进制文本） |

精确数值以规范约分的十进制文本保存（`numerator/denominator`，分母恒正，
零项显式存为 `0/1` 而非省略），不使用 SQLite REAL 或有损聚合作为正式值。
单条质量 ≤ 10^12 可放入 64 位整数列；累计总量以无界十进制文本保存，
超过 SQLite 整数范围也不截断。分析人员移走输入文件后，仍可从
`recycle_source.raw_json` 读取原文、用 `source_sha256` 核对摘要，并利用
实际质量与比例独立重算方程和终端守恒。

## 5. 确定性与失败重跑边界

- 输入只捕获一次原始字节，解析与 SHA256 来自同一份内容。
- stdout 为确定性 JSON（键排序），相同输入重建的摘要字节一致；SQLite 按
  稳定键查询的逻辑内容一致（不要求数据库文件二进制一致），不写入当前
  时间或机器绝对路径。
- 单写者离线构建：在目标同目录的临时 SQLite 中完成全部写入、检查与关闭，
  再 `os.replace` 原子替换目标。
- 目标不得与输入指向同一文件，含可识别的符号/硬链接别名（按 `st_dev`/
  `st_ino` 与解析路径双重判断）。
- 输入、计算、写库或替换失败时进程非零退出，旧目标保持完整，临时文件
  在正常与异常路径都清理；校验失败发生在任何写库之前。
- 替换成功后若 stdout 失败，数据库仍已发布，CLI 以退出码 3 如实说明。
- 不提供并发发布或断电恢复协议。
