# 离线称量小票采集、封账与导出契约（intake_tickets v1）

本契约描述第三类数据：电子秤分批导出的**原始称量小票**。小票可能分多次到达，
在凑齐并封账之前**不是**完整生产谱系。采集阶段只收票，封账时才把该 campaign
的全部票作为一批，按既有批次契约（[contract.md](contract.md)）核验，并保存
不可变的规范 `lots`/`recipes` JSON 供现有 `build`/`lot_lineage.cli` 直接读取。
它与 `lots/recipes` 批次库（表 `lots`/`transforms`/...）和稳态库
（`recycle_*`）表名完全不重叠；旧库不改动、统计口径不变。

## 1. 输入 CSV

UTF-8 文本，有且仅有表头，精确且按序为：

```
ticket_id,kind,lot_id,input_lot_id,mass,unit
```

每一数据行是一张小票。**数据记录从 1 编号，表头不计**；错误信息定位到
`record N`（必要时附 `ticket_id` 与物理行号）。以下任一情况整份文件拒绝：

- 表头重复、缺失、多余或顺序/大小写不符；重复/错序表头；
- 字段数不等于 6 的参差行；CSV 语法错误；非法 UTF-8；
- 没有任何数据行；空文件。

字段规则：

| 字段 | 规则 |
| --- | --- |
| `ticket_id` | 非空、无首尾空白的字符串，大小写敏感；campaign 内唯一业务身份 |
| `kind` | 精确为 `raw`、`output`、`input`、`loss` |
| `lot_id` | 非空、无首尾空白；`raw` 时为原料批次，其余三种为产出批次 |
| `input_lot_id` | 仅 `kind=input` 必须非空且无首尾空白；其他 kind 必须为空 |
| `mass` | 非负普通十进制字符串，最多三位小数 |
| `unit` | 精确为 `g` 或 `kg` |

`mass` 不接受符号、空白、指数、`NaN`/`Infinity`、十六进制、下划线；小数位
最多三位。换算在**整数域**内完成（`g`×1，`kg`×1000），绝不经过 `float`；
换算结果必须是整数克（如 `0.0001 kg` = 0.1 g 拒绝）。`raw`/`output`/`input`
的克数须为 1..10^12；`loss` 可为 0..10^12。`0.500 kg` 与 `500 g` 规范化为
同为 500 克。

## 2. 采集（ingest）与小票身份

采集阶段**只**校验单行结构/数值与 `(campaign_id, ticket_id)` 身份，不要求
引用完整：可以先收投入或损耗，之后再补原料和产出；不因暂时缺引用删票或
生成虚假批次。

- 身份键是 `(campaign_id, ticket_id)`。同一 campaign 内规范化内容
  `(kind, lot_id, input_lot_id, 整数克)` 完全相同即视为**重传**，不新增票、
  不重复扣料；相同 `ticket_id` 不同规范内容为**冲突**。
- 同一规则在单文件内部与跨文件一致，**不依赖行序**。同一文件内相同 ID 的
  多次相同内容都接受（首次出现为规范内容），任一处不同内容则整份文件拒绝。
- 不同 `ticket_id` 即使其他内容完全相同也各表示**独立称量**，不按质量或批次
  去重。
- 不同 campaign 之间 `ticket_id`、`lot_id`、`source` 内容可重用，互不混算。

### 2.1 来源与位置

每次 ingest 只捕获**一次**原始字节，解析、SHA256 与入库原文都用这同一份内容。
对每个**先前未见过**的规范内容（按原文 SHA256）保存一次原始字节，并保存该文件
里**每一个**数据记录的位置 `(source, record_no, line_no)`——包括同票重传所在
来源，而非只记首次来源。完全相同字节的重复导入不新增来源也不新增位置关系。
一个 campaign 可以有多个来源（含同票等价内容来自不同字节的不同来源）。

### 2.2 事务与回执

一份文件的全部新票、来源与全部位置关系在**同一个写事务**中共同提交。任何
冲突、写库异常或中断都回滚整份文件，库保持此前的 `open` 票与来源；修正后可
重跑。写事务采用 `BEGIN IMMEDIATE`；并发写者使用 SQLite 有界等待
（`busy_timeout`，CLI 默认 5 秒），并在写事务内读取 campaign 状态与票身份，
不用事务外的旧查询覆盖事实。

ingest 回执区分：`new_tickets`（本次新增的票身份数）、`replayed_occurrences`
（本次文件中已存在/同文件重复出现的记录数）、`new_source`（0/1）、当前
`ticket_count`、`source_count`。若 stdout 打印失败，CLI 退出码 3 并在 stderr
如实说明事务**已提交**，不假称回滚。

## 3. 封账（seal）

`seal` 把该 campaign 全部已接收的规范小票作为**一批最终状态**核验：

1. 每个出现的 `lot` 恰好一张 `raw` 或 `output` 定义票（零张或多张皆失败）。
2. 每个 `output`（produced）至少一张 `input`，且恰好一张 `loss` 票；显式零
   损耗也必须有小票。
3. `raw` 不得有投入或损耗票。
4. 一个 `(output, input_lot)` 可有多张不同投入小票，先**精确整数求和**形成
   唯一配方边；合计仍须遵守既有单项 10^12 克上限（合计超限拒绝）。
5. 引用存在（被引用的投入批次必须有定义票）；
6. 每道投入 = 产出 + 损耗；
7. 全局各批总消耗不超过批次质量；
8. 全图无环，包括与主线无关的连通分量。

此外复用既有批次校验/拓扑函数 `validate_document` 与 `propagate`，保证产物
正是旧契约接受的形态。未知 campaign、以及存在但没有任何小票的 campaign 拒绝。

### 3.1 不可变封账产物

封账成功在**同一事务**中：

- 写出规范 `lots`/`recipes` JSON 字节及其 SHA256；
- 记录每条封账配方边的合计克数（十进制文本）与形成它的票数，以及每条边由
  哪些 `ticket_id` 累加形成；
- 把 `intake_campaign.status` 变为 `sealed`。

数组与配方投入按业务 ID 稳定排序，不加入时间、绝对路径或来源文件顺序；
相同业务小票集合（与到达顺序、等价单位写法无关）应得到相同 JSON 字节与
相同 SHA256。

封账失败保留原 `open` 票与来源，不留下部分配方或 `sealed` 标记，可补票后
再 seal。`sealed` 重复 seal 幂等并返回同一摘要。封账后任何 `ingest` 都拒绝
（即使仅重传旧票）；**不提供解封、覆盖或更正历史功能**。

## 4. 导出（export）

`export` 只读**已存在的** `sealed` campaign，把库中已封账的规范 JSON 原样
安全写到 `--output`。产物可直接由现有 `build` 与旧 CLI 读取。

- `open` 或缺失库导出失败；不隐式创建库。
- 先从**同一 SQLite 快照**完整读取封账 JSON 并核对 SHA256，再在输出同目录
  写临时文件、`os.replace` 原子替换目标；失败保持旧目标完整，清理正常与异常
  路径的临时文件。
- 拒绝 `output` 与 `database` 为同一文件，包括可识别的符号/硬链接别名。
- 导出失败不撤销已封账事实，可重跑；数据库提交与输出文件**不是**跨文件事务。

## 5. 状态（status）

`status` 只读、不建库，报告 campaign 的 `open`/`sealed` 状态、唯一票数
（`ticket_count`）、来源数（`source_count`）以及封账摘要（open 时为 `null`）。

## 6. 库格式（`intake_tickets` v1）

全部开启外键。关键表、键与关联：

| 表 | 主键 | 关联与说明 |
| --- | --- | --- |
| `intake_meta` | `key` | `schema_family`、`schema_version` 等 |
| `intake_campaign` | `campaign_id` | `status` ∈ {open,sealed}；sealed 时非空 `seal_sha256`、`sealed_json`；CHECK 约束保证 open 不带封账产物 |
| `intake_source` | `source_pk` | 外键 campaign；`(campaign_id, source_sha256)` 唯一；原始字节 BLOB 与长度 |
| `intake_ticket` | `(campaign_id, ticket_id)` | 外键 campaign；规范化 `kind/lot_id/input_lot_id/grams`；CHECK 保证仅 input 有 `input_lot_id` |
| `intake_source_ticket` | `(source_pk, record_no)` | 外键 source 与 `(campaign_id,ticket_id)`；保存每张票在每个来源中的记录号与物理行号 |
| `intake_sealed_edge` | `(campaign_id, output_lot, input_lot)` | 封账配方边：合计克数（十进制 TEXT）、形成它的票数 |
| `intake_sealed_edge_ticket` | `(campaign_id, output_lot, input_lot, ticket_id)` | 每条封账边由哪些票累加形成 |

单张小票克数 ≤ 10^12 放入 SQLite 整数列；封账配方边的合计与任何累计总量以
十进制 TEXT 保存，超过 SQLite 有符号 64 位范围也不截断。

移走输入 CSV 后，仍可从 `intake_source.raw_bytes` 读取原文、用
`source_sha256` 核对摘要；用 `intake_source_ticket` 查到每张规范票的全部来源
与记录位置；用 `intake_sealed_edge` / `intake_sealed_edge_ticket` 查到每条
封账配方边由哪些票累加形成。库中不只保存一份无法关联原始小票的最终 JSON。

示例查询：

```sh
sqlite3 run/intake.sqlite \
  "SELECT status, seal_sha256 FROM intake_campaign WHERE campaign_id='ceramic-1';"
sqlite3 run/intake.sqlite \
  "SELECT COUNT(*) FROM intake_ticket WHERE campaign_id='ceramic-1';"
sqlite3 run/intake.sqlite \
  "SELECT s.source_sha256, length(s.raw_bytes), st.ticket_id, st.record_no, st.line_no
   FROM intake_source s JOIN intake_source_ticket st USING(source_pk)
   WHERE s.campaign_id='ceramic-1' AND st.ticket_id='T-0001';"
sqlite3 run/intake.sqlite \
  "SELECT output_lot,input_lot,total_grams,ticket_count FROM intake_sealed_edge
   WHERE campaign_id='ceramic-1' ORDER BY output_lot,input_lot;"
sqlite3 run/intake.sqlite \
  "SELECT output_lot,input_lot,ticket_id FROM intake_sealed_edge_ticket
   WHERE campaign_id='ceramic-1' ORDER BY output_lot,input_lot;"
```

## 7. 拒绝旧库与兼容性

使用明确的新库格式 `intake_tickets` v1。打开既有数据库时先只读检测：

- 含 `intake_meta` 且声明 `intake_tickets` v1 → 接受；
- 声明了其他格式族或版本 → 拒绝；
- 识别到稳态库（`recycle_meta`）、批次库（`lots`+`transforms`+`inputs`+
  `origin_mix`）或其他非空库 → 拒绝并**不改动**原文件；
- 空文件/不存在 → ingest 可初始化新库。

原 `build`（`lot_lineage.pipeline`）、`lot_lineage.cli` 与独立
`lot_lineage.recycle` 的行为、统计口径与测试保持兼容；本功能只用 Python
标准库与 SQLite，不连接真实设备或服务，不提交运行库、真实生产小票或凭据。

## 8. 确定性与失败边界

- 原始字节只捕获一次，解析、SHA256 与保存原文来自同一内容。
- 封账 JSON 为确定性字节（键排序、稳定数组顺序、无时间/绝对路径）；
  相同业务小票集合 → 相同字节与摘要。重复导出字节一致。
- 每份文件采集、每次封账、每次导出替换都在明确的失败点原子化：失败保持
  旧状态、清理临时文件、可重跑。
- 并发写事务由 SQLite 有界等待串行化；不依赖任意外部 sleep 猜时序。
