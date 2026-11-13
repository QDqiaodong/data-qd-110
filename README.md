# data-qd-110 · 陶瓷配料批次追溯与闭路回料稳态核算

将多级陶瓷配料的合成批次与配方接入 SQLite，校验投入产出、消耗上限和图结构，
按拓扑顺序计算每批原料来源贡献。该数据基线帮助核验加工链追溯口径，
核心为图数据转换和来源分析，不是库存管理后台。

示例原料总量1800克，加工损耗100克，剩余各批可用量1700克。
X由A600/B400投入，产出900，A/B比例3/5和2/5；Y由X300/C200混合，
A/B/C比例9/25、6/25、2/5。可用原料来源A=940、B=460、C=300克。
命令行 JSON 与 SQLite 的 lots/inputs/origin_mix 支持独立核算。

## 闭路回料稳态核算（独立新能力）

陶瓷配料线会把部分混合料送回上游继续混合，同一份物料可能多次经过设备。
沿批次边逐层累加会重复计量，删掉回流边又会改变来源比例。因此新增一套与
批次模型完全独立的稳态核算：描述同一核算时段、库存无净变化的稳态平均物流，
各条流量折算为整数克，设备完全均匀混合，产品、损耗及返回上游的物料带走
相同来源比例。结果是**稳态平均来源估计**，不表示某个实际批次的历史路径，
也不模拟初始库存或达到稳态的过程。

输入为 `schema_version: 2` 的 UTF-8 JSON（`processes/feeds/transfers/sinks`），
支持自回流、平行物流和多级循环分量。方程

`M_v·x[v,o] − Σ m[u,v]·x[u,o] = feed[v,o]`

按内部转移图的强连通分量（SCC）组织、沿凝聚图拓扑序求解：只在循环分量内
用精确 `Fraction` 高斯-若尔当共同求解，无环节点退化为加权平均；不枚举回流
路径、不按克数循环、不切断回流、不做浮点容差传播。纯内部循环（无外部
来源/无出口）即使局部平衡也整次拒绝并定位单元。字段与口径见
[`docs/contract_recycle.md`](docs/contract_recycle.md)。

### 稳态命令与 SQLite 查询

```sh
PYTHONPATH=src python3 -m lot_lineage.recycle \
  --source fixtures/recycle_pq.json --database run/recycle.sqlite
```

两单元循环例（A→P 90g、B→Q 10g、P→Q 120g、Q→P 60g、产品 30+60g、损耗 10g）
精确得到 P 的 A/B 比例 39/41 与 2/41，Q 为 36/41 与 5/41；产品中
A/B = 3330/41g、360/41g，损耗中 A/B = 360/41g、50/41g。外部投入 100g、
产品 90g、损耗 10g，内部转移 180g 不出现在外部来源守恒账上。

```sh
# 节点来源比例（精确十进制文本，零项显式为 0/1）
sqlite3 run/recycle.sqlite \
  "SELECT process_id, origin_id, numerator||'/'||denominator
   FROM recycle_process_origin ORDER BY process_id, origin_id;"
# 逐 sink 的来源质量
sqlite3 run/recycle.sqlite \
  "SELECT sink_id, origin_id, numerator||'/'||denominator
   FROM recycle_sink_origin ORDER BY sink_id, origin_id;"
# 各外部来源的投入、产品与损耗总量
sqlite3 run/recycle.sqlite "SELECT * FROM recycle_origin_terminal ORDER BY origin_id;"
# 输入被移走后仍可取回原文并核对摘要
sqlite3 run/recycle.sqlite \
  "SELECT source_sha256, length(raw_json) FROM recycle_source;"
# SCC 成员与凝聚顺序
sqlite3 run/recycle.sqlite \
  "SELECT s.scc_id, s.cyclic, m.process_id FROM recycle_scc s
   JOIN recycle_scc_member m USING(scc_id) ORDER BY s.scc_id, m.member_order;"
```

把无环批次快照转换为稳态视图（每个 lot 一个混合节点，raw 为外部 feed，
recipe 投入为 transfer，available 为 product sink，loss 为 loss sink，
零项省略）：

```sh
PYTHONPATH=src python3 examples/convert_lots_to_steady.py \
  fixtures/lots.json run/converted.json
PYTHONPATH=src python3 -m lot_lineage.recycle \
  --source run/converted.json --database run/converted.sqlite
```

转换视图复现旧来源比例与产品来源总量，并给出损耗来源 A=60g、B=40g。

## 离线称量小票采集与封账导出（独立新能力）

电子秤按文件分批导出称量小票，原料登记、成品产出、投入称量和损耗小票可能
分几次到达。现有批次追溯只接收完整 `lots`/`recipes` JSON，不能把尚未凑齐的
生产记录当成完整谱系。新增的 `lot_lineage.intake` 把这些多次到达的原始小票
先收进独立 SQLite 库，操作者**封账**后才导出一份可直接交给现有批次追溯的
完整 JSON。

小票 CSV 表头精确有序为 `ticket_id,kind,lot_id,input_lot_id,mass,unit`。
`kind` 为 `raw`/`output`/`input`/`loss`；`lot_id` 对 `raw` 指原料批次，
对其他三种指产出批次；仅 `input` 必须填写 `input_lot_id`；`unit` 只接受
`g`/`kg`，质量按整数克精确换算（不经 `float`）。完整字段、事务与封账契约见
[`docs/contract_intake.md`](docs/contract_intake.md)。

```sh
PYTHONPATH=src python3 -m lot_lineage.intake ingest \
  --database run/intake.sqlite --campaign ceramic-1 --source fixtures/intake/batch1.csv
PYTHONPATH=src python3 -m lot_lineage.intake ingest \
  --database run/intake.sqlite --campaign ceramic-1 --source fixtures/intake/batch2.csv
PYTHONPATH=src python3 -m lot_lineage.intake status \
  --database run/intake.sqlite --campaign ceramic-1
PYTHONPATH=src python3 -m lot_lineage.intake seal \
  --database run/intake.sqlite --campaign ceramic-1
PYTHONPATH=src python3 -m lot_lineage.intake export \
  --database run/intake.sqlite --campaign ceramic-1 --output run/sealed.json
# 交回现有批次追溯（旧 CLI 原样可用）
PYTHONPATH=src python3 -m lot_lineage.cli \
  --source run/sealed.json --database run/lineage.sqlite
```

示例先送 X 投入 A 600 g / B 0.400 kg 及损耗 100 g（`batch1.csv`），再补
A 1000 g、B 500 g、C 300 g、X 产出 900 g、Y 投入 X 300 g / C 200 g、
Y 产出 450 g 与损耗 50 g（`batch2.csv`）。封账导出后旧 CLI 得到原料 1800、
损耗 150、可用 1650 克；Y 的 A/B/C 比例 9/25、6/25、2/5，可用来源
A 922、B 448、C 280 克。

同一 campaign 内 `ticket_id` 是小票业务身份：规范化内容完全相同（`0.500 kg`
与 `500 g` 等价）视为重传，不重复扣料；相同 ID 不同内容为冲突，整次文件
回滚。不同 `ticket_id` 即使内容相同也是独立称量。封账在单一事务中保存
不可变的规范 JSON 及 SHA256 并把 campaign 变为 `sealed`，之后任何 `ingest`
都拒绝（即使只重传旧票），不提供解封或更正。不同 campaign 的票与批次 ID
可重用且互不混算。移走原始 CSV 后，仍可从库中读取原文、核对 SHA256，并
查到每张规范票的全部来源与记录位置、每条封账配方边由哪些票累加形成。

## 离线库快照与库外恢复（独立新能力）

三套业务库（批次库、稳态库、采集库）都只是磁盘上的 SQLite 文件。需要在不
改动活库的前提下留一份可校验、可恢复的离线副本时，新增的
`lot_lineage.snapshot` 把源库只读地读成一个**固定已提交快照**，按固定
65536 字节块写成内容寻址对象，再发布一份不可变清单；清单的 SHA256 就是
快照 ID。恢复时把整像原样发布为库外**新文件**，可直接交给旧入口继续使用。

- 源库只读：不写主文件、不 checkpoint、不替换；并发写者未提交的数据不可见，
  写者可以继续提交；已经提交在 WAL 中的数据属于快照。capture 结束会把本次
  只读打开**新建**的 `-wal`/`-shm` 缓存文件删掉，已存在的 sidecar 不动。
- 块有序，最后一块可短，相同块可重复出现；相同字节的块只存一份对象。
- 对象完整写入并 fsync 后才引用它的清单才可能发布；清单原子改名即提交。
  失败清理本次临时文件，无人引用的孤块留待 `gc`。
- `verify` 逐块核对摘要与长度、整像 SHA256、SQLite 完整性/外键与库族；
  `forget` 只删清单；`gc` 先验证全部引用对象，再删除孤块，I/O 部分删除
  如实报告。拒绝未知族、坏格式、符号链接、穿越与别名。
- 恢复保留全部数据、SQLite 类型与模式；副本可继续 ingest/seal/export，
  已封账 campaign 仍拒绝 ingest。不提供逻辑去重、签名、跨版本确定性与
  断电保证。完整契约见 [`docs/contract_snapshot.md`](docs/contract_snapshot.md)。

```sh
# 先后构建三套库（上一节命令），再对它们做快照
PYTHONPATH=src python3 -m lot_lineage.snapshot capture \
  --database run/lineage.sqlite --store run/snapshots --family lineage
PYTHONPATH=src python3 -m lot_lineage.snapshot capture \
  --database run/recycle.sqlite --store run/snapshots --family recycle
PYTHONPATH=src python3 -m lot_lineage.snapshot capture \
  --database run/intake.sqlite --store run/snapshots --family intake_tickets
PYTHONPATH=src python3 -m lot_lineage.snapshot list --store run/snapshots
PYTHONPATH=src python3 -m lot_lineage.snapshot verify --store run/snapshots
# 恢复为库外新文件（拒绝覆盖已有文件或源库本体）
PYTHONPATH=src python3 -m lot_lineage.snapshot restore \
  --store run/snapshots --snapshot <ID> --output run/restored-intake.sqlite \
  --library run/intake.sqlite
# 恢复出的采集库仍可 status/export，再交给旧 CLI
PYTHONPATH=src python3 -m lot_lineage.intake status \
  --database run/restored-intake.sqlite --campaign ceramic-1
# 只删清单，再回收无人引用的块
PYTHONPATH=src python3 -m lot_lineage.snapshot forget \
  --store run/snapshots --snapshot <ID>
PYTHONPATH=src python3 -m lot_lineage.snapshot gc --store run/snapshots
```

一次跑完全部演示（建三库、快照、移走源库与原始 CSV 后恢复、WAL 并发写、
重复共享块与 >3 块 BLOB、独立可达集与 gc）：

```sh
PYTHONPATH=src python3 examples/snapshot_demo.py
```

## 安装与运行

要求 Python 3.11 或更高；先确认 `python3 --version`。以下命令在仓库根目录执行：

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-build.lock
.venv/bin/python -m pip install --no-build-isolation --no-deps .
.venv/bin/python -m lot_lineage.cli --source fixtures/lots.json --database run/lineage.sqlite
.venv/bin/python -m lot_lineage.recycle --source fixtures/recycle_pq.json --database run/recycle.sqlite
.venv/bin/python -m unittest discover -s tests -v
```

不安装时可直接用 `PYTHONPATH=src python3 -m ...` 运行，全部运行依赖与测试
仅使用 Python 标准库和 SQLite；首次安装构建工具通常需要联网。每个仓库有
独立 `.venv`。安装的是源码的构建副本；编辑源码后须再次执行安装命令。
所有演示输出在忽略的 `run/` 中；演示可重复执行。临时目录中的测试不改变
fixture。CLI 参数错误或无效数据返回非零；失败原因打印到 stderr。

## 目录

- `src/lot_lineage/`：批次解析、业务转换及命令行入口。
- `src/lot_lineage/intake/`：离线称量小票采集、封账、导出与
  `python -m lot_lineage.intake` 入口（SQLite 独立格式 `intake_tickets`）。
- `src/lot_lineage/recycle/`：schema_version 2 稳态校验、SCC 精确求解、
  SQLite 分析库与 `python -m lot_lineage.recycle` 入口。
- `src/lot_lineage/snapshot/`：三套业务库的只读快照、内容寻址对象、
  不可变清单、校验/恢复/forget/gc 与 `python -m lot_lineage.snapshot` 入口。
- `fixtures/`：MIT 许可的合成输入（含批次、PQ 循环、转换视图、多级循环演示例，
  以及 `fixtures/intake/` 下分两批到达的合成称量小票）。
- `examples/convert_lots_to_steady.py`：批次快照到稳态 JSON 的转换脚本。
- `examples/snapshot_demo.py`：三库快照、移源后恢复、WAL 并发与 gc 演示。
- `docs/contract.md`、`docs/contract_recycle.md`、`docs/contract_intake.md`、
  `docs/contract_snapshot.md`：四套字段与口径、边界及失败约定。
- `tests/`：业务结果、边界与失败测试（旧 9 项 + 稳态 37 项 + 采集封账 59 项
  + 快照 65 项）。
- `requirements*.lock`、`pyproject.toml`：环境和构建声明。

本基线不承诺实时同步或生产调度。仅验证本机环境的事实见交付记录；本仓库不
声称已经验证任何特定 Linux 容器。所有路径均由调用参数提供，无宿主机绝对路径。
