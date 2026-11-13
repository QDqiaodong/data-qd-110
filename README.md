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
- `src/lot_lineage/recycle/`：schema_version 2 稳态校验、SCC 精确求解、
  SQLite 分析库与 `python -m lot_lineage.recycle` 入口。
- `fixtures/`：MIT 许可的合成输入（含批次、PQ 循环、转换视图、多级循环演示例）。
- `examples/convert_lots_to_steady.py`：批次快照到稳态 JSON 的转换脚本。
- `docs/contract.md`、`docs/contract_recycle.md`：两套字段与口径、边界及失败约定。
- `tests/`：业务结果、边界与失败测试（旧 9 项 + 稳态 34 项）。
- `requirements*.lock`、`pyproject.toml`：环境和构建声明。

本基线不承诺实时同步或生产调度。仅验证本机环境的事实见交付记录；本仓库不
声称已经验证任何特定 Linux 容器。所有路径均由调用参数提供，无宿主机绝对路径。
