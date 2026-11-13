# 小票模型差分 CLI 契约（diffharness v1）

本契约描述第四套独立能力：对离线称量小票采集模型（`intake_tickets`，见
[contract_intake.md](contract_intake.md)）做黑盒差分测试的命令行工具
`python -m lot_lineage.diffharness`。它与批次库、稳态库、采集库和快照库的
表名、入口与统计口径完全不重叠，**只新增、不修改任何旧业务**；运行期仅使用
Python 标准库与 SQLite，被测系统只通过其命令行以真实子进程访问。

## 1. 设计边界与独立性

- 差分工具自带一个**独立内存模型**（`diffharness/oracle.py`），从契约文本
  重新实现 CSV 解析、单位整数换算、重传/冲突身份、封账八条核验、无环检查与
  规范 `lots`/`recipes` JSON 构造。整个 `diffharness` 包**不得** import
  `lot_lineage.intake.*`、`lot_lineage.graph` 或任何生产解析/校验/封账函数；
  测试以源码扫描强制该边界。内存模型只用于产生预期，不接触真实数据库。
- 被测命令（SUT）通过可配置前缀以**真实子进程**执行，默认前缀
  `python -m lot_lineage.intake`；也可传入包装器（如
  `python3 /path/buggy_wrapper.py`）在不改生产代码的前提下注入故障。
- 每次完整执行都在新建临时目录里使用一个**此前不存在的空 SQLite 文件**，
  运行之间无任何状态继承；约简的每个候选同样在全新空库重跑。
- 对真实库的观测只使用标准库 `sqlite3` 的只读 SQL，不调用任何生产函数。

## 2. 场景格式（generate）

`generate --seed N --steps K --output scenario.json` 输出确定性、自包含的
场景对象（格式标识 `lot_lineage_diff_scenario_v1`）。同一 `seed`/`steps`
两次生成的 JSON **逐字节一致**：随机源为包内确定性 LCG，不依赖全局
`random` 种子；键排序、固定缩进、固定行尾。

场景跨**两个业务 campaign**（`cmp-a`、`cmp-b`）外加一个只读的
`cmp-unknown`，两个 campaign 故意复用相同 `ticket_id` 与批次名以验证互不
混算。操作覆盖下列契约分支，每个分支带稳定 `coverage_tags`：

- 部分到达的新票 ingest；
- 等价重传（`500 g` 与 `0.500 kg`）与逐字节相同重传（后者不新增来源）；
- 单文件内同票冲突、跨文件同票冲突（整文件回滚，新票连带回滚）；
- 结构坏文件（坏表头、参差行、坏单位、非整克小数）在写库前拒绝；
- 缺票状态先封账失败，补齐后封账成功，重复封账幂等；
- 封账后新票、等价重传、坏文件三类 ingest 全部拒绝；
- open/unknown 的 status，open/unknown/sealed 的 export；
- 跨 campaign 的 ID 与批次名重用。

每个引用到的 CSV 以 UTF-8 文本内嵌在 `files[]` 并附 SHA256；场景不依赖任何
外部生成文件。`steps` 小于完整计划时取前缀，大于时以确定性的幂等
ingest/status/export 填充。

## 3. 运行与差异判定（run）

`run --scenario ... [--prefix ...] [--counterexample ce.json]` 逐操作并行推进
内存模型与真实子进程，在首个差异处停止。每个操作比较四层，按顺序取首个
差异作为“首差异”：

1. **成败类别**：内存模型给出 `ok` 与六类失败（malformed / conflict /
   sealed / seal_failed / unknown_campaign / not_sealed）；运行器仅依据进程
   退出码与独立 stderr 子串把真实结果映射到同一词表，不 import 生产异常。
2. **回执投影**：成功时按操作类型比较固定键集合（如 ingest 的
   `new_tickets`/`replayed_occurrences`/`new_source`/各计数，seal 的摘要与
   `already_sealed`，export 的 `sha256`/`json_bytes`）。回显输入的
   `campaign` 键不作为模型预期比较。
3. **库状态**：对每个 campaign 独立只读 SQL 取得状态、票集合（含票/来源
   关系与记录位置）、来源集合、封账边与封账 JSON，与内存模型视图逐字段比较。
4. **导出文件**：成功导出必须与内存模型预测的封账 JSON **逐字节一致**且
   摘要匹配；导出失败时，已存在的旧导出文件摘要必须保持不变。

任何预测为失败的操作，其前后库视图与旧导出文件都必须不变；该“失败不改
状态、不改旧导出”保证由第三、四层比较强制。

无差异时退出码 0，stdout 只报告 `covered_count` 与 `coverage_tags`，并显式
声明“本场景未见差异不等于实现无错”，**绝不宣称无错**。发现差异时退出码 4，
并可写出自包含反例。

## 4. 反例格式与 replay

反例标识为 `lot_lineage_diff_counterexample_v1`，完全自包含：

- 原始 CSV 全文（`files[].csv_text`）与全部操作序列；
- 首差异签名（操作 `op_id`、操作类型、campaign、差异层级、字段、预期/实际
  成败类别）及其规范化 SHA256；
- 首差异的预期/实际结构、真实进程退出码、stdout 与 stderr 尾部、操作前后的
  独立库视图；
- 使用的命令前缀。

`replay --counterexample ... [--prefix ...]` 仅从反例内嵌数据重建场景，
**不读取 seed、不依赖 generate 产物或任何外部 CSV**，在新空库重跑；签名一致
时 `reproduces=true` 并以退出码 4 返回。

## 5. 约简（reduce）

`reduce --counterexample ... --budget B --prefix ... --output reduced.json`
严格按三轮 delta debugging 进行，且**每个候选都在全新空库真实重跑**：

1. 逐操作删除（含首差异之后的尾部），并剪枝不再被引用的 CSV 文件；
2. 逐条删除仍被引用 CSV 的数据记录（表头-only 候选跳过；故意坏文件不做
   记录/质量手术，因为那会改变故障类别）；
3. 对剩余质量单元格按确定性减半阶梯缩小（保持整克与字段约束，`kg` 非整千
   克时回退 `g` 表示，零值仅允许 loss）。

候选仅当仍发生差异且**签名完全相同**（同 `op_id`、差异层级、字段、成败
类别）时才被接受。完成时执行最小性核查：任删一个剩余操作或任删一条剩余
CSV 记录都不再复现同签名，核查通过才标 `complete=true`；预算（真实重跑次数
上限）耗尽则 `complete=false` 并如实给出已用次数，不假装完成。

同一种子/预算的约简输出逐字节一致。约简报告给出操作数、CSV 记录数与 CSV
字节数的前后对比；约简后的反例仍可用 `replay` 在新进程复现同签名。

## 6. 退出码与确定性

| 退出码 | 含义 |
| --- | --- |
| 0 | generate/reduce 成功；或 run/replay 未见差异 |
| 1 | replay 未能复现反例签名 |
| 2 | 参数或输入文件错误（stderr 说明） |
| 4 | run 发现差异，或 replay 成功复现反例 |

所有 JSON 产物（场景、反例、约简报告、stdout 回执）均为确定性字节：键排序、
固定两空格缩进、单个结尾换行；不写入时间戳、主机名或绝对路径。子进程超时、
非零退出与坏 stdout 都被捕获并归入差异，不导致工具自身崩溃。

## 7. 与旧业务的关系

本能力不创建任何业务表、不打开或修改既有业务库（除被测 SUT 自己在临时目录
中创建的临时库外），不改变 `lot_lineage.intake`、`lot_lineage.cli`、
`lot_lineage.recycle`、`lot_lineage.snapshot` 的任何函数与入口。故障注入通过
外部包装器脚本（`tests/buggy_wrappers/`，仅标准库）实现，生产源码保持不变；
旧 171 项测试与本包 20 项测试共同通过。
