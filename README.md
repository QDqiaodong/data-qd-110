# data-qd-110 · 陶瓷配料批次追溯

将多级陶瓷配料的合成批次与配方接入 SQLite，校验投入产出、消耗上限和图结构，
按拓扑顺序计算每批原料来源贡献。该数据基线帮助核验加工链追溯口径，
核心为图数据转换和来源分析，不是库存管理后台。

示例原料总量1800克，加工损耗100克，剩余各批可用量1700克。
X由A600/B400投入，产出900，A/B比例3/5和2/5；Y由X300/C200混合，
A/B/C比例9/25、6/25、2/5。可用原料来源A=940、B=460、C=300克。
命令行 JSON 与 SQLite 的 lots/inputs/origin_mix 支持独立核算。

## 安装与运行

要求 Python 3.11 或更高；先确认 `python3 --version`。以下命令在仓库根目录执行：

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-build.lock
.venv/bin/python -m pip install --no-build-isolation --no-deps .
.venv/bin/python -m lot_lineage.cli --source fixtures/lots.json --database run/lineage.sqlite
.venv/bin/python -m unittest discover -s tests -v
```

运行依赖与测试仅使用标准库；首次安装构建工具通常需要联网。每个仓库有独立
 `.venv`。安装的是源码的构建副本；编辑源码后须再次执行安装命令。
所有演示输出在忽略的 `run/` 中；演示可重复执行。临时目录中的测试不改变 fixture。
CLI 参数错误或无效数据返回非零；失败原因打印到 stderr。

## 目录

- `src/lot_lineage/`：解析、业务转换及命令行入口。
- `fixtures/`：MIT 许可的合成输入；来源声明见 DATA-SOURCES.md。
- `docs/contract.md`：字段和口径、边界及失败约定。
- `tests/`：业务结果、边界与失败测试。
- `requirements*.lock`、`pyproject.toml`：环境和构建声明。

本基线不承诺实时同步或生产调度。仅验证本机环境的事实见交付记录；本仓库不
 声称已经验证任何特定 Linux 容器。所有路径均由调用参数提供，无宿主机绝对路径。
