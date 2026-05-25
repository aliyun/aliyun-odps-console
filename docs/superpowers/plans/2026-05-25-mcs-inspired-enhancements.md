# maxc-cli 增强路线（mcs 启发版）

> 作者：dingxin · 日期：2026-05-25
> 参考对象：`/Users/dingxin/pythonProject/meta_ai_agent`（`mcs` / Semantic CLI）
> 关联：`docs/roadmap.md`、`docs/superpowers/plans/2026-05-21-cli-quality-pass.md`

## 背景与动机

maxc-cli 与 mcs 同为"给 AI agent 调用的 MaxCompute 结构化 CLI"，但在版本治理、SKILL 分发、代码组织等若干环节上 mcs 更成熟。本文把可借鉴点拆成 4 期，按"价值 × 实施成本"排序，每期独立可发布。

完成 4 期后，本仓库具备：
- 客户端能感知/强制升级、服务端能 disable 有缺陷的版本；
- SKILL 与 CLI 命令名不会静默漂移；
- 单文件巨石被拆，后续重构成本下降；
- 对 agent 调用友好（全局 flag 容忍、exit code 分级）。

## 对比结论（摘要）

| 维度 | maxc-cli | mcs | 取舍 |
|---|---|---|---|
| CLI 命令分组 / `--help` | argparse 平铺 | click `_OrderedGroup` 按用途 | 暂保留 argparse，仅借鉴排序思想 |
| 全局 flag 位置容忍 | 严格 | `_hoist_global_flags` 自动前移 | 借鉴 |
| 输出 envelope | `Envelope v2.0` 已有 | 一致，但增加 `remediation`/`context` 字段 | 借鉴字段补全 |
| Exit code | 一律 1 | 按错误类 4/5/6… | 借鉴 |
| SKILL 子命令 | 仅 `install-skill` | `install/update/uninstall/diff/list/path` 全套 | 借鉴 |
| SKILL 平台扩展 | 7 平台硬编码 | dataclass + registry + deprecated alias | 借鉴 |
| SKILL ↔ CLI 漂移检测 | 无 | 单元测试比对 verb | 借鉴 |
| 自升级 | 无 | `mcs update` + 安装方式探测 + `os.execvp` | 借鉴 |
| 版本探测 | 无 | daemon 探 `latest.json`，6h 缓存，banner | 借鉴 |
| 版本 hard-block | 无 | `disabled_versions`/`min_supported` | 借鉴 |
| 发布触发 | `workflow_dispatch` 手动 | tag `v*` push 自动 | 借鉴 |
| CI 数量 | GitHub + Aone + GitLab 三套 | Aone 一套 | 瘦身 |
| 代码组织 | `app.py` 4110 行 / `cli.py` 1897 行 god class | 一命令一文件分层 | 重构 |
| 测试 fixture | 基础 mock | autouse 防 picker hang、stub `~/.maxc`、mini http server | 借鉴 |

---

## P1 · 版本治理（自升级 + 探测 + hard-block）

**目标**：让用户/agent 知道有新版，让维护方能 disable 有缺陷的版本。

### 范围

1. **`maxc self-upgrade` 子命令**
   - 探测安装方式：`uv tool` / `pipx` / `pip --user` / `pip` / `UNKNOWN`；UNKNOWN 给手动 hint。
   - `os.execvp` 启动新 CLI `--version` 自验证（Windows 提示重开 shell）。
   - 支持 `--version <pin>` 降级、`--check` 仅探测不升级、`--from-url <override>` 用于内网测试。
   - 升级成功后联动重装 SKILL（见 P2 完成后再接，本期先打 stub）。

2. **被动版本检查**
   - 启动 daemon 线程探 `${MAXC_UPDATE_BASE_URL or OSS 默认}/latest.json`，6h TTL，缓存到 XDG `cache_dir`。
   - 在 CLI 主进程退出 `finally` 阶段渲染 banner（`✨ maxc 0.x.y 可用，运行 maxc self-upgrade 升级`）。
   - 静默条件：`--format json` / `--json` / `--quiet` / `MAXC_NO_UPDATE_CHECK=1` / `--version` / `--help` / `self-upgrade` 子命令自身。
   - 错误 carry-forward：网络瞬时失败保留上次缓存的"有新版"信号。

3. **`latest.json` schema 与服务端**
   - 字段：`schema_version`、`latest`、`stable`、`channels`、`disabled_versions`、`min_supported`、`notice`、`released_at`、`sha256_by_platform`、`wheel_url`、`bin_url_by_platform`。
   - 在 CI release job 中由 `scripts/publish_latest.sh` 写入 OSS。
   - `disabled` 命中时客户端 exit 2 并打印强提示；`min_supported` 不满足时给出非阻塞警告（避免锁死）。

4. **版本号 single source**
   - 移除 `setup.py:12` 硬编码，全部走 `src/maxc_cli/__init__.py:5 __version__`。CI release job 校验 git tag 与 `__version__` 一致才允许发布。

### 交付物

- 新代码：`src/maxc_cli/upgrade.py`（探测 + 执行）、`src/maxc_cli/update_check.py`（daemon + 缓存 + banner）。
- CLI 入口：`maxc self-upgrade`（cli.py 子命令）。
- 服务端：`scripts/publish_latest.sh` 生成并上传 `latest.json`；首版 schema 文档放 `docs/LATEST_JSON_SCHEMA.md`。
- 测试：mini http server fixture（仿照 mcs `latest_json_server`）、安装方式探测单测、banner 静默矩阵测试。

### 风险与边界

- 网络阻塞主进程：必须 daemon 线程 + 短超时（建议 2s connect / 3s read）。
- 企业内网无外网：`MAXC_UPDATE_BASE_URL` 可改为内网镜像；探测失败必须 silently degrade。
- `os.execvp` 在 macOS code-signed 场景可能要求 sigtool 重签；先在 pip/uv-tool 路径下走 wheel 升级，不动 PyInstaller 二进制（后者由 `bootstrap.sh` 重新下载替换）。
- envelope 模式下 banner 一律不出，避免破坏 agent 解析。

### 验收

- `maxc self-upgrade --check` 在新版可用时 exit 0 并打印 JSON envelope。
- `MAXC_NO_UPDATE_CHECK=1 maxc query 'select 1'` 不发起任何网络请求。
- 把 `latest.json.disabled_versions` 写入当前版本，下次任意 `maxc ...` 命令 exit 2 并打印明确升级指引。
- coverage：upgrade.py + update_check.py ≥ 90%。

---

## P2 · SKILL 分发体系成熟

**目标**：把已经投入大量内容的 SKILL 升到与 mcs 等价的分发/治理水平。

### 范围

1. **`agent skill` 子命令展开**
   - `install` / `update` / `uninstall` / `diff` / `list` / `path`。
   - 现有 `install-skill` 保留为 deprecated alias，输出 warning，下版本（v0.5）移除。
   - `update --all` 遍历所有曾安装的平台 symlink 重指向当前 wheel 内 `skills/`。
   - `diff` 输出本地已安装 SKILL 与 wheel 内的差异（文件级 + 行级）。
   - `list` 显示所有曾安装的平台 + 时间戳 + 版本戳（来自 `.maxc-skill-version`）。
   - `path [--platform X]` 打印路径，便于脚本拼接。

2. **平台注册表化**
   - `src/maxc_cli/agent_platforms.py`：`Platform` dataclass + `REGISTRY` 列表。每平台字段：`name`、`target_path_template`、`requires_dir`、`extra_files`（如 `claude-code` 的 `plugin.json`）、`aliases`、`deprecated_aliases`。
   - 当前硬编码的 7 平台逻辑迁入；从 `app.py` 抽离 ~200 行。

3. **SKILL ↔ CLI 漂移检测**
   - 新测试 `tests/test_skill_cli_consistency.py`：解析 `skills/SKILL.md` + `skills/references/*.md` 中的 ` ```bash` 与 ` ```shell` 块，提取所有 `maxc <verb>` 调用，与实际 `cli.build_parser` 注册的子命令求差集。任何漂移即 fail。
   - 例外白名单（如示例输出里的字符串）显式标注。

4. **SKILL 内容微改**
   - 在 `SKILL.md` 顶部增加 mcs 风格的"按 intent 加载 reference"决策矩阵（保留双语 Red-lines 风格）。
   - 每个 `references/*.md` 顶部加 `> Loaded on demand —` 自我说明。
   - 不动主体内容，避免节外生枝。

### 交付物

- `app.py` 中 SKILL 相关代码迁出到 `src/maxc_cli/skills_manager.py` 与 `agent_platforms.py`。
- 新测试文件 2 个。
- `docs/SKILL_INSTALL.md` 更新命令清单。

### 风险

- `diff` 输出过大：默认只显示文件级 + counts，加 `--unified` 才出 diff。
- 漂移检测假阳性：`grep` 提取要容忍 here-doc / 引号包裹的命令模板。

### 验收

- `maxc agent skill list` 在新机器上无输出，安装一次后出现该平台。
- 故意把 SKILL.md 里某个 `maxc query` 改成 `maxc qeury`，`pytest tests/test_skill_cli_consistency.py` 必 fail。
- `maxc agent install-skill` 仍可用，但 stderr 打印 deprecation。

---

## P3 · 代码拆分（巨石文件）

**目标**：消除 `app.py` / `cli.py` 单文件巨石，为后续任何重构腾出空间。这一期是纯重构，对外行为零变化。

### 范围

1. **`app.py` 4110 行按命令组拆分**
   ```
   src/maxc_cli/app/
   ├── __init__.py          # 重新导出 MaxCApp facade
   ├── core.py              # MaxCApp 类骨架 + 共享 helper（backend lazy init、envelope 包装）
   ├── query.py             # query/run/cost/explain/submit handler
   ├── job.py               # job 全套
   ├── meta.py              # meta + semantic
   ├── data.py              # sample/profile/upload/download
   ├── auth.py              # login/login-external/whoami/can-i
   ├── agent.py             # context/skill/install-skill
   ├── cache.py             # build/status/clear
   └── session.py           # set/show/unset
   ```
   `MaxCApp` 保留为 facade，方法委派给对应模块的 free function 或 mixin。

2. **`cli.py` 1897 行同步拆**
   ```
   src/maxc_cli/cli/
   ├── __init__.py          # main / run 入口
   ├── parser.py            # build_parser
   ├── handlers/{query,job,meta,...}.py
   ├── emit.py              # _emit_envelope / 错误 envelope / agent_hints 路由
   └── samples.py           # 现 _samples.py 内容
   ```

3. **测试不破**
   - 所有现有 import path 通过 `src/maxc_cli/app/__init__.py` 与 `src/maxc_cli/cli/__init__.py` 的 re-export 保持兼容（如 `from maxc_cli.app import MaxCApp`、`from maxc_cli.cli import main` 仍然 work）。
   - 内部测试用的 `from tests.test_cli_mock import FakeODPS` 反向 import 暂保留但加 TODO。

4. **PR 拆分**
   - 一次 PR = 一个命令组，从 `agent`（最独立）开始，最后做 `query`/`job`（相互引用多）。每个 PR 必须全测过 + smoke pass。

### 风险

- 循环 import：先把 `MaxCApp` 共享状态（backend、config、cache）抽成 `core.py` 单独导入。
- import path 变化破坏外部 import：保留 re-export 一个版本周期。

### 验收

- 拆完后 `app/` 各文件 < 500 行；`cli/` 同。
- `pytest -m "unit"` 与 `tests/test_pyinstaller_bundle.py` 全绿。

---

## P4 · 体验细节 & 工程瘦身

**目标**：低成本高收益的小修。

### 范围

1. **全局 flag hoist**：参照 mcs `_hoist_global_flags`，把 `--format` / `--json` / `--config` / `--quiet` / `--debug` / `-v` / `--version` 从子命令后位置自动前移到主 parser。覆盖测试。
2. **Exit code 分级**：`exceptions.py` 每类异常加 `exit_code` ClassVar（`ValidationError=4`、`AuthError=5`、`NotFound=6`、`Permission=7`、`SqlError=8`、`BackendConn=9`、内部错=1）；envelope 与 stderr 同时携带 exit_code。文档化到 `docs/EXIT_CODES.md`。
3. **Envelope 字段补全**：`error` 段补 `remediation`（人类可读修复建议）和 `context`（结构化触发上下文）。`Envelope v2.0` 兼容，老消费者忽略未知字段。
4. **CI 瘦身**
   - 保留：GitHub Actions = release（tag 触发，五平台 PyInstaller + PyPI），Aone = 质量门（lint + unittest + audit）。
   - 移除：GitLab CI（`.gitlab-ci.yml`）。
   - GitHub release 工作流改为 tag `v*` push 自动触发，并校验 tag = `__version__`。
5. **MegaLinter / coverage gate**：在 Aone 加 RUFF + MYPY + coverage ≥ 80%（先按目录加豁免，不强求全仓库 strict）。
6. **仓库根杂物清理**：`nohup.out`、`2026-04-29-*.txt`、`use-maxc-cli.zip`、`.qoder/` 等加入 `.gitignore` 或删除。

### 验收

- `maxc query 'select 1' --json` 与 `maxc --json query 'select 1'` 等价。
- `pytest -q` 覆盖率 ≥ 80%（按 P3 完成后的目录粒度计）。
- 仓库 `git status -s` 在 clean checkout 后无杂项 untracked。

---

## 版本目标对应

| 期次 | 目标版本 | 预计窗口 |
|---|---|---|
| P1 版本治理 | v0.4.0 | 2026-06 |
| P2 SKILL 体系 | v0.5.0 | 2026-06 末 |
| P3 代码拆分 | v0.6.0 | 2026-07 |
| P4 体验 & 工程 | v0.7.0 | 2026-07 末 |

P1 与 P2 可并行（不同文件）；P3 必须串行（动 `app.py` 时不要同时改命令）；P4 散落在 P1–P3 PR 中顺手做也可。

## 不做的事情（避免越界）

- 不引入 click 替换 argparse（成本高、收益小）。
- 不引入插件化后端协议（当前仅 `OdpsBackend`，YAGNI）。
- 不做 telemetry/上报（隐私敏感，留给商业版决定）。
- 不重写 SKILL 内容主体，仅做结构调整。

## 待澄清（进入 P1 brainstorming 处理）

- `latest.json` 该放 OSS 哪个路径？沿用 `maxcompute-repo/maxc-cli/latest.json` 还是新增？
- 版本检查 banner 是否应在 `--help` 之后追加？
- 当客户端是 PyInstaller 二进制时，自升级走 `bootstrap.sh` 重下还是 in-process 替换？
- `min_supported` 不满足时的措辞强度（警告 vs 阻塞）。
- 是否把 banner 落到 stderr 还是 stdout（影响 shell pipe）。
