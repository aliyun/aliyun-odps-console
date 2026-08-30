# maxc-cli 开发路线图

> 最后更新：2026-08-30

## 当前状态

maxc-cli 已经进入发布硬化阶段。当前工作重点不再是补命令骨架，而是让 CLI 契约、文档、测试和外部 Codex skill 同步发布。

### 已完成

- `query / job / meta / data / auth / cache`
- OAuth 优先的 `auth login`、`auth whoami`、schema-aware `auth can-i`
- `session set/show/unset`
- `data upload/download`；下载默认拒绝覆盖已有本地文件
- `agent context/doctor/manifest`
- 标准化 JSON envelope 与 `agent_hints`
- SQLite 本地缓存、语义元数据缓存、结构化审计日志
- repo-tracked Skill `alibabacloud-maxcompute-cli` 统一由
  `src/maxc_cli/skills/` 随包安装
- `agent skill install/update/uninstall/list/diff/path` 覆盖 Skill 生命周期
- `cache build --json` 统一为单 envelope stdout + `stderr` 进度

## 短期任务（P0）

### 1. CLI + SKILL 联合发布硬化

- [x] 将 skill source 统一为 `src/maxc_cli/skills/`（随 pip 包安装，唯一源）
- [x] `agent skill` 六动词命令注册到各 Agent 平台
- [x] 对齐 README、设计文档、实现文档、测试说明
- [x] 对齐真实集成测试到当前 envelope 契约
- [ ] 固化 release smoke checks / CI，避免 CLI 与 skill 再次漂移

### 2. 真实 backend 能力补强

- [ ] 接入真实血缘 API，替换 `meta lineage` 的 unsupported 占位结果
- [x] 增加 Tunnel CSV/TSV 上传、下载能力
- [ ] 扩展 `auth can-i` 到更多只读操作

### 3. 语义搜索增强

- [ ] `maxc meta search` 支持 FTS5 语义搜索
- [ ] 结合 `table_semantic` 表的语义元数据
- [ ] 评估向量索引（sqlite-vec）用于更好的语义匹配

## 中期任务（P1）

### 4. 性能与缓存优化

- [ ] 元数据缓存自动更新策略
- [ ] 查询结果缓存过期清理
- [ ] 并发控制优化

### 5. 安全与身份增强

- [ ] 敏感数据脱敏输出
- [ ] 操作审计日志增强
- [ ] 多凭证管理
- [ ] 更细粒度权限预检查

### 6. 数据操作扩展

- [x] `data download`（已有文件保护 + 显式 `--overwrite`）
- [x] `data upload`（支持 `--dry-run` 和显式 overwrite）
- [ ] 大文件断点续传与可恢复批次

## 长期任务（P2）

### 7. 认证增强

- [x] OAuth Authorization Code + PKCE
- [x] OAuth token 刷新与临时 STS 交换
- [ ] 统一凭证轮换策略

### 8. 血缘分析

- [ ] `meta lineage`
- [ ] `meta impact`

### 9. 资源管理

- [ ] `resource quota`
- [ ] `resource usage`

## 不做的事情

基于工具层定位，以下能力由外部 Agent 实现，maxc-cli 不做：

- 自然语言转 SQL
- 自主执行计划
- Skill 执行引擎
- 内置 LLM 调用
- 交互式 REPL

## 技术债务

### 已清理

- [x] 移除 Skill 执行相关代码
- [x] 移除 `agent plan/run`
- [x] 统一 Skill 名与安装目录为 `alibabacloud-maxcompute-cli`

### 待补充

- [ ] 发布前自动化回归
- [ ] CLI 帮助文档进一步完善
- [ ] 错误消息国际化

## 版本计划

| 版本 | 目标 | 预计时间 |
|-----|------|---------|
| v0.2 | 联合发布 CLI + Codex skill，收敛发布流程 | 已完成 |
| v0.3 | 安全增强，性能优化 | 已完成 |
| v0.4 | 数据导入导出、OAuth 与 Agent 契约增强 | 已发布，持续硬化 |
| v1.0 | 稳定版，完整自动化发布 | 2026-Q4 |
