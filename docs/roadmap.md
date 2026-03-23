# maxc-cli 开发路线图

> 最后更新：2026-03-23

## 当前状态

maxc-cli 已完成 MVP 骨架，核心功能可用：

### 已实现

- **查询执行**：`query`（同步/异步/dry-run/分页/cursor复用）、`query cost`、`query explain`
- **任务管理**：`job submit/status/wait/result/cancel/list/diagnose`
- **元数据**：`meta list-tables/describe/search/search-columns/partitions/latest-partition/freshness/lineage`
- **数据操作**：`data sample/profile`
- **差异比较**：`diff schema/partition/data`
- **缓存管理**：`cache build/status/clear/save-semantic/get-semantic`
- **认证管理**：`auth login/whoami/can-i`
- **Agent 辅助**：`agent context`

### 基础设施

- `pip install maxc-cli` 后可直接使用 `maxc` 入口
- `pyodps` / `pandas` 已提升为基础依赖
- `auth login` 可将 AccessKey 持久化到配置文件，环境变量仍然优先
- SQLite 本地缓存（会话/元数据/语义索引/FTS5）
- agent_hints 输出
- 结构化错误处理
- 配置层级（全局/项目/.maxc）

---

## 短期任务（P0）

### 1. 发布外部 Agent 接入说明

- [ ] 提供面向外部 Agent 的 `use-maxc-cli` / Skill 指南
- [ ] 固化安装、`auth login`、`auth whoami` 的最短接入路径
- [ ] 给出环境变量优先于配置文件的明确约定

### 2. 语义搜索增强

- [ ] `maxc meta search` 支持 FTS5 语义搜索
- [ ] 结合 `table_semantic` 表的语义元数据
- [ ] 考虑向量索引（sqlite-vec）用于更好的语义匹配

### 3. 真实 backend 能力补强

- [ ] 接入真实血缘 API，替换 `meta lineage` 的 unsupported 占位结果
- [ ] 增加大结果导出 / 下载能力
- [ ] 扩展 `auth can-i` 到更多只读操作

---

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

- [ ] `data download` - 数据导出
- [ ] `data upload` - 数据导入（需评估安全风险）

---

## 长期任务（P2）

### 7. 认证增强

- [ ] OAuth2/OIDC 支持
- [ ] Token 刷新机制
- [ ] 统一凭证轮换策略

### 8. 血缘分析

- [ ] `meta lineage` - 表血缘关系
- [ ] `meta impact` - 影响分析

### 9. 资源管理

- [ ] `resource quota` - 配额查看
- [ ] `resource usage` - 使用统计

---

## 不做的事情

基于 **工具层** 定位，以下功能由外部 Agent 实现，maxc-cli 不做：

- ❌ 自然语言转 SQL（外部 Agent 负责）
- ❌ 自主执行计划（外部 Agent 负责）
- ❌ Skill 执行引擎（已移除）
- ❌ 内置 LLM 调用
- ❌ 交互式 REPL

---

## 技术债务

### 待清理

- [x] 移除 Skill 执行相关代码（skills.py 已删除）
- [x] 移除 `skill list/info` 命令
- [x] 移除 `agent skill/plan/run` 命令
- [x] 移除 `docs/base-api-roadmap.md`（过时）
- [x] 更新 `docs/implementation.md`

### 待补充

- [ ] 完善单元测试覆盖
- [ ] 添加集成测试
- [ ] CLI 帮助文档完善
- [ ] 错误消息国际化

---

## 外部 Agent 集成示例

maxc-cli 的价值在于被外部 Agent 使用。以下是推荐的集成模式：

### Claude Code 集成

```markdown
# .claude/instructions.md

当用户需要查询 MaxCompute 数据时，使用以下工具：

1. 首次接入时确保已经安装 `maxc-cli`，并完成 `maxc auth login --from-env --json` 或显式登录。
2. 先自检身份：`maxc auth whoami --json`
3. 再获取上下文：`maxc agent context --json`
4. 搜索相关表：`maxc meta search <关键词> --json`
5. 查看表结构：`maxc meta describe <表名> --json`
6. 查看样本数据：`maxc data sample <表名> --json`
7. 执行查询：`maxc query "<SQL>" --json`

所有命令默认输出 JSON，直接解析 data 字段获取结果。
```

### Cursor 集成

```markdown
# .cursorrules

使用 maxc 命令访问 MaxCompute 数据仓库。
- 初次配置优先执行 `maxc auth whoami --json` 确认身份来源
- 查询前先用 `maxc meta search` 确认表存在
- 用 `maxc data sample` 了解数据结构
- 解析 JSON 输出的 agent_hints 字段获取建议
```

---

## 版本计划

| 版本 | 目标 | 预计时间 |
|-----|------|---------|
| v0.2 | 补充 Skill 文档，FTS5 搜索 | 2026-Q1 |
| v0.3 | 安全增强，性能优化 | 2026-Q2 |
| v0.4 | 数据导入导出 | 2026-Q2 |
| v1.0 | 稳定版，完整文档 | 2026-Q3 |
